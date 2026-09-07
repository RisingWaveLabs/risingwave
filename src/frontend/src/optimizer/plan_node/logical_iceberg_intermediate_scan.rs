// Copyright 2026 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;

use educe::Educe;
use iceberg::expr::Predicate;
use itertools::Itertools;
use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::catalog::{ColumnCatalog, Field};
use risingwave_common::types::DataType;
use risingwave_connector::source::iceberg::IcebergTimeTravelInfo;

use super::generic::GenericPlanRef;
use super::utils::{Distill, childless_record};
use super::{
    ColPrunable, ExprRewritable, Logical, LogicalPlanRef as PlanRef, PlanBase, PredicatePushdown,
    ToBatch, ToStream, generic,
};
use crate::catalog::source_catalog::SourceCatalog;
use crate::error::Result;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::utils::column_names_pretty;
use crate::optimizer::plan_node::{
    ColumnPruningContext, LogicalFilter, LogicalProject, LogicalSource, PredicatePushdownContext,
    RewriteStreamContext, ToStreamContext,
};
use crate::utils::{
    ColIndexMapping, Condition, ExtractIcebergPredicateResult, extract_iceberg_predicate,
};

/// Predicate and column mapping needed when rewriting an Iceberg scan to a
/// Hummock `LogicalScan`. Only consumed by `IcebergEngineStorageSelectionRule`;
/// the normal Iceberg path ignores these fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HummockRewriteInfo {
    /// The accumulated predicate over *table* column indices (not output columns),
    /// built from the extracted portion of pushed-down predicates.
    pub origin_condition: Condition,
    /// Maps current output column indices → original table column indices.
    pub output_column_mapping: ColIndexMapping,
}

impl HummockRewriteInfo {
    /// Create with an initial source→table column mapping.
    /// For non-engine iceberg sources (no associated table), pass an identity mapping.
    pub fn new(source_to_table_mapping: ColIndexMapping) -> Self {
        Self {
            origin_condition: Condition::true_cond(),
            output_column_mapping: source_to_table_mapping,
        }
    }

    /// Accumulate a new predicate, remapping it through the current column mapping.
    pub fn add_predicate(&self, extracted_condition: Condition) -> Self {
        let mut mapping = self.output_column_mapping.clone();
        let remapped = extracted_condition.rewrite_expr(&mut mapping);
        Self {
            origin_condition: self.origin_condition.clone().and(remapped),
            output_column_mapping: mapping,
        }
    }

    /// Prune columns: compose the column mapping so that the new output indices
    /// still map back to the original table column indices.
    pub fn prune_columns(&self, required_cols: &[usize]) -> Self {
        let map = required_cols
            .iter()
            .map(|&idx| Some(self.output_column_mapping.map(idx)))
            .collect();
        Self {
            origin_condition: self.origin_condition.clone(),
            output_column_mapping: ColIndexMapping::new(
                map,
                self.output_column_mapping.target_size(),
            ),
        }
    }
}

/// `LogicalIcebergIntermediateScan` is an intermediate plan node used during optimization
/// of Iceberg scans. It accumulates predicates and column pruning information before
/// being converted to the final `LogicalIcebergScan` with delete file anti-joins.
///
/// This node is introduced to reduce the number of Iceberg metadata reads. Instead of
/// reading metadata when creating `LogicalIcebergScan`, we defer the metadata read
/// until all optimizations (predicate pushdown, column pruning) are applied.
///
/// The optimization flow is:
/// 1. `LogicalSource` (iceberg) -> `LogicalIcebergIntermediateScan`
/// 2. Predicate pushdown and column pruning are applied to `LogicalIcebergIntermediateScan`
/// 3. `LogicalIcebergIntermediateScan` -> `LogicalIcebergScan` (with anti-joins for delete files)
#[derive(Debug, Clone, PartialEq, Educe)]
#[educe(Hash)]
pub struct LogicalIcebergIntermediateScan {
    pub base: PlanBase<Logical>,
    pub core: generic::Source,
    #[educe(Hash(ignore))]
    pub iceberg_predicate: Predicate,
    pub time_travel_info: IcebergTimeTravelInfo,
    /// For Iceberg engine tables: maps source column name → target Hummock `DataType`.
    /// This remapping is applied to the output schema so that the intermediate scan's
    /// output types match the Hummock table types, avoiding unnecessary double casts
    /// when the storage selection rule rewrites to a Hummock `LogicalScan`.
    /// Empty for non-engine-table Iceberg sources.
    #[educe(Hash(ignore))]
    pub table_column_type_mapping: HashMap<String, DataType>,
    /// Info needed only when rewriting to a Hummock row-store scan.
    #[educe(Hash(ignore))]
    pub hummock_rewrite: HummockRewriteInfo,
}

impl Eq for LogicalIcebergIntermediateScan {}

impl LogicalIcebergIntermediateScan {
    pub fn new(
        logical_source: &LogicalSource,
        time_travel_info: IcebergTimeTravelInfo,
        table_column_type_mapping: HashMap<String, DataType>,
        // Maps source-column indices to Hummock table-column indices so that
        // `HummockRewriteInfo` tracks predicates and projections in table
        // index space. Pass `ColIndexMapping::identity(n)` when there is no
        // associated Hummock table (e.g. standalone iceberg sources).
        source_to_table_mapping: ColIndexMapping,
    ) -> Self {
        assert!(logical_source.core.is_iceberg_connector());

        let mut core = logical_source.core.clone();
        // Apply type remapping: change the source column types to Hummock table types
        // so that the output schema has Hummock types.
        for col in &mut core.column_catalog {
            if let Some(target_type) = table_column_type_mapping.get(col.name()) {
                col.column_desc.data_type = target_type.clone();
            }
        }
        let hummock_rewrite = HummockRewriteInfo::new(source_to_table_mapping);
        let base = PlanBase::new_logical_with_core(&core);
        assert!(logical_source.output_exprs.is_none());
        LogicalIcebergIntermediateScan {
            base,
            core,
            iceberg_predicate: Predicate::AlwaysTrue,
            time_travel_info,
            table_column_type_mapping,
            hummock_rewrite,
        }
    }

    pub fn source_catalog(&self) -> Option<&SourceCatalog> {
        self.core.catalog.as_deref()
    }

    /// Fields carrying the iceberg-side column types (before the engine-table Hummock
    /// type remapping), for predicate pushdown. Derived from the source catalog, which
    /// the remapping never touches.
    fn iceberg_side_fields(&self) -> Vec<Field> {
        let catalog = self
            .core
            .catalog
            .as_ref()
            .expect("iceberg intermediate scan must have a source catalog");
        let by_name: HashMap<&str, &ColumnCatalog> =
            catalog.columns.iter().map(|c| (c.name(), c)).collect();
        self.core
            .column_catalog
            .iter()
            .map(|col| {
                let source_col = by_name
                    .get(col.name())
                    .expect("output column must exist in the source catalog");
                Field::from(&source_col.column_desc)
            })
            .collect()
    }

    pub fn output_columns(&self) -> impl ExactSizeIterator<Item = &str> {
        self.core.column_catalog.iter().map(|c| c.name.as_str())
    }

    pub fn add_predicate(
        &self,
        iceberg_predicate: Predicate,
        extracted_condition: Condition,
    ) -> Self {
        LogicalIcebergIntermediateScan {
            iceberg_predicate: self.iceberg_predicate.clone().and(iceberg_predicate),
            hummock_rewrite: self.hummock_rewrite.add_predicate(extracted_condition),
            ..self.clone()
        }
    }

    /// Returns true if this intermediate scan has type remapping for Iceberg engine tables.
    pub fn has_type_mapping(&self) -> bool {
        !self.table_column_type_mapping.is_empty()
    }

    pub fn clone_with_required_cols(&self, required_cols: &[usize]) -> Self {
        assert!(!required_cols.is_empty());

        let mut core = self.core.clone();
        core.column_catalog = required_cols
            .iter()
            .map(|idx| core.column_catalog[*idx].clone())
            .collect();
        core.row_id_index = required_cols
            .iter()
            .position(|idx| Some(*idx) == self.core.row_id_index);

        let base = PlanBase::new_logical_with_core(&core);

        LogicalIcebergIntermediateScan {
            base,
            core,
            iceberg_predicate: self.iceberg_predicate.clone(),
            time_travel_info: self.time_travel_info.clone(),
            table_column_type_mapping: self.table_column_type_mapping.clone(),
            hummock_rewrite: self.hummock_rewrite.prune_columns(required_cols),
        }
    }
}

impl_plan_tree_node_for_leaf! { Logical, LogicalIcebergIntermediateScan }

impl Distill for LogicalIcebergIntermediateScan {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let verbose = self.base.ctx().is_explain_verbose();
        let mut fields = Vec::with_capacity(if verbose { 4 } else { 2 });

        if let Some(catalog) = self.source_catalog() {
            fields.push(("source", Pretty::from(catalog.name.clone())));
        } else {
            fields.push(("source", Pretty::from("unknown")));
        }
        fields.push(("columns", column_names_pretty(self.schema())));

        if verbose {
            fields.push(("predicate", Pretty::debug(&self.iceberg_predicate)));
            fields.push((
                "output_column",
                Pretty::debug(&self.output_columns().collect_vec()),
            ));
            fields.push(("time_travel_info", Pretty::debug(&self.time_travel_info)));
        }

        childless_record("LogicalIcebergIntermediateScan", fields)
    }
}

impl ColPrunable for LogicalIcebergIntermediateScan {
    fn prune_col(&self, required_cols: &[usize], _ctx: &mut ColumnPruningContext) -> PlanRef {
        if required_cols.is_empty() {
            // If required_cols is empty, we use the first column of iceberg to avoid the empty schema.
            LogicalProject::new(self.clone_with_required_cols(&[0]).into(), vec![]).into()
        } else {
            self.clone_with_required_cols(required_cols).into()
        }
    }
}

impl ExprRewritable<Logical> for LogicalIcebergIntermediateScan {}

impl ExprVisitable for LogicalIcebergIntermediateScan {}

impl PredicatePushdown for LogicalIcebergIntermediateScan {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        _ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        let ExtractIcebergPredicateResult {
            iceberg_predicate,
            extracted_condition,
            remaining_condition,
        } = extract_iceberg_predicate(predicate, &self.iceberg_side_fields());
        let plan = self
            .add_predicate(iceberg_predicate, extracted_condition)
            .into();
        if remaining_condition.always_true() {
            plan
        } else {
            LogicalFilter::create(plan, remaining_condition)
        }
    }
}

impl ToBatch for LogicalIcebergIntermediateScan {
    fn to_batch(&self) -> Result<crate::optimizer::plan_node::BatchPlanRef> {
        // This should not be called directly. The intermediate scan should be
        // converted to LogicalIcebergScan first via the materialization rule.
        Err(crate::error::ErrorCode::InternalError(
            "LogicalIcebergIntermediateScan should be converted to LogicalIcebergScan before to_batch".to_owned()
        )
        .into())
    }
}

impl ToStream for LogicalIcebergIntermediateScan {
    fn to_stream(
        &self,
        _ctx: &mut ToStreamContext,
    ) -> Result<crate::optimizer::plan_node::StreamPlanRef> {
        unreachable!("LogicalIcebergIntermediateScan is only for batch queries")
    }

    fn logical_rewrite_for_stream(
        &self,
        _ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        unreachable!("LogicalIcebergIntermediateScan is only for batch queries")
    }
}
