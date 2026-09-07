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

//! When `iceberg_query_storage_mode` is `auto`, this rule may rewrite
//! `LogicalIcebergIntermediateScan` (columnar Iceberg) to `LogicalScan` (row Hummock)
//! for Iceberg engine tables.

use std::collections::HashSet;
use std::sync::Arc;

use risingwave_common::session_config::IcebergQueryStorageMode;

use super::prelude::{PlanRef, *};
use crate::TableCatalog;
use crate::catalog::source_catalog::SourceCatalog;
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{Logical, LogicalIcebergIntermediateScan, LogicalScan, generic};
use crate::optimizer::rule::InfallibleRule;
use crate::session::SessionImpl;

pub struct IcebergEngineStorageSelectionRule;

impl InfallibleRule<Logical> for IcebergEngineStorageSelectionRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let scan = plan.as_logical_iceberg_intermediate_scan()?;
        let ctx = plan.ctx();
        let session = ctx.session_ctx();

        // Only apply when storage mode is auto.
        if session.config().iceberg_query_storage_mode() != IcebergQueryStorageMode::Auto {
            return None;
        }
        let source_catalog = scan.source_catalog()?;
        let table = get_table_from_iceberg_source(session, source_catalog)?;

        // Append-only Iceberg engine tables do not materialize rows in their Hummock table. Their
        // batch plan combines Iceberg with the sink log store, so rewriting the Iceberg side to
        // the dummy Hummock table would silently drop committed rows.
        if table.append_only {
            return None;
        }

        let prefer_rowstore = check_point_lookup(scan, &table);
        if !prefer_rowstore {
            return None;
        }

        rewrite_to_table_scan(scan, &table)
    }
}

impl IcebergEngineStorageSelectionRule {
    pub fn create() -> BoxedRule {
        Box::new(IcebergEngineStorageSelectionRule)
    }
}

/// Rewrite the intermediate Iceberg scan to a Hummock `LogicalScan`.
fn rewrite_to_table_scan(
    scan: &LogicalIcebergIntermediateScan,
    table: &Arc<TableCatalog>,
) -> Option<PlanRef> {
    // output_column_mapping already maps to table-column indices (built at
    // construction time), so we can use it and origin_condition directly.
    let output_col_idx = scan
        .hummock_rewrite
        .output_column_mapping
        .to_parts()
        .0
        .iter()
        .copied()
        .try_collect()?;
    let table_scan = generic::TableScan::new(
        output_col_idx,
        table.clone(),
        vec![],
        vec![],
        scan.ctx(),
        scan.hummock_rewrite.origin_condition.clone(),
        scan.core.as_of.clone(),
    );
    Some(LogicalScan::from(table_scan).into())
}

fn get_table_from_iceberg_source(
    session: &SessionImpl,
    source_catalog: &SourceCatalog,
) -> Option<Arc<TableCatalog>> {
    let catalog_reader = session.env().catalog_reader().read_guard();
    let schema = catalog_reader
        .get_schema_by_id(source_catalog.database_id, source_catalog.schema_id)
        .ok()?;
    let table_name = source_catalog.iceberg_table_name()?;
    let table = schema.get_created_table_by_name(&table_name)?;
    Some(table.clone())
}

/// Returns `true` when the predicate has equality-to-constant conditions on
/// *all* PK columns of the table, making this a point lookup that benefits
/// from the row store's key-value access pattern.
fn check_point_lookup(scan: &LogicalIcebergIntermediateScan, table: &TableCatalog) -> bool {
    let pk_column_names: HashSet<&str> = table.pk_column_names().into_iter().collect();
    if pk_column_names.is_empty() {
        return false;
    }

    // origin_condition is already in table-column index space.
    let eq_input_refs = scan
        .hummock_rewrite
        .origin_condition
        .get_eq_const_input_refs();
    let eq_col_names: HashSet<&str> = eq_input_refs
        .iter()
        .filter_map(|input_ref| table.columns().get(input_ref.index()))
        .filter(|c| !c.is_hidden())
        .map(|c| c.name.as_str())
        .collect();

    // All PK columns must be covered by equality predicates.
    pk_column_names.is_subset(&eq_col_names)
}
