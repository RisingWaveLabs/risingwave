// Copyright 2025 RisingWave Labs
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

use std::sync::Arc;

use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::array::VECTOR_DISTANCE_TYPE;
use risingwave_common::bail;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::{DataType, StructType};
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_pb::common::PbDistanceType;
use risingwave_sqlparser::ast::AsOf;

use crate::OptimizerContextRef;
use crate::catalog::index_catalog::VectorIndex;
use crate::expr::{ExprDisplay, ExprImpl};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::generic::{
    GenericPlanNode, GenericPlanRef, VectorIndexLookupJoin, ensure_sorted_required_cols,
};
use crate::optimizer::plan_node::utils::{Distill, childless_record};
use crate::optimizer::plan_node::{LogicalPlanRef as PlanRef, *};
use crate::optimizer::property::FunctionalDependencySet;
use crate::utils::Condition;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct VectorSearchLookupJoinCore {
    top_n: u64,
    distance_type: PbDistanceType,

    input: PlanRef,
    input_vector_col_idx: usize,
    lookup: PlanRef,
    lookup_vector: ExprImpl,

    /// The indices of lookup that will be included in the output.
    /// The index of distance column is `lookup_output_indices.len()`
    lookup_output_indices: Vec<usize>,
    include_distance: bool,
}

impl VectorSearchLookupJoinCore {
    pub(crate) fn clone_with_input(&self, input: PlanRef, lookup: PlanRef) -> Self {
        Self {
            top_n: self.top_n,
            distance_type: self.distance_type,
            input,
            input_vector_col_idx: self.input_vector_col_idx,
            lookup,
            lookup_vector: self.lookup_vector.clone(),
            lookup_output_indices: self.lookup_output_indices.clone(),
            include_distance: self.include_distance,
        }
    }

    fn struct_type(&self) -> StructType {
        StructType::row_expr_type(
            self.lookup_output_indices
                .iter()
                .map(|i| {
                    let field = &self.lookup.schema().fields[*i];
                    field.data_type.clone()
                })
                .chain(self.include_distance.then_some(VECTOR_DISTANCE_TYPE)),
        )
    }
}

impl GenericPlanNode for VectorSearchLookupJoinCore {
    fn functional_dependency(&self) -> FunctionalDependencySet {
        // TODO: include dependency of array_agg column
        FunctionalDependencySet::new(self.input.schema().len() + 1)
    }

    fn schema(&self) -> Schema {
        let fields = self
            .input
            .schema()
            .fields
            .iter()
            .cloned()
            .chain([Field::new(
                "array",
                DataType::Struct(self.struct_type()).list(),
            )])
            .collect();

        Schema { fields }
    }

    fn stream_key(&self) -> Option<Vec<usize>> {
        self.input.stream_key().map(|key| key.to_vec())
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.input.ctx()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalVectorSearchLookupJoin {
    pub base: PlanBase<Logical>,
    core: VectorSearchLookupJoinCore,
}

impl LogicalVectorSearchLookupJoin {
    pub(crate) fn new(
        top_n: u64,
        distance_type: PbDistanceType,
        input: PlanRef,
        input_vector_col_idx: usize,
        lookup: PlanRef,
        lookup_vector: ExprImpl,
        lookup_output_indices: Vec<usize>,
        include_distance: bool,
    ) -> Self {
        let core = VectorSearchLookupJoinCore {
            top_n,
            distance_type,
            input,
            input_vector_col_idx,
            lookup,
            lookup_vector,
            lookup_output_indices,
            include_distance,
        };
        Self::with_core(core)
    }

    fn with_core(core: VectorSearchLookupJoinCore) -> Self {
        let base = PlanBase::new_logical_with_core(&core);
        Self { base, core }
    }
}

impl_plan_tree_node_for_binary! { Logical, LogicalVectorSearchLookupJoin }

impl PlanTreeNodeBinary<Logical> for LogicalVectorSearchLookupJoin {
    fn left(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn right(&self) -> PlanRef {
        self.core.lookup.clone()
    }

    fn clone_with_left_right(&self, left: PlanRef, right: PlanRef) -> Self {
        let core = self.core.clone_with_input(left, right);
        Self::with_core(core)
    }
}

impl Distill for LogicalVectorSearchLookupJoin {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let verbose = self.base.ctx().is_explain_verbose();
        let mut vec = Vec::with_capacity(if verbose { 4 } else { 6 });
        vec.push(("distance_type", Pretty::debug(&self.core.distance_type)));
        vec.push(("top_n", Pretty::debug(&self.core.top_n)));
        vec.push((
            "input_vector",
            Pretty::debug(&self.core.input.schema()[self.core.input_vector_col_idx]),
        ));

        vec.push((
            "lookup_vector",
            Pretty::debug(&ExprDisplay {
                expr: &self.core.lookup_vector,
                input_schema: self.core.lookup.schema(),
            }),
        ));

        if verbose {
            vec.push((
                "lookup_output_columns",
                Pretty::Array(
                    self.core
                        .lookup_output_indices
                        .iter()
                        .map(|input_idx| {
                            Pretty::debug(&self.core.lookup.schema().fields()[*input_idx])
                        })
                        .collect(),
                ),
            ));
            vec.push((
                "include_distance",
                Pretty::debug(&self.core.include_distance),
            ));
        }

        childless_record("LogicalVectorSearchLookupJoin", vec)
    }
}

impl ColPrunable for LogicalVectorSearchLookupJoin {
    fn prune_col(&self, required_cols: &[usize], ctx: &mut ColumnPruningContext) -> PlanRef {
        let (project_exprs, mut required_cols) =
            ensure_sorted_required_cols(required_cols, self.base.schema());
        assert!(required_cols.is_sorted());
        if let Some(last_col) = required_cols.last()
            && *last_col == self.core.input.schema().len()
        {
            // pop the array_agg column, since we only prune base input
            required_cols.pop();
            let output_vector = required_cols.contains(&self.core.input_vector_col_idx);
            if !output_vector {
                // include vector column in the input
                required_cols.push(self.core.input_vector_col_idx);
            }

            let new_input = self.core.input.prune_col(&required_cols, ctx);
            let mut core = self
                .core
                .clone_with_input(new_input, self.core.lookup.clone());

            core.input_vector_col_idx = ColIndexMapping::with_remaining_columns(
                &required_cols,
                self.core.input.schema().len(),
            )
            .map(self.core.input_vector_col_idx);
            let vector_search = Self::with_core(core).into();
            let input = if output_vector {
                vector_search
            } else {
                // prune the vector column in the end of input, and include the array_agg column
                LogicalProject::with_out_col_idx(
                    vector_search,
                    (0..required_cols.len() - 1).chain([required_cols.len()]),
                )
                .into()
            };

            LogicalProject::create(input, project_exprs)
        } else {
            // the array_agg column is pruned, no need to lookup
            let input = self.core.input.prune_col(&required_cols, ctx);
            LogicalProject::create(input, project_exprs)
        }
    }
}

impl ExprRewritable<Logical> for LogicalVectorSearchLookupJoin {}

impl ExprVisitable for LogicalVectorSearchLookupJoin {}

impl PredicatePushdown for LogicalVectorSearchLookupJoin {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        // TODO: push down to input when possible
        let input = self
            .core
            .input
            .predicate_pushdown(Condition::true_cond(), ctx);
        let lookup = self
            .core
            .lookup
            .predicate_pushdown(Condition::true_cond(), ctx);
        let core = self.core.clone_with_input(input, lookup);
        LogicalFilter::create(Self::with_core(core).into(), predicate)
    }
}

impl ToStream for LogicalVectorSearchLookupJoin {
    fn logical_rewrite_for_stream(
        &self,
        ctx: &mut RewriteStreamContext,
    ) -> crate::error::Result<(PlanRef, ColIndexMapping)> {
        if !self
            .core
            .input
            .logical_rewrite_for_stream(ctx)?
            .1
            .is_identity()
        {
            // TODO: support it
            bail!(
                "LogicalVectorSearchLookupJoin does not support input that can possibly be rewritten"
            )
        }
        Ok((
            self.clone().into(),
            ColIndexMapping::identity(self.base.schema().len()),
        ))
    }

    fn to_stream(&self, ctx: &mut ToStreamContext) -> Result<StreamPlanRef> {
        if let Some(core) = self.to_vector_index_lookup_join(|plan| plan.to_stream(ctx))? {
            // `ProcessTimeBroadcast` is intentionally excluded: the vector-index lookup executor
            // does not consume a broadcast change stream or maintain replicated lookup state.
            if !matches!(&core.as_of, Some(AsOf::ProcessTime)) {
                bail!("streaming vector index lookup join must be proctime temporal join");
            }
            return Ok(StreamVectorIndexLookupJoin::new(core)?.into());
        }
        bail!("LogicalVectorSearchLookupJoin should use proper vector index in streaming job")
    }
}

impl LogicalVectorSearchLookupJoin {
    pub(crate) fn as_index_lookup(&self) -> Option<(&Arc<VectorIndex>, Vec<usize>, Option<AsOf>)> {
        if let Some(scan) = self.core.lookup.as_logical_scan()
            && let Some((
                index,
                _covered_table_cols_idx,
                non_covered_table_cols_idx,
                primary_table_col_in_output,
            )) = LogicalVectorSearch::resolve_vector_index_lookup(
                scan,
                &self.core.lookup_vector,
                self.core.distance_type,
                &self.core.lookup_output_indices,
            )
            && non_covered_table_cols_idx.is_empty()
        {
            let info_output_indices = primary_table_col_in_output
                .iter()
                .map(|(covered, idx_in_index_info_columns)| {
                    assert!(*covered);
                    *idx_in_index_info_columns
                })
                .collect();
            Some((index, info_output_indices, scan.as_of()))
        } else {
            None
        }
    }
}

impl LogicalVectorSearchLookupJoin {
    fn to_vector_index_lookup_join<PlanRef>(
        &self,
        gen_input: impl FnOnce(&LogicalPlanRef) -> Result<PlanRef>,
    ) -> Result<Option<VectorIndexLookupJoin<PlanRef>>> {
        if let Some((index, info_output_indices, as_of)) = self.as_index_lookup() {
            let hnsw_ef_search =
                index.resolve_hnsw_ef_search(&self.core.ctx().session_ctx().config());
            let core = VectorIndexLookupJoin {
                input: gen_input(&self.core.input)?,
                top_n: self.core.top_n,
                distance_type: self.core.distance_type,
                index_name: index.index_table.name.clone(),
                index_table_id: index.index_table.id,
                info_column_desc: index.info_column_desc(),
                info_output_indices,
                include_distance: self.core.include_distance,
                as_of,
                vector_column_idx: self.core.input_vector_col_idx,
                hnsw_ef_search,
                ctx: self.core.ctx(),
            };
            return Ok(Some(core));
        }
        Ok(None)
    }
}

impl ToBatch for LogicalVectorSearchLookupJoin {
    fn to_batch(&self) -> Result<BatchPlanRef> {
        if let Some(core) = self.to_vector_index_lookup_join(|plan| plan.to_batch())? {
            return Ok(BatchVectorSearch::with_core(core).into());
        }

        bail!("no index found for BatchVectorSearchLookupJoin")
    }
}
