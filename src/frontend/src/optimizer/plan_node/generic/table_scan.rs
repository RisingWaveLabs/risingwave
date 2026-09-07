// Copyright 2024 RisingWave Labs
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

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use educe::Educe;
use fixedbitset::FixedBitSet;
use pretty_xmlish::Pretty;
use risingwave_common::catalog::{ColumnCatalog, ColumnDesc, Field, Schema};
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_common::util::sort_util::ColumnOrder;
use risingwave_sqlparser::ast::AsOf;

use super::GenericPlanNode;
use crate::TableCatalog;
use crate::catalog::ColumnId;
use crate::catalog::index_catalog::{TableIndex, VectorIndex};
use crate::catalog::table_catalog::TableType;
use crate::expr::{Expr, ExprImpl, ExprRewriter, ExprVisitor, FunctionCall, InputRef};
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::property::{FunctionalDependencySet, Order, WatermarkColumns};
use crate::utils::{ColIndexMappingRewriteExt, Condition};

/// [`TableScan`] returns contents of a RisingWave Table.
#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct TableScan {
    /// Include `output_col_idx` and columns required in `predicate`
    pub required_col_idx: Vec<usize>,
    pub output_col_idx: Vec<usize>,
    /// Table Catalog of the upstream table that the descriptor is derived from.
    pub table_catalog: Arc<TableCatalog>,
    // FIXME(kwannoel): Currently many places in the code reference this,
    // but now we have table catalog.
    // We should remove this and use table catalog in those call-sites instead.
    // It's introduced in https://github.com/risingwavelabs/risingwave/pull/13622.
    // We kept this field to avoid extensive refactor in that PR.
    /// Table Desc (subset of table catalog).
    /// Descriptors of all indexes on this table
    pub table_indexes: Vec<Arc<TableIndex>>,
    pub vector_indexes: Vec<Arc<VectorIndex>>,
    /// Table column indices used as the stream key of this scan operator.
    table_scan_stream_key: Option<Vec<usize>>,
    /// The pushed down predicates. It refers to column indexes of the table.
    pub predicate: Condition,
    /// syntax `FOR SYSTEM_TIME AS OF PROCTIME()` is used for temporal join.
    /// syntax `FOR SYSTEM_TIME AS OF '1986-10-26 01:21:00'` is used for iceberg.
    /// syntax `FOR SYSTEM_TIME AS OF 499162860` is used for iceberg.
    /// syntax `FOR SYSTEM_VERSION AS OF 10963874102873;` is used for iceberg.
    pub as_of: Option<AsOf>,
    #[educe(PartialEq(ignore))]
    #[educe(Hash(ignore))]
    pub ctx: OptimizerContextRef,
}

#[derive(Clone, Copy)]
enum UnmappedInputRefBehavior {
    Panic,
    RewriteWithOffset(usize),
}

/// Rewrites expressions from primary-table column indices to index-table column indices.
pub(crate) struct IndexExprRewriter<'a> {
    primary_to_secondary_mapping: &'a BTreeMap<usize, usize>,
    function_mapping: &'a HashMap<FunctionCall, usize>,
    unmapped_input_ref_behavior: UnmappedInputRefBehavior,
    covered_by_index: bool,
    used_materialized_expression: bool,
}

impl<'a> IndexExprRewriter<'a> {
    pub(crate) fn new_strict(
        primary_to_secondary_mapping: &'a BTreeMap<usize, usize>,
        function_mapping: &'a HashMap<FunctionCall, usize>,
    ) -> Self {
        Self::new(
            primary_to_secondary_mapping,
            function_mapping,
            UnmappedInputRefBehavior::Panic,
        )
    }

    pub(crate) fn new_with_unmapped_input_offset(
        primary_to_secondary_mapping: &'a BTreeMap<usize, usize>,
        function_mapping: &'a HashMap<FunctionCall, usize>,
        offset: usize,
    ) -> Self {
        Self::new(
            primary_to_secondary_mapping,
            function_mapping,
            UnmappedInputRefBehavior::RewriteWithOffset(offset),
        )
    }

    fn new(
        primary_to_secondary_mapping: &'a BTreeMap<usize, usize>,
        function_mapping: &'a HashMap<FunctionCall, usize>,
        unmapped_input_ref_behavior: UnmappedInputRefBehavior,
    ) -> Self {
        Self {
            primary_to_secondary_mapping,
            function_mapping,
            unmapped_input_ref_behavior,
            covered_by_index: true,
            used_materialized_expression: false,
        }
    }

    pub(crate) fn covered_by_index(&self) -> bool {
        self.covered_by_index
    }

    pub(crate) fn used_materialized_expression(&self) -> bool {
        self.used_materialized_expression
    }
}

impl ExprRewriter for IndexExprRewriter<'_> {
    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        let index = match self.primary_to_secondary_mapping.get(&input_ref.index()) {
            Some(index) => *index,
            None => match self.unmapped_input_ref_behavior {
                UnmappedInputRefBehavior::Panic => {
                    panic!("primary column must be covered by index")
                }
                UnmappedInputRefBehavior::RewriteWithOffset(offset) => {
                    self.covered_by_index = false;
                    input_ref.index() + offset
                }
            },
        };
        InputRef::new(index, input_ref.return_type()).into()
    }

    fn rewrite_function_call(&mut self, func_call: FunctionCall) -> ExprImpl {
        if let Some(index) = self.function_mapping.get(&func_call) {
            self.used_materialized_expression = true;
            return InputRef::new(*index, func_call.return_type()).into();
        }

        let (func_type, inputs, ret) = func_call.decompose();
        let inputs = inputs
            .into_iter()
            .map(|expr| self.rewrite_expr(expr))
            .collect();
        FunctionCall::new_unchecked(func_type, inputs, ret).into()
    }
}

impl TableScan {
    pub(crate) fn rewrite_exprs(&mut self, r: &mut dyn ExprRewriter) {
        self.predicate = self.predicate.clone().rewrite_expr(r);
    }

    pub(crate) fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.predicate.visit_expr(v);
    }

    pub fn table_name(&self) -> &str {
        self.table_catalog.name()
    }

    /// The mapped distribution key of the scan operator.
    ///
    /// The column indices in it is the position in the `output_col_idx`, instead of the position
    /// in all the columns of the table (which is the table's distribution key).
    ///
    /// Return `None` if the table's distribution key are not all in the `output_col_idx`.
    pub fn distribution_key(&self) -> Option<Vec<usize>> {
        let tb_idx_to_op_idx = self
            .output_col_idx
            .iter()
            .enumerate()
            .map(|(op_idx, tb_idx)| (*tb_idx, op_idx))
            .collect::<HashMap<_, _>>();
        self.table_catalog
            .distribution_key
            .iter()
            .map(|&tb_idx| tb_idx_to_op_idx.get(&tb_idx).cloned())
            .collect()
    }

    /// Get the ids of the output columns.
    pub fn output_column_ids(&self) -> Vec<ColumnId> {
        self.output_col_idx
            .iter()
            .map(|i| self.get_table_columns()[*i].column_id)
            .collect()
    }

    pub fn primary_key(&self) -> &[ColumnOrder] {
        &self.table_catalog.pk
    }

    pub(crate) fn table_scan_stream_key(&self) -> Option<&[usize]> {
        self.table_scan_stream_key.as_deref()
    }

    pub(crate) fn extend_table_scan_stream_key_with_primary_key(&mut self) {
        let primary_key_indices = self
            .primary_key()
            .iter()
            .map(|c| c.column_index)
            .collect::<Vec<_>>();
        let Some(table_scan_stream_key) = &mut self.table_scan_stream_key else {
            #[cfg(debug_assertions)]
            {
                panic!(
                    "table scan stream key should exist before extending with primary key: table: {}, output_col_idx: {:?}, pk: {:?}",
                    self.table_name(),
                    self.output_col_idx,
                    primary_key_indices
                );
            }
            #[cfg(not(debug_assertions))]
            {
                tracing::warn!(
                    table = self.table_name(),
                    output_col_idx = ?self.output_col_idx,
                    pk = ?primary_key_indices,
                    "table scan stream key should exist before extending with primary key"
                );
                return;
            }
        };

        for primary_key in primary_key_indices {
            if !table_scan_stream_key.contains(&primary_key) {
                table_scan_stream_key.push(primary_key);
            }
        }
    }

    pub fn watermark_columns(&self) -> WatermarkColumns {
        // TODO(rc): For now, we still use `FixedBitSet` for watermark columns in `TableDesc`.
        // So when we scan from a table, we have to conservatively assign each watermark column
        // a separate watermark group. We should record the watermark group information in
        // `TableDesc` later.
        let mut watermark_columns = WatermarkColumns::new();
        for idx in self.table_catalog.watermark_columns.ones() {
            watermark_columns.insert(idx, self.ctx.next_watermark_group_id());
        }
        watermark_columns.map_clone(&self.i2o_col_mapping())
    }

    pub(crate) fn column_names_with_table_prefix(&self) -> Vec<String> {
        self.output_col_idx
            .iter()
            .map(|&i| format!("{}.{}", self.table_name(), self.get_table_columns()[i].name))
            .collect()
    }

    pub(crate) fn column_names(&self) -> Vec<String> {
        self.output_col_idx
            .iter()
            .map(|&i| self.get_table_columns()[i].name.clone())
            .collect()
    }

    pub(crate) fn out_fields(&self) -> FixedBitSet {
        let out_fields_vec = self.output_col_idx.clone();
        FixedBitSet::from_iter(out_fields_vec)
    }

    pub(crate) fn order_names(&self) -> Vec<String> {
        self.table_catalog
            .order_column_indices()
            .map(|i| self.get_table_columns()[i].name.clone())
            .collect()
    }

    pub(crate) fn order_names_with_table_prefix(&self) -> Vec<String> {
        self.table_catalog
            .order_column_indices()
            .map(|i| format!("{}.{}", self.table_name(), self.get_table_columns()[i].name))
            .collect()
    }

    /// Return indices of fields the output is ordered by and
    /// corresponding direction
    pub fn get_out_column_index_order(&self) -> Order {
        let id_to_tb_idx = self.table_catalog.get_id_to_op_idx_mapping();
        let order = Order::new(
            self.table_catalog
                .pk
                .iter()
                .map(|order| {
                    let idx = id_to_tb_idx
                        .get(&self.table_catalog.columns[order.column_index].column_id)
                        .unwrap();
                    ColumnOrder::new(*idx, order.order_type)
                })
                .collect(),
        );
        self.i2o_col_mapping().rewrite_provided_order(&order)
    }

    /// get the Mapping of columnIndex from internal column index to output column index
    pub fn i2o_col_mapping(&self) -> ColIndexMapping {
        ColIndexMapping::with_remaining_columns(
            &self.output_col_idx,
            self.get_table_columns().len(),
        )
    }

    /// Get the ids of the output columns and primary key columns.
    pub fn output_and_pk_column_ids(&self) -> Vec<ColumnId> {
        let mut ids = self.output_column_ids();
        for column_order in self.primary_key() {
            let id = self.get_table_columns()[column_order.column_index].column_id;
            if !ids.contains(&id) {
                ids.push(id);
            }
        }
        ids
    }

    /// Prerequisite: the caller should guarantee that `primary_to_secondary_mapping` must cover the
    /// scan.
    pub fn to_index_scan(
        &self,
        index_table_catalog: Arc<TableCatalog>,
        primary_to_secondary_mapping: &BTreeMap<usize, usize>,
        function_mapping: &HashMap<FunctionCall, usize>,
    ) -> Self {
        let new_output_col_idx = self
            .output_col_idx
            .iter()
            .map(|col_idx| *primary_to_secondary_mapping.get(col_idx).unwrap())
            .collect();

        let mut rewriter =
            IndexExprRewriter::new_strict(primary_to_secondary_mapping, function_mapping);

        let new_predicate = self.predicate.clone().rewrite_expr(&mut rewriter);

        Self::new(
            new_output_col_idx,
            index_table_catalog,
            vec![],
            vec![],
            self.ctx.clone(),
            new_predicate,
            self.as_of.clone(),
        )
    }

    /// Create a `LogicalScan` node. Used internally by optimizer.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        output_col_idx: Vec<usize>, // the column index in the table
        table_catalog: Arc<TableCatalog>,
        table_indexes: Vec<Arc<TableIndex>>,
        vector_indexes: Vec<Arc<VectorIndex>>,
        ctx: OptimizerContextRef,
        predicate: Condition, // refers to column indexes of the table
        as_of: Option<AsOf>,
    ) -> Self {
        let table_scan_stream_key = (!matches!(table_catalog.table_type, TableType::Internal))
            .then(|| table_catalog.stream_key());
        Self::new_inner(
            output_col_idx,
            table_catalog,
            table_indexes,
            vector_indexes,
            ctx,
            predicate,
            as_of,
            table_scan_stream_key,
        )
    }

    pub(crate) fn new_inner(
        output_col_idx: Vec<usize>, // the column index in the table
        table_catalog: Arc<TableCatalog>,
        table_indexes: Vec<Arc<TableIndex>>,
        vector_indexes: Vec<Arc<VectorIndex>>,
        ctx: OptimizerContextRef,
        predicate: Condition, // refers to column indexes of the table
        as_of: Option<AsOf>,
        table_scan_stream_key: Option<Vec<usize>>,
    ) -> Self {
        // here we have 3 concepts
        // 1. column_id: ColumnId, stored in catalog and a ID to access data from storage.
        // 2. table_idx: usize, column index in the TableDesc or tableCatalog.
        // 3. operator_idx: usize, column index in the ScanOperator's schema.
        // In a query we get the same version of catalog, so the mapping from column_id and
        // table_idx will not change. And the `required_col_idx` is the `table_idx` of the
        // required columns, i.e., the mapping from operator_idx to table_idx.

        let mut required_col_idx = output_col_idx.clone();
        let predicate_col_idx = predicate.collect_input_refs(table_catalog.columns().len());
        predicate_col_idx.ones().for_each(|idx| {
            if !required_col_idx.contains(&idx) {
                required_col_idx.push(idx);
            }
        });

        Self {
            required_col_idx,
            output_col_idx,
            table_catalog,
            table_indexes,
            vector_indexes,
            table_scan_stream_key,
            predicate,
            as_of,
            ctx,
        }
    }

    pub(crate) fn columns_pretty<'a>(&self, verbose: bool) -> Pretty<'a> {
        Pretty::Array(
            match verbose {
                true => self.column_names_with_table_prefix(),
                false => self.column_names(),
            }
            .into_iter()
            .map(Pretty::from)
            .collect(),
        )
    }

    pub(crate) fn fields_pretty_schema(&self) -> Schema {
        let fields = self
            .table_catalog
            .columns
            .iter()
            .map(|col| Field::from_with_table_name_prefix(&col.column_desc, self.table_name()))
            .collect();
        Schema { fields }
    }

    // Check if the scan is cross-database
    pub(crate) fn cross_database(&self) -> bool {
        self.table_catalog.database_id != self.ctx().session_ctx().database_id()
    }
}

impl GenericPlanNode for TableScan {
    fn schema(&self) -> Schema {
        let fields = self
            .output_col_idx
            .iter()
            .map(|tb_idx| {
                let col = &self.get_table_columns()[*tb_idx];
                Field::from_with_table_name_prefix(&col.column_desc, self.table_name())
            })
            .collect();
        Schema { fields }
    }

    fn stream_key(&self) -> Option<Vec<usize>> {
        let stream_key = self.table_scan_stream_key.as_deref()?;
        let id_to_op_idx =
            Self::get_id_to_op_idx_mapping(&self.output_col_idx, &self.table_catalog);
        stream_key
            .iter()
            .map(|&c| {
                id_to_op_idx
                    .get(&self.table_catalog.columns[c].column_id)
                    .copied()
            })
            .collect()
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.ctx.clone()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        let pk_indices = self.stream_key();
        let col_num = self.output_col_idx.len();
        match &pk_indices {
            Some(pk_indices) => FunctionalDependencySet::with_key(col_num, pk_indices),
            None => FunctionalDependencySet::new(col_num),
        }
    }
}

impl TableScan {
    pub fn get_table_columns(&self) -> &[ColumnCatalog] {
        &self.table_catalog.columns
    }

    pub fn append_only(&self) -> bool {
        self.table_catalog.append_only
    }

    /// Get the descs of the output columns.
    pub fn column_descs(&self) -> Vec<ColumnDesc> {
        self.output_col_idx
            .iter()
            .map(|&i| self.get_table_columns()[i].column_desc.clone())
            .collect()
    }

    /// Helper function to create a mapping from `column_id` to `operator_idx`
    pub fn get_id_to_op_idx_mapping(
        output_col_idx: &[usize],
        table_catalog: &TableCatalog,
    ) -> HashMap<ColumnId, usize> {
        ColumnDesc::get_id_to_op_idx_mapping(&table_catalog.columns, Some(output_col_idx))
    }
}
