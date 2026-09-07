// Copyright 2023 RisingWave Labs
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

use std::rc::Rc;
use std::str::FromStr;

use anyhow::anyhow;
use educe::Educe;
use pretty_xmlish::Pretty;
use risingwave_common::catalog::{CdcTableDesc, ColumnDesc, Field, Schema};
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_common::util::sort_util::ColumnOrder;
use risingwave_connector::source::cdc::external::ExternalCdcTableType;
use risingwave_connector::source::cdc::{
    CDC_BACKFILL_AS_EVEN_SPLITS, CDC_BACKFILL_ENABLE_KEY, CDC_BACKFILL_MAX_PARALLELISM,
    CDC_BACKFILL_NUM_ROWS_PER_SPLIT, CDC_BACKFILL_PARALLELISM,
    CDC_BACKFILL_SNAPSHOT_BATCH_SIZE_KEY, CDC_BACKFILL_SNAPSHOT_INTERVAL_KEY,
    CDC_BACKFILL_SPLIT_COLUMN_NAME, CDC_BACKFILL_SPLIT_PK_COLUMN_INDEX, CdcScanOptions,
};

use super::GenericPlanNode;
use crate::WithOptions;
use crate::catalog::ColumnId;
use crate::error::Result;
use crate::expr::{ExprRewriter, ExprVisitor};
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::property::{FunctionalDependencySet, MonotonicityMap, WatermarkColumns};

/// [`CdcScan`] reads rows of a table from an external upstream database
#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct CdcScan {
    pub table_name: String,
    /// Include `output_col_idx` and columns required in `predicate`
    pub output_col_idx: Vec<usize>,
    /// Descriptor of the external table for CDC
    pub cdc_table_desc: Rc<CdcTableDesc>,
    #[educe(PartialEq(ignore))]
    #[educe(Hash(ignore))]
    pub ctx: OptimizerContextRef,

    pub options: CdcScanOptions,
}

pub fn build_cdc_scan_options_with_options(
    with_options: &WithOptions,
    cdc_table_type: &ExternalCdcTableType,
) -> Result<CdcScanOptions> {
    // Update this after more CDC table type is supported for backfill v2.
    let support_backfill_v2 = matches!(
        cdc_table_type,
        ExternalCdcTableType::Postgres | ExternalCdcTableType::Mock
    );

    // unspecified option will use default values
    let mut scan_options = CdcScanOptions::default();

    // disable backfill if 'snapshot=false'
    if let Some(snapshot) = with_options.get(CDC_BACKFILL_ENABLE_KEY) {
        scan_options.disable_backfill = !(bool::from_str(snapshot)
            .map_err(|_| anyhow!("Invalid value for {}", CDC_BACKFILL_ENABLE_KEY))?);
    };

    if let Some(snapshot_interval) = with_options.get(CDC_BACKFILL_SNAPSHOT_INTERVAL_KEY) {
        scan_options.snapshot_barrier_interval = u32::from_str(snapshot_interval)
            .map_err(|_| anyhow!("Invalid value for {}", CDC_BACKFILL_SNAPSHOT_INTERVAL_KEY))?;
    };

    if let Some(snapshot_batch_size) = with_options.get(CDC_BACKFILL_SNAPSHOT_BATCH_SIZE_KEY) {
        scan_options.snapshot_batch_size = u32::from_str(snapshot_batch_size)
            .map_err(|_| anyhow!("Invalid value for {}", CDC_BACKFILL_SNAPSHOT_BATCH_SIZE_KEY))?;
    };

    if support_backfill_v2 {
        if let Some(backfill_parallelism) = with_options.get(CDC_BACKFILL_PARALLELISM) {
            scan_options.backfill_parallelism = u32::from_str(backfill_parallelism)
                .map_err(|_| anyhow!("Invalid value for {}", CDC_BACKFILL_PARALLELISM))?;
            if scan_options.backfill_parallelism > CDC_BACKFILL_MAX_PARALLELISM {
                return Err(anyhow!(
                    "Invalid value for {}, should be in range [0,{}] ",
                    CDC_BACKFILL_PARALLELISM,
                    CDC_BACKFILL_MAX_PARALLELISM
                )
                .into());
            }
        }

        if let Some(backfill_num_rows_per_split) = with_options.get(CDC_BACKFILL_NUM_ROWS_PER_SPLIT)
        {
            scan_options.backfill_num_rows_per_split = u64::from_str(backfill_num_rows_per_split)
                .map_err(|_| {
                anyhow!("Invalid value for {}", CDC_BACKFILL_NUM_ROWS_PER_SPLIT)
            })?;
        }

        if let Some(backfill_as_even_splits) = with_options.get(CDC_BACKFILL_AS_EVEN_SPLITS) {
            scan_options.backfill_as_even_splits = bool::from_str(backfill_as_even_splits)
                .map_err(|_| anyhow!("Invalid value for {}", CDC_BACKFILL_AS_EVEN_SPLITS))?;
        }

        if let Some(backfill_split_pk_column_index) =
            with_options.get(CDC_BACKFILL_SPLIT_PK_COLUMN_INDEX)
        {
            scan_options.backfill_split_pk_column_index =
                u32::from_str(backfill_split_pk_column_index).map_err(|_| {
                    anyhow!("Invalid value for {}", CDC_BACKFILL_SPLIT_PK_COLUMN_INDEX)
                })?;
        }

        if let Some(backfill_split_column_name) = with_options.get(CDC_BACKFILL_SPLIT_COLUMN_NAME) {
            scan_options.backfill_split_column_name = Some(backfill_split_column_name.to_owned());
        }
    }

    Ok(scan_options)
}

impl CdcScan {
    pub fn rewrite_exprs(&self, _rewriter: &mut dyn ExprRewriter) {}

    pub fn visit_exprs(&self, _v: &mut dyn ExprVisitor) {}

    /// Get the ids of the output columns.
    pub fn output_column_ids(&self) -> Vec<ColumnId> {
        self.output_col_idx
            .iter()
            .map(|i| self.get_table_columns()[*i].column_id)
            .collect()
    }

    pub fn primary_key(&self) -> &[ColumnOrder] {
        &self.cdc_table_desc.pk
    }

    pub fn watermark_columns(&self) -> WatermarkColumns {
        WatermarkColumns::new()
    }

    pub fn columns_monotonicity(&self) -> MonotonicityMap {
        MonotonicityMap::new()
    }

    pub(crate) fn column_names_with_table_prefix(&self) -> Vec<String> {
        self.output_col_idx
            .iter()
            .map(|&i| format!("{}.{}", self.table_name, self.get_table_columns()[i].name))
            .collect()
    }

    pub(crate) fn column_names(&self) -> Vec<String> {
        self.output_col_idx
            .iter()
            .map(|&i| self.get_table_columns()[i].name.clone())
            .collect()
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

    /// Create a logical scan node for CDC backfill
    pub(crate) fn new(
        table_name: String,
        output_col_idx: Vec<usize>, // the column index in the table
        cdc_table_desc: Rc<CdcTableDesc>,
        ctx: OptimizerContextRef,
        options: CdcScanOptions,
    ) -> Self {
        Self {
            table_name,
            output_col_idx,
            cdc_table_desc,
            ctx,
            options,
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
}

// TODO: extend for cdc table
impl GenericPlanNode for CdcScan {
    fn schema(&self) -> Schema {
        let fields = self
            .output_col_idx
            .iter()
            .map(|tb_idx| {
                let col = &self.get_table_columns()[*tb_idx];
                Field::from_with_table_name_prefix(col, &self.table_name)
            })
            .collect();
        Schema { fields }
    }

    fn stream_key(&self) -> Option<Vec<usize>> {
        Some(self.cdc_table_desc.stream_key.clone())
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

impl CdcScan {
    pub fn get_table_columns(&self) -> &[ColumnDesc] {
        &self.cdc_table_desc.columns
    }

    /// Get the descs of the output columns.
    pub fn column_descs(&self) -> Vec<ColumnDesc> {
        self.output_col_idx
            .iter()
            .map(|&i| self.get_table_columns()[i].clone())
            .collect()
    }
}
