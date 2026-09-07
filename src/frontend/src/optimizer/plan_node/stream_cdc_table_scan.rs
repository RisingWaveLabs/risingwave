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

use itertools::Itertools;
use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::catalog::{ColumnCatalog, Field};
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::OrderType;
use risingwave_connector::source::cdc::normalize_simple_postgres_quoted_table_name;
use risingwave_pb::stream_plan::PbStreamNode;
use risingwave_pb::stream_plan::stream_node::{PbNodeBody, PbStreamKind};

use super::stream::prelude::*;
use super::utils::{Distill, childless_record};
use super::{ExprRewritable, PlanBase, StreamNode, StreamPlanRef as PlanRef, generic};
use crate::catalog::ColumnId;
use crate::expr::{Expr, ExprImpl, ExprRewriter, ExprType, ExprVisitor, FunctionCall, InputRef};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::utils::{IndicesDisplay, TableCatalogBuilder};
use crate::optimizer::property::{Distribution, DistributionDisplay};
use crate::scheduler::SchedulerResult;
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::{Explain, TableCatalog};

/// `StreamCdcTableScan` is a virtual plan node to represent a stream cdc table scan.
/// It will be converted to cdc backfill + merge node (for upstream source)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamCdcTableScan {
    pub base: PlanBase<Stream>,
    core: generic::CdcScan,
}

impl StreamCdcTableScan {
    pub fn new(core: generic::CdcScan) -> Self {
        let distribution = Distribution::SomeShard;
        let base = PlanBase::new_stream_with_core(
            &core,
            distribution,
            StreamKind::Retract,
            false,
            core.watermark_columns(),
            core.columns_monotonicity(),
        );
        Self { base, core }
    }

    pub fn table_name(&self) -> &str {
        &self.core.table_name
    }

    pub fn core(&self) -> &generic::CdcScan {
        &self.core
    }

    /// Build catalog for cdc backfill state.
    ///
    /// For non-parallelized cdc backfill:
    /// Right now we only persist whether the backfill is finished and the corresponding cdc offset
    /// schema: | `split_id` | `pk...` | `backfill_finished` | `row_count` | `cdc_offset` |
    ///
    /// For parallelized cdc backfill:
    /// schema: | `split_id` | `backfill_finished` | `row_count` | `cdc_offset_low` | `cdc_offset_high` | `pk...` |
    pub fn build_backfill_state_catalog(
        &self,
        state: &mut BuildFragmentGraphState,
        is_parallelized_backfill: bool,
    ) -> TableCatalog {
        if is_parallelized_backfill {
            let mut catalog_builder = TableCatalogBuilder::default();
            let upstream_schema = &self.core.get_table_columns();
            // Use `split_id` as primary key in state table.
            catalog_builder.add_column(&Field::with_name(DataType::Int64, "split_id"));
            catalog_builder.add_order_column(0, OrderType::ascending());

            catalog_builder.add_column(&Field::with_name(DataType::Boolean, "backfill_finished"));
            // `row_count` column, the number of rows read from snapshot
            catalog_builder.add_column(&Field::with_name(DataType::Int64, "row_count"));
            catalog_builder.add_column(&Field::with_name(DataType::Jsonb, "cdc_offset_low"));
            catalog_builder.add_column(&Field::with_name(DataType::Jsonb, "cdc_offset_high"));

            // pk columns
            for col_order in self.core.primary_key() {
                let col = &upstream_schema[col_order.column_index];
                catalog_builder.add_column(&Field::from(col));
            }

            catalog_builder
                .build(vec![], 1)
                .with_id(state.gen_table_id_wrapped())
        } else {
            let mut catalog_builder = TableCatalogBuilder::default();
            let upstream_schema = &self.core.get_table_columns();

            // Use `split_id` as primary key in state table.
            // Currently we only support single split for cdc backfill.
            catalog_builder.add_column(&Field::with_name(DataType::Varchar, "split_id"));
            catalog_builder.add_order_column(0, OrderType::ascending());

            // pk columns
            for col_order in self.core.primary_key() {
                let col = &upstream_schema[col_order.column_index];
                catalog_builder.add_column(&Field::from(col));
            }

            catalog_builder.add_column(&Field::with_name(DataType::Boolean, "backfill_finished"));

            // `row_count` column, the number of rows read from snapshot
            catalog_builder.add_column(&Field::with_name(DataType::Int64, "row_count"));

            // The offset is only for observability, not for recovery right now
            catalog_builder.add_column(&Field::with_name(DataType::Jsonb, "cdc_offset"));

            // leave dist key empty, since the cdc backfill executor is singleton
            catalog_builder
                .build(vec![], 1)
                .with_id(state.gen_table_id_wrapped())
        }
    }
}

impl_plan_tree_node_for_leaf! { Stream, StreamCdcTableScan }

impl Distill for StreamCdcTableScan {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let verbose = self.base.ctx().is_explain_verbose();
        let mut vec = Vec::with_capacity(4);
        vec.push(("table", Pretty::from(self.core.table_name.clone())));
        vec.push(("columns", self.core.columns_pretty(verbose)));

        if verbose {
            let pk = IndicesDisplay {
                indices: self.stream_key().unwrap_or_default(),
                schema: self.base.schema(),
            };
            vec.push(("pk", pk.distill()));
            let dist = Pretty::display(&DistributionDisplay {
                distribution: self.distribution(),
                input_schema: self.base.schema(),
            });
            vec.push(("dist", dist));
        }

        childless_record("StreamCdcTableScan", vec)
    }
}

impl StreamNode for StreamCdcTableScan {
    fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> PbNodeBody {
        unreachable!(
            "stream scan cannot be converted into a prost body -- call `adhoc_to_stream_prost` instead."
        )
    }
}

impl StreamCdcTableScan {
    /// plan: merge -> filter -> exchange(simple) -> `stream_scan`
    pub fn adhoc_to_stream_prost(
        &self,
        state: &mut BuildFragmentGraphState,
    ) -> SchedulerResult<PbStreamNode> {
        use risingwave_pb::stream_plan::*;

        let stream_key = self
            .stream_key()
            .unwrap_or_else(|| {
                panic!(
                    "should always have a stream key in the stream plan but not, sub plan: {}",
                    PlanRef::from(self.clone()).explain_to_string()
                )
            })
            .iter()
            .map(|x| *x as u32)
            .collect_vec();

        // The schema of the shared cdc source upstream is different from snapshot.
        let cdc_source_schema = ColumnCatalog::debezium_cdc_source_cols()
            .into_iter()
            .map(|c| Field::from(c.column_desc).to_prost())
            .collect_vec();

        let catalog = self
            .build_backfill_state_catalog(state, self.core.options.is_parallelized_backfill())
            .to_internal_table_prost();

        // We need to pass the id of upstream source job here
        let upstream_source_id = self.core.cdc_table_desc.source_id;

        // filter upstream source chunk by the value of `_rw_table_name` column
        let filter_expr =
            Self::build_cdc_filter_expr(self.core.cdc_table_desc.external_table_name.as_str());

        let filter_operator_id = self.core.ctx.next_plan_node_id();
        // The filter node receive chunks in `(payload, _rw_offset, _rw_table_name)` schema
        let filter_stream_node = StreamNode {
            operator_id: filter_operator_id.to_stream_node_operator_id(),
            input: vec![
                // The merge node body will be filled by the `ActorBuilder` on the meta service.
                PbStreamNode {
                    node_body: Some(PbNodeBody::Merge(Default::default())),
                    identity: "Upstream".into(),
                    fields: cdc_source_schema.clone(),
                    stream_key: vec![], // not used
                    ..Default::default()
                },
            ],
            stream_key: vec![], // not used
            stream_kind: PbStreamKind::AppendOnly as _,
            identity: "StreamCdcFilter".to_owned(),
            fields: cdc_source_schema.clone(),
            node_body: Some(PbNodeBody::CdcFilter(Box::new(CdcFilterNode {
                search_condition: Some(filter_expr.to_expr_proto()),
                upstream_source_id,
            }))),
        };

        let exchange_operator_id = self.core.ctx.next_plan_node_id();
        let strategy = if self.core.options.is_parallelized_backfill() {
            DispatchStrategy {
                r#type: DispatcherType::Broadcast as _,
                dist_key_indices: vec![],
                output_mapping: PbDispatchOutputMapping::identical(cdc_source_schema.len()).into(),
            }
        } else {
            DispatchStrategy {
                r#type: DispatcherType::Simple as _,
                dist_key_indices: vec![], // simple exchange doesn't need dist key
                output_mapping: PbDispatchOutputMapping::identical(cdc_source_schema.len()).into(),
            }
        };
        // Add a simple exchange node between filter and stream scan
        let exchange_stream_node = StreamNode {
            operator_id: exchange_operator_id.to_stream_node_operator_id(),
            input: vec![filter_stream_node],
            stream_key: vec![], // not used
            stream_kind: PbStreamKind::AppendOnly as _,
            identity: "Exchange".to_owned(),
            fields: cdc_source_schema,
            node_body: Some(PbNodeBody::Exchange(Box::new(ExchangeNode {
                strategy: Some(strategy),
            }))),
        };

        // The required columns from the external table
        let upstream_column_ids = self
            .core
            .output_and_pk_column_ids()
            .iter()
            .map(ColumnId::get_id)
            .collect_vec();

        let output_indices = self
            .core
            .output_column_ids()
            .iter()
            .map(|i| {
                upstream_column_ids
                    .iter()
                    .position(|&x| x == i.get_id())
                    .unwrap() as u32
            })
            .collect_vec();

        tracing::debug!(
            output_column_ids=?self.core.output_column_ids(),
            ?upstream_column_ids,
            ?output_indices,
            "stream cdc table scan output indices"
        );

        let options = self.core.options.to_proto();
        let stream_scan_body = PbNodeBody::StreamCdcScan(Box::new(StreamCdcScanNode {
            table_id: upstream_source_id.as_cdc_table_id(),
            upstream_column_ids,
            output_indices,
            // The table desc used by backfill executor
            state_table: Some(catalog),
            cdc_table_desc: Some(self.core.cdc_table_desc.to_protobuf()),
            rate_limit: self.base.ctx().overwrite_options().backfill_rate_limit,
            disable_backfill: options.disable_backfill,
            options: Some(options),
        }));

        // plan: merge -> filter -> exchange(simple) -> stream_scan
        Ok(PbStreamNode {
            fields: self.schema().to_prost(),
            input: vec![exchange_stream_node],
            node_body: Some(stream_scan_body),
            stream_key,
            operator_id: self.base.id().to_stream_node_operator_id(),
            identity: self.distill_to_string(),
            stream_kind: self.stream_kind().to_protobuf() as i32,
        })
    }

    // The filter node receive input chunks in `(payload, _rw_offset, _rw_table_name)` schema
    pub fn build_cdc_filter_expr(cdc_table_name: &str) -> ExprImpl {
        let table_name_ref: ExprImpl = InputRef::new(2, DataType::Varchar).into();
        let mut filter_expr = build_cdc_table_name_eq_expr(table_name_ref.clone(), cdc_table_name);

        if let Some(compat_table_name) = normalize_simple_postgres_quoted_table_name(cdc_table_name)
        {
            // The left branch keeps matching the catalog/user-facing table name stored in
            // `ExternalTableDesc`, e.g. `public."TableName"`. The right branch matches the
            // Debezium runtime routing key carried in `_rw_table_name`, e.g. `public.TableName`.
            // See `DbzChangeEventConsumer` for deriving the runtime table name from Debezium
            // topics, and `DebeziumCdcMeta::extract_table_name` for how `_rw_table_name` is
            // populated from that runtime name.
            filter_expr = FunctionCall::new(
                ExprType::Or,
                vec![
                    filter_expr,
                    build_cdc_table_name_eq_expr(table_name_ref, &compat_table_name),
                ],
            )
            .unwrap()
            .into();
        }

        filter_expr
    }
}

fn build_cdc_table_name_eq_expr(table_name_ref: ExprImpl, cdc_table_name: &str) -> ExprImpl {
    FunctionCall::new(
        ExprType::Equal,
        vec![
            table_name_ref,
            ExprImpl::literal_varchar(cdc_table_name.into()),
        ],
    )
    .unwrap()
    .into()
}

impl ExprRewritable<Stream> for StreamCdcTableScan {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let core = self.core.clone();
        core.rewrite_exprs(r);
        Self::new(core).into()
    }
}

impl ExprVisitable for StreamCdcTableScan {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.core.visit_exprs(v);
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use risingwave_common::row::OwnedRow;
    use risingwave_common::types::{JsonbVal, ScalarImpl};

    use super::*;

    #[tokio::test]
    async fn test_cdc_filter_expr() {
        let t1_json = JsonbVal::from_str(r#"{ "before": null, "after": { "v": 111, "v2": 222.2 }, "source": { "version": "2.2.0.Alpha3", "connector": "mysql", "name": "dbserver1", "ts_ms": 1678428689000, "snapshot": "false", "db": "inventory", "sequence": null, "table": "t1", "server_id": 223344, "gtid": null, "file": "mysql-bin.000003", "pos": 774, "row": 0, "thread": 8, "query": null }, "op": "c", "ts_ms": 1678428689389, "transaction": null }"#).unwrap();
        let t2_json = JsonbVal::from_str(r#"{ "before": null, "after": { "v": 333, "v2": 666.6 }, "source": { "version": "2.2.0.Alpha3", "connector": "mysql", "name": "dbserver1", "ts_ms": 1678428689000, "snapshot": "false", "db": "inventory", "sequence": null, "table": "t2", "server_id": 223344, "gtid": null, "file": "mysql-bin.000003", "pos": 884, "row": 0, "thread": 8, "query": null }, "op": "c", "ts_ms": 1678428689389, "transaction": null }"#).unwrap();

        // NOTES: transaction metadata column expects to be filtered out before going to cdc filter
        let trx_json = JsonbVal::from_str(r#"{"data_collections": null, "event_count": null, "id": "35319:3962662584", "status": "BEGIN", "ts_ms": 1704263537068}"#).unwrap();
        let row1 = OwnedRow::new(vec![
            Some(t1_json.into()),
            Some(r#"{"file": "1.binlog", "pos": 100}"#.into()),
            Some("public.t2".into()),
        ]);
        let row2 = OwnedRow::new(vec![
            Some(t2_json.into()),
            Some(r#"{"file": "2.binlog", "pos": 100}"#.into()),
            Some("abs.t2".into()),
        ]);

        let row3 = OwnedRow::new(vec![
            Some(trx_json.into()),
            Some(r#"{"file": "3.binlog", "pos": 100}"#.into()),
            Some("public.t2".into()),
        ]);

        let filter_expr = StreamCdcTableScan::build_cdc_filter_expr("public.t2");
        assert_eq!(
            filter_expr.eval_row(&row1).await.unwrap(),
            Some(ScalarImpl::Bool(true))
        );
        assert_eq!(
            filter_expr.eval_row(&row2).await.unwrap(),
            Some(ScalarImpl::Bool(false))
        );
        assert_eq!(
            filter_expr.eval_row(&row3).await.unwrap(),
            Some(ScalarImpl::Bool(true))
        )
    }

    #[tokio::test]
    async fn test_cdc_filter_expr_postgres_quoted_table_name_compat() {
        let row_with_unquoted_schema_table =
            OwnedRow::new(vec![None, None, Some("public.QuotedNoteE2e".into())]);
        let row_with_other_table = OwnedRow::new(vec![None, None, Some("public.other".into())]);

        let filter_expr = StreamCdcTableScan::build_cdc_filter_expr(r#"public."QuotedNoteE2e""#);
        assert_eq!(
            filter_expr
                .eval_row(&row_with_unquoted_schema_table)
                .await
                .unwrap(),
            Some(ScalarImpl::Bool(true))
        );
        assert_eq!(
            filter_expr.eval_row(&row_with_other_table).await.unwrap(),
            Some(ScalarImpl::Bool(false))
        );
    }
}
