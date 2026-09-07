// Copyright 2022 RisingWave Labs
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

use std::assert_matches;
use std::num::NonZeroU32;

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::catalog::{
    ColumnCatalog, ConflictBehavior, CreateType, Engine, StreamJobStatus, TableId,
};
use risingwave_common::hash::VnodeCount;
use risingwave_common::id::FragmentId;
use risingwave_common::types::DataType;
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
use risingwave_pb::catalog::PbWebhookSourceInfo;
use risingwave_pb::stream_plan::stream_node::PbNodeBody;

use super::derive::derive_columns;
use super::stream::prelude::*;
use super::utils::{Distill, TableCatalogBuilder, childless_record};
use super::{
    ExprRewritable, PlanTreeNodeUnary, StreamNode, StreamPlanRef as PlanRef, reorganize_elements_id,
};
use crate::catalog::table_catalog::{TableCatalog, TableType, TableVersion};
use crate::catalog::{DatabaseId, SchemaId};
use crate::error::Result;
use crate::optimizer::StreamOptimizedLogicalPlanRoot;
use crate::optimizer::plan_node::derive::derive_pk;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::utils::plan_can_use_background_ddl;
use crate::optimizer::plan_node::{PlanBase, PlanNodeMeta};
use crate::optimizer::property::{Cardinality, Distribution, Order, RequiredDist};
use crate::optimizer::variant_key::variant_key_error;
use crate::stream_fragmenter::BuildFragmentGraphState;

/// Materializes a stream.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamMaterialize {
    pub base: PlanBase<Stream>,
    /// Child of Materialize plan
    input: PlanRef,
    table: TableCatalog,
    /// For refreshable tables, staging table for collecting new data during refresh
    staging_table: Option<TableCatalog>,
    /// For refreshable tables, progress table for tracking refresh progress
    refresh_progress_table: Option<TableCatalog>,
}

impl StreamMaterialize {
    pub fn new(input: PlanRef, table: TableCatalog) -> Result<Self> {
        Self::new_with_staging_and_progress(input, table, None, None)
    }

    pub fn new_with_staging_and_progress(
        input: PlanRef,
        table: TableCatalog,
        staging_table: Option<TableCatalog>,
        refresh_progress_table: Option<TableCatalog>,
    ) -> Result<Self> {
        let kind = match table.conflict_behavior() {
            ConflictBehavior::NoCheck => {
                reject_upsert_input!(input, "Materialize without conflict handling")
            }

            // When conflict handling is enabled, upsert stream can be converted to retract stream.
            ConflictBehavior::Overwrite
            | ConflictBehavior::IgnoreConflict
            | ConflictBehavior::DoUpdateIfNotNull => match input.stream_kind() {
                StreamKind::AppendOnly => StreamKind::AppendOnly,
                StreamKind::Retract | StreamKind::Upsert => StreamKind::Retract,
            },
        };
        let base = PlanBase::new_stream(
            input.ctx(),
            input.schema().clone(),
            Some(table.stream_key()),
            input.functional_dependency().clone(),
            input.distribution().clone(),
            kind,
            input.emit_on_window_close(),
            input.watermark_columns().clone(),
            input.columns_monotonicity().clone(),
        );

        Ok(Self {
            base,
            input,
            table,
            staging_table,
            refresh_progress_table,
        })
    }

    /// Create a materialize node, for `MATERIALIZED VIEW` and `INDEX`.
    ///
    /// When creating index, `TableType` should be `Index`. Then, materialize will distribute keys
    /// using `user_distributed_by`.
    pub fn create(
        StreamOptimizedLogicalPlanRoot {
            plan: input,
            required_dist: user_distributed_by,
            required_order: user_order_by,
            out_fields: user_cols,
            out_names,
            ..
        }: StreamOptimizedLogicalPlanRoot,
        name: String,
        database_id: DatabaseId,
        schema_id: SchemaId,
        definition: String,
        table_type: TableType,
        cardinality: Cardinality,
        retention_seconds: Option<NonZeroU32>,
    ) -> Result<Self> {
        let input = Self::rewrite_input(input, user_distributed_by.clone(), table_type)?;
        // the hidden column name might refer some expr id
        let input = reorganize_elements_id(input);
        let columns = derive_columns(input.schema(), out_names, &user_cols)?;

        let create_type = if matches!(table_type, TableType::MaterializedView)
            && input.ctx().session_ctx().config().background_ddl()
            && plan_can_use_background_ddl(&input)
        {
            CreateType::Background
        } else {
            CreateType::Foreground
        };

        // For upsert stream, use `Overwrite` conflict behavior to convert into retract stream.
        let conflict_behavior = match input.stream_kind() {
            StreamKind::Retract | StreamKind::AppendOnly => ConflictBehavior::NoCheck,
            StreamKind::Upsert => ConflictBehavior::Overwrite,
        };

        let table = Self::derive_table_catalog(
            input.clone(),
            name,
            database_id,
            schema_id,
            user_distributed_by,
            user_order_by,
            columns,
            definition,
            conflict_behavior,
            vec![],
            None,
            vec![],
            None,
            table_type,
            None,
            cardinality,
            retention_seconds,
            create_type,
            None,
            Engine::Hummock,
            false,
        )?;

        Self::new(input, table)
    }

    /// Create a materialize node, for `TABLE`.
    ///
    /// Different from `create`, the `columns` are passed in directly, instead of being derived from
    /// the input. So the column IDs are preserved from the SQL columns binding step and will be
    /// consistent with the source node and DML node.
    #[expect(clippy::too_many_arguments)]
    pub fn create_for_table(
        input: PlanRef,
        name: String,
        database_id: DatabaseId,
        schema_id: SchemaId,
        user_distributed_by: RequiredDist,
        user_order_by: Order,
        columns: Vec<ColumnCatalog>,
        definition: String,
        conflict_behavior: ConflictBehavior,
        version_column_indices: Vec<usize>,
        pk_column_indices: Vec<usize>,
        ttl_watermark_indices: Vec<usize>,
        row_id_index: Option<usize>,
        version: TableVersion,
        retention_seconds: Option<NonZeroU32>,
        webhook_info: Option<PbWebhookSourceInfo>,
        engine: Engine,
        refreshable: bool,
    ) -> Result<Self> {
        let input = Self::rewrite_input(input, user_distributed_by.clone(), TableType::Table)?;

        let table = Self::derive_table_catalog(
            input.clone(),
            name.clone(),
            database_id,
            schema_id,
            user_distributed_by,
            user_order_by,
            columns,
            definition,
            conflict_behavior,
            version_column_indices,
            Some(pk_column_indices),
            ttl_watermark_indices,
            row_id_index,
            TableType::Table,
            Some(version),
            Cardinality::unknown(), // unknown cardinality for tables
            retention_seconds,
            CreateType::Foreground,
            webhook_info,
            engine,
            refreshable,
        )?;

        // For refreshable tables, create staging table and progress table
        let (staging_table, refresh_progress_table) = if refreshable {
            let staging = Some(Self::derive_staging_table_catalog(table.clone()));
            let progress = Some(Self::derive_refresh_progress_table_catalog(table.clone()));
            (staging, progress)
        } else {
            (None, None)
        };

        tracing::info!(
            table_name = %name,
            refreshable = %refreshable,
            has_staging_table = %staging_table.is_some(),
            has_progress_table = %refresh_progress_table.is_some(),
            "Creating StreamMaterialize with staging and progress table info"
        );

        Self::new_with_staging_and_progress(input, table, staging_table, refresh_progress_table)
    }

    /// Rewrite the input to satisfy the required distribution if necessary, according to the type.
    fn rewrite_input(
        input: PlanRef,
        user_distributed_by: RequiredDist,
        table_type: TableType,
    ) -> Result<PlanRef> {
        let required_dist = match input.distribution() {
            Distribution::Single => RequiredDist::single(),
            _ => match table_type {
                TableType::Table => {
                    assert_matches!(
                        user_distributed_by,
                        RequiredDist::ShardByKey(_) | RequiredDist::ShardByExactKey(_)
                    );
                    user_distributed_by
                }
                TableType::MaterializedView => {
                    assert_matches!(user_distributed_by, RequiredDist::Any);
                    // ensure the same pk will not shuffle to different node
                    let required_dist =
                        RequiredDist::shard_by_key(input.schema().len(), input.expect_stream_key());

                    // If the input is a stream join, enforce the stream key as the materialized
                    // view distribution key to avoid slow backfilling caused by
                    // data skew of the dimension table join key.
                    // See <https://github.com/risingwavelabs/risingwave/issues/12824> for more information.
                    let is_stream_join = matches!(input.as_stream_hash_join(), Some(_join))
                        || matches!(input.as_stream_temporal_join(), Some(_join))
                        || matches!(input.as_stream_delta_join(), Some(_join));

                    if is_stream_join {
                        return Ok(required_dist.stream_enforce(input));
                    }

                    required_dist
                }
                TableType::Index => {
                    assert_matches!(
                        user_distributed_by,
                        RequiredDist::PhysicalDist(Distribution::HashShard(_))
                    );
                    user_distributed_by
                }
                TableType::VectorIndex => {
                    unreachable!("VectorIndex should not be created by StreamMaterialize")
                }
                TableType::Internal => unreachable!(),
            },
        };

        required_dist.streaming_enforce_if_not_satisfies(input)
    }

    /// Derive the table catalog with the given arguments.
    ///
    /// - The caller must ensure the validity of the given `columns`.
    /// - The `rewritten_input` should be generated by `rewrite_input`.
    #[expect(clippy::too_many_arguments)]
    fn derive_table_catalog(
        rewritten_input: PlanRef,
        name: String,
        database_id: DatabaseId,
        schema_id: SchemaId,
        user_distributed_by: RequiredDist,
        user_order_by: Order,
        columns: Vec<ColumnCatalog>,
        definition: String,
        conflict_behavior: ConflictBehavior,
        version_column_indices: Vec<usize>,
        pk_column_indices: Option<Vec<usize>>, // Is some when create table
        ttl_watermark_indices: Vec<usize>,
        row_id_index: Option<usize>,
        table_type: TableType,
        version: Option<TableVersion>,
        cardinality: Cardinality,
        retention_seconds: Option<NonZeroU32>,
        create_type: CreateType,
        webhook_info: Option<PbWebhookSourceInfo>,
        engine: Engine,
        refreshable: bool,
    ) -> Result<TableCatalog> {
        let input = rewritten_input;

        let value_indices = (0..columns.len()).collect_vec();
        let distribution_key = input.distribution().dist_column_indices().to_vec();
        let append_only = input.append_only();
        // TODO(rc): In `TableCatalog` we still use `FixedBitSet` for watermark columns, ignoring the watermark group information.
        // We will record the watermark group information in `TableCatalog` in the future. For now, let's flatten the watermark columns.
        let watermark_columns = input.watermark_columns().indices().collect();

        let (table_pk, mut stream_key) = if let Some(pk_column_indices) = pk_column_indices {
            let table_pk = pk_column_indices
                .iter()
                .map(|idx| ColumnOrder::new(*idx, OrderType::ascending()))
                .collect();
            // No order by for create table, so stream key is identical to table pk.
            (table_pk, pk_column_indices)
        } else {
            derive_pk(input, user_distributed_by, user_order_by, &columns)
        };

        // Guards only this table's own pk (table pk, index keys, MV ORDER BY); internal state
        // tables are covered by `reject_variant_in_internal_storage_key` in the stream fragmenter.
        for order in &table_pk {
            let column = &columns[order.column_index];
            if column.data_type().contains_variant() {
                return Err(variant_key_error(format!(
                    "VARIANT column \"{}\" is part of the storage primary key",
                    column.name(),
                )));
            }
        }

        // Add TTL watermark column to stream key.
        // When a row comes in to a TTL-ed table and we cannot find it in the table, we still cannot tell
        // whether it is a new row or an update to an expired row. Adding the TTL watermark column to the stream key
        // can ensure there's no double-insert from the view of the downstream jobs. See RFC for more details.
        for idx in ttl_watermark_indices.iter().copied() {
            if !stream_key.contains(&idx) {
                stream_key.push(idx);
            }
        }

        let read_prefix_len_hint = table_pk.len();
        Ok(TableCatalog {
            id: TableId::placeholder(),
            schema_id,
            database_id,
            associated_source_id: None,
            name,
            columns,
            pk: table_pk,
            stream_key,
            distribution_key,
            table_type,
            append_only,
            owner: risingwave_common::catalog::DEFAULT_SUPER_USER_ID,
            fragment_id: FragmentId::placeholder(),
            dml_fragment_id: None,
            vnode_col_index: None,
            row_id_index,
            value_indices,
            definition,
            conflict_behavior,
            version_column_indices,
            read_prefix_len_hint,
            version,
            watermark_columns,
            dist_key_in_pk: vec![],
            cardinality,
            created_at_epoch: None,
            initialized_at_epoch: None,
            create_type,
            stream_job_status: StreamJobStatus::Creating,
            description: None,
            initialized_at_cluster_version: None,
            created_at_cluster_version: None,
            retention_seconds: retention_seconds.map(|i| i.into()),
            cdc_table_id: None,
            vnode_count: VnodeCount::Placeholder, // will be filled in by the meta service later
            webhook_info,
            job_id: None,
            engine: match table_type {
                TableType::Table => engine,
                TableType::MaterializedView
                | TableType::Index
                | TableType::Internal
                | TableType::VectorIndex => {
                    assert_eq!(engine, Engine::Hummock);
                    engine
                }
            },
            clean_watermark_index_in_pk: None, // TODO: fill this field
            clean_watermark_indices: ttl_watermark_indices,
            refreshable,
            vector_index_info: None,
            cdc_table_type: None,
        })
    }

    /// The staging table is a pk-only table.
    fn derive_staging_table_catalog(
        TableCatalog {
            id,
            schema_id,
            database_id,
            associated_source_id,
            name,
            columns,
            pk,
            stream_key,
            table_type: _,
            distribution_key,
            append_only,
            cardinality,
            owner,
            retention_seconds,
            fragment_id,
            dml_fragment_id: _,
            vnode_col_index,
            row_id_index,
            value_indices: _,
            definition,
            conflict_behavior,
            version_column_indices,
            read_prefix_len_hint: _,
            version,
            watermark_columns: _,
            dist_key_in_pk,
            created_at_epoch,
            initialized_at_epoch,
            create_type,
            stream_job_status,
            description,
            created_at_cluster_version,
            initialized_at_cluster_version,
            cdc_table_id,
            vnode_count,
            webhook_info,
            job_id,
            engine,
            clean_watermark_index_in_pk,
            clean_watermark_indices,
            refreshable,
            vector_index_info,
            cdc_table_type,
        }: TableCatalog,
    ) -> TableCatalog {
        tracing::info!(
            table_name = %name,
            "Creating staging table for refreshable table"
        );

        assert!(row_id_index.is_none());
        assert!(retention_seconds.is_none());
        assert!(refreshable);

        // Keep pk columns in pk order, so the staging chunk projected by pk indices can be
        // written directly without reordering.
        let pk_col_indices = pk.iter().map(|pk| pk.column_index).collect_vec();
        let pk_cols = pk_col_indices
            .iter()
            .map(|&col_idx| columns[col_idx].clone())
            .collect_vec();
        let mapping = ColIndexMapping::with_remaining_columns(&pk_col_indices, columns.len());
        TableCatalog {
            id,
            schema_id,
            database_id,
            associated_source_id,
            name,
            value_indices: (0..pk_cols.len()).collect(),
            columns: pk_cols,
            pk: pk
                .iter()
                .map(|pk| ColumnOrder::new(mapping.map(pk.column_index), pk.order_type))
                .collect(),
            stream_key: mapping.try_map_all(stream_key).unwrap(),
            vnode_col_index: vnode_col_index.map(|i| mapping.map(i)),
            // `dist_key_in_pk` is already based on pk positions, so it should keep unchanged.
            dist_key_in_pk,
            distribution_key: mapping.try_map_all(distribution_key).unwrap(),
            table_type: TableType::Internal,
            watermark_columns: FixedBitSet::new(),
            append_only,
            cardinality,
            owner,
            retention_seconds: None,
            fragment_id,
            dml_fragment_id: None,
            row_id_index: None,
            definition,
            conflict_behavior,
            version_column_indices,
            // Refresh staging tables are consumed by the refresh merge path with
            // `iter_keyed_row_with_vnode` range scans. They are not read by
            // fixed-prefix lookups, so prefix SST filters would only add write cost.
            read_prefix_len_hint: 0,
            version,
            created_at_epoch,
            initialized_at_epoch,
            create_type,
            stream_job_status,
            description,
            created_at_cluster_version,
            initialized_at_cluster_version,
            cdc_table_id,
            vnode_count,
            webhook_info,
            job_id,
            engine,
            clean_watermark_index_in_pk,
            clean_watermark_indices,
            refreshable: false,
            vector_index_info,
            cdc_table_type,
        }
    }

    /// The refresh progress table is used to track refresh operation progress.
    /// Simplified Schema: vnode (i32), `current_pos`... (variable PK from upstream),
    /// `is_completed` (bool), `processed_rows` (i64)
    fn derive_refresh_progress_table_catalog(table: TableCatalog) -> TableCatalog {
        tracing::debug!(
            table_name = %table.name,
            "Creating refresh progress table for refreshable table"
        );

        // Define the simplified schema for the refresh progress table
        // Schema: | vnode | current_pos... | is_completed | processed_rows |
        let mut columns = vec![ColumnCatalog {
            column_desc: risingwave_common::catalog::ColumnDesc::named(
                "vnode",
                0.into(),
                DataType::Int16,
            ),
            is_hidden: false,
        }];

        // Add current_pos columns (mirror upstream table's primary key)
        let mut col_index = 1;
        for pk_col in &table.pk {
            let upstream_col = &table.columns[pk_col.column_index];
            columns.push(ColumnCatalog {
                column_desc: risingwave_common::catalog::ColumnDesc::named(
                    format!("pos_{}", upstream_col.name()),
                    col_index.into(),
                    upstream_col.data_type().clone(),
                ),
                is_hidden: false,
            });
            col_index += 1;
        }

        // Add metadata columns
        for (name, data_type) in [
            ("is_completed", DataType::Boolean),
            ("processed_rows", DataType::Int64),
        ] {
            columns.push(ColumnCatalog {
                column_desc: risingwave_common::catalog::ColumnDesc::named(
                    name,
                    col_index.into(),
                    data_type,
                ),
                is_hidden: false,
            });
            col_index += 1;
        }

        let mut builder = TableCatalogBuilder::default();

        // Add all columns to builder
        for column in &columns {
            builder.add_column(&(&column.column_desc).into());
        }

        // Primary key is vnode (column 0)
        builder.add_order_column(0, OrderType::ascending());
        builder.set_vnode_col_idx(0);
        builder.set_value_indices((0..columns.len()).collect());
        builder.set_dist_key_in_pk(vec![0]);

        builder.build(vec![0], 1)
    }

    /// Get a reference to the stream materialize's table.
    #[must_use]
    pub fn table(&self) -> &TableCatalog {
        &self.table
    }

    /// Get a reference to the stream materialize's staging table.
    #[must_use]
    pub fn staging_table(&self) -> Option<&TableCatalog> {
        self.staging_table.as_ref()
    }

    /// Get a reference to the stream materialize's refresh progress table.
    #[must_use]
    pub fn refresh_progress_table(&self) -> Option<&TableCatalog> {
        self.refresh_progress_table.as_ref()
    }

    pub fn name(&self) -> &str {
        self.table.name()
    }
}

impl Distill for StreamMaterialize {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let table = self.table();

        let column_names = (table.columns.iter())
            .map(|col| col.name_with_hidden().to_string())
            .map(Pretty::from)
            .collect();

        let stream_key = (table.stream_key().iter())
            .map(|&k| table.columns[k].name().to_owned())
            .map(Pretty::from)
            .collect();

        let pk_columns = (table.pk.iter())
            .map(|o| table.columns[o.column_index].name().to_owned())
            .map(Pretty::from)
            .collect();
        let mut vec = Vec::with_capacity(5);
        vec.push(("columns", Pretty::Array(column_names)));
        vec.push(("stream_key", Pretty::Array(stream_key)));
        vec.push(("pk_columns", Pretty::Array(pk_columns)));
        let pk_conflict_behavior = self.table.conflict_behavior().debug_to_string();

        vec.push(("pk_conflict", Pretty::from(pk_conflict_behavior)));

        let watermark_columns = &self.base.watermark_columns();
        if self.base.watermark_columns().n_indices() > 0 {
            // TODO(rc): we ignore the watermark group info here, will be fixed it later
            let watermark_column_names = watermark_columns
                .indices()
                .map(|i| table.columns()[i].name_with_hidden().to_string())
                .map(Pretty::from)
                .collect();
            vec.push(("watermark_columns", Pretty::Array(watermark_column_names)));
        };
        childless_record("StreamMaterialize", vec)
    }
}

impl PlanTreeNodeUnary<Stream> for StreamMaterialize {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let new = Self::new_with_staging_and_progress(
            input,
            self.table().clone(),
            self.staging_table.clone(),
            self.refresh_progress_table.clone(),
        )
        .unwrap();
        new.base
            .schema()
            .fields
            .iter()
            .zip_eq_fast(self.base.schema().fields.iter())
            .for_each(|(a, b)| {
                assert_eq!(a.data_type, b.data_type);
            });
        assert_eq!(new.plan_base().stream_key(), self.plan_base().stream_key());
        new
    }
}

impl_plan_tree_node_for_unary! { Stream, StreamMaterialize }

impl StreamNode for StreamMaterialize {
    fn to_stream_prost_body(&self, state: &mut BuildFragmentGraphState) -> PbNodeBody {
        use risingwave_pb::stream_plan::*;

        tracing::debug!(
            table_name = %self.table().name(),
            refreshable = %self.table().refreshable,
            has_staging_table = %self.staging_table.is_some(),
            has_progress_table = %self.refresh_progress_table.is_some(),
            staging_table_name = ?self.staging_table.as_ref().map(|t| (&t.id, &t.name)),
            progress_table_name = ?self.refresh_progress_table.as_ref().map(|t| (&t.id, &t.name)),
            "Converting StreamMaterialize to protobuf"
        );

        let staging_table_prost = self
            .staging_table
            .clone()
            .map(|t| t.with_id(state.gen_table_id_wrapped()).to_prost());

        let refresh_progress_table_prost = self
            .refresh_progress_table
            .clone()
            .map(|t| t.with_id(state.gen_table_id_wrapped()).to_prost());

        PbNodeBody::Materialize(Box::new(MaterializeNode {
            // Do not fill `table` and `table_id` here to avoid duplication. It will be filled by
            // meta service after global information is generated.
            table_id: 0.into(),
            table: None,
            // Pass staging table catalog if available for refreshable tables
            staging_table: staging_table_prost,
            // Pass refresh progress table catalog if available for refreshable tables
            refresh_progress_table: refresh_progress_table_prost,

            column_orders: self
                .table()
                .pk()
                .iter()
                .copied()
                .map(ColumnOrder::to_protobuf)
                .collect(),

            // Equavalency: `clean_watermark_indices` is set iff there's a TTL watermark column.
            // See `StreamMaterialize::derive_table_catalog`.
            cleaned_by_ttl_watermark: !self.table.clean_watermark_indices.is_empty(),
        }))
    }
}

impl ExprRewritable<Stream> for StreamMaterialize {}

impl ExprVisitable for StreamMaterialize {}
