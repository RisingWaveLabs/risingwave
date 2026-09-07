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
use std::sync::Arc;

use iceberg::spec::Transform;
use itertools::Itertools;
use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::catalog::{
    ColumnCatalog, ConflictBehavior, CreateType, FieldLike, RISINGWAVE_ICEBERG_ROW_ID,
    ROW_ID_COLUMN_NAME,
};
use risingwave_common::types::{DataType, StructType};
use risingwave_common::util::iter_util::ZipEqDebug;
use risingwave_connector::sink::catalog::desc::SinkDesc;
use risingwave_connector::sink::catalog::{SinkFormat, SinkFormatDesc, SinkId, SinkType};
use risingwave_connector::sink::file_sink::fs::FsSink;
use risingwave_connector::sink::iceberg::{ENABLE_PK_INDEX, ICEBERG_SINK};
use risingwave_connector::sink::trivial::TABLE_SINK;
use risingwave_connector::sink::{
    CONNECTOR_TYPE_KEY, SINK_TYPE_APPEND_ONLY, SINK_TYPE_DEBEZIUM, SINK_TYPE_OPTION,
    SINK_TYPE_RETRACT, SINK_TYPE_UPSERT, SINK_USER_FORCE_APPEND_ONLY_OPTION,
    SINK_USER_IGNORE_DELETE_OPTION, SINK_USER_PRESERVE_ROW_LEVEL_CHANGES,
};
use risingwave_connector::{AUTO_SCHEMA_CHANGE_KEY, WithPropertiesExt, match_sink_name_str};
use risingwave_pb::expr::expr_node::Type;
use risingwave_pb::stream_plan::SinkLogStoreType;
use risingwave_pb::stream_plan::stream_node::PbNodeBody;

use super::derive::{derive_columns, derive_pk};
use super::stream::prelude::*;
use super::utils::{
    Distill, IndicesDisplay, childless_record, infer_kv_log_store_table_catalog_inner,
};
use super::{
    ExprRewritable, PlanBase, StreamExchange, StreamNode, StreamPlanRef as PlanRef, StreamProject,
    StreamSyncLogStore, generic,
};
use crate::TableCatalog;
use crate::error::{ErrorCode, Result, RwError, bail_bind_error, bail_invalid_input_syntax};
use crate::expr::{ExprImpl, FunctionCall, InputRef};
use crate::optimizer::StreamOptimizedLogicalPlanRoot;
use crate::optimizer::plan_node::PlanTreeNodeUnary;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::utils::plan_can_use_background_ddl;
use crate::optimizer::property::{Distribution, RequiredDist};
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::utils::WithOptionsSecResolved;

const DOWNSTREAM_PK_KEY: &str = "primary_key";
const CREATE_TABLE_IF_NOT_EXISTS: &str = "create_table_if_not_exists";

fn target_table_requires_row_level_conflict_handling(target_table: &TableCatalog) -> bool {
    !target_table.version_column_indices.is_empty()
        || matches!(
            target_table.conflict_behavior(),
            ConflictBehavior::DoUpdateIfNotNull | ConflictBehavior::IgnoreConflict
        )
}

/// ## Why we need `PartitionComputeInfo`?
///
/// For some sink, it will write the data into different file based on the partition value. E.g. iceberg sink(<https://iceberg.apache.org/spec/#partitioning>)
/// For this kind of sink, the file num can be reduced if we can shuffle the data based on the partition value. More details can be found in <https://github.com/risingwavelabs/rfcs/pull/77>.
/// So if the `PartitionComputeInfo` provided, we will create a `StreamProject` node to compute the partition value and shuffle the data based on the partition value before the sink.
///
/// ## What is `PartitionComputeInfo`?
/// The `PartitionComputeInfo` contains the information about partition compute. The stream sink will use
/// these information to create the corresponding expression in `StreamProject` node.
///
/// #TODO
/// Maybe we should move this in sink?
pub enum PartitionComputeInfo {
    Iceberg(IcebergPartitionInfo),
}

impl PartitionComputeInfo {
    pub fn convert_to_expression(self, columns: &[ColumnCatalog]) -> Result<ExprImpl> {
        match self {
            PartitionComputeInfo::Iceberg(info) => info.convert_to_expression(columns),
        }
    }
}

pub struct IcebergPartitionInfo {
    pub partition_type: StructType,
    // (partition_field_name, partition_field_transform)
    pub partition_fields: Vec<(String, Transform)>,
}

impl IcebergPartitionInfo {
    #[inline]
    fn transform_to_expression(
        transform: &Transform,
        col_id: usize,
        columns: &[ColumnCatalog],
        result_type: DataType,
    ) -> Result<ExprImpl> {
        match transform {
            Transform::Identity => {
                if columns[col_id].column_desc.data_type != result_type {
                    return Err(ErrorCode::InvalidInputSyntax(format!(
                        "The partition field {} has type {}, but the partition field is {}",
                        columns[col_id].column_desc.name,
                        columns[col_id].column_desc.data_type,
                        result_type
                    ))
                    .into());
                }
                Ok(ExprImpl::InputRef(
                    InputRef::new(col_id, result_type).into(),
                ))
            }
            Transform::Void => Ok(ExprImpl::literal_null(result_type)),
            _ => Ok(ExprImpl::FunctionCall(
                FunctionCall::new_unchecked(
                    Type::IcebergTransform,
                    vec![
                        ExprImpl::literal_varchar(transform.to_string()),
                        ExprImpl::InputRef(
                            InputRef::new(col_id, columns[col_id].column_desc.data_type.clone())
                                .into(),
                        ),
                    ],
                    result_type,
                )
                .into(),
            )),
        }
    }

    pub fn convert_to_expression(self, columns: &[ColumnCatalog]) -> Result<ExprImpl> {
        let child_exprs = self
            .partition_fields
            .into_iter()
            .zip_eq_debug(self.partition_type.iter())
            .map(|((field_name, transform), (_, result_type))| {
                let col_id = find_column_idx_by_name(columns, &field_name)?;
                Self::transform_to_expression(&transform, col_id, columns, result_type.clone())
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(ExprImpl::FunctionCall(
            FunctionCall::new_unchecked(
                Type::Row,
                child_exprs,
                DataType::Struct(self.partition_type),
            )
            .into(),
        ))
    }
}

#[inline]
fn find_column_idx_by_name(columns: &[ColumnCatalog], col_name: &str) -> Result<usize> {
    columns
        .iter()
        .position(|col| col.column_desc.name == col_name)
        .ok_or_else(|| {
            ErrorCode::InvalidInputSyntax(format!("Sink primary key column not found: {}. Please use ',' as the delimiter for different primary key columns.", col_name))
                .into()
        })
}

/// [`StreamSink`] represents a table/connector sink at the very end of the graph.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamSink {
    pub base: PlanBase<Stream>,
    input: PlanRef,
    sink_desc: SinkDesc,
    log_store_type: SinkLogStoreType,
}

impl StreamSink {
    #[must_use]
    pub fn new(input: PlanRef, sink_desc: SinkDesc, log_store_type: SinkLogStoreType) -> Self {
        // The sink executor will transform the chunk into desired format based on the sink type
        // before writing to the sink or emitting to the downstream. Thus, we need to derive the
        // stream kind based on the sink type.
        // We assert here because checks should already be done in `derive_sink_type`.
        let input_kind = input.stream_kind();
        let kind = match sink_desc.sink_type {
            SinkType::AppendOnly => {
                if !sink_desc.ignore_delete {
                    assert_eq!(
                        input_kind,
                        StreamKind::AppendOnly,
                        "{input_kind} stream cannot be used as input of append-only sink",
                    );
                }
                StreamKind::AppendOnly
            }
            SinkType::Upsert => StreamKind::Upsert,
            SinkType::Retract => {
                assert_ne!(
                    input_kind,
                    StreamKind::Upsert,
                    "upsert stream cannot be used as input of retract sink",
                );
                StreamKind::Retract
            }
        };

        let base = PlanBase::new_stream(
            input.ctx(),
            input.schema().clone(),
            // FIXME: We may reconstruct the chunk based on user-specified downstream pk, so
            // we should also use `downstream_pk` as the stream key of the output. Though this
            // is unlikely to result in correctness issues:
            // - for sink-into-table, the `Materialize` node in the downstream table will always
            //   enforce the pk consistency
            // - for other sinks, the output of `Sink` node is not used
            input.stream_key().map(|v| v.to_vec()),
            input.functional_dependency().clone(),
            input.distribution().clone(),
            kind,
            input.emit_on_window_close(),
            input.watermark_columns().clone(),
            input.columns_monotonicity().clone(),
        );

        Self {
            base,
            input,
            sink_desc,
            log_store_type,
        }
    }

    pub fn sink_desc(&self) -> &SinkDesc {
        &self.sink_desc
    }

    fn derive_iceberg_sink_distribution(
        input: PlanRef,
        partition_info: Option<PartitionComputeInfo>,
        columns: &[ColumnCatalog],
    ) -> Result<(RequiredDist, PlanRef, Option<usize>)> {
        // For here, we need to add the plan node to compute the partition value, and add it as a extra column.
        if let Some(partition_info) = partition_info {
            let input_fields = input.schema().fields();

            let mut exprs: Vec<_> = input_fields
                .iter()
                .enumerate()
                .map(|(idx, field)| InputRef::new(idx, field.data_type.clone()).into())
                .collect();

            // Add the partition compute expression to the end of the exprs
            exprs.push(partition_info.convert_to_expression(columns)?);
            let partition_col_idx = exprs.len() - 1;
            let project = StreamProject::new(generic::Project::new(exprs.clone(), input));
            Ok((
                RequiredDist::shard_by_key(project.schema().len(), &[partition_col_idx]),
                project.into(),
                Some(partition_col_idx),
            ))
        } else {
            Ok((
                RequiredDist::shard_by_key(input.schema().len(), input.expect_stream_key()),
                input,
                None,
            ))
        }
    }

    #[expect(clippy::too_many_arguments)]
    pub fn create(
        StreamOptimizedLogicalPlanRoot {
            plan: mut input,
            required_dist: user_distributed_by,
            required_order: user_order_by,
            out_fields: user_cols,
            out_names,
            ..
        }: StreamOptimizedLogicalPlanRoot,
        name: String,
        db_name: String,
        sink_from_table_name: String,
        target_table: Option<Arc<TableCatalog>>,
        target_table_mapping: Option<Vec<Option<usize>>>,
        definition: String,
        mut properties: WithOptionsSecResolved,
        format_desc: Option<SinkFormatDesc>,
        partition_info: Option<PartitionComputeInfo>,
        auto_refresh_schema_from_table: Option<Arc<TableCatalog>>,
    ) -> Result<Self> {
        let (sink_type, ignore_delete) =
            Self::derive_sink_type(input.stream_kind(), &properties, format_desc.as_ref())?;

        let mut emit_pk_extension_notice: Option<String> = None;
        let mut columns = derive_columns(input.schema(), out_names, &user_cols)?;
        let (pk, _) = derive_pk(
            input.clone(),
            user_distributed_by.clone(),
            user_order_by,
            &columns,
        );
        let derived_pk = pk.iter().map(|k| k.column_index).collect_vec();

        let is_iceberg_pk_index = properties.is_iceberg_connector()
            && properties
                .get(ENABLE_PK_INDEX)
                .is_some_and(|v| v.eq_ignore_ascii_case("true"));

        // For Iceberg pk-index sinks, the iceberg primary key is derived entirely from the
        // upstream stream key, so the user must not specify `primary_key` explicitly.
        if is_iceberg_pk_index && properties.get(DOWNSTREAM_PK_KEY).is_some() {
            return Err(ErrorCode::InvalidInputSyntax(
                "Iceberg sink with `enable_pk_index='true'` does not allow a user-specified `primary_key`. \
                The primary key is automatically derived from the upstream stream key.".to_owned(),
            )
            .into());
        }

        // Get downstream pk from user input, override and perform some checks if applicable.
        let mut downstream_pk = properties
            .get(DOWNSTREAM_PK_KEY)
            .map(|v| Self::parse_downstream_pk(v, &columns))
            .transpose()?;

        if let Some(t) = &target_table {
            let user_defined_primary_key_table = t.row_id_index.is_none();
            let sink_is_append_only = sink_type.is_append_only();

            if !user_defined_primary_key_table && !sink_is_append_only {
                return Err(RwError::from(ErrorCode::BindError(
                        "Only append-only sinks can sink to a table without primary keys. please try to add type = 'append-only' in the with option. e.g. create sink s into t as select * from t1 with (type = 'append-only')".to_owned(),
                    )));
            }

            if t.append_only && !sink_is_append_only {
                return Err(RwError::from(ErrorCode::BindError(
                        "Only append-only sinks can sink to a append only table. please try to add type = 'append-only' in the with option. e.g. create sink s into t as select * from t1 with (type = 'append-only')".to_owned(),
                    )));
            }

            if sink_is_append_only {
                downstream_pk = None;
            } else {
                let target_table_mapping = target_table_mapping.unwrap();
                let pk = t.pk()
                        .iter()
                        .map(|c| {
                            target_table_mapping[c.column_index].ok_or_else(
                                || ErrorCode::InvalidInputSyntax("When using non append only sink into table, the primary key of the table must be included in the sink result.".to_owned()).into())
                        })
                        .try_collect::<_, _, RwError>()?;
                downstream_pk = Some(pk);
            }
        } else if downstream_pk.is_none()
            && sink_type == SinkType::Upsert
            && (properties
                .get(CREATE_TABLE_IF_NOT_EXISTS)
                .is_some_and(|v| v.eq_ignore_ascii_case("true"))
                || properties.is_iceberg_connector())
        {
            downstream_pk = Some(derived_pk.clone())
        } else if is_iceberg_pk_index {
            // For Iceberg pk-index sinks: derive the iceberg primary key entirely from the
            // upstream stream key. Every stream-key column becomes a pk column; hidden ones are
            // promoted to visible so they are carried into the iceberg table verbatim.
            let (pk, promoted) = promote_iceberg_pk_index_stream_key(&input, &mut columns)?;

            let pk_names = pk
                .iter()
                .map(|&i| columns[i].name().to_owned())
                .collect::<Vec<_>>()
                .join(",");

            // Promoting hidden stream-key columns adds new columns to the iceberg table, which is
            // only possible at table-creation time. Sinking into an existing iceberg table would
            // silently drop them and corrupt the pk index, so require `create_table_if_not_exists`.
            if promoted
                && !properties
                    .get(CREATE_TABLE_IF_NOT_EXISTS)
                    .is_some_and(|v| v.eq_ignore_ascii_case("true"))
            {
                return Err(ErrorCode::InvalidInputSyntax(
                    "Iceberg sink with `enable_pk_index='true'` requires `create_table_if_not_exists='true'` \
                     because the planner needs to add the hidden upstream stream-key columns to the iceberg table. \
                     Existing iceberg tables cannot be extended in-place by this sink."
                        .to_owned(),
                )
                .into());
            }

            // Inform the user of the derived iceberg primary key, since they did not (and
            // cannot) specify it explicitly.
            emit_pk_extension_notice = Some(pk_names.clone());
            properties.insert(DOWNSTREAM_PK_KEY.to_owned(), pk_names);
            downstream_pk = Some(pk);
        }

        // Since we've already rejected empty pk in `parse_downstream_pk`, if we still get an empty pk here,
        // it's likely that the derived stream key is used and it's empty, which is possible in cases of
        // operators outputting at most one row (like `SimpleAgg`). This is legitimate. However, currently
        // the sink implementation may confuse empty pk with not specifying pk, so we still reject this case
        // for correctness.
        if let Some(pk) = &downstream_pk
            && pk.is_empty()
        {
            bail_invalid_input_syntax!(
                "Empty primary key is not supported. \
                 Please specify the primary key in WITH options."
            )
        }

        // The "upsert" property is defined based on a specific stream key: columns other than the
        // stream key might not be valid. We should reject the cases referencing such columns in
        // primary key unless the user explicitly opts in to the unsafe behavior.
        if let StreamKind::Upsert = input.stream_kind()
            && let Some(downstream_pk) = &downstream_pk
            && !downstream_pk.iter().all(|i| derived_pk.contains(i))
        {
            let unsafe_allow_pk_mismatch = input
                .ctx()
                .session_ctx()
                .config()
                .streaming_unsafe_allow_upsert_sink_pk_mismatch();
            if !unsafe_allow_pk_mismatch {
                bail_bind_error!(
                    "When sinking from an upsert stream, \
                     the downstream primary key must be the same as or a subset of the one derived from the stream."
                )
            }
            input.ctx().session_ctx().notice_to_user(
                "Unsafe upsert sink primary-key mismatch is allowed by session variable \
                 `streaming_unsafe_allow_upsert_sink_pk_mismatch`. This may leave stale rows in \
                 the downstream system if a downstream primary-key column changes without its \
                 old value being emitted.",
            );
        }

        if let Some(upstream_table) = &auto_refresh_schema_from_table
            && let Some(downstream_pk) = &downstream_pk
        {
            let upstream_table_pk_col_names = upstream_table
                .pk
                .iter()
                .map(|order| {
                    upstream_table.columns[order.column_index]
                        .column_desc
                        .name()
                })
                .collect_vec();
            let sink_pk_col_names = downstream_pk
                .iter()
                .map(|&column_index| columns[column_index].name())
                .collect_vec();
            if upstream_table_pk_col_names != sink_pk_col_names {
                let is_iceberg_row_id_alias = properties.is_iceberg_connector()
                    && upstream_table_pk_col_names.len() == 1
                    && upstream_table_pk_col_names[0] == ROW_ID_COLUMN_NAME
                    && sink_pk_col_names.len() == 1
                    && sink_pk_col_names[0] == RISINGWAVE_ICEBERG_ROW_ID;
                if !is_iceberg_row_id_alias {
                    return Err(ErrorCode::InvalidInputSyntax(format!(
                        "sink with auto schema change should have same pk as upstream table {:?}, but got {:?}",
                        upstream_table_pk_col_names, sink_pk_col_names
                    ))
                    .into());
                }
            }
        }

        let mut extra_partition_col_idx = None;

        let required_dist = match input.distribution() {
            Distribution::Single => RequiredDist::single(),
            _ => {
                match properties.get("connector") {
                    Some(s) if s == "jdbc" && sink_type == SinkType::Upsert => {
                        let Some(downstream_pk) = &downstream_pk else {
                            return Err(ErrorCode::InvalidInputSyntax(format!(
                                "Primary key must be defined for upsert JDBC sink. Please specify the \"{key}='pk1,pk2,...'\" in WITH options.",
                                key = DOWNSTREAM_PK_KEY
                            )).into());
                        };
                        // for upsert jdbc sink we align distribution to downstream to avoid
                        // lock contentions
                        RequiredDist::hash_shard(downstream_pk)
                    }
                    Some(s) if s == ICEBERG_SINK => {
                        // pk-index sinks shard by pk for state-table locality and never use the
                        // partition-based shuffle, so skip computing the extra partition column
                        // entirely.
                        let partition_info = if is_iceberg_pk_index {
                            None
                        } else {
                            partition_info
                        };
                        let (default_dist, new_input, partition_col_idx) =
                            Self::derive_iceberg_sink_distribution(
                                input,
                                partition_info,
                                &columns,
                            )?;
                        input = new_input;
                        extra_partition_col_idx = partition_col_idx;
                        // Use an exact hash-shard on the full pk (not `shard_by_key`): the pk-index
                        // state table is distributed by the whole derived pk in pk order, so the
                        // writer fragment must hash on exactly the same columns and order.
                        if is_iceberg_pk_index && let Some(pk) = &downstream_pk {
                            RequiredDist::hash_shard(pk)
                        } else {
                            default_dist
                        }
                    }
                    _ => {
                        assert_matches!(user_distributed_by, RequiredDist::Any);
                        if let Some(downstream_pk) = &downstream_pk {
                            // force the same primary key be written into the same sink shard to make sure the sink pk mismatch compaction effective
                            // https://github.com/risingwavelabs/risingwave/blob/6d88344c286f250ea8a7e7ef6b9d74dea838269e/src/stream/src/executor/sink.rs#L169-L198
                            RequiredDist::shard_by_key(input.schema().len(), downstream_pk)
                        } else {
                            RequiredDist::shard_by_key(
                                input.schema().len(),
                                input.expect_stream_key(),
                            )
                        }
                    }
                }
            }
        };
        let input = required_dist.streaming_enforce_if_not_satisfies(input)?;
        let input = if input.ctx().session_ctx().config().streaming_separate_sink()
            && input.as_stream_exchange().is_none()
        {
            StreamExchange::new_no_shuffle(input).into()
        } else {
            input
        };

        let distribution_key = input.distribution().dist_column_indices().to_vec();
        let create_type = if input.ctx().session_ctx().config().background_ddl()
            && plan_can_use_background_ddl(&input)
        {
            CreateType::Background
        } else {
            CreateType::Foreground
        };
        let (mut properties, secret_refs) = properties.into_parts();
        if let Some(target_table) = &target_table
            && target_table_requires_row_level_conflict_handling(target_table)
        {
            properties.insert(
                SINK_USER_PRESERVE_ROW_LEVEL_CHANGES.to_owned(),
                "true".to_owned(),
            );
        }
        let is_exactly_once = properties
            .get("is_exactly_once")
            .map(|v| v.to_lowercase() == "true");

        let mut sink_desc = SinkDesc {
            id: SinkId::placeholder(),
            name,
            db_name,
            sink_from_name: sink_from_table_name,
            definition,
            columns,
            plan_pk: pk,
            downstream_pk,
            distribution_key,
            properties,
            secret_refs,
            sink_type,
            ignore_delete,
            format_desc,
            target_table: target_table.as_ref().map(|catalog| catalog.id()),
            extra_partition_col_idx,
            create_type,
            is_exactly_once,
            auto_refresh_schema_from_table: auto_refresh_schema_from_table
                .as_ref()
                .map(|table| table.id),
        };

        let unsupported_sink = |sink: &str| -> Result<_> {
            Err(ErrorCode::InvalidInputSyntax(format!("unsupported sink type {}", sink)).into())
        };

        // check and ensure that the sink connector is specified and supported
        let sink_decouple = match sink_desc.properties.get(CONNECTOR_TYPE_KEY) {
            Some(connector) => {
                let connector_type = connector.to_lowercase();
                match_sink_name_str!(
                    connector_type.as_str(),
                    SinkType,
                    {
                        // the table sink is created by with properties
                        if connector == TABLE_SINK && sink_desc.target_table.is_none() {
                            unsupported_sink(TABLE_SINK)
                        } else {
                            sink_desc.properties.remove(AUTO_SCHEMA_CHANGE_KEY);
                            SinkType::set_default_commit_checkpoint_interval(
                                &mut sink_desc,
                                &input.ctx().session_ctx().config().sink_decouple(),
                            )?;
                            let support_schema_change = SinkType::support_schema_change();
                            if !support_schema_change && auto_refresh_schema_from_table.is_some() {
                                return Err(ErrorCode::InvalidInputSyntax(format!(
                                    "{} sink does not support schema change",
                                    connector_type
                                ))
                                .into());
                            }
                            SinkType::is_sink_decouple(
                                &input.ctx().session_ctx().config().sink_decouple(),
                            )
                            .map_err(Into::into)
                        }
                    },
                    |other: &str| unsupported_sink(other)
                )?
            }
            None => {
                return Err(ErrorCode::InvalidInputSyntax(
                    "connector not specified when create sink".to_owned(),
                )
                .into());
            }
        };
        if !sink_decouple
            && sink_desc.is_exactly_once.is_none()
            && let Some(connector) = sink_desc.properties.get(CONNECTOR_TYPE_KEY)
        {
            let connector_type = connector.to_lowercase();
            if connector_type == ICEBERG_SINK {
                // iceberg sink defaults to exactly once
                // However, when sink_decouple is disabled, we enforce it to false.
                sink_desc
                    .properties
                    .insert("is_exactly_once".to_owned(), "false".to_owned());
            }
        }
        let log_store_type = if sink_decouple {
            SinkLogStoreType::KvLogStore
        } else {
            SinkLogStoreType::InMemoryLogStore
        };

        // sink into table should have logstore for sink_decouple
        let input = if sink_decouple && target_table.is_some() {
            StreamSyncLogStore::new(input).into()
        } else {
            input
        };

        let sink = Self::new(input, sink_desc, log_store_type);
        if let Some(pk_names) = emit_pk_extension_notice {
            sink.base.ctx().session_ctx().notice_to_user(format!(
                "Iceberg pk-index sink `{}`: the iceberg primary key was automatically derived from \
                 the upstream stream key as ({}).",
                sink.sink_desc.name, pk_names,
            ));
        }
        Ok(sink)
    }

    fn sink_type_in_prop(properties: &WithOptionsSecResolved) -> Result<Option<SinkType>> {
        if let Some(sink_type) = properties.get(SINK_TYPE_OPTION) {
            let sink_type = match sink_type.as_str() {
                SINK_TYPE_APPEND_ONLY => SinkType::AppendOnly,
                SINK_TYPE_UPSERT => {
                    if properties.is_iceberg_connector() {
                        // Iceberg sink must use retract to represent deletes
                        SinkType::Retract
                    } else {
                        SinkType::Upsert
                    }
                }
                SINK_TYPE_RETRACT | SINK_TYPE_DEBEZIUM => SinkType::Retract,
                _ => {
                    return Err(ErrorCode::InvalidInputSyntax(format!(
                        "`{}` must be {}, {}, {}, or {}",
                        SINK_TYPE_OPTION,
                        SINK_TYPE_APPEND_ONLY,
                        SINK_TYPE_RETRACT,
                        SINK_TYPE_UPSERT,
                        SINK_TYPE_DEBEZIUM,
                    ))
                    .into());
                }
            };
            return Ok(Some(sink_type));
        }
        Ok(None)
    }

    /// `ignore_delete` option, with backward-compatible alias `force_append_only`.
    fn is_user_ignore_delete(properties: &WithOptionsSecResolved) -> Result<bool> {
        let has_ignore_delete = properties.contains_key(SINK_USER_IGNORE_DELETE_OPTION);
        let has_force_append_only = properties.contains_key(SINK_USER_FORCE_APPEND_ONLY_OPTION);

        if has_ignore_delete && has_force_append_only {
            return Err(ErrorCode::InvalidInputSyntax(format!(
                "`{}` is an alias of `{}`, only one of them can be specified.",
                SINK_USER_FORCE_APPEND_ONLY_OPTION, SINK_USER_IGNORE_DELETE_OPTION
            ))
            .into());
        }

        let key = if has_ignore_delete {
            SINK_USER_IGNORE_DELETE_OPTION
        } else if has_force_append_only {
            SINK_USER_FORCE_APPEND_ONLY_OPTION
        } else {
            return Ok(false);
        };

        if properties.value_eq_ignore_case(key, "true") {
            Ok(true)
        } else if properties.value_eq_ignore_case(key, "false") {
            Ok(false)
        } else {
            Err(ErrorCode::InvalidInputSyntax(format!("`{key}` must be true or false")).into())
        }
    }

    /// Derive the sink type based on...
    ///
    /// - the derived stream kind of the plan, from the optimizer
    /// - sink format required by [`SinkFormatDesc`], if any
    /// - user-specified sink type in WITH options, if any
    /// - user-specified `ignore_delete` (`force_append_only`) in WITH options, if any
    ///
    /// Returns the `sink_type` and `ignore_delete`.
    fn derive_sink_type(
        derived_stream_kind: StreamKind,
        properties: &WithOptionsSecResolved,
        format_desc: Option<&SinkFormatDesc>,
    ) -> Result<(SinkType, bool)> {
        let (user_defined_sink_type, user_ignore_delete, syntax_legacy) = match format_desc {
            Some(f) => (
                Some(match f.format {
                    SinkFormat::AppendOnly => SinkType::AppendOnly,
                    SinkFormat::Upsert => SinkType::Upsert,
                    SinkFormat::Debezium => SinkType::Retract,
                }),
                Self::is_user_ignore_delete(&WithOptionsSecResolved::without_secrets(
                    f.options.clone(),
                ))?,
                false,
            ),
            None => (
                Self::sink_type_in_prop(properties)?,
                Self::is_user_ignore_delete(properties)?,
                true,
            ),
        };

        if let Some(user_defined_sink_type) = user_defined_sink_type {
            match user_defined_sink_type {
                SinkType::AppendOnly => {
                    if derived_stream_kind != StreamKind::AppendOnly && !user_ignore_delete {
                        return Err(ErrorCode::InvalidInputSyntax(format!(
                            "The sink of {} stream cannot be append-only. Please add \"force_append_only='true'\" in {} options to force the sink to be append-only. \
                             Notice that this will cause the sink executor to drop DELETE messages and convert UPDATE messages to INSERT.",
                            derived_stream_kind,
                            if syntax_legacy { "WITH" } else { "FORMAT ENCODE" }
                    ))
                        .into());
                    }
                }
                SinkType::Upsert => { /* always qualified */ }
                SinkType::Retract => {
                    if user_ignore_delete {
                        bail_invalid_input_syntax!(
                            "Retract sink type does not support `ignore_delete`. \
                             Please use `type = 'append-only'` or `type = 'upsert'` instead.",
                        );
                    }
                    if derived_stream_kind == StreamKind::Upsert {
                        bail_invalid_input_syntax!(
                            "The sink of upsert stream cannot be retract. \
                             Please create a materialized view or sink-into-table with this query before sinking it.",
                        );
                    }
                }
            }
            Ok((user_defined_sink_type, user_ignore_delete))
        } else {
            // No specification at all, follow the optimizer's derivation.
            // This is also the case for sink-into-table.
            let sink_type = match derived_stream_kind {
                // We downgrade `Retract` to `Upsert` unless explicitly specified the type in options,
                // as it is well supported by most sinks and reduces the amount of data written.
                StreamKind::Retract | StreamKind::Upsert => SinkType::Upsert,
                StreamKind::AppendOnly => SinkType::AppendOnly,
            };
            Ok((sink_type, user_ignore_delete))
        }
    }

    /// Extract user-defined downstream pk columns from with options. Return the indices of the pk
    /// columns. An empty list of columns is not allowed.
    ///
    /// The format of `downstream_pk_str` should be 'col1,col2,...' (delimited by `,`) in order to
    /// get parsed.
    fn parse_downstream_pk(
        downstream_pk_str: &str,
        columns: &[ColumnCatalog],
    ) -> Result<Vec<usize>> {
        // If the user defines the downstream primary key, we find out their indices.
        let downstream_pk = downstream_pk_str.split(',').collect_vec();
        let mut downstream_pk_indices = Vec::with_capacity(downstream_pk.len());
        for key in downstream_pk {
            let trimmed_key = key.trim();
            if trimmed_key.is_empty() {
                continue;
            }
            downstream_pk_indices.push(find_column_idx_by_name(columns, trimmed_key)?);
        }
        if downstream_pk_indices.is_empty() {
            bail_invalid_input_syntax!(
                "Specified primary key should not be empty. \
                To use derived primary key, remove {DOWNSTREAM_PK_KEY} from WITH options instead."
            );
        }
        Ok(downstream_pk_indices)
    }

    /// The table schema is: | epoch | seq id | row op | sink columns |
    /// Pk is: | epoch | seq id |
    fn infer_kv_log_store_table_catalog(&self) -> TableCatalog {
        infer_kv_log_store_table_catalog_inner(&self.input, &self.sink_desc().columns)
    }

    /// Convert this `StreamSink` into a `PlanRef`.
    ///
    /// For Iceberg pk index sinks, this rewrites the plan into
    /// `Upstream → Writer → Exchange(Hash) → PositionDeleteMerger` instead of a single `SinkNode`.
    /// For all other sinks, returns `self` as-is.
    pub fn into_stream_plan(self) -> Result<PlanRef> {
        use super::{StreamIcebergWithPkIndexPositionDeleteMerger, StreamIcebergWithPkIndexWriter};

        if !is_iceberg_with_pk_index_sink(&self.sink_desc)? {
            return Ok(self.into());
        }

        let writer: PlanRef = StreamIcebergWithPkIndexWriter::from_stream_sink(&self)?.into();
        let position_delete_merger: PlanRef =
            StreamIcebergWithPkIndexPositionDeleteMerger::new(writer, self.sink_desc).into();
        Ok(position_delete_merger)
    }
}

pub fn is_iceberg_with_pk_index_sink(sink_desc: &SinkDesc) -> Result<bool> {
    if !sink_desc
        .properties
        .get(CONNECTOR_TYPE_KEY)
        .is_some_and(|connector| connector.eq_ignore_ascii_case(ICEBERG_SINK))
    {
        return Ok(false);
    }

    let res = sink_desc
        .properties
        .get(ENABLE_PK_INDEX)
        .is_some_and(|v| v.eq_ignore_ascii_case("true"));
    Ok(res)
}

/// For Iceberg pk-index sinks, promote the `downstream_pk` and sink columns that use the
/// full upstream stream key as the iceberg primary key.
///
/// Every stream-key column becomes an iceberg pk column. A stream-key column that is hidden
/// (e.g. a join key the user did not `SELECT`, or a RisingWave-internal column such as
/// `_row_id`) is promoted to visible (`is_hidden = false`) so it is carried into the iceberg
/// table, keeping its type.
///
/// Returns `(downstream_pk, promoted)` where `promoted` is true if any hidden column was promoted
fn promote_iceberg_pk_index_stream_key(
    input: &PlanRef,
    columns: &mut Vec<ColumnCatalog>,
) -> Result<(Vec<usize>, bool)> {
    let mut promoted = false;
    let stream_key = input.expect_stream_key();
    if stream_key.is_empty() {
        bail_invalid_input_syntax!(
            "Iceberg sink with `enable_pk_index='true'` requires a non-empty upstream stream key \
             to derive the primary key from."
        );
    }

    for &i in stream_key {
        if !columns[i].is_hidden {
            continue;
        }
        // Promote the hidden stream-key column to visible so it is carried into iceberg.
        columns[i].is_hidden = false;
        promoted = true;
    }

    // The iceberg pk-index writer writes every input column verbatim (it has no hidden-column
    // projection). After promoting the stream-key columns, any column still hidden would be one
    // that is neither user-selected nor part of the stream key — the optimizer is expected to have
    // pruned such columns. A violation here is an internal planner bug rather than invalid user
    // input, so fail loudly at planning time instead of silently writing the column into the
    // iceberg table.
    if let Some(col) = columns.iter().find(|c| c.is_hidden) {
        return Err(ErrorCode::InternalError(format!(
            "iceberg pk-index sink has a hidden column `{}` after stream-key promotion; \
             all sink columns must be visible",
            col.name()
        ))
        .into());
    }

    let downstream_pk = stream_key.to_vec();
    Ok((downstream_pk, promoted))
}

impl PlanTreeNodeUnary<Stream> for StreamSink {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input, self.sink_desc.clone(), self.log_store_type)
        // TODO(nanderstabel): Add assertions (assert_eq!)
    }
}

impl_plan_tree_node_for_unary! { Stream, StreamSink }

impl Distill for StreamSink {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let sink_type = if self.sink_desc.sink_type.is_append_only() {
            "append-only"
        } else {
            "upsert"
        };
        let column_names = self
            .sink_desc
            .columns
            .iter()
            .map(|col| col.name_with_hidden().to_string())
            .map(Pretty::from)
            .collect();
        let column_names = Pretty::Array(column_names);
        let mut vec = Vec::with_capacity(3);
        vec.push(("type", Pretty::from(sink_type)));
        vec.push(("columns", column_names));
        if let Some(pk) = &self.sink_desc.downstream_pk {
            let sink_pk = IndicesDisplay {
                indices: pk,
                schema: self.base.schema(),
            };
            vec.push(("downstream_pk", sink_pk.distill()));
        }
        childless_record("StreamSink", vec)
    }
}

impl StreamNode for StreamSink {
    fn to_stream_prost_body(&self, state: &mut BuildFragmentGraphState) -> PbNodeBody {
        use risingwave_pb::stream_plan::*;

        // We need to create a table for sink with a kv log store.
        let table = self
            .infer_kv_log_store_table_catalog()
            .with_id(state.gen_table_id_wrapped());

        PbNodeBody::Sink(Box::new(SinkNode {
            sink_desc: Some(self.sink_desc.to_proto()),
            table: Some(table.to_internal_table_prost()),
            log_store_type: self.log_store_type as i32,
            rate_limit: self.base.ctx().overwrite_options().sink_rate_limit,
        }))
    }
}

impl ExprRewritable<Stream> for StreamSink {}

impl ExprVisitable for StreamSink {}

#[cfg(test)]
mod test {
    use fixedbitset::FixedBitSet;
    use risingwave_common::catalog::{
        ColumnCatalog, ColumnDesc, ColumnId, ConflictBehavior, Field,
    };
    use risingwave_common::types::{DataType, StructType};
    use risingwave_common::util::iter_util::ZipEqDebug;
    use risingwave_pb::expr::expr_node::Type;

    use super::{IcebergPartitionInfo, *};
    use crate::catalog::table_catalog::TableType;
    use crate::expr::{Expr, ExprImpl};
    use crate::optimizer::plan_node::utils::TableCatalogBuilder;

    fn create_column_catalog() -> Vec<ColumnCatalog> {
        vec![
            ColumnCatalog {
                column_desc: ColumnDesc::named("v1", ColumnId::new(1), DataType::Int32),
                is_hidden: false,
            },
            ColumnCatalog {
                column_desc: ColumnDesc::named("v2", ColumnId::new(2), DataType::Timestamptz),
                is_hidden: false,
            },
            ColumnCatalog {
                column_desc: ColumnDesc::named("v3", ColumnId::new(2), DataType::Timestamp),
                is_hidden: false,
            },
        ]
    }

    fn test_target_table(
        conflict_behavior: ConflictBehavior,
        version_column_indices: Vec<usize>,
    ) -> TableCatalog {
        let mut builder = TableCatalogBuilder::default();
        let col_idx = builder.add_column(&Field::with_name(DataType::Int32, "v1"));
        let mut table = builder.build(vec![], 0);
        table.table_type = TableType::Table;
        table.columns = vec![ColumnCatalog {
            column_desc: ColumnDesc::named("v1", ColumnId::new(col_idx as i32), DataType::Int32),
            is_hidden: false,
        }];
        table.conflict_behavior = conflict_behavior;
        table.version_column_indices = version_column_indices;
        table.watermark_columns = FixedBitSet::with_capacity(table.columns.len());
        table
    }

    #[test]
    fn test_target_table_requires_row_level_conflict_handling() {
        assert!(target_table_requires_row_level_conflict_handling(
            &test_target_table(ConflictBehavior::DoUpdateIfNotNull, vec![])
        ));
        assert!(target_table_requires_row_level_conflict_handling(
            &test_target_table(ConflictBehavior::IgnoreConflict, vec![])
        ));
        assert!(target_table_requires_row_level_conflict_handling(
            &test_target_table(ConflictBehavior::Overwrite, vec![0])
        ));
        assert!(!target_table_requires_row_level_conflict_handling(
            &test_target_table(ConflictBehavior::Overwrite, vec![])
        ));
    }

    #[test]
    fn test_iceberg_convert_to_expression() {
        let partition_type = StructType::new(vec![
            ("f1", DataType::Int32),
            ("f2", DataType::Int32),
            ("f3", DataType::Int32),
            ("f4", DataType::Int32),
            ("f5", DataType::Int32),
            ("f6", DataType::Int32),
            ("f7", DataType::Int32),
            ("f8", DataType::Int32),
            ("f9", DataType::Int32),
        ]);
        let partition_fields = vec![
            ("v1".into(), Transform::Identity),
            ("v1".into(), Transform::Bucket(10)),
            ("v1".into(), Transform::Truncate(3)),
            ("v2".into(), Transform::Year),
            ("v2".into(), Transform::Month),
            ("v3".into(), Transform::Day),
            ("v3".into(), Transform::Hour),
            ("v1".into(), Transform::Void),
            ("v3".into(), Transform::Void),
        ];
        let partition_info = IcebergPartitionInfo {
            partition_type: partition_type.clone(),
            partition_fields: partition_fields.clone(),
        };
        let catalog = create_column_catalog();
        let actual_expr = partition_info.convert_to_expression(&catalog).unwrap();
        let actual_expr = actual_expr.as_function_call().unwrap();

        assert_eq!(
            actual_expr.return_type(),
            DataType::Struct(partition_type.clone())
        );
        assert_eq!(actual_expr.inputs().len(), partition_fields.len());
        assert_eq!(actual_expr.func_type(), Type::Row);

        for ((expr, (_, transform)), (_, expect_type)) in actual_expr
            .inputs()
            .iter()
            .zip_eq_debug(partition_fields.iter())
            .zip_eq_debug(partition_type.iter())
        {
            match transform {
                Transform::Identity => {
                    assert!(expr.is_input_ref());
                    assert_eq!(expr.return_type(), *expect_type);
                }
                Transform::Void => {
                    assert!(expr.is_literal());
                    assert_eq!(expr.return_type(), *expect_type);
                }
                _ => {
                    let expr = expr.as_function_call().unwrap();
                    assert_eq!(expr.func_type(), Type::IcebergTransform);
                    assert_eq!(expr.inputs().len(), 2);
                    assert_eq!(
                        expr.inputs()[0],
                        ExprImpl::literal_varchar(transform.to_string())
                    );
                }
            }
        }
    }
}
