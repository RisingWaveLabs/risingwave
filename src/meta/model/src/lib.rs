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

use std::collections::BTreeMap;

pub use risingwave_common::id::*;
use risingwave_pb::catalog::{PbCreateType, PbStreamJobStatus};
use risingwave_pb::meta::table_fragments::PbState as PbStreamJobState;
use risingwave_pb::secret::PbSecretRef;
use risingwave_pb::stream_plan::{PbDispatcherType, PbStreamNode};
use sea_orm::entity::prelude::*;
use sea_orm::{DeriveActiveEnum, EnumIter, FromJsonQueryResult, Value};
use serde::{Deserialize, Serialize};

pub mod prelude;

pub mod catalog_version;
pub mod cdc_table_snapshot_split;
pub mod cluster;
pub mod compaction_config;
pub mod compaction_status;
pub mod compaction_task;
pub mod connection;
pub mod database;
pub mod exactly_once_iceberg_sink;
pub mod fragment;
pub mod fragment_relation;
pub mod fragment_splits;
pub mod function;
pub mod hummock_epoch_to_version;
pub mod hummock_gc_history;
pub mod hummock_pinned_snapshot;
pub mod hummock_pinned_version;
pub mod hummock_sequence;
pub mod hummock_sstable_info;
pub mod hummock_table_change_log;
pub mod hummock_time_travel_delta;
pub mod hummock_time_travel_version;
pub mod hummock_version_delta;
pub mod hummock_version_stats;
pub mod iceberg_namespace_properties;
pub mod iceberg_tables;
pub mod index;
pub mod object;
pub mod object_dependency;
pub mod pending_sink_state;
pub mod refresh_job;
pub mod schema;
pub mod secret;
pub mod serde_seaql_migration;
pub mod session_parameter;
pub mod sink;
pub mod source;
pub mod streaming_job;
pub mod subscription;
pub mod system_parameter;
pub mod table;
pub mod user;
pub mod user_default_privilege;
pub mod user_privilege;
pub mod view;
pub mod worker;
pub mod worker_property;

#[macro_export]
macro_rules! for_all_meta_model_entities {
    ($macro:ident) => {
        $macro! {
            catalog_version,
            cdc_table_snapshot_split,
            cluster,
            compaction_config,
            compaction_status,
            compaction_task,
            connection,
            database,
            exactly_once_iceberg_sink,
            fragment,
            fragment_relation,
            fragment_splits,
            function,
            hummock_epoch_to_version,
            hummock_gc_history,
            hummock_pinned_snapshot,
            hummock_pinned_version,
            hummock_sequence,
            hummock_sstable_info,
            hummock_table_change_log,
            hummock_time_travel_delta,
            hummock_time_travel_version,
            hummock_version_delta,
            hummock_version_stats,
            iceberg_namespace_properties,
            iceberg_tables,
            index,
            object,
            object_dependency,
            pending_sink_state,
            refresh_job,
            schema,
            secret,
            serde_seaql_migration,
            session_parameter,
            sink,
            source,
            streaming_job,
            subscription,
            system_parameter,
            table,
            user,
            user_default_privilege,
            user_privilege,
            view,
            worker,
            worker_property
        }
    };
}

pub type TransactionId = i32;

pub type PrivilegeId = i32;
pub type DefaultPrivilegeId = i32;

pub use risingwave_pb::id::{CompactionGroupId, HummockSstableObjectId, HummockVersionId};
pub type Epoch = i64;
pub type CompactionTaskId = i64;

#[derive(Clone, Copy, Debug, PartialEq, Eq, EnumIter, DeriveActiveEnum, Serialize, Deserialize)]
#[sea_orm(rs_type = "String", db_type = "string(None)")]
pub enum JobStatus {
    #[sea_orm(string_value = "INITIAL")]
    Initial,
    #[sea_orm(string_value = "CREATING")]
    Creating,
    #[sea_orm(string_value = "CREATED")]
    Created,
}

impl From<JobStatus> for PbStreamJobStatus {
    fn from(job_status: JobStatus) -> Self {
        match job_status {
            JobStatus::Initial => Self::Unspecified,
            JobStatus::Creating => Self::Creating,
            JobStatus::Created => Self::Created,
        }
    }
}

// todo: deprecate job status in catalog and unify with this one.
impl From<JobStatus> for PbStreamJobState {
    fn from(status: JobStatus) -> Self {
        match status {
            JobStatus::Initial => PbStreamJobState::Initial,
            JobStatus::Creating => PbStreamJobState::Creating,
            JobStatus::Created => PbStreamJobState::Created,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, EnumIter, DeriveActiveEnum, Serialize, Deserialize)]
#[sea_orm(rs_type = "String", db_type = "string(None)")]
pub enum CreateType {
    #[sea_orm(string_value = "BACKGROUND")]
    Background,
    #[sea_orm(string_value = "FOREGROUND")]
    Foreground,
}

impl From<CreateType> for PbCreateType {
    fn from(create_type: CreateType) -> Self {
        match create_type {
            CreateType::Background => Self::Background,
            CreateType::Foreground => Self::Foreground,
        }
    }
}

impl From<PbCreateType> for CreateType {
    fn from(create_type: PbCreateType) -> Self {
        match create_type {
            PbCreateType::Background => Self::Background,
            PbCreateType::Foreground => Self::Foreground,
            PbCreateType::Unspecified => unreachable!("Unspecified create type"),
        }
    }
}

impl CreateType {
    pub fn as_str(&self) -> &'static str {
        match self {
            CreateType::Background => "BACKGROUND",
            CreateType::Foreground => "FOREGROUND",
        }
    }
}

/// Defines struct with a single pb field that derives `FromJsonQueryResult`, it will helps to map json value stored in database to Pb struct.
#[macro_export]
macro_rules! derive_from_json_struct {
    ($struct_name:ident, $field_type:ty) => {
        #[derive(Clone, Debug, PartialEq, FromJsonQueryResult, Serialize, Deserialize, Default)]
        pub struct $struct_name(pub $field_type);
        impl Eq for $struct_name {}
        impl From<$field_type> for $struct_name {
            fn from(value: $field_type) -> Self {
                Self(value)
            }
        }

        impl $struct_name {
            pub fn into_inner(self) -> $field_type {
                self.0
            }

            pub fn inner_ref(&self) -> &$field_type {
                &self.0
            }
        }
    };
}

/// Defines struct with a byte array that derives `DeriveValueType`, it will helps to map blob stored in database to Pb struct.
macro_rules! derive_from_blob {
    ($struct_name:ident, $field_type:ty) => {
        #[derive(Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, DeriveValueType)]
        pub struct $struct_name(#[sea_orm] Vec<u8>);

        impl $struct_name {
            pub fn to_protobuf(&self) -> $field_type {
                prost::Message::decode(self.0.as_slice()).unwrap()
            }

            fn from_protobuf(val: &$field_type) -> Self {
                Self(prost::Message::encode_to_vec(val))
            }
        }

        impl sea_orm::sea_query::Nullable for $struct_name {
            fn null() -> Value {
                Value::Bytes(None)
            }
        }

        impl From<&$field_type> for $struct_name {
            fn from(value: &$field_type) -> Self {
                Self::from_protobuf(value)
            }
        }

        impl std::fmt::Debug for $struct_name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.to_protobuf().fmt(f)
            }
        }

        impl Default for $struct_name {
            fn default() -> Self {
                Self::from_protobuf(&<$field_type>::default())
            }
        }
    };
}

/// Defines struct with a byte array that derives `DeriveValueType`, it will helps to map blob stored in database to Pb struct array.
macro_rules! derive_array_from_blob {
    ($struct_name:ident, $field_type:ty, $field_array_name:ident) => {
        #[derive(Clone, PartialEq, Eq, DeriveValueType, serde::Deserialize, serde::Serialize)]
        pub struct $struct_name(#[sea_orm] Vec<u8>);

        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct $field_array_name {
            #[prost(message, repeated, tag = "1")]
            inner: Vec<$field_type>,
        }
        impl Eq for $field_array_name {}

        impl $struct_name {
            pub fn to_protobuf(&self) -> Vec<$field_type> {
                let data: $field_array_name = prost::Message::decode(self.0.as_slice()).unwrap();
                data.inner
            }

            fn from_protobuf(val: Vec<$field_type>) -> Self {
                Self(prost::Message::encode_to_vec(&$field_array_name {
                    inner: val,
                }))
            }
        }

        impl From<Vec<$field_type>> for $struct_name {
            fn from(value: Vec<$field_type>) -> Self {
                Self::from_protobuf(value)
            }
        }

        impl std::fmt::Debug for $struct_name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.to_protobuf().fmt(f)
            }
        }

        impl Default for $struct_name {
            fn default() -> Self {
                Self(vec![])
            }
        }

        impl sea_orm::sea_query::Nullable for $struct_name {
            fn null() -> Value {
                Value::Bytes(None)
            }
        }
    };
}

macro_rules! derive_btreemap_from_blob {
    ($struct_name:ident, $key_type:ty, $value_type:ty, $field_type:ident) => {
        #[derive(Clone, PartialEq, Eq, DeriveValueType, serde::Deserialize, serde::Serialize)]
        pub struct $struct_name(#[sea_orm] Vec<u8>);

        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct $field_type {
            #[prost(btree_map = "string, message")]
            inner: BTreeMap<$key_type, $value_type>,
        }
        impl Eq for $field_type {}

        impl $struct_name {
            pub fn to_protobuf(&self) -> BTreeMap<$key_type, $value_type> {
                let data: $field_type = prost::Message::decode(self.0.as_slice()).unwrap();
                data.inner
            }

            fn from_protobuf(val: BTreeMap<$key_type, $value_type>) -> Self {
                Self(prost::Message::encode_to_vec(&$field_type { inner: val }))
            }
        }

        impl From<BTreeMap<$key_type, $value_type>> for $struct_name {
            fn from(value: BTreeMap<$key_type, $value_type>) -> Self {
                Self::from_protobuf(value)
            }
        }

        impl std::fmt::Debug for $struct_name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.to_protobuf().fmt(f)
            }
        }

        impl Default for $struct_name {
            fn default() -> Self {
                Self(vec![])
            }
        }

        impl sea_orm::sea_query::Nullable for $struct_name {
            fn null() -> Value {
                Value::Bytes(None)
            }
        }
    };
}

pub(crate) use derive_array_from_blob;
pub(crate) use derive_from_blob;

derive_from_json_struct!(TableIdArray, Vec<TableId>);

derive_from_json_struct!(EpochArray, Vec<Epoch>);

derive_from_json_struct!(I32Array, Vec<i32>);

impl From<Vec<u32>> for I32Array {
    fn from(value: Vec<u32>) -> Self {
        Self(value.into_iter().map(|id| id as _).collect())
    }
}

impl I32Array {
    pub fn into_u32_array(self) -> Vec<u32> {
        self.0.into_iter().map(|id| id as _).collect()
    }
}

derive_btreemap_from_blob!(SecretRef, String, PbSecretRef, PbSecretRefMap);

derive_from_blob!(StreamNode, PbStreamNode);
derive_from_blob!(DataType, risingwave_pb::data::PbDataType);
derive_array_from_blob!(
    DataTypeArray,
    risingwave_pb::data::PbDataType,
    PbDataTypeArray
);
derive_array_from_blob!(
    FieldArray,
    risingwave_pb::plan_common::PbField,
    PbFieldArray
);
derive_from_json_struct!(Property, BTreeMap<String, String>);
derive_from_blob!(ColumnCatalog, risingwave_pb::plan_common::PbColumnCatalog);
derive_array_from_blob!(
    ColumnCatalogArray,
    risingwave_pb::plan_common::PbColumnCatalog,
    PbColumnCatalogArray
);
derive_from_blob!(StreamSourceInfo, risingwave_pb::catalog::PbStreamSourceInfo);
derive_from_blob!(
    WebhookSourceInfo,
    risingwave_pb::catalog::PbWebhookSourceInfo
);
derive_from_blob!(WatermarkDesc, risingwave_pb::catalog::PbWatermarkDesc);
derive_array_from_blob!(
    WatermarkDescArray,
    risingwave_pb::catalog::PbWatermarkDesc,
    PbWatermarkDescArray
);
derive_array_from_blob!(
    ExprNodeArray,
    risingwave_pb::expr::PbExprNode,
    PbExprNodeArray
);
derive_array_from_blob!(
    ColumnOrderArray,
    risingwave_pb::common::PbColumnOrder,
    PbColumnOrderArray
);
derive_array_from_blob!(
    IndexColumnPropertiesArray,
    risingwave_pb::catalog::PbIndexColumnProperties,
    PbIndexColumnPropertiesArray
);
derive_from_blob!(SinkFormatDesc, risingwave_pb::catalog::PbSinkFormatDesc);
derive_from_blob!(Cardinality, risingwave_pb::plan_common::PbCardinality);
derive_from_blob!(TableVersion, risingwave_pb::catalog::table::PbTableVersion);
derive_from_blob!(
    PrivateLinkService,
    risingwave_pb::catalog::connection::PbPrivateLinkService
);
derive_from_blob!(ConnectionParams, risingwave_pb::catalog::ConnectionParams);
derive_from_blob!(AuthInfo, risingwave_pb::user::PbAuthInfo);
derive_from_blob!(
    WorkerResource,
    risingwave_pb::common::worker_node::PbResource
);

derive_from_blob!(ConnectorSplits, risingwave_pb::source::ConnectorSplits);
derive_from_blob!(VnodeBitmap, risingwave_pb::common::Buffer);
derive_from_blob!(ActorMapping, risingwave_pb::stream_plan::PbActorMapping);
derive_from_blob!(ExprContext, risingwave_pb::plan_common::PbExprContext);
derive_from_blob!(
    SourceRefreshMode,
    risingwave_pb::plan_common::PbSourceRefreshMode
);

derive_from_blob!(
    SinkSchemachange,
    risingwave_pb::stream_plan::PbSinkSchemaChange
);

derive_array_from_blob!(
    TypePairArray,
    risingwave_pb::stream_plan::dispatch_output_mapping::TypePair,
    PbTypePairArray
);

derive_array_from_blob!(
    HummockVersionDeltaArray,
    risingwave_pb::hummock::PbHummockVersionDelta,
    PbHummockVersionDeltaArray
);

derive_array_from_blob!(
    SstableInfoArray,
    risingwave_pb::hummock::PbSstableInfo,
    PbSstableInfoArray
);

#[derive(Clone, Debug, PartialEq, FromJsonQueryResult, Serialize, Deserialize)]
pub enum StreamingParallelism {
    Adaptive,
    Fixed(usize),
    Custom,
}

impl Eq for StreamingParallelism {}

#[derive(
    Hash, Copy, Clone, Debug, PartialEq, Eq, EnumIter, DeriveActiveEnum, Serialize, Deserialize,
)]
#[sea_orm(rs_type = "String", db_type = "string(None)")]
pub enum DispatcherType {
    #[sea_orm(string_value = "HASH")]
    Hash,
    #[sea_orm(string_value = "BROADCAST")]
    Broadcast,
    #[sea_orm(string_value = "SIMPLE")]
    Simple,
    #[sea_orm(string_value = "NO_SHUFFLE")]
    NoShuffle,
}

impl From<PbDispatcherType> for DispatcherType {
    fn from(val: PbDispatcherType) -> Self {
        match val {
            PbDispatcherType::Unspecified => unreachable!(),
            PbDispatcherType::Hash => DispatcherType::Hash,
            PbDispatcherType::Broadcast => DispatcherType::Broadcast,
            PbDispatcherType::Simple => DispatcherType::Simple,
            PbDispatcherType::NoShuffle => DispatcherType::NoShuffle,
        }
    }
}

impl From<DispatcherType> for PbDispatcherType {
    fn from(val: DispatcherType) -> Self {
        match val {
            DispatcherType::Hash => PbDispatcherType::Hash,
            DispatcherType::Broadcast => PbDispatcherType::Broadcast,
            DispatcherType::Simple => PbDispatcherType::Simple,
            DispatcherType::NoShuffle => PbDispatcherType::NoShuffle,
        }
    }
}
