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

use core::num::NonZeroU64;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use deltalake::DeltaTable;
use deltalake::aws::constants::{
    AWS_ACCESS_KEY_ID, AWS_ALLOW_HTTP, AWS_ENDPOINT_URL, AWS_REGION, AWS_S3_ALLOW_UNSAFE_RENAME,
    AWS_SECRET_ACCESS_KEY,
};
use deltalake::kernel::transaction::{CommitBuilder, CommitProperties};
use deltalake::kernel::{Action, Add, DataType as DeltaLakeDataType, PrimitiveType, Transaction};
use deltalake::protocol::{DeltaOperation, SaveMode};
use deltalake::writer::{DeltaWriter, RecordBatchWriter};
use phf::{Set, phf_set};
use risingwave_common::array::StreamChunk;
use risingwave_common::array::arrow::DeltaLakeConvert;
use risingwave_common::bail;
use risingwave_common::catalog::Schema;
use risingwave_common::types::DataType;
use risingwave_common::util::iter_util::ZipEqDebug;
use risingwave_pb::connector_service::SinkMetadata;
use risingwave_pb::connector_service::sink_metadata::Metadata::Serialized;
use risingwave_pb::connector_service::sink_metadata::SerializedMetadata;
use serde::{Deserialize, Serialize};
use serde_with::{DisplayFromStr, serde_as};
use tokio::sync::mpsc::UnboundedSender;
use url::Url;
use with_options::WithOptions;

use crate::connector_common::{AwsAuthProps, IcebergSinkCompactionUpdate};
use crate::enforce_secret::{EnforceSecret, EnforceSecretError};
use crate::sink::coordinate::CoordinatedLogSinker;
use crate::sink::decouple_checkpoint_log_sink::default_commit_checkpoint_interval;
use crate::sink::writer::SinkWriter;
use crate::sink::{
    Result, SINK_TYPE_APPEND_ONLY, SINK_USER_FORCE_APPEND_ONLY_OPTION,
    SinglePhaseCommitCoordinator, Sink, SinkCommitCoordinator, SinkError, SinkParam,
    SinkWriterParam, TwoPhaseCommitCoordinator,
};

pub const DEFAULT_REGION: &str = "us-east-1";
pub const GCS_SERVICE_ACCOUNT: &str = "service_account_key";

pub const DELTALAKE_SINK: &str = "deltalake";

#[serde_as]
#[derive(Deserialize, Debug, Clone, WithOptions)]
pub struct DeltaLakeCommon {
    #[serde(rename = "location")]
    pub location: String,
    #[serde(flatten)]
    pub aws_auth_props: AwsAuthProps,

    #[serde(rename = "gcs.service.account")]
    #[with_option(allow_alter_on_fly)]
    pub gcs_service_account: Option<String>,
    /// Commit every n(>0) checkpoints, default is 10.
    #[serde(default = "default_commit_checkpoint_interval")]
    #[serde_as(as = "DisplayFromStr")]
    #[with_option(allow_alter_on_fly)]
    pub commit_checkpoint_interval: u64,
}

impl EnforceSecret for DeltaLakeCommon {
    const ENFORCE_SECRET_PROPERTIES: Set<&'static str> = phf_set! {
        "gcs.service.account",
    };

    fn enforce_one(prop: &str) -> crate::error::ConnectorResult<()> {
        AwsAuthProps::enforce_one(prop)?;
        if Self::ENFORCE_SECRET_PROPERTIES.contains(prop) {
            return Err(EnforceSecretError {
                key: prop.to_owned(),
            }
            .into());
        }

        Ok(())
    }
}

impl DeltaLakeCommon {
    pub async fn create_deltalake_client(&self) -> Result<DeltaTable> {
        let table = match Self::get_table_url(&self.location)? {
            DeltaTableUrl::S3(s3_path) => {
                let storage_options = self.build_delta_lake_config_for_aws().await?;
                deltalake::aws::register_handlers(None);
                let url = Url::parse(&s3_path).map_err(|e| SinkError::DeltaLake(anyhow!(e)))?;
                deltalake::open_table_with_storage_options(url, storage_options).await?
            }
            DeltaTableUrl::Local(local_path) => {
                let url = Url::parse(&format!("file://{}", local_path))
                    .map_err(|e| SinkError::DeltaLake(anyhow!(e)))?;
                deltalake::open_table(url).await?
            }
            DeltaTableUrl::Gcs(gcs_path) => {
                let mut storage_options = HashMap::new();
                storage_options.insert(
                    GCS_SERVICE_ACCOUNT.to_owned(),
                    self.gcs_service_account.clone().ok_or_else(|| {
                        SinkError::Config(anyhow!(
                            "gcs.service.account is required with Google Cloud Storage (GCS)"
                        ))
                    })?,
                );
                deltalake::gcp::register_handlers(None);
                let url = Url::parse(&gcs_path).map_err(|e| SinkError::DeltaLake(anyhow!(e)))?;
                deltalake::open_table_with_storage_options(url, storage_options).await?
            }
        };
        Ok(table)
    }

    fn get_table_url(path: &str) -> Result<DeltaTableUrl> {
        if path.starts_with("s3://") || path.starts_with("s3a://") {
            Ok(DeltaTableUrl::S3(path.to_owned()))
        } else if path.starts_with("gs://") {
            Ok(DeltaTableUrl::Gcs(path.to_owned()))
        } else if let Some(path) = path.strip_prefix("file://") {
            Ok(DeltaTableUrl::Local(path.to_owned()))
        } else {
            Err(SinkError::DeltaLake(anyhow!(
                "path should start with 's3://','s3a://'(s3) ,gs://(gcs) or file://(local)"
            )))
        }
    }

    async fn build_delta_lake_config_for_aws(&self) -> Result<HashMap<String, String>> {
        let mut storage_options = HashMap::new();
        storage_options.insert(AWS_ALLOW_HTTP.to_owned(), "true".to_owned());
        storage_options.insert(AWS_S3_ALLOW_UNSAFE_RENAME.to_owned(), "true".to_owned());
        let sdk_config = self.aws_auth_props.build_config().await?;
        let credentials = sdk_config
            .credentials_provider()
            .ok_or_else(|| {
                SinkError::Config(anyhow!(
                    "s3.access.key and s3.secret.key is required with aws s3"
                ))
            })?
            .as_ref()
            .provide_credentials()
            .await
            .map_err(|e| SinkError::Config(e.into()))?;
        let region = sdk_config.region();
        let endpoint = sdk_config.endpoint_url();
        storage_options.insert(
            AWS_ACCESS_KEY_ID.to_owned(),
            credentials.access_key_id().to_owned(),
        );
        storage_options.insert(
            AWS_SECRET_ACCESS_KEY.to_owned(),
            credentials.secret_access_key().to_owned(),
        );
        if endpoint.is_none() && region.is_none() {
            return Err(SinkError::Config(anyhow!(
                "s3.endpoint and s3.region need to be filled with at least one"
            )));
        }
        storage_options.insert(
            AWS_REGION.to_owned(),
            region
                .map(|r| r.as_ref().to_owned())
                .unwrap_or_else(|| DEFAULT_REGION.to_owned()),
        );
        if let Some(s3_endpoint) = endpoint {
            storage_options.insert(AWS_ENDPOINT_URL.to_owned(), s3_endpoint.to_owned());
        }
        Ok(storage_options)
    }
}

enum DeltaTableUrl {
    S3(String),
    Local(String),
    Gcs(String),
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct DeltaLakeConfig {
    #[serde(flatten)]
    pub common: DeltaLakeCommon,

    pub r#type: String,

    /// Whether to use the Delta transaction log to deduplicate replayed epoch commits.
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub is_exactly_once: Option<bool>,

    #[serde(flatten)]
    pub unknown_fields: std::collections::HashMap<String, String>,
}

crate::impl_sink_unknown_fields!(DeltaLakeConfig);

impl EnforceSecret for DeltaLakeConfig {
    fn enforce_one(prop: &str) -> crate::error::ConnectorResult<()> {
        DeltaLakeCommon::enforce_one(prop)
    }
}

impl DeltaLakeConfig {
    pub fn from_btreemap(properties: BTreeMap<String, String>) -> Result<Self> {
        let mut config = serde_json::from_value::<DeltaLakeConfig>(
            serde_json::to_value(properties).map_err(|e| SinkError::DeltaLake(e.into()))?,
        )
        .map_err(|e| SinkError::Config(anyhow!(e)))?;
        for key in [
            "aws.credentials.access_key_id",
            "aws.credentials.role.arn",
            "aws.credentials.role.external_id",
            "aws.credentials.secret_access_key",
            "aws.credentials.session_token",
            "aws.endpoint_url",
            "aws.msk.signer_timeout_sec",
            "aws.profile",
            "aws.region",
            "access_key",
            "arn",
            "commit_checkpoint_interval",
            "endpoint",
            "endpoint_url",
            "external_id",
            "gcs.service.account",
            "is_exactly_once",
            "location",
            "profile",
            "region",
            "s3.access.key",
            "s3.endpoint",
            "s3.region",
            "s3.secret.key",
            "secret_key",
            "session_token",
            "type",
        ] {
            config.unknown_fields.remove(key);
        }
        Ok(config)
    }
}

#[derive(Debug)]
pub struct DeltaLakeSink {
    pub config: DeltaLakeConfig,
    param: SinkParam,
}

impl EnforceSecret for DeltaLakeSink {
    fn enforce_secret<'a>(
        prop_iter: impl Iterator<Item = &'a str>,
    ) -> crate::error::ConnectorResult<()> {
        for prop in prop_iter {
            DeltaLakeCommon::enforce_one(prop)?;
        }
        Ok(())
    }
}

impl DeltaLakeSink {
    pub fn new(config: DeltaLakeConfig, param: SinkParam) -> Result<Self> {
        Ok(Self { config, param })
    }
}

fn check_field_type(rw_data_type: &DataType, dl_data_type: &DeltaLakeDataType) -> Result<bool> {
    let result = match rw_data_type {
        DataType::Boolean => {
            matches!(
                dl_data_type,
                DeltaLakeDataType::Primitive(PrimitiveType::Boolean)
            )
        }
        DataType::Int16 => {
            matches!(
                dl_data_type,
                DeltaLakeDataType::Primitive(PrimitiveType::Short)
            )
        }
        DataType::Int32 => {
            matches!(
                dl_data_type,
                DeltaLakeDataType::Primitive(PrimitiveType::Integer)
            )
        }
        DataType::Int64 => {
            matches!(
                dl_data_type,
                DeltaLakeDataType::Primitive(PrimitiveType::Long)
            )
        }
        DataType::Float32 => {
            matches!(
                dl_data_type,
                DeltaLakeDataType::Primitive(PrimitiveType::Float)
            )
        }
        DataType::Float64 => {
            matches!(
                dl_data_type,
                DeltaLakeDataType::Primitive(PrimitiveType::Double)
            )
        }
        DataType::Decimal => {
            matches!(
                dl_data_type,
                DeltaLakeDataType::Primitive(PrimitiveType::Decimal(_))
            )
        }
        DataType::Date => {
            matches!(
                dl_data_type,
                DeltaLakeDataType::Primitive(PrimitiveType::Date)
            )
        }
        DataType::Varchar => {
            matches!(
                dl_data_type,
                DeltaLakeDataType::Primitive(PrimitiveType::String)
            )
        }
        DataType::Timestamptz => {
            matches!(
                dl_data_type,
                DeltaLakeDataType::Primitive(PrimitiveType::Timestamp)
            )
        }
        DataType::Struct(rw_struct) => {
            if let DeltaLakeDataType::Struct(dl_struct) = dl_data_type {
                let mut result = true;
                for ((rw_name, rw_type), dl_field) in
                    rw_struct.iter().zip_eq_debug(dl_struct.fields())
                {
                    result = check_field_type(rw_type, dl_field.data_type())?
                        && result
                        && rw_name.eq(dl_field.name());
                }
                result
            } else {
                false
            }
        }
        DataType::List(rw_list) => {
            if let DeltaLakeDataType::Array(dl_list) = dl_data_type {
                check_field_type(rw_list.elem(), dl_list.element_type())?
            } else {
                false
            }
        }
        _ => {
            return Err(SinkError::DeltaLake(anyhow!(
                "Type {:?} is not supported for DeltaLake sink.",
                rw_data_type.to_owned()
            )));
        }
    };
    Ok(result)
}

impl Sink for DeltaLakeSink {
    type LogSinker = CoordinatedLogSinker<DeltaLakeSinkWriter>;

    const SINK_NAME: &'static str = DELTALAKE_SINK;

    crate::impl_validate_sink_unknown_fields!();

    async fn new_log_sinker(&self, writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        let inner = DeltaLakeSinkWriter::new(
            self.config.clone(),
            self.param.schema().clone(),
            self.param.downstream_pk_or_empty(),
        )
        .await?;

        let commit_checkpoint_interval =
            NonZeroU64::new(self.config.common.commit_checkpoint_interval).expect(
                "commit_checkpoint_interval should be greater than 0, and it should be checked in config validation",
            );

        let writer = CoordinatedLogSinker::new(
            &writer_param,
            self.param.clone(),
            inner,
            commit_checkpoint_interval,
        )
        .await?;

        Ok(writer)
    }

    fn validate_alter_config(config: &BTreeMap<String, String>) -> Result<()> {
        DeltaLakeConfig::from_btreemap(config.clone())?;
        Ok(())
    }

    async fn validate(&self) -> Result<()> {
        if self.config.r#type != SINK_TYPE_APPEND_ONLY
            && self.config.r#type != SINK_USER_FORCE_APPEND_ONLY_OPTION
        {
            return Err(SinkError::Config(anyhow!(
                "only append-only delta lake sink is supported",
            )));
        }
        let table = self.config.common.create_deltalake_client().await?;
        let snapshot = table.snapshot()?;
        let delta_schema = snapshot.schema();
        let deltalake_fields: HashMap<&String, &DeltaLakeDataType> = delta_schema
            .fields()
            .map(|f| (f.name(), f.data_type()))
            .collect();
        if deltalake_fields.len() != self.param.schema().fields().len() {
            return Err(SinkError::DeltaLake(anyhow!(
                "Columns mismatch. RisingWave schema has {} fields, DeltaLake schema has {} fields",
                self.param.schema().fields().len(),
                deltalake_fields.len()
            )));
        }
        for field in self.param.schema().fields() {
            if !deltalake_fields.contains_key(&field.name) {
                return Err(SinkError::DeltaLake(anyhow!(
                    "column {} not found in deltalake table",
                    field.name
                )));
            }
            let deltalake_field_type = deltalake_fields.get(&field.name).ok_or_else(|| {
                SinkError::DeltaLake(anyhow!("cannot find field type for {}", field.name))
            })?;
            if !check_field_type(&field.data_type, deltalake_field_type)? {
                return Err(SinkError::DeltaLake(anyhow!(
                    "column '{}' type mismatch: deltalake type is {:?}, RisingWave type is {:?}",
                    field.name,
                    deltalake_field_type,
                    field.data_type
                )));
            }
        }
        if self.config.common.commit_checkpoint_interval == 0 {
            return Err(SinkError::Config(anyhow!(
                "`commit_checkpoint_interval` must be greater than 0"
            )));
        }
        Ok(())
    }

    fn is_coordinated_sink(&self) -> bool {
        true
    }

    async fn new_coordinator(
        &self,
        _iceberg_compact_stat_sender: Option<UnboundedSender<IcebergSinkCompactionUpdate>>,
    ) -> Result<SinkCommitCoordinator> {
        let coordinator = DeltaLakeSinkCommitter {
            table: self.config.common.create_deltalake_client().await?,
            app_id: format!("risingwave-deltalake-{}", self.param.sink_id),
            exactly_once: self.config.is_exactly_once.unwrap_or_default(),
        };
        if coordinator.exactly_once {
            Ok(SinkCommitCoordinator::TwoPhase(Box::new(coordinator)))
        } else {
            Ok(SinkCommitCoordinator::SinglePhase(Box::new(coordinator)))
        }
    }
}

impl TryFrom<SinkParam> for DeltaLakeSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let config = DeltaLakeConfig::from_btreemap(param.properties.clone())?;
        DeltaLakeSink::new(config, param)
    }
}

pub struct DeltaLakeSinkWriter {
    pub config: DeltaLakeConfig,
    #[expect(dead_code)]
    schema: Schema,
    #[expect(dead_code)]
    pk_indices: Vec<usize>,
    writer: RecordBatchWriter,
    dl_schema: Arc<deltalake::arrow::datatypes::Schema>,
    #[expect(dead_code)]
    dl_table: DeltaTable,
}

impl DeltaLakeSinkWriter {
    pub async fn new(
        config: DeltaLakeConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
    ) -> Result<Self> {
        let dl_table = config.common.create_deltalake_client().await?;
        let writer = RecordBatchWriter::for_table(&dl_table)?;
        let dl_schema = writer.arrow_schema();

        Ok(Self {
            config,
            schema,
            pk_indices,
            writer,
            dl_schema,
            dl_table,
        })
    }

    async fn write(&mut self, chunk: StreamChunk) -> Result<()> {
        let a = DeltaLakeConvert
            .to_record_batch(self.dl_schema.clone(), &chunk)
            .context("convert record batch error")
            .map_err(SinkError::DeltaLake)?;
        self.writer.write(a).await?;
        Ok(())
    }
}

#[async_trait]
impl SinkWriter for DeltaLakeSinkWriter {
    type CommitMetadata = Option<SinkMetadata>;

    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        self.write(chunk).await
    }

    async fn begin_epoch(&mut self, _epoch: u64) -> Result<()> {
        Ok(())
    }

    async fn abort(&mut self) -> Result<()> {
        Ok(())
    }

    async fn barrier(&mut self, is_checkpoint: bool) -> Result<Option<SinkMetadata>> {
        if !is_checkpoint {
            return Ok(None);
        }

        let adds = self.writer.flush().await?;
        Ok(Some(SinkMetadata::try_from(&DeltaLakeWriteResult {
            adds,
        })?))
    }
}

pub struct DeltaLakeSinkCommitter {
    table: DeltaTable,
    app_id: String,
    exactly_once: bool,
}

impl DeltaLakeSinkCommitter {
    fn collect_write_adds(metadata: &[SinkMetadata]) -> Result<Vec<Add>> {
        Ok(metadata
            .iter()
            .map(DeltaLakeWriteResult::try_from)
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flat_map(|v| v.adds.into_iter())
            .collect())
    }

    fn delta_txn_version(epoch: u64) -> Result<i64> {
        Ok(i64::try_from(epoch).context("delta lake epoch exceeds i64 range")?)
    }

    async fn commit_actions(
        &mut self,
        epoch: u64,
        adds: Vec<Add>,
        txn_identity: Option<&DeltaLakeTxnIdentity>,
    ) -> Result<()> {
        if adds.is_empty() {
            return Ok(());
        }

        if let Some(txn_identity) = txn_identity
            && self.is_txn_committed(txn_identity).await?
        {
            tracing::info!(
                "DeltaLake epoch {epoch} already committed for app id {}, txn version {}, skip committing again.",
                txn_identity.app_id,
                txn_identity.version
            );
            return Ok(());
        }

        let write_adds = adds.into_iter().map(Action::Add).collect();

        let partition_cols = self
            .table
            .snapshot()?
            .metadata()
            .partition_columns()
            .to_vec();
        let partition_by = if !partition_cols.is_empty() {
            Some(partition_cols)
        } else {
            None
        };
        let operation = DeltaOperation::Write {
            mode: SaveMode::Append,
            partition_by,
            predicate: None,
        };
        let commit_builder = if let Some(txn_identity) = txn_identity {
            let commit_builder: CommitBuilder = CommitProperties::default()
                .with_application_transaction(Transaction::new(
                    &txn_identity.app_id,
                    txn_identity.version,
                ))
                .into();
            commit_builder
        } else {
            CommitBuilder::default()
        };
        let version = commit_builder
            .with_actions(write_adds)
            .build(
                Some(self.table.snapshot()?),
                self.table.log_store().clone(),
                operation,
            )
            .await?
            .version();
        self.table.update_state().await?;
        tracing::debug!(
            "Succeeded to commit to DeltaLake table in epoch {epoch}, version {version}."
        );
        Ok(())
    }

    async fn is_txn_committed(&mut self, txn_identity: &DeltaLakeTxnIdentity) -> Result<bool> {
        self.table.update_state().await?;
        let log_store = self.table.log_store();
        let committed_version = self
            .table
            .snapshot()?
            .transaction_version(log_store.as_ref(), &txn_identity.app_id)
            .await?;
        Ok(committed_version.is_some_and(|version| version >= txn_identity.version))
    }
}

#[async_trait::async_trait]
impl SinglePhaseCommitCoordinator for DeltaLakeSinkCommitter {
    async fn init(&mut self) -> Result<()> {
        tracing::info!("DeltaLake commit coordinator inited.");
        Ok(())
    }

    async fn commit_data(&mut self, epoch: u64, metadata: Vec<SinkMetadata>) -> Result<()> {
        tracing::debug!("Starting DeltaLake commit in epoch {epoch}.");

        let adds = Self::collect_write_adds(&metadata)?;
        self.commit_actions(epoch, adds, None).await
    }
}

#[async_trait::async_trait]
impl TwoPhaseCommitCoordinator for DeltaLakeSinkCommitter {
    async fn init(&mut self) -> Result<()> {
        tracing::info!("DeltaLake commit coordinator inited.");
        Ok(())
    }

    async fn pre_commit(
        &mut self,
        epoch: u64,
        metadata: Vec<SinkMetadata>,
        _schema_change: Option<risingwave_pb::stream_plan::PbSinkSchemaChange>,
    ) -> Result<Option<Vec<u8>>> {
        tracing::debug!("Starting DeltaLake pre commit in epoch {epoch}.");

        let adds = Self::collect_write_adds(&metadata)?;
        if adds.is_empty() {
            return Ok(None);
        }

        let txn_identity = DeltaLakeTxnIdentity {
            app_id: self.app_id.clone(),
            version: Self::delta_txn_version(epoch)?,
        };
        Ok(Some(
            DeltaLakePreCommitMetadata { adds, txn_identity }.try_into_bytes()?,
        ))
    }

    async fn commit_data(&mut self, epoch: u64, commit_metadata: Vec<u8>) -> Result<()> {
        tracing::debug!("Starting DeltaLake exactly-once commit in epoch {epoch}.");

        if commit_metadata.is_empty() {
            return Ok(());
        }

        let pre_commit_metadata = DeltaLakePreCommitMetadata::try_from_bytes(&commit_metadata)?;
        self.commit_actions(
            epoch,
            pre_commit_metadata.adds,
            Some(&pre_commit_metadata.txn_identity),
        )
        .await
    }

    async fn abort(&mut self, epoch: u64, _commit_metadata: Vec<u8>) {
        tracing::debug!("Abort not implemented yet for DeltaLake epoch {epoch}");
    }
}

#[derive(Serialize, Deserialize)]
struct DeltaLakeWriteResult {
    adds: Vec<Add>,
}

#[derive(Serialize, Deserialize)]
struct DeltaLakePreCommitMetadata {
    adds: Vec<Add>,
    txn_identity: DeltaLakeTxnIdentity,
}

#[derive(Serialize, Deserialize)]
struct DeltaLakeTxnIdentity {
    app_id: String,
    version: i64,
}

impl DeltaLakePreCommitMetadata {
    fn try_into_bytes(self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(&self).context("cannot serialize deltalake pre commit metadata")?)
    }

    fn try_from_bytes(value: &[u8]) -> Result<Self> {
        Ok(serde_json::from_slice(value)
            .context("cannot deserialize deltalake pre commit metadata")?)
    }
}

impl<'a> TryFrom<&'a DeltaLakeWriteResult> for SinkMetadata {
    type Error = SinkError;

    fn try_from(value: &'a DeltaLakeWriteResult) -> std::result::Result<Self, Self::Error> {
        let metadata =
            serde_json::to_vec(&value.adds).context("cannot serialize deltalake sink metadata")?;
        Ok(SinkMetadata {
            metadata: Some(Serialized(SerializedMetadata { metadata })),
        })
    }
}

impl DeltaLakeWriteResult {
    fn try_from(value: &SinkMetadata) -> Result<Self> {
        if let Some(Serialized(v)) = &value.metadata {
            let adds = serde_json::from_slice::<Vec<Add>>(&v.metadata)
                .context("Can't deserialize deltalake sink metadata")?;
            Ok(DeltaLakeWriteResult { adds })
        } else {
            bail!("Can't create deltalake sink write result from empty data!")
        }
    }
}

impl From<::deltalake::DeltaTableError> for SinkError {
    fn from(value: ::deltalake::DeltaTableError) -> Self {
        SinkError::DeltaLake(anyhow!(value))
    }
}

#[cfg(all(test, not(madsim)))]
mod tests {
    use deltalake::kernel::DataType as SchemaDataType;
    use deltalake::operations::create::CreateBuilder;
    use maplit::btreemap;
    use risingwave_common::array::{Array, I32Array, Op, StreamChunk, Utf8Array};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;

    use super::{DeltaLakeConfig, DeltaLakeSinkCommitter, DeltaLakeSinkWriter};
    use crate::sink::writer::SinkWriter;
    use crate::sink::{SinglePhaseCommitCoordinator, TwoPhaseCommitCoordinator};

    #[tokio::test]
    async fn test_deltalake() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        CreateBuilder::new()
            .with_location(path)
            .with_column(
                "id",
                SchemaDataType::Primitive(deltalake::kernel::PrimitiveType::Integer),
                false,
                Default::default(),
            )
            .with_column(
                "name",
                SchemaDataType::Primitive(deltalake::kernel::PrimitiveType::String),
                false,
                Default::default(),
            )
            .await
            .unwrap();

        let properties = btreemap! {
            "connector".to_owned() => "deltalake".to_owned(),
            "force_append_only".to_owned() => "true".to_owned(),
            "type".to_owned() => "append-only".to_owned(),
            "location".to_owned() => format!("file://{}", path),
        };

        let schema = Schema::new(vec![
            Field {
                data_type: DataType::Int32,
                name: "id".into(),
            },
            Field {
                data_type: DataType::Varchar,
                name: "name".into(),
            },
        ]);

        let deltalake_config = DeltaLakeConfig::from_btreemap(properties).unwrap();
        let deltalake_table = deltalake_config
            .common
            .create_deltalake_client()
            .await
            .unwrap();

        let mut deltalake_writer = DeltaLakeSinkWriter::new(deltalake_config, schema, vec![0])
            .await
            .unwrap();
        let chunk = StreamChunk::new(
            vec![Op::Insert, Op::Insert, Op::Insert],
            vec![
                I32Array::from_iter(vec![1, 2, 3]).into_ref(),
                Utf8Array::from_iter(vec!["Alice", "Bob", "Clare"]).into_ref(),
            ],
        );
        deltalake_writer.write(chunk).await.unwrap();
        let mut committer = DeltaLakeSinkCommitter {
            table: deltalake_table,
            app_id: "test-single-phase".to_owned(),
            exactly_once: false,
        };
        let metadata = deltalake_writer.barrier(true).await.unwrap().unwrap();
        SinglePhaseCommitCoordinator::commit_data(&mut committer, 1, vec![metadata])
            .await
            .unwrap();
        let snapshot = committer.table.snapshot().unwrap();
        assert_eq!(1, snapshot.log_data().num_files());
    }

    #[tokio::test]
    async fn test_deltalake_exactly_once() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        CreateBuilder::new()
            .with_location(path)
            .with_column(
                "id",
                SchemaDataType::Primitive(deltalake::kernel::PrimitiveType::Integer),
                false,
                Default::default(),
            )
            .with_column(
                "name",
                SchemaDataType::Primitive(deltalake::kernel::PrimitiveType::String),
                false,
                Default::default(),
            )
            .await
            .unwrap();

        let properties = btreemap! {
            "connector".to_owned() => "deltalake".to_owned(),
            "force_append_only".to_owned() => "true".to_owned(),
            "type".to_owned() => "append-only".to_owned(),
            "location".to_owned() => format!("file://{}", path),
            "is_exactly_once".to_owned() => "true".to_owned(),
        };

        let schema = Schema::new(vec![
            Field {
                data_type: DataType::Int32,
                name: "id".into(),
            },
            Field {
                data_type: DataType::Varchar,
                name: "name".into(),
            },
        ]);

        let deltalake_config = DeltaLakeConfig::from_btreemap(properties).unwrap();
        let deltalake_table = deltalake_config
            .common
            .create_deltalake_client()
            .await
            .unwrap();

        let mut deltalake_writer = DeltaLakeSinkWriter::new(deltalake_config, schema, vec![0])
            .await
            .unwrap();
        let chunk = StreamChunk::new(
            vec![Op::Insert, Op::Insert, Op::Insert],
            vec![
                I32Array::from_iter(vec![1, 2, 3]).into_ref(),
                Utf8Array::from_iter(vec!["Alice", "Bob", "Clare"]).into_ref(),
            ],
        );
        deltalake_writer.write(chunk).await.unwrap();

        let mut committer = DeltaLakeSinkCommitter {
            table: deltalake_table,
            app_id: "test-exactly-once".to_owned(),
            exactly_once: true,
        };
        let metadata = deltalake_writer.barrier(true).await.unwrap().unwrap();
        let pre_commit_metadata =
            TwoPhaseCommitCoordinator::pre_commit(&mut committer, 1, vec![metadata], None)
                .await
                .unwrap()
                .unwrap();

        TwoPhaseCommitCoordinator::commit_data(&mut committer, 1, pre_commit_metadata.clone())
            .await
            .unwrap();
        assert_eq!(committer.table.version(), Some(1));
        assert_eq!(
            committer.table.snapshot().unwrap().log_data().num_files(),
            1
        );

        let log_store = committer.table.log_store();
        let txn_version = committer
            .table
            .snapshot()
            .unwrap()
            .transaction_version(log_store.as_ref(), &committer.app_id)
            .await
            .unwrap();
        assert_eq!(txn_version, Some(1));

        TwoPhaseCommitCoordinator::commit_data(&mut committer, 1, pre_commit_metadata)
            .await
            .unwrap();
        assert_eq!(committer.table.version(), Some(1));
        assert_eq!(
            committer.table.snapshot().unwrap().log_data().num_files(),
            1
        );
    }
}
