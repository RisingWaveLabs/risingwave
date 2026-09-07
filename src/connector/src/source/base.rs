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

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
use enum_as_inner::EnumAsInner;
use futures::future::try_join_all;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt, TryStreamExt};
use itertools::Itertools;
use risingwave_common::array::StreamChunk;
use risingwave_common::bail;
use risingwave_common::id::{ActorId, FragmentId, SourceId};
use risingwave_common::secret::LocalSecretManager;
use risingwave_common::types::{JsonbVal, Scalar};
use risingwave_pb::catalog::{PbSource, PbStreamSourceInfo};
use risingwave_pb::plan_common::ExternalTableDesc;
use risingwave_pb::source::ConnectorSplit;
use rw_futures_util::select_all;
use serde::de::DeserializeOwned;
use serde_json::json;
use tokio::sync::mpsc;

use super::cdc::DebeziumCdcMeta;
use super::datagen::DatagenMeta;
use super::google_pubsub::GooglePubsubMeta;
use super::kafka::KafkaMeta;
use super::kinesis::KinesisMeta;
use super::monitor::SourceMetrics;
use super::nats::source::NatsMeta;
use super::nexmark::source::message::NexmarkMeta;
use super::pulsar::source::PulsarMeta;
use crate::enforce_secret::EnforceSecret;
use crate::error::ConnectorResult as Result;
use crate::parser::ParserConfig;
use crate::parser::schema_change::SchemaChangeEnvelope;
use crate::source::SplitImpl::{
    CitusCdc, MongodbCdc, MysqlCdc, OracleCdc, PostgresCdc, SqlServerCdc,
};
use crate::source::batch::BatchSourceSplitImpl;
use crate::source::monitor::EnumeratorMetrics;
use crate::with_options::WithOptions;
use crate::{
    WithOptionsSecResolved, WithPropertiesExt, dispatch_source_prop, dispatch_split_impl,
    for_all_connections, for_all_sources, impl_connection, impl_connector_properties, impl_split,
    match_source_name_str,
};

const SPLIT_TYPE_FIELD: &str = "split_type";
const SPLIT_INFO_FIELD: &str = "split_info";
pub const UPSTREAM_SOURCE_KEY: &str = "connector";

pub const WEBHOOK_CONNECTOR: &str = "webhook";

/// Callback wrapper for reporting CDC auto schema change fail events
/// Parameters: (`table_id`, `table_name`, `cdc_table_id`, `upstream_ddl`, `fail_info`)
#[derive(Clone)]
pub struct CdcAutoSchemaChangeFailCallback(
    Arc<dyn Fn(SourceId, String, String, String, String) + Send + Sync>,
);

impl CdcAutoSchemaChangeFailCallback {
    pub fn new<F>(f: F) -> Self
    where
        F: Fn(SourceId, String, String, String, String) + Send + Sync + 'static,
    {
        Self(Arc::new(f))
    }

    pub fn call(
        &self,
        source_id: SourceId,
        table_name: String,
        cdc_table_id: String,
        upstream_ddl: String,
        fail_info: String,
    ) {
        self.0(source_id, table_name, cdc_table_id, upstream_ddl, fail_info);
    }
}

impl std::fmt::Debug for CdcAutoSchemaChangeFailCallback {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CdcAutoSchemaChangeFailCallback")
    }
}
pub trait TryFromBTreeMap: Sized + UnknownFields {
    /// Used to initialize the source properties from the raw untyped `WITH` options.
    fn try_from_btreemap(
        props: BTreeMap<String, String>,
        deny_unknown_fields: bool,
    ) -> Result<Self>;
}

/// Represents `WITH` options for sources.
///
/// Each instance should add a `#[derive(with_options::WithOptions)]` marker.
pub trait SourceProperties:
    TryFromBTreeMap + Clone + WithOptions + std::fmt::Debug + EnforceSecret
{
    const SOURCE_NAME: &'static str;
    type Split: SplitMetaData
        + TryFrom<SplitImpl, Error = crate::error::ConnectorError>
        + Into<SplitImpl>;
    type SplitEnumerator: SplitEnumerator<Properties = Self, Split = Self::Split>;
    type SplitReader: SplitReader<Split = Self::Split, Properties = Self>;

    /// Load additional info from `PbSource`. Currently only used by CDC.
    fn init_from_pb_source(&mut self, _source: &PbSource) {}

    /// Load additional info from `ExternalTableDesc`. Currently only used by CDC.
    fn init_from_pb_cdc_table_desc(&mut self, _table_desc: &ExternalTableDesc) {}
}

pub trait UnknownFields {
    /// Unrecognized fields in the `WITH` clause.
    fn unknown_fields(&self) -> HashMap<String, String>;
}

impl<P: DeserializeOwned + UnknownFields> TryFromBTreeMap for P {
    fn try_from_btreemap(
        props: BTreeMap<String, String>,
        deny_unknown_fields: bool,
    ) -> Result<Self> {
        let json_value = serde_json::to_value(props)?;
        let res = serde_json::from_value::<P>(json_value)?;

        if !deny_unknown_fields || res.unknown_fields().is_empty() {
            Ok(res)
        } else {
            bail!(
                "Unknown fields in the WITH clause: {:?}",
                res.unknown_fields()
            )
        }
    }
}

#[derive(Default)]
pub struct CreateSplitReaderOpt {
    pub support_multiple_splits: bool,
    pub seek_to_latest: bool,
}

#[derive(Default)]
pub struct CreateSplitReaderResult {
    pub latest_splits: Option<Vec<SplitImpl>>,
    pub backfill_info: HashMap<SplitId, BackfillInfo>,
}

pub async fn create_split_readers<P: SourceProperties>(
    prop: P,
    splits: Vec<SplitImpl>,
    parser_config: ParserConfig,
    source_ctx: SourceContextRef,
    columns: Option<Vec<Column>>,
    opt: CreateSplitReaderOpt,
) -> Result<(BoxSourceReaderEventStream, CreateSplitReaderResult)> {
    let splits = splits.into_iter().map(P::Split::try_from).try_collect()?;
    let mut res = CreateSplitReaderResult {
        backfill_info: HashMap::new(),
        latest_splits: None,
    };
    if opt.support_multiple_splits {
        let mut reader = P::SplitReader::new(
            prop.clone(),
            splits,
            parser_config.clone(),
            source_ctx.clone(),
            columns.clone(),
        )
        .await?;
        if opt.seek_to_latest {
            res.latest_splits = Some(reader.seek_to_latest().await?);
        }
        res.backfill_info = reader.backfill_info();
        Ok((reader.into_event_stream().boxed(), res))
    } else {
        let mut readers = try_join_all(splits.into_iter().map(|split| {
            // TODO: is this reader split across multiple threads...? Realistically, we want
            // source_ctx to live in a single actor.
            P::SplitReader::new(
                prop.clone(),
                vec![split],
                parser_config.clone(),
                source_ctx.clone(),
                columns.clone(),
            )
        }))
        .await?;
        if opt.seek_to_latest {
            let mut latest_splits = vec![];
            for reader in &mut readers {
                latest_splits.extend(reader.seek_to_latest().await?);
            }
            res.latest_splits = Some(latest_splits);
        }
        res.backfill_info = readers.iter().flat_map(|r| r.backfill_info()).collect();
        Ok((
            select_all(readers.into_iter().map(|r| r.into_event_stream())).boxed(),
            res,
        ))
    }
}

/// [`SplitEnumerator`] fetches the split metadata from the external source service.
/// NOTE: It runs in the meta server, so probably it should be moved to the `meta` crate.
#[async_trait]
pub trait SplitEnumerator: Sized + Send {
    type Split: SplitMetaData + Send;
    type Properties;

    async fn new(properties: Self::Properties, context: SourceEnumeratorContextRef)
    -> Result<Self>;
    async fn list_splits(&mut self) -> Result<Vec<Self::Split>>;
    /// Do some cleanup work when a fragment is dropped, e.g., drop Kafka consumer group.
    async fn on_drop_fragments(&mut self, _fragment_ids: Vec<FragmentId>) -> Result<()> {
        Ok(())
    }
    /// Do some cleanup work when a backfill fragment is finished, e.g., drop Kafka consumer group.
    async fn on_finish_backfill(&mut self, _fragment_ids: Vec<FragmentId>) -> Result<()> {
        Ok(())
    }
    /// Called after `worker.tick()` execution to perform periodic operations,
    /// such as monitoring upstream PostgreSQL `confirmed_flush_lsn`, etc.
    /// This can be extended to support more periodic operations in the future.
    async fn on_tick(&mut self) -> Result<()> {
        Ok(())
    }
}

pub type SourceContextRef = Arc<SourceContext>;
pub type SourceEnumeratorContextRef = Arc<SourceEnumeratorContext>;

/// Dyn-compatible [`SplitEnumerator`].
#[async_trait]
pub trait AnySplitEnumerator: Send {
    async fn list_splits(&mut self) -> Result<Vec<SplitImpl>>;
    async fn on_drop_fragments(&mut self, fragment_ids: Vec<FragmentId>) -> Result<()>;
    async fn on_finish_backfill(&mut self, fragment_ids: Vec<FragmentId>) -> Result<()>;
    async fn on_tick(&mut self) -> Result<()>;
}

#[async_trait]
impl<T: SplitEnumerator<Split: Into<SplitImpl>> + 'static> AnySplitEnumerator for T {
    async fn list_splits(&mut self) -> Result<Vec<SplitImpl>> {
        SplitEnumerator::list_splits(self)
            .await
            .map(|s| s.into_iter().map(|s| s.into()).collect())
    }

    async fn on_drop_fragments(&mut self, fragment_ids: Vec<FragmentId>) -> Result<()> {
        SplitEnumerator::on_drop_fragments(self, fragment_ids).await
    }

    async fn on_finish_backfill(&mut self, fragment_ids: Vec<FragmentId>) -> Result<()> {
        SplitEnumerator::on_finish_backfill(self, fragment_ids).await
    }

    async fn on_tick(&mut self) -> Result<()> {
        SplitEnumerator::on_tick(self).await
    }
}

/// The max size of a chunk yielded by source stream.
pub const MAX_CHUNK_SIZE: usize = 1024;

#[derive(Debug, Clone, Copy)]
pub struct SourceCtrlOpts {
    /// The max size of a chunk yielded by source stream.
    pub chunk_size: usize,
    /// Whether to allow splitting a transaction into multiple chunks to meet the `max_chunk_size`.
    pub split_txn: bool,
}

// The options in `SourceCtrlOpts` are so important that we don't want to impl `Default` for it,
// so that we can prevent any unintentional use of the default value.
impl !Default for SourceCtrlOpts {}

impl SourceCtrlOpts {
    #[cfg(test)]
    pub fn for_test() -> Self {
        SourceCtrlOpts {
            chunk_size: 256,
            split_txn: false,
        }
    }
}

#[derive(Debug)]
pub struct SourceEnumeratorContext {
    pub info: SourceEnumeratorInfo,
    pub metrics: Arc<EnumeratorMetrics>,
}

impl SourceEnumeratorContext {
    /// Create a dummy `SourceEnumeratorContext` for testing purpose, or for the situation
    /// where the real context doesn't matter.
    pub fn dummy() -> SourceEnumeratorContext {
        SourceEnumeratorContext {
            info: SourceEnumeratorInfo {
                source_id: 0.into(),
            },
            metrics: Arc::new(EnumeratorMetrics::default()),
        }
    }
}

#[derive(Clone, Debug)]
pub struct SourceEnumeratorInfo {
    pub source_id: SourceId,
}

#[derive(Clone, Debug)]
pub struct SourceContext {
    pub actor_id: ActorId,
    pub source_id: SourceId,
    pub fragment_id: FragmentId,
    pub source_name: String,
    pub metrics: Arc<SourceMetrics>,
    pub source_ctrl_opts: SourceCtrlOpts,
    pub connector_props: ConnectorProperties,
    // source parser put schema change event into this channel
    pub schema_change_tx:
        Option<mpsc::Sender<(SchemaChangeEnvelope, tokio::sync::oneshot::Sender<()>)>>,
    // callback function to report CDC auto schema change fail events
    pub on_cdc_auto_schema_change_failure: Option<CdcAutoSchemaChangeFailCallback>,
}

impl SourceContext {
    pub fn new(
        actor_id: ActorId,
        source_id: SourceId,
        fragment_id: FragmentId,
        source_name: String,
        metrics: Arc<SourceMetrics>,
        source_ctrl_opts: SourceCtrlOpts,
        connector_props: ConnectorProperties,
        schema_change_channel: Option<
            mpsc::Sender<(SchemaChangeEnvelope, tokio::sync::oneshot::Sender<()>)>,
        >,
    ) -> Self {
        Self::new_with_auto_schema_change_callback(
            actor_id,
            source_id,
            fragment_id,
            source_name,
            metrics,
            source_ctrl_opts,
            connector_props,
            schema_change_channel,
            None,
        )
    }

    pub fn new_with_auto_schema_change_callback(
        actor_id: ActorId,
        source_id: SourceId,
        fragment_id: FragmentId,
        source_name: String,
        metrics: Arc<SourceMetrics>,
        source_ctrl_opts: SourceCtrlOpts,
        connector_props: ConnectorProperties,
        schema_change_channel: Option<
            mpsc::Sender<(SchemaChangeEnvelope, tokio::sync::oneshot::Sender<()>)>,
        >,
        on_cdc_auto_schema_change_failure: Option<CdcAutoSchemaChangeFailCallback>,
    ) -> Self {
        Self {
            actor_id,
            source_id,
            fragment_id,
            source_name,
            metrics,
            source_ctrl_opts,
            connector_props,
            schema_change_tx: schema_change_channel,
            on_cdc_auto_schema_change_failure,
        }
    }

    /// Create a dummy `SourceContext` for testing purpose, or for the situation
    /// where the real context doesn't matter.
    pub fn dummy() -> Self {
        Self::new(
            0.into(),
            SourceId::new(0),
            0.into(),
            "dummy".to_owned(),
            Arc::new(SourceMetrics::default()),
            SourceCtrlOpts {
                chunk_size: MAX_CHUNK_SIZE,
                split_txn: false,
            },
            ConnectorProperties::default(),
            None,
        )
    }

    /// Report CDC auto schema change fail event
    /// Parameters: (`source_id`, `table_name`, `cdc_table_id`, `upstream_ddl`, `fail_info`)
    pub fn on_cdc_auto_schema_change_failure(
        &self,
        source_id: SourceId,
        table_name: String,
        cdc_table_id: String,
        upstream_ddl: String,
        fail_info: String,
    ) {
        if let Some(ref cdc_auto_schema_change_fail_callback) =
            self.on_cdc_auto_schema_change_failure
        {
            cdc_auto_schema_change_fail_callback.call(
                source_id,
                table_name,
                cdc_table_id,
                upstream_ddl,
                fail_info,
            );
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum SourceFormat {
    #[default]
    Invalid,
    Native,
    None,
    Debezium,
    DebeziumMongo,
    Maxwell,
    Canal,
    Upsert,
    Plain,
}

/// Refer to [`crate::parser::EncodingProperties`]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum SourceEncode {
    #[default]
    Invalid,
    Native,
    None,
    Avro,
    Csv,
    Protobuf,
    Json,
    Bytes,
    Parquet,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct SourceStruct {
    pub format: SourceFormat,
    pub encode: SourceEncode,
}

impl SourceStruct {
    pub fn new(format: SourceFormat, encode: SourceEncode) -> Self {
        Self { format, encode }
    }
}

// Only return valid (format, encode)
pub fn extract_source_struct(info: &PbStreamSourceInfo) -> Result<SourceStruct> {
    use risingwave_pb::plan_common::{PbEncodeType, PbFormatType, RowFormatType};

    // old version meta.
    if let Ok(format) = info.get_row_format() {
        let (format, encode) = match format {
            RowFormatType::Json => (SourceFormat::Plain, SourceEncode::Json),
            RowFormatType::Protobuf => (SourceFormat::Plain, SourceEncode::Protobuf),
            RowFormatType::DebeziumJson => (SourceFormat::Debezium, SourceEncode::Json),
            RowFormatType::Avro => (SourceFormat::Plain, SourceEncode::Avro),
            RowFormatType::Maxwell => (SourceFormat::Maxwell, SourceEncode::Json),
            RowFormatType::CanalJson => (SourceFormat::Canal, SourceEncode::Json),
            RowFormatType::Csv => (SourceFormat::Plain, SourceEncode::Csv),
            RowFormatType::Native => (SourceFormat::Native, SourceEncode::Native),
            RowFormatType::DebeziumAvro => (SourceFormat::Debezium, SourceEncode::Avro),
            RowFormatType::UpsertJson => (SourceFormat::Upsert, SourceEncode::Json),
            RowFormatType::UpsertAvro => (SourceFormat::Upsert, SourceEncode::Avro),
            RowFormatType::DebeziumMongoJson => (SourceFormat::DebeziumMongo, SourceEncode::Json),
            RowFormatType::Bytes => (SourceFormat::Plain, SourceEncode::Bytes),
            RowFormatType::RowUnspecified => unreachable!(),
        };
        return Ok(SourceStruct::new(format, encode));
    }
    let source_format = info.get_format()?;
    let source_encode = info.get_row_encode()?;
    let (format, encode) = match (source_format, source_encode) {
        (PbFormatType::Plain, PbEncodeType::Json) => (SourceFormat::Plain, SourceEncode::Json),
        (PbFormatType::Plain, PbEncodeType::Protobuf) => {
            (SourceFormat::Plain, SourceEncode::Protobuf)
        }
        (PbFormatType::Debezium, PbEncodeType::Json) => {
            (SourceFormat::Debezium, SourceEncode::Json)
        }
        (PbFormatType::Plain, PbEncodeType::Avro) => (SourceFormat::Plain, SourceEncode::Avro),
        (PbFormatType::Maxwell, PbEncodeType::Json) => (SourceFormat::Maxwell, SourceEncode::Json),
        (PbFormatType::Canal, PbEncodeType::Json) => (SourceFormat::Canal, SourceEncode::Json),
        (PbFormatType::Plain, PbEncodeType::Csv) => (SourceFormat::Plain, SourceEncode::Csv),
        (PbFormatType::Plain, PbEncodeType::Parquet) => {
            (SourceFormat::Plain, SourceEncode::Parquet)
        }
        (PbFormatType::Native, PbEncodeType::Native) => {
            (SourceFormat::Native, SourceEncode::Native)
        }
        (PbFormatType::None, PbEncodeType::None) => (SourceFormat::None, SourceEncode::None),
        (PbFormatType::Debezium, PbEncodeType::Avro) => {
            (SourceFormat::Debezium, SourceEncode::Avro)
        }
        (PbFormatType::Upsert, PbEncodeType::Json) => (SourceFormat::Upsert, SourceEncode::Json),
        (PbFormatType::Upsert, PbEncodeType::Avro) => (SourceFormat::Upsert, SourceEncode::Avro),
        (PbFormatType::DebeziumMongo, PbEncodeType::Json) => {
            (SourceFormat::DebeziumMongo, SourceEncode::Json)
        }
        (PbFormatType::Plain, PbEncodeType::Bytes) => (SourceFormat::Plain, SourceEncode::Bytes),
        (PbFormatType::Upsert, PbEncodeType::Protobuf) => {
            (SourceFormat::Upsert, SourceEncode::Protobuf)
        }
        (format, encode) => {
            bail!(
                "Unsupported combination of format {:?} and encode {:?}",
                format,
                encode
            );
        }
    };
    Ok(SourceStruct::new(format, encode))
}

/// Stream of [`SourceMessage`]. Messages flow through the stream in the unit of a batch.
pub type BoxSourceMessageStream =
    BoxStream<'static, crate::error::ConnectorResult<Vec<SourceMessage>>>;
/// Stream of source message events.
pub type BoxSourceMessageEventStream =
    BoxStream<'static, crate::error::ConnectorResult<SourceMessageEvent>>;
/// Stream of [`StreamChunk`]s parsed from the messages from the external source.
pub type BoxSourceChunkStream = BoxStream<'static, crate::error::ConnectorResult<StreamChunk>>;
/// Stream of source reader events.
pub type BoxSourceReaderEventStream =
    BoxStream<'static, crate::error::ConnectorResult<SourceReaderEvent>>;
/// `StreamChunk` with the latest split state.
/// The state is constructed in `StreamReaderBuilder::into_retry_stream`
pub type StreamChunkWithState = (StreamChunk, HashMap<SplitId, SplitImpl>);
/// See [`StreamChunkWithState`].
pub type BoxSourceChunkWithStateStream =
    BoxStream<'static, crate::error::ConnectorResult<StreamChunkWithState>>;

#[derive(Debug)]
pub enum SourceMessageEvent {
    Data(Vec<SourceMessage>),
    SplitProgress(HashMap<SplitId, String>),
}

#[derive(Debug)]
pub enum SourceReaderEvent {
    DataChunk(StreamChunk),
    SplitProgress(HashMap<SplitId, String>),
}

/// Stream of [`Option<StreamChunk>`]s parsed from the messages from the external source.
pub type BoxStreamingFileSourceChunkStream =
    BoxStream<'static, crate::error::ConnectorResult<Option<StreamChunk>>>;

// Manually expand the trait alias to improve IDE experience.
pub trait SourceChunkStream:
    Stream<Item = crate::error::ConnectorResult<StreamChunk>> + Send + 'static
{
}
impl<T> SourceChunkStream for T where
    T: Stream<Item = crate::error::ConnectorResult<StreamChunk>> + Send + 'static
{
}

pub type BoxTryStream<M> = BoxStream<'static, crate::error::ConnectorResult<M>>;

/// [`SplitReader`] is a new abstraction of the external connector read interface which is
/// responsible for parsing, it is used to read messages from the outside and transform them into a
/// stream of parsed [`StreamChunk`]
#[async_trait]
pub trait SplitReader: Sized + Send {
    type Properties;
    type Split: SplitMetaData;

    async fn new(
        properties: Self::Properties,
        state: Vec<Self::Split>,
        parser_config: ParserConfig,
        source_ctx: SourceContextRef,
        columns: Option<Vec<Column>>,
    ) -> crate::error::ConnectorResult<Self>;

    fn into_stream(self) -> BoxSourceChunkStream;

    fn into_event_stream(self) -> BoxSourceReaderEventStream {
        self.into_stream()
            .map_ok(SourceReaderEvent::DataChunk)
            .boxed()
    }

    fn backfill_info(&self) -> HashMap<SplitId, BackfillInfo> {
        HashMap::new()
    }

    async fn seek_to_latest(&mut self) -> Result<Vec<SplitImpl>> {
        Err(anyhow!("seek_to_latest is not supported for this connector").into())
    }
}

/// Information used to determine whether we should start and finish source backfill.
///
/// XXX: if a connector cannot provide the latest offsets (but we want to make it shareable),
/// perhaps we should ban blocking DDL for it.
#[derive(Debug, Clone)]
pub enum BackfillInfo {
    HasDataToBackfill {
        /// The last available offsets for each split (**inclusive**).
        ///
        /// This will be used to determine whether source backfill is finished when
        /// there are no _new_ messages coming from upstream `SourceExecutor`. Otherwise,
        /// blocking DDL cannot finish until new messages come.
        ///
        /// When there are upstream messages, we will use the latest offsets from the upstream.
        latest_offset: String,
    },
    /// If there are no messages in the split at all, we don't need to start backfill.
    /// In this case, there will be no message from the backfill stream too.
    /// If we started backfill, we cannot finish it until new messages come.
    /// So we mark this a special case for optimization.
    NoDataToBackfill,
}

for_all_sources!(impl_connector_properties);

impl Default for ConnectorProperties {
    fn default() -> Self {
        ConnectorProperties::Test(Box::default())
    }
}

impl ConnectorProperties {
    /// Creates typed source properties from the raw `WITH` properties.
    ///
    /// It checks the `connector` field, and them dispatches to the corresponding type's `try_from_btreemap` method.
    ///
    /// `deny_unknown_fields`: Since `WITH` options are persisted in meta, we do not deny unknown fields when restoring from
    /// existing data to avoid breaking backwards compatibility. We only deny unknown fields when creating new sources.
    pub fn extract(
        with_properties: WithOptionsSecResolved,
        deny_unknown_fields: bool,
    ) -> Result<Self> {
        let (options, secret_refs) = with_properties.into_parts();
        let mut options_with_secret =
            LocalSecretManager::global().fill_secrets(options, secret_refs)?;
        let connector = options_with_secret
            .remove(UPSTREAM_SOURCE_KEY)
            .ok_or_else(|| anyhow!("Must specify 'connector' in WITH clause"))?
            .to_lowercase();
        match_source_name_str!(
            connector.as_str(),
            PropType,
            PropType::try_from_btreemap(options_with_secret, deny_unknown_fields)
                .map(ConnectorProperties::from),
            |other| bail!("connector '{}' is not supported", other)
        )
    }

    pub fn enforce_secret_source(
        with_properties: &impl WithPropertiesExt,
    ) -> crate::error::ConnectorResult<()> {
        let connector = with_properties
            .get_connector()
            .ok_or_else(|| anyhow!("Must specify 'connector' in WITH clause"))?
            .to_lowercase();
        let key_iter = with_properties.key_iter();
        match_source_name_str!(
            connector.as_str(),
            PropType,
            PropType::enforce_secret(key_iter),
            |other| bail!("connector '{}' is not supported", other)
        )
    }

    pub fn enable_drop_split(&self) -> bool {
        // enable split scale in just for Kinesis
        matches!(
            self,
            ConnectorProperties::Kinesis(_)
                | ConnectorProperties::Nats(_)
                | ConnectorProperties::GooglePubsub(_)
        )
    }

    /// For most connectors, this should be false. When enabled, RisingWave should not track any progress.
    pub fn enable_adaptive_splits(&self) -> bool {
        matches!(
            self,
            ConnectorProperties::Nats(_) | ConnectorProperties::GooglePubsub(_)
        )
    }

    /// Load additional info from `PbSource`. Currently only used by CDC.
    pub fn init_from_pb_source(&mut self, source: &PbSource) {
        dispatch_source_prop!(self, |prop| prop.init_from_pb_source(source))
    }

    /// Load additional info from `ExternalTableDesc`. Currently only used by CDC.
    pub fn init_from_pb_cdc_table_desc(&mut self, cdc_table_desc: &ExternalTableDesc) {
        dispatch_source_prop!(self, |prop| prop
            .init_from_pb_cdc_table_desc(cdc_table_desc))
    }

    pub fn support_multiple_splits(&self) -> bool {
        matches!(self, ConnectorProperties::Kafka(_))
            || matches!(self, ConnectorProperties::OpendalS3(_))
            || matches!(self, ConnectorProperties::Gcs(_))
            || matches!(self, ConnectorProperties::Azblob(_))
    }

    pub async fn create_split_enumerator(
        self,
        context: crate::source::base::SourceEnumeratorContextRef,
    ) -> crate::error::ConnectorResult<Box<dyn AnySplitEnumerator>> {
        let enumerator: Box<dyn AnySplitEnumerator> = dispatch_source_prop!(self, |prop| Box::new(
            <PropType as SourceProperties>::SplitEnumerator::new(*prop, context).await?
        ));
        Ok(enumerator)
    }

    pub async fn create_split_reader(
        self,
        splits: Vec<SplitImpl>,
        parser_config: ParserConfig,
        source_ctx: SourceContextRef,
        columns: Option<Vec<Column>>,
        mut opt: crate::source::CreateSplitReaderOpt,
    ) -> Result<(
        BoxSourceReaderEventStream,
        crate::source::CreateSplitReaderResult,
    )> {
        opt.support_multiple_splits = self.support_multiple_splits();
        tracing::debug!(
            ?splits,
            support_multiple_splits = opt.support_multiple_splits,
            "spawning connector split reader",
        );

        dispatch_source_prop!(self, |prop| create_split_readers(
            *prop,
            splits,
            parser_config,
            source_ctx,
            columns,
            opt
        )
        .await)
    }
}

for_all_sources!(impl_split);
for_all_connections!(impl_connection);

impl From<&SplitImpl> for ConnectorSplit {
    fn from(split: &SplitImpl) -> Self {
        dispatch_split_impl!(split, |inner| {
            ConnectorSplit {
                split_type: String::from(PropType::SOURCE_NAME),
                encoded_split: inner.encode_to_bytes().to_vec(),
            }
        })
    }
}

impl TryFrom<&ConnectorSplit> for SplitImpl {
    type Error = crate::error::ConnectorError;

    fn try_from(split: &ConnectorSplit) -> std::result::Result<Self, Self::Error> {
        let split_type = split.split_type.to_lowercase();
        match_source_name_str!(
            split_type.as_str(),
            PropType,
            {
                <PropType as SourceProperties>::Split::restore_from_bytes(
                    split.encoded_split.as_ref(),
                )
                .map(Into::into)
            },
            |other| bail!("connector '{}' is not supported", other)
        )
    }
}

impl SplitImpl {
    fn restore_from_json_inner(split_type: &str, value: JsonbVal) -> Result<Self> {
        let split_type = split_type.to_lowercase();
        match_source_name_str!(
            split_type.as_str(),
            PropType,
            <PropType as SourceProperties>::Split::restore_from_json(value).map(Into::into),
            |other| bail!("connector '{}' is not supported", other)
        )
    }

    pub fn is_cdc_split(&self) -> bool {
        matches!(
            self,
            MysqlCdc(_)
                | PostgresCdc(_)
                | MongodbCdc(_)
                | CitusCdc(_)
                | SqlServerCdc(_)
                | OracleCdc(_)
        )
    }

    /// Get the current split offset.
    pub fn get_cdc_split_offset(&self) -> String {
        match self {
            MysqlCdc(split) => split.start_offset().clone().unwrap_or_default(),
            PostgresCdc(split) => split.start_offset().clone().unwrap_or_default(),
            MongodbCdc(split) => split.start_offset().clone().unwrap_or_default(),
            CitusCdc(split) => split.start_offset().clone().unwrap_or_default(),
            SqlServerCdc(split) => split.start_offset().clone().unwrap_or_default(),
            OracleCdc(split) => split.start_offset().clone().unwrap_or_default(),
            _ => unreachable!("get_cdc_split_offset() is only for cdc split"),
        }
    }

    pub fn into_batch_split(self) -> Option<BatchSourceSplitImpl> {
        match self {
            SplitImpl::BatchPosixFs(batch_posix_fs_split) => {
                Some(BatchSourceSplitImpl::BatchPosixFs(batch_posix_fs_split))
            }
            _ => None,
        }
    }
}

impl SplitMetaData for SplitImpl {
    fn id(&self) -> SplitId {
        dispatch_split_impl!(self, |inner| inner.id())
    }

    fn encode_to_json(&self) -> JsonbVal {
        use serde_json::json;
        let inner = self.encode_to_json_inner().take();
        json!({ SPLIT_TYPE_FIELD: self.get_type(), SPLIT_INFO_FIELD: inner}).into()
    }

    fn restore_from_json(value: JsonbVal) -> Result<Self> {
        let mut value = value.take();
        let json_obj = value.as_object_mut().unwrap();
        let split_type = json_obj
            .remove(SPLIT_TYPE_FIELD)
            .unwrap()
            .as_str()
            .unwrap()
            .to_owned();
        let inner_value = json_obj.remove(SPLIT_INFO_FIELD).unwrap();
        Self::restore_from_json_inner(&split_type, inner_value.into())
    }

    fn update_offset(&mut self, last_seen_offset: String) -> Result<()> {
        dispatch_split_impl!(self, |inner| inner.update_offset(last_seen_offset))
    }
}

impl SplitImpl {
    pub fn get_type(&self) -> String {
        dispatch_split_impl!(self, |_inner| PropType::SOURCE_NAME.to_owned())
    }

    pub fn update_in_place(&mut self, last_seen_offset: String) -> Result<()> {
        dispatch_split_impl!(self, |inner| inner.update_offset(last_seen_offset)?);
        Ok(())
    }

    pub fn encode_to_json_inner(&self) -> JsonbVal {
        dispatch_split_impl!(self, |inner| inner.encode_to_json())
    }
}

use risingwave_common::types::DataType;

#[derive(Clone, Debug)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    /// This field is only used by datagen.
    pub is_visible: bool,
}

/// Split id resides in every source message, use `Arc` to avoid copying.
pub type SplitId = Arc<str>;

/// The message pumped from the external source service.
/// The third-party message structs will eventually be transformed into this struct.
#[derive(Debug, Clone)]
pub struct SourceMessage {
    pub key: Option<Vec<u8>>,
    pub payload: Option<Vec<u8>>,
    pub offset: String, // TODO: use `Arc<str>`
    pub split_id: SplitId,
    pub meta: SourceMeta,
}

impl SourceMessage {
    /// Create a dummy `SourceMessage` with all fields unset for testing purposes.
    pub fn dummy() -> Self {
        Self {
            key: None,
            payload: None,
            offset: "".to_owned(),
            split_id: "".into(),
            meta: SourceMeta::Empty,
        }
    }

    /// Check whether the source message is a CDC heartbeat message.
    pub fn is_cdc_heartbeat(&self) -> bool {
        self.key.is_none() && self.payload.is_none()
    }
}

#[derive(Debug, Clone)]
pub enum SourceMeta {
    Kafka(KafkaMeta),
    Kinesis(KinesisMeta),
    Pulsar(PulsarMeta),
    Nexmark(NexmarkMeta),
    GooglePubsub(GooglePubsubMeta),
    Datagen(DatagenMeta),
    DebeziumCdc(DebeziumCdcMeta),
    Nats(NatsMeta),
    // For the source that doesn't have meta data.
    Empty,
}

/// Implement Eq manually to ignore the `meta` field.
impl PartialEq for SourceMessage {
    fn eq(&self, other: &Self) -> bool {
        self.offset == other.offset
            && self.split_id == other.split_id
            && self.payload == other.payload
    }
}
impl Eq for SourceMessage {}

/// The metadata of a split.
pub trait SplitMetaData: Sized {
    fn id(&self) -> SplitId;
    fn encode_to_bytes(&self) -> Bytes {
        self.encode_to_json()
            .as_scalar_ref()
            .value_serialize()
            .into()
    }
    fn restore_from_bytes(bytes: &[u8]) -> Result<Self> {
        Self::restore_from_json(JsonbVal::value_deserialize(bytes).unwrap())
    }

    /// Encode the whole split metadata to a JSON object
    fn encode_to_json(&self) -> JsonbVal;
    fn restore_from_json(value: JsonbVal) -> Result<Self>;
    fn update_offset(&mut self, last_seen_offset: String) -> crate::error::ConnectorResult<()>;
}

/// [`ConnectorState`] maintains the consuming splits' info. In specific split readers,
/// `ConnectorState` cannot be [`None`] and contains one(for mq split readers) or many(for fs
/// split readers) [`SplitImpl`]. If no split is assigned to source executor, `ConnectorState` is
/// [`None`] and the created source stream will be a pending stream.
pub type ConnectorState = Option<Vec<SplitImpl>>;

#[cfg(test)]
mod tests {
    use maplit::*;
    use nexmark::event::EventType;

    use super::*;
    use crate::source::cdc::{DebeziumCdcSplit, Mysql};
    use crate::source::kafka::KafkaSplit;

    #[test]
    fn test_split_impl_get_fn() -> Result<()> {
        let split = KafkaSplit::new(0, Some(0), Some(0), "demo".to_owned());
        let split_impl = SplitImpl::Kafka(split.clone());
        let get_value = split_impl.into_kafka().unwrap();
        println!("{:?}", get_value);
        assert_eq!(split.encode_to_bytes(), get_value.encode_to_bytes());
        assert_eq!(split.encode_to_json(), get_value.encode_to_json());

        Ok(())
    }

    #[test]
    fn test_cdc_split_state() -> Result<()> {
        let offset_str = "{\"sourcePartition\":{\"server\":\"RW_CDC_mydb.products\"},\"sourceOffset\":{\"transaction_id\":null,\"ts_sec\":1670407377,\"file\":\"binlog.000001\",\"pos\":98587,\"row\":2,\"server_id\":1,\"event\":2}}";
        let split = DebeziumCdcSplit::<Mysql>::new(1001, Some(offset_str.to_owned()), None);
        let split_impl = SplitImpl::MysqlCdc(split);
        let encoded_split = split_impl.encode_to_bytes();
        let restored_split_impl = SplitImpl::restore_from_bytes(encoded_split.as_ref())?;
        assert_eq!(
            split_impl.encode_to_bytes(),
            restored_split_impl.encode_to_bytes()
        );
        assert_eq!(
            split_impl.encode_to_json(),
            restored_split_impl.encode_to_json()
        );

        let encoded_split = split_impl.encode_to_json();
        let restored_split_impl = SplitImpl::restore_from_json(encoded_split)?;
        assert_eq!(
            split_impl.encode_to_bytes(),
            restored_split_impl.encode_to_bytes()
        );
        assert_eq!(
            split_impl.encode_to_json(),
            restored_split_impl.encode_to_json()
        );
        Ok(())
    }

    #[test]
    fn test_extract_nexmark_config() {
        let props = convert_args!(btreemap!(
            "connector" => "nexmark",
            "nexmark.table.type" => "Person",
            "nexmark.split.num" => "1",
        ));

        let props =
            ConnectorProperties::extract(WithOptionsSecResolved::without_secrets(props), true)
                .unwrap();

        if let ConnectorProperties::Nexmark(props) = props {
            assert_eq!(props.table_type, Some(EventType::Person));
            assert_eq!(props.split_num, 1);
        } else {
            panic!("extract nexmark config failed");
        }
    }

    #[test]
    fn test_extract_kafka_config() {
        let props = convert_args!(btreemap!(
            "connector" => "kafka",
            "properties.bootstrap.server" => "b1,b2",
            "topic" => "test",
            "scan.startup.mode" => "earliest",
            "broker.rewrite.endpoints" => r#"{"b-1:9092":"dns-1", "b-2:9092":"dns-2"}"#,
        ));

        let props =
            ConnectorProperties::extract(WithOptionsSecResolved::without_secrets(props), true)
                .unwrap();
        if let ConnectorProperties::Kafka(k) = props {
            let btreemap = btreemap! {
                "b-1:9092".to_owned() => "dns-1".to_owned(),
                "b-2:9092".to_owned() => "dns-2".to_owned(),
            };
            assert_eq!(k.privatelink_common.broker_rewrite_map, Some(btreemap));
        } else {
            panic!("extract kafka config failed");
        }
    }

    #[test]
    fn test_extract_cdc_properties() {
        let user_props_mysql = convert_args!(btreemap!(
            "connector" => "mysql-cdc",
            "database.hostname" => "127.0.0.1",
            "database.port" => "3306",
            "database.user" => "root",
            "database.password" => "123456",
            "database.name" => "mydb",
            "table.name" => "products",
        ));

        let user_props_postgres = convert_args!(btreemap!(
            "connector" => "postgres-cdc",
            "database.hostname" => "127.0.0.1",
            "database.port" => "5432",
            "database.user" => "root",
            "database.password" => "654321",
            "schema.name" => "public",
            "database.name" => "mypgdb",
            "table.name" => "orders",
        ));

        let conn_props = ConnectorProperties::extract(
            WithOptionsSecResolved::without_secrets(user_props_mysql),
            true,
        )
        .unwrap();
        if let ConnectorProperties::MysqlCdc(c) = conn_props {
            assert_eq!(c.properties.get("database.hostname").unwrap(), "127.0.0.1");
            assert_eq!(c.properties.get("database.port").unwrap(), "3306");
            assert_eq!(c.properties.get("database.user").unwrap(), "root");
            assert_eq!(c.properties.get("database.password").unwrap(), "123456");
            assert_eq!(c.properties.get("database.name").unwrap(), "mydb");
            assert_eq!(c.properties.get("table.name").unwrap(), "products");
        } else {
            panic!("extract cdc config failed");
        }

        let conn_props = ConnectorProperties::extract(
            WithOptionsSecResolved::without_secrets(user_props_postgres),
            true,
        )
        .unwrap();
        if let ConnectorProperties::PostgresCdc(c) = conn_props {
            assert_eq!(c.properties.get("database.hostname").unwrap(), "127.0.0.1");
            assert_eq!(c.properties.get("database.port").unwrap(), "5432");
            assert_eq!(c.properties.get("database.user").unwrap(), "root");
            assert_eq!(c.properties.get("database.password").unwrap(), "654321");
            assert_eq!(c.properties.get("schema.name").unwrap(), "public");
            assert_eq!(c.properties.get("database.name").unwrap(), "mypgdb");
            assert_eq!(c.properties.get("table.name").unwrap(), "orders");
        } else {
            panic!("extract cdc config failed");
        }
    }
}
