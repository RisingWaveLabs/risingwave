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
use std::ops::Deref;
use std::sync::LazyLock;

use anyhow::anyhow;
use futures::TryFutureExt;
use futures::future::{TryJoinAll, try_join_all};
use futures::prelude::TryFuture;
use itertools::Itertools;
use mongodb::bson::{Array, Bson, Document, bson, doc};
use mongodb::{Client, Namespace};
use risingwave_common::array::{Op, RowRef, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::log::LogSuppressor;
use risingwave_common::row::Row;
use risingwave_common::types::ScalarRefImpl;
use serde::Deserialize;
use serde_with::{DisplayFromStr, serde_as};
use thiserror_ext::AsReport;
use with_options::WithOptions;

use super::encoder::BsonEncoder;
use super::log_store::DeliveryFutureManagerAddFuture;
use super::writer::{
    AsyncTruncateLogSinkerOf, AsyncTruncateSinkWriter, AsyncTruncateSinkWriterExt,
};
use crate::connector_common::MongodbCommon;
use crate::deserialize_bool_from_string;
use crate::enforce_secret::EnforceSecret;
use crate::sink::encoder::RowEncoder;
use crate::sink::{
    Result, SINK_TYPE_APPEND_ONLY, SINK_TYPE_OPTION, SINK_TYPE_UPSERT, Sink, SinkError, SinkParam,
    SinkWriterParam,
};

mod send_bulk_write_command_future {
    use core::future::Future;

    use anyhow::anyhow;
    use mongodb::Database;
    use mongodb::bson::Document;

    use crate::sink::{Result, SinkError};

    pub(super) type SendBulkWriteCommandFuture = impl Future<Output = Result<()>> + 'static;

    #[define_opaque(SendBulkWriteCommandFuture)]
    pub(super) fn send_bulk_write_commands(
        db: Database,
        upsert: Option<Document>,
        delete: Option<Document>,
    ) -> SendBulkWriteCommandFuture {
        async move {
            if let Some(upsert) = upsert {
                send_bulk_write_command(db.clone(), upsert).await?;
            }
            if let Some(delete) = delete {
                send_bulk_write_command(db, delete).await?;
            }
            Ok(())
        }
    }

    async fn send_bulk_write_command(db: Database, command: Document) -> Result<()> {
        let result = db.run_command(command).await.map_err(|err| {
            SinkError::Mongodb(anyhow!(err).context(format!(
                "sending bulk write command failed, database: {}",
                db.name()
            )))
        })?;

        if let Ok(ok) = result.get_i32("ok")
            && ok != 1
        {
            return Err(SinkError::Mongodb(anyhow!("bulk write write errors")));
        }

        if let Ok(write_errors) = result.get_array("writeErrors") {
            return Err(SinkError::Mongodb(anyhow!(
                "bulk write respond with write errors: {:?}",
                write_errors,
            )));
        }

        if let Ok(write_concern_error) = result.get_array("writeConcernError") {
            return Err(SinkError::Mongodb(anyhow!(
                "bulk write respond with write errors: {:?}",
                write_concern_error,
            )));
        }

        Ok(())
    }
}

pub const MONGODB_SINK: &str = "mongodb";
const MONGODB_SEND_FUTURE_BUFFER_MAX_SIZE: usize = 4096;

pub const MONGODB_PK_NAME: &str = "_id";

static LOG_SUPPRESSOR: LazyLock<LogSuppressor> = LazyLock::new(LogSuppressor::default);

const fn _default_bulk_write_max_entries() -> usize {
    1024
}
#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct MongodbConfig {
    #[serde(flatten)]
    pub common: MongodbCommon,

    pub r#type: String, // accept "append-only" or "upsert"

    /// The dynamic collection name where data should be sunk to. If specified, the field value will be used
    /// as the collection name. The collection name format is same as `collection.name`. If the field value is
    /// null or an empty string, then the `collection.name` will be used as a fallback destination.
    #[serde(rename = "collection.name.field")]
    pub collection_name_field: Option<String>,

    /// Controls whether the field value of `collection.name.field` should be dropped when sinking.
    /// Set this option to true to avoid the duplicate values of `collection.name.field` being written to the
    /// result collection.
    #[serde(
        default,
        deserialize_with = "deserialize_bool_from_string",
        rename = "collection.name.field.drop"
    )]
    pub drop_collection_name_field: bool,

    /// The maximum entries will accumulate before performing the bulk write, defaults to 1024.
    #[serde(
        rename = "mongodb.bulk_write.max_entries",
        default = "_default_bulk_write_max_entries"
    )]
    #[serde_as(as = "DisplayFromStr")]
    #[deprecated]
    pub bulk_write_max_entries: usize,

    #[serde(flatten)]
    pub unknown_fields: std::collections::HashMap<String, String>,
}

crate::impl_sink_unknown_fields!(MongodbConfig);

impl EnforceSecret for MongodbConfig {
    fn enforce_one(prop: &str) -> crate::error::ConnectorResult<()> {
        MongodbCommon::enforce_one(prop)
    }
}

impl MongodbConfig {
    pub fn from_btreemap(properties: BTreeMap<String, String>) -> crate::sink::Result<Self> {
        let config =
            serde_json::from_value::<MongodbConfig>(serde_json::to_value(properties).unwrap())
                .map_err(|e| SinkError::Config(anyhow!(e)))?;
        if config.r#type != SINK_TYPE_APPEND_ONLY && config.r#type != SINK_TYPE_UPSERT {
            return Err(SinkError::Config(anyhow!(
                "`{}` must be {}, or {}",
                SINK_TYPE_OPTION,
                SINK_TYPE_APPEND_ONLY,
                SINK_TYPE_UPSERT
            )));
        }
        Ok(config)
    }
}

/// An async-drop style `Guard` for `mongodb::Client`. Use this guard to hold a client,
/// the `client::shutdown` is called in an async manner when the guard is dropped.
/// Please be aware this is a "best effort" style shutdown, which may not be successful if the
/// tokio runtime is in the process of terminating. However, the server-side resources will be
/// cleaned up eventually due to the session expiration.
/// see [this issue](https://github.com/mongodb/mongo-rust-driver/issues/719) for more information
struct ClientGuard {
    _tx: tokio::sync::oneshot::Sender<()>,
    client: Client,
}

impl ClientGuard {
    fn new(name: String, client: Client) -> Self {
        let client_copy = client.clone();
        let (_tx, rx) = tokio::sync::oneshot::channel::<()>();
        tokio::spawn(async move {
            tracing::debug!(%name, "waiting for client to shut down");
            let _ = rx.await;
            tracing::debug!(%name, "sender dropped now calling client's shutdown");
            // shutdown may stuck if the resources created by client are not dropped at this point.
            // As recommended by [shutdown](https://docs.rs/mongodb/2.8.2/mongodb/struct.Client.html#method.shutdown)
            // documentation, we should make our resources usage shorter-lived than the client. So if this happens,
            // there are some programming error in our code.
            client_copy.shutdown().await;
            tracing::debug!(%name, "client shutdown succeeded");
        });
        Self { _tx, client }
    }
}

impl Deref for ClientGuard {
    type Target = Client;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

#[derive(Debug)]
pub struct MongodbSink {
    pub config: MongodbConfig,
    param: SinkParam,
    schema: Schema,
    pk_indices: Vec<usize>,
    is_append_only: bool,
}

impl EnforceSecret for MongodbSink {
    fn enforce_secret<'a>(
        prop_iter: impl Iterator<Item = &'a str>,
    ) -> crate::sink::ConnectorResult<()> {
        for prop in prop_iter {
            MongodbConfig::enforce_one(prop)?;
        }
        Ok(())
    }
}

impl MongodbSink {
    pub fn new(param: SinkParam) -> Result<Self> {
        let config = MongodbConfig::from_btreemap(param.properties.clone())?;
        let pk_indices = param.downstream_pk_or_empty();
        let is_append_only = param.sink_type.is_append_only();
        let schema = param.schema();
        Ok(Self {
            config,
            param,
            schema,
            pk_indices,
            is_append_only,
        })
    }
}

impl TryFrom<SinkParam> for MongodbSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        MongodbSink::new(param)
    }
}

impl Sink for MongodbSink {
    type LogSinker = AsyncTruncateLogSinkerOf<MongodbSinkWriter>;

    const SINK_NAME: &'static str = MONGODB_SINK;

    crate::impl_validate_sink_unknown_fields!();

    async fn validate(&self) -> Result<()> {
        if !self.is_append_only {
            if self.pk_indices.is_empty() {
                return Err(SinkError::Config(anyhow!(
                    "Primary key not defined for upsert mongodb sink (please define in `primary_key` field)"
                )));
            }

            // checking if there is a non-pk field's name is `_id`
            if self
                .schema
                .fields
                .iter()
                .enumerate()
                .any(|(i, field)| !self.pk_indices.contains(&i) && field.name == MONGODB_PK_NAME)
            {
                return Err(SinkError::Config(anyhow!(
                    "_id field must be the sink's primary key, but a non primary key field name is _id",
                )));
            }

            // assume the sink's pk is (a, b) and then the data written to mongodb will be
            // { "_id": {"a": 1, "b": 2}, "a": 1, "b": 2, ... }
            // you can see that the compound pk (a, b) is turned into an Object {"a": 1, "b": 2}
            // and the each pk field is become as a field of the document
            // but if the sink's pk is (_id, b) and the data will be:
            // { "_id": {"_id": 1, "b": 2}, "b": 2, ... }
            // in this case, the original _id field of the compound pk has been overridden
            // we should consider this is a schema error
            if self.pk_indices.len() > 1
                && self
                    .pk_indices
                    .iter()
                    .map(|&idx| self.schema.fields[idx].name.as_str())
                    .any(|field| field == MONGODB_PK_NAME)
            {
                return Err(SinkError::Config(anyhow!(
                    "primary key fields must not contain a field named _id"
                )));
            }
        }

        if let Err(err) = self.config.common.collection_name.parse::<Namespace>() {
            return Err(SinkError::Config(anyhow!(err).context(format!(
                "invalid collection.name {}",
                self.config.common.collection_name
            ))));
        }

        // checking reachability
        let client = self.config.common.build_client().await?;
        let client = ClientGuard::new(self.param.sink_name.clone(), client);
        client
            .database("admin")
            .run_command(doc! {"hello":1})
            .await
            .map_err(|err| {
                SinkError::Mongodb(anyhow!(err).context("failed to send hello command to mongodb"))
            })?;

        if self.config.drop_collection_name_field && self.config.collection_name_field.is_none() {
            return Err(SinkError::Config(anyhow!(
                "collection.name.field must be specified when collection.name.field.drop is enabled"
            )));
        }

        // checking dynamic collection name settings
        if let Some(coll_field) = &self.config.collection_name_field {
            let fields = self.schema.fields();

            let coll_field_index = fields
                .iter()
                .enumerate()
                .find_map(|(index, field)| {
                    if &field.name == coll_field {
                        Some(index)
                    } else {
                        None
                    }
                })
                .ok_or(SinkError::Config(anyhow!(
                    "collection.name.field {} not found",
                    coll_field
                )))?;

            if fields[coll_field_index].data_type() != risingwave_common::types::DataType::Varchar {
                return Err(SinkError::Config(anyhow!(
                    "the type of collection.name.field {} must be varchar",
                    coll_field
                )));
            }

            if !self.is_append_only && self.pk_indices.contains(&coll_field_index) {
                return Err(SinkError::Config(anyhow!(
                    "collection.name.field {} must not be equal to the primary key field",
                    coll_field
                )));
            }
        }

        Ok(())
    }

    async fn new_log_sinker(&self, writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        Ok(MongodbSinkWriter::new(
            format!("{}-{}", writer_param.executor_id, self.param.sink_name),
            self.config.clone(),
            self.schema.clone(),
            self.pk_indices.clone(),
            self.is_append_only,
        )
        .await?
        .into_log_sinker(MONGODB_SEND_FUTURE_BUFFER_MAX_SIZE))
    }
}

use send_bulk_write_command_future::*;

pub struct MongodbSinkWriter {
    pub config: MongodbConfig,
    payload_writer: MongodbPayloadWriter,
    is_append_only: bool,
}

impl MongodbSinkWriter {
    pub async fn new(
        name: String,
        config: MongodbConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        is_append_only: bool,
    ) -> Result<Self> {
        let client = config.common.build_client().await?;

        let default_namespace =
            config
                .common
                .collection_name
                .parse()
                .map_err(|err: mongodb::error::Error| {
                    SinkError::Mongodb(anyhow!(err).context("parsing default namespace failed"))
                })?;

        let coll_name_field_index =
            config
                .collection_name_field
                .as_ref()
                .and_then(|coll_name_field| {
                    schema
                        .names_str()
                        .iter()
                        .position(|&name| coll_name_field == name)
                });

        let col_indices = if let Some(coll_name_field_index) = coll_name_field_index
            && config.drop_collection_name_field
        {
            (0..schema.fields.len())
                .filter(|idx| *idx != coll_name_field_index)
                .collect_vec()
        } else {
            (0..schema.fields.len()).collect_vec()
        };

        let row_encoder = BsonEncoder::new(schema.clone(), Some(col_indices), pk_indices.clone());

        let payload_writer = MongodbPayloadWriter::new(
            schema,
            pk_indices,
            default_namespace,
            coll_name_field_index,
            ClientGuard::new(name, client),
            row_encoder,
        );

        Ok(Self {
            config,
            payload_writer,
            is_append_only,
        })
    }

    fn append(&mut self, chunk: StreamChunk) -> Result<TryJoinAll<SendBulkWriteCommandFuture>> {
        let mut insert_builder: HashMap<MongodbNamespace, InsertCommandBuilder> = HashMap::new();
        for (op, row) in chunk.rows() {
            if op != Op::Insert {
                if let Ok(suppressed_count) = LOG_SUPPRESSOR.check() {
                    tracing::warn!(
                        suppressed_count,
                        ?op,
                        ?row,
                        "non-insert op received in append-only mode"
                    );
                }
                continue;
            }
            self.payload_writer.append(&mut insert_builder, row)?;
        }
        Ok(self.payload_writer.flush_insert(insert_builder))
    }

    fn upsert(&mut self, chunk: StreamChunk) -> Result<TryJoinAll<SendBulkWriteCommandFuture>> {
        let mut upsert_builder: HashMap<MongodbNamespace, UpsertCommandBuilder> = HashMap::new();
        for (op, row) in chunk.rows() {
            if op == Op::UpdateDelete {
                // we should ignore the `UpdateDelete` in upsert mode
                continue;
            }
            self.payload_writer.upsert(&mut upsert_builder, op, row)?;
        }
        Ok(self.payload_writer.flush_upsert(upsert_builder))
    }
}

pub type MongodbSinkDeliveryFuture = impl TryFuture<Ok = (), Error = SinkError> + Unpin + 'static;

impl AsyncTruncateSinkWriter for MongodbSinkWriter {
    type DeliveryFuture = MongodbSinkDeliveryFuture;

    #[define_opaque(MongodbSinkDeliveryFuture)]
    async fn write_chunk<'a>(
        &'a mut self,
        chunk: StreamChunk,
        mut add_future: DeliveryFutureManagerAddFuture<'a, Self::DeliveryFuture>,
    ) -> Result<()> {
        let futures = if self.is_append_only {
            self.append(chunk)?
        } else {
            self.upsert(chunk)?
        };
        add_future
            .add_future_may_await(futures.map_ok(|_: Vec<()>| ()))
            .await?;
        Ok(())
    }
}

struct InsertCommandBuilder {
    coll: String,
    inserts: Array,
}

impl InsertCommandBuilder {
    fn new(coll: String) -> Self {
        Self {
            coll,
            inserts: Array::new(),
        }
    }

    fn append(&mut self, row: Document) {
        self.inserts.push(Bson::Document(row));
    }

    fn build(self) -> Document {
        doc! {
            "insert": self.coll,
            "ordered": true,
            "documents": self.inserts,
        }
    }
}

struct UpsertCommandBuilder {
    coll: String,
    updates: Array,
    deletes: HashMap<Vec<u8>, Document>,
}

impl UpsertCommandBuilder {
    fn new(coll: String) -> Self {
        Self {
            coll,
            updates: Array::new(),
            deletes: HashMap::new(),
        }
    }

    fn add_upsert(&mut self, pk: Document, row: Document) -> Result<()> {
        let pk_data = mongodb::bson::to_vec(&pk).map_err(|err| {
            SinkError::Mongodb(anyhow!(err).context("cannot serialize primary key"))
        })?;
        // under same pk, if the record currently being upserted was marked for deletion previously, we should
        // revert the deletion, otherwise, the upserting record may be accidentally deleted.
        // see https://github.com/risingwavelabs/risingwave/pull/17102#discussion_r1630684160 for more information.
        self.deletes.remove(&pk_data);

        self.updates.push(bson!( {
            "q": pk,
            "u": bson!( {
                "$set": row,
            }),
            "upsert": true,
            "multi": false,
        }));

        Ok(())
    }

    fn add_delete(&mut self, pk: Document) -> Result<()> {
        let pk_data = mongodb::bson::to_vec(&pk).map_err(|err| {
            SinkError::Mongodb(anyhow!(err).context("cannot serialize primary key"))
        })?;
        self.deletes.insert(pk_data, pk);
        Ok(())
    }

    fn build(self) -> (Option<Document>, Option<Document>) {
        let (mut upsert_document, mut delete_document) = (None, None);
        if !self.updates.is_empty() {
            upsert_document = Some(doc! {
                "update": self.coll.clone(),
                "ordered": true,
                "updates": self.updates,
            });
        }
        if !self.deletes.is_empty() {
            let deletes = self
                .deletes
                .into_values()
                .map(|pk| {
                    bson!({
                        "q": pk,
                        "limit": 1,
                    })
                })
                .collect::<Array>();

            delete_document = Some(doc! {
                "delete": self.coll,
                "ordered": true,
                "deletes": deletes,
            });
        }
        (upsert_document, delete_document)
    }
}

type MongodbNamespace = (String, String);

// In the future, we may build the payload into RawBSON to gain a better performance.
// The current API (mongodb-2.8.2) lacks the support of writing RawBSON.
struct MongodbPayloadWriter {
    schema: Schema,
    pk_indices: Vec<usize>,
    default_namespace: Namespace,
    coll_name_field_index: Option<usize>,
    client: ClientGuard,
    row_encoder: BsonEncoder,
}

impl MongodbPayloadWriter {
    fn new(
        schema: Schema,
        pk_indices: Vec<usize>,
        default_namespace: Namespace,
        coll_name_field_index: Option<usize>,
        client: ClientGuard,
        row_encoder: BsonEncoder,
    ) -> Self {
        Self {
            schema,
            pk_indices,
            default_namespace,
            coll_name_field_index,
            client,
            row_encoder,
        }
    }

    fn extract_namespace_from_row_ref(&self, row: RowRef<'_>) -> MongodbNamespace {
        let ns = self.coll_name_field_index.and_then(|coll_name_field_index| {
            match row.datum_at(coll_name_field_index) {
                Some(ScalarRefImpl::Utf8(v)) => match v.parse::<Namespace>() {
                    Ok(ns) => Some(ns),
                    Err(err) => {
                        if let Ok(suppressed_count) = LOG_SUPPRESSOR.check() {
                            tracing::warn!(
                                suppressed_count,
                                error = %err.as_report(),
                                collection_name = %v,
                                "parsing collection name failed, fallback to use default collection.name"
                            );
                        }
                        None
                    }
                },
                _ => {
                    if let Ok(suppressed_count) = LOG_SUPPRESSOR.check() {
                        tracing::warn!(
                            suppressed_count,
                            "the value of collection.name.field is null, fallback to use default collection.name"
                        );
                    }
                    None
                }
            }
        });
        match ns {
            Some(ns) => (ns.db, ns.coll),
            None => (
                self.default_namespace.db.clone(),
                self.default_namespace.coll.clone(),
            ),
        }
    }

    fn append(
        &mut self,
        insert_builder: &mut HashMap<MongodbNamespace, InsertCommandBuilder>,
        row: RowRef<'_>,
    ) -> Result<()> {
        let document = self.row_encoder.encode(row)?;
        let ns = self.extract_namespace_from_row_ref(row);
        let coll = ns.1.clone();

        insert_builder
            .entry(ns)
            .or_insert_with(|| InsertCommandBuilder::new(coll))
            .append(document);
        Ok(())
    }

    fn upsert(
        &mut self,
        upsert_builder: &mut HashMap<MongodbNamespace, UpsertCommandBuilder>,
        op: Op,
        row: RowRef<'_>,
    ) -> Result<()> {
        let mut document = self.row_encoder.encode(row)?;
        let ns = self.extract_namespace_from_row_ref(row);
        let coll = ns.1.clone();

        let pk = self.row_encoder.construct_pk(row);

        // Specify the primary key (_id) for the MongoDB collection if the user does not provide one.
        if self.pk_indices.len() > 1
            || self.schema.fields[self.pk_indices[0]].name != MONGODB_PK_NAME
        {
            // compound pk should not have a field named `_id`
            document.insert(MONGODB_PK_NAME, pk.clone());
        }

        let pk = doc! {MONGODB_PK_NAME: pk};
        match op {
            Op::Insert | Op::UpdateInsert => upsert_builder
                .entry(ns)
                .or_insert_with(|| UpsertCommandBuilder::new(coll))
                .add_upsert(pk, document)?,
            Op::UpdateDelete => (),
            Op::Delete => upsert_builder
                .entry(ns)
                .or_insert_with(|| UpsertCommandBuilder::new(coll))
                .add_delete(pk)?,
        }
        Ok(())
    }

    fn flush_insert(
        &self,
        insert_builder: HashMap<MongodbNamespace, InsertCommandBuilder>,
    ) -> TryJoinAll<SendBulkWriteCommandFuture> {
        // TODO try sending bulk-write of each collection concurrently to improve the performance when
        // `dynamic collection` is enabled. We may need to provide best practice to guide user on setting
        // the MongoDB driver's connection properties.
        let futures = insert_builder.into_iter().map(|(ns, builder)| {
            let db = self.client.database(&ns.0);
            send_bulk_write_commands(db, Some(builder.build()), None)
        });
        try_join_all(futures)
    }

    fn flush_upsert(
        &self,
        upsert_builder: HashMap<MongodbNamespace, UpsertCommandBuilder>,
    ) -> TryJoinAll<SendBulkWriteCommandFuture> {
        // TODO try sending bulk-write of each collection concurrently to improve the performance when
        // `dynamic collection` is enabled. We may need to provide best practice to guide user on setting
        // the MongoDB driver's connection properties.
        let futures = upsert_builder.into_iter().map(|(ns, builder)| {
            let (upsert, delete) = builder.build();
            // we are sending the bulk upsert first because, under same pk, the `Insert` and `UpdateInsert`
            // should always appear before `Delete`. we have already ignored the `UpdateDelete`
            // which is useless in upsert mode.
            let db = self.client.database(&ns.0);
            send_bulk_write_commands(db, upsert, delete)
        });
        try_join_all(futures)
    }
}
