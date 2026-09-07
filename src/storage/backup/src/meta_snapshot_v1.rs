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

use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};

use bytes::{Buf, BufMut, BytesMut};
use itertools::Itertools;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_hummock_sdk::HummockRawObjectId;
use risingwave_hummock_sdk::version::HummockVersion;
use risingwave_pb::catalog::{
    Connection, Database, Function, Index, Schema, Secret, Sink, Source, Subscription, Table, View,
};
use risingwave_pb::hummock::{CompactionGroup, HummockVersionStats, PbHummockVersion};
use risingwave_pb::meta::{SystemParams, TableFragments};
use risingwave_pb::user::UserInfo;

use crate::error::{BackupError, BackupResult};
use crate::meta_snapshot::{
    MetaSnapshot, Metadata, SnapshotPayloadReader, SnapshotPayloadWriter, read_u32_le,
};

// TODO: remove `ClusterMetadata` and even the trait, after applying model v2.
pub type MetaSnapshotV1 = MetaSnapshot<ClusterMetadata>;

impl Display for ClusterMetadata {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "default_cf:")?;
        for (k, v) in &self.default_cf {
            let key = String::from_utf8(k.clone()).unwrap();
            writeln!(f, "{} {:x?}", key, v)?;
        }
        writeln!(f, "hummock_version:")?;
        writeln!(f, "{:#?}", self.hummock_version)?;
        writeln!(f, "version_stats:")?;
        writeln!(f, "{:#?}", self.version_stats)?;
        writeln!(f, "compaction_groups:")?;
        writeln!(f, "{:#?}", self.compaction_groups)?;
        writeln!(f, "database:")?;
        writeln!(f, "{:#?}", self.database)?;
        writeln!(f, "schema:")?;
        writeln!(f, "{:#?}", self.schema)?;
        writeln!(f, "table:")?;
        writeln!(f, "{:#?}", self.table)?;
        writeln!(f, "index:")?;
        writeln!(f, "{:#?}", self.index)?;
        writeln!(f, "sink:")?;
        writeln!(f, "{:#?}", self.sink)?;
        writeln!(f, "source:")?;
        writeln!(f, "{:#?}", self.source)?;
        writeln!(f, "view:")?;
        writeln!(f, "{:#?}", self.view)?;
        writeln!(f, "connection:")?;
        writeln!(f, "{:#?}", self.connection)?;
        writeln!(f, "table_fragments:")?;
        writeln!(f, "{:#?}", self.table_fragments)?;
        writeln!(f, "user_info:")?;
        writeln!(f, "{:#?}", self.user_info)?;
        writeln!(f, "function:")?;
        writeln!(f, "{:#?}", self.function)?;
        writeln!(f, "system_param:")?;
        writeln!(f, "{:#?}", self.system_param)?;
        writeln!(f, "cluster_id:")?;
        writeln!(f, "{:#?}", self.cluster_id)?;
        writeln!(f, "subscription:")?;
        writeln!(f, "{:#?}", self.subscription)?;
        writeln!(f, "secret:")?;
        writeln!(f, "{:#?}", self.secret)?;
        Ok(())
    }
}

impl Metadata for ClusterMetadata {
    async fn encode_to_writer(&self, mut writer: SnapshotPayloadWriter) -> BackupResult<()> {
        let mut buf = BytesMut::new();
        self.encode_to(&mut buf)?;
        writer.write_snapshot_bytes(buf.freeze()).await?;
        writer.finish().await?;
        Ok(())
    }

    async fn decode_from_reader(reader: SnapshotPayloadReader) -> BackupResult<Self> {
        let data = reader.read_to_end().await?;
        ClusterMetadata::decode(&data)
    }

    fn hummock_version_ref(&self) -> &HummockVersion {
        &self.hummock_version
    }

    fn hummock_version(self) -> HummockVersion {
        self.hummock_version
    }

    fn storage_url(&self) -> BackupResult<String> {
        unreachable!("");
    }

    fn storage_directory(&self) -> BackupResult<String> {
        unreachable!("");
    }

    fn table_change_log_object_ids(&self) -> HashSet<HummockRawObjectId> {
        HashSet::default()
    }
}

/// For backward compatibility, never remove fields and only append new field.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct ClusterMetadata {
    /// Unlike other metadata that has implemented `MetadataModel`,
    /// `DEFAULT_COLUMN_FAMILY` stores various single row metadata, e.g. id offset and epoch offset.
    /// So we use `default_cf` stores raw KVs for them.
    pub default_cf: HashMap<Vec<u8>, Vec<u8>>,
    pub hummock_version: HummockVersion,
    pub version_stats: HummockVersionStats,
    pub compaction_groups: Vec<CompactionGroup>,
    pub database: Vec<Database>,
    pub schema: Vec<Schema>,
    pub table: Vec<Table>,
    pub index: Vec<Index>,
    pub sink: Vec<Sink>,
    pub source: Vec<Source>,
    pub view: Vec<View>,
    pub table_fragments: Vec<TableFragments>,
    pub user_info: Vec<UserInfo>,
    pub function: Vec<Function>,
    pub connection: Vec<Connection>,
    pub system_param: SystemParams,
    pub cluster_id: String,
    pub subscription: Vec<Subscription>,
    pub secret: Vec<Secret>,
}

impl ClusterMetadata {
    pub fn encode_to(&self, buf: &mut impl BufMut) -> BackupResult<()> {
        let default_cf_keys = self.default_cf.keys().collect_vec();
        let default_cf_values = self.default_cf.values().collect_vec();
        Self::encode_prost_message_list(&default_cf_keys, buf);
        Self::encode_prost_message_list(&default_cf_values, buf);
        Self::encode_prost_message(&PbHummockVersion::from(&self.hummock_version), buf);
        Self::encode_prost_message(&self.version_stats, buf);
        Self::encode_prost_message_list(&self.compaction_groups.iter().collect_vec(), buf);
        Self::encode_prost_message_list(&self.table_fragments.iter().collect_vec(), buf);
        Self::encode_prost_message_list(&self.user_info.iter().collect_vec(), buf);
        Self::encode_prost_message_list(&self.database.iter().collect_vec(), buf);
        Self::encode_prost_message_list(&self.schema.iter().collect_vec(), buf);
        Self::encode_prost_message_list(&self.table.iter().collect_vec(), buf);
        Self::encode_prost_message_list(&self.index.iter().collect_vec(), buf);
        Self::encode_prost_message_list(&self.sink.iter().collect_vec(), buf);
        Self::encode_prost_message_list(&self.source.iter().collect_vec(), buf);
        Self::encode_prost_message_list(&self.view.iter().collect_vec(), buf);
        Self::encode_prost_message_list(&self.function.iter().collect_vec(), buf);
        Self::encode_prost_message_list(&self.connection.iter().collect_vec(), buf);
        Self::encode_prost_message(&self.system_param, buf);
        Self::encode_prost_message(&self.cluster_id, buf);
        Self::encode_prost_message_list(&self.subscription.iter().collect_vec(), buf);
        Self::encode_prost_message_list(&self.secret.iter().collect_vec(), buf);
        Ok(())
    }

    pub fn decode(mut buf: &[u8]) -> BackupResult<Self> {
        let default_cf_keys: Vec<Vec<u8>> = Self::decode_prost_message_list(&mut buf)?;
        let default_cf_values: Vec<Vec<u8>> = Self::decode_prost_message_list(&mut buf)?;
        let default_cf = default_cf_keys
            .into_iter()
            .zip_eq_fast(default_cf_values.into_iter())
            .collect();
        let hummock_version =
            HummockVersion::from_persisted_protobuf_owned(Self::decode_prost_message(&mut buf)?);
        let version_stats = Self::decode_prost_message(&mut buf)?;
        let compaction_groups: Vec<CompactionGroup> = Self::decode_prost_message_list(&mut buf)?;
        let table_fragments: Vec<TableFragments> = Self::decode_prost_message_list(&mut buf)?;
        let user_info: Vec<UserInfo> = Self::decode_prost_message_list(&mut buf)?;
        let database: Vec<Database> = Self::decode_prost_message_list(&mut buf)?;
        let schema: Vec<Schema> = Self::decode_prost_message_list(&mut buf)?;
        let table: Vec<Table> = Self::decode_prost_message_list(&mut buf)?;
        let index: Vec<Index> = Self::decode_prost_message_list(&mut buf)?;
        let sink: Vec<Sink> = Self::decode_prost_message_list(&mut buf)?;
        let source: Vec<Source> = Self::decode_prost_message_list(&mut buf)?;
        let view: Vec<View> = Self::decode_prost_message_list(&mut buf)?;
        let function: Vec<Function> = Self::decode_prost_message_list(&mut buf)?;
        let connection: Vec<Connection> = Self::decode_prost_message_list(&mut buf)?;
        let system_param: SystemParams = Self::decode_prost_message(&mut buf)?;
        let cluster_id: String = Self::decode_prost_message(&mut buf)?;
        let subscription: Vec<Subscription> =
            Self::try_decode_prost_message_list(&mut buf).unwrap_or_else(|| Ok(vec![]))?;
        let secret: Vec<Secret> =
            Self::try_decode_prost_message_list(&mut buf).unwrap_or_else(|| Ok(vec![]))?;

        Ok(Self {
            default_cf,
            hummock_version,
            version_stats,
            compaction_groups,
            database,
            schema,
            table,
            index,
            sink,
            source,
            view,
            table_fragments,
            user_info,
            function,
            connection,
            system_param,
            cluster_id,
            subscription,
            secret,
        })
    }

    fn encode_prost_message(message: &impl prost::Message, buf: &mut impl BufMut) {
        let encoded_message = message.encode_to_vec();
        buf.put_u32_le(encoded_message.len() as u32);
        buf.put_slice(&encoded_message);
    }

    fn decode_prost_message<T>(buf: &mut &[u8]) -> BackupResult<T>
    where
        T: prost::Message + Default,
    {
        let len = read_u32_le(buf)? as usize;
        if buf.remaining() < len {
            return Err(BackupError::Other(anyhow::anyhow!(
                "prost payload length {} exceeds remaining buffer {}",
                len,
                buf.remaining()
            )));
        }
        let v = buf[..len].to_vec();
        buf.advance(len);
        T::decode(v.as_slice()).map_err(|e| BackupError::Decoding(e.into()))
    }

    fn encode_prost_message_list(messages: &[&impl prost::Message], buf: &mut impl BufMut) {
        buf.put_u32_le(messages.len() as u32);
        for message in messages {
            Self::encode_prost_message(*message, buf);
        }
    }

    fn decode_prost_message_list<T>(buf: &mut &[u8]) -> BackupResult<Vec<T>>
    where
        T: prost::Message + Default,
    {
        let vec_len = read_u32_le(buf)? as usize;
        let mut result = vec![];
        for _ in 0..vec_len {
            let v: T = Self::decode_prost_message(buf)?;
            result.push(v);
        }
        Ok(result)
    }

    fn try_decode_prost_message_list<T>(buf: &mut &[u8]) -> Option<BackupResult<Vec<T>>>
    where
        T: prost::Message + Default,
    {
        if buf.is_empty() {
            return None;
        }
        Some(Self::decode_prost_message_list(buf))
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use risingwave_hummock_sdk::HummockVersionId;
    use risingwave_pb::hummock::{CompactionGroup, TableStats};

    use crate::meta_snapshot_v1::ClusterMetadata;

    #[test]
    fn test_metadata_encoding_decoding() {
        let mut buf = BytesMut::new();
        let mut metadata = ClusterMetadata::default();
        metadata.hummock_version.id = HummockVersionId::new(321);
        metadata.default_cf.insert(vec![0, 1, 2], vec![3, 4, 5]);
        metadata.version_stats.hummock_version_id = 10.into();
        metadata.version_stats.table_stats.insert(
            200.into(),
            TableStats {
                total_key_count: 1000,
                ..Default::default()
            },
        );
        metadata.compaction_groups.push(CompactionGroup {
            id: 3000.into(),
            ..Default::default()
        });
        metadata.encode_to(&mut buf).unwrap();
        let decoded = ClusterMetadata::decode(&buf).unwrap();
        assert_eq!(metadata, decoded);
    }
}
