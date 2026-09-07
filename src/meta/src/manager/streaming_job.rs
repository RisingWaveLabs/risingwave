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

use std::collections::HashSet;

use risingwave_common::bail_not_implemented;
use risingwave_common::catalog::TableVersionId;
use risingwave_common::id::{ConnectionId, DatabaseId, JobId, SchemaId, SecretId, TableId, UserId};
use risingwave_meta_model::object::ObjectType;
use risingwave_meta_model::prelude::{SourceModel, TableModel};
use risingwave_meta_model::{TableVersion, source, table};
use risingwave_pb::catalog::{CreateType, Index, PbSource, Sink, Table};
use risingwave_pb::ddl_service::TableJobType;
use sea_orm::entity::prelude::*;
use sea_orm::{DatabaseTransaction, QuerySelect};
use strum::{EnumIs, EnumTryAs};

use super::{
    get_referred_connection_ids_from_sink, get_referred_connection_ids_from_source,
    get_referred_secret_ids_from_sink, get_referred_secret_ids_from_source,
};
use crate::stream::StreamFragmentGraph;
use crate::{MetaError, MetaResult};

// This enum is used to re-use code in `DdlServiceImpl` for creating MaterializedView and
// Sink.
#[derive(Debug, Clone, EnumIs, EnumTryAs)]
pub enum StreamingJob {
    MaterializedView(Table),
    Sink(Sink, Option<TableId>),
    Table(Option<PbSource>, Table, TableJobType),
    Index(Index, Table),
    Source(PbSource),
}

impl std::fmt::Display for StreamingJob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamingJob::MaterializedView(table) => {
                write!(f, "MaterializedView: {}({})", table.name, table.id)
            }
            StreamingJob::Sink(sink, _) => write!(f, "Sink: {}({})", sink.name, sink.id),
            StreamingJob::Table(_, table, _) => write!(f, "Table: {}({})", table.name, table.id),
            StreamingJob::Index(index, _) => write!(f, "Index: {}({})", index.name, index.id),
            StreamingJob::Source(source) => write!(f, "Source: {}({})", source.name, source.id),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StreamingJobType {
    MaterializedView,
    Sink,
    Table(TableJobType),
    Index,
    Source,
}

impl From<&StreamingJob> for StreamingJobType {
    fn from(job: &StreamingJob) -> Self {
        match job {
            StreamingJob::MaterializedView(_) => StreamingJobType::MaterializedView,
            StreamingJob::Sink(..) => StreamingJobType::Sink,
            StreamingJob::Table(_, _, ty) => StreamingJobType::Table(*ty),
            StreamingJob::Index(_, _) => StreamingJobType::Index,
            StreamingJob::Source(_) => StreamingJobType::Source,
        }
    }
}

#[cfg(test)]
#[expect(clippy::derivable_impls)]
impl Default for StreamingJobType {
    fn default() -> Self {
        // This should not be used by mock services,
        // so we can just pick an arbitrary default variant.
        StreamingJobType::MaterializedView
    }
}

// TODO: basically we want to ensure that the `Table` persisted in the catalog is the same as the
// one in the `Materialize` node in the actor. However, they are currently handled separately
// and can be out of sync. Shall we directly copy the whole struct from the actor to the catalog
// to avoid `set`ting each field separately?
impl StreamingJob {
    /// Set the vnode count of the table.
    pub fn set_table_vnode_count(&mut self, vnode_count: usize) {
        match self {
            Self::MaterializedView(table) | Self::Index(_, table) | Self::Table(_, table, ..) => {
                table.maybe_vnode_count = Some(vnode_count as u32);
            }
            Self::Sink(..) | Self::Source(_) => {}
        }
    }

    /// Add some info which is only available in fragment graph to the catalog.
    pub fn set_info_from_graph(&mut self, graph: &StreamFragmentGraph) {
        match self {
            Self::Table(_, table, ..) => {
                table.fragment_id = graph.table_fragment_id();
                table.dml_fragment_id = graph.dml_fragment_id();
            }
            Self::MaterializedView(table) | Self::Index(_, table) => {
                table.fragment_id = graph.table_fragment_id();
            }
            Self::Sink(..) | Self::Source(_) => {}
        }
    }

    pub fn id(&self) -> JobId {
        match self {
            Self::MaterializedView(table) => table.id.as_job_id(),
            Self::Sink(sink, _) => sink.id.as_job_id(),
            Self::Table(_, table, ..) => table.id.as_job_id(),
            Self::Index(index, _) => index.id.as_job_id(),
            Self::Source(source) => source.id.as_share_source_job_id(),
        }
    }

    /// Returns the reference to the [`Table`] of the job if it exists.
    pub fn table(&self) -> Option<&Table> {
        match self {
            Self::MaterializedView(table) | Self::Index(_, table) | Self::Table(_, table, ..) => {
                Some(table)
            }
            Self::Sink(..) | Self::Source(_) => None,
        }
    }

    pub fn schema_id(&self) -> SchemaId {
        match self {
            Self::MaterializedView(table) => table.schema_id,
            Self::Sink(sink, _) => sink.schema_id,
            Self::Table(_, table, ..) => table.schema_id,
            Self::Index(index, _) => index.schema_id,
            Self::Source(source) => source.schema_id,
        }
    }

    pub fn database_id(&self) -> DatabaseId {
        match self {
            Self::MaterializedView(table) => table.database_id,
            Self::Sink(sink, _) => sink.database_id,
            Self::Table(_, table, ..) => table.database_id,
            Self::Index(index, _) => index.database_id,
            Self::Source(source) => source.database_id,
        }
    }

    pub fn name(&self) -> String {
        match self {
            Self::MaterializedView(table) => table.name.clone(),
            Self::Sink(sink, _) => sink.name.clone(),
            Self::Table(_, table, ..) => table.name.clone(),
            Self::Index(index, _) => index.name.clone(),
            Self::Source(source) => source.name.clone(),
        }
    }

    pub fn owner(&self) -> UserId {
        match self {
            StreamingJob::MaterializedView(mv) => mv.owner,
            StreamingJob::Sink(sink, _) => sink.owner,
            StreamingJob::Table(_, table, ..) => table.owner,
            StreamingJob::Index(index, _) => index.owner,
            StreamingJob::Source(source) => source.owner,
        }
    }

    pub fn job_type(&self) -> StreamingJobType {
        self.into()
    }

    pub fn job_type_str(&self) -> &'static str {
        match self {
            StreamingJob::MaterializedView(_) => "materialized view",
            StreamingJob::Sink(..) => "sink",
            StreamingJob::Table(_, _, _) => "table",
            StreamingJob::Index(_, _) => "index",
            StreamingJob::Source(_) => "source",
        }
    }

    pub fn definition(&self) -> String {
        match self {
            Self::MaterializedView(table) => table.definition.clone(),
            Self::Table(_, table, ..) => table.definition.clone(),
            Self::Index(_, table) => table.definition.clone(),
            Self::Sink(sink, _) => sink.definition.clone(),
            Self::Source(source) => source.definition.clone(),
        }
    }

    pub fn object_type(&self) -> ObjectType {
        match self {
            Self::MaterializedView(_) => ObjectType::Table, // Note MV is special.
            Self::Sink(..) => ObjectType::Sink,
            Self::Table(_, _, _) => ObjectType::Table,
            Self::Index(_, _) => ObjectType::Index,
            Self::Source(_) => ObjectType::Source,
        }
    }

    /// Returns the [`TableVersionId`] if this job is `Table`.
    pub fn table_version_id(&self) -> Option<TableVersionId> {
        if let Self::Table(_, table, ..) = self {
            Some(
                table
                    .get_version()
                    .expect("table must be versioned")
                    .version,
            )
        } else {
            None
        }
    }

    pub fn create_type(&self) -> CreateType {
        match self {
            Self::Table(_, table, ..) => table.get_create_type().unwrap_or(CreateType::Foreground),
            Self::MaterializedView(table) => {
                table.get_create_type().unwrap_or(CreateType::Foreground)
            }
            Self::Sink(s, _) => s.get_create_type().unwrap_or(CreateType::Foreground),
            Self::Index(index, _) => {
                CreateType::try_from(index.create_type).unwrap_or(CreateType::Foreground)
            }
            _ => CreateType::Foreground,
        }
    }

    pub fn dependent_connection_ids(&self) -> MetaResult<HashSet<ConnectionId>> {
        match self {
            StreamingJob::Source(source) => Ok(get_referred_connection_ids_from_source(source)),
            StreamingJob::Table(source, _, _) => {
                if let Some(source) = source {
                    Ok(get_referred_connection_ids_from_source(source))
                } else {
                    Ok(HashSet::new())
                }
            }
            StreamingJob::Sink(sink, _) => Ok(get_referred_connection_ids_from_sink(sink)),
            StreamingJob::MaterializedView(_) | StreamingJob::Index(_, _) => Ok(HashSet::new()),
        }
    }

    // Get the secret ids that are referenced by this job.
    pub fn dependent_secret_ids(&self) -> MetaResult<HashSet<SecretId>> {
        match self {
            StreamingJob::Sink(sink, _) => Ok(get_referred_secret_ids_from_sink(sink)),
            StreamingJob::Table(source, _, _) => {
                if let Some(source) = source {
                    get_referred_secret_ids_from_source(source)
                } else {
                    Ok(HashSet::new())
                }
            }
            StreamingJob::Source(source) => get_referred_secret_ids_from_source(source),
            StreamingJob::MaterializedView(_) | StreamingJob::Index(_, _) => Ok(HashSet::new()),
        }
    }

    /// Verify the new version is the next version of the original version.
    pub async fn verify_version_for_replace(&self, txn: &DatabaseTransaction) -> MetaResult<()> {
        let id = self.id();

        match self {
            StreamingJob::Table(_source, table, _table_job_type) => {
                let new_version = table.get_version()?.get_version();
                let original_version: Option<TableVersion> =
                    TableModel::find_by_id(id.as_mv_table_id())
                        .select_only()
                        .column(table::Column::Version)
                        .into_tuple()
                        .one(txn)
                        .await?
                        .ok_or_else(|| MetaError::catalog_id_not_found(self.job_type_str(), id))?;
                let original_version = original_version
                    .expect("version for table should exist")
                    .to_protobuf();
                if new_version != original_version.version + 1 {
                    return Err(MetaError::permission_denied("table version is stale"));
                }
            }
            StreamingJob::Source(source) => {
                let new_version = source.get_version();
                let original_version: Option<i64> =
                    SourceModel::find_by_id(id.as_shared_source_id())
                        .select_only()
                        .column(source::Column::Version)
                        .into_tuple()
                        .one(txn)
                        .await?
                        .ok_or_else(|| MetaError::catalog_id_not_found(self.job_type_str(), id))?;
                let original_version = original_version.expect("version for source should exist");
                if new_version != original_version as u64 + 1 {
                    return Err(MetaError::permission_denied("source version is stale"));
                }
            }
            StreamingJob::MaterializedView(_) => {
                // No version check for materialized view, since `ALTER MATERIALIZED VIEW AS QUERY`
                // is a full rewrite.
            }
            StreamingJob::Sink(..) => {
                // No version check for sink, since sink fragment altering is triggered along with Table
            }
            StreamingJob::Index(_, _) => {
                bail_not_implemented!("schema change for {}", self.job_type_str())
            }
        }
        Ok(())
    }

    pub fn is_sink_into_table(&self) -> bool {
        matches!(self, Self::Sink(sink, _) if sink.target_table.is_some())
    }
}
