// Copyright 2026 RisingWave Labs
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

use risingwave_meta_model::{
    Epoch, ObjectId, SinkId, SinkSchemachange, object, pending_sink_state,
};
use risingwave_pb::stream_plan::PbSinkSchemaChange;
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseConnection, EntityTrait, Order, QueryFilter, QueryOrder,
    QuerySelect, Set, TransactionTrait,
};
use thiserror_ext::AsReport;

// Helpers for accessing the `pending_sink_state` system table used by exactly-once sink coordinators
// (both the generic sink coordinator and the Iceberg pk-index sink coordinator).

async fn sink_object_exists<C: ConnectionTrait>(
    db: &C,
    sink_id: SinkId,
) -> Result<bool, sea_orm::DbErr> {
    let object_id: Option<ObjectId> = object::Entity::find_by_id(sink_id.as_object_id())
        .select_only()
        .column(object::Column::Oid)
        .into_tuple()
        .one(db)
        .await?;
    Ok(object_id.is_some())
}

pub async fn persist_pre_commit_metadata(
    db: &DatabaseConnection,
    sink_id: SinkId,
    epoch: u64,
    commit_metadata: Option<Vec<u8>>,
    schema_change: Option<&PbSinkSchemaChange>,
) -> anyhow::Result<()> {
    fail::fail_point!("iceberg_v3_persist_pre_commit_fail", |_| Err(
        anyhow::anyhow!("injected: iceberg_v3_persist_pre_commit_fail")
    ));
    let schema_change = schema_change.map(Into::into);
    let m = pending_sink_state::ActiveModel {
        sink_id: Set(sink_id),
        epoch: Set(epoch as Epoch),
        sink_state: Set(pending_sink_state::SinkState::Pending),
        metadata: Set(commit_metadata),
        schema_change: Set(schema_change),
    };
    match pending_sink_state::Entity::insert(m).exec(db).await {
        Ok(_) => Ok(()),
        Err(e) => {
            // `DROP SINK` currently removes the catalog object before the stop barrier reaches
            // the sink actor. A commit request already in flight can therefore race with the
            // cascading deletion of `pending_sink_state`. Once the object is gone there is no
            // state to recover, so let the request finish instead of failing the actor and the
            // whole database. Other insert errors must still be surfaced.
            if !sink_object_exists(db, sink_id).await? {
                tracing::debug!(
                    %sink_id,
                    epoch,
                    "skip persisting exactly-once metadata for a dropped sink"
                );
                return Ok(());
            }
            tracing::error!(
                "Error inserting into exactly once system table: {:?}",
                e.as_report()
            );
            Err(e.into())
        }
    }
}

pub async fn commit_and_prune_epoch(
    db: &DatabaseConnection,
    sink_id: SinkId,
    epoch: u64,
    prev_epoch: Option<u64>,
) -> anyhow::Result<()> {
    fail::fail_point!("iceberg_v3_commit_prune_fail", |_| Err(anyhow::anyhow!(
        "injected: iceberg_v3_commit_prune_fail"
    )));
    let txn = db.begin().await?;
    let update_result = pending_sink_state::Entity::update(pending_sink_state::ActiveModel {
        sink_id: Set(sink_id),
        epoch: Set(epoch as Epoch),
        sink_state: Set(pending_sink_state::SinkState::Committed),
        ..Default::default()
    })
    .exec(&txn)
    .await;

    if let Err(e) = update_result {
        // The row may have been cascade-deleted while an external two-phase commit was in
        // progress. Roll back before checking through `db`: PostgreSQL leaves a transaction in
        // an aborted state after a statement error.
        txn.rollback().await?;
        if !sink_object_exists(db, sink_id).await? {
            tracing::debug!(
                %sink_id,
                epoch,
                "skip marking exactly-once metadata committed for a dropped sink"
            );
            return Ok(());
        }
        return Err(e.into());
    }

    if let Some(prev_epoch) = prev_epoch {
        pending_sink_state::Entity::delete_many()
            .filter(
                pending_sink_state::Column::SinkId
                    .eq(sink_id)
                    .and(pending_sink_state::Column::Epoch.eq(prev_epoch as Epoch)),
            )
            .exec(&txn)
            .await?;
    }

    match txn.commit().await {
        Ok(_) => Ok(()),
        Err(e) => {
            tracing::error!(
                "Error marking item to committed exactly once system table: {:?}",
                e.as_report()
            );
            Err(e.into())
        }
    }
}

pub async fn clean_aborted_records(
    db: &DatabaseConnection,
    sink_id: SinkId,
    aborted_epochs: Vec<u64>,
) -> anyhow::Result<()> {
    if aborted_epochs.is_empty() {
        return Ok(());
    }

    match pending_sink_state::Entity::delete_many()
        .filter(
            pending_sink_state::Column::SinkId
                .eq(sink_id)
                .and(pending_sink_state::Column::Epoch.is_in(aborted_epochs)),
        )
        .exec(db)
        .await
    {
        Ok(_) => Ok(()),
        Err(e) => {
            tracing::error!(
                "Error deleting records from exactly once system table: {:?}",
                e.as_report()
            );
            Err(e.into())
        }
    }
}

type PendingSinkStateRow = (
    Epoch,
    pending_sink_state::SinkState,
    Option<Vec<u8>>,
    Option<SinkSchemachange>,
);

pub async fn list_sink_states_ordered_by_epoch(
    db: &DatabaseConnection,
    sink_id: SinkId,
) -> anyhow::Result<
    Vec<(
        u64,
        pending_sink_state::SinkState,
        Option<Vec<u8>>,
        Option<PbSinkSchemaChange>,
    )>,
> {
    let rows: Vec<PendingSinkStateRow> = match pending_sink_state::Entity::find()
        .select_only()
        .columns([
            pending_sink_state::Column::Epoch,
            pending_sink_state::Column::SinkState,
            pending_sink_state::Column::Metadata,
            pending_sink_state::Column::SchemaChange,
        ])
        .filter(pending_sink_state::Column::SinkId.eq(sink_id))
        .order_by(pending_sink_state::Column::Epoch, Order::Asc)
        .into_tuple()
        .all(db)
        .await
    {
        Ok(rows) => rows,
        Err(e) => {
            tracing::error!("Error querying pending sink states: {:?}", e.as_report());
            return Err(e.into());
        }
    };

    Ok(rows
        .into_iter()
        .map(|(epoch, state, metadata, schema_change)| {
            (
                epoch as u64,
                state,
                metadata,
                schema_change.map(|v| v.to_protobuf()),
            )
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use sea_orm::{ConnectionTrait, Database, DatabaseConnection, DbBackend, Statement};

    use super::{commit_and_prune_epoch, persist_pre_commit_metadata};

    async fn prepare_db() -> DatabaseConnection {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        for ddl in [
            "PRAGMA foreign_keys = ON",
            "CREATE TABLE object (oid INTEGER PRIMARY KEY)",
            "CREATE TABLE pending_sink_state (\
                sink_id INTEGER NOT NULL, \
                epoch BIGINT NOT NULL, \
                sink_state STRING NOT NULL, \
                metadata BLOB, \
                schema_change BLOB, \
                PRIMARY KEY (sink_id, epoch), \
                FOREIGN KEY (sink_id) REFERENCES object(oid) ON DELETE CASCADE\
            )",
        ] {
            db.execute(Statement::from_string(DbBackend::Sqlite, ddl))
                .await
                .unwrap();
        }
        db
    }

    async fn insert_object(db: &DatabaseConnection, sink_id: i32) {
        db.execute(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "INSERT INTO object (oid) VALUES (?)",
            [sink_id.into()],
        ))
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_exactly_once_metadata_writes_ignore_dropped_sink() {
        let db = prepare_db().await;
        insert_object(&db, 1).await;

        persist_pre_commit_metadata(&db, 1.into(), 100, Some(vec![1, 2, 3]), None)
            .await
            .unwrap();
        db.execute(Statement::from_string(
            DbBackend::Sqlite,
            "DELETE FROM object WHERE oid = 1",
        ))
        .await
        .unwrap();

        // The first row was cascade-deleted while the coordinator was committing it, and a new
        // pre-commit request arrived after the parent object was removed. Both are expected
        // during `DROP SINK` and must not fail the sink actor.
        commit_and_prune_epoch(&db, 1.into(), 100, None)
            .await
            .unwrap();
        persist_pre_commit_metadata(&db, 1.into(), 101, Some(vec![4, 5, 6]), None)
            .await
            .unwrap();

        let row_count = db
            .query_one(Statement::from_string(
                DbBackend::Sqlite,
                "SELECT COUNT(*) AS count FROM pending_sink_state",
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get::<i64>("", "count")
            .unwrap();
        assert_eq!(row_count, 0);
    }

    #[tokio::test]
    async fn test_missing_state_is_error_for_existing_sink() {
        let db = prepare_db().await;
        insert_object(&db, 1).await;

        assert!(
            commit_and_prune_epoch(&db, 1.into(), 100, None)
                .await
                .is_err()
        );

        persist_pre_commit_metadata(&db, 1.into(), 101, None, None)
            .await
            .unwrap();
        assert!(
            persist_pre_commit_metadata(&db, 1.into(), 101, None, None)
                .await
                .is_err()
        );
    }
}
