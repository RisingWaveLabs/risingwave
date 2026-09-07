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

use std::future::Future;

use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_batch::task::ShutdownToken;
use risingwave_common::bail;
use risingwave_common::catalog::Engine;
use risingwave_connector::sink::CONNECTOR_TYPE_KEY;
use risingwave_sqlparser::ast::{ObjectName, VacuumMode};

use crate::binder::Binder;
use crate::error::{ErrorCode, Result, RwError};
use crate::handler::{HandlerArgs, RwPgResponse};
use crate::scheduler::SchedulerError;

async fn await_cancelable<T, E, F>(shutdown_rx: &mut ShutdownToken, future: F) -> Result<T>
where
    E: Into<RwError>,
    F: Future<Output = std::result::Result<T, E>>,
{
    tokio::pin!(future);

    tokio::select! {
        result = &mut future => result.map_err(Into::into),
        _ = shutdown_rx.cancelled() => {
            Err(SchedulerError::QueryCancelled("Cancelled by user".to_owned()).into())
        }
    }
}

pub async fn handle_vacuum(
    handler_args: HandlerArgs,
    object_name: ObjectName,
    mode: VacuumMode,
) -> Result<RwPgResponse> {
    let session = &handler_args.session;
    let db_name = &session.database();

    let sink_id = {
        let (schema_name, real_object_name) =
            Binder::resolve_schema_qualified_name(db_name, &object_name)?;
        let catalog_reader = session.env().catalog_reader().read_guard();
        let search_path = session.config().search_path();
        let user_name = session.user_name();
        let schema_path = crate::catalog::root_catalog::SchemaPath::new(
            schema_name.as_deref(),
            &search_path,
            &user_name,
        );

        if let Ok((table, _)) =
            catalog_reader.get_created_table_by_name(db_name, schema_path, &real_object_name)
        {
            if table.engine() == Engine::Iceberg {
                // For iceberg engine table, get the associated iceberg sink name
                let sink_name = table.iceberg_sink_name().ok_or_else(|| {
                    RwError::from(ErrorCode::CatalogError(
                        format!("No iceberg sink name found for table {}", real_object_name).into(),
                    ))
                })?;

                // Find the iceberg sink
                let (sink, _) = catalog_reader
                    .get_created_sink_by_name(db_name, schema_path, &sink_name)
                    .map_err(|_| {
                        RwError::from(ErrorCode::CatalogError(
                            format!(
                                "Iceberg sink {} not found for table {}",
                                sink_name, real_object_name
                            )
                            .into(),
                        ))
                    })?;

                sink.id
            } else {
                return Err(ErrorCode::InvalidInputSyntax(format!(
                    "VACUUM can only be used on Iceberg engine tables or Iceberg sinks, but table '{}' uses {:?} engine",
                    real_object_name,
                    table.engine()
                ))
                    .into());
            }
        } else if let Ok((sink, _)) =
            catalog_reader.get_created_sink_by_name(db_name, schema_path, &real_object_name)
        {
            if let Some(connector_type) = sink.properties.get(CONNECTOR_TYPE_KEY) {
                if connector_type == "iceberg" {
                    sink.id
                } else {
                    return Err(ErrorCode::InvalidInputSyntax(format!(
                        "VACUUM can only be used on Iceberg sinks, but sink '{}' is of type '{}'",
                        real_object_name, connector_type
                    ))
                    .into());
                }
            } else {
                return Err(ErrorCode::InvalidInputSyntax(format!(
                    "VACUUM can only be used on Iceberg sinks, but sink '{}' has no connector type specified",
                    real_object_name
                ))
                    .into());
            }
        } else {
            bail!("object {} not found", real_object_name);
        }
    };

    let mut shutdown_rx = session.reset_cancel_query_flag();

    if mode == VacuumMode::OrphanFiles {
        let orphan_file_count = await_cancelable(
            &mut shutdown_rx,
            session
                .env()
                .meta_client()
                .remove_iceberg_table_orphan_files(sink_id),
        )
        .await?
        .orphan_file_count;

        return Ok(PgResponse::builder(StatementType::VACUUM)
            .notice(format!(
                "Iceberg orphan-file cleanup removed {} files",
                orphan_file_count
            ))
            .into());
    }

    if mode == VacuumMode::Full {
        // VACUUM FULL compacts data files before rewriting the resulting manifests.
        await_cancelable(
            &mut shutdown_rx,
            session.env().meta_client().compact_iceberg_table(sink_id),
        )
        .await?;
    }

    // Run the same eligibility check as periodic manifest maintenance immediately,
    // then expire snapshots so replaced manifests can be cleaned up.
    await_cancelable(
        &mut shutdown_rx,
        session
            .env()
            .meta_client()
            .rewrite_iceberg_table_manifests(sink_id),
    )
    .await?;
    await_cancelable(
        &mut shutdown_rx,
        session
            .env()
            .meta_client()
            .expire_iceberg_table_snapshots(sink_id),
    )
    .await?;

    Ok(PgResponse::builder(StatementType::VACUUM).into())
}
