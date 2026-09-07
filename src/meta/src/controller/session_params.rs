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

use std::collections::HashMap;
use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::config::SessionInitConfig;
use risingwave_common::session_config::{SessionConfig, SessionConfigError};
use risingwave_meta_model::prelude::SessionParameter;
use risingwave_meta_model::session_parameter;
use risingwave_pb::meta::SetSessionParamRequest;
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use sea_orm::ActiveValue::Set;
use sea_orm::{DatabaseConnection, EntityTrait, TransactionTrait};
use thiserror_ext::AsReport;
use tokio::sync::RwLock;
use tracing::info;

use crate::controller::SqlMetaStore;
use crate::manager::{LocalNotification, NotificationManagerRef};
use crate::{MetaError, MetaResult};

pub type SessionParamsControllerRef = Arc<SessionParamsController>;

const ENABLE_LOCALITY_BACKFILL_PARAM: &str = "enable_locality_backfill";
const LOCALITY_BACKFILL_MODE_PARAM: &str = "locality_backfill_mode";

/// Manages the global default session params on meta.
/// Note that the session params in each session will be initialized from the default value here.
pub struct SessionParamsController {
    db: DatabaseConnection,
    // Cached parameters.
    params: RwLock<SessionConfig>,
    notification_manager: NotificationManagerRef,
}

impl SessionParamsController {
    pub async fn new(
        sql_meta_store: SqlMetaStore,
        notification_manager: NotificationManagerRef,
        session_init: SessionInitConfig,
    ) -> MetaResult<Self> {
        let db = sql_meta_store.conn;

        // Precedence (high to low):
        // 1. Persisted value in Meta store (`session_parameter`)
        // 2. Explicit value in `[session_init]`
        // 3. Built-in `SessionConfig::default()`
        let mut init_params = SessionConfig::default();

        // Apply the explicitly-configured `[session_init]` values onto the built-in defaults.
        // Record the normalized value of each so we can later detect when a persisted value
        // takes precedence and warn the operator.
        let mut session_init_values: HashMap<String, String> = HashMap::new();
        for (name, value) in session_init.entries() {
            let normalized = init_params.set(name, value.to_owned(), &mut ())?;
            session_init_values.insert(name.to_owned(), normalized);
        }
        if !session_init_values.is_empty() {
            info!(
                "[session_init] seeds session parameters during cluster bootstrap only; \
                 persisted values in the meta store take precedence on existing clusters"
            );
        }

        // Persisted values take precedence over `[session_init]`.
        let params = SessionParameter::find().all(&db).await?;
        let has_locality_backfill_mode = params
            .iter()
            .any(|param| param.name == LOCALITY_BACKFILL_MODE_PARAM);
        let has_legacy_locality_backfill = params
            .iter()
            .any(|param| param.name == ENABLE_LOCALITY_BACKFILL_PARAM);
        for param in params {
            if let Some(configured) = session_init_values.get(&param.name)
                && *configured != param.value
            {
                tracing::warn!(
                    parameter = %param.name,
                    session_init_value = %configured,
                    persisted_value = %param.value,
                    "session_init value differs from persisted session parameter, using persisted value"
                );
            }
            if let Err(e) = init_params.set(&param.name, param.value, &mut ()) {
                match e {
                    SessionConfigError::InvalidValue { .. } => {
                        tracing::error!(error = %e.as_report(), "failed to set parameter from meta database, using default value {}", init_params.get(&param.name)?)
                    }
                    SessionConfigError::UnrecognizedEntry(_) => {
                        tracing::error!(error = %e.as_report(), "failed to set parameter from meta database")
                    }
                }
            }
        }
        // Before the mode existed, enabling locality backfill always skipped the size check.
        if !has_locality_backfill_mode && has_legacy_locality_backfill {
            init_params.set(LOCALITY_BACKFILL_MODE_PARAM, "always".to_owned(), &mut ())?;
        }

        info!(?init_params, "session parameters");

        let ctl = Self {
            db,
            params: RwLock::new(init_params.clone()),
            notification_manager,
        };
        // flush to db.
        ctl.flush_params().await?;

        Ok(ctl)
    }

    pub async fn get_params(&self) -> SessionConfig {
        self.params.read().await.clone()
    }

    async fn flush_params(&self) -> MetaResult<()> {
        let params = self.params.read().await.list_all();
        let models = params
            .into_iter()
            .map(|param| session_parameter::ActiveModel {
                name: Set(param.name),
                value: Set(param.setting),
                description: Set(Some(param.description)),
            })
            .collect_vec();
        let txn = self.db.begin().await?;
        // delete all params first and then insert all params. It follows the same logic
        // as the old code, we'd better change it to another way later to keep consistency.
        SessionParameter::delete_many().exec(&txn).await?;
        SessionParameter::insert_many(models).exec(&txn).await?;
        txn.commit().await?;
        Ok(())
    }

    pub async fn set_param(&self, name: &str, value: Option<String>) -> MetaResult<String> {
        let mut params_guard = self.params.write().await;
        let name = SessionConfig::alias_to_entry_name(name);
        let Some(param) = SessionParameter::find_by_id(name.clone())
            .one(&self.db)
            .await?
        else {
            return Err(MetaError::system_params(format!(
                "unrecognized session parameter {:?}",
                name
            )));
        };
        let old_batch_parallelism = params_guard.batch_parallelism();
        // FIXME: use a real reporter
        let reporter = &mut ();
        let new_param = if let Some(value) = value {
            params_guard.set(&name, value, reporter)?
        } else {
            params_guard.reset(&name, reporter)?
        };

        let mut param: session_parameter::ActiveModel = param.into();
        param.value = Set(new_param.clone());
        SessionParameter::update(param).exec(&self.db).await?;
        let new_batch_parallelism = params_guard.batch_parallelism();
        self.notify_workers(name.clone(), new_param.clone());
        if old_batch_parallelism != new_batch_parallelism {
            self.notification_manager
                .notify_local_subscribers(LocalNotification::BatchParallelismChange);
        }

        Ok(new_param)
    }

    pub fn notify_workers(&self, name: String, value: String) {
        self.notification_manager.notify_frontend_without_version(
            Operation::Update,
            Info::SessionParam(SetSessionParamRequest {
                param: name,
                value: Some(value),
            }),
        );
    }
}

#[cfg(test)]
mod tests {
    use sea_orm::ColumnTrait;

    use super::*;
    use crate::manager::MetaSrvEnv;

    #[tokio::test]
    async fn test_session_params() {
        use sea_orm::QueryFilter;

        let env = MetaSrvEnv::for_test().await;
        let meta_store = env.meta_store_ref();
        let init_params = SessionConfig::default();

        // init system parameter controller as first launch.
        let session_param_ctl = SessionParamsController::new(
            meta_store.clone(),
            env.notification_manager_ref(),
            SessionInitConfig::default(),
        )
        .await
        .unwrap();
        let params = session_param_ctl.get_params().await;
        assert_eq!(params, init_params);

        // set parameter.
        let new_params = session_param_ctl
            .set_param("rw_implicit_flush", Some("true".into()))
            .await
            .unwrap();

        // insert deprecated params.
        let deprecated_param = session_parameter::ActiveModel {
            name: Set("deprecated_param".into()),
            value: Set("foo".into()),
            description: Set(None),
        };
        SessionParameter::insert(deprecated_param)
            .exec(&session_param_ctl.db)
            .await
            .unwrap();

        // init system parameter controller as not first launch.
        let session_param_ctl = SessionParamsController::new(
            meta_store.clone(),
            env.notification_manager_ref(),
            SessionInitConfig::default(),
        )
        .await
        .unwrap();
        // check deprecated params are cleaned up.
        assert!(
            SessionParameter::find_by_id("deprecated_param".to_owned())
                .one(&session_param_ctl.db)
                .await
                .unwrap()
                .is_none()
        );
        // check new params are set.
        let params = session_param_ctl.get_params().await;
        assert_eq!(params.get("rw_implicit_flush").unwrap(), new_params);
        assert_eq!(
            params.get("rw_implicit_flush").unwrap(),
            params.get("implicit_flush").unwrap()
        );
        // check db consistency.
        // rw_implicit_flush is alias to implicit_flush <https://github.com/risingwavelabs/risingwave/pull/18769>
        let models = SessionParameter::find()
            .filter(session_parameter::Column::Name.eq("rw_implicit_flush"))
            .one(&session_param_ctl.db)
            .await
            .unwrap();
        assert!(models.is_none());
        let models = SessionParameter::find()
            .filter(session_parameter::Column::Name.eq("implicit_flush"))
            .one(&session_param_ctl.db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(models.value, params.get("rw_implicit_flush").unwrap());
    }

    /// Scenario 1: on a new cluster, `[session_init]` seeds values into the meta store, including
    /// the `default` placeholder which must be persisted verbatim.
    #[tokio::test]
    async fn test_session_init_bootstrap() {
        let env = MetaSrvEnv::for_test().await;
        let meta_store = env.meta_store_ref().clone();
        // Simulate an empty store: drop the rows seeded while constructing the test env.
        SessionParameter::delete_many()
            .exec(&meta_store.conn)
            .await
            .unwrap();

        let session_init = SessionInitConfig {
            streaming_parallelism: Some("bounded(8)".to_owned()),
            streaming_parallelism_for_table: Some("default".to_owned()),
            ..Default::default()
        };
        let ctl = SessionParamsController::new(
            meta_store.clone(),
            env.notification_manager_ref(),
            session_init,
        )
        .await
        .unwrap();

        let params = ctl.get_params().await;
        assert_eq!(params.get("streaming_parallelism").unwrap(), "bounded(8)");
        // `default` must be persisted as-is, not materialized into a concrete value.
        assert_eq!(
            params.get("streaming_parallelism_for_table").unwrap(),
            "default"
        );

        // Values are persisted to the meta store.
        let persisted = SessionParameter::find_by_id("streaming_parallelism".to_owned())
            .one(&meta_store.conn)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(persisted.value, "bounded(8)");
    }

    /// Scenario 2: on an existing cluster, a persisted value takes precedence over `[session_init]`.
    #[tokio::test]
    async fn test_session_init_persisted_takes_precedence() {
        let env = MetaSrvEnv::for_test().await;
        let meta_store = env.meta_store_ref().clone();

        // First launch: seed the store and then mimic an `ALTER SYSTEM SET`.
        let ctl = SessionParamsController::new(
            meta_store.clone(),
            env.notification_manager_ref(),
            SessionInitConfig::default(),
        )
        .await
        .unwrap();
        ctl.set_param("streaming_parallelism", Some("ratio(0.5)".to_owned()))
            .await
            .unwrap();

        // Restart with a conflicting `[session_init]`: the persisted value must win.
        let session_init = SessionInitConfig {
            streaming_parallelism: Some("bounded(8)".to_owned()),
            ..Default::default()
        };
        let ctl = SessionParamsController::new(
            meta_store.clone(),
            env.notification_manager_ref(),
            session_init,
        )
        .await
        .unwrap();
        let params = ctl.get_params().await;
        assert_eq!(params.get("streaming_parallelism").unwrap(), "ratio(0.5)");
    }

    /// Scenario 3: a newly supported field missing from the persisted `session_parameter` table is
    /// seeded from `[session_init]` on restart.
    #[tokio::test]
    async fn test_session_init_seeds_missing_field() {
        let env = MetaSrvEnv::for_test().await;
        let meta_store = env.meta_store_ref().clone();

        // First launch seeds and persists defaults for all fields.
        SessionParamsController::new(
            meta_store.clone(),
            env.notification_manager_ref(),
            SessionInitConfig::default(),
        )
        .await
        .unwrap();
        // Simulate an older cluster that has no row for this field.
        SessionParameter::delete_by_id("streaming_parallelism_for_materialized_view".to_owned())
            .exec(&meta_store.conn)
            .await
            .unwrap();

        let session_init = SessionInitConfig {
            streaming_parallelism_for_materialized_view: Some("bounded(4)".to_owned()),
            ..Default::default()
        };
        let ctl = SessionParamsController::new(
            meta_store.clone(),
            env.notification_manager_ref(),
            session_init,
        )
        .await
        .unwrap();
        let params = ctl.get_params().await;
        assert_eq!(
            params
                .get("streaming_parallelism_for_materialized_view")
                .unwrap(),
            "bounded(4)"
        );
    }

    #[tokio::test]
    async fn test_session_init_invalid_value_fails_without_persisting() {
        let env = MetaSrvEnv::for_test().await;
        let meta_store = env.meta_store_ref().clone();
        // Simulate an empty store: drop the rows seeded while constructing the test env.
        SessionParameter::delete_many()
            .exec(&meta_store.conn)
            .await
            .unwrap();

        let session_init = SessionInitConfig {
            streaming_parallelism_for_backfill: Some("bounded(2)".to_owned()),
            ..Default::default()
        };
        let err = match SessionParamsController::new(
            meta_store.clone(),
            env.notification_manager_ref(),
            session_init,
        )
        .await
        {
            Ok(_) => panic!("invalid [session_init] should fail"),
            Err(err) => err,
        };
        assert!(err
            .to_string()
            .contains("Session parameters error: Invalid value `bounded(2)` for `streaming_parallelism_for_backfill`"));

        let persisted = SessionParameter::find()
            .all(&meta_store.conn)
            .await
            .unwrap();
        assert!(persisted.is_empty());
    }
}
