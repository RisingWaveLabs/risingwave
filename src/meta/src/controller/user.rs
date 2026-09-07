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

use itertools::Itertools;
use risingwave_common::catalog::{DEFAULT_SUPER_USER, DEFAULT_SUPER_USER_FOR_PG};
use risingwave_meta_model::object::ObjectType;
use risingwave_meta_model::prelude::{Object, User, UserDefaultPrivilege, UserPrivilege};
use risingwave_meta_model::user_privilege::Action;
use risingwave_meta_model::{
    AuthInfo, DatabaseId, DefaultPrivilegeId, PrivilegeId, SchemaId, UserId, object, user,
    user_default_privilege, user_privilege,
};
use risingwave_pb::common::PbObjectType;
use risingwave_pb::meta::subscribe_response::{
    Info as NotificationInfo, Operation as NotificationOperation,
};
use risingwave_pb::user::update_user_request::PbUpdateField;
use risingwave_pb::user::{PbAction, PbGrantPrivilege, PbUserInfo};
use sea_orm::ActiveValue::Set;
use sea_orm::sea_query::{OnConflict, SimpleExpr, Value};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, EntityTrait, IntoActiveModel, JoinType,
    PaginatorTrait, QueryFilter, QuerySelect, RelationTrait, TransactionTrait,
};

use crate::controller::catalog::CatalogController;
use crate::controller::utils::{
    PartialUserPrivilege, check_user_name_duplicate, ensure_object_id,
    ensure_privileges_not_referred, ensure_user_id, extract_grant_obj_id,
    get_iceberg_related_object_ids, get_index_state_tables_by_table_id, get_internal_tables_by_id,
    get_object_owner, get_referring_privileges_cascade, get_user_privilege, list_user_info_by_ids,
    upsert_user_privileges,
};
use crate::manager::{IGNORED_NOTIFICATION_VERSION, NotificationVersion};
use crate::{MetaError, MetaResult};

impl CatalogController {
    pub(crate) async fn notify_users_update(
        &self,
        user_infos: Vec<PbUserInfo>,
    ) -> NotificationVersion {
        let mut version = 0;
        for info in user_infos {
            version = self
                .notify_frontend(NotificationOperation::Update, NotificationInfo::User(info))
                .await;
        }
        version
    }

    pub async fn create_user(&self, pb_user: PbUserInfo) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        check_user_name_duplicate(&pb_user.name, &txn).await?;

        let grant_privileges = pb_user.grant_privileges.clone();
        let user: user::ActiveModel = pb_user.into();
        let user = user.insert(&txn).await?;

        if !grant_privileges.is_empty() {
            let mut privileges = vec![];
            for gp in &grant_privileges {
                let id = extract_grant_obj_id(gp.get_object()?);
                for action_with_opt in &gp.action_with_opts {
                    privileges.push(user_privilege::ActiveModel {
                        user_id: Set(user.user_id),
                        oid: Set(id),
                        granted_by: Set(action_with_opt.granted_by as _),
                        action: Set(action_with_opt.get_action()?.into()),
                        with_grant_option: Set(action_with_opt.with_grant_option),
                        ..Default::default()
                    });
                }
            }
            UserPrivilege::insert_many(privileges).exec(&txn).await?;
        }
        txn.commit().await?;

        let mut user_info: PbUserInfo = user.into();
        user_info.grant_privileges = grant_privileges;
        let version = self
            .notify_frontend(
                NotificationOperation::Add,
                NotificationInfo::User(user_info),
            )
            .await;

        Ok(version)
    }

    pub async fn update_user(
        &self,
        update_user: PbUserInfo,
        update_fields: &[PbUpdateField],
    ) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let rename_flag = update_fields.contains(&PbUpdateField::Rename);
        if rename_flag {
            check_user_name_duplicate(&update_user.name, &inner.db).await?;
        }

        let user = User::find_by_id(update_user.id as UserId)
            .one(&inner.db)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("user", update_user.id))?;
        let mut user = user.into_active_model();
        update_fields.iter().for_each(|&field| match field {
            PbUpdateField::Unspecified => unreachable!(),
            PbUpdateField::Super => user.is_super = Set(update_user.is_super),
            PbUpdateField::Login => user.can_login = Set(update_user.can_login),
            PbUpdateField::CreateDb => user.can_create_db = Set(update_user.can_create_db),
            PbUpdateField::CreateUser => user.can_create_user = Set(update_user.can_create_user),
            PbUpdateField::AuthInfo => {
                user.auth_info = Set(update_user.auth_info.as_ref().map(AuthInfo::from))
            }
            PbUpdateField::Rename => user.name = Set(update_user.name.clone()),
            PbUpdateField::Admin => user.is_admin = Set(update_user.is_admin),
        });

        let user = user.update(&inner.db).await?;
        let mut user_info: PbUserInfo = user.into();
        user_info.grant_privileges = get_user_privilege(user_info.id as _, &inner.db).await?;
        let version = self
            .notify_frontend(
                NotificationOperation::Update,
                NotificationInfo::User(user_info),
            )
            .await;

        Ok(version)
    }

    #[cfg(test)]
    pub async fn get_user(&self, id: UserId) -> MetaResult<user::Model> {
        let inner = self.inner.read().await;
        let user = User::find_by_id(id)
            .one(&inner.db)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("user", id))?;
        Ok(user)
    }

    #[cfg(test)]
    pub async fn get_user_by_name(&self, name: &str) -> MetaResult<user::Model> {
        let inner = self.inner.read().await;
        let user = User::find()
            .filter(user::Column::Name.eq(name))
            .one(&inner.db)
            .await?
            .ok_or_else(|| anyhow::anyhow!("user {name} not found"))?;
        Ok(user)
    }

    pub async fn drop_user(&self, user_id: UserId) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        let user = User::find_by_id(user_id)
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("user", user_id))?;
        if user.name == DEFAULT_SUPER_USER || user.name == DEFAULT_SUPER_USER_FOR_PG {
            return Err(MetaError::permission_denied(format!(
                "drop default super user {} is not allowed",
                user.name
            )));
        }

        // check if the user is the owner of any objects.
        let count = Object::find()
            .filter(object::Column::OwnerId.eq(user_id))
            .count(&txn)
            .await?;
        if count != 0 {
            return Err(MetaError::permission_denied(format!(
                "drop user {} is not allowed, because it owns {} objects",
                user.name, count
            )));
        }

        // check if the user granted any privileges to other users.
        let count = UserPrivilege::find()
            .filter(user_privilege::Column::GrantedBy.eq(user_id))
            .count(&txn)
            .await?;
        if count != 0 {
            return Err(MetaError::permission_denied(format!(
                "drop user {} is not allowed, because it granted {} privileges to others",
                user.name, count
            )));
        }

        let res = User::delete_by_id(user_id).exec(&txn).await?;
        if res.rows_affected != 1 {
            return Err(MetaError::catalog_id_not_found("user", user_id));
        }
        txn.commit().await?;

        let version = self
            .notify_frontend(
                NotificationOperation::Delete,
                NotificationInfo::User(PbUserInfo {
                    id: user_id as _,
                    ..Default::default()
                }),
            )
            .await;

        Ok(version)
    }

    pub async fn grant_privilege(
        &self,
        user_ids: Vec<UserId>,
        new_grant_privileges: &[PbGrantPrivilege],
        grantor: UserId,
        with_grant_option: bool,
    ) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        for user_id in &user_ids {
            ensure_user_id(*user_id, &txn).await?;
        }

        let mut privileges = vec![];
        for gp in new_grant_privileges {
            let id = extract_grant_obj_id(gp.get_object()?);
            let internal_table_ids = get_internal_tables_by_id(id.as_job_id(), &txn).await?;
            let index_state_table_ids =
                get_index_state_tables_by_table_id(id.as_table_id(), &txn).await?;
            for action_with_opt in &gp.action_with_opts {
                let action = action_with_opt.get_action()?.into();
                privileges.push(user_privilege::ActiveModel {
                    oid: Set(id),
                    granted_by: Set(grantor),
                    action: Set(action),
                    with_grant_option: Set(with_grant_option),
                    ..Default::default()
                });
                if action == Action::Select {
                    privileges.extend(
                        internal_table_ids
                            .iter()
                            .chain(index_state_table_ids.iter())
                            .map(|&tid| user_privilege::ActiveModel {
                                oid: Set(tid.as_object_id()),
                                granted_by: Set(grantor),
                                action: Set(Action::Select),
                                with_grant_option: Set(with_grant_option),
                                ..Default::default()
                            }),
                    );
                    let iceberg_privilege_object_ids =
                        get_iceberg_related_object_ids(id, &txn).await?;
                    privileges.extend(iceberg_privilege_object_ids.iter().map(
                        |&iceberg_object_id| user_privilege::ActiveModel {
                            oid: Set(iceberg_object_id),
                            granted_by: Set(grantor),
                            action: Set(action),
                            with_grant_option: Set(with_grant_option),
                            ..Default::default()
                        },
                    ));
                }
            }
        }

        // check whether grantor has the privilege to grant the privilege.
        let user = User::find_by_id(grantor)
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("user", grantor))?;
        let mut filtered_privileges = vec![];
        if !user.is_super {
            for mut privilege in privileges {
                if grantor == get_object_owner(*privilege.oid.as_ref(), &txn).await? {
                    filtered_privileges.push(privilege);
                    continue;
                }
                let filter = user_privilege::Column::UserId
                    .eq(grantor)
                    .and(user_privilege::Column::Oid.eq(*privilege.oid.as_ref()))
                    .and(user_privilege::Column::Action.eq(*privilege.action.as_ref()))
                    .and(user_privilege::Column::WithGrantOption.eq(true));
                let privilege_id: Option<PrivilegeId> = UserPrivilege::find()
                    .select_only()
                    .column(user_privilege::Column::Id)
                    .filter(filter)
                    .into_tuple()
                    .one(&txn)
                    .await?;
                let Some(privilege_id) = privilege_id else {
                    tracing::warn!(
                        "user {} don't have privilege {:?} or grant option",
                        grantor,
                        privilege.action
                    );
                    continue;
                };
                privilege.dependent_id = Set(Some(privilege_id));
                filtered_privileges.push(privilege);
            }
        } else {
            filtered_privileges = privileges;
        }

        // insert privileges
        let user_privileges = user_ids
            .iter()
            .flat_map(|user_id| {
                filtered_privileges.iter().map(|p| {
                    let mut p = p.clone();
                    p.user_id = Set(*user_id);
                    p
                })
            })
            .collect_vec();
        upsert_user_privileges(&txn, user_privileges).await?;

        let user_infos = list_user_info_by_ids(user_ids, &txn).await?;

        txn.commit().await?;

        let version = self.notify_users_update(user_infos).await;
        Ok(version)
    }

    pub async fn revoke_privilege(
        &self,
        user_ids: Vec<UserId>,
        revoke_grant_privileges: &[PbGrantPrivilege],
        granted_by: UserId,
        revoke_by: UserId,
        revoke_grant_option: bool,
        cascade: bool,
    ) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        for user_id in &user_ids {
            ensure_user_id(*user_id, &txn).await?;
        }
        // check whether revoke has the privilege to grant the privilege.
        let revoke_user = User::find_by_id(revoke_by)
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("user", revoke_by))?;

        // check whether user can revoke the privilege.
        if !revoke_user.is_super && granted_by != revoke_by {
            let granted_user_name: String = User::find_by_id(granted_by)
                .select_only()
                .column(user::Column::Name)
                .into_tuple()
                .one(&txn)
                .await?
                .ok_or_else(|| MetaError::catalog_id_not_found("user", granted_by))?;
            return Err(MetaError::permission_denied(format!(
                "user {} is not super, can't revoke privileges for {}",
                revoke_user.name, granted_user_name
            )));
        }

        let mut revoke_items = HashMap::new();
        for privilege in revoke_grant_privileges {
            let obj = extract_grant_obj_id(privilege.get_object()?);
            let internal_table_ids = get_internal_tables_by_id(obj.as_job_id(), &txn).await?;
            let index_state_table_ids =
                get_index_state_tables_by_table_id(obj.as_table_id(), &txn).await?;
            let mut include_select = false;
            let actions = privilege
                .action_with_opts
                .iter()
                .map(|ao| {
                    let action = Action::from(ao.get_action().unwrap());
                    if action == Action::Select {
                        include_select = true;
                    }
                    action
                })
                .collect_vec();
            revoke_items.insert(obj, actions);
            if include_select {
                revoke_items.extend(
                    internal_table_ids
                        .iter()
                        .chain(index_state_table_ids.iter())
                        .map(|&tid| (tid.as_object_id(), vec![Action::Select])),
                );
                let iceberg_privilege_object_ids =
                    get_iceberg_related_object_ids(obj, &txn).await?;
                if !iceberg_privilege_object_ids.is_empty() {
                    revoke_items.extend(
                        iceberg_privilege_object_ids
                            .into_iter()
                            .map(|iceberg_object_id| (iceberg_object_id, vec![Action::Select])),
                    );
                }
            }
        }

        let filter = if !revoke_user.is_super {
            // ensure user have grant options or is owner of the object.
            for (obj, actions) in &revoke_items {
                if revoke_by == get_object_owner(*obj, &txn).await? {
                    continue;
                }
                let owned_actions: HashSet<Action> = UserPrivilege::find()
                    .select_only()
                    .column(user_privilege::Column::Action)
                    .filter(
                        user_privilege::Column::UserId
                            .eq(granted_by)
                            .and(user_privilege::Column::Oid.eq(*obj))
                            .and(user_privilege::Column::WithGrantOption.eq(true)),
                    )
                    .into_tuple::<Action>()
                    .all(&txn)
                    .await?
                    .into_iter()
                    .collect();
                if actions.iter().any(|ac| !owned_actions.contains(ac)) {
                    return Err(MetaError::permission_denied(format!(
                        "user {} don't have privileges {:?} or grant option",
                        revoke_user.name, actions,
                    )));
                }
            }

            user_privilege::Column::GrantedBy
                .eq(granted_by)
                .and(user_privilege::Column::UserId.is_in(user_ids.clone()))
        } else {
            user_privilege::Column::UserId.is_in(user_ids.clone())
        };
        let mut root_user_privileges: Vec<PartialUserPrivilege> = vec![];
        for (obj, actions) in revoke_items {
            let filter = filter
                .clone()
                .and(user_privilege::Column::Oid.eq(obj))
                .and(user_privilege::Column::Action.is_in(actions));
            root_user_privileges.extend(
                UserPrivilege::find()
                    .select_only()
                    .columns([user_privilege::Column::Id, user_privilege::Column::UserId])
                    .filter(filter)
                    .into_partial_model()
                    .all(&txn)
                    .await?,
            );
        }
        if root_user_privileges.is_empty() {
            tracing::warn!("no privilege to revoke, ignore it");
            return Ok(IGNORED_NOTIFICATION_VERSION);
        }

        // check if the user granted any privileges to other users.
        let root_privilege_ids = root_user_privileges.iter().map(|ur| ur.id).collect_vec();
        let (all_privilege_ids, to_update_user_ids) = if !cascade {
            ensure_privileges_not_referred(root_privilege_ids.clone(), &txn).await?;
            (
                root_privilege_ids.clone(),
                root_user_privileges
                    .iter()
                    .map(|ur| ur.user_id)
                    .collect_vec(),
            )
        } else {
            let all_user_privileges =
                get_referring_privileges_cascade(root_privilege_ids.clone(), &txn).await?;
            (
                all_user_privileges.iter().map(|ur| ur.id).collect_vec(),
                all_user_privileges
                    .iter()
                    .map(|ur| ur.user_id)
                    .collect_vec(),
            )
        };

        if revoke_grant_option {
            UserPrivilege::update_many()
                .col_expr(
                    user_privilege::Column::WithGrantOption,
                    SimpleExpr::Value(Value::Bool(Some(false))),
                )
                .filter(
                    user_privilege::Column::Id
                        .is_in(all_privilege_ids)
                        .and(user_privilege::Column::WithGrantOption.eq(true)),
                )
                .exec(&txn)
                .await?;
        } else {
            // The dependent privileges will be deleted cascade.
            UserPrivilege::delete_many()
                .filter(user_privilege::Column::Id.is_in(root_privilege_ids))
                .exec(&txn)
                .await?;
        }

        let user_infos = list_user_info_by_ids(to_update_user_ids, &txn).await?;

        txn.commit().await?;

        let version = self.notify_users_update(user_infos).await;
        Ok(version)
    }

    pub async fn grant_default_privileges(
        &self,
        user_ids: Vec<UserId>,
        database_id: DatabaseId,
        schema_ids: Vec<SchemaId>,
        grantor: UserId,
        actions: Vec<PbAction>,
        object_type: PbObjectType,
        grantees: Vec<UserId>,
        with_grant_option: bool,
    ) -> MetaResult<()> {
        tracing::debug!(
            ?user_ids,
            %database_id,
            ?schema_ids,
            ?actions,
            ?object_type,
            ?grantees,
            with_grant_option,
            "grant default privileges",
        );
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        for user_id in &user_ids {
            ensure_user_id(*user_id, &txn).await?;
        }
        ensure_object_id(ObjectType::Database, database_id, &txn).await?;
        for schema_id in &schema_ids {
            ensure_object_id(ObjectType::Schema, *schema_id, &txn).await?;
        }
        for grantee in &grantees {
            ensure_user_id(*grantee, &txn).await?;
        }
        if object_type == PbObjectType::Schema {
            assert!(
                schema_ids.is_empty(),
                "schema_ids should be empty when object_type is Schema"
            );

            // Note that the UNIQUE constraint does not treat NULL values as equal, we cannot rely on conflict check
            // to update it and have to check existing default privileges manually.
            let actions = actions.iter().map(|&a| Action::from(a)).collect_vec();
            let existing_default_privileges: HashMap<_, _> = UserDefaultPrivilege::find()
                .select_only()
                .columns([
                    user_default_privilege::Column::Id,
                    user_default_privilege::Column::UserId,
                    user_default_privilege::Column::Grantee,
                    user_default_privilege::Column::Action,
                ])
                .filter(
                    user_default_privilege::Column::DatabaseId
                        .eq(database_id)
                        .and(user_default_privilege::Column::ObjectType.eq(ObjectType::Schema))
                        .and(user_default_privilege::Column::UserId.is_in(user_ids.clone()))
                        .and(user_default_privilege::Column::Grantee.is_in(grantees.clone()))
                        .and(user_default_privilege::Column::Action.is_in(actions.clone())),
                )
                .into_tuple::<(DefaultPrivilegeId, UserId, UserId, Action)>()
                .all(&txn)
                .await?
                .into_iter()
                .map(|(id, user_id, grantee, action)| ((user_id, grantee, action), id))
                .collect();

            for user_id in user_ids {
                for grantee in &grantees {
                    for action in &actions {
                        if let Some(existing_id) =
                            existing_default_privileges.get(&(user_id, *grantee, *action))
                            && with_grant_option
                        {
                            // If the default privilege already exists, we should update the grant option.
                            UserDefaultPrivilege::update(user_default_privilege::ActiveModel {
                                id: Set(*existing_id),
                                with_grant_option: Set(true),
                                granted_by: Set(grantor as _),
                                ..Default::default()
                            })
                            .exec(&txn)
                            .await?;
                        } else {
                            UserDefaultPrivilege::insert(user_default_privilege::ActiveModel {
                                id: Default::default(),
                                database_id: Set(database_id),
                                schema_id: Set(None),
                                object_type: Set(ObjectType::Schema),
                                for_materialized_view: Set(false),
                                user_id: Set(user_id),
                                grantee: Set(*grantee),
                                granted_by: Set(grantor as _),
                                action: Set(*action),
                                with_grant_option: Set(with_grant_option),
                            })
                            .exec(&txn)
                            .await?;
                        }
                    }
                }
            }
        } else {
            let mut default_privileges = vec![];
            for user_id in user_ids {
                for grantee in &grantees {
                    for action in &actions {
                        if schema_ids.is_empty() {
                            default_privileges.push(user_default_privilege::ActiveModel {
                                id: Default::default(),
                                database_id: Set(database_id),
                                schema_id: Set(None),
                                object_type: Set(object_type.into()),
                                for_materialized_view: Set(object_type == PbObjectType::Mview),
                                user_id: Set(user_id),
                                grantee: Set(*grantee),
                                granted_by: Set(grantor as _),
                                action: Set((*action).into()),
                                with_grant_option: Set(with_grant_option),
                            });
                            continue;
                        }
                        for schema_id in &schema_ids {
                            default_privileges.push(user_default_privilege::ActiveModel {
                                id: Default::default(),
                                database_id: Set(database_id),
                                schema_id: Set(Some(*schema_id)),
                                object_type: Set(object_type.into()),
                                for_materialized_view: Set(object_type == PbObjectType::Mview),
                                user_id: Set(user_id),
                                grantee: Set(*grantee),
                                granted_by: Set(grantor as _),
                                action: Set((*action).into()),
                                with_grant_option: Set(with_grant_option),
                            });
                        }
                    }
                }
            }

            let mut on_conflict = OnConflict::columns([
                user_default_privilege::Column::UserId,
                user_default_privilege::Column::DatabaseId,
                user_default_privilege::Column::SchemaId,
                user_default_privilege::Column::ObjectType,
                user_default_privilege::Column::ForMaterializedView,
                user_default_privilege::Column::Grantee,
                user_default_privilege::Column::Action,
            ]);
            if with_grant_option {
                on_conflict.update_column(user_default_privilege::Column::WithGrantOption);
            } else {
                // Workaround to support MYSQL for `DO NOTHING`.
                on_conflict.update_column(user_default_privilege::Column::UserId);
            }
            UserDefaultPrivilege::insert_many(default_privileges)
                .on_conflict(on_conflict)
                .do_nothing()
                .exec(&txn)
                .await?;
        }

        txn.commit().await?;
        Ok(())
    }

    pub async fn revoke_default_privileges(
        &self,
        user_ids: Vec<UserId>,
        database_id: DatabaseId,
        schema_ids: Vec<SchemaId>,
        actions: Vec<PbAction>,
        object_type: PbObjectType,
        grantees: Vec<UserId>,
        revoke_grant_option: bool,
    ) -> MetaResult<()> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        for user_id in &user_ids {
            ensure_user_id(*user_id, &txn).await?;
        }

        let schema_filter = if schema_ids.is_empty() {
            user_default_privilege::Column::SchemaId.is_null()
        } else {
            user_default_privilege::Column::SchemaId.is_in(schema_ids)
        };
        let filter = user_default_privilege::Column::DatabaseId
            .eq(database_id)
            .and(schema_filter)
            .and(user_default_privilege::Column::UserId.is_in(user_ids))
            .and(user_default_privilege::Column::ObjectType.eq(ObjectType::from(object_type)))
            .and(user_default_privilege::Column::Grantee.is_in(grantees))
            .and(
                user_default_privilege::Column::Action
                    .is_in(actions.iter().map(|&a| Action::from(a))),
            );

        if revoke_grant_option {
            // update the `with_grant_option` field to false
            let res = UserDefaultPrivilege::update_many()
                .col_expr(
                    user_default_privilege::Column::WithGrantOption,
                    SimpleExpr::Value(Value::Bool(Some(false))),
                )
                .filter(filter.and(user_default_privilege::Column::WithGrantOption.eq(true)))
                .exec(&txn)
                .await?;
            tracing::info!(
                "revoke {count} grant option for default privileges",
                count = res.rows_affected
            );
        } else {
            let res = UserDefaultPrivilege::delete_many()
                .filter(filter)
                .exec(&txn)
                .await?;
            tracing::info!(
                "revoke {count} default privileges",
                count = res.rows_affected
            );
        }

        txn.commit().await?;
        Ok(())
    }
}

impl CatalogController {
    /// `DROP OWNED BY`: revoke all privileges granted by or to the given users on objects in
    /// the given database or on `Database` objects themselves (PG "shared objects"
    /// semantics), and delete their default privileges in the database.
    ///
    /// Unlike PostgreSQL, objects owned by the users are NOT dropped: the statement fails if
    /// any exist, and their ownership should be transferred with `REASSIGN OWNED BY` (or the
    /// objects dropped) first.
    pub async fn drop_owned(
        &self,
        user_ids: Vec<UserId>,
        database_id: DatabaseId,
        cascade: bool,
    ) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        for user_id in &user_ids {
            ensure_user_id(*user_id, &txn).await?;
        }

        let owned_count = Object::find()
            .filter(
                object::Column::OwnerId.is_in(user_ids.clone()).and(
                    object::Column::DatabaseId
                        .eq(database_id)
                        .or(object::Column::ObjType.eq(ObjectType::Database)),
                ),
            )
            .count(&txn)
            .await?;
        if owned_count != 0 {
            return Err(MetaError::permission_denied(format!(
                "DROP OWNED in RisingWave only revokes privileges and does not drop \
                 objects, but the specified user(s) still own {owned_count} object(s) \
                 in the current database (or own databases); use REASSIGN OWNED BY to \
                 transfer the ownership or drop the objects first"
            )));
        }

        let updated_user_ids = drop_owned_privileges(&user_ids, database_id, cascade, &txn).await?;
        let user_infos = list_user_info_by_ids(updated_user_ids, &txn).await?;

        txn.commit().await?;

        // Returns `IGNORED_NOTIFICATION_VERSION` when there's no user to notify.
        Ok(self.notify_users_update(user_infos).await)
    }
}

/// For `DROP OWNED BY`: delete all privileges granted by or to the given users on objects in
/// the given database or on `Database` objects themselves (PG "shared objects" semantics),
/// along with the users' default privileges in that database.
///
/// With `cascade = false`, returns an error if any privilege to delete has been re-granted
/// to a user outside the deleted set.
///
/// Returns the ids of users whose privilege sets changed.
async fn drop_owned_privileges<C>(
    user_ids: &[UserId],
    database_id: DatabaseId,
    cascade: bool,
    db: &C,
) -> MetaResult<Vec<UserId>>
where
    C: ConnectionTrait,
{
    let root_privileges: Vec<(PrivilegeId, UserId)> = UserPrivilege::find()
        .select_only()
        .columns([user_privilege::Column::Id, user_privilege::Column::UserId])
        .join(JoinType::InnerJoin, user_privilege::Relation::Object.def())
        .filter(
            user_privilege::Column::UserId
                .is_in(user_ids.iter().cloned())
                .or(user_privilege::Column::GrantedBy.is_in(user_ids.iter().cloned())),
        )
        .filter(
            object::Column::DatabaseId
                .eq(database_id)
                .or(object::Column::ObjType.eq(ObjectType::Database)),
        )
        .into_tuple()
        .all(db)
        .await?;

    let root_privilege_ids = root_privileges.iter().map(|(id, _)| *id).collect_vec();
    let mut updated_user_ids: HashSet<UserId> = root_privileges
        .iter()
        .map(|(_, user_id)| *user_id)
        .collect();

    if !root_privilege_ids.is_empty() {
        if !cascade {
            let outside_count = UserPrivilege::find()
                .filter(
                    user_privilege::Column::DependentId
                        .is_in(root_privilege_ids.clone())
                        .and(user_privilege::Column::Id.is_not_in(root_privilege_ids.clone())),
                )
                .count(db)
                .await?;
            if outside_count != 0 {
                return Err(MetaError::permission_denied(format!(
                    "{} dependent privileges exist, use CASCADE to revoke them too",
                    outside_count
                )));
            }
        } else {
            let all_privileges =
                get_referring_privileges_cascade(root_privilege_ids.clone(), db).await?;
            updated_user_ids.extend(all_privileges.iter().map(|p| p.user_id));
        }

        // Dependent privileges are deleted by cascade.
        UserPrivilege::delete_many()
            .filter(user_privilege::Column::Id.is_in(root_privilege_ids))
            .exec(db)
            .await?;
    }

    // Drop the users' default privileges in the database, including those where they are
    // the grantee or the grantor, so that no new privileges referencing them are generated.
    UserDefaultPrivilege::delete_many()
        .filter(user_default_privilege::Column::DatabaseId.eq(database_id))
        .filter(
            user_default_privilege::Column::UserId
                .is_in(user_ids.iter().cloned())
                .or(user_default_privilege::Column::Grantee.is_in(user_ids.iter().cloned()))
                .or(user_default_privilege::Column::GrantedBy.is_in(user_ids.iter().cloned())),
        )
        .exec(db)
        .await?;

    Ok(updated_user_ids.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use risingwave_pb::user::grant_privilege::{PbActionWithGrantOption, PbObject};

    use super::*;
    use crate::manager::MetaSrvEnv;

    const TEST_DATABASE_ID: DatabaseId = DatabaseId::new(1);
    const TEST_ROOT_USER_ID: UserId = UserId::new(1);

    fn make_test_user(name: &str) -> PbUserInfo {
        PbUserInfo {
            name: name.to_owned(),
            ..Default::default()
        }
    }

    fn make_privilege(
        object: PbObject,
        actions: &[PbAction],
        with_grant_option: bool,
    ) -> PbGrantPrivilege {
        PbGrantPrivilege {
            object: Some(object),
            action_with_opts: actions
                .iter()
                .map(|&action| PbActionWithGrantOption {
                    action: action as _,
                    with_grant_option,
                    ..Default::default()
                })
                .collect(),
        }
    }

    #[tokio::test]
    async fn test_user_and_privilege() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;
        mgr.create_user(make_test_user("test_user_1")).await?;
        mgr.create_user(make_test_user("test_user_2")).await?;
        let user_1 = mgr.get_user_by_name("test_user_1").await?;
        let user_2 = mgr.get_user_by_name("test_user_2").await?;

        assert!(
            mgr.create_user(make_test_user("test_user_1"))
                .await
                .is_err(),
            "user_1 already exists"
        );
        mgr.update_user(
            PbUserInfo {
                id: user_1.user_id as _,
                name: "test_user_1_new".to_owned(),
                ..Default::default()
            },
            &[PbUpdateField::Rename],
        )
        .await?;
        let user_1 = mgr.get_user(user_1.user_id).await?;
        assert_eq!(user_1.name, "test_user_1_new".to_owned());

        let conn_with_option = make_privilege(TEST_DATABASE_ID.into(), &[PbAction::Connect], true);
        let create_without_option =
            make_privilege(TEST_DATABASE_ID.into(), &[PbAction::Create], false);
        // ROOT grant CONN with grant option to user_1.
        mgr.grant_privilege(
            vec![user_1.user_id],
            std::slice::from_ref(&conn_with_option),
            TEST_ROOT_USER_ID,
            true,
        )
        .await?;
        // ROOT grant CREATE without grant option to user_1.
        mgr.grant_privilege(
            vec![user_1.user_id],
            std::slice::from_ref(&create_without_option),
            TEST_ROOT_USER_ID,
            false,
        )
        .await?;
        // user_1 grant CONN with grant option to user_2.
        mgr.grant_privilege(
            vec![user_2.user_id],
            std::slice::from_ref(&conn_with_option),
            user_1.user_id,
            true,
        )
        .await?;
        mgr.grant_privilege(
            vec![user_2.user_id],
            std::slice::from_ref(&create_without_option),
            user_1.user_id,
            false,
        )
        .await?;

        assert!(
            mgr.drop_user(user_1.user_id).await.is_err(),
            "user_1 can't be dropped"
        );

        let privilege_1 = get_user_privilege(user_1.user_id, &mgr.inner.read().await.db).await?;
        assert_eq!(privilege_1.len(), 2);
        assert!(
            privilege_1
                .iter()
                .all(|gp| gp.object == Some(TEST_DATABASE_ID.into())
                    && gp.action_with_opts[0].granted_by == TEST_ROOT_USER_ID)
        );

        let privilege_2 = get_user_privilege(user_2.user_id, &mgr.inner.read().await.db).await?;
        assert_eq!(privilege_2.len(), 1);
        assert!(
            privilege_2
                .iter()
                .all(|gp| gp.object == Some(TEST_DATABASE_ID.into())
                    && gp.action_with_opts[0].granted_by == user_1.user_id
                    && gp.action_with_opts[0].with_grant_option)
        );

        // revoke privilege for others by non-super user.
        assert!(
            mgr.revoke_privilege(
                vec![user_1.user_id],
                std::slice::from_ref(&conn_with_option),
                TEST_ROOT_USER_ID,
                user_2.user_id,
                false,
                false
            )
            .await
            .is_err(),
            "user_2 can't revoke for user_1"
        );

        // revoke privilege without grant option.
        assert!(
            mgr.revoke_privilege(
                vec![user_2.user_id],
                std::slice::from_ref(&create_without_option),
                user_1.user_id,
                user_1.user_id,
                false,
                false
            )
            .await
            .is_err(),
            "user_2 don't have grant option for CREATE"
        );

        // revoke referred privilege in restrict mode.
        assert!(
            mgr.revoke_privilege(
                vec![user_1.user_id],
                std::slice::from_ref(&conn_with_option),
                TEST_ROOT_USER_ID,
                TEST_ROOT_USER_ID,
                false,
                false
            )
            .await
            .is_err(),
            "permission deny in restrict mode, CONN granted to user_2"
        );

        // revoke non-referred privilege in restrict mode.
        mgr.revoke_privilege(
            vec![user_1.user_id],
            std::slice::from_ref(&create_without_option),
            TEST_ROOT_USER_ID,
            TEST_ROOT_USER_ID,
            false,
            false,
        )
        .await?;

        let privilege_1 = get_user_privilege(user_1.user_id, &mgr.inner.read().await.db).await?;
        assert_eq!(privilege_1.len(), 1);
        assert!(
            privilege_1
                .iter()
                .all(|gp| gp.object == Some(TEST_DATABASE_ID.into())
                    && gp.action_with_opts[0].action == PbAction::Connect as i32)
        );

        // revoke grant option for referred privilege in cascade mode.
        mgr.revoke_privilege(
            vec![user_1.user_id],
            std::slice::from_ref(&conn_with_option),
            TEST_ROOT_USER_ID,
            TEST_ROOT_USER_ID,
            true,
            true,
        )
        .await?;
        let privilege_1 = get_user_privilege(user_1.user_id, &mgr.inner.read().await.db).await?;
        assert_eq!(privilege_1.len(), 1);
        assert!(
            privilege_1
                .iter()
                .all(|gp| gp.object == Some(TEST_DATABASE_ID.into())
                    && gp.action_with_opts[0].action == PbAction::Connect as i32
                    && !gp.action_with_opts[0].with_grant_option)
        );
        let privilege_2 = get_user_privilege(user_2.user_id, &mgr.inner.read().await.db).await?;
        assert_eq!(privilege_2.len(), 1);
        assert!(
            privilege_2
                .iter()
                .all(|gp| gp.object == Some(TEST_DATABASE_ID.into())
                    && gp.action_with_opts[0].action == PbAction::Connect as i32
                    && !gp.action_with_opts[0].with_grant_option)
        );

        // revoke referred privilege in cascade mode.
        mgr.revoke_privilege(
            vec![user_1.user_id],
            std::slice::from_ref(&conn_with_option),
            TEST_ROOT_USER_ID,
            TEST_ROOT_USER_ID,
            false,
            true,
        )
        .await?;
        let privilege_1 = get_user_privilege(user_1.user_id, &mgr.inner.read().await.db).await?;
        assert!(privilege_1.is_empty());
        let privilege_2 = get_user_privilege(user_2.user_id, &mgr.inner.read().await.db).await?;
        assert!(privilege_2.is_empty());

        mgr.drop_user(user_1.user_id).await?;
        mgr.drop_user(user_2.user_id).await?;
        Ok(())
    }
}
