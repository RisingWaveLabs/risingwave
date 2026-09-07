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

use risingwave_common::acl::AclMode;
use risingwave_common::catalog::{DEFAULT_SUPER_USER, DEFAULT_SUPER_USER_FOR_PG};
use risingwave_pb::user::PbAction;
use risingwave_pb::user::grant_privilege::PbObject;
use risingwave_sqlparser::ast::Ident;

use crate::Binder;
use crate::catalog::{CatalogError, OwnedByUserCatalog};
use crate::error::ErrorCode::PermissionDenied;
use crate::error::Result;
use crate::session::SessionImpl;
use crate::user::UserId;
use crate::user::user_catalog::UserCatalog;

/// Resolve the target users of `DROP OWNED BY` / `REASSIGN OWNED BY`, rejecting the default
/// superusers. `check` is run on each resolved user for the statement-specific permission
/// checks, and `action` names the statement in error messages (e.g. "drop", "reassign").
pub(crate) fn resolve_owned_by_users(
    session: &SessionImpl,
    owners: Vec<Ident>,
    action: &str,
    check: impl Fn(&UserCatalog) -> Result<()>,
) -> Result<Vec<UserId>> {
    let user_reader = session.env().user_info_reader().read_guard();
    let mut owner_ids = Vec::with_capacity(owners.len());
    for owner in owners {
        let user_name = Binder::resolve_user_name(vec![owner].into())?;
        let user = user_reader
            .get_user_by_name(&user_name)
            .ok_or_else(|| CatalogError::not_found("user", user_name.clone()))?;
        if user.name == DEFAULT_SUPER_USER || user.name == DEFAULT_SUPER_USER_FOR_PG {
            return Err(PermissionDenied(format!(
                "cannot {} objects owned by {} because they are required by the database system",
                action, user.name
            ))
            .into());
        }
        check(user)?;
        owner_ids.push(user.id);
    }
    Ok(owner_ids)
}

#[derive(Debug)]
pub struct ObjectCheckItem {
    pub(crate) owner: UserId,
    pub(crate) mode: AclMode,
    pub(crate) name: String,
    // todo: change it to object id.
    pub(crate) object: PbObject,
}

impl ObjectCheckItem {
    pub fn new(owner: UserId, mode: AclMode, name: String, object: impl Into<PbObject>) -> Self {
        Self {
            owner,
            mode,
            name,
            object: object.into(),
        }
    }

    pub fn error_message(&self) -> String {
        let object_type = match self.object {
            PbObject::DatabaseId(_) => "database",
            PbObject::SchemaId(_) => "schema",
            PbObject::TableId(_) => "table or materialized view",
            PbObject::ViewId(_) => "view",
            PbObject::SourceId(_) => "source",
            PbObject::SinkId(_) => "sink",
            PbObject::FunctionId(_) => "function",
            PbObject::SubscriptionId(_) => "subscription",
            PbObject::ConnectionId(_) => "connection",
            PbObject::SecretId(_) => "secret",
        };
        format!(
            "permission denied for {} \"{}\": {:?}",
            object_type,
            self.name,
            PbAction::from(self.mode).as_str_name()
        )
    }
}

impl SessionImpl {
    /// Check whether the user of the current session has privileges in `items`.
    pub fn check_privileges(&self, items: &[ObjectCheckItem]) -> Result<()> {
        let user_reader = self.env().user_info_reader();
        let reader = user_reader.read_guard();

        if let Some(user) = reader.get_user_by_name(&self.user_name()) {
            if user.is_super {
                return Ok(());
            }
            for item in items {
                if item.owner == user.id {
                    continue;
                }
                let has_privilege = user.has_privilege(item.object, item.mode);
                if !has_privilege {
                    return Err(PermissionDenied(item.error_message()).into());
                }
            }
        } else {
            return Err(PermissionDenied("Session user is invalid".to_owned()).into());
        }

        Ok(())
    }

    /// Returns `true` if the user of the current session is a super user.
    pub fn is_super_user(&self) -> bool {
        let reader = self.env().user_info_reader().read_guard();

        if let Some(info) = reader.get_user_by_name(&self.user_name()) {
            info.is_super
        } else {
            false
        }
    }

    /// Check whether the user of the current session has the privilege to drop or alter the
    /// relation `relation` in the schema with name `schema_name`.
    ///
    /// Note that the right to drop or alter in PostgreSQL is special and not covered by the general
    /// `GRANT`s.
    ///
    /// > The right to drop an object, or to alter its definition in any way, is not treated as a
    /// > grantable privilege; it is inherent in the owner, and cannot be granted or revoked.
    /// >
    /// > Reference: <https://www.postgresql.org/docs/current/sql-grant.html>
    pub fn check_privilege_for_drop_alter(
        &self,
        schema_name: &str,
        relation: &impl OwnedByUserCatalog,
    ) -> Result<()> {
        let schema_owner = self
            .env()
            .catalog_reader()
            .read_guard()
            .get_schema_by_name(&self.database(), schema_name)
            .unwrap()
            .owner();

        // https://www.postgresql.org/docs/current/sql-droptable.html
        if self.user_id() != relation.owner()
            && self.user_id() != schema_owner
            && !self.is_super_user()
        {
            return Err(PermissionDenied(
                "Only the relation owner, the schema owner, and superuser can drop or alter a relation.".to_owned(),
            )
            .into());
        }

        Ok(())
    }

    /// Check whether the user of the current session has the privilege to drop or alter the
    /// `db_schema`, which is either a database or schema.
    /// > Only the owner of the database, or a superuser, can drop a database.
    /// >
    /// > Reference: <https://www.postgresql.org/docs/current/manage-ag-dropdb.html>
    /// >
    /// > A schema can only be dropped by its owner or a superuser.
    /// >
    /// > Reference: <https://www.postgresql.org/docs/current/sql-dropschema.html>
    pub fn check_privilege_for_drop_alter_db_schema(
        &self,
        db_schema: &impl OwnedByUserCatalog,
    ) -> Result<()> {
        if self.user_id() != db_schema.owner() && !self.is_super_user() {
            return Err(PermissionDenied(
                "Only the owner, and superuser can drop or alter a schema or database.".to_owned(),
            )
            .into());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::{DEFAULT_DATABASE_NAME, DEFAULT_SUPER_USER_ID};

    use super::*;
    use crate::test_utils::LocalFrontend;

    #[tokio::test]
    async fn test_check_privileges() {
        let frontend = LocalFrontend::new(Default::default()).await;
        let session = frontend.session_ref();
        let catalog_reader = session.env().catalog_reader();
        frontend.run_sql("CREATE SCHEMA schema").await.unwrap();

        let schema = catalog_reader
            .read_guard()
            .get_schema_by_name(DEFAULT_DATABASE_NAME, "schema")
            .unwrap()
            .clone();
        let check_items = vec![ObjectCheckItem::new(
            DEFAULT_SUPER_USER_ID,
            AclMode::Create,
            "schema".to_owned(),
            schema.id(),
        )];
        assert!(&session.check_privileges(&check_items).is_ok());

        frontend
            .run_sql(
                "CREATE USER user WITH NOSUPERUSER PASSWORD 'md5827ccb0eea8a706c4c34a16891f84e7b'",
            )
            .await
            .unwrap();
        let database = DEFAULT_DATABASE_NAME.to_owned();
        let user_name = "user".to_owned();
        let user_id = {
            let user_reader = session.env().user_info_reader();
            user_reader
                .read_guard()
                .get_user_by_name("user")
                .unwrap()
                .id
        };
        let session = frontend.session_user_ref(database, user_name, user_id);
        assert!(&session.check_privileges(&check_items).is_err());

        frontend
            .run_sql("GRANT CREATE ON SCHEMA schema TO user")
            .await
            .unwrap();
        assert!(&session.check_privileges(&check_items).is_ok());
    }
}
