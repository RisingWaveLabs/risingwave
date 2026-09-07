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

use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_sqlparser::ast::Ident;

use super::RwPgResponse;
use crate::Binder;
use crate::catalog::CatalogError;
use crate::error::ErrorCode::PermissionDenied;
use crate::error::Result;
use crate::handler::HandlerArgs;
use crate::handler::privilege::resolve_owned_by_users;

pub async fn handle_reassign_owned(
    handler_args: HandlerArgs,
    old_owners: Vec<Ident>,
    new_owner: Ident,
) -> Result<RwPgResponse> {
    let session = handler_args.session;

    // PostgreSQL requires membership of both the old and new roles. RisingWave has no role
    // membership, so restrict the statement to superusers.
    if !session.is_super_user() {
        return Err(
            PermissionDenied("must be superuser to reassign owned objects".to_owned()).into(),
        );
    }

    let old_owner_ids = resolve_owned_by_users(&session, old_owners, "reassign", |user| {
        if user.is_admin {
            return Err(PermissionDenied(format!(
                "cannot reassign objects owned by admin user {}",
                user.name
            ))
            .into());
        }
        Ok(())
    })?;

    let new_owner_id = {
        let user_name = Binder::resolve_user_name(vec![new_owner].into())?;
        let user_reader = session.env().user_info_reader().read_guard();
        user_reader
            .get_user_by_name(&user_name)
            .ok_or_else(|| CatalogError::not_found("user", user_name))?
            .id
    };

    let database_id = {
        let catalog_reader = session.env().catalog_reader().read_guard();
        catalog_reader
            .get_database_by_name(&session.database())?
            .id()
    };

    let catalog_writer = session.catalog_writer()?;
    catalog_writer
        .reassign_owned(old_owner_ids, new_owner_id, database_id)
        .await?;

    Ok(PgResponse::empty_result(StatementType::REASSIGN_OWNED))
}
