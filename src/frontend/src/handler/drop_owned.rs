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
use crate::error::ErrorCode::PermissionDenied;
use crate::error::Result;
use crate::handler::HandlerArgs;
use crate::handler::privilege::resolve_owned_by_users;

pub async fn handle_drop_owned(
    handler_args: HandlerArgs,
    owners: Vec<Ident>,
    cascade: bool,
) -> Result<RwPgResponse> {
    let session = handler_args.session;

    let (current_user_id, current_is_super, current_is_admin) = {
        let user_reader = session.env().user_info_reader().read_guard();
        let current_user = user_reader
            .get_user_by_name(&session.user_name())
            .ok_or_else(|| PermissionDenied("Session user is invalid".to_owned()))?;
        (
            current_user.id,
            current_user.is_super,
            current_user.is_admin,
        )
    };

    let owner_ids = resolve_owned_by_users(&session, owners, "drop", |user| {
        // PostgreSQL requires membership of the target roles. RisingWave has no role
        // membership, so only allow superusers or the target user themselves.
        if !current_is_super && user.id != current_user_id {
            return Err(PermissionDenied(format!(
                "permission denied to drop objects owned by {}",
                user.name
            ))
            .into());
        }
        if user.is_admin && !current_is_admin {
            return Err(PermissionDenied(format!(
                "only admin users can drop objects owned by admin user {}",
                user.name
            ))
            .into());
        }
        Ok(())
    })?;

    let database_id = {
        let catalog_reader = session.env().catalog_reader().read_guard();
        catalog_reader
            .get_database_by_name(&session.database())?
            .id()
    };

    let catalog_writer = session.catalog_writer()?;
    catalog_writer
        .drop_owned(owner_ids, database_id, cascade)
        .await?;

    Ok(PgResponse::empty_result(StatementType::DROP_OWNED))
}
