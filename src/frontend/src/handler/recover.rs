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

use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_sqlparser::ast::ObjectName;

use super::RwPgResponse;
use crate::binder::Binder;
use crate::catalog::DatabaseId;
use crate::error::{ErrorCode, Result};
use crate::handler::HandlerArgs;
use crate::session::SessionImpl;

pub(super) async fn handle_recover(
    handler_args: HandlerArgs,
    database_name: Option<ObjectName>,
) -> Result<RwPgResponse> {
    // Only permit recovery for super users.
    if !handler_args.session.is_super_user() {
        return Err(ErrorCode::PermissionDenied(
            "only superusers can trigger adhoc recovery".to_owned(),
        )
        .into());
    }
    let database_id = if let Some(database_name) = database_name {
        let database_name = Binder::resolve_database_name(database_name)?;
        Some(
            handler_args
                .session
                .env()
                .catalog_reader()
                .read_guard()
                .get_database_by_name(&database_name)?
                .id(),
        )
    } else {
        None
    };
    do_recover(&handler_args.session, database_id).await?;
    Ok(PgResponse::empty_result(StatementType::RECOVER))
}

pub(crate) async fn do_recover(
    session: &SessionImpl,
    database_id: Option<DatabaseId>,
) -> Result<()> {
    let client = session.env().meta_client();
    client.recover(database_id).await?;
    Ok(())
}
