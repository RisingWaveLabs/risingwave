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

use super::util::{LongRunningNotificationAction, execute_with_long_running_notification};
use super::{HandlerArgs, RwPgResponse};
use crate::Binder;
use crate::catalog::root_catalog::SchemaPath;
use crate::error::Result;

pub async fn handle_drop_subscription(
    handler_args: HandlerArgs,
    subscription_name: ObjectName,
    if_exists: bool,
    cascade: bool,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let db_name = &session.database();
    let (schema_name, subscription_name) =
        Binder::resolve_schema_qualified_name(db_name, &subscription_name)?;
    let search_path = session.config().search_path();
    let user_name = &session.user_name();
    let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

    let subscription = {
        let catalog_reader = session.env().catalog_reader().read_guard();
        let (subscription, schema_name) =
            match catalog_reader.get_subscription_by_name(db_name, schema_path, &subscription_name)
            {
                Ok((subscription, schema)) => (subscription.clone(), schema),
                Err(e) => {
                    return if if_exists {
                        Ok(RwPgResponse::builder(StatementType::DROP_SUBSCRIPTION)
                            .notice(format!(
                                "subscription \"{}\" does not exist, skipping",
                                subscription_name
                            ))
                            .into())
                    } else {
                        Err(e.into())
                    };
                }
            };

        session.check_privilege_for_drop_alter(schema_name, &*subscription)?;

        subscription
    };

    let subscription_id = subscription.id;

    let catalog_writer = session.catalog_writer()?;
    execute_with_long_running_notification(
        catalog_writer.drop_subscription(subscription_id, cascade, if_exists),
        &session,
        "DROP SUBSCRIPTION",
        LongRunningNotificationAction::SuggestRecover,
    )
    .await?;

    Ok(PgResponse::empty_result(StatementType::DROP_SUBSCRIPTION))
}
