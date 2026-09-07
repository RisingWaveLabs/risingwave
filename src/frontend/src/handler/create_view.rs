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

//! Handle creation of logical (non-materialized) views.

use either::Either;
use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_pb::catalog::PbView;
use risingwave_sqlparser::ast::{Ident, ObjectName, Query};

use super::RwPgResponse;
use crate::binder::{Binder, BoundStatement};
use crate::error::Result;
use crate::handler::HandlerArgs;
use crate::handler::util::reject_internal_table_dependencies;
use crate::optimizer::{OptimizerContext, RelationCollectorVisitor};
use crate::planner::Planner;

pub async fn handle_create_view(
    handler_args: HandlerArgs,
    if_not_exists: bool,
    name: ObjectName,
    columns: Vec<Ident>,
    query: Query,
) -> Result<RwPgResponse> {
    let session = handler_args.session.clone();
    let db_name = &session.database();
    let (schema_name, view_name) = Binder::resolve_schema_qualified_name(db_name, &name)?;

    let (database_id, schema_id) = session.get_database_and_schema_id_for_create(schema_name)?;

    let properties = handler_args.with_options.clone();

    if let Either::Right(resp) = session.check_relation_name_duplicated(
        name.clone(),
        StatementType::CREATE_VIEW,
        if_not_exists,
    )? {
        return Ok(resp);
    }

    // Bind and plan the query to validate it and resolve its schema and dependencies. The batch
    // plan is only used for validation and dependency collection. It is neither stored nor
    // executed, so creating a logical view does not depend on the selected batch execution engine.
    let (dependent_relations, dependent_secrets, schema) = {
        let mut binder = Binder::new_for_batch(&session);
        let bound_query = binder.bind_query(&query)?;
        let dependent_relations = binder.included_relations().clone();
        let dependent_secrets = binder.included_secrets().clone();

        let context = OptimizerContext::from_handler_args(handler_args);
        let logical = Planner::new_for_batch_dql(context.into())
            .plan(BoundStatement::Query(bound_query.into()))?;
        let schema = logical.schema();
        let batch_plan = logical.gen_batch_plan()?;
        let dependent_relations =
            RelationCollectorVisitor::collect_with(dependent_relations, batch_plan.plan);

        reject_internal_table_dependencies(&session, &dependent_relations, "CREATE VIEW")?;

        (dependent_relations, dependent_secrets, schema)
    };

    let columns = if columns.is_empty() {
        schema.fields().to_vec()
    } else {
        if columns.len() != schema.fields().len() {
            return Err(crate::error::ErrorCode::InternalError(
                "view has different number of columns than the query's columns".to_owned(),
            )
            .into());
        }
        schema
            .fields()
            .iter()
            .zip_eq_fast(columns)
            .map(|(f, c)| {
                let mut field = f.clone();
                field.name = c.real_value();
                field
            })
            .collect()
    };

    let (properties, secret_refs, connection_refs) = properties.into_parts();
    if !secret_refs.is_empty() || !connection_refs.is_empty() {
        return Err(crate::error::ErrorCode::InvalidParameterValue(
            "Secret reference and Connection reference are not allowed in create view options"
                .to_owned(),
        )
        .into());
    }

    let view = PbView {
        id: 0.into(),
        schema_id,
        database_id,
        name: view_name,
        properties,
        owner: session.user_id(),
        sql: format!("{}", query),
        columns: columns.into_iter().map(|f| f.to_prost()).collect(),
        created_at_epoch: None,
        created_at_cluster_version: None,
    };

    let catalog_writer = session.catalog_writer()?;
    catalog_writer
        .create_view(
            view,
            dependent_relations
                .into_iter()
                .chain(
                    dependent_secrets
                        .iter()
                        .copied()
                        .map(|id| id.as_object_id()),
                )
                .collect(),
        )
        .await?;

    Ok(PgResponse::empty_result(StatementType::CREATE_VIEW))
}
