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

use std::sync::Arc;

use futures::future::join_all;
use itertools::Itertools;
use pgwire::pg_field_descriptor::PgFieldDescriptor;
use pgwire::pg_protocol::truncated_fmt;
use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_batch::worker_manager::worker_node_manager::WorkerNodeManagerRef;
use risingwave_common::acl::AclMode;
use risingwave_common::bail_not_implemented;
use risingwave_common::catalog::{ColumnCatalog, ColumnDesc};
use risingwave_common::session_config::{SearchPath, USER_NAME_WILD_CARD};
use risingwave_common::types::{DataType, Datum, Fields, Timestamptz, ToOwnedDatum, WithDataType};
use risingwave_common::util::addr::HostAddr;
use risingwave_connector::source::kafka::PRIVATELINK_CONNECTION;
use risingwave_expr::scalar::like::{i_like_default, like_default};
use risingwave_pb::catalog::connection;
use risingwave_pb::frontend_service::{
    GetAllCursorsRequest, GetAllSubCursorsRequest, GetRunningSqlsRequest,
};
use risingwave_pb::id::WorkerId;
use risingwave_rpc_client::FrontendClientPoolRef;
use risingwave_sqlparser::ast::{
    Ident, ObjectName, ShowCreateType, ShowObject, ShowStatementFilter, display_comma_separated,
};
use thiserror_ext::AsReport;

use super::{RwPgResponse, RwPgResponseBuilderExt, fields_to_descriptors};
use crate::binder::{Binder, Relation};
use crate::catalog::catalog_service::CatalogReadGuard;
use crate::catalog::root_catalog::SchemaPath;
use crate::catalog::schema_catalog::SchemaCatalog;
use crate::catalog::{CatalogError, IndexCatalog};
use crate::error::{ErrorCode, Result, RwError};
use crate::handler::HandlerArgs;
use crate::handler::create_connection::print_connection_params_with_secret_visibility;
use crate::handler::privilege::ObjectCheckItem;
use crate::session::{SessionImpl, WorkerProcessId};
use crate::user::user_catalog::UserCatalog;
use crate::user::{has_access_to_object, has_schema_usage_privilege};

pub fn get_columns_from_table(
    session: &SessionImpl,
    table_name: ObjectName,
) -> Result<Vec<ColumnCatalog>> {
    let mut binder = Binder::new_for_batch(session);
    let relation = binder.bind_relation_by_name(&table_name, None, None, false)?;
    let column_catalogs = match relation {
        Relation::Source(s) => s.catalog.columns,
        Relation::BaseTable(t) => t.table_catalog.columns.clone(),
        Relation::SystemTable(t) => t.sys_table_catalog.columns.clone(),
        _ => {
            return Err(CatalogError::not_found("table or source", table_name.to_string()).into());
        }
    };

    Ok(column_catalogs)
}

pub fn get_columns_from_sink(
    session: &SessionImpl,
    sink_name: ObjectName,
) -> Result<Vec<ColumnCatalog>> {
    let binder = Binder::new_for_system(session);
    let sink = binder.bind_sink_by_name(sink_name)?;

    let catalog_reader = session.env().catalog_reader().read_guard();
    let schema = catalog_reader
        .get_schema_by_id(sink.sink_catalog.database_id, sink.sink_catalog.schema_id)?;
    session.check_privileges(&[
        ObjectCheckItem::new(schema.owner, AclMode::Usage, schema.name(), schema.id()),
        ObjectCheckItem::new(
            sink.sink_catalog.owner,
            AclMode::Select,
            sink.sink_catalog.name.clone(),
            sink.sink_catalog.id,
        ),
    ])?;

    Ok(sink.sink_catalog.full_columns().to_vec())
}

pub fn get_columns_from_view(
    session: &SessionImpl,
    view_name: ObjectName,
) -> Result<Vec<ColumnCatalog>> {
    let _authorized_relation =
        Binder::new_for_batch(session).bind_relation_by_name(&view_name, None, None, false)?;
    let binder = Binder::new_for_system(session);
    let view = binder.bind_view_by_name(view_name)?;

    Ok(view
        .view_catalog
        .columns
        .iter()
        .enumerate()
        .map(|(idx, field)| ColumnCatalog {
            column_desc: ColumnDesc::from_field_with_column_id(field, idx as _),
            is_hidden: false,
        })
        .collect())
}

pub fn get_indexes_from_table(
    session: &SessionImpl,
    table_name: ObjectName,
) -> Result<Vec<Arc<IndexCatalog>>> {
    let mut binder = Binder::new_for_batch(session);
    let relation = binder.bind_relation_by_name(&table_name, None, None, false)?;
    let indexes = match relation {
        Relation::BaseTable(t) => t.table_indexes,
        _ => {
            return Err(CatalogError::not_found("table or source", table_name.to_string()).into());
        }
    };

    Ok(indexes)
}

fn schema_or_search_path(
    session: &Arc<SessionImpl>,
    schema: &Option<Ident>,
    search_path: &SearchPath,
) -> Vec<String> {
    if let Some(s) = schema {
        vec![s.real_value()]
    } else {
        search_path
            .real_path()
            .iter()
            .map(|s| {
                if s.eq(USER_NAME_WILD_CARD) {
                    session.user_name()
                } else {
                    s.clone()
                }
            })
            .collect()
    }
}

fn iter_schema_items<F, T>(
    session: &Arc<SessionImpl>,
    schema: &Option<Ident>,
    reader: &CatalogReadGuard,
    current_user: &UserCatalog,
    mut f: F,
) -> Vec<T>
where
    F: FnMut(&SchemaCatalog) -> Vec<T>,
{
    let search_path = session.config().search_path();

    schema_or_search_path(session, schema, &search_path)
        .into_iter()
        .filter_map(|schema| {
            if let Ok(schema_catalog) =
                reader.get_schema_by_name(&session.database(), schema.as_ref())
                && (has_schema_usage_privilege(
                    current_user,
                    schema_catalog.id(),
                    schema_catalog.owner,
                ))
            {
                Some(schema_catalog)
            } else {
                None
            }
        })
        .flat_map(|s| f(s).into_iter())
        .collect()
}

/// Wrapper for `ObjectName` to be used as a field in a row.
// TODO: replace remaining `name: String` with `name: ObjectNameField`
struct ObjectNameField(ObjectName);

fn with_schema_name(schema_name: &str, name: &str) -> ObjectNameField {
    if schema_name.is_empty() {
        ObjectNameField(ObjectName(vec![name.into()]))
    } else {
        ObjectNameField(ObjectName(vec![schema_name.into(), name.into()]))
    }
}

impl WithDataType for ObjectNameField {
    fn default_data_type() -> DataType {
        DataType::Varchar
    }
}

impl ToOwnedDatum for ObjectNameField {
    fn to_owned_datum(self) -> Datum {
        Some(self.0.to_string().into())
    }
}

#[derive(Fields)]
#[fields(style = "Title Case")]
struct ShowObjectRow {
    name: ObjectNameField,
}

impl ShowObjectRow {
    fn base_name(&self) -> String {
        self.name.0.base_name()
    }
}

fn is_permission_denied(err: &RwError) -> bool {
    matches!(err.inner(), ErrorCode::PermissionDenied(_))
}

#[derive(Fields)]
#[fields(style = "Title Case")]
struct ShowDatabaseRow {
    name: String,
}

#[derive(Fields)]
#[fields(style = "Title Case")]
struct ShowSchemaRow {
    name: String,
}

#[derive(Fields)]
#[fields(style = "Title Case")]
pub struct ShowColumnRow {
    pub name: ShowColumnName,
    pub r#type: String,
    pub is_hidden: Option<String>, // XXX: why not bool?
    pub description: Option<String>,
}

#[derive(Clone, Debug)]
enum ShowColumnNameSegment {
    Field(Ident),
    ListElement,
}

impl ShowColumnNameSegment {
    pub fn field(name: &str) -> Self {
        ShowColumnNameSegment::Field(Ident::from_real_value(name))
    }
}

/// The name of a column in the output of `SHOW COLUMNS` or `DESCRIBE`.
#[derive(Clone, Debug)]
pub struct ShowColumnName(Vec<ShowColumnNameSegment>);

impl ShowColumnName {
    /// Create a special column name without quoting. Used only for extra information like `primary key`
    /// in the output of `DESCRIBE`.
    pub fn special(name: &str) -> Self {
        ShowColumnName(vec![ShowColumnNameSegment::Field(Ident::new_unchecked(
            name,
        ))])
    }
}

impl WithDataType for ShowColumnName {
    fn default_data_type() -> DataType {
        DataType::Varchar
    }
}

impl ToOwnedDatum for ShowColumnName {
    fn to_owned_datum(self) -> Datum {
        use std::fmt::Write;

        let mut s = String::new();
        for segment in self.0 {
            match segment {
                ShowColumnNameSegment::Field(ident) => {
                    if !s.is_empty() {
                        // TODO: shall we add parentheses, so that it's valid field access SQL?
                        s.push('.');
                    }
                    write!(s, "{ident}").unwrap();
                }
                ShowColumnNameSegment::ListElement => {
                    s.push_str("[1]");
                }
            }
        }
        s.to_owned_datum()
    }
}

impl ShowColumnRow {
    /// Create a row with the given information. If the data type is a struct or list,
    /// flatten the data type to also generate rows for its fields.
    fn flatten(
        name: ShowColumnName,
        data_type: DataType,
        is_hidden: bool,
        description: Option<String>,
    ) -> Vec<Self> {
        // TODO(struct): use struct's type name once supported.
        let r#type = match &data_type {
            DataType::Struct(_) => "struct".to_owned(),
            DataType::List(list) if let DataType::Struct(_) = list.elem() => "struct[]".to_owned(),
            d => d.to_string(),
        };

        let mut rows = vec![ShowColumnRow {
            name: name.clone(),
            r#type,
            is_hidden: Some(is_hidden.to_string()),
            description,
        }];

        match data_type {
            DataType::Struct(st) => {
                rows.extend(st.iter().flat_map(|(field_name, field_data_type)| {
                    let mut name = name.clone();
                    name.0.push(ShowColumnNameSegment::field(field_name));
                    Self::flatten(name, field_data_type.clone(), is_hidden, None)
                }));
            }

            DataType::List(list) if let DataType::Struct(_) = list.elem() => {
                let mut name = name.clone();
                name.0.push(ShowColumnNameSegment::ListElement);
                rows.extend(Self::flatten(name, list.into_elem(), is_hidden, None));
            }

            _ => {}
        }

        rows
    }

    pub fn from_catalog(col: ColumnCatalog) -> Vec<Self> {
        Self::flatten(
            ShowColumnName(vec![ShowColumnNameSegment::field(&col.column_desc.name)]),
            col.column_desc.data_type,
            col.is_hidden,
            col.column_desc.description,
        )
    }
}

#[derive(Fields)]
#[fields(style = "Title Case")]
struct ShowConnectionRow {
    name: ObjectNameField,
    r#type: String,
    properties: String,
}

#[derive(Fields)]
#[fields(style = "Title Case")]
struct ShowFunctionRow {
    name: ObjectNameField,
    arguments: String,
    return_type: String,
    language: String,
    link: Option<String>,
}

#[derive(Fields)]
#[fields(style = "Title Case")]
struct ShowIndexRow {
    name: String,
    on: String,
    key: String,
    include: String,
    distributed_by: String,
}

impl From<Arc<IndexCatalog>> for ShowIndexRow {
    fn from(index: Arc<IndexCatalog>) -> Self {
        let index_display = index.display();
        ShowIndexRow {
            name: index.name.clone(),
            on: index.primary_table.name.clone(),
            key: display_comma_separated(&index_display.index_columns_with_ordering).to_string(),
            include: display_comma_separated(&index_display.include_columns).to_string(),
            distributed_by: display_comma_separated(&index_display.distributed_by_columns)
                .to_string(),
        }
    }
}

#[derive(Fields)]
#[fields(style = "Title Case")]
struct ShowClusterRow {
    id: WorkerId,
    addr: String,
    r#type: String,
    state: String,
    parallelism: Option<i32>,
    is_streaming: Option<bool>,
    is_serving: Option<bool>,
    started_at: Option<Timestamptz>,
}

#[derive(Fields)]
#[fields(style = "Title Case")]
struct ShowJobRow {
    id: i64,
    statement: String,
    create_type: String,
    progress: String,
}

#[derive(Fields)]
#[fields(style = "Title Case")]
struct ShowProcessListRow {
    worker_id: String,
    id: String,
    user: String,
    host: String,
    database: String,
    time: Option<String>,
    info: Option<String>,
}

#[derive(Fields)]
#[fields(style = "Title Case")]
struct ShowCreateObjectRow {
    name: String,
    create_sql: String,
}

#[derive(Fields)]
#[fields(style = "Title Case")]
struct ShowSubscriptionRow {
    name: ObjectNameField,
    retention_seconds: i64,
}

#[derive(Fields)]
#[fields(style = "Title Case")]
struct ShowCursorRow {
    worker_id: String,
    session_id: String,
    user: String,
    host: String,
    database: String,
    cursor_name: String,
    info: Option<String>,
}

#[derive(Fields)]
#[fields(style = "Title Case")]
struct ShowSubscriptionCursorRow {
    worker_id: String,
    session_id: String,
    user: String,
    host: String,
    database: String,
    cursor_name: String,
    subscription_name: String,
    state: String,
    idle_duration_ms: i64,
    info: Option<String>,
}

/// Infer the row description for different show objects.
pub fn infer_show_object(objects: &ShowObject) -> Vec<PgFieldDescriptor> {
    fields_to_descriptors(match objects {
        ShowObject::Database => ShowDatabaseRow::fields(),
        ShowObject::Schema => ShowSchemaRow::fields(),
        ShowObject::Columns { .. } => ShowColumnRow::fields(),
        ShowObject::Connection { .. } => ShowConnectionRow::fields(),
        ShowObject::Function { .. } => ShowFunctionRow::fields(),
        ShowObject::Indexes { .. } => ShowIndexRow::fields(),
        ShowObject::Cluster => ShowClusterRow::fields(),
        ShowObject::Jobs => ShowJobRow::fields(),
        ShowObject::ProcessList => ShowProcessListRow::fields(),
        ShowObject::Cursor => ShowCursorRow::fields(),
        ShowObject::SubscriptionCursor => ShowSubscriptionCursorRow::fields(),
        ShowObject::Subscription { .. } => ShowSubscriptionRow::fields(),

        ShowObject::Table { .. }
        | ShowObject::InternalTable { .. }
        | ShowObject::View { .. }
        | ShowObject::MaterializedView { .. }
        | ShowObject::Source { .. }
        | ShowObject::Sink { .. }
        | ShowObject::Secret { .. } => ShowObjectRow::fields(),
    })
}

pub async fn handle_show_object(
    handler_args: HandlerArgs,
    command: ShowObject,
    filter: Option<ShowStatementFilter>,
) -> Result<RwPgResponse> {
    let session = handler_args.session;

    if let Some(ShowStatementFilter::Where(..)) = filter {
        bail_not_implemented!("WHERE clause in SHOW statement");
    }

    let catalog_reader = session.env().catalog_reader();
    let user_reader = session.env().user_info_reader();
    let get_catalog_reader = || {
        let reader = catalog_reader.read_guard();
        let user_reader = user_reader.read_guard();
        let current_user = user_reader
            .get_user_by_name(&session.user_name())
            .expect("user not found")
            .clone();
        (reader, current_user)
    };

    let rows: Vec<ShowObjectRow> = match command {
        ShowObject::Table { schema } => {
            let (reader, current_user) = get_catalog_reader();
            iter_schema_items(&session, &schema, &reader, &current_user, |schema| {
                schema
                    .iter_user_table_with_acl(&current_user)
                    .map(|t| with_schema_name(&schema.name, &t.name))
                    .map(|name| ShowObjectRow { name })
                    .collect()
            })
        }
        ShowObject::InternalTable { schema } => {
            let (reader, current_user) = get_catalog_reader();
            iter_schema_items(&session, &schema, &reader, &current_user, |schema| {
                schema
                    .iter_internal_table_with_acl(&current_user)
                    .map(|t| with_schema_name(&schema.name, &t.name))
                    .map(|name| ShowObjectRow { name })
                    .collect()
            })
        }
        ShowObject::Database => {
            let reader = catalog_reader.read_guard();
            let rows = reader
                .get_all_database_names()
                .into_iter()
                .map(|name| ShowDatabaseRow { name });
            return Ok(PgResponse::builder(StatementType::SHOW_COMMAND)
                .rows(rows)
                .into());
        }
        ShowObject::Schema => {
            let reader = catalog_reader.read_guard();
            let rows = reader
                .get_all_schema_names(&session.database())?
                .into_iter()
                .map(|name| ShowSchemaRow { name });
            return Ok(PgResponse::builder(StatementType::SHOW_COMMAND)
                .rows(rows)
                .into());
        }
        ShowObject::View { schema } => {
            let (reader, current_user) = get_catalog_reader();
            iter_schema_items(&session, &schema, &reader, &current_user, |schema| {
                schema
                    .iter_view_with_acl(&current_user)
                    .map(|t| with_schema_name(&schema.name, &t.name))
                    .map(|name| ShowObjectRow { name })
                    .collect()
            })
        }
        ShowObject::MaterializedView { schema } => {
            let (reader, current_user) = get_catalog_reader();
            iter_schema_items(&session, &schema, &reader, &current_user, |schema| {
                schema
                    .iter_created_mvs_with_acl(&current_user)
                    .map(|t| with_schema_name(&schema.name, &t.name))
                    .map(|name| ShowObjectRow { name })
                    .collect()
            })
        }
        ShowObject::Source { schema } => {
            let (reader, current_user) = get_catalog_reader();
            let mut sources =
                iter_schema_items(&session, &schema, &reader, &current_user, |schema| {
                    schema
                        .iter_source_with_acl(&current_user)
                        .map(|t| with_schema_name(&schema.name, &t.name))
                        .map(|name| ShowObjectRow { name })
                        .collect()
                });
            sources.extend(
                session
                    .temporary_source_manager()
                    .keys()
                    .into_iter()
                    .map(|t| with_schema_name("", &t))
                    .map(|name| ShowObjectRow { name }),
            );
            sources
        }
        ShowObject::Sink { schema } => {
            let (reader, current_user) = get_catalog_reader();
            iter_schema_items(&session, &schema, &reader, &current_user, |schema| {
                schema
                    .iter_sink_with_acl(&current_user)
                    .map(|t| with_schema_name(&schema.name, &t.name))
                    .map(|name| ShowObjectRow { name })
                    .collect()
            })
        }
        ShowObject::Subscription { schema } => {
            let (reader, current_user) = get_catalog_reader();
            let rows = iter_schema_items(&session, &schema, &reader, &current_user, |schema| {
                schema
                    .iter_subscription_with_acl(&current_user)
                    .map(|t| ShowSubscriptionRow {
                        name: with_schema_name(&schema.name, &t.name),
                        retention_seconds: t.retention_seconds as i64,
                    })
                    .collect()
            });
            return Ok(PgResponse::builder(StatementType::SHOW_COMMAND)
                .rows(rows)
                .into());
        }
        ShowObject::Secret { schema } => {
            let (reader, current_user) = get_catalog_reader();
            iter_schema_items(&session, &schema, &reader, &current_user, |schema| {
                schema
                    .iter_secret_with_acl(&current_user)
                    .map(|t| with_schema_name(&schema.name, &t.name))
                    .map(|name| ShowObjectRow { name })
                    .collect()
            })
        }
        ShowObject::Columns { table } => {
            let columns = match get_columns_from_table(&session, table.clone()) {
                Ok(columns) => columns,
                Err(err) if is_permission_denied(&err) => {
                    return Err(err);
                }
                Err(_) => match get_columns_from_sink(&session, table.clone()) {
                    Ok(columns) => columns,
                    Err(err) if is_permission_denied(&err) => {
                        return Err(err);
                    }
                    Err(_) => match get_columns_from_view(&session, table.clone()) {
                        Ok(columns) => columns,
                        Err(err) if is_permission_denied(&err) => {
                            return Err(err);
                        }
                        Err(_) => {
                            return Err(CatalogError::not_found(
                                "table, source, sink or view",
                                table.to_string(),
                            )
                            .into());
                        }
                    },
                },
            };

            return Ok(PgResponse::builder(StatementType::SHOW_COMMAND)
                .rows(columns.into_iter().flat_map(ShowColumnRow::from_catalog))
                .into());
        }
        ShowObject::Indexes { table } => {
            let indexes = get_indexes_from_table(&session, table)?;

            return Ok(PgResponse::builder(StatementType::SHOW_COMMAND)
                .rows(indexes.into_iter().map(ShowIndexRow::from))
                .into());
        }
        ShowObject::Connection { schema } => {
            let (reader, current_user) = get_catalog_reader();
            let rows = iter_schema_items(&session, &schema, &reader, &current_user, |schema| {
                schema.iter_connections_with_acl(&current_user)
                .map(|c| {
                    let name = c.name.clone();
                    let r#type = match &c.info {
                        #[expect(deprecated)]
                        connection::Info::PrivateLinkService(_) => {
                            PRIVATELINK_CONNECTION.to_owned()
                        },
                        connection::Info::ConnectionParams(params) => {
                            params.get_connection_type().unwrap().as_str_name().to_owned()
                        }
                    };
                    let source_names = schema
                        .get_source_ids_by_connection(c.id)
                        .unwrap_or_default()
                        .into_iter()
                        .filter_map(|sid| {
                            schema.get_source_by_id(sid).and_then(|catalog| {
                                has_access_to_object(&current_user, catalog.id, catalog.owner)
                                    .then_some(catalog.name.as_str())
                            })
                        })
                        .collect_vec();
                    let sink_names = schema
                        .get_sink_ids_by_connection(c.id)
                        .unwrap_or_default()
                        .into_iter()
                        .filter_map(|sid| {
                            schema.get_sink_by_id(sid).and_then(|catalog| {
                                has_access_to_object(&current_user, catalog.id, catalog.owner)
                                    .then_some(catalog.name.as_str())
                            })
                        })
                        .collect_vec();
                    let properties = match &c.info {
                        #[expect(deprecated)]
                        connection::Info::PrivateLinkService(i) => {
                            format!(
                                "provider: {}\nservice_name: {}\nendpoint_id: {}\navailability_zones: {}\nsources: {}\nsinks: {}",
                                i.get_provider().unwrap().as_str_name(),
                                i.service_name,
                                i.endpoint_id,
                                serde_json::to_string(&i.dns_entries.keys().collect_vec()).unwrap(),
                                serde_json::to_string(&source_names).unwrap(),
                                serde_json::to_string(&sink_names).unwrap(),
                            )
                        }
                        connection::Info::ConnectionParams(params) => {
                            // todo: show dep relations
                            print_connection_params_with_secret_visibility(
                                &session.database(),
                                params,
                                &reader,
                                &current_user,
                            )
                        }
                    };
                    ShowConnectionRow {
                        name: with_schema_name(&schema.name, &name),
                        r#type,
                        properties,
                    }
                }).collect_vec()
            });
            return Ok(PgResponse::builder(StatementType::SHOW_COMMAND)
                .rows(rows)
                .into());
        }
        ShowObject::Function { schema } => {
            let (reader, current_user) = get_catalog_reader();
            let rows = iter_schema_items(&session, &schema, &reader, &current_user, |schema| {
                schema
                    .iter_function_with_acl(&current_user)
                    .map(|t| ShowFunctionRow {
                        name: with_schema_name(&schema.name, &t.name),
                        arguments: t.arg_types.iter().map(|t| t.to_string()).join(", "),
                        return_type: t.return_type.to_string(),
                        language: t.language.clone(),
                        link: t.link.clone(),
                    })
                    .collect()
            });
            return Ok(PgResponse::builder(StatementType::SHOW_COMMAND)
                .rows(rows)
                .into());
        }
        ShowObject::Cluster => {
            let workers = session.env().meta_client().list_all_nodes().await?;
            let rows = workers.into_iter().sorted_by_key(|w| w.id).map(|worker| {
                let addr: HostAddr = worker.host.as_ref().unwrap().into();
                let property = worker.property.as_ref();
                ShowClusterRow {
                    id: worker.id,
                    addr: addr.to_string(),
                    r#type: worker.get_type().unwrap().as_str_name().into(),
                    state: worker.get_state().unwrap().as_str_name().to_owned(),
                    parallelism: worker.parallelism().map(|parallelism| parallelism as i32),
                    is_streaming: property.map(|p| p.is_streaming),
                    is_serving: property.map(|p| p.is_serving),
                    started_at: worker
                        .started_at
                        .map(|ts| Timestamptz::from_secs(ts as i64).unwrap()),
                }
            });
            return Ok(PgResponse::builder(StatementType::SHOW_COMMAND)
                .rows(rows)
                .into());
        }
        ShowObject::Jobs => {
            let resp = session.env().meta_client().get_ddl_progress().await?;
            let rows = resp.into_iter().map(|job| ShowJobRow {
                id: job.id as i64,
                statement: job.statement,
                create_type: job.create_type,
                progress: job.progress,
            });
            return Ok(PgResponse::builder(StatementType::SHOW_COMMAND)
                .rows(rows)
                .into());
        }
        ShowObject::ProcessList => {
            let rows = show_process_list_impl(
                session.env().frontend_client_pool(),
                session.env().worker_node_manager_ref(),
            )
            .await;
            return Ok(PgResponse::builder(StatementType::SHOW_COMMAND)
                .rows(rows)
                .into());
        }
        ShowObject::Cursor => {
            let rows = show_all_cursors_impl(
                session.env().frontend_client_pool(),
                session.env().worker_node_manager_ref(),
            )
            .await;
            return Ok(PgResponse::builder(StatementType::SHOW_COMMAND)
                .rows(rows)
                .into());
        }
        ShowObject::SubscriptionCursor => {
            let rows = show_all_sub_cursors_impl(
                session.env().frontend_client_pool(),
                session.env().worker_node_manager_ref(),
            )
            .await;

            return Ok(PgResponse::builder(StatementType::SHOW_COMMAND)
                .rows(rows)
                .into());
        }
    };

    // Apply filters.
    let mut result = Vec::new();

    for row in rows {
        let matched = match &filter {
            Some(ShowStatementFilter::Like(pattern)) => like_default(&row.base_name(), pattern)?,
            Some(ShowStatementFilter::ILike(pattern)) => i_like_default(&row.base_name(), pattern)?,
            Some(ShowStatementFilter::Where(..)) => unreachable!(),
            None => true,
        };

        if matched {
            result.push(row);
        }
    }

    Ok(PgResponse::builder(StatementType::SHOW_COMMAND)
        .rows(result)
        .into())
}

pub fn infer_show_create_object() -> Vec<PgFieldDescriptor> {
    fields_to_descriptors(ShowCreateObjectRow::fields())
}

pub fn handle_show_create_object(
    handle_args: HandlerArgs,
    show_create_type: ShowCreateType,
    name: ObjectName,
) -> Result<RwPgResponse> {
    let session = handle_args.session;
    let catalog_reader = session.env().catalog_reader().read_guard();
    let database = session.database();
    let (schema_name, object_name) = Binder::resolve_schema_qualified_name(&database, &name)?;
    let search_path = session.config().search_path();
    let user_name = &session.user_name();
    let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);
    let user_reader = session.env().user_info_reader().read_guard();
    let current_user = user_reader
        .get_user_by_name(user_name)
        .expect("user not found");

    let (sql, schema_name) = match show_create_type {
        ShowCreateType::MaterializedView => {
            let (mv, schema) = schema_path
                .try_find(|schema_name| {
                    Ok::<_, RwError>(
                        catalog_reader
                            .get_schema_by_name(&database, schema_name)?
                            .get_created_table_by_name(&object_name)
                            .filter(|t| {
                                t.is_mview() && has_access_to_object(current_user, t.id, t.owner)
                            }),
                    )
                })?
                .ok_or_else(|| CatalogError::not_found("materialized view", name.to_string()))?;
            (mv.create_sql(), schema)
        }
        ShowCreateType::View => {
            let (view, schema) =
                catalog_reader.get_view_by_name(&database, schema_path, &object_name)?;
            if !view.is_system_view() && !has_access_to_object(current_user, view.id, view.owner) {
                return Err(CatalogError::not_found("view", name.to_string()).into());
            }
            (view.create_sql(schema.to_owned()), schema)
        }
        ShowCreateType::Table => {
            let (table, schema) = schema_path
                .try_find(|schema_name| {
                    Ok::<_, RwError>(
                        catalog_reader
                            .get_schema_by_name(&database, schema_name)?
                            .get_created_table_by_name(&object_name)
                            .filter(|t| {
                                t.is_user_table()
                                    && has_access_to_object(current_user, t.id, t.owner)
                            }),
                    )
                })?
                .ok_or_else(|| CatalogError::not_found("table", name.to_string()))?;

            (table.create_sql_purified(), schema)
        }
        ShowCreateType::Sink => {
            let (sink, schema) =
                catalog_reader.get_any_sink_by_name(&database, schema_path, &object_name)?;
            if !has_access_to_object(current_user, sink.id, sink.owner) {
                return Err(CatalogError::not_found("sink", name.to_string()).into());
            }
            (sink.create_sql(), schema)
        }
        ShowCreateType::Source => {
            let (source, schema) = schema_path
                .try_find(|schema_name| {
                    Ok::<_, RwError>(
                        catalog_reader
                            .get_schema_by_name(&database, schema_name)?
                            .get_source_by_name(&object_name)
                            .filter(|s| {
                                s.associated_table_id.is_none()
                                    && has_access_to_object(current_user, s.id, s.owner)
                            }),
                    )
                })?
                .ok_or_else(|| CatalogError::not_found("source", name.to_string()))?;
            (source.create_sql_purified(), schema)
        }
        ShowCreateType::Index => {
            let (index, schema) = schema_path
                .try_find(|schema_name| {
                    Ok::<_, RwError>(
                        catalog_reader
                            .get_schema_by_name(&database, schema_name)?
                            .get_created_table_by_name(&object_name)
                            .filter(|t| {
                                t.is_index() && has_access_to_object(current_user, t.id, t.owner)
                            }),
                    )
                })?
                .ok_or_else(|| CatalogError::not_found("index", name.to_string()))?;
            (index.create_sql(), schema)
        }
        ShowCreateType::Function => {
            bail_not_implemented!("show create on: {}", show_create_type);
        }
        ShowCreateType::Subscription => {
            let (subscription, schema) =
                catalog_reader.get_subscription_by_name(&database, schema_path, &object_name)?;
            if !has_access_to_object(current_user, subscription.id, subscription.owner) {
                return Err(CatalogError::not_found("subscription", name.to_string()).into());
            }
            (subscription.create_sql(), schema)
        }
    };
    let name = format!("{}.{}", schema_name, object_name);

    Ok(PgResponse::builder(StatementType::SHOW_COMMAND)
        .rows([ShowCreateObjectRow {
            name,
            create_sql: sql,
        }])
        .into())
}

async fn show_all_sub_cursors_impl(
    frontend_client_pool: FrontendClientPoolRef,
    worker_node_manager: WorkerNodeManagerRef,
) -> Vec<ShowSubscriptionCursorRow> {
    fn on_error(worker_id: WorkerId, err_msg: String) -> Vec<ShowSubscriptionCursorRow> {
        vec![ShowSubscriptionCursorRow {
            worker_id: format!("{}", worker_id),
            session_id: "".to_owned(),
            user: "".to_owned(),
            host: "".to_owned(),
            database: "".to_owned(),
            cursor_name: "".to_owned(),
            subscription_name: "".to_owned(),
            state: "".to_owned(),
            idle_duration_ms: 0,
            info: Some(format!(
                "Failed to show subscription cursor from worker {worker_id} due to: {err_msg}"
            )),
        }]
    }

    let futures = worker_node_manager
        .list_frontend_nodes()
        .into_iter()
        .map(|worker| {
            let frontend_client_pool_ = frontend_client_pool.clone();
            async move {
                let client = match frontend_client_pool_.get(&worker).await {
                    Ok(client) => client,
                    Err(e) => {
                        return on_error(worker.id, format!("{}", e.as_report()));
                    }
                };

                let resp = match client.get_all_sub_cursors(GetAllSubCursorsRequest {}).await {
                    Ok(resp) => resp,
                    Err(e) => {
                        return on_error(worker.id, format!("{}", e.as_report()));
                    }
                };

                resp.into_inner()
                    .subscription_cursors
                    .into_iter()
                    .flat_map(|sub_cursor| {
                        let worker_id = worker.id.to_string();
                        let session_id = sub_cursor.session_id.to_string();

                        let user = sub_cursor.user_name;
                        let host = sub_cursor.peer_addr;
                        let database = sub_cursor.database;

                        let size = sub_cursor.states.len();
                        let mut sub_cursors = vec![];
                        for index in 0..size {
                            sub_cursors.push(ShowSubscriptionCursorRow {
                                worker_id: worker_id.clone(),
                                session_id: session_id.clone(),
                                user: user.clone(),
                                host: host.clone(),
                                database: database.clone(),
                                cursor_name: sub_cursor.cursor_names[index].clone(),
                                subscription_name: sub_cursor.subscription_names[index].clone(),
                                state: sub_cursor.states[index].clone(),
                                idle_duration_ms: sub_cursor.idle_durations[index] as i64,
                                info: None,
                            })
                        }
                        sub_cursors
                    })
                    .collect_vec()
            }
        })
        .collect_vec();
    join_all(futures).await.into_iter().flatten().collect()
}

async fn show_all_cursors_impl(
    frontend_client_pool: FrontendClientPoolRef,
    worker_node_manager: WorkerNodeManagerRef,
) -> Vec<ShowCursorRow> {
    fn on_error(worker_id: WorkerId, err_msg: String) -> Vec<ShowCursorRow> {
        vec![ShowCursorRow {
            worker_id: format!("{}", worker_id),
            session_id: "".to_owned(),
            user: "".to_owned(),
            host: "".to_owned(),
            database: "".to_owned(),
            cursor_name: "".to_owned(),
            info: Some(format!(
                "Failed to show cursor from worker {worker_id} due to: {err_msg}"
            )),
        }]
    }
    let futures = worker_node_manager
        .list_frontend_nodes()
        .into_iter()
        .map(|worker| {
            let frontend_client_pool_ = frontend_client_pool.clone();
            async move {
                let client = match frontend_client_pool_.get(&worker).await {
                    Ok(client) => client,
                    Err(e) => {
                        return on_error(worker.id, format!("{}", e.as_report()));
                    }
                };

                let resp = match client.get_all_cursors(GetAllCursorsRequest {}).await {
                    Ok(resp) => resp,
                    Err(e) => {
                        return on_error(worker.id, format!("{}", e.as_report()));
                    }
                };

                resp.into_inner()
                    .all_cursors
                    .into_iter()
                    .flat_map(|cursors| {
                        let worker_id = worker.id.to_string();
                        let session_id = cursors.session_id.to_string();

                        let user = cursors.user_name;
                        let host = cursors.peer_addr;
                        let database = cursors.database;

                        cursors
                            .cursor_names
                            .into_iter()
                            .map(|cursor_name| ShowCursorRow {
                                worker_id: worker_id.clone(),
                                session_id: session_id.clone(),
                                user: user.clone(),
                                host: host.clone(),
                                database: database.clone(),
                                cursor_name,
                                info: None,
                            })
                            .collect_vec()
                    })
                    .collect_vec()
            }
        })
        .collect_vec();
    join_all(futures).await.into_iter().flatten().collect()
}

async fn show_process_list_impl(
    frontend_client_pool: FrontendClientPoolRef,
    worker_node_manager: WorkerNodeManagerRef,
) -> Vec<ShowProcessListRow> {
    // Create a placeholder row for the worker in case of any errors while fetching its running SQLs.
    fn on_error(worker_id: WorkerId, err_msg: String) -> Vec<ShowProcessListRow> {
        vec![ShowProcessListRow {
            worker_id: format!("{}", worker_id),
            id: "".to_owned(),
            user: "".to_owned(),
            host: "".to_owned(),
            database: "".to_owned(),
            time: None,
            info: Some(format!(
                "Failed to show process list from worker {worker_id} due to: {err_msg}"
            )),
        }]
    }
    let futures = worker_node_manager
        .list_frontend_nodes()
        .into_iter()
        .map(|worker| {
            let frontend_client_pool_ = frontend_client_pool.clone();
            async move {
                let client = match frontend_client_pool_.get(&worker).await {
                    Ok(client) => client,
                    Err(e) => {
                        return on_error(worker.id, format!("{}", e.as_report()));
                    }
                };
                let resp = match client.get_running_sqls(GetRunningSqlsRequest {}).await {
                    Ok(resp) => resp,
                    Err(e) => {
                        return on_error(worker.id, format!("{}", e.as_report()));
                    }
                };
                resp.into_inner()
                    .running_sqls
                    .into_iter()
                    .map(|sql| ShowProcessListRow {
                        worker_id: format!("{}", worker.id),
                        id: format!("{}", WorkerProcessId::new(worker.id, sql.process_id)),
                        user: sql.user_name,
                        host: sql.peer_addr,
                        database: sql.database,
                        time: sql.elapsed_millis.map(|mills| format!("{}ms", mills)),
                        info: sql
                            .sql
                            .map(|sql| format!("{}", truncated_fmt::TruncatedFmt(&sql, 1024))),
                    })
                    .collect_vec()
            }
        })
        .collect_vec();
    join_all(futures).await.into_iter().flatten().collect()
}

#[cfg(test)]
mod tests {
    use std::ops::Index;

    use futures_async_stream::for_await;

    use crate::test_utils::{LocalFrontend, PROTO_FILE_DATA, create_proto_file};

    #[tokio::test]
    async fn test_show_source() {
        let frontend = LocalFrontend::new(Default::default()).await;

        let sql = r#"CREATE SOURCE t1 (column1 varchar)
        WITH (connector = 'kafka', kafka.topic = 'abc', kafka.brokers = 'localhost:1001')
        FORMAT PLAIN ENCODE JSON"#;
        frontend.run_sql(sql).await.unwrap();

        let mut rows = frontend.query_formatted_result("SHOW SOURCES").await;
        rows.sort();
        assert_eq!(rows, vec!["Row([Some(b\"public.t1\")])".to_owned(),]);
    }

    #[tokio::test]
    async fn test_show_column() {
        let proto_file = create_proto_file(PROTO_FILE_DATA);
        let sql = format!(
            r#"CREATE SOURCE t
    WITH (connector = 'kafka', kafka.topic = 'abc', kafka.brokers = 'localhost:1001')
    FORMAT PLAIN ENCODE PROTOBUF (message = '.test.TestRecord', schema.location = 'file://{}')"#,
            proto_file.path().to_str().unwrap()
        );
        let frontend = LocalFrontend::new(Default::default()).await;
        frontend.run_sql(sql).await.unwrap();

        let sql = "show columns from t";
        let mut pg_response = frontend.run_sql(sql).await.unwrap();

        let mut columns = Vec::new();
        #[for_await]
        for row_set in pg_response.values_stream() {
            let row_set = row_set.unwrap();
            for row in row_set {
                columns.push((
                    std::str::from_utf8(row.index(0).as_ref().unwrap())
                        .unwrap()
                        .to_owned(),
                    std::str::from_utf8(row.index(1).as_ref().unwrap())
                        .unwrap()
                        .to_owned(),
                ));
            }
        }

        expect_test::expect![[r#"
            [
                (
                    "id",
                    "integer",
                ),
                (
                    "country",
                    "struct",
                ),
                (
                    "country.address",
                    "character varying",
                ),
                (
                    "country.city",
                    "struct",
                ),
                (
                    "country.city.address",
                    "character varying",
                ),
                (
                    "country.city.zipcode",
                    "character varying",
                ),
                (
                    "country.zipcode",
                    "character varying",
                ),
                (
                    "zipcode",
                    "bigint",
                ),
                (
                    "rate",
                    "real",
                ),
                (
                    "_rw_kafka_timestamp",
                    "timestamp with time zone",
                ),
                (
                    "_rw_kafka_partition",
                    "character varying",
                ),
                (
                    "_rw_kafka_offset",
                    "character varying",
                ),
                (
                    "_row_id",
                    "serial",
                ),
            ]
        "#]]
        .assert_debug_eq(&columns);
    }
}
