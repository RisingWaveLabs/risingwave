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

use std::collections::BTreeMap;
use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::acl::AclMode;
use risingwave_common::bail_not_implemented;
use risingwave_common::catalog::{
    Engine, Field, debug_assert_column_ids_distinct, is_system_schema,
};
use risingwave_common::session_config::USER_NAME_WILD_CARD;
use risingwave_connector::WithPropertiesExt;
use risingwave_connector::sink::catalog::SinkCatalog;
use risingwave_connector::sink::iceberg::IcebergMetadataTableType;
use risingwave_pb::secret::PbSecretRef;
use risingwave_pb::user::grant_privilege::PbObject;
use risingwave_sqlparser::ast::{AsOf, ObjectName, Statement, TableAlias};
use risingwave_sqlparser::parser::Parser;
use thiserror_ext::AsReport;

use super::BoundShare;
use crate::binder::relation::BoundShareInput;
use crate::binder::{BindFor, Binder, Relation};
use crate::catalog::root_catalog::SchemaPath;
use crate::catalog::source_catalog::SourceCatalog;
use crate::catalog::system_catalog::SystemTableCatalog;
use crate::catalog::table_catalog::{TableCatalog, TableType};
use crate::catalog::view_catalog::ViewCatalog;
use crate::catalog::{CatalogError, CatalogResult, DatabaseId, IndexCatalog, TableId};
use crate::error::ErrorCode::PermissionDenied;
use crate::error::{ErrorCode, Result, RwError};
use crate::handler::privilege::ObjectCheckItem;

#[derive(Debug, Clone)]
pub struct BoundBaseTable {
    pub table_id: TableId,
    pub table_catalog: Arc<TableCatalog>,
    pub table_indexes: Vec<Arc<IndexCatalog>>,
    pub as_of: Option<AsOf>,
}

#[derive(Debug, Clone)]
pub struct BoundSystemTable {
    pub table_id: TableId,
    pub sys_table_catalog: Arc<SystemTableCatalog>,
}

#[derive(Debug, Clone)]
pub struct BoundIcebergMetadataTable {
    pub metadata_type: IcebergMetadataTableType,
    pub properties: BTreeMap<String, String>,
    pub secret_refs: BTreeMap<String, PbSecretRef>,
    pub as_of: Option<AsOf>,
}

enum IcebergMetadataBaseRelation {
    Table(Arc<TableCatalog>),
    Source(SourceCatalog, bool),
    Sink(Arc<SinkCatalog>),
}

#[derive(Debug, Clone)]
pub struct BoundSource {
    pub catalog: SourceCatalog,
    pub as_of: Option<AsOf>,
}

impl BoundSource {
    pub fn is_shareable_cdc_connector(&self) -> bool {
        self.catalog.with_properties.is_shareable_cdc_connector()
    }

    pub fn is_shared(&self) -> bool {
        self.catalog.info.is_shared()
    }
}

impl Binder {
    pub fn bind_catalog_relation_by_object_name(
        &mut self,
        object_name: &ObjectName,
        bind_creating_relations: bool,
    ) -> Result<Relation> {
        let (schema_name, table_name) =
            Binder::resolve_schema_qualified_name(&self.db_name, object_name)?;
        self.bind_catalog_relation_by_name(
            None,
            schema_name.as_deref(),
            &table_name,
            None,
            None,
            bind_creating_relations,
        )
    }

    /// Binds table or source, or logical view according to what we get from the catalog.
    pub fn bind_catalog_relation_by_name(
        &mut self,
        db_name: Option<&str>,
        schema_name: Option<&str>,
        table_name: &str,
        alias: Option<&TableAlias>,
        as_of: Option<&AsOf>,
        bind_creating_relations: bool,
    ) -> Result<Relation> {
        // define some helper functions converting catalog to bound relation
        let resolve_sys_table_relation = |sys_table_catalog: &Arc<SystemTableCatalog>| {
            let table = BoundSystemTable {
                table_id: sys_table_catalog.id(),
                sys_table_catalog: sys_table_catalog.clone(),
            };
            (
                Relation::SystemTable(Box::new(table)),
                sys_table_catalog
                    .columns
                    .iter()
                    .map(|c| (c.is_hidden, Field::from(&c.column_desc)))
                    .collect_vec(),
            )
        };

        // check db_name if exists first
        if let Some(db_name) = db_name {
            let _ = self.catalog.get_database_by_name(db_name)?;
        }

        // start to bind
        let (ret, columns) = {
            match schema_name {
                Some(schema_name) => {
                    let db_name = db_name.unwrap_or(&self.db_name).to_owned();
                    let schema_path = SchemaPath::Name(schema_name);
                    if is_system_schema(schema_name) {
                        if let Ok(sys_table_catalog) =
                            self.catalog
                                .get_sys_table_by_name(&db_name, schema_name, table_name)
                        {
                            resolve_sys_table_relation(sys_table_catalog)
                        } else if let Ok((view_catalog, _)) =
                            self.catalog
                                .get_view_by_name(&db_name, schema_path, table_name)
                        {
                            self.resolve_view_relation(&view_catalog.clone())?
                        } else {
                            bail_not_implemented!(
                                issue = 1695,
                                r###"{}.{} is not supported, please use `SHOW` commands for now.
`SHOW TABLES`,
`SHOW MATERIALIZED VIEWS`,
`DESCRIBE <table>`,
`SHOW COLUMNS FROM [table]`
"###,
                                schema_name,
                                table_name
                            );
                        }
                    } else if let Some(source_catalog) =
                        self.temporary_source_manager.get_source(table_name)
                    // don't care about the database and schema
                    {
                        self.resolve_source_relation(&source_catalog.clone(), as_of, true)?
                    } else if let Ok((table_catalog, schema_name)) = self
                        .catalog
                        .get_any_table_by_name(&db_name, schema_path, table_name)
                        && (bind_creating_relations
                            || table_catalog.is_internal_table()
                            || table_catalog.is_created())
                    {
                        self.resolve_table_relation(
                            table_catalog.clone(),
                            &db_name,
                            schema_name,
                            as_of,
                        )?
                    } else if let Ok((source_catalog, _)) =
                        self.catalog
                            .get_source_by_name(&db_name, schema_path, table_name)
                    {
                        self.resolve_source_relation(&source_catalog.clone(), as_of, false)?
                    } else if let Ok((view_catalog, _)) =
                        self.catalog
                            .get_view_by_name(&db_name, schema_path, table_name)
                    {
                        self.resolve_view_relation(&view_catalog.clone())?
                    } else if let Some(table_catalog) =
                        self.staging_catalog_manager.get_table(table_name)
                    {
                        // don't care about the database and schema
                        self.resolve_table_relation(
                            table_catalog.clone().into(),
                            &db_name,
                            schema_name,
                            as_of,
                        )?
                    } else {
                        self.resolve_iceberg_metadata_relation(
                            &db_name,
                            Some(schema_name),
                            table_name,
                            as_of,
                        )?
                    }
                }
                None => (|| {
                    // If schema is not specified, db must be unspecified.
                    // So we should always use current database here.
                    assert!(db_name.is_none());
                    let db_name = self.db_name.clone();
                    let user_name = self.auth_context.user_name.clone();

                    for path in self.search_path.path() {
                        if is_system_schema(path)
                            && let Ok(sys_table_catalog) = self
                                .catalog
                                .get_sys_table_by_name(&db_name, path, table_name)
                        {
                            return Ok(resolve_sys_table_relation(sys_table_catalog));
                        } else {
                            let schema_name = if path == USER_NAME_WILD_CARD {
                                &user_name
                            } else {
                                &path.clone()
                            };

                            if let Ok(schema) =
                                self.catalog.get_schema_by_name(&db_name, schema_name)
                            {
                                if let Some(source_catalog) =
                                    self.temporary_source_manager.get_source(table_name)
                                // don't care about the database and schema
                                {
                                    return self.resolve_source_relation(
                                        &source_catalog.clone(),
                                        as_of,
                                        true,
                                    );
                                } else if let Some(table_catalog) =
                                    schema.get_any_table_by_name(table_name)
                                    && (bind_creating_relations
                                        || table_catalog.is_internal_table()
                                        || table_catalog.is_created())
                                {
                                    return self.resolve_table_relation(
                                        table_catalog.clone(),
                                        &db_name,
                                        schema_name,
                                        as_of,
                                    );
                                } else if let Some(source_catalog) =
                                    schema.get_source_by_name(table_name)
                                {
                                    return self.resolve_source_relation(
                                        &source_catalog.clone(),
                                        as_of,
                                        false,
                                    );
                                } else if let Some(view_catalog) =
                                    schema.get_view_by_name(table_name)
                                {
                                    return self.resolve_view_relation(&view_catalog.clone());
                                } else if let Some(table_catalog) =
                                    self.staging_catalog_manager.get_table(table_name)
                                {
                                    // don't care about the database and schema
                                    return self.resolve_table_relation(
                                        table_catalog.clone().into(),
                                        &db_name,
                                        schema_name,
                                        as_of,
                                    );
                                }
                            }
                        }
                    }

                    self.resolve_iceberg_metadata_relation(&db_name, None, table_name, as_of)
                })()?,
            }
        };

        self.bind_table_to_context(
            columns,
            table_name.to_owned(),
            schema_name.map(|s| s.to_owned()),
            alias,
        )?;
        Ok(ret)
    }

    fn resolve_iceberg_metadata_relation(
        &mut self,
        db_name: &str,
        schema_name: Option<&str>,
        relation_name: &str,
        as_of: Option<&AsOf>,
    ) -> Result<(Relation, Vec<(bool, Field)>)> {
        let Some((base_name, suffix)) = relation_name.rsplit_once('$') else {
            return Err(CatalogError::not_found("table or source", relation_name).into());
        };
        let Some(metadata_type) = IcebergMetadataTableType::from_suffix(suffix) else {
            return Err(CatalogError::not_found("table or source", relation_name).into());
        };
        if base_name.is_empty() {
            return Err(CatalogError::not_found("table or source", relation_name).into());
        }

        if metadata_type == IcebergMetadataTableType::Snapshots && as_of.is_some() {
            return Err(ErrorCode::BindError(
                "time travel is only supported for Iceberg manifests and files metadata relations"
                    .to_owned(),
            )
            .into());
        }
        if matches!(
            as_of,
            Some(AsOf::ProcessTime | AsOf::ProcessTimeBroadcast | AsOf::ProcessTimeWithInterval(_))
        ) {
            bail_not_implemented!(
                "As Of ProcessTime() is not supported for Iceberg metadata relations."
            );
        }

        let (base_relation, resolved_schema_name) = if let Some(source) =
            self.temporary_source_manager.get_source(base_name)
        {
            (
                IcebergMetadataBaseRelation::Source(source.clone(), true),
                None,
            )
        } else {
            let schema_path = self.bind_schema_path(schema_name);
            let Some((base_relation, resolved_schema_name)) =
                schema_path.try_find(|schema_name| -> CatalogResult<_> {
                    let schema = self.catalog.get_schema_by_name(db_name, schema_name)?;
                    Ok(schema
                        .get_created_table_by_name(base_name)
                        .map(|table| IcebergMetadataBaseRelation::Table(table.clone()))
                        .or_else(|| {
                            schema.get_source_by_name(base_name).map(|source| {
                                IcebergMetadataBaseRelation::Source(source.as_ref().clone(), false)
                            })
                        })
                        .or_else(|| {
                            schema
                                .get_created_sink_by_name(base_name)
                                .map(|sink| IcebergMetadataBaseRelation::Sink(sink.clone()))
                        }))
                })?
            else {
                return Err(
                    CatalogError::not_found("Iceberg table, source, or sink", base_name).into(),
                );
            };
            (base_relation, Some(resolved_schema_name.to_owned()))
        };

        let (properties, secret_refs) = match base_relation {
            IcebergMetadataBaseRelation::Table(table) => {
                self.check_privilege(
                    ObjectCheckItem::new(
                        table.owner,
                        AclMode::Select,
                        table.name.clone(),
                        table.id(),
                    ),
                    table.database_id,
                )?;
                if table.engine() != Engine::Iceberg {
                    return Err(ErrorCode::BindError(format!(
                        "metadata relation \"{relation_name}\" requires an Iceberg engine table, source, or sink, but table \"{base_name}\" uses {:?}",
                        table.engine()
                    ))
                    .into());
                }
                self.included_relations.insert(table.id().as_object_id());

                let sink_name = table.iceberg_sink_name().ok_or_else(|| {
                    ErrorCode::CatalogError(
                        format!("no Iceberg sink found for table \"{base_name}\"").into(),
                    )
                })?;
                let sink = self
                    .catalog
                    .get_created_sink_by_name(
                        db_name,
                        SchemaPath::Name(
                            resolved_schema_name
                                .as_deref()
                                .expect("catalog tables always have a schema"),
                        ),
                        &sink_name,
                    )
                    .map_err(|_| {
                        ErrorCode::CatalogError(
                            format!(
                                "Iceberg sink \"{sink_name}\" not found for table \"{base_name}\""
                            )
                            .into(),
                        )
                    })?
                    .0
                    .clone();
                (sink.properties.clone(), sink.secret_refs.clone())
            }
            IcebergMetadataBaseRelation::Source(source, is_temporary) => {
                if !is_temporary {
                    self.check_privilege(
                        ObjectCheckItem::new(
                            source.owner,
                            AclMode::Select,
                            source.name.clone(),
                            source.id,
                        ),
                        source.database_id,
                    )?;
                }
                if !source.is_iceberg_connector() {
                    return Err(ErrorCode::BindError(format!(
                        "metadata relation \"{relation_name}\" requires an Iceberg source, but source \"{base_name}\" uses a different connector"
                    ))
                    .into());
                }
                self.included_relations.insert(source.id.as_object_id());
                source.with_properties.into_parts()
            }
            IcebergMetadataBaseRelation::Sink(sink) => {
                self.check_privilege(
                    ObjectCheckItem::new(sink.owner, AclMode::Select, sink.name.clone(), sink.id),
                    sink.database_id,
                )?;
                if !sink.properties.is_iceberg_connector() {
                    return Err(ErrorCode::BindError(format!(
                        "metadata relation \"{relation_name}\" requires an Iceberg sink, but sink \"{base_name}\" uses a different connector"
                    ))
                    .into());
                }
                self.included_relations.insert(sink.id.as_object_id());
                (sink.properties.clone(), sink.secret_refs.clone())
            }
        };

        let columns = metadata_type
            .schema()
            .fields
            .into_iter()
            .map(|field| (false, field))
            .collect();
        Ok((
            Relation::IcebergMetadataTable(Box::new(BoundIcebergMetadataTable {
                metadata_type,
                properties,
                secret_refs,
                as_of: as_of.cloned(),
            })),
            columns,
        ))
    }

    pub(crate) fn check_privilege(
        &self,
        item: ObjectCheckItem,
        database_id: DatabaseId,
    ) -> Result<()> {
        // security invoker is disabled for view, ignore privilege check.
        if self.context.disable_security_invoker {
            return Ok(());
        }

        match self.bind_for {
            BindFor::Stream | BindFor::Batch => {
                // reject sources for cross-db access
                if matches!(self.bind_for, BindFor::Stream)
                    && self.database_id != database_id
                    && matches!(item.object, PbObject::SourceId(_))
                {
                    return Err(PermissionDenied(format!(
                        "SOURCE \"{}\" is not allowed for cross-db access",
                        item.name
                    ))
                    .into());
                }
                if let Some(user) = self.user.get_user_by_name(&self.auth_context.user_name) {
                    if user.is_super || user.id == item.owner {
                        return Ok(());
                    }
                    if !user.has_privilege(item.object, item.mode) {
                        return Err(PermissionDenied(item.error_message()).into());
                    }

                    // check CONNECT privilege for cross-db access
                    if self.database_id != database_id
                        && !user.has_privilege(database_id, AclMode::Connect)
                    {
                        let db_name = self.catalog.get_database_by_id(database_id)?.name.clone();

                        return Err(PermissionDenied(format!(
                            "permission denied for database \"{db_name}\""
                        ))
                        .into());
                    }
                } else {
                    return Err(PermissionDenied("Session user is invalid".to_owned()).into());
                }
            }
            BindFor::Ddl | BindFor::System => {
                // do nothing.
            }
        }
        Ok(())
    }

    fn resolve_table_relation(
        &mut self,
        table_catalog: Arc<TableCatalog>,
        db_name: &str,
        schema_name: &str,
        as_of: Option<&AsOf>,
    ) -> Result<(Relation, Vec<(bool, Field)>)> {
        let table_id = table_catalog.id();
        let columns = table_catalog
            .columns
            .iter()
            .map(|c| (c.is_hidden, Field::from(&c.column_desc)))
            .collect_vec();
        self.check_privilege(
            ObjectCheckItem::new(
                table_catalog.owner,
                AclMode::Select,
                table_catalog.name.clone(),
                table_id,
            ),
            table_catalog.database_id,
        )?;
        self.included_relations.insert(table_id.as_object_id());

        let table_indexes = self.resolve_table_indexes(db_name, schema_name, table_id)?;

        let table = BoundBaseTable {
            table_id,
            table_catalog,
            table_indexes,
            as_of: as_of.cloned(),
        };

        Ok::<_, RwError>((Relation::BaseTable(Box::new(table)), columns))
    }

    fn resolve_source_relation(
        &mut self,
        source_catalog: &SourceCatalog,
        as_of: Option<&AsOf>,
        is_temporary: bool,
    ) -> Result<(Relation, Vec<(bool, Field)>)> {
        debug_assert_column_ids_distinct(&source_catalog.columns);
        if !is_temporary {
            self.check_privilege(
                ObjectCheckItem::new(
                    source_catalog.owner,
                    AclMode::Select,
                    source_catalog.name.clone(),
                    source_catalog.id,
                ),
                source_catalog.database_id,
            )?;
        }
        self.included_relations
            .insert(source_catalog.id.as_object_id());
        Ok((
            Relation::Source(Box::new(BoundSource {
                catalog: source_catalog.clone(),
                as_of: as_of.cloned(),
            })),
            source_catalog
                .columns
                .iter()
                .map(|c| (c.is_hidden, Field::from(&c.column_desc)))
                .collect_vec(),
        ))
    }

    fn resolve_view_relation(
        &mut self,
        view_catalog: &ViewCatalog,
    ) -> Result<(Relation, Vec<(bool, Field)>)> {
        if !view_catalog.is_system_view() {
            self.check_privilege(
                ObjectCheckItem::new(
                    view_catalog.owner,
                    AclMode::Select,
                    view_catalog.name.clone(),
                    view_catalog.id,
                ),
                view_catalog.database_id,
            )?;
        }

        let ast = Parser::parse_sql(&view_catalog.sql)
            .expect("a view's sql should be parsed successfully");
        let Statement::Query(query) = Itertools::exactly_one(ast.into_iter())
            .expect("a view should contain only one statement")
        else {
            unreachable!("a view should contain a query statement");
        };
        let query = self.bind_query_for_view(&query).map_err(|e| {
            ErrorCode::BindError(format!(
                "failed to bind view {}, sql: {}\nerror: {}",
                view_catalog.name,
                view_catalog.sql,
                e.as_report()
            ))
        })?;

        let columns = view_catalog.columns.clone();

        if !itertools::equal(
            query.schema().fields().iter().map(|f| &f.data_type),
            view_catalog.columns.iter().map(|f| &f.data_type),
        ) {
            return Err(ErrorCode::BindError(format!(
                "failed to bind view {}. The SQL's schema is different from catalog's schema sql: {}, bound schema: {:?}, catalog schema: {:?}",
                view_catalog.name, view_catalog.sql, query.schema(), columns
            )).into());
        }

        let share_id = match self.shared_views.get(&view_catalog.id) {
            Some(share_id) => *share_id,
            None => {
                let share_id = self.next_share_id();
                self.shared_views.insert(view_catalog.id, share_id);
                self.included_relations
                    .insert(view_catalog.id.as_object_id());
                share_id
            }
        };
        Ok((
            Relation::Share(Box::new(BoundShare {
                share_id,
                input: BoundShareInput::Query(query),
            })),
            columns.iter().map(|c| (false, c.clone())).collect_vec(),
        ))
    }

    fn resolve_table_indexes(
        &self,
        db_name: &str,
        schema_name: &str,
        table_id: TableId,
    ) -> Result<Vec<Arc<IndexCatalog>>> {
        let schema = self.catalog.get_schema_by_name(db_name, schema_name)?;
        assert!(
            schema.get_table_by_id(table_id).is_some() || table_id.is_placeholder(),
            "table {table_id} not found in {db_name}.{schema_name}"
        );

        Ok(schema.get_created_indexes_by_table_id(table_id))
    }

    pub(crate) fn bind_table(
        &mut self,
        schema_name: Option<&str>,
        table_name: &str,
    ) -> Result<BoundBaseTable> {
        let db_name = &self.db_name;
        let schema_path = self.bind_schema_path(schema_name);
        let (table_catalog, schema_name) =
            self.catalog
                .get_created_table_by_name(db_name, schema_path, table_name)?;
        let table_catalog = table_catalog.clone();

        let table_id = table_catalog.id();
        let table_indexes = self.resolve_table_indexes(db_name, schema_name, table_id)?;

        let columns = table_catalog.columns.clone();

        self.bind_table_to_context(
            columns
                .iter()
                .map(|c| (c.is_hidden, (&c.column_desc).into())),
            table_name.to_owned(),
            Some(schema_name.to_owned()),
            None,
        )?;

        Ok(BoundBaseTable {
            table_id,
            table_catalog,
            table_indexes,
            as_of: None,
        })
    }

    pub(crate) fn check_for_dml(table: &TableCatalog, is_insert: bool) -> Result<()> {
        let table_name = &table.name;
        match table.table_type() {
            TableType::Table => {}
            TableType::Index | TableType::VectorIndex => {
                return Err(ErrorCode::InvalidInputSyntax(format!(
                    "cannot change index \"{table_name}\""
                ))
                .into());
            }
            TableType::MaterializedView => {
                return Err(ErrorCode::InvalidInputSyntax(format!(
                    "cannot change materialized view \"{table_name}\""
                ))
                .into());
            }
            TableType::Internal => {
                return Err(ErrorCode::InvalidInputSyntax(format!(
                    "cannot change internal table \"{table_name}\""
                ))
                .into());
            }
        }

        if table.append_only && !is_insert {
            return Err(ErrorCode::BindError(
                "append-only table does not support update or delete".to_owned(),
            )
            .into());
        }

        Ok(())
    }
}
