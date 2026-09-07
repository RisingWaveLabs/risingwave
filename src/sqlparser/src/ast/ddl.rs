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

//! AST types specific to CREATE/ALTER variants of [`crate::ast::Statement`]
//! (commonly referred to as Data Definition Language, or DDL)

use std::fmt;

use super::{ConfigParam, FormatEncodeOptions, SqlOption};
use crate::ast::{
    DataType, Expr, Ident, ObjectName, Query, SecretRefValue, SetVariableValue, Value,
    display_comma_separated, display_separated,
};
use crate::tokenizer::Token;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AlterDatabaseOperation {
    ChangeOwner {
        new_owner_name: Ident,
    },
    RenameDatabase {
        database_name: ObjectName,
    },
    SetParam(ConfigParam),
    /// `SET RESOURCE_GROUP TO 'RESOURCE GROUP' [ DEFERRED ]`
    /// `RESET RESOURCE_GROUP [ DEFERRED ]`
    SetResourceGroup {
        resource_group: Option<SetVariableValue>,
        deferred: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AlterSchemaOperation {
    ChangeOwner { new_owner_name: Ident },
    RenameSchema { schema_name: ObjectName },
    SwapRenameSchema { target_schema: ObjectName },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AlterRateLimitType {
    Source,
    Backfill,
    Dml,
    Sink,
}

impl AlterRateLimitType {
    pub fn as_str(self) -> &'static str {
        match self {
            AlterRateLimitType::Source => "SOURCE_RATE_LIMIT",
            AlterRateLimitType::Backfill => "BACKFILL_RATE_LIMIT",
            AlterRateLimitType::Dml => "DML_RATE_LIMIT",
            AlterRateLimitType::Sink => "SINK_RATE_LIMIT",
        }
    }
}

impl fmt::Display for AlterRateLimitType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AlterRateLimit {
    pub rate_limit_type: AlterRateLimitType,
    pub rate_limit: i32,
}

impl fmt::Display for AlterRateLimit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SET {} TO {}", self.rate_limit_type, self.rate_limit)
    }
}

/// An `ALTER TABLE` (`Statement::AlterTable`) operation
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AlterTableOperation {
    /// `ADD <table_constraint>`
    AddConstraint(TableConstraint),
    /// `ADD [ COLUMN ] <column_def>`
    AddColumn {
        column_def: ColumnDef,
    },
    /// TODO: implement `DROP CONSTRAINT <name>`
    DropConstraint {
        name: Ident,
    },
    /// `DROP [ COLUMN ] [ IF EXISTS ] <column_name> [ CASCADE ]`
    DropColumn {
        column_name: Ident,
        if_exists: bool,
        cascade: bool,
    },
    /// `RENAME [ COLUMN ] <old_column_name> TO <new_column_name>`
    RenameColumn {
        old_column_name: Ident,
        new_column_name: Ident,
    },
    /// `RENAME TO <table_name>`
    RenameTable {
        table_name: ObjectName,
    },
    // CHANGE [ COLUMN ] <old_name> <new_name> <data_type> [ <options> ]
    ChangeColumn {
        old_name: Ident,
        new_name: Ident,
        data_type: DataType,
        options: Vec<ColumnOption>,
    },
    /// `RENAME CONSTRAINT <old_constraint_name> TO <new_constraint_name>`
    ///
    /// Note: this is a PostgreSQL-specific operation.
    RenameConstraint {
        old_name: Ident,
        new_name: Ident,
    },
    /// `ALTER [ COLUMN ]`
    AlterColumn {
        column_name: Ident,
        op: AlterColumnOperation,
    },
    /// `ALTER WATERMARK FOR <column> AS <expr> [WITH TTL]`
    AlterWatermark {
        column_name: Ident,
        expr: Expr,
        with_ttl: bool,
    },
    /// `OWNER TO <owner_name>`
    ChangeOwner {
        new_owner_name: Ident,
    },
    /// `SET SCHEMA <schema_name>`
    SetSchema {
        new_schema_name: ObjectName,
    },
    /// `SET PARALLELISM TO <parallelism> [ DEFERRED ]`
    SetParallelism {
        parallelism: SetVariableValue,
        deferred: bool,
    },
    /// `SET BACKFILL_PARALLELISM TO <parallelism> [ DEFERRED ]`
    SetBackfillParallelism {
        parallelism: SetVariableValue,
        deferred: bool,
    },
    /// `SET CONFIG (key = value, ...)`
    SetConfig {
        entries: Vec<SqlOption>,
    },
    /// `RESET CONFIG (key, ...)`
    ResetConfig {
        keys: Vec<ObjectName>,
    },
    RefreshSchema,
    AlterRateLimit(AlterRateLimit),
    /// `SWAP WITH <table_name>`
    SwapRenameTable {
        target_table: ObjectName,
    },
    /// `DROP CONNECTOR`
    DropConnector,

    /// `ALTER CONNECTOR WITH (<connector_props>)`
    AlterConnectorProps {
        alter_props: Vec<SqlOption>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AlterIndexOperation {
    RenameIndex {
        index_name: ObjectName,
    },
    /// `SET PARALLELISM TO <parallelism> [ DEFERRED ]`
    SetParallelism {
        parallelism: SetVariableValue,
        deferred: bool,
    },
    /// `SET BACKFILL_PARALLELISM TO <parallelism> [ DEFERRED ]`
    SetBackfillParallelism {
        parallelism: SetVariableValue,
        deferred: bool,
    },
    /// `SET RESOURCE_GROUP TO 'RESOURCE GROUP' [ DEFERRED ]`
    /// `RESET RESOURCE_GROUP [ DEFERRED ]`
    SetResourceGroup {
        resource_group: Option<SetVariableValue>,
        deferred: bool,
    },
    /// `SET CONFIG (key = value, ...)`
    SetConfig {
        entries: Vec<SqlOption>,
    },
    /// `RESET CONFIG (key, ...)`
    ResetConfig {
        keys: Vec<ObjectName>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AlterViewOperation {
    RenameView {
        view_name: ObjectName,
    },
    ChangeOwner {
        new_owner_name: Ident,
    },
    SetSchema {
        new_schema_name: ObjectName,
    },
    /// `SET PARALLELISM TO <parallelism> [ DEFERRED ]`
    SetParallelism {
        parallelism: SetVariableValue,
        deferred: bool,
    },
    /// `SET BACKFILL_PARALLELISM TO <parallelism> [ DEFERRED ]`
    SetBackfillParallelism {
        parallelism: SetVariableValue,
        deferred: bool,
    },
    /// `SET RESOURCE_GROUP TO 'RESOURCE GROUP' [ DEFERRED ]`
    /// `RESET RESOURCE_GROUP [ DEFERRED ]`
    SetResourceGroup {
        resource_group: Option<SetVariableValue>,
        deferred: bool,
    },
    AlterRateLimit(AlterRateLimit),
    /// `SWAP WITH <view_name>`
    SwapRenameView {
        target_view: ObjectName,
    },
    SetStreamingEnableUnalignedJoin {
        enable: bool,
    },
    /// `AS <query>`
    AsQuery {
        query: Box<Query>,
    },
    /// `SET CONFIG ( streaming.some_config_key = <some_config_value>, .. )`
    SetConfig {
        entries: Vec<SqlOption>,
    },
    /// `RESET CONFIG ( streaming.some_config_key, .. )`
    ResetConfig {
        keys: Vec<ObjectName>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AlterSinkOperation {
    RenameSink {
        sink_name: ObjectName,
    },
    ChangeOwner {
        new_owner_name: Ident,
    },
    SetSchema {
        new_schema_name: ObjectName,
    },
    /// `SET PARALLELISM TO <parallelism> [ DEFERRED ]`
    SetParallelism {
        parallelism: SetVariableValue,
        deferred: bool,
    },
    /// `SET BACKFILL_PARALLELISM TO <parallelism> [ DEFERRED ]`
    SetBackfillParallelism {
        parallelism: SetVariableValue,
        deferred: bool,
    },
    /// `SET RESOURCE_GROUP TO 'RESOURCE GROUP' [ DEFERRED ]`
    /// `RESET RESOURCE_GROUP [ DEFERRED ]`
    SetResourceGroup {
        resource_group: Option<SetVariableValue>,
        deferred: bool,
    },
    /// `SET CONFIG (key = value, ...)`
    SetConfig {
        entries: Vec<SqlOption>,
    },
    /// `RESET CONFIG (key, ...)`
    ResetConfig {
        keys: Vec<ObjectName>,
    },
    /// `SWAP WITH <sink_name>`
    SwapRenameSink {
        target_sink: ObjectName,
    },
    AlterRateLimit(AlterRateLimit),
    AlterConnectorProps {
        alter_props: Vec<SqlOption>,
    },
    SetStreamingEnableUnalignedJoin {
        enable: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AlterSubscriptionOperation {
    RenameSubscription { subscription_name: ObjectName },
    ChangeOwner { new_owner_name: Ident },
    SetSchema { new_schema_name: ObjectName },
    SetRetention { retention: Value },
    SwapRenameSubscription { target_subscription: ObjectName },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AlterSourceOperation {
    RenameSource {
        source_name: ObjectName,
    },
    AddColumn {
        column_def: ColumnDef,
    },
    ChangeOwner {
        new_owner_name: Ident,
    },
    SetSchema {
        new_schema_name: ObjectName,
    },
    FormatEncode {
        format_encode: FormatEncodeOptions,
    },
    RefreshSchema,
    AlterRateLimit(AlterRateLimit),
    SwapRenameSource {
        target_source: ObjectName,
    },
    /// `SET PARALLELISM TO <parallelism> [ DEFERRED ]`
    SetParallelism {
        parallelism: SetVariableValue,
        deferred: bool,
    },
    /// `SET BACKFILL_PARALLELISM TO <parallelism> [ DEFERRED ]`
    SetBackfillParallelism {
        parallelism: SetVariableValue,
        deferred: bool,
    },
    /// `SET CONFIG (key = value, ...)`
    SetConfig {
        entries: Vec<SqlOption>,
    },
    /// `RESET CONFIG (key, ...)`
    ResetConfig {
        keys: Vec<ObjectName>,
    },
    /// `RESET` - Reset CDC source offset to latest
    ResetSource,
    AlterConnectorProps {
        alter_props: Vec<SqlOption>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AlterFunctionOperation {
    SetSchema { new_schema_name: ObjectName },
    ChangeOwner { new_owner_name: Ident },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AlterConnectionOperation {
    SetSchema { new_schema_name: ObjectName },
    ChangeOwner { new_owner_name: Ident },
    AlterConnectorProps { alter_props: Vec<SqlOption> },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AlterSecretOperation {
    ChangeCredential {
        with_options: Vec<SqlOption>,
        new_credential: Value,
    },
    ChangeOwner {
        new_owner_name: Ident,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AlterFragmentOperation {
    AlterRateLimit(AlterRateLimit),
    SetParallelism { parallelism: SetVariableValue },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AlterCompactionGroupOperation {
    Set { configs: Vec<ConfigParam> },
}

impl fmt::Display for AlterDatabaseOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AlterDatabaseOperation::ChangeOwner { new_owner_name } => {
                write!(f, "OWNER TO {}", new_owner_name)
            }
            AlterDatabaseOperation::RenameDatabase { database_name } => {
                write!(f, "RENAME TO {}", database_name)
            }
            AlterDatabaseOperation::SetParam(ConfigParam { param, value }) => {
                write!(f, "SET {} TO {}", param, value)
            }
            AlterDatabaseOperation::SetResourceGroup {
                resource_group,
                deferred,
            } => {
                let deferred = if *deferred { " DEFERRED" } else { "" };

                if let Some(resource_group) = resource_group {
                    write!(f, "SET RESOURCE_GROUP TO {}{}", resource_group, deferred)
                } else {
                    write!(f, "RESET RESOURCE_GROUP{}", deferred)
                }
            }
        }
    }
}

impl fmt::Display for AlterSchemaOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AlterSchemaOperation::ChangeOwner { new_owner_name } => {
                write!(f, "OWNER TO {}", new_owner_name)
            }
            AlterSchemaOperation::RenameSchema { schema_name } => {
                write!(f, "RENAME TO {}", schema_name)
            }
            AlterSchemaOperation::SwapRenameSchema { target_schema } => {
                write!(f, "SWAP WITH {}", target_schema)
            }
        }
    }
}

impl fmt::Display for AlterTableOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AlterTableOperation::AddConstraint(c) => write!(f, "ADD {}", c),
            AlterTableOperation::AddColumn { column_def } => {
                write!(f, "ADD COLUMN {}", column_def)
            }
            AlterTableOperation::AlterColumn { column_name, op } => {
                write!(f, "ALTER COLUMN {} {}", column_name, op)
            }
            AlterTableOperation::AlterWatermark {
                column_name,
                expr,
                with_ttl,
            } => {
                write!(f, "ALTER WATERMARK FOR {} AS {}", column_name, expr)?;
                if *with_ttl {
                    write!(f, " WITH TTL")?;
                }
                Ok(())
            }
            AlterTableOperation::DropConstraint { name } => write!(f, "DROP CONSTRAINT {}", name),
            AlterTableOperation::DropColumn {
                column_name,
                if_exists,
                cascade,
            } => write!(
                f,
                "DROP COLUMN {}{}{}",
                if *if_exists { "IF EXISTS " } else { "" },
                column_name,
                if *cascade { " CASCADE" } else { "" }
            ),
            AlterTableOperation::RenameColumn {
                old_column_name,
                new_column_name,
            } => write!(
                f,
                "RENAME COLUMN {} TO {}",
                old_column_name, new_column_name
            ),
            AlterTableOperation::RenameTable { table_name } => {
                write!(f, "RENAME TO {}", table_name)
            }
            AlterTableOperation::ChangeColumn {
                old_name,
                new_name,
                data_type,
                options,
            } => {
                write!(f, "CHANGE COLUMN {} {} {}", old_name, new_name, data_type)?;
                if options.is_empty() {
                    Ok(())
                } else {
                    write!(f, " {}", display_separated(options, " "))
                }
            }
            AlterTableOperation::RenameConstraint { old_name, new_name } => {
                write!(f, "RENAME CONSTRAINT {} TO {}", old_name, new_name)
            }
            AlterTableOperation::ChangeOwner { new_owner_name } => {
                write!(f, "OWNER TO {}", new_owner_name)
            }
            AlterTableOperation::SetSchema { new_schema_name } => {
                write!(f, "SET SCHEMA {}", new_schema_name)
            }
            AlterTableOperation::SetParallelism {
                parallelism,
                deferred,
            } => {
                write!(
                    f,
                    "SET PARALLELISM TO {}{}",
                    parallelism,
                    if *deferred { " DEFERRED" } else { "" }
                )
            }
            AlterTableOperation::SetBackfillParallelism {
                parallelism,
                deferred,
            } => {
                write!(
                    f,
                    "SET BACKFILL_PARALLELISM TO {}{}",
                    parallelism,
                    if *deferred { " DEFERRED" } else { "" }
                )
            }
            AlterTableOperation::SetConfig { entries } => {
                write!(f, "SET CONFIG ({})", display_comma_separated(entries))
            }
            AlterTableOperation::ResetConfig { keys } => {
                write!(f, "RESET CONFIG ({})", display_comma_separated(keys))
            }
            AlterTableOperation::RefreshSchema => {
                write!(f, "REFRESH SCHEMA")
            }
            AlterTableOperation::AlterRateLimit(rate_limit) => write!(f, "{rate_limit}"),
            AlterTableOperation::SwapRenameTable { target_table } => {
                write!(f, "SWAP WITH {}", target_table)
            }
            AlterTableOperation::DropConnector => {
                write!(f, "DROP CONNECTOR")
            }
            AlterTableOperation::AlterConnectorProps { alter_props } => {
                write!(
                    f,
                    "CONNECTOR WITH ({})",
                    display_comma_separated(alter_props)
                )
            }
        }
    }
}

impl fmt::Display for AlterIndexOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AlterIndexOperation::RenameIndex { index_name } => {
                write!(f, "RENAME TO {index_name}")
            }
            AlterIndexOperation::SetParallelism {
                parallelism,
                deferred,
            } => {
                write!(
                    f,
                    "SET PARALLELISM TO {}{}",
                    parallelism,
                    if *deferred { " DEFERRED" } else { "" }
                )
            }
            AlterIndexOperation::SetBackfillParallelism {
                parallelism,
                deferred,
            } => {
                write!(
                    f,
                    "SET BACKFILL_PARALLELISM TO {}{}",
                    parallelism,
                    if *deferred { " DEFERRED" } else { "" }
                )
            }
            AlterIndexOperation::SetResourceGroup {
                resource_group,
                deferred,
            } => {
                let deferred = if *deferred { " DEFERRED" } else { "" };

                if let Some(resource_group) = resource_group {
                    write!(f, "SET RESOURCE_GROUP TO {}{}", resource_group, deferred)
                } else {
                    write!(f, "RESET RESOURCE_GROUP{}", deferred)
                }
            }
            AlterIndexOperation::SetConfig { entries } => {
                write!(f, "SET CONFIG ({})", display_comma_separated(entries))
            }
            AlterIndexOperation::ResetConfig { keys } => {
                write!(f, "RESET CONFIG ({})", display_comma_separated(keys))
            }
        }
    }
}

impl fmt::Display for AlterViewOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AlterViewOperation::RenameView { view_name } => {
                write!(f, "RENAME TO {view_name}")
            }
            AlterViewOperation::ChangeOwner { new_owner_name } => {
                write!(f, "OWNER TO {}", new_owner_name)
            }
            AlterViewOperation::SetSchema { new_schema_name } => {
                write!(f, "SET SCHEMA {}", new_schema_name)
            }
            AlterViewOperation::SetParallelism {
                parallelism,
                deferred,
            } => {
                write!(
                    f,
                    "SET PARALLELISM TO {}{}",
                    parallelism,
                    if *deferred { " DEFERRED" } else { "" }
                )
            }
            AlterViewOperation::SetBackfillParallelism {
                parallelism,
                deferred,
            } => {
                write!(
                    f,
                    "SET BACKFILL_PARALLELISM TO {}{}",
                    parallelism,
                    if *deferred { " DEFERRED" } else { "" }
                )
            }
            AlterViewOperation::AlterRateLimit(rate_limit) => write!(f, "{rate_limit}"),
            AlterViewOperation::SwapRenameView { target_view } => {
                write!(f, "SWAP WITH {}", target_view)
            }
            AlterViewOperation::SetResourceGroup {
                resource_group,
                deferred,
            } => {
                let deferred = if *deferred { " DEFERRED" } else { "" };

                if let Some(resource_group) = resource_group {
                    write!(f, "SET RESOURCE_GROUP TO {}{}", resource_group, deferred)
                } else {
                    write!(f, "RESET RESOURCE_GROUP{}", deferred)
                }
            }
            AlterViewOperation::SetStreamingEnableUnalignedJoin { enable } => {
                write!(f, "SET STREAMING_ENABLE_UNALIGNED_JOIN TO {}", enable)
            }
            AlterViewOperation::AsQuery { query } => {
                write!(f, "AS {}", query)
            }
            AlterViewOperation::SetConfig { entries } => {
                write!(f, "SET CONFIG ({})", display_comma_separated(entries))
            }
            AlterViewOperation::ResetConfig { keys } => {
                write!(f, "RESET CONFIG ({})", display_comma_separated(keys))
            }
        }
    }
}

impl fmt::Display for AlterSinkOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AlterSinkOperation::RenameSink { sink_name } => {
                write!(f, "RENAME TO {sink_name}")
            }
            AlterSinkOperation::ChangeOwner { new_owner_name } => {
                write!(f, "OWNER TO {}", new_owner_name)
            }
            AlterSinkOperation::SetSchema { new_schema_name } => {
                write!(f, "SET SCHEMA {}", new_schema_name)
            }
            AlterSinkOperation::SetParallelism {
                parallelism,
                deferred,
            } => {
                write!(
                    f,
                    "SET PARALLELISM TO {}{}",
                    parallelism,
                    if *deferred { " DEFERRED" } else { "" }
                )
            }
            AlterSinkOperation::SetBackfillParallelism {
                parallelism,
                deferred,
            } => {
                write!(
                    f,
                    "SET BACKFILL_PARALLELISM TO {}{}",
                    parallelism,
                    if *deferred { " DEFERRED" } else { "" }
                )
            }
            AlterSinkOperation::SetResourceGroup {
                resource_group,
                deferred,
            } => {
                let deferred = if *deferred { " DEFERRED" } else { "" };

                if let Some(resource_group) = resource_group {
                    write!(f, "SET RESOURCE_GROUP TO {}{}", resource_group, deferred)
                } else {
                    write!(f, "RESET RESOURCE_GROUP{}", deferred)
                }
            }
            AlterSinkOperation::SetConfig { entries } => {
                write!(f, "SET CONFIG ({})", display_comma_separated(entries))
            }
            AlterSinkOperation::ResetConfig { keys } => {
                write!(f, "RESET CONFIG ({})", display_comma_separated(keys))
            }
            AlterSinkOperation::SwapRenameSink { target_sink } => {
                write!(f, "SWAP WITH {}", target_sink)
            }
            AlterSinkOperation::AlterRateLimit(rate_limit) => write!(f, "{rate_limit}"),
            AlterSinkOperation::AlterConnectorProps {
                alter_props: changed_props,
            } => {
                write!(
                    f,
                    "CONNECTOR WITH ({})",
                    display_comma_separated(changed_props)
                )
            }
            AlterSinkOperation::SetStreamingEnableUnalignedJoin { enable } => {
                write!(f, "SET STREAMING_ENABLE_UNALIGNED_JOIN TO {}", enable)
            }
        }
    }
}

impl fmt::Display for AlterSubscriptionOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AlterSubscriptionOperation::RenameSubscription { subscription_name } => {
                write!(f, "RENAME TO {subscription_name}")
            }
            AlterSubscriptionOperation::ChangeOwner { new_owner_name } => {
                write!(f, "OWNER TO {}", new_owner_name)
            }
            AlterSubscriptionOperation::SetSchema { new_schema_name } => {
                write!(f, "SET SCHEMA {}", new_schema_name)
            }
            AlterSubscriptionOperation::SetRetention { retention } => {
                write!(f, "SET RETENTION TO {}", retention)
            }
            AlterSubscriptionOperation::SwapRenameSubscription {
                target_subscription,
            } => {
                write!(f, "SWAP WITH {}", target_subscription)
            }
        }
    }
}

impl fmt::Display for AlterSourceOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AlterSourceOperation::RenameSource { source_name } => {
                write!(f, "RENAME TO {source_name}")
            }
            AlterSourceOperation::AddColumn { column_def } => {
                write!(f, "ADD COLUMN {column_def}")
            }
            AlterSourceOperation::ChangeOwner { new_owner_name } => {
                write!(f, "OWNER TO {}", new_owner_name)
            }
            AlterSourceOperation::SetSchema { new_schema_name } => {
                write!(f, "SET SCHEMA {}", new_schema_name)
            }
            AlterSourceOperation::FormatEncode { format_encode } => {
                write!(f, "{format_encode}")
            }
            AlterSourceOperation::RefreshSchema => {
                write!(f, "REFRESH SCHEMA")
            }
            AlterSourceOperation::AlterRateLimit(rate_limit) => write!(f, "{rate_limit}"),
            AlterSourceOperation::SwapRenameSource { target_source } => {
                write!(f, "SWAP WITH {}", target_source)
            }
            AlterSourceOperation::SetParallelism {
                parallelism,
                deferred,
            } => {
                write!(
                    f,
                    "SET PARALLELISM TO {}{}",
                    parallelism,
                    if *deferred { " DEFERRED" } else { "" }
                )
            }
            AlterSourceOperation::SetBackfillParallelism {
                parallelism,
                deferred,
            } => {
                write!(
                    f,
                    "SET BACKFILL_PARALLELISM TO {}{}",
                    parallelism,
                    if *deferred { " DEFERRED" } else { "" }
                )
            }
            AlterSourceOperation::SetConfig { entries } => {
                write!(f, "SET CONFIG ({})", display_comma_separated(entries))
            }
            AlterSourceOperation::ResetConfig { keys } => {
                write!(f, "RESET CONFIG ({})", display_comma_separated(keys))
            }
            AlterSourceOperation::ResetSource => {
                write!(f, "RESET")
            }
            AlterSourceOperation::AlterConnectorProps { alter_props } => {
                write!(
                    f,
                    "CONNECTOR WITH ({})",
                    display_comma_separated(alter_props)
                )
            }
        }
    }
}

impl fmt::Display for AlterFunctionOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AlterFunctionOperation::SetSchema { new_schema_name } => {
                write!(f, "SET SCHEMA {new_schema_name}")
            }
            AlterFunctionOperation::ChangeOwner { new_owner_name } => {
                write!(f, "OWNER TO {new_owner_name}")
            }
        }
    }
}

impl fmt::Display for AlterConnectionOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AlterConnectionOperation::SetSchema { new_schema_name } => {
                write!(f, "SET SCHEMA {new_schema_name}")
            }
            AlterConnectionOperation::ChangeOwner { new_owner_name } => {
                write!(f, "OWNER TO {new_owner_name}")
            }
            AlterConnectionOperation::AlterConnectorProps { alter_props } => {
                write!(
                    f,
                    "CONNECTOR WITH ({})",
                    display_comma_separated(alter_props)
                )
            }
        }
    }
}

impl fmt::Display for AlterSecretOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AlterSecretOperation::ChangeCredential {
                new_credential,
                with_options,
            } => {
                write!(
                    f,
                    "WITH ({}) AS {}",
                    display_comma_separated(with_options),
                    new_credential
                )
            }
            AlterSecretOperation::ChangeOwner { new_owner_name } => {
                write!(f, "OWNER TO {new_owner_name}")
            }
        }
    }
}

/// An `ALTER COLUMN` (`Statement::AlterTable`) operation
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AlterColumnOperation {
    /// `SET NOT NULL`
    SetNotNull,
    /// `DROP NOT NULL`
    DropNotNull,
    /// `SET DEFAULT <expr>`
    SetDefault { value: Expr },
    /// `DROP DEFAULT`
    DropDefault,
    /// `[SET DATA] TYPE <data_type> [USING <expr>]`
    SetDataType {
        data_type: DataType,
        /// PostgreSQL specific
        using: Option<Expr>,
    },
}

impl fmt::Display for AlterColumnOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AlterColumnOperation::SetNotNull => write!(f, "SET NOT NULL",),
            AlterColumnOperation::DropNotNull => write!(f, "DROP NOT NULL",),
            AlterColumnOperation::SetDefault { value } => {
                write!(f, "SET DEFAULT {}", value)
            }
            AlterColumnOperation::DropDefault => {
                write!(f, "DROP DEFAULT")
            }
            AlterColumnOperation::SetDataType { data_type, using } => {
                if let Some(expr) = using {
                    write!(f, "SET DATA TYPE {} USING {}", data_type, expr)
                } else {
                    write!(f, "SET DATA TYPE {}", data_type)
                }
            }
        }
    }
}

impl fmt::Display for AlterFragmentOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AlterFragmentOperation::AlterRateLimit(rate_limit) => write!(f, "{rate_limit}"),
            AlterFragmentOperation::SetParallelism { parallelism } => {
                write!(f, "SET PARALLELISM TO {}", parallelism)
            }
        }
    }
}

impl fmt::Display for AlterCompactionGroupOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AlterCompactionGroupOperation::Set { configs } => {
                struct Assign<'a>(&'a ConfigParam);

                impl fmt::Display for Assign<'_> {
                    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                        write!(f, "{} = {}", self.0.param, self.0.value)
                    }
                }

                let assigns: Vec<Assign<'_>> = configs.iter().map(Assign).collect();
                write!(f, "SET {}", display_comma_separated(&assigns))
            }
        }
    }
}

/// The watermark on source.
/// `WATERMARK FOR <column> AS (<expr>)`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SourceWatermark {
    pub column: Ident,
    pub expr: Expr,
    /// Whether `WITH TTL` is specified.
    pub with_ttl: bool,
}

impl fmt::Display for SourceWatermark {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "WATERMARK FOR {} AS {}", self.column, self.expr,)?;
        if self.with_ttl {
            write!(f, " WITH TTL")?;
        }
        Ok(())
    }
}

/// A table-level constraint, specified in a `CREATE TABLE` or an
/// `ALTER TABLE ADD <constraint>` statement.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TableConstraint {
    /// `[ CONSTRAINT <name> ] { PRIMARY KEY | UNIQUE } (<columns>)`
    Unique {
        name: Option<Ident>,
        columns: Vec<Ident>,
        /// Whether this is a `PRIMARY KEY` or just a `UNIQUE` constraint
        is_primary: bool,
    },
    /// A referential integrity constraint (`[ CONSTRAINT <name> ] FOREIGN KEY (<columns>)
    /// REFERENCES <foreign_table> (<referred_columns>)
    /// { [ON DELETE <referential_action>] [ON UPDATE <referential_action>] |
    ///   [ON UPDATE <referential_action>] [ON DELETE <referential_action>]
    /// }`).
    ForeignKey {
        name: Option<Ident>,
        columns: Vec<Ident>,
        foreign_table: ObjectName,
        referred_columns: Vec<Ident>,
        on_delete: Option<ReferentialAction>,
        on_update: Option<ReferentialAction>,
    },
    /// `[ CONSTRAINT <name> ] CHECK (<expr>)`
    Check {
        name: Option<Ident>,
        expr: Box<Expr>,
    },
}

impl fmt::Display for TableConstraint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TableConstraint::Unique {
                name,
                columns,
                is_primary,
            } => write!(
                f,
                "{}{} ({})",
                display_constraint_name(name),
                if *is_primary { "PRIMARY KEY" } else { "UNIQUE" },
                display_comma_separated(columns)
            ),
            TableConstraint::ForeignKey {
                name,
                columns,
                foreign_table,
                referred_columns,
                on_delete,
                on_update,
            } => {
                write!(
                    f,
                    "{}FOREIGN KEY ({}) REFERENCES {}({})",
                    display_constraint_name(name),
                    display_comma_separated(columns),
                    foreign_table,
                    display_comma_separated(referred_columns),
                )?;
                if let Some(action) = on_delete {
                    write!(f, " ON DELETE {}", action)?;
                }
                if let Some(action) = on_update {
                    write!(f, " ON UPDATE {}", action)?;
                }
                Ok(())
            }
            TableConstraint::Check { name, expr } => {
                write!(f, "{}CHECK ({})", display_constraint_name(name), expr)
            }
        }
    }
}

/// SQL column definition
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnDef {
    pub name: Ident,
    pub data_type: Option<DataType>,
    pub collation: Option<ObjectName>,
    pub options: Vec<ColumnOptionDef>,
}

impl ColumnDef {
    pub fn new(
        name: Ident,
        data_type: DataType,
        collation: Option<ObjectName>,
        options: Vec<ColumnOptionDef>,
    ) -> Self {
        ColumnDef {
            name,
            data_type: Some(data_type),
            collation,
            options,
        }
    }

    pub fn is_generated(&self) -> bool {
        self.options
            .iter()
            .any(|option| matches!(option.option, ColumnOption::GeneratedColumns(_)))
    }
}

impl fmt::Display for ColumnDef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} {}",
            self.name,
            if let Some(data_type) = &self.data_type {
                data_type.to_string()
            } else {
                "None".to_owned()
            }
        )?;
        for option in &self.options {
            write!(f, " {}", option)?;
        }
        Ok(())
    }
}

/// An optionally-named `ColumnOption`: `[ CONSTRAINT <name> ] <column-option>`.
///
/// Note that implementations are substantially more permissive than the ANSI
/// specification on what order column options can be presented in, and whether
/// they are allowed to be named. The specification distinguishes between
/// constraints (NOT NULL, UNIQUE, PRIMARY KEY, and CHECK), which can be named
/// and can appear in any order, and other options (DEFAULT, GENERATED), which
/// cannot be named and must appear in a fixed order. PostgreSQL, however,
/// allows preceding any option with `CONSTRAINT <name>`, even those that are
/// not really constraints, like NULL and DEFAULT. MSSQL is less permissive,
/// allowing DEFAULT, UNIQUE, PRIMARY KEY and CHECK to be named, but not NULL or
/// NOT NULL constraints (the last of which is in violation of the spec).
///
/// For maximum flexibility, we don't distinguish between constraint and
/// non-constraint options, lumping them all together under the umbrella of
/// "column options," and we allow any column option to be named.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnOptionDef {
    pub name: Option<Ident>,
    pub option: ColumnOption,
}

impl fmt::Display for ColumnOptionDef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", display_constraint_name(&self.name), self.option)
    }
}

/// `ColumnOption`s are modifiers that follow a column definition in a `CREATE
/// TABLE` statement.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ColumnOption {
    /// `NULL`
    Null,
    /// `NOT NULL`
    NotNull,
    /// `DEFAULT <restricted-expr>`
    DefaultValue(Expr),
    /// Default value from previous bound `DefaultColumnDesc`. Used internally
    /// for schema change and should not be specified by users.
    DefaultValueInternal {
        /// Protobuf encoded `DefaultColumnDesc`.
        persisted: Box<[u8]>,
        /// Optional AST for unparsing. If `None`, the default value will be
        /// shown as `DEFAULT INTERNAL` which is for demonstrating and should
        /// not be specified by users.
        expr: Option<Expr>,
    },
    /// `{ PRIMARY KEY | UNIQUE }`
    Unique { is_primary: bool },
    /// A referential integrity constraint (`[FOREIGN KEY REFERENCES
    /// <foreign_table> (<referred_columns>)
    /// { [ON DELETE <referential_action>] [ON UPDATE <referential_action>] |
    ///   [ON UPDATE <referential_action>] [ON DELETE <referential_action>]
    /// }`).
    ForeignKey {
        foreign_table: ObjectName,
        referred_columns: Vec<Ident>,
        on_delete: Option<ReferentialAction>,
        on_update: Option<ReferentialAction>,
    },
    /// `CHECK (<expr>)`
    Check(Expr),
    /// Dialect-specific options, such as:
    /// - MySQL's `AUTO_INCREMENT` or SQLite's `AUTOINCREMENT`
    /// - ...
    DialectSpecific(Vec<Token>),
    /// AS ( <generation_expr> )`
    GeneratedColumns(Expr),
}

impl fmt::Display for ColumnOption {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use ColumnOption::*;
        match self {
            Null => write!(f, "NULL"),
            NotNull => write!(f, "NOT NULL"),
            DefaultValue(expr) => write!(f, "DEFAULT {}", expr),
            DefaultValueInternal { persisted: _, expr } => {
                if let Some(expr) = expr {
                    write!(f, "DEFAULT {}", expr)
                } else {
                    write!(f, "DEFAULT INTERNAL")
                }
            }
            Unique { is_primary } => {
                write!(f, "{}", if *is_primary { "PRIMARY KEY" } else { "UNIQUE" })
            }
            ForeignKey {
                foreign_table,
                referred_columns,
                on_delete,
                on_update,
            } => {
                write!(f, "REFERENCES {}", foreign_table)?;
                if !referred_columns.is_empty() {
                    write!(f, " ({})", display_comma_separated(referred_columns))?;
                }
                if let Some(action) = on_delete {
                    write!(f, " ON DELETE {}", action)?;
                }
                if let Some(action) = on_update {
                    write!(f, " ON UPDATE {}", action)?;
                }
                Ok(())
            }
            Check(expr) => write!(f, "CHECK ({})", expr),
            DialectSpecific(val) => write!(f, "{}", display_separated(val, " ")),
            GeneratedColumns(expr) => write!(f, "AS {}", expr),
        }
    }
}

fn display_constraint_name(name: &'_ Option<Ident>) -> impl fmt::Display + '_ {
    struct ConstraintName<'a>(&'a Option<Ident>);
    impl fmt::Display for ConstraintName<'_> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            if let Some(name) = self.0 {
                write!(f, "CONSTRAINT {} ", name)?;
            }
            Ok(())
        }
    }
    ConstraintName(name)
}

/// `<referential_action> =
/// { RESTRICT | CASCADE | SET NULL | NO ACTION | SET DEFAULT }`
///
/// Used in foreign key constraints in `ON UPDATE` and `ON DELETE` options.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ReferentialAction {
    Restrict,
    Cascade,
    SetNull,
    NoAction,
    SetDefault,
}

impl fmt::Display for ReferentialAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            ReferentialAction::Restrict => "RESTRICT",
            ReferentialAction::Cascade => "CASCADE",
            ReferentialAction::SetNull => "SET NULL",
            ReferentialAction::NoAction => "NO ACTION",
            ReferentialAction::SetDefault => "SET DEFAULT",
        })
    }
}

/// secure secret definition for webhook source
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WebhookSourceInfo {
    pub secret_ref: Option<SecretRefValue>,
    pub signature_expr: Option<Expr>,
    pub wait_for_persistence: bool,
    pub is_batched: bool,
}
