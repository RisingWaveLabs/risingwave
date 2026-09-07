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

use anyhow::Context;
use itertools::Itertools;
use mysql_async::consts::ColumnType as MySqlColumnType;
use mysql_async::prelude::*;
use risingwave_common::array::arrow::IcebergArrowConvert;
use risingwave_common::secret::LocalSecretManager;
use risingwave_common::types::{DataType, ScalarImpl, StructType};
use risingwave_connector::connector_common::sql_server::{
    MssqlConnectionConfig, describe_mssql_query,
};
use risingwave_connector::connector_common::{PgConnectionConfig, create_pg_client};
use risingwave_connector::source::iceberg::{
    FileScanBackend, extract_bucket_and_file_name, get_parquet_fields, list_data_directory,
    new_azblob_operator, new_gcs_operator, new_s3_operator,
};
use risingwave_pb::expr::PbTableFunction;
pub use risingwave_pb::expr::table_function::PbType as TableFunctionType;
use tokio_postgres::types::Type as TokioPgType;

use super::{Expr, ExprImpl, ExprRewriter, Literal, RwResult, infer_type};
use crate::catalog::catalog_service::CatalogReadGuard;
use crate::catalog::function_catalog::{FunctionCatalog, FunctionKind};
use crate::catalog::root_catalog::SchemaPath;
use crate::error::ErrorCode::BindError;
use crate::expr::reject_impure;
use crate::utils::FRONTEND_RUNTIME;

const INLINE_ARG_LEN: usize = 6;
/// Inline arg length for the SQL Server flavor of `_query` TVFs, which appends
/// `encrypt` and `trust_cert` (8 total) on top of the standard 6.
const MSSQL_INLINE_ARG_LEN: usize = 8;
const CDC_SOURCE_ARG_LEN: usize = 2;

/// Parse a strict boolean TVF argument. Returns the default when the argument
/// is absent (e.g. the 2-arg source-reference form) and an error when the user
/// supplies a value that is not a case-insensitive `true` / `false`. We avoid
/// `s.parse::<bool>().unwrap_or(default)` because that would silently coerce
/// typos like `yes` or `1` — and a typo on a security-critical flag like
/// `trust_cert` would weaken transport security without any user-visible error.
fn parse_strict_bool_arg(value: Option<&String>, default: bool) -> anyhow::Result<bool> {
    match value {
        None => Ok(default),
        Some(s) => match s.trim().to_ascii_lowercase().as_str() {
            "true" => Ok(true),
            "false" => Ok(false),
            other => Err(anyhow::anyhow!(
                "expected 'true' or 'false', got {:?}",
                other
            )),
        },
    }
}

/// A table function takes a row as input and returns a table. It is also known as Set-Returning
/// Function.
///
/// See also [`TableFunction`](risingwave_expr::table_function::TableFunction) trait in expr crate
/// and [`ProjectSetSelectItem`](risingwave_pb::expr::ProjectSetSelectItem).
#[derive(Clone, Eq, PartialEq, Hash)]
pub struct TableFunction {
    pub args: Vec<ExprImpl>,
    pub return_type: DataType,
    pub function_type: TableFunctionType,
    /// Catalog of user defined table function.
    pub user_defined: Option<Arc<FunctionCatalog>>,
}

impl TableFunction {
    /// Create a `TableFunction` expr with the return type inferred from `func_type` and types of
    /// `inputs`.
    pub fn new(func_type: TableFunctionType, mut args: Vec<ExprImpl>) -> RwResult<Self> {
        let return_type = infer_type(func_type.into(), &mut args)?;
        Ok(TableFunction {
            args,
            return_type,
            function_type: func_type,
            user_defined: None,
        })
    }

    /// Create a user-defined `TableFunction`.
    pub fn new_user_defined(catalog: Arc<FunctionCatalog>, args: Vec<ExprImpl>) -> Self {
        let FunctionKind::Table = &catalog.kind else {
            panic!("not a table function");
        };
        TableFunction {
            args,
            return_type: catalog.return_type.clone(),
            function_type: TableFunctionType::UserDefined,
            user_defined: Some(catalog),
        }
    }

    /// A special table function which would be transformed into `LogicalFileScan` by `TableFunctionToFileScanRule` in the optimizer.
    /// select * from `file_scan`('parquet', 's3', region, ak, sk, location)
    pub fn new_file_scan(mut args: Vec<ExprImpl>) -> RwResult<Self> {
        let return_type = {
            // arguments:
            // file format e.g. parquet
            // storage type e.g. s3, gcs, azblob
            // For s3: file_scan('parquet', 's3', s3_region, s3_access_key, s3_secret_key, file_location_or_directory)
            // For gcs: file_scan('parquet', 'gcs', credential, file_location_or_directory)
            // For azblob: file_scan('parquet', 'azblob', endpoint, account_name, account_key, file_location)
            let mut eval_args: Vec<String> = vec![];
            for arg in &args {
                if arg.return_type() != DataType::Varchar {
                    return Err(BindError(
                        "file_scan function only accepts string arguments".to_owned(),
                    )
                    .into());
                }
                match arg.try_fold_const() {
                    Some(Ok(value)) => {
                        if value.is_none() {
                            return Err(BindError(
                                "file_scan function does not accept null arguments".to_owned(),
                            )
                            .into());
                        }
                        match value {
                            Some(ScalarImpl::Utf8(s)) => {
                                eval_args.push(s.to_string());
                            }
                            _ => {
                                return Err(BindError(
                                    "file_scan function only accepts string arguments".to_owned(),
                                )
                                .into());
                            }
                        }
                    }
                    Some(Err(err)) => {
                        return Err(err);
                    }
                    None => {
                        return Err(BindError(
                            "file_scan function only accepts constant arguments".to_owned(),
                        )
                        .into());
                    }
                }
            }

            if (eval_args.len() != 4 && eval_args.len() != 6)
                || (eval_args.len() == 4 && !"gcs".eq_ignore_ascii_case(&eval_args[1]))
                || (eval_args.len() == 6
                    && !"s3".eq_ignore_ascii_case(&eval_args[1])
                    && !"azblob".eq_ignore_ascii_case(&eval_args[1]))
            {
                return Err(BindError(
                "file_scan function supports three backends: s3, gcs, and azblob. Their formats are as follows: \n
                    file_scan('parquet', 's3', s3_region, s3_access_key, s3_secret_key, file_location) \n
                    file_scan('parquet', 'gcs', credential, service_account, file_location) \n
                    file_scan('parquet', 'azblob', endpoint, account_name, account_key, file_location)"
                        .to_owned(),
                )
                .into());
            }
            if !"parquet".eq_ignore_ascii_case(&eval_args[0]) {
                return Err(BindError(
                    "file_scan function only accepts 'parquet' as file format".to_owned(),
                )
                .into());
            }

            if !"s3".eq_ignore_ascii_case(&eval_args[1])
                && !"gcs".eq_ignore_ascii_case(&eval_args[1])
                && !"azblob".eq_ignore_ascii_case(&eval_args[1])
            {
                return Err(BindError(
                    "file_scan function only accepts 's3', 'gcs' or 'azblob' as storage type"
                        .to_owned(),
                )
                .into());
            }

            #[cfg(madsim)]
            return Err(crate::error::ErrorCode::BindError(
                "file_scan can't be used in the madsim mode".to_string(),
            )
            .into());

            #[cfg(not(madsim))]
            {
                let (file_scan_backend, input_file_location) =
                    if "s3".eq_ignore_ascii_case(&eval_args[1]) {
                        (FileScanBackend::S3, eval_args[5].clone())
                    } else if "gcs".eq_ignore_ascii_case(&eval_args[1]) {
                        (FileScanBackend::Gcs, eval_args[3].clone())
                    } else if "azblob".eq_ignore_ascii_case(&eval_args[1]) {
                        (FileScanBackend::Azblob, eval_args[5].clone())
                    } else {
                        unreachable!();
                    };
                let op = match file_scan_backend {
                    FileScanBackend::S3 => {
                        let (bucket, _) = extract_bucket_and_file_name(
                            &eval_args[5].clone(),
                            &file_scan_backend,
                        )?;

                        let (s3_region, s3_endpoint) = match eval_args[2].starts_with("http") {
                            true => ("us-east-1".to_owned(), eval_args[2].clone()), /* for minio, hard code region as not used but needed. */
                            false => (
                                eval_args[2].clone(),
                                format!("https://{}.s3.{}.amazonaws.com", bucket, eval_args[2],),
                            ),
                        };
                        new_s3_operator(
                            s3_region,
                            eval_args[3].clone(),
                            eval_args[4].clone(),
                            bucket,
                            s3_endpoint,
                        )?
                    }
                    FileScanBackend::Gcs => {
                        let (bucket, _) =
                            extract_bucket_and_file_name(&input_file_location, &file_scan_backend)?;

                        new_gcs_operator(eval_args[2].clone(), bucket)?
                    }
                    FileScanBackend::Azblob => {
                        let (bucket, _) =
                            extract_bucket_and_file_name(&input_file_location, &file_scan_backend)?;

                        new_azblob_operator(
                            eval_args[2].clone(),
                            eval_args[3].clone(),
                            eval_args[4].clone(),
                            bucket,
                        )?
                    }
                };
                let files = if input_file_location.ends_with('/') {
                    let files = tokio::task::block_in_place(|| {
                        FRONTEND_RUNTIME.block_on(async {
                            let files = list_data_directory(
                                op.clone(),
                                input_file_location.clone(),
                                &file_scan_backend,
                            )
                            .await?;

                            Ok::<Vec<String>, anyhow::Error>(files)
                        })
                    })?;
                    if files.is_empty() {
                        return Err(BindError(
                            "file_scan function only accepts non-empty directory".to_owned(),
                        )
                        .into());
                    }

                    Some(files)
                } else {
                    None
                };
                let schema = tokio::task::block_in_place(|| {
                    FRONTEND_RUNTIME.block_on(async {
                        let location = match files.as_ref() {
                            Some(files) => files[0].clone(),
                            None => input_file_location.clone(),
                        };
                        let (_, file_name) =
                            extract_bucket_and_file_name(&location, &file_scan_backend)?;

                        let fields = get_parquet_fields(op, file_name).await?;

                        let mut rw_types = vec![];
                        for field in &fields {
                            rw_types.push((
                                field.name().clone(),
                                IcebergArrowConvert.type_from_field(field)?,
                            ));
                        }

                        Ok::<risingwave_common::types::DataType, anyhow::Error>(DataType::Struct(
                            StructType::new(rw_types),
                        ))
                    })
                })?;

                if let Some(files) = files {
                    // if the file location is a directory, we need to remove the last argument and add all files in the directory as arguments
                    match file_scan_backend {
                        FileScanBackend::S3 => args.remove(5),
                        FileScanBackend::Gcs => args.remove(3),
                        FileScanBackend::Azblob => args.remove(5),
                    };
                    for file in files {
                        args.push(ExprImpl::Literal(Box::new(Literal::new(
                            Some(ScalarImpl::Utf8(file.into())),
                            DataType::Varchar,
                        ))));
                    }
                }

                schema
            }
        };

        Ok(TableFunction {
            args,
            return_type,
            function_type: TableFunctionType::FileScan,
            user_defined: None,
        })
    }

    fn handle_postgres_or_mysql_query_args(
        catalog_reader: &CatalogReadGuard,
        db_name: &str,
        schema_path: SchemaPath<'_>,
        args: Vec<ExprImpl>,
        expect_connector_name: &str,
    ) -> RwResult<Vec<ExprImpl>> {
        let is_mssql = expect_connector_name.eq_ignore_ascii_case("sqlserver-cdc");
        let cast_args = match args.len() {
            INLINE_ARG_LEN if !is_mssql => {
                let mut cast_args = Vec::with_capacity(INLINE_ARG_LEN);
                for arg in args {
                    let arg = arg.cast_implicit(&DataType::Varchar)?;
                    cast_args.push(arg);
                }
                cast_args
            }
            MSSQL_INLINE_ARG_LEN if is_mssql => {
                let mut cast_args = Vec::with_capacity(MSSQL_INLINE_ARG_LEN);
                for arg in args {
                    let arg = arg.cast_implicit(&DataType::Varchar)?;
                    cast_args.push(arg);
                }
                cast_args
            }
            CDC_SOURCE_ARG_LEN => {
                let source_name = expr_impl_to_string_fn(&args[0])?;
                let source_catalog = catalog_reader
                    .get_source_by_name(db_name, schema_path, &source_name)?
                    .0;
                if !source_catalog
                    .connector_name()
                    .eq_ignore_ascii_case(expect_connector_name)
                {
                    return Err(BindError(format!("TVF function only accepts `mysql-cdc`, `postgres-cdc` and `sqlserver-cdc` source. Expected: {}, but got: {}", expect_connector_name, source_catalog.connector_name())).into());
                }

                let (props, secret_refs) = source_catalog.with_properties.clone().into_parts();
                let secret_resolved =
                    LocalSecretManager::global().fill_secrets(props, secret_refs)?;

                let mut args_vec = vec![
                    ExprImpl::literal_varchar(secret_resolved["hostname"].clone()),
                    ExprImpl::literal_varchar(secret_resolved["port"].clone()),
                    ExprImpl::literal_varchar(secret_resolved["username"].clone()),
                    ExprImpl::literal_varchar(secret_resolved["password"].clone()),
                    ExprImpl::literal_varchar(secret_resolved["database.name"].clone()),
                    args.get(1)
                        .unwrap()
                        .clone()
                        .cast_implicit(&DataType::Varchar)?,
                ];

                if expect_connector_name.eq_ignore_ascii_case("postgres-cdc") {
                    args_vec.push(ExprImpl::literal_varchar(
                        secret_resolved.get("ssl.mode").cloned().unwrap_or_default(),
                    ));
                    args_vec.push(ExprImpl::literal_varchar(
                        secret_resolved
                            .get("ssl.root.cert")
                            .cloned()
                            .unwrap_or_default(),
                    ));
                } else if expect_connector_name.eq_ignore_ascii_case("sqlserver-cdc") {
                    // The CDC source is the source of truth for whether the
                    // SQL Server connection requires SSL. The executor treats
                    // anything other than the literal string `"true"` as
                    // "no encryption" (matching the CDC source-side default
                    // handling in `connector_common::sql_server::create_mssql_client`).
                    // The "true" / "false" string is passed through to the
                    // executor; if the source defined `database.encrypt =
                    // "true"`, propagate that — otherwise default to "false".
                    let encrypt = secret_resolved
                        .get("database.encrypt")
                        .map(|v| v.eq_ignore_ascii_case("true").to_string())
                        .unwrap_or_else(|| "false".to_owned());
                    args_vec.push(ExprImpl::literal_varchar(encrypt));
                    // The CDC implementation sets `trust_cert` unconditionally
                    // (see `connector::sink::sqlserver::SqlServerClient::new`),
                    // so the source-reference form mirrors that.
                    args_vec.push(ExprImpl::literal_varchar("true".to_owned()));
                }

                args_vec
            }
            _ => {
                return Err(BindError(
                    "postgres_query / mysql_query / mssql_query accept either \
                    2 arguments: (cdc_source_name varchar, query varchar) or \
                    6 arguments (postgres_query / mysql_query only): \
                    (hostname varchar, port varchar, username varchar, password varchar, database_name varchar, query varchar); \
                    mssql_query requires 8 arguments in the inline form, with `encrypt` and `trust_cert` after the query"
                        .to_owned(),
                )
                .into());
            }
        };

        Ok(cast_args)
    }

    pub fn new_postgres_query(args: Vec<ExprImpl>) -> RwResult<Self> {
        let evaled_args = args
            .iter()
            .map(expr_impl_to_string_fn)
            .collect::<RwResult<Vec<_>>>()?;

        #[cfg(madsim)]
        {
            return Err(crate::error::ErrorCode::BindError(
                "postgres_query can't be used in the madsim mode".to_string(),
            )
            .into());
        }

        #[cfg(not(madsim))]
        {
            let schema = tokio::task::block_in_place(|| {
                FRONTEND_RUNTIME.block_on(async {
                    let ssl_mode = evaled_args
                        .get(6)
                        .and_then(|s| s.parse().ok())
                        .unwrap_or_default();

                    let ssl_root_cert = evaled_args
                        .get(7)
                        .and_then(|s| if s.is_empty() { None } else { Some(s.clone()) });

                    let port = evaled_args[1]
                        .parse::<u16>()
                        .with_context(|| format!("invalid postgres port `{}`", evaled_args[1]))?;
                    let pg_conn = PgConnectionConfig {
                        host: evaled_args[0].clone(),
                        port,
                        user: evaled_args[2].clone(),
                        password: evaled_args[3].clone(),
                        database: evaled_args[4].clone(),
                        ssl_mode,
                        ssl_root_cert,
                    };
                    let client = create_pg_client(&pg_conn, None).await?;

                    let statement = client.prepare(evaled_args[5].as_str()).await?;

                    let mut rw_types = vec![];
                    for column in statement.columns() {
                        let name = column.name().to_owned();
                        let data_type = match *column.type_() {
                            TokioPgType::BOOL => DataType::Boolean,
                            TokioPgType::INT2 => DataType::Int16,
                            TokioPgType::INT4 => DataType::Int32,
                            TokioPgType::INT8 => DataType::Int64,
                            TokioPgType::FLOAT4 => DataType::Float32,
                            TokioPgType::FLOAT8 => DataType::Float64,
                            TokioPgType::NUMERIC => DataType::Decimal,
                            TokioPgType::DATE => DataType::Date,
                            TokioPgType::TIME => DataType::Time,
                            TokioPgType::TIMESTAMP => DataType::Timestamp,
                            TokioPgType::TIMESTAMPTZ => DataType::Timestamptz,
                            TokioPgType::TEXT | TokioPgType::VARCHAR => DataType::Varchar,
                            TokioPgType::INTERVAL => DataType::Interval,
                            TokioPgType::JSONB => DataType::Jsonb,
                            TokioPgType::BYTEA => DataType::Bytea,
                            _ => {
                                return Err(crate::error::ErrorCode::BindError(format!(
                                    "unsupported column type: {}",
                                    column.type_()
                                ))
                                .into());
                            }
                        };
                        rw_types.push((name, data_type));
                    }
                    Ok::<risingwave_common::types::DataType, anyhow::Error>(DataType::Struct(
                        StructType::new(rw_types),
                    ))
                })
            })?;

            Ok(TableFunction {
                args,
                return_type: schema,
                function_type: TableFunctionType::PostgresQuery,
                user_defined: None,
            })
        }
    }

    pub fn new_mysql_query(args: Vec<ExprImpl>) -> RwResult<Self> {
        let evaled_args = args
            .iter()
            .map(expr_impl_to_string_fn)
            .collect::<RwResult<Vec<_>>>()?;

        #[cfg(madsim)]
        {
            return Err(crate::error::ErrorCode::BindError(
                "postgres_query can't be used in the madsim mode".to_string(),
            )
            .into());
        }

        #[cfg(not(madsim))]
        {
            let schema = tokio::task::block_in_place(|| {
                FRONTEND_RUNTIME.block_on(async {
                    let database_opts: mysql_async::Opts = {
                        let port = evaled_args[1]
                            .parse::<u16>()
                            .context("failed to parse port")?;
                        mysql_async::OptsBuilder::default()
                            .ip_or_hostname(evaled_args[0].clone())
                            .tcp_port(port)
                            .user(Some(evaled_args[2].clone()))
                            .pass(Some(evaled_args[3].clone()))
                            .db_name(Some(evaled_args[4].clone()))
                            .into()
                    };

                    let pool = mysql_async::Pool::new(database_opts);
                    let mut conn = pool
                        .get_conn()
                        .await
                        .context("failed to connect to mysql in binder")?;

                    let query = evaled_args[5].clone();
                    let statement = conn
                        .prep(query)
                        .await
                        .context("failed to prepare mysql_query in binder")?;

                    let mut rw_types = vec![];

                    for column in statement.columns() {
                        let name = column.name_str().to_string();
                        let data_type = match column.column_type() {
                            // Boolean types
                            MySqlColumnType::MYSQL_TYPE_BIT if column.column_length() == 1 => {
                                DataType::Boolean
                            }

                            // Numeric types
                            // NOTE(kwannoel): Although `bool/boolean` is a synonym of TINY(1) in MySQL,
                            // we treat it as Int16 here. It is better to be straightforward in our conversion.
                            MySqlColumnType::MYSQL_TYPE_TINY => DataType::Int16,
                            MySqlColumnType::MYSQL_TYPE_SHORT => DataType::Int16,
                            MySqlColumnType::MYSQL_TYPE_INT24 => DataType::Int32,
                            MySqlColumnType::MYSQL_TYPE_LONG => DataType::Int32,
                            MySqlColumnType::MYSQL_TYPE_LONGLONG => DataType::Int64,
                            MySqlColumnType::MYSQL_TYPE_FLOAT => DataType::Float32,
                            MySqlColumnType::MYSQL_TYPE_DOUBLE => DataType::Float64,
                            MySqlColumnType::MYSQL_TYPE_NEWDECIMAL => DataType::Decimal,
                            MySqlColumnType::MYSQL_TYPE_DECIMAL => DataType::Decimal,

                            // Date time types
                            MySqlColumnType::MYSQL_TYPE_YEAR => DataType::Int32,
                            MySqlColumnType::MYSQL_TYPE_DATE => DataType::Date,
                            MySqlColumnType::MYSQL_TYPE_NEWDATE => DataType::Date,
                            MySqlColumnType::MYSQL_TYPE_TIME => DataType::Time,
                            MySqlColumnType::MYSQL_TYPE_TIME2 => DataType::Time,
                            MySqlColumnType::MYSQL_TYPE_DATETIME => DataType::Timestamp,
                            MySqlColumnType::MYSQL_TYPE_DATETIME2 => DataType::Timestamp,
                            MySqlColumnType::MYSQL_TYPE_TIMESTAMP => DataType::Timestamptz,
                            MySqlColumnType::MYSQL_TYPE_TIMESTAMP2 => DataType::Timestamptz,

                            // String types
                            MySqlColumnType::MYSQL_TYPE_VARCHAR => DataType::Varchar,
                            // mysql_async does not have explicit `varbinary` and `binary` types,
                            // we need to check the `ColumnFlags` to distinguish them.
                            MySqlColumnType::MYSQL_TYPE_STRING
                            | MySqlColumnType::MYSQL_TYPE_VAR_STRING => {
                                if column
                                    .flags()
                                    .contains(mysql_common::constants::ColumnFlags::BINARY_FLAG)
                                {
                                    DataType::Bytea
                                } else {
                                    DataType::Varchar
                                }
                            }

                            // JSON types
                            MySqlColumnType::MYSQL_TYPE_JSON => DataType::Jsonb,

                            // Binary types
                            MySqlColumnType::MYSQL_TYPE_BIT
                            | MySqlColumnType::MYSQL_TYPE_BLOB
                            | MySqlColumnType::MYSQL_TYPE_TINY_BLOB
                            | MySqlColumnType::MYSQL_TYPE_MEDIUM_BLOB
                            | MySqlColumnType::MYSQL_TYPE_LONG_BLOB => DataType::Bytea,

                            MySqlColumnType::MYSQL_TYPE_UNKNOWN
                            | MySqlColumnType::MYSQL_TYPE_TYPED_ARRAY
                            | MySqlColumnType::MYSQL_TYPE_ENUM
                            | MySqlColumnType::MYSQL_TYPE_SET
                            | MySqlColumnType::MYSQL_TYPE_GEOMETRY
                            | MySqlColumnType::MYSQL_TYPE_VECTOR
                            | MySqlColumnType::MYSQL_TYPE_NULL => {
                                return Err(crate::error::ErrorCode::BindError(format!(
                                    "unsupported column type: {:?}",
                                    column.column_type()
                                ))
                                .into());
                            }
                        };
                        rw_types.push((name, data_type));
                    }
                    Ok::<risingwave_common::types::DataType, anyhow::Error>(DataType::Struct(
                        StructType::new(rw_types),
                    ))
                })
            })?;

            Ok(TableFunction {
                args,
                return_type: schema,
                function_type: TableFunctionType::MysqlQuery,
                user_defined: None,
            })
        }
    }

    /// Bind a `mssql_query(...)` table function call. Supports both calling
    /// forms:
    ///
    /// * **2-arg source-reference form**: `mssql_query(<cdc_source_name>, <query>)`.
    ///   The connection parameters (host, port, user, password, database)
    ///   are looked up from the named `connector = 'sqlserver-cdc'` source.
    ///   The `encrypt` argument is taken from the source's
    ///   `database.encrypt` property (with `true` / non-`true` mapping to
    ///   `"true"` / `"false"`); `trust_cert` is forced to `"true"` because
    ///   the CDC implementation sets it unconditionally.
    /// * **8-arg inline form**: `mssql_query(<host>, <port>, <user>, <password>,
    ///   <database>, <query>, <encrypt>, <trust_cert>)`. All eight arguments
    ///   are required; the 6-arg form is *not* supported.
    ///
    /// Returns a `TableFunction` whose `return_type` is a `DataType::Struct`
    /// discovered at bind time by calling `describe_mssql_query` against the
    /// user-provided query. The `encrypt` and `trust_cert` flags are parsed
    /// strictly (any value other than case-insensitive `true` / `false` is
    /// a bind error).
    pub fn new_mssql_query(
        catalog_reader: &CatalogReadGuard,
        db_name: &str,
        schema_path: SchemaPath<'_>,
        args: Vec<ExprImpl>,
    ) -> RwResult<Self> {
        let args = Self::handle_postgres_or_mysql_query_args(
            catalog_reader,
            db_name,
            schema_path,
            args,
            "sqlserver-cdc",
        )?;
        let evaled_args = args
            .iter()
            .map(expr_impl_to_string_fn)
            .collect::<RwResult<Vec<_>>>()?;

        #[cfg(madsim)]
        {
            return Err(crate::error::ErrorCode::BindError(
                "mssql_query can't be used in the madsim mode".to_string(),
            )
            .into());
        }

        #[cfg(not(madsim))]
        {
            let schema = tokio::task::block_in_place(|| {
                FRONTEND_RUNTIME.block_on(async {
                    let port: u16 = evaled_args[1]
                        .parse()
                        .with_context(|| format!("invalid sql server port `{}`", evaled_args[1]))?;
                    // Inline (8-arg) form carries encrypt/trust_cert; CDC
                    // source (2-arg) form omits them and we fall back to
                    // conservative defaults (off encryption, trust cert).
                    // Both options must parse cleanly to a boolean; if the
                    // user supplies an invalid value (e.g. "yes" or "1"),
                    // reject the query rather than silently coercing it —
                    // otherwise a typo could open a plaintext connection or
                    // bypass certificate validation.
                    let encrypt = parse_strict_bool_arg(evaled_args.get(6), false).context(
                        "invalid `encrypt` value for mssql_query: expected 'true' or 'false'",
                    )?;
                    let trust_cert = parse_strict_bool_arg(evaled_args.get(7), true).context(
                        "invalid `trust_cert` value for mssql_query: expected 'true' or 'false'",
                    )?;

                    let conn_config = MssqlConnectionConfig {
                        host: evaled_args[0].clone(),
                        port,
                        user: evaled_args[2].clone(),
                        password: evaled_args[3].clone(),
                        database: evaled_args[4].clone(),
                        encrypt,
                        trust_cert,
                    };

                    let rw_types = describe_mssql_query(&conn_config, &evaled_args[5]).await?;

                    Ok::<risingwave_common::types::DataType, anyhow::Error>(DataType::Struct(
                        StructType::new(rw_types),
                    ))
                })
            })?;

            Ok(TableFunction {
                args,
                return_type: schema,
                function_type: TableFunctionType::MssqlQuery,
                user_defined: None,
            })
        }
    }

    /// This is a highly specific _internal_ table function meant to scan and aggregate
    /// `backfill_table_id`, `row_count` for all MVs which are still being created.
    pub fn new_internal_backfill_progress() -> Self {
        TableFunction {
            args: vec![],
            return_type: DataType::Struct(StructType::new(vec![
                ("job_id".to_owned(), DataType::Int32),
                ("fragment_id".to_owned(), DataType::Int32),
                ("backfill_state_table_id".to_owned(), DataType::Int32),
                ("current_row_count".to_owned(), DataType::Int64),
                ("min_epoch".to_owned(), DataType::Int64),
            ])),
            function_type: TableFunctionType::InternalBackfillProgress,
            user_defined: None,
        }
    }

    pub fn new_internal_source_backfill_progress() -> Self {
        TableFunction {
            args: vec![],
            return_type: DataType::Struct(StructType::new(vec![
                ("job_id".to_owned(), DataType::Int32),
                ("fragment_id".to_owned(), DataType::Int32),
                ("backfill_state_table_id".to_owned(), DataType::Int32),
                ("partition_id".to_owned(), DataType::Varchar),
                ("backfill_progress".to_owned(), DataType::Jsonb),
            ])),
            function_type: TableFunctionType::InternalSourceBackfillProgress,
            user_defined: None,
        }
    }

    pub fn new_internal_get_channel_delta_stats(args: Vec<ExprImpl>) -> Self {
        Self {
            args,
            return_type: DataType::Struct(StructType::new(vec![
                ("upstream_fragment_id".to_owned(), DataType::Int32),
                ("downstream_fragment_id".to_owned(), DataType::Int32),
                ("backpressure_rate".to_owned(), DataType::Float64),
                ("recv_throughput".to_owned(), DataType::Float64),
                ("send_throughput".to_owned(), DataType::Float64),
            ])),
            function_type: TableFunctionType::InternalGetChannelDeltaStats,
            user_defined: None,
        }
    }

    pub fn to_protobuf(&self) -> PbTableFunction {
        PbTableFunction {
            function_type: self.function_type as i32,
            args: self.args.iter().map(|c| c.to_expr_proto()).collect_vec(),
            return_type: Some(self.return_type.to_protobuf()),
            udf: self.user_defined.as_ref().map(|c| c.as_ref().into()),
        }
    }

    /// Serialize the table function. Returns an error if this will result in an impure table
    /// function on a retract stream, which may lead to inconsistent results.
    pub fn to_protobuf_checked_pure(&self, retract: bool) -> crate::error::Result<PbTableFunction> {
        if retract {
            reject_impure(self.clone(), "table function")?;
        }

        let args = self
            .args
            .iter()
            .map(|arg| arg.to_expr_proto_checked_pure(retract, "table function argument"))
            .collect::<crate::error::Result<Vec<_>>>()?;

        Ok(PbTableFunction {
            function_type: self.function_type as i32,
            args,
            return_type: Some(self.return_type.to_protobuf()),
            udf: self.user_defined.as_ref().map(|c| c.as_ref().into()),
        })
    }

    /// Get the name of the table function.
    pub fn name(&self) -> String {
        match self.function_type {
            TableFunctionType::UserDefined => self.user_defined.as_ref().unwrap().name.clone(),
            t => t.as_str_name().to_lowercase(),
        }
    }

    pub fn rewrite(self, rewriter: &mut impl ExprRewriter) -> Self {
        Self {
            args: self
                .args
                .into_iter()
                .map(|e| rewriter.rewrite_expr(e))
                .collect(),
            ..self
        }
    }
}

impl std::fmt::Debug for TableFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if f.alternate() {
            f.debug_struct("FunctionCall")
                .field("function_type", &self.function_type)
                .field("return_type", &self.return_type)
                .field("args", &self.args)
                .finish()
        } else {
            let func_name = format!("{:?}", self.function_type);
            let mut builder = f.debug_tuple(&func_name);
            self.args.iter().for_each(|child| {
                builder.field(child);
            });
            builder.finish()
        }
    }
}

impl Expr for TableFunction {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    fn try_to_expr_proto(&self) -> Result<risingwave_pb::expr::ExprNode, String> {
        Err("Table function should not be converted to ExprNode".to_owned())
    }
}

pub(crate) fn expr_impl_to_string_fn(arg: &ExprImpl) -> RwResult<String> {
    match arg.try_fold_const() {
        Some(Ok(value)) => {
            let Some(scalar) = value else {
                return Err(BindError(
                    "postgres_query / mysql_query / mssql_query do not accept null arguments"
                        .to_owned(),
                )
                .into());
            };
            Ok(scalar.into_utf8().to_string())
        }
        Some(Err(err)) => Err(err),
        None => Err(BindError(
            "postgres_query / mysql_query / mssql_query only accept constant arguments".to_owned(),
        )
        .into()),
    }
}
