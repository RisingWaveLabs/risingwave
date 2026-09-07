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

use anyhow::Context;
use futures_async_stream::try_stream;
use futures_util::stream::StreamExt;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum, Decimal, ScalarImpl};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_connector::connector_common::{PgConnectionConfig, create_pg_client};
use risingwave_pb::batch_plan::plan_node::NodeBody;
use tokio_postgres;

use crate::error::BatchError;
use crate::executor::{BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder};

/// `PostgresQuery` executor. Runs a query against a Postgres database.
pub struct PostgresQueryExecutor {
    schema: Schema,
    config: PgConnectionConfig,
    query: String,
    identity: String,
    chunk_size: usize,
}

impl Executor for PostgresQueryExecutor {
    fn schema(&self) -> &risingwave_common::catalog::Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        &self.identity
    }

    fn execute(self: Box<Self>) -> super::BoxedDataChunkStream {
        self.do_execute().boxed()
    }
}

pub fn postgres_row_to_owned_row(
    row: tokio_postgres::Row,
    schema: &Schema,
) -> Result<OwnedRow, BatchError> {
    let mut datums = vec![];
    for i in 0..schema.fields.len() {
        let rw_field = &schema.fields[i];
        let name = rw_field.name.as_str();
        let datum = postgres_cell_to_scalar_impl(&row, &rw_field.data_type, i, name)?;
        datums.push(datum);
    }
    Ok(OwnedRow::new(datums))
}

// TODO(kwannoel): Support more types, see postgres connector's ScalarAdapter.
fn postgres_cell_to_scalar_impl(
    row: &tokio_postgres::Row,
    data_type: &DataType,
    i: usize,
    name: &str,
) -> Result<Datum, BatchError> {
    let datum = match data_type {
        DataType::Boolean
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Float32
        | DataType::Float64
        | DataType::Date
        | DataType::Time
        | DataType::Timestamp
        | DataType::Timestamptz
        | DataType::Jsonb
        | DataType::Interval
        | DataType::Varchar
        | DataType::Bytea => {
            // ScalarAdapter is also fine. But ScalarImpl is more efficient
            row.try_get::<_, Option<ScalarImpl>>(i)?
        }
        DataType::Decimal => {
            // Decimal is more efficient than PgNumeric in ScalarAdapter
            let val = row.try_get::<_, Option<Decimal>>(i)?;
            val.map(ScalarImpl::from)
        }
        _ => {
            tracing::warn!(name, ?data_type, "unsupported data type, set to null");
            None
        }
    };
    Ok(datum)
}

impl PostgresQueryExecutor {
    pub fn new(
        schema: Schema,
        config: PgConnectionConfig,
        query: String,
        identity: String,
        chunk_size: usize,
    ) -> Self {
        Self {
            schema,
            config,
            query,
            identity,
            chunk_size,
        }
    }

    #[try_stream(ok = DataChunk, error = BatchError)]
    async fn do_execute(self: Box<Self>) {
        tracing::debug!("postgres_query_executor: started");

        let client = create_pg_client(&self.config, None).await?;

        let params: &[&str] = &[];
        let row_stream = client
            .query_raw(&self.query, params)
            .await
            .context("postgres_query received error from remote server")?;
        let mut builder = DataChunkBuilder::new(self.schema.data_types(), self.chunk_size);
        tracing::debug!("postgres_query_executor: query executed, start deserializing rows");
        // deserialize the rows
        #[for_await]
        for row in row_stream {
            let row = row?;
            let owned_row = postgres_row_to_owned_row(row, &self.schema)?;
            if let Some(chunk) = builder.append_one_row(owned_row) {
                yield chunk;
            }
        }
        if let Some(chunk) = builder.consume_all() {
            yield chunk;
        }
        return Ok(());
    }
}

pub struct PostgresQueryExecutorBuilder {}

impl BoxedExecutorBuilder for PostgresQueryExecutorBuilder {
    async fn new_boxed_executor(
        source: &ExecutorBuilder<'_>,
        _inputs: Vec<BoxedExecutor>,
    ) -> crate::error::Result<BoxedExecutor> {
        let postgres_query_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::PostgresQuery
        )?;

        let port = postgres_query_node
            .port
            .parse::<u16>()
            .with_context(|| format!("invalid postgres port `{}`", postgres_query_node.port))?;
        Ok(Box::new(PostgresQueryExecutor::new(
            Schema::from_iter(postgres_query_node.columns.iter().map(Field::from)),
            PgConnectionConfig {
                host: postgres_query_node.hostname.clone(),
                port,
                user: postgres_query_node.username.clone(),
                password: postgres_query_node.password.clone(),
                database: postgres_query_node.database.clone(),
                ssl_mode: postgres_query_node.ssl_mode.parse().unwrap_or_default(),
                ssl_root_cert: if postgres_query_node.ssl_root_cert.is_empty() {
                    None
                } else {
                    Some(postgres_query_node.ssl_root_cert.clone())
                },
            },
            postgres_query_node.query.clone(),
            source.plan_node().get_identity().clone(),
            source.context().get_config().developer.chunk_size,
        )))
    }
}
