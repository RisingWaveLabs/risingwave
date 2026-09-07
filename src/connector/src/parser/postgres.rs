// Copyright 2023 RisingWave Labs
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

use std::sync::LazyLock;

use anyhow::{Context, anyhow, bail};
use bytes::Buf;
use risingwave_common::array::Finite32;
use risingwave_common::catalog::Schema;
use risingwave_common::log::LogSuppressor;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum, Decimal, ScalarImpl, VectorVal};
use thiserror_ext::AsReport;
use tokio_postgres::types::{FromSql, Type};

use crate::parser::scalar_adapter::ScalarAdapter;
use crate::parser::utils::log_error;

static LOG_SUPPRESSOR: LazyLock<LogSuppressor> = LazyLock::new(LogSuppressor::default);

/// Adapter for PostgreSQL `vector` type in CDC snapshot reads.
/// It parses pgvector binary format.
struct PgVectorAdapter(Vec<f32>);

impl<'a> FromSql<'a> for PgVectorAdapter {
    fn accepts(ty: &Type) -> bool {
        ty.name() == "vector"
    }

    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Self::parse_binary(raw)
    }
}

impl PgVectorAdapter {
    fn parse_binary(raw: &[u8]) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        // Binary format from pgvector extension:
        // int16 dimension, int16 unused, repeated float4 values.
        if raw.len() < 4 {
            return Err("invalid vector binary payload".into());
        }
        let mut buf = raw;
        let dim = buf.get_u16() as usize;
        let _unused = buf.get_u16();
        if buf.remaining() != dim * std::mem::size_of::<f32>() {
            return Err("invalid vector binary payload length".into());
        }
        let mut elems = Vec::with_capacity(dim);
        for _ in 0..dim {
            elems.push(buf.get_f32());
        }
        Ok(Self(elems))
    }
}

macro_rules! try_handle_data_type {
    ($row:expr, $i:expr, $name:expr, $type:ty) => {{
        $row.try_get::<_, Option<$type>>($i)
            .map(|value| value.map(ScalarImpl::from))
            .with_context(|| {
                format!(
                    "failed to decode PostgreSQL snapshot column `{}` as {}",
                    $name,
                    stringify!($type)
                )
            })
    }};
}

pub fn postgres_row_to_owned_row(row: tokio_postgres::Row, schema: &Schema) -> OwnedRow {
    let mut datums = vec![];
    for i in 0..schema.fields.len() {
        let rw_field = &schema.fields[i];
        let name = rw_field.name.as_str();
        let datum = postgres_cell_to_scalar_impl(&row, &rw_field.data_type, i, name);
        datums.push(datum);
    }
    OwnedRow::new(datums)
}

/// Decode primary-key columns strictly while preserving the legacy lenient behavior for all
/// other columns in a PostgreSQL CDC snapshot row.
pub fn postgres_row_to_owned_row_with_strict_pk(
    row: tokio_postgres::Row,
    schema: &Schema,
    pk_indices: &[usize],
) -> anyhow::Result<OwnedRow> {
    super::decode_row_with_strict_pk(
        "PostgreSQL",
        schema,
        pk_indices,
        |index, field| {
            postgres_cell_to_scalar_impl_strict(&row, &field.data_type, index, &field.name)
        },
        |name, err| log_error!(name, err, "parse column failed"),
    )
}

pub fn postgres_cell_to_scalar_impl(
    row: &tokio_postgres::Row,
    data_type: &DataType,
    i: usize,
    name: &str,
) -> Option<ScalarImpl> {
    match postgres_cell_to_scalar_impl_strict(row, data_type, i, name) {
        Ok(datum) => datum,
        Err(err) => {
            log_error!(name, err, "parse column failed");
            None
        }
    }
}

pub fn postgres_cell_to_scalar_impl_strict(
    row: &tokio_postgres::Row,
    data_type: &DataType,
    i: usize,
    name: &str,
) -> anyhow::Result<Datum> {
    // We observe several incompatibility issue in Debezium's Postgres connector. We summarize them here:
    // Issue #1. The null of enum list is not supported in Debezium. An enum list contains `NULL` will fallback to `NULL`.
    // Issue #2. In our parser, when there's inf, -inf, nan or invalid item in a list, the whole list will fallback null.
    match data_type {
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
        | DataType::Bytea => {
            // ScalarAdapter is also fine. But ScalarImpl is more efficient
            row.try_get::<_, Option<ScalarImpl>>(i)
                .with_context(|| format!("failed to decode PostgreSQL snapshot column `{name}`"))
        }
        DataType::Decimal => {
            // Decimal is more efficient than PgNumeric in ScalarAdapter
            try_handle_data_type!(row, i, name, Decimal)
        }
        DataType::Varchar | DataType::Int256 | DataType::Struct(_) => {
            match row
                .try_get::<_, Option<ScalarAdapter>>(i)
                .with_context(|| format!("failed to decode PostgreSQL snapshot column `{name}`"))?
            {
                Some(value) => value.into_scalar(data_type).map(Some).ok_or_else(|| {
                    anyhow!("failed to convert PostgreSQL snapshot column `{name}` to {data_type}")
                }),
                None => Ok(None),
            }
        }
        DataType::Vector(expected_size) => {
            match row
                .try_get::<_, Option<PgVectorAdapter>>(i)
                .with_context(|| format!("failed to decode PostgreSQL snapshot column `{name}`"))?
            {
                Some(PgVectorAdapter(v)) => {
                    if v.len() != *expected_size {
                        bail!(
                            "PostgreSQL snapshot column `{name}` vector dimension mismatch: \
                             expected {}, got {}",
                            expected_size,
                            v.len()
                        );
                    }
                    let finite = v
                        .into_iter()
                        .map(Finite32::try_from)
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(anyhow::Error::msg)
                        .with_context(|| {
                            format!(
                                "PostgreSQL snapshot column `{name}` contains a non-finite vector \
                                 element"
                            )
                        })?;
                    Ok(Some(ScalarImpl::Vector(VectorVal::from(finite))))
                }
                None => Ok(None),
            }
        }
        DataType::List(list) => match list.elem() {
            // TODO(Kexiang): allow DataType::List(_)
            elem @ (DataType::Struct(_) | DataType::List(_) | DataType::Serial) => {
                bail!("unsupported PostgreSQL snapshot list element type {elem}")
            }
            _ => {
                match row
                    .try_get::<_, Option<ScalarAdapter>>(i)
                    .with_context(|| {
                        format!("failed to decode PostgreSQL snapshot list column `{name}`")
                    })? {
                    Some(value) => value.into_scalar(data_type).map(Some).ok_or_else(|| {
                        anyhow!(
                            "failed to convert PostgreSQL snapshot column `{name}` to {data_type}"
                        )
                    }),
                    None => Ok(None),
                }
            }
        },
        DataType::Serial | DataType::Map(_) | DataType::Variant => {
            bail!("unsupported PostgreSQL snapshot data type {data_type} for column `{name}`")
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio_postgres::NoTls;

    use crate::parser::postgres::PgVectorAdapter;
    use crate::parser::scalar_adapter::EnumString;
    const DB: &str = "postgres";
    const USER: &str = "kexiang";

    #[test]
    fn test_pg_vector_adapter_parse_binary() {
        let mut raw = vec![];
        // dim = 3
        raw.extend_from_slice(&(3u16.to_be_bytes()));
        // unused
        raw.extend_from_slice(&(0u16.to_be_bytes()));
        raw.extend_from_slice(&1.5f32.to_be_bytes());
        raw.extend_from_slice(&(-2.25f32).to_be_bytes());
        raw.extend_from_slice(&3.0f32.to_be_bytes());

        let v = PgVectorAdapter::parse_binary(&raw).unwrap();
        assert_eq!(v.0, vec![1.5, -2.25, 3.0]);
    }

    #[ignore]
    #[tokio::test]
    async fn enum_string_integration_test() {
        let connect = format!(
            "host=localhost port=5432 user={} password={} dbname={}",
            USER, DB, DB
        );
        let (client, connection) = tokio_postgres::connect(connect.as_str(), NoTls)
            .await
            .unwrap();

        // The connection object performs the actual communication with the database,
        // so spawn it off to run on its own.
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        // allow type existed
        let _ = client
            .execute("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')", &[])
            .await;
        client
            .execute(
                "CREATE TABLE IF NOT EXISTS person(id int PRIMARY KEY, current_mood mood)",
                &[],
            )
            .await
            .unwrap();
        client.execute("DELETE FROM person;", &[]).await.unwrap();
        client
            .execute("INSERT INTO person VALUES (1, 'happy')", &[])
            .await
            .unwrap();

        // test from_sql
        let got: EnumString = client
            .query_one("SELECT * FROM person", &[])
            .await
            .unwrap()
            .get::<usize, Option<EnumString>>(1)
            .unwrap();
        assert_eq!("happy", got.0.as_str());

        client.execute("DELETE FROM person", &[]).await.unwrap();

        // test to_sql
        client
            .execute("INSERT INTO person VALUES (2, $1)", &[&got])
            .await
            .unwrap();

        let got_new: EnumString = client
            .query_one("SELECT * FROM person", &[])
            .await
            .unwrap()
            .get::<usize, Option<EnumString>>(1)
            .unwrap();
        assert_eq!("happy", got_new.0.as_str());
        client.execute("DROP TABLE person", &[]).await.unwrap();
        client.execute("DROP TYPE mood", &[]).await.unwrap();
    }
}
