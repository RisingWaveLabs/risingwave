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

use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Mutex;

use anyhow::{Context, anyhow};
use futures::StreamExt;
use futures::stream::{self, BoxStream};
use futures_async_stream::try_stream;
use prost::Message;
use risingwave_common::bail;
use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema};
use risingwave_common::global_jvm::Jvm;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::{DataType, ScalarImpl, ToText};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_jni_core::call_static_method;
use risingwave_jni_core::jvm_runtime::execute_with_jni_env;
use risingwave_pb::connector_service::{
    OracleDatum, OracleExternalTableRequest, OracleExternalTableResponse, OracleRow, TableSchema,
};
use serde::{Deserialize, Serialize};
use thiserror_ext::AsReport;

use crate::error::{ConnectorError, ConnectorResult};
use crate::source::CdcTableSnapshotSplit;
use crate::source::cdc::external::{
    CdcOffset, CdcTableSnapshotSplitOption, ExternalTableConfig, ExternalTableReader,
    SchemaTableName,
};

#[derive(Debug, Clone, Default, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct OracleOffset {
    pub scn: u64,
}

pub struct OracleExternalTable {
    column_descs: Vec<ColumnDesc>,
    pk_names: Vec<String>,
}

impl OracleExternalTable {
    pub async fn connect(config: ExternalTableConfig) -> ConnectorResult<Self> {
        let request = OracleExternalTableRequest {
            properties: config.oracle_connection_properties(),
            ..Default::default()
        };
        let response = tokio::task::spawn_blocking(move || discover_table(request))
            .await
            .context("failed to join Oracle schema discovery task")??;
        ensure_success(&response)?;

        let table_schema = response
            .table_schema
            .context("Oracle schema discovery returned no table schema")?;
        let column_descs = table_schema
            .columns
            .iter()
            .map(ColumnDesc::from)
            .collect::<Vec<_>>();
        let pk_names = table_schema
            .pk_indices
            .iter()
            .map(|index| {
                table_schema
                    .columns
                    .get(*index as usize)
                    .map(|column| column.name.clone())
                    .with_context(|| {
                        format!("Oracle primary-key index {index} is unable to extract column name")
                    })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;

        if column_descs.is_empty() {
            bail!("Oracle schema discovery returned no columns");
        }
        if pk_names.is_empty() {
            bail!("Oracle schema discovery returned no valid primary keys");
        }

        Ok(Self {
            column_descs,
            pk_names,
        })
    }

    pub fn column_descs(&self) -> &Vec<ColumnDesc> {
        &self.column_descs
    }

    pub fn pk_names(&self) -> &Vec<String> {
        &self.pk_names
    }
}

#[derive(Debug)]
pub struct OracleExternalTableReader {
    config: ExternalTableConfig,
    rw_schema: Schema,
    pk_indices: Vec<usize>,
    table_schema: TableSchema,
    snapshot_scn: Mutex<Option<u64>>,
}

impl ExternalTableReader for OracleExternalTableReader {
    async fn current_cdc_offset(&self) -> ConnectorResult<CdcOffset> {
        let request = OracleExternalTableRequest {
            properties: self.config.oracle_connection_properties(),
            ..Default::default()
        };
        let response = tokio::task::spawn_blocking(move || current_scn(request))
            .await
            .context("failed to join Oracle SCN query task")??;
        ensure_success(&response)?;
        if response.snapshot_scn == 0 {
            bail!("Oracle returned an invalid current SCN");
        }
        *self.snapshot_scn.lock().unwrap() = Some(response.snapshot_scn);
        Ok(CdcOffset::Oracle(OracleOffset {
            scn: response.snapshot_scn,
        }))
    }

    fn snapshot_read(
        &self,
        table_name: SchemaTableName,
        start_pk: Option<OwnedRow>,
        primary_keys: Vec<String>,
        limit: u32,
    ) -> BoxStream<'_, ConnectorResult<OwnedRow>> {
        self.snapshot_read_inner(table_name, start_pk, primary_keys, limit)
    }

    fn get_parallel_cdc_splits(
        &self,
        _options: CdcTableSnapshotSplitOption,
    ) -> BoxStream<'_, ConnectorResult<CdcTableSnapshotSplit>> {
        // TODO(#26804): Implement Oracle snapshot split discovery for parallel CDC backfill.
        stream::empty().boxed()
    }

    fn split_snapshot_read(
        &self,
        _table_name: SchemaTableName,
        _left: OwnedRow,
        _right: OwnedRow,
        _split_columns: Vec<Field>,
    ) -> BoxStream<'_, ConnectorResult<OwnedRow>> {
        // TODO(#26804): Implement Oracle split-range snapshot reads for parallel CDC backfill.
        stream::once(async {
            Err(anyhow!("Oracle CDC parallelized backfill is not implemented").into())
        })
        .boxed()
    }
}

impl OracleExternalTableReader {
    pub fn new(
        config: ExternalTableConfig,
        rw_schema: Schema,
        pk_indices: Vec<usize>,
    ) -> ConnectorResult<Self> {
        if pk_indices.is_empty() {
            bail!("Oracle snapshot reader requires at least one primary-key column");
        }
        for &index in &pk_indices {
            if index >= rw_schema.len() {
                bail!(
                    "Oracle snapshot primary-key index {index} is outside a {}-column schema",
                    rw_schema.len()
                );
            }
        }

        let table_schema = TableSchema {
            columns: rw_schema
                .fields
                .iter()
                .map(|field| {
                    ColumnDesc::named(
                        field.name.clone(),
                        ColumnId::placeholder(),
                        field.data_type.clone(),
                    )
                    .to_protobuf()
                })
                .collect(),
            pk_indices: pk_indices.iter().map(|index| *index as u32).collect(),
        };

        Ok(Self {
            config,
            rw_schema,
            pk_indices,
            table_schema,
            snapshot_scn: Mutex::new(None),
        })
    }

    #[try_stream(boxed, ok = OwnedRow, error = ConnectorError)]
    async fn snapshot_read_inner(
        &self,
        table_name: SchemaTableName,
        start_pk: Option<OwnedRow>,
        primary_keys: Vec<String>,
        limit: u32,
    ) {
        if limit == 0 {
            Err(anyhow!("Oracle snapshot read has an invalid limit"))?;
        }
        if primary_keys.len() != self.pk_indices.len() {
            Err(anyhow!(
                "Oracle snapshot read received {} primary-key columns but there are actually {} primary-key columns",
                primary_keys.len(),
                self.pk_indices.len()
            ))?;
        }

        let snapshot_scn = self
            .snapshot_scn
            .lock()
            .unwrap()
            .context("Oracle snapshot read started before obtaining the current SCN")?;
        let start_pk = start_pk
            .map(|row| encode_start_pk(row, &self.rw_schema, &self.pk_indices))
            .transpose()?;

        let properties = self
            .config
            .oracle_connection_properties_with_schema_table_name(table_name);
        let request = OracleExternalTableRequest {
            properties,
            table_schema: Some(self.table_schema.clone()),
            snapshot_scn,
            start_pk: start_pk.unwrap_or_default(),
            primary_keys,
            limit,
        };
        let response = tokio::task::spawn_blocking(move || read_snapshot(request))
            .await
            .context("failed to join Oracle snapshot query task")??;
        ensure_success(&response)?;

        for row in response.rows {
            yield decode_row(row, &self.rw_schema, &self.pk_indices)?;
        }
    }
}

impl ExternalTableConfig {
    pub fn oracle_connection_properties(&self) -> HashMap<String, String> {
        HashMap::from([
            ("hostname".to_owned(), self.host.clone()),
            ("port".to_owned(), self.port.clone()),
            ("username".to_owned(), self.username.clone()),
            ("password".to_owned(), self.password.clone()),
            ("database.name".to_owned(), self.database.clone()),
            ("database.pdb.name".to_owned(), self.pdb_name.clone()),
            ("schema.name".to_owned(), self.schema.clone()),
            ("table.name".to_owned(), self.table.clone()),
        ])
    }

    pub fn oracle_connection_properties_with_schema_table_name(
        &self,
        table_name: SchemaTableName,
    ) -> HashMap<String, String> {
        let mut properties = self.oracle_connection_properties();
        properties.insert("schema.name".to_owned(), table_name.schema_name);
        properties.insert("table.name".to_owned(), table_name.table_name);
        properties
    }
}

fn encode_start_pk(
    row: OwnedRow,
    schema: &Schema,
    pk_indices: &[usize],
) -> ConnectorResult<Vec<OracleDatum>> {
    if row.len() != pk_indices.len() {
        bail!(
            "Oracle snapshot start key has {} values for {} primary-key columns",
            row.len(),
            pk_indices.len()
        );
    }
    row.into_iter()
        .zip_eq_fast(pk_indices)
        .enumerate()
        .map(|(position, (datum, &schema_index))| {
            let scalar = datum.with_context(|| {
                format!("Oracle snapshot primary-key position {position} cannot be NULL")
            })?;
            let value = match &schema.fields[schema_index].data_type {
                DataType::Bytea => scalar.into_bytea().into_vec(),
                data_type => scalar
                    .as_scalar_ref_impl()
                    .to_text_with_type(data_type)
                    .into_bytes(),
            };
            Ok(OracleDatum {
                is_null: false,
                value,
            })
        })
        .collect()
}

fn decode_row(row: OracleRow, schema: &Schema, pk_indices: &[usize]) -> ConnectorResult<OwnedRow> {
    if row.values.len() != schema.len() {
        bail!(
            "Oracle snapshot row has {} values for a {}-column schema",
            row.values.len(),
            schema.len()
        );
    }

    let datums = row
        .values
        .into_iter()
        .zip_eq_fast(&schema.fields)
        .enumerate()
        .map(|(index, (datum, field))| {
            if datum.is_null {
                if pk_indices.contains(&index) {
                    return Err(anyhow!(
                        "Oracle snapshot primary key `{}` cannot be NULL",
                        field.name
                    ));
                }
                return Ok(None);
            }

            let parsed = match &field.data_type {
                DataType::Bytea => Ok(ScalarImpl::Bytea(datum.value.into())),
                data_type => {
                    let text = std::str::from_utf8(&datum.value).with_context(|| {
                        format!("Oracle snapshot column `{}` is not valid UTF-8", field.name)
                    })?;
                    ScalarImpl::from_text(text, data_type).map_err(|error| anyhow!(error))
                }
            };
            match parsed {
                Ok(value) => Ok(Some(value)),
                Err(error) if pk_indices.contains(&index) => Err(error.context(format!(
                    "failed to decode Oracle snapshot primary key `{}`",
                    field.name
                ))),
                Err(error) => {
                    tracing::warn!(
                        column = field.name,
                        data_type = %field.data_type,
                        error = %error.as_report(),
                        "failed to decode Oracle snapshot value; using NULL",
                    );
                    Ok(None)
                }
            }
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(OwnedRow::new(datums))
}

fn ensure_success(response: &OracleExternalTableResponse) -> ConnectorResult<()> {
    if let Some(error) = &response.error {
        bail!(
            "Oracle external table operation failed: {}",
            error.error_message
        );
    }
    Ok(())
}

fn discover_table(
    request: OracleExternalTableRequest,
) -> anyhow::Result<OracleExternalTableResponse> {
    invoke_jni_discover(&request)
}

fn current_scn(request: OracleExternalTableRequest) -> anyhow::Result<OracleExternalTableResponse> {
    invoke_jni_current_scn(&request)
}

fn read_snapshot(
    request: OracleExternalTableRequest,
) -> anyhow::Result<OracleExternalTableResponse> {
    invoke_jni_snapshot_read(&request)
}

fn invoke_jni_discover(
    request: &OracleExternalTableRequest,
) -> anyhow::Result<OracleExternalTableResponse> {
    let jvm = Jvm::get_or_init()?;
    execute_with_jni_env(jvm, |env| {
        let request_bytes = env.byte_array_from_slice(&request.encode_to_vec())?;
        let response_bytes = call_static_method!(
            env,
            {com.risingwave.connector.source.common.JniOracleExternalTable},
            {byte[] discover(byte[] requestBytes)},
            &request_bytes
        )?;
        OracleExternalTableResponse::decode(
            risingwave_jni_core::to_guarded_slice(&response_bytes, env)?.deref(),
        )
        .map_err(Into::into)
    })
}

fn invoke_jni_current_scn(
    request: &OracleExternalTableRequest,
) -> anyhow::Result<OracleExternalTableResponse> {
    let jvm = Jvm::get_or_init()?;
    execute_with_jni_env(jvm, |env| {
        let request_bytes = env.byte_array_from_slice(&request.encode_to_vec())?;
        let response_bytes = call_static_method!(
            env,
            {com.risingwave.connector.source.common.JniOracleExternalTable},
            {byte[] currentScn(byte[] requestBytes)},
            &request_bytes
        )?;
        OracleExternalTableResponse::decode(
            risingwave_jni_core::to_guarded_slice(&response_bytes, env)?.deref(),
        )
        .map_err(Into::into)
    })
}

fn invoke_jni_snapshot_read(
    request: &OracleExternalTableRequest,
) -> anyhow::Result<OracleExternalTableResponse> {
    let jvm = Jvm::get_or_init()?;
    execute_with_jni_env(jvm, |env| {
        let request_bytes = env.byte_array_from_slice(&request.encode_to_vec())?;
        let response_bytes = call_static_method!(
            env,
            {com.risingwave.connector.source.common.JniOracleExternalTable},
            {byte[] snapshotRead(byte[] requestBytes)},
            &request_bytes
        )?;
        OracleExternalTableResponse::decode(
            risingwave_jni_core::to_guarded_slice(&response_bytes, env)?.deref(),
        )
        .map_err(Into::into)
    })
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::row::OwnedRow;
    use risingwave_common::types::{DataType, ScalarImpl};
    use risingwave_pb::connector_service::{OracleDatum, OracleRow};

    use super::decode_row;

    /// Verifies that every snapshot column is decoded, while invalid primary-key values reject
    /// the row and an invalid non-primary-key value is logged and represented as `NULL`.
    #[test]
    fn snapshot_row_decode_is_strict_only_for_primary_keys() {
        let schema = Schema::new(vec![
            Field::with_name(DataType::Int32, "id"),
            Field::with_name(DataType::Int32, "value"),
            Field::with_name(DataType::Bytea, "bytes"),
        ]);
        let row = OracleRow {
            values: vec![
                OracleDatum {
                    is_null: false,
                    value: b"7".to_vec(),
                },
                OracleDatum {
                    is_null: false,
                    value: b"not-an-int".to_vec(),
                },
                OracleDatum {
                    is_null: false,
                    value: vec![0, 255],
                },
            ],
        };
        assert_eq!(
            decode_row(row, &schema, &[0]).unwrap(),
            OwnedRow::new(vec![
                Some(ScalarImpl::Int32(7)),
                None,
                Some(ScalarImpl::Bytea(vec![0, 255].into())),
            ])
        );

        let null_pk = OracleRow {
            values: vec![
                OracleDatum {
                    is_null: true,
                    value: vec![],
                },
                OracleDatum {
                    is_null: true,
                    value: vec![],
                },
                OracleDatum {
                    is_null: true,
                    value: vec![],
                },
            ],
        };
        assert!(decode_row(null_pk, &schema, &[0]).is_err());
    }

    /// Verifies that Rust accepts the textual wire formats emitted by the Java snapshot reader
    /// for decimal, timestamp, timestamp-with-time-zone, and JSON values.
    #[test]
    fn snapshot_row_decodes_java_canonical_text() {
        let schema = Schema::new(vec![
            Field::with_name(DataType::Decimal, "amount"),
            Field::with_name(DataType::Timestamp, "created_at"),
            Field::with_name(DataType::Timestamptz, "updated_at"),
            Field::with_name(DataType::Jsonb, "metadata"),
        ]);
        let row = OracleRow {
            values: [
                b"123.45".as_slice(),
                b"2026-09-02T12:34:56.123456".as_slice(),
                b"2026-09-02T12:34:56.123456Z".as_slice(),
                br#"{"ok":true}"#.as_slice(),
            ]
            .into_iter()
            .map(|value| OracleDatum {
                is_null: false,
                value: value.to_vec(),
            })
            .collect(),
        };

        let decoded = decode_row(row, &schema, &[0]).unwrap();
        assert!(decoded.as_inner().iter().all(Option::is_some));
    }
}
