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

use super::*;

/// TODO: make hidden columns additional columns, instead of normal columns?
pub async fn extract_iceberg_columns(
    with_properties: &WithOptionsSecResolved,
) -> anyhow::Result<Vec<ColumnCatalog>> {
    let props = ConnectorProperties::extract(with_properties.clone(), true)?;
    if let ConnectorProperties::Iceberg(properties) = props {
        let table = properties.load_table().await?;
        let iceberg_schema: arrow_schema_iceberg::Schema =
            ::iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema())?;

        let mut columns: Vec<ColumnCatalog> = iceberg_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, field)| -> anyhow::Result<ColumnCatalog> {
                let column_desc = ColumnDesc::named(
                    field.name(),
                    ColumnId::new((i + 1).try_into().unwrap()),
                    IcebergArrowConvert
                        .type_from_field(field)
                        .with_context(|| {
                            format!(
                                "failed to infer RisingWave type for Iceberg field {}",
                                field.name()
                            )
                        })?,
                );
                Ok(ColumnCatalog {
                    column_desc,
                    // hide the _row_id column for iceberg engine table
                    // This column is auto generated when users define a table without primary key
                    is_hidden: field.name() == ROW_ID_COLUMN_NAME,
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        columns.extend(ColumnCatalog::iceberg_hidden_cols());

        tracing::info!("iceberg columns: {:?}", columns);

        Ok(columns)
    } else {
        Err(anyhow!(format!(
            "Invalid properties for iceberg source: {:?}",
            props
        )))
    }
}
