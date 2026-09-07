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

use anyhow::anyhow;
use risingwave_meta_model::ColumnCatalogArray;

use super::*;

impl CatalogController {
    pub(super) async fn comment_on_table(
        txn: &DatabaseTransaction,
        object: object::Model,
        object_type: PbObjectType,
        comment: PbComment,
    ) -> MetaResult<PbObjectInfo> {
        let table_id = object.oid.as_table_id();
        let expected_table_type = match object_type {
            PbObjectType::Table => TableType::Table,
            PbObjectType::Mview => TableType::MaterializedView,
            _ => unreachable!("comment_on_table only handles tables and materialized views"),
        };

        let table_type: TableType = Table::find_by_id(table_id)
            .select_only()
            .column(table::Column::TableType)
            .into_tuple()
            .one(txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("table", table_id))?;
        if table_type != expected_table_type {
            return Err(MetaError::invalid_parameter(format!(
                "{} is not a {}",
                table_id,
                match expected_table_type {
                    TableType::Table => "table",
                    TableType::MaterializedView => "materialized view",
                    _ => unreachable!(),
                }
            )));
        }

        let table = if let Some(col_idx) = comment.column_index {
            let columns: ColumnCatalogArray = Table::find_by_id(table_id)
                .select_only()
                .column(table::Column::Columns)
                .into_tuple()
                .one(txn)
                .await?
                .ok_or_else(|| MetaError::catalog_id_not_found("table", table_id))?;
            let mut pb_columns = columns.to_protobuf();

            let column = pb_columns
                .get_mut(col_idx as usize)
                .ok_or_else(|| MetaError::catalog_id_not_found("column", col_idx))?;
            let column_desc = column.column_desc.as_mut().ok_or_else(|| {
                anyhow!(
                    "column desc at index {} for table id {} not found",
                    col_idx,
                    table_id
                )
            })?;
            column_desc.description = comment.description;
            table::ActiveModel {
                table_id: Set(table_id),
                columns: Set(pb_columns.into()),
                ..Default::default()
            }
            .update(txn)
            .await?
        } else {
            table::ActiveModel {
                table_id: Set(table_id),
                description: Set(comment.description),
                ..Default::default()
            }
            .update(txn)
            .await?
        };
        let streaming_job = StreamingJob::find_by_id(table_id.as_job_id())
            .one(txn)
            .await?;

        Ok(PbObjectInfo::Table(
            ObjectModel(table, object, streaming_job).into(),
        ))
    }
}
