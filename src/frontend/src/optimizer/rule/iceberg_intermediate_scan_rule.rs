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

//! This rule materializes a `LogicalIcebergIntermediateScan` to the final
//! `LogicalIcebergScan` with delete file anti-joins.
//!
//! This is the final step in the Iceberg scan optimization pipeline:
//! 1. `LogicalSource` -> `LogicalIcebergIntermediateScan`
//! 2. Predicate pushdown and column pruning on `LogicalIcebergIntermediateScan`
//! 3. `LogicalIcebergIntermediateScan` -> `LogicalIcebergScan` (this rule)
//!
//! At this point, the intermediate scan has accumulated:
//! - The predicate to be pushed down to Iceberg
//! - The output column indices for projection
//!
//! This rule:
//! 1. Reads file scan tasks from Iceberg (data files and delete files)
//! 2. Creates the `LogicalIcebergScan` for data files with pre-computed splits
//! 3. Creates anti-joins for equality delete and position delete files
//! 4. Adds a project if output columns differ from scan columns

use std::collections::HashMap;

use anyhow::Context;
use iceberg::scan::FileScanTask;
use iceberg::spec::{DataContentType, FormatVersion};
use risingwave_common::catalog::{
    ColumnCatalog, ICEBERG_FILE_PATH_COLUMN_NAME, ICEBERG_FILE_POS_COLUMN_NAME,
    ICEBERG_SEQUENCE_NUM_COLUMN_NAME,
};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_connector::source::iceberg::{IcebergFileScanTask, IcebergSplitEnumerator};
use risingwave_connector::source::{ConnectorProperties, SourceEnumeratorContext};

use super::prelude::{PlanRef, *};
use crate::error::Result;
use crate::expr::{ExprImpl, ExprType, FunctionCall, InputRef};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{
    Logical, LogicalIcebergIntermediateScan, LogicalIcebergScan, LogicalJoin, LogicalProject,
    LogicalValues,
};
use crate::optimizer::rule::{ApplyResult, FallibleRule};
use crate::utils::{ColIndexMapping, Condition, FRONTEND_RUNTIME};

pub struct IcebergIntermediateScanRule;

impl FallibleRule<Logical> for IcebergIntermediateScanRule {
    fn apply(&self, plan: PlanRef) -> ApplyResult<PlanRef> {
        let scan: &LogicalIcebergIntermediateScan =
            match plan.as_logical_iceberg_intermediate_scan() {
                Some(s) => s,
                None => return ApplyResult::NotApplicable,
            };

        let Some(catalog) = scan.source_catalog() else {
            return ApplyResult::NotApplicable;
        };

        // Create the IcebergSplitEnumerator to get file scan tasks
        let enumerator = if let ConnectorProperties::Iceberg(prop) =
            ConnectorProperties::extract(catalog.with_properties.clone(), false)?
        {
            IcebergSplitEnumerator::new_inner(*prop, SourceEnumeratorContext::dummy().into())
        } else {
            return ApplyResult::NotApplicable;
        };

        #[cfg(madsim)]
        return ApplyResult::Err(
            crate::error::ErrorCode::BindError(
                "iceberg_scan can't be used in the madsim mode".to_string(),
            )
            .into(),
        );

        #[cfg(not(madsim))]
        {
            use risingwave_connector::source::iceberg::IcebergListResult;

            let list_result = tokio::task::block_in_place(|| {
                FRONTEND_RUNTIME.block_on(enumerator.list_scan_tasks(
                    Some(scan.time_travel_info.clone()),
                    scan.iceberg_predicate.clone(),
                ))
            })?;
            let Some(IcebergListResult {
                mut data_files,
                mut equality_delete_files,
                position_delete_files,
                equality_delete_columns,
                format_version,
                schema: table_schema,
            }) = list_result
            else {
                tracing::info!(
                    "There is no valid snapshot for the Iceberg table, returning empty table plan"
                );
                return ApplyResult::Ok(empty_table_plan(&plan, scan));
            };
            if data_files.is_empty() {
                tracing::info!(
                    "There is no data file for the Iceberg table, returning empty table plan"
                );
                return ApplyResult::Ok(empty_table_plan(&plan, scan));
            }

            // Build the data file scan with pre-computed splits
            let mut projection_columns: Vec<&str> = scan
                .output_columns()
                .chain(equality_delete_columns.iter().map(|s| s.as_str()))
                .collect();
            projection_columns.sort_unstable_by_key(|&s| table_schema.field_id_by_name(s));
            projection_columns.dedup();
            set_project_field_ids(
                &mut data_files,
                table_schema.as_ref(),
                projection_columns.iter(),
            )?;
            match format_version {
                FormatVersion::V1 | FormatVersion::V2 => {
                    for file in &mut data_files {
                        file.deletes.clear();
                    }
                }
                FormatVersion::V3 => {
                    for file in &mut data_files {
                        file.deletes
                            .retain(|delete| delete.file_type == DataContentType::PositionDeletes);
                    }
                }
            }

            let column_catalog_map: HashMap<&str, &ColumnCatalog> = catalog
                .columns
                .iter()
                .map(|c| (c.column_desc.name.as_str(), c))
                .collect();
            if !equality_delete_files.is_empty() {
                projection_columns.push(ICEBERG_SEQUENCE_NUM_COLUMN_NAME);
            }
            let use_position_delete_join =
                !position_delete_files.is_empty() && format_version < FormatVersion::V3;
            if use_position_delete_join {
                projection_columns.push(ICEBERG_FILE_PATH_COLUMN_NAME);
                projection_columns.push(ICEBERG_FILE_POS_COLUMN_NAME);
            }
            let column_catalogs =
                build_column_catalogs(projection_columns.iter(), &column_catalog_map)?;
            let core = scan.core.clone_with_column_catalog(column_catalogs);
            let mut plan: PlanRef =
                LogicalIcebergScan::new(core, IcebergFileScanTask::Data(data_files)).into();

            // Add anti-join for equality delete files
            if !equality_delete_files.is_empty() {
                set_project_field_ids(
                    &mut equality_delete_files,
                    table_schema.as_ref(),
                    equality_delete_columns.iter(),
                )?;
                plan = build_equality_delete_hashjoin_scan(
                    scan,
                    &column_catalog_map,
                    plan,
                    equality_delete_files,
                    equality_delete_columns,
                )?;
            }

            // Add anti-join for position delete files
            if use_position_delete_join {
                plan = build_position_delete_hashjoin_scan(
                    scan,
                    &column_catalog_map,
                    plan,
                    position_delete_files,
                )?;
            }

            // Add projection if output columns differ from scan columns
            let schema_len = plan.schema().len();
            let schema_names = plan.schema().fields.iter().map(|f| f.name.as_str());
            let output_columns = scan.output_columns();
            if schema_len != output_columns.len() || !itertools::equal(schema_names, output_columns)
            {
                let col_map: HashMap<&str, usize> = plan
                    .schema()
                    .fields
                    .iter()
                    .enumerate()
                    .map(|(idx, field)| (field.name.as_str(), idx))
                    .collect();
                let output_col_idx: Vec<_> = scan
                    .output_columns()
                    .map(|col| {
                        col_map.get(col).copied().with_context(|| {
                            format!("Output column {} not found in scan schema", col)
                        })
                    })
                    .try_collect()?;
                let mapping = ColIndexMapping::with_remaining_columns(&output_col_idx, schema_len);
                plan = LogicalProject::with_mapping(plan, mapping).into();
            }

            // For Iceberg engine tables, the intermediate scan's output schema has
            // Hummock types (via table_column_type_mapping), but the LogicalIcebergScan
            // reads from Iceberg with Iceberg types. Add casts to match the expected
            // Hummock output types.
            if scan.has_type_mapping() {
                let cast_exprs: Vec<ExprImpl> = plan
                    .schema()
                    .fields
                    .iter()
                    .enumerate()
                    .map(|(i, field)| {
                        let input_ref: ExprImpl = InputRef::new(i, field.data_type.clone()).into();
                        if let Some(target_type) = scan.table_column_type_mapping.get(&field.name) {
                            if &field.data_type != target_type {
                                match input_ref.cast_explicit(target_type) {
                                    Ok(casted) => casted,
                                    Err(_) => InputRef::new(i, field.data_type.clone()).into(),
                                }
                            } else {
                                input_ref
                            }
                        } else {
                            input_ref
                        }
                    })
                    .collect();
                plan = LogicalProject::create(plan, cast_exprs);
            }

            ApplyResult::Ok(plan)
        }
    }
}

impl IcebergIntermediateScanRule {
    pub fn create() -> BoxedRule {
        Box::new(IcebergIntermediateScanRule)
    }
}

/// Returns an empty table plan with the same schema as the scan.
fn empty_table_plan(plan: &PlanRef, scan: &LogicalIcebergIntermediateScan) -> PlanRef {
    LogicalValues::new(vec![], scan.schema().clone(), plan.ctx()).into()
}

/// Builds a mapping of column names to their catalogs by looking them up from a catalog map.
fn build_column_catalogs(
    column_names: impl Iterator<Item = impl AsRef<str>>,
    column_catalog_map: &HashMap<&str, &ColumnCatalog>,
) -> Result<Vec<ColumnCatalog>> {
    let res = column_names
        .map(|name| {
            let name = name.as_ref();
            column_catalog_map
                .get(name)
                .map(|&c| c.clone())
                .with_context(|| format!("Column catalog not found for column {}", name))
        })
        .try_collect()?;
    Ok(res)
}

/// Sets the project field IDs for a list of files based on column names.
fn set_project_field_ids(
    files: &mut [FileScanTask],
    schema: &iceberg::spec::Schema,
    column_names: impl Iterator<Item = impl AsRef<str>>,
) -> Result<()> {
    let project_field_ids: Vec<i32> = column_names
        .map(|name| {
            let name = name.as_ref();
            schema
                .field_id_by_name(name)
                .with_context(|| format!("Column {} not found in table schema", name))
        })
        .try_collect()?;
    for file in files {
        file.project_field_ids = project_field_ids.clone();
    }
    Ok(())
}

/// Builds equality conditions between two sets of input references.
fn build_equal_conditions(
    left_inputs: Vec<InputRef>,
    right_inputs: Vec<InputRef>,
) -> Result<Vec<ExprImpl>> {
    left_inputs
        .into_iter()
        .zip_eq_fast(right_inputs.into_iter())
        .map(|(left, right)| {
            Ok(FunctionCall::new(ExprType::Equal, vec![left.into(), right.into()])?.into())
        })
        .collect()
}

pub fn build_equality_delete_hashjoin_scan(
    scan: &LogicalIcebergIntermediateScan,
    column_catalog_map: &HashMap<&str, &ColumnCatalog>,
    child: PlanRef,
    equality_delete_files: Vec<FileScanTask>,
    equality_delete_columns: Vec<String>,
) -> Result<PlanRef> {
    let column_names = equality_delete_columns
        .iter()
        .map(|s| s.as_str())
        .chain(std::iter::once(ICEBERG_SEQUENCE_NUM_COLUMN_NAME));
    let column_catalogs = build_column_catalogs(column_names, column_catalog_map)?;
    let source = scan.core.clone_with_column_catalog(column_catalogs);

    let equality_delete_iceberg_scan: PlanRef = LogicalIcebergScan::new(
        source,
        IcebergFileScanTask::EqualityDelete(equality_delete_files),
    )
    .into();

    let data_columns_len = child.schema().len();
    // Build join condition: equality delete columns are equal AND sequence number is less than.
    // Join type is LeftAnti to exclude rows that match delete records.
    let build_inputs = |scan: &PlanRef, offset: usize| -> Result<(Vec<InputRef>, InputRef)> {
        let delete_column_index_map = scan
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(index, data_column)| (&data_column.name, (index, &data_column.data_type)))
            .collect::<std::collections::HashMap<_, _>>();
        let delete_column_inputs = equality_delete_columns
            .iter()
            .map(|name| {
                let (index, data_type) = delete_column_index_map
                    .get(name)
                    .with_context(|| format!("Delete column {} not found in scan schema", name))?;
                Ok(InputRef {
                    index: offset + index,
                    data_type: (*data_type).clone(),
                })
            })
            .collect::<Result<Vec<InputRef>>>()?;
        let seq_num_inputs = InputRef {
            index: scan
                .schema()
                .fields()
                .iter()
                .position(|f| f.name.eq(ICEBERG_SEQUENCE_NUM_COLUMN_NAME))
                .context("Sequence number column not found in scan schema")?
                + offset,
            data_type: risingwave_common::types::DataType::Int64,
        };
        Ok((delete_column_inputs, seq_num_inputs))
    };
    let (left_delete_column_inputs, left_seq_num_input) = build_inputs(&child, 0)?;
    let (right_delete_column_inputs, right_seq_num_input) =
        build_inputs(&equality_delete_iceberg_scan, data_columns_len)?;

    let mut eq_join_expr =
        build_equal_conditions(left_delete_column_inputs, right_delete_column_inputs)?;
    eq_join_expr.push(
        FunctionCall::new(
            ExprType::LessThan,
            vec![left_seq_num_input.into(), right_seq_num_input.into()],
        )?
        .into(),
    );
    let on = Condition {
        conjunctions: eq_join_expr,
    };
    let join = LogicalJoin::new(
        child,
        equality_delete_iceberg_scan,
        risingwave_pb::plan_common::JoinType::LeftAnti,
        on,
    );
    Ok(join.into())
}

pub fn build_position_delete_hashjoin_scan(
    scan: &LogicalIcebergIntermediateScan,
    column_catalog_map: &HashMap<&str, &ColumnCatalog>,
    child: PlanRef,
    position_delete_files: Vec<FileScanTask>,
) -> Result<PlanRef> {
    // Position delete files use file path and position to identify deleted rows.
    let delete_column_names = [ICEBERG_FILE_PATH_COLUMN_NAME, ICEBERG_FILE_POS_COLUMN_NAME];
    let column_catalogs = build_column_catalogs(delete_column_names.iter(), column_catalog_map)?;
    let position_delete_source = scan.core.clone_with_column_catalog(column_catalogs);

    let position_delete_iceberg_scan: PlanRef = LogicalIcebergScan::new(
        position_delete_source,
        IcebergFileScanTask::PositionDelete(position_delete_files),
    )
    .into();
    let data_columns_len = child.schema().len();

    let build_inputs = |scan: &PlanRef, offset: usize| {
        scan.schema()
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(index, data_column)| {
                if data_column.name.eq(ICEBERG_FILE_PATH_COLUMN_NAME)
                    || data_column.name.eq(ICEBERG_FILE_POS_COLUMN_NAME)
                {
                    Some(InputRef {
                        index: offset + index,
                        data_type: data_column.data_type(),
                    })
                } else {
                    None
                }
            })
            .collect::<Vec<InputRef>>()
    };
    let left_delete_column_inputs = build_inputs(&child, 0);
    let right_delete_column_inputs = build_inputs(&position_delete_iceberg_scan, data_columns_len);
    let eq_join_expr =
        build_equal_conditions(left_delete_column_inputs, right_delete_column_inputs)?;
    let on = Condition {
        conjunctions: eq_join_expr,
    };
    let join = LogicalJoin::new(
        child,
        position_delete_iceberg_scan,
        risingwave_pb::plan_common::JoinType::LeftAnti,
        on,
    );
    Ok(join.into())
}
