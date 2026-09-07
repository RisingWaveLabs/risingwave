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
use std::str::FromStr;
use std::sync::LazyLock;

use anyhow::{Context, anyhow};
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::{
    FormatVersion, NullOrder, SortDirection, SortField, SortOrder, TableProperties, Transform,
    UnboundPartitionField, UnboundPartitionSpec,
};
use iceberg::table::Table;
use iceberg::{Catalog, NamespaceIdent, TableCreation};
use itertools::Itertools;
use regex::Regex;
use risingwave_common::array::arrow::arrow_schema_iceberg::{
    self, DataType as ArrowDataType, Field as ArrowField, Fields as ArrowFields,
    Schema as ArrowSchema,
};
use risingwave_common::array::arrow::{IcebergArrowConvert, IcebergCreateTableArrowConvert};
use risingwave_common::bail;
use risingwave_common::catalog::Schema;
use risingwave_common::util::iter_util::ZipEqFast;
use url::Url;

use super::{IcebergConfig, PARTITION_DATA_ID_START, SinkError};
use crate::connector_common::{IcebergCatalogKind, IcebergCatalogRuntime};
use crate::sink::{Result, SinkParam};

static ORDER_KEY_COLUMN_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[A-Za-z_][A-Za-z0-9_]*$").expect("valid order key regex"));

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IcebergOrderKeyField {
    pub column: String,
    pub direction: SortDirection,
    pub null_order: NullOrder,
}

impl IcebergOrderKeyField {
    fn default_null_order(direction: SortDirection) -> NullOrder {
        match direction {
            SortDirection::Ascending => NullOrder::First,
            SortDirection::Descending => NullOrder::Last,
        }
    }
}

pub async fn create_and_validate_table_impl(
    config: &IcebergConfig,
    param: &SinkParam,
) -> Result<Table> {
    if config.create_table_if_not_exists {
        create_table_if_not_exists_impl(config, param).await?;
    }

    let table = config
        .load_table()
        .await
        .map_err(|err| SinkError::Iceberg(anyhow!(err)))?;

    if config.enable_pk_index {
        let table_format_version = table.metadata().format_version();
        if table_format_version < FormatVersion::V2 {
            return Err(SinkError::Config(anyhow!(
                "`enable_pk_index` requires an Iceberg table with format version >= 2, \
                 but the target table is format version {}",
                table_format_version
            )));
        }
    }

    let sink_schema = param.schema();
    let iceberg_arrow_schema = schema_to_arrow_schema(table.metadata().current_schema())
        .map_err(|err| SinkError::Iceberg(anyhow!(err)))?;

    try_matches_arrow_schema(&sink_schema, &iceberg_arrow_schema)
        .map_err(|err| SinkError::Iceberg(anyhow!(err)))?;

    Ok(table)
}

/// Returns `true` if this call created the table, `false` if it already existed.
pub(super) async fn create_table_if_not_exists_impl(
    config: &IcebergConfig,
    param: &SinkParam,
) -> Result<bool> {
    let catalog = config.create_catalog().await?;
    let table_id = config
        .full_table_name()
        .context("Unable to parse table name")?;
    let namespace = table_id.namespace().clone();
    let table_name = table_id.name().to_owned();
    create_namespace_if_not_exists(catalog.as_ref(), &namespace).await?;

    if catalog
        .table_exists(&table_id)
        .await
        .map_err(|e| SinkError::Iceberg(anyhow!(e)))?
    {
        return Ok(false);
    }

    if config.table_format_version() < FormatVersion::V3
        && let Some(column) = param
            .columns
            .iter()
            .find(|column| column.data_type.contains_variant())
    {
        return Err(SinkError::Config(anyhow!(
            "creating an Iceberg table with VARIANT column `{}` requires `format_version = '3'`",
            column.name
        )));
    }

    let iceberg_create_table_arrow_convert = IcebergCreateTableArrowConvert::default();
    // convert risingwave schema -> arrow schema -> iceberg schema
    let arrow_fields = param
        .columns
        .iter()
        .map(|column| {
            Ok(iceberg_create_table_arrow_convert
                .to_arrow_field(&column.name, &column.data_type)
                .map_err(|e| SinkError::Iceberg(anyhow!(e)))
                .context(format!(
                    "failed to convert {}: {} to arrow type",
                    column.name, column.data_type
                ))?)
        })
        .collect::<Result<Vec<ArrowField>>>()?;
    let arrow_schema = arrow_schema_iceberg::Schema::new(arrow_fields);
    let iceberg_schema = iceberg::arrow::arrow_schema_to_schema(&arrow_schema)
        .map_err(|e| SinkError::Iceberg(anyhow!(e)))
        .context("failed to convert arrow schema to iceberg schema")?;

    let location = {
        let mut names = namespace.clone().inner();
        names.push(table_name.clone());
        match &config.common.warehouse_path {
            Some(warehouse_path) => {
                let is_s3_tables = warehouse_path.starts_with("arn:aws:s3tables");
                // Lakehouse Iceberg REST catalog federation uses bq:// prefix for BigQuery-managed Iceberg tables.
                let is_bq_catalog_federation = warehouse_path.starts_with("bq://");
                let url = Url::parse(warehouse_path);
                if url.is_err() || is_s3_tables || is_bq_catalog_federation {
                    // For rest catalog, the warehouse_path could be a warehouse name.
                    // In this case, we should specify the location when creating a table.
                    if config
                        .common
                        .is_rest_catalog()
                        .map_err(|err| SinkError::Config(anyhow!(err)))?
                    {
                        None
                    } else {
                        bail!(format!("Invalid warehouse path: {}", warehouse_path))
                    }
                } else if warehouse_path.ends_with('/') {
                    Some(format!("{}{}", warehouse_path, names.join("/")))
                } else {
                    Some(format!("{}/{}", warehouse_path, names.join("/")))
                }
            }
            None => None,
        }
    };

    let partition_spec = match &config.partition_by {
        Some(partition_by) => {
            let mut partition_fields = Vec::<UnboundPartitionField>::new();
            for (i, (column, transform)) in parse_partition_by_exprs(partition_by.clone())?
                .into_iter()
                .enumerate()
            {
                match iceberg_schema.field_id_by_name(&column) {
                    Some(id) => partition_fields.push(
                        UnboundPartitionField::builder()
                            .source_id(id)
                            .transform(transform)
                            .name(format!("_p_{}", column))
                            .field_id(PARTITION_DATA_ID_START + i as i32)
                            .build(),
                    ),
                    None => bail!(format!(
                        "Partition source column does not exist in schema: {}",
                        column
                    )),
                };
            }
            Some(
                UnboundPartitionSpec::builder()
                    .with_spec_id(0)
                    .add_partition_fields(partition_fields)
                    .map_err(|e| SinkError::Iceberg(anyhow!(e)))
                    .context("failed to add partition columns")?
                    .build(),
            )
        }
        None => None,
    };

    let sort_order = match &config.order_key {
        Some(order_key) => Some(build_sort_order(order_key, &iceberg_schema)?),
        None => None,
    };

    // Some JNI catalogs extract `format-version` from table properties, while
    // native Rust Glue rejects reserved properties before creating metadata.
    let properties = if matches!(
        config.catalog_kind()?,
        IcebergCatalogKind::Glue(IcebergCatalogRuntime::NativeRust)
    ) {
        HashMap::new()
    } else {
        HashMap::from([(
            TableProperties::PROPERTY_FORMAT_VERSION.to_owned(),
            (config.format_version as u8).to_string(),
        )])
    };

    let table_creation_builder = TableCreation::builder()
        .name(table_name)
        .schema(iceberg_schema)
        .format_version(config.table_format_version())
        .properties(properties);

    let table_creation = match (location, partition_spec, sort_order) {
        (Some(location), Some(partition_spec), Some(sort_order)) => table_creation_builder
            .location(location)
            .partition_spec(partition_spec)
            .sort_order(sort_order)
            .build(),
        (Some(location), Some(partition_spec), None) => table_creation_builder
            .location(location)
            .partition_spec(partition_spec)
            .build(),
        (Some(location), None, Some(sort_order)) => table_creation_builder
            .location(location)
            .sort_order(sort_order)
            .build(),
        (Some(location), None, None) => table_creation_builder.location(location).build(),
        (None, Some(partition_spec), Some(sort_order)) => table_creation_builder
            .partition_spec(partition_spec)
            .sort_order(sort_order)
            .build(),
        (None, Some(partition_spec), None) => table_creation_builder
            .partition_spec(partition_spec)
            .build(),
        (None, None, Some(sort_order)) => table_creation_builder.sort_order(sort_order).build(),
        (None, None, None) => table_creation_builder.build(),
    };

    catalog
        .create_table(&namespace, table_creation)
        .await
        .map_err(|e| SinkError::Iceberg(anyhow!(e)))
        .context("failed to create iceberg table")?;
    Ok(true)
}

async fn create_namespace_if_not_exists(
    catalog: &dyn Catalog,
    namespace: &NamespaceIdent,
) -> Result<()> {
    let mut namespaces = vec![namespace.clone()];
    let mut parent = namespace.parent();
    while let Some(parent_namespace) = parent {
        parent = parent_namespace.parent();
        namespaces.push(parent_namespace);
    }

    for namespace in namespaces.into_iter().rev() {
        if !catalog
            .namespace_exists(&namespace)
            .await
            .map_err(|e| SinkError::Iceberg(anyhow!(e)))?
        {
            catalog
                .create_namespace(&namespace, HashMap::default())
                .await
                .map_err(|e| SinkError::Iceberg(anyhow!(e)))
                .with_context(|| format!("failed to create iceberg namespace: {namespace}"))?;
        }
    }

    Ok(())
}

const MAP_KEY: &str = "key";
const MAP_VALUE: &str = "value";

fn field_is_compatible(
    rw_type: &risingwave_common::types::DataType,
    arrow_field: &ArrowField,
) -> anyhow::Result<bool> {
    use risingwave_common::types::DataType as RwDataType;

    // A Variant is physically an Arrow struct, but its extension metadata makes it a leaf
    // Iceberg type. The binding is exclusive in both directions: a plain struct mimicking
    // the physical layout must not be written into a VARIANT column.
    let arrow_is_variant = matches!(
        IcebergArrowConvert.type_from_field(arrow_field),
        Ok(RwDataType::Variant)
    );
    if matches!(rw_type, RwDataType::Variant) || arrow_is_variant {
        return Ok(matches!(rw_type, RwDataType::Variant) && arrow_is_variant);
    }

    let converted_arrow_data_type = IcebergArrowConvert
        .to_arrow_field("", rw_type)
        .map_err(|e| anyhow!(e))?
        .data_type()
        .clone();

    match (rw_type, &converted_arrow_data_type, arrow_field.data_type()) {
        (_, ArrowDataType::Decimal128(_, _), ArrowDataType::Decimal128(_, _)) => Ok(true),
        (_, ArrowDataType::Binary, ArrowDataType::LargeBinary)
        | (_, ArrowDataType::LargeBinary, ArrowDataType::Binary) => Ok(true),
        (RwDataType::List(list), ArrowDataType::List(_), ArrowDataType::List(element)) => {
            field_is_compatible(list.elem(), element)
        }
        (RwDataType::Map(map), ArrowDataType::Map(_, _), ArrowDataType::Map(entries, _)) => {
            let ArrowDataType::Struct(fields) = entries.data_type() else {
                return Ok(false);
            };
            let key = fields.iter().find(|field| field.name() == MAP_KEY);
            let value = fields.iter().find(|field| field.name() == MAP_VALUE);
            match (key, value) {
                (Some(key), Some(value)) => Ok(field_is_compatible(map.key(), key)?
                    && field_is_compatible(map.value(), value)?),
                _ => Ok(false),
            }
        }
        (
            RwDataType::Struct(rw_fields),
            ArrowDataType::Struct(_),
            ArrowDataType::Struct(arrow_fields),
        ) => {
            if rw_fields.len() != arrow_fields.len() {
                return Ok(false);
            }
            // `struct_to_arrow` writes struct children by position, so nested fields
            // must match by position as well, like the top-level column-order check.
            for ((rw_name, rw_type), arrow_field) in rw_fields.iter().zip_eq_fast(arrow_fields) {
                if rw_name != arrow_field.name().as_str() {
                    return Ok(false);
                }
                if !field_is_compatible(rw_type, arrow_field)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        (_, left, right) => Ok(left.equals_datatype(right)),
    }
}

fn check_compatibility(
    schema_fields: HashMap<&str, &risingwave_common::types::DataType>,
    fields: &ArrowFields,
) -> anyhow::Result<bool> {
    for arrow_field in fields {
        let our_field_type = schema_fields
            .get(arrow_field.name().as_str())
            .ok_or_else(|| anyhow!("Field {} not found in our schema", arrow_field.name()))?;

        if !field_is_compatible(our_field_type, arrow_field)? {
            let converted_arrow_data_type = IcebergArrowConvert
                .to_arrow_field("", our_field_type)
                .map_err(|e| anyhow!(e))?
                .data_type()
                .clone();
            bail!(
                "field {}'s type is incompatible\nRisingWave converted data type: {}\niceberg's data type: {}",
                arrow_field.name(),
                converted_arrow_data_type,
                arrow_field.data_type()
            );
        }
    }
    Ok(true)
}

/// Try to match our schema with iceberg schema.
pub fn try_matches_arrow_schema(rw_schema: &Schema, arrow_schema: &ArrowSchema) -> Result<()> {
    if rw_schema.fields.len() != arrow_schema.fields().len() {
        bail!(
            "Schema length mismatch, risingwave is {}, and iceberg is {}",
            rw_schema.fields.len(),
            arrow_schema.fields.len()
        );
    }

    let mut schema_fields = HashMap::new();
    rw_schema.fields.iter().for_each(|field| {
        let res = schema_fields.insert(field.name.as_str(), &field.data_type);
        // This assert is to make sure there is no duplicate field name in the schema.
        assert!(res.is_none())
    });

    check_compatibility(schema_fields, &arrow_schema.fields)?;

    // The sink writes columns to the Iceberg table by position, so the column order
    // must match. The check above only validates the name set and types.
    for (idx, (rw_field, arrow_field)) in rw_schema
        .fields
        .iter()
        .zip_eq_fast(arrow_schema.fields().iter())
        .enumerate()
    {
        if rw_field.name.as_str() != arrow_field.name().as_str() {
            bail!(
                "Column order mismatch at position {}: the sink has column `{}` but the \
                 Iceberg table has column `{}`. The Iceberg sink maps columns to the table \
                 by position, so the sink's column order must match the Iceberg table \
                 columns [{}].",
                idx,
                rw_field.name,
                arrow_field.name(),
                arrow_schema.fields().iter().map(|f| f.name()).join(", "),
            );
        }
    }

    Ok(())
}

pub fn parse_partition_by_exprs(
    expr: String,
) -> std::result::Result<Vec<(String, Transform)>, anyhow::Error> {
    // captures column, transform(column), transform(n,column), transform(n, column)
    let re = Regex::new(r"(?<transform>\w+)(\(((?<n>\d+)?(?:,|(,\s)))?(?<field>\w+)\))?").unwrap();
    if !re.is_match(&expr) {
        bail!(format!(
            "Invalid partition fields: {}\nHINT: Supported formats are column, transform(column), transform(n,column), transform(n, column)",
            expr
        ))
    }
    let caps = re.captures_iter(&expr);

    let mut partition_columns = vec![];

    for mat in caps {
        let (column, transform) = if mat.name("n").is_none() && mat.name("field").is_none() {
            (&mat["transform"], Transform::Identity)
        } else {
            let mut func = mat["transform"].to_owned();
            if func == "bucket" || func == "truncate" {
                let n = &mat
                    .name("n")
                    .ok_or_else(|| anyhow!("The `n` must be set with `bucket` and `truncate`"))?
                    .as_str();
                func = format!("{func}[{n}]");
            }
            (
                &mat["field"],
                Transform::from_str(&func)
                    .with_context(|| format!("invalid transform function {}", func))?,
            )
        };
        partition_columns.push((column.to_owned(), transform));
    }
    Ok(partition_columns)
}

pub fn parse_order_key_exprs(
    expr: String,
) -> std::result::Result<Vec<IcebergOrderKeyField>, anyhow::Error> {
    let mut order_keys = Vec::new();
    let mut seen_columns = std::collections::HashSet::new();

    for raw_item in expr.split(',') {
        let item = raw_item.trim();
        if item.is_empty() {
            bail!("Invalid order key: empty item in `{expr}`");
        }

        let tokens = item.split_whitespace().collect_vec();
        if tokens.is_empty() {
            bail!("Invalid order key item `{item}`");
        }
        if tokens.len() > 4 {
            bail!(
                "Invalid order key item `{item}`\nHINT: Supported format is `column [asc|desc] [nulls first|last]`"
            );
        }

        let column = tokens[0];
        if !ORDER_KEY_COLUMN_RE.is_match(column) {
            bail!(
                "Invalid order key column `{column}`\nHINT: Only plain column names are supported in order_key"
            );
        }
        if !seen_columns.insert(column.to_ascii_lowercase()) {
            bail!("Duplicate column `{column}` in order_key");
        }

        let mut direction = SortDirection::Ascending;
        let mut null_order = None;
        let mut idx = 1;
        while idx < tokens.len() {
            match tokens[idx].to_ascii_lowercase().as_str() {
                "asc" => {
                    direction = SortDirection::Ascending;
                    idx += 1;
                }
                "desc" => {
                    direction = SortDirection::Descending;
                    idx += 1;
                }
                "nulls" => {
                    let order = tokens.get(idx + 1).ok_or_else(|| {
                        anyhow!(
                            "Invalid order key item `{item}`: `NULLS` must be followed by `FIRST` or `LAST`"
                        )
                    })?;
                    null_order = Some(match order.to_ascii_lowercase().as_str() {
                        "first" => NullOrder::First,
                        "last" => NullOrder::Last,
                        _ => bail!(
                            "Invalid order key item `{item}`\nHINT: `NULLS` must be followed by `FIRST` or `LAST`"
                        ),
                    });
                    idx += 2;
                }
                token => {
                    bail!(
                        "Invalid order key token `{token}` in `{item}`\nHINT: Supported format is `column [asc|desc] [nulls first|last]`"
                    );
                }
            }
        }

        order_keys.push(IcebergOrderKeyField {
            column: column.to_owned(),
            direction,
            null_order: null_order
                .unwrap_or_else(|| IcebergOrderKeyField::default_null_order(direction)),
        });
    }

    if order_keys.is_empty() {
        bail!("order_key must not be empty");
    }

    Ok(order_keys)
}

pub fn validate_order_key_columns<'a>(
    order_key: &str,
    columns: impl IntoIterator<Item = &'a str>,
) -> std::result::Result<Vec<IcebergOrderKeyField>, anyhow::Error> {
    let parsed = parse_order_key_exprs(order_key.to_owned())?;
    let columns = columns
        .into_iter()
        .map(|column| column.to_ascii_lowercase())
        .collect::<std::collections::HashSet<_>>();
    for item in &parsed {
        if item.column.starts_with('_') {
            bail!(
                "System column `{}` is not allowed in order_key",
                item.column
            );
        }
        if !columns.contains(&item.column.to_ascii_lowercase()) {
            bail!("Order key column does not exist in schema: {}", item.column);
        }
    }
    Ok(parsed)
}

fn build_sort_order(order_key: &str, schema: &iceberg::spec::Schema) -> Result<SortOrder> {
    let order_fields = validate_order_key_columns(
        order_key,
        schema
            .as_struct()
            .fields()
            .iter()
            .map(|field| field.name.as_str()),
    )?;
    let mut builder = SortOrder::builder();
    for field in order_fields {
        let source_id = schema.field_id_by_name(&field.column).ok_or_else(|| {
            anyhow!(
                "Order key column does not exist in schema: {}",
                field.column
            )
        })?;
        builder.with_sort_field(
            SortField::builder()
                .source_id(source_id)
                .transform(Transform::Identity)
                .direction(field.direction)
                .null_order(field.null_order)
                .build(),
        );
    }
    builder
        .build(schema)
        .map_err(|e| SinkError::Iceberg(anyhow!(e)))
}
