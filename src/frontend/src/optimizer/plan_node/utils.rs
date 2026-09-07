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

use std::collections::HashMap;
use std::default::Default;
use std::ops::Bound;
use std::vec;

use anyhow::anyhow;
use chrono::{MappedLocalTime, TimeZone};
use fixedbitset::FixedBitSet;
use itertools::Itertools;
use pretty_xmlish::{Pretty, Str, StrAssocArr, XmlNode};
use risingwave_common::catalog::{
    ColumnCatalog, ColumnDesc, ConflictBehavior, CreateType, Engine, Field, FieldDisplay, Schema,
    StreamJobStatus,
};
use risingwave_common::constants::log_store::v2::{
    KV_LOG_STORE_PREDEFINED_COLUMNS, PK_ORDERING, VNODE_COLUMN_INDEX,
};
use risingwave_common::hash::VnodeCount;
use risingwave_common::license::Feature;
use risingwave_common::types::{DataType, Interval, ScalarImpl, Timestamptz};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::scan_range::{ScanRange, is_full_range};
use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
use risingwave_connector::source::iceberg::IcebergTimeTravelInfo;
use risingwave_expr::aggregate::PbAggKind;
use risingwave_expr::bail;
use risingwave_pb::stream_plan::StreamScanType;
use risingwave_sqlparser::ast::AsOf;

use super::generic::{self, GenericPlanRef, PhysicalPlanRef};
use super::{BatchPlanRef, StreamPlanRef, pretty_config};
use crate::catalog::table_catalog::TableType;
use crate::catalog::{ColumnId, FragmentId, TableCatalog, TableId};
use crate::error::{ErrorCode, Result};
use crate::expr::InputRef;
use crate::optimizer::plan_node::generic::Agg;
use crate::optimizer::plan_node::{BatchSimpleAgg, PlanAggCall};
use crate::optimizer::property::{Cardinality, Order, RequiredDist, WatermarkColumns};
use crate::utils::{Condition, IndexSet};

#[derive(Default)]
pub struct TableCatalogBuilder {
    /// All columns in this table
    columns: Vec<ColumnCatalog>,
    pk: Vec<ColumnOrder>,
    value_indices: Option<Vec<usize>>,
    vnode_col_idx: Option<usize>,
    column_names: HashMap<String, i32>,
    watermark_columns: Option<FixedBitSet>,
    dist_key_in_pk: Option<Vec<usize>>,
    /// Indices of watermark columns in all columns that should be used for state cleaning.
    clean_watermark_indices: Vec<usize>,
}

/// For DRY, mainly used for construct internal table catalog in stateful streaming executors.
/// Be careful of the order of add column.
impl TableCatalogBuilder {
    /// Add a column from Field info, return the column index of the table
    pub fn add_column(&mut self, field: &Field) -> usize {
        let column_idx = self.columns.len();
        let column_id = column_idx as i32;
        // Add column desc.
        let mut column_desc = ColumnDesc::from_field_with_column_id(field, column_id);

        // Replace dot of the internal table column name with underline.
        column_desc.name = column_desc.name.replace('.', "_");
        // Avoid column name duplicate.
        self.avoid_duplicate_col_name(&mut column_desc);

        self.columns.push(ColumnCatalog {
            column_desc,
            // All columns in internal table are invisible to batch query.
            is_hidden: false,
        });
        column_idx
    }

    /// Extend the columns with column ids reset. The input columns should NOT have duplicate names.
    ///
    /// Returns the indices of the extended columns.
    pub fn extend_columns(&mut self, columns: &[ColumnCatalog]) -> Vec<usize> {
        let base_idx = self.columns.len();
        columns.iter().enumerate().for_each(|(i, col)| {
            assert!(!self.column_names.contains_key(col.name()));
            self.column_names.insert(col.name().to_owned(), 0);

            // Reset the column id for the columns.
            let mut new_col = col.clone();
            new_col.column_desc.column_id = ColumnId::new((base_idx + i) as _);
            self.columns.push(new_col);
        });
        Vec::from_iter(base_idx..(base_idx + columns.len()))
    }

    /// Check whether need to add a ordered column. Different from value, order desc equal pk in
    /// semantics and they are encoded as storage key.
    pub fn add_order_column(&mut self, column_index: usize, order_type: OrderType) {
        self.pk.push(ColumnOrder::new(column_index, order_type));
    }

    /// get the current exist field number of the primary key.
    pub fn get_current_pk_len(&self) -> usize {
        self.pk.len()
    }

    pub fn set_vnode_col_idx(&mut self, vnode_col_idx: usize) {
        self.vnode_col_idx = Some(vnode_col_idx);
    }

    pub fn set_value_indices(&mut self, value_indices: Vec<usize>) {
        self.value_indices = Some(value_indices);
    }

    pub fn set_dist_key_in_pk(&mut self, dist_key_in_pk: Vec<usize>) {
        self.dist_key_in_pk = Some(dist_key_in_pk);
    }

    /// Set the indices of watermark columns used for state cleaning.
    /// These are column indices in the table schema.
    pub fn set_clean_watermark_indices(&mut self, clean_watermark_indices: Vec<usize>) {
        self.clean_watermark_indices = clean_watermark_indices;
    }

    /// Check the column name whether exist before. if true, record occurrence and change the name
    /// to avoid duplicate.
    fn avoid_duplicate_col_name(&mut self, column_desc: &mut ColumnDesc) {
        if let Some(old_identity) = self.column_names.get(&column_desc.name) {
            let column_name = column_desc.name.clone();
            let mut identity = *old_identity;
            loop {
                column_desc.name = format!("{}_{}", column_name, identity);
                identity += 1;
                if !self.column_names.contains_key(&column_desc.name) {
                    break;
                }
            }
            *self.column_names.get_mut(&column_name).unwrap() = identity;
        }
        self.column_names.insert(column_desc.name.clone(), 0);
    }

    /// Consume builder and create `TableCatalog` (for proto). The `read_prefix_len_hint` is the
    /// anticipated read prefix pattern (number of fields) for the table, which can be utilized for
    /// implementing the table's SST filter or other storage optimization techniques.
    pub fn build(self, distribution_key: Vec<usize>, read_prefix_len_hint: usize) -> TableCatalog {
        assert!(read_prefix_len_hint <= self.pk.len());
        let watermark_columns = match self.watermark_columns {
            Some(w) => w,
            None => FixedBitSet::with_capacity(self.columns.len()),
        };

        // If `dist_key_in_pk` is set, check if it matches with `distribution_key`.
        // Note that we cannot derive in the opposite direction, because there can be a column
        // appearing multiple times in the PK.
        if let Some(dist_key_in_pk) = &self.dist_key_in_pk {
            let derived_dist_key = dist_key_in_pk
                .iter()
                .map(|idx| self.pk[*idx].column_index)
                .collect_vec();
            assert_eq!(
                derived_dist_key, distribution_key,
                "dist_key mismatch with dist_key_in_pk"
            );
        }

        TableCatalog {
            id: TableId::placeholder(),
            schema_id: 0.into(),
            database_id: 0.into(),
            associated_source_id: None,
            name: String::new(),
            columns: self.columns.clone(),
            pk: self.pk,
            stream_key: vec![],
            distribution_key,
            // NOTE: This should be altered if `TableCatalogBuilder` is used to build something
            // other than internal tables.
            table_type: TableType::Internal,
            append_only: false,
            owner: risingwave_common::catalog::DEFAULT_SUPER_USER_ID,
            fragment_id: FragmentId::placeholder(),
            dml_fragment_id: None,
            vnode_col_index: self.vnode_col_idx,
            row_id_index: None,
            value_indices: self
                .value_indices
                .unwrap_or_else(|| (0..self.columns.len()).collect_vec()),
            definition: "".into(),
            conflict_behavior: ConflictBehavior::NoCheck,
            version_column_indices: vec![],
            read_prefix_len_hint,
            version: None, // the internal table is not versioned and can't be schema changed
            watermark_columns,
            dist_key_in_pk: self.dist_key_in_pk.unwrap_or_default(),
            cardinality: Cardinality::unknown(), // TODO(card): cardinality of internal table
            created_at_epoch: None,
            initialized_at_epoch: None,
            // NOTE(kwannoel): This may not match the create type of the materialized table.
            // It should be ignored for internal tables.
            create_type: CreateType::Foreground,
            stream_job_status: StreamJobStatus::Creating,
            description: None,
            initialized_at_cluster_version: None,
            created_at_cluster_version: None,
            retention_seconds: None,
            cdc_table_id: None,
            vnode_count: VnodeCount::Placeholder, // will be filled in by the meta service later
            webhook_info: None,
            job_id: None,
            engine: Engine::Hummock,
            clean_watermark_index_in_pk: None,
            clean_watermark_indices: self.clean_watermark_indices,
            refreshable: false, // Internal tables are not refreshable
            vector_index_info: None,
            cdc_table_type: None,
        }
    }

    pub fn columns(&self) -> &[ColumnCatalog] {
        &self.columns
    }
}

/// See also [`super::generic::DistillUnit`].
pub trait Distill {
    fn distill<'a>(&self) -> XmlNode<'a>;

    fn distill_to_string(&self) -> String {
        let mut config = pretty_config();
        let mut output = String::with_capacity(2048);
        config.unicode(&mut output, &Pretty::Record(self.distill()));
        output
    }
}

pub(super) fn childless_record<'a>(
    name: impl Into<Str<'a>>,
    fields: StrAssocArr<'a>,
) -> XmlNode<'a> {
    XmlNode::simple_record(name, fields, Default::default())
}

macro_rules! impl_distill_by_unit {
    ($ty:ty, $core:ident, $name:expr) => {
        use pretty_xmlish::XmlNode;
        use $crate::optimizer::plan_node::generic::DistillUnit;
        use $crate::optimizer::plan_node::utils::Distill;
        impl Distill for $ty {
            fn distill<'a>(&self) -> XmlNode<'a> {
                self.$core.distill_with_name($name)
            }
        }
    };
}
pub(crate) use impl_distill_by_unit;

pub(crate) fn column_names_pretty<'a>(schema: &Schema) -> Pretty<'a> {
    let columns = (schema.fields.iter())
        .map(|f| f.name.clone())
        .map(Pretty::from)
        .collect();
    Pretty::Array(columns)
}

pub(crate) fn watermark_pretty<'a>(
    watermark_columns: &WatermarkColumns,
    schema: &Schema,
) -> Option<Pretty<'a>> {
    if watermark_columns.is_empty() {
        None
    } else {
        let groups = watermark_columns.grouped();
        let pretty_groups = groups
            .values()
            .map(|cols| {
                Pretty::Array(
                    cols.indices()
                        .map(|idx| FieldDisplay(schema.fields.get(idx).unwrap()))
                        .map(|d| Pretty::display(&d))
                        .collect::<Vec<_>>(),
                )
            })
            .collect();
        Some(Pretty::Array(pretty_groups))
    }
}

#[derive(Clone, Copy)]
pub struct IndicesDisplay<'a> {
    pub indices: &'a [usize],
    pub schema: &'a Schema,
}

impl<'a> IndicesDisplay<'a> {
    /// Returns `None` means all
    pub fn from_join<'b>(
        join: &'a generic::Join<impl GenericPlanRef>,
        input_schema: &'a Schema,
    ) -> Pretty<'b> {
        let col_num = join.internal_column_num();
        let id = Self::from(&join.output_indices, col_num, input_schema);
        id.map_or_else(|| Pretty::from("all"), Self::distill)
    }

    /// Returns `None` means all
    fn from(indices: &'a [usize], col_num: usize, schema: &'a Schema) -> Option<Self> {
        if indices.iter().copied().eq(0..col_num) {
            return None;
        }
        Some(Self { indices, schema })
    }

    pub fn distill<'b>(self) -> Pretty<'b> {
        let vec = self.indices.iter().map(|&i| {
            let name = self.schema.fields.get(i).unwrap().name.clone();
            Pretty::from(name)
        });
        Pretty::Array(vec.collect())
    }
}

pub(crate) fn sum_affected_row(dml: BatchPlanRef) -> Result<BatchPlanRef> {
    let dml = RequiredDist::single().batch_enforce_if_not_satisfies(dml, &Order::any())?;
    // Accumulate the affected rows.
    let sum_agg = PlanAggCall {
        agg_type: PbAggKind::Sum.into(),
        return_type: DataType::Int64,
        inputs: vec![InputRef::new(0, DataType::Int64)],
        distinct: false,
        order_by: vec![],
        filter: Condition::true_cond(),
        direct_args: vec![],
    };
    let agg = Agg::new(vec![sum_agg], IndexSet::empty(), dml);
    let batch_agg = BatchSimpleAgg::new(agg);
    Ok(batch_agg.into())
}

/// Call `debug_struct` on the given formatter to create a debug struct builder.
/// If a property list is provided, properties in it will be added to the struct name according to
/// the condition of that property.
macro_rules! plan_node_name {
    ($name:literal $(, { $prop:literal, $cond:expr } )* $(,)?) => {
        {

            let mut properties: Vec<&str> = vec![];
            $( if $cond { properties.push($prop); } )*
            let mut name = $name.to_string();
            if !properties.is_empty() {
                name += " [";
                name += &properties.join(", ");
                name += "]";
            }
            name
        }
    };
}
pub(crate) use plan_node_name;
use risingwave_pb::common::{PbBatchQueryEpoch, batch_query_epoch};

pub fn infer_kv_log_store_table_catalog_inner(
    input: &StreamPlanRef,
    columns: &[ColumnCatalog],
) -> TableCatalog {
    let mut table_catalog_builder = TableCatalogBuilder::default();

    let mut value_indices =
        Vec::with_capacity(KV_LOG_STORE_PREDEFINED_COLUMNS.len() + input.schema().fields().len());

    for (name, data_type) in KV_LOG_STORE_PREDEFINED_COLUMNS {
        let indice = table_catalog_builder.add_column(&Field::with_name(data_type, name));
        value_indices.push(indice);
    }

    table_catalog_builder.set_vnode_col_idx(VNODE_COLUMN_INDEX);

    for (i, ordering) in PK_ORDERING.iter().enumerate() {
        table_catalog_builder.add_order_column(i, *ordering);
    }

    let read_prefix_len_hint = 0;

    if columns.len() != input.schema().fields().len()
        || columns
            .iter()
            .zip_eq_fast(input.schema().fields())
            .any(|(c, f)| *c.data_type() != f.data_type())
    {
        tracing::warn!(
            "sink schema different with upstream schema: sink columns: {:?}, input schema: {:?}.",
            columns,
            input.schema()
        );
    }
    for field in input.schema().fields() {
        let indice = table_catalog_builder.add_column(field);
        value_indices.push(indice);
    }
    table_catalog_builder.set_value_indices(value_indices);

    // Modify distribution key indices based on the pre-defined columns.
    let dist_key = input
        .distribution()
        .dist_column_indices()
        .iter()
        .map(|idx| idx + KV_LOG_STORE_PREDEFINED_COLUMNS.len())
        .collect_vec();

    table_catalog_builder.build(dist_key, read_prefix_len_hint)
}

pub fn infer_synced_kv_log_store_table_catalog_inner(
    input: &StreamPlanRef,
    columns: &[Field],
) -> TableCatalog {
    let mut table_catalog_builder = TableCatalogBuilder::default();

    let mut value_indices =
        Vec::with_capacity(KV_LOG_STORE_PREDEFINED_COLUMNS.len() + columns.len());

    for (name, data_type) in KV_LOG_STORE_PREDEFINED_COLUMNS {
        let indice = table_catalog_builder.add_column(&Field::with_name(data_type, name));
        value_indices.push(indice);
    }

    table_catalog_builder.set_vnode_col_idx(VNODE_COLUMN_INDEX);

    for (i, ordering) in PK_ORDERING.iter().enumerate() {
        table_catalog_builder.add_order_column(i, *ordering);
    }

    let read_prefix_len_hint = 0;

    let payload_indices = {
        let mut payload_indices = Vec::with_capacity(columns.len());
        for column in columns {
            let payload_index = table_catalog_builder.add_column(column);
            payload_indices.push(payload_index);
        }
        payload_indices
    };

    value_indices.extend(payload_indices);
    table_catalog_builder.set_value_indices(value_indices);

    // Modify distribution key indices based on the pre-defined columns.
    let dist_key = input
        .distribution()
        .dist_column_indices()
        .iter()
        .map(|idx| idx + KV_LOG_STORE_PREDEFINED_COLUMNS.len())
        .collect_vec();

    table_catalog_builder.build(dist_key, read_prefix_len_hint)
}

/// Check that all leaf nodes support recoverable background DDL.
pub(crate) fn plan_can_use_background_ddl(plan: &StreamPlanRef) -> bool {
    if plan.inputs().is_empty() {
        if plan.as_stream_source_scan().is_some()
            || plan.as_stream_now().is_some()
            || plan.as_stream_source().is_some()
        {
            true
        } else if let Some(scan) = plan.as_stream_table_scan() {
            #[expect(deprecated)]
            match scan.stream_scan_type() {
                StreamScanType::Backfill
                | StreamScanType::ArrangementBackfill
                | StreamScanType::CrossDbSnapshotBackfill
                | StreamScanType::SnapshotBackfill
                | StreamScanType::UpstreamOnly => true,
                StreamScanType::Unspecified => unreachable!(),
                // These legacy scan types do not persist snapshot progress.
                StreamScanType::Chain | StreamScanType::Rearrange => false,
            }
        } else {
            // Other leaf nodes, such as `StreamValues`, do not support recovery.
            false
        }
    } else {
        assert!(!plan.inputs().is_empty());
        plan.inputs().iter().all(plan_can_use_background_ddl)
    }
}

pub fn unix_timestamp_sec_to_epoch(ts: i64) -> risingwave_common::util::epoch::Epoch {
    let ts = ts.checked_add(1).unwrap();
    risingwave_common::util::epoch::Epoch::from_unix_millis_or_earliest(
        u64::try_from(ts).unwrap_or(0).checked_mul(1000).unwrap(),
    )
}

pub fn to_batch_query_epoch(a: &Option<AsOf>) -> Result<Option<PbBatchQueryEpoch>> {
    let Some(a) = a else {
        return Ok(None);
    };
    Feature::TimeTravel.check_available()?;
    let timestamp = match a {
        AsOf::ProcessTime | AsOf::ProcessTimeBroadcast => {
            return Err(ErrorCode::NotSupported(
                "AS OF PROCTIME is not supported".to_owned(),
                "please use AS OF TIMESTAMP".to_owned(),
            )
            .into());
        }
        AsOf::TimestampNum(ts) => *ts,
        AsOf::TimestampString(ts) => {
            let date_time = speedate::DateTime::parse_str_rfc3339(ts)
                .map_err(|_e| anyhow!("failed to parse the timestamp"))?;
            if date_time.time.tz_offset.is_none() {
                // If the input does not specify a time zone, use the time zone set by the "SET TIME ZONE" command.
                risingwave_expr::expr_context::TIME_ZONE::try_with(|set_time_zone| {
                    let tz =
                        Timestamptz::lookup_time_zone(set_time_zone).map_err(|e| anyhow!(e))?;
                    match tz.with_ymd_and_hms(
                        date_time.date.year.into(),
                        date_time.date.month.into(),
                        date_time.date.day.into(),
                        date_time.time.hour.into(),
                        date_time.time.minute.into(),
                        date_time.time.second.into(),
                    ) {
                        MappedLocalTime::Single(d) => Ok(d.timestamp()),
                        MappedLocalTime::Ambiguous(_, _) | MappedLocalTime::None => {
                            Err(anyhow!(format!(
                                "failed to parse the timestamp {ts} with the specified time zone {tz}"
                            )))
                        }
                    }
                })??
            } else {
                date_time.timestamp_tz()
            }
        }
        AsOf::VersionNum(_) | AsOf::VersionString(_) => {
            return Err(ErrorCode::NotSupported(
                "AS OF VERSION is not supported".to_owned(),
                "please use AS OF TIMESTAMP".to_owned(),
            )
            .into());
        }
        AsOf::ProcessTimeWithInterval((value, leading_field)) => {
            let interval = Interval::parse_with_fields(
                value,
                Some(crate::Binder::bind_date_time_field(*leading_field)),
            )
            .map_err(|_| anyhow!("failed to parse the interval"))?;
            let interval_sec = (interval.epoch_in_micros() / 1_000_000) as i64;
            chrono::Utc::now()
                .timestamp()
                .checked_sub(interval_sec)
                .ok_or_else(|| anyhow!("invalid timestamp"))?
        }
    };
    Ok(Some(PbBatchQueryEpoch {
        epoch: Some(batch_query_epoch::PbEpoch::TimeTravel(
            unix_timestamp_sec_to_epoch(timestamp).0,
        )),
    }))
}

pub fn to_iceberg_time_travel_as_of(
    a: &Option<AsOf>,
    timezone: &String,
) -> Result<Option<IcebergTimeTravelInfo>> {
    Ok(match a {
        Some(AsOf::VersionNum(v)) => Some(IcebergTimeTravelInfo::Version(*v)),
        Some(AsOf::TimestampNum(ts)) => Some(IcebergTimeTravelInfo::TimestampMs(ts * 1000)),
        Some(AsOf::VersionString(_)) => {
            bail!("Unsupported version string in iceberg time travel")
        }
        Some(AsOf::TimestampString(ts)) => {
            let date_time = speedate::DateTime::parse_str_rfc3339(ts)
                .map_err(|_e| anyhow!("failed to parse the timestamp"))?;
            let timestamp = if date_time.time.tz_offset.is_none() {
                // If the input does not specify a time zone, use the time zone set by the "SET TIME ZONE" command.
                let tz = Timestamptz::lookup_time_zone(timezone).map_err(|e| anyhow!(e))?;
                match tz.with_ymd_and_hms(
                    date_time.date.year.into(),
                    date_time.date.month.into(),
                    date_time.date.day.into(),
                    date_time.time.hour.into(),
                    date_time.time.minute.into(),
                    date_time.time.second.into(),
                ) {
                    MappedLocalTime::Single(d) => Ok(d.timestamp()),
                    MappedLocalTime::Ambiguous(_, _) | MappedLocalTime::None => {
                        Err(anyhow!(format!(
                            "failed to parse the timestamp {ts} with the specified time zone {tz}"
                        )))
                    }
                }?
            } else {
                date_time.timestamp_tz()
            };

            Some(IcebergTimeTravelInfo::TimestampMs(
                timestamp * 1000 + date_time.time.microsecond as i64 / 1000,
            ))
        }
        Some(AsOf::ProcessTime)
        | Some(AsOf::ProcessTimeBroadcast)
        | Some(AsOf::ProcessTimeWithInterval(_)) => {
            unreachable!()
        }
        None => None,
    })
}

pub fn scan_ranges_as_strs(order_names: Vec<String>, scan_ranges: &Vec<ScanRange>) -> Vec<String> {
    let mut range_strs = vec![];

    let explain_max_range = 20;
    for scan_range in scan_ranges.iter().take(explain_max_range) {
        #[expect(clippy::disallowed_methods)]
        let mut range_str = scan_range
            .eq_conds
            .iter()
            .zip(order_names.iter())
            .map(|(v, name)| match v {
                Some(v) => format!("{} = {:?}", name, v),
                None => format!("{} IS NULL", name),
            })
            .collect_vec();

        let len = scan_range.eq_conds.len();
        if !is_full_range(&scan_range.range) {
            let bound_range_str = match (&scan_range.range.0, &scan_range.range.1) {
                (Bound::Unbounded, Bound::Unbounded) => unreachable!(),
                (Bound::Unbounded, ub) => ub_to_string(&order_names[len..], ub),
                (lb, Bound::Unbounded) => lb_to_string(&order_names[len..], lb),
                (lb, ub) => format!(
                    "{} AND {}",
                    lb_to_string(&order_names[len..], lb),
                    ub_to_string(&order_names[len..], ub)
                ),
            };
            range_str.push(bound_range_str);
        }
        range_strs.push(range_str.join(" AND "));
    }
    if scan_ranges.len() > explain_max_range {
        range_strs.push("...".to_owned());
    }
    range_strs
}

pub fn ub_to_string(order_names: &[String], ub: &Bound<Vec<Option<ScalarImpl>>>) -> String {
    match ub {
        Bound::Included(v) => {
            let (name, value) = row_to_string(order_names, v);
            format!("{} <= {}", name, value)
        }
        Bound::Excluded(v) => {
            let (name, value) = row_to_string(order_names, v);
            format!("{} < {}", name, value)
        }
        Bound::Unbounded => unreachable!(),
    }
}

pub fn lb_to_string(order_names: &[String], lb: &Bound<Vec<Option<ScalarImpl>>>) -> String {
    match lb {
        Bound::Included(v) => {
            let (name, value) = row_to_string(order_names, v);
            format!("{} >= {}", name, value)
        }
        Bound::Excluded(v) => {
            let (name, value) = row_to_string(order_names, v);
            format!("{} > {}", name, value)
        }
        Bound::Unbounded => unreachable!(),
    }
}

pub fn row_to_string(
    order_names: &[String],
    struct_values: &Vec<Option<ScalarImpl>>,
) -> (String, String) {
    let mut names = vec![];
    let mut values = vec![];
    #[expect(clippy::disallowed_methods)]
    for (name, value) in order_names.iter().zip(struct_values.iter()) {
        names.push(name);
        match value {
            Some(v) => values.push(format!("{:?}", v)),
            None => values.push("null".to_owned()),
        }
    }
    if names.len() == 1 {
        (names[0].clone(), values[0].clone())
    } else {
        (
            format!("({})", names.iter().join(", ")),
            format!("({})", values.iter().join(", ")),
        )
    }
}
