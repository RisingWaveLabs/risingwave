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

use anyhow::Context;
use risingwave_common::catalog::ColumnCatalog;
use risingwave_connector::dispatch_sink;
use risingwave_connector::sink::catalog::SinkCatalog;
use risingwave_connector::sink::iceberg::ICEBERG_SINK;
use risingwave_connector::sink::trivial::BLACKHOLE_SINK;
use risingwave_connector::sink::{CONNECTOR_TYPE_KEY, Sink, SinkParam, build_sink};
use risingwave_pb::catalog::PbSink;

use crate::{MetaError, MetaResult};

#[await_tree::instrument(boxed)]
pub async fn validate_sink(prost_sink_catalog: &PbSink) -> MetaResult<()> {
    let sink_catalog = SinkCatalog::from(prost_sink_catalog);
    reject_variant_sink(&sink_catalog)?;
    let param = SinkParam::try_from_sink_catalog(sink_catalog)?;

    let sink = build_sink(param)?;
    sink.validate_unknown_fields()?;

    dispatch_sink!(
        sink,
        sink,
        Ok(sink.validate().await.context("failed to validate sink")?)
    )
}

/// Returns the first column whose type contains VARIANT (nested included). Callers use this to
/// reject connectors that cannot encode variant values.
pub fn first_variant_column(columns: &[ColumnCatalog]) -> Option<&ColumnCatalog> {
    columns
        .iter()
        .find(|column| column.data_type().contains_variant())
}

/// Reject sinks that carry VARIANT columns at creation time unless their connector supports it.
fn reject_variant_sink(sink_catalog: &SinkCatalog) -> MetaResult<()> {
    // Sinks into tables and blackhole sinks do not serialize values with an external encoder;
    // Iceberg uses the standard Parquet Variant physical layout.
    if sink_catalog.target_table.is_some() {
        return Ok(());
    }
    if let Some(connector) = sink_catalog.properties.get(CONNECTOR_TYPE_KEY)
        && (connector.eq_ignore_ascii_case(BLACKHOLE_SINK)
            || connector.eq_ignore_ascii_case(ICEBERG_SINK))
    {
        return Ok(());
    }

    if let Some(column) = first_variant_column(sink_catalog.full_columns()) {
        return Err(MetaError::invalid_parameter(format!(
            "sinking VARIANT columns is not supported yet: column `{}`",
            column.name_with_hidden()
        )));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use risingwave_common::catalog::{ColumnCatalog, ColumnDesc, ColumnId};
    use risingwave_common::types::{DataType, StructType};
    use risingwave_pb::catalog::{PbSink, SinkType as PbSinkType};

    use super::*;

    fn make_sink(connector: &str, data_type: DataType) -> SinkCatalog {
        SinkCatalog::from(PbSink {
            columns: vec![
                ColumnCatalog::visible(ColumnDesc::named("payload", ColumnId::new(0), data_type))
                    .to_protobuf(),
            ],
            properties: BTreeMap::from([(CONNECTOR_TYPE_KEY.to_owned(), connector.to_owned())]),
            sink_type: PbSinkType::AppendOnly as i32,
            ..Default::default()
        })
    }

    #[test]
    fn test_reject_variant_sink() {
        for connector in ["kafka", "doris", "jdbc", "mongodb"] {
            for data_type in [
                DataType::Variant,
                DataType::list(DataType::Variant),
                DataType::Struct(StructType::new(vec![("v", DataType::Variant)])),
            ] {
                let sink = make_sink(connector, data_type.clone());
                let err = reject_variant_sink(&sink).unwrap_err();
                assert!(
                    err.to_string()
                        .contains("sinking VARIANT columns is not supported yet: column `payload`"),
                    "{connector} / {data_type:?}: {err:?}"
                );
            }
        }
    }

    #[test]
    fn test_allow_sink_without_variant() {
        let sink = make_sink("kafka", DataType::Jsonb);
        reject_variant_sink(&sink).unwrap();
    }

    #[test]
    fn test_allow_variant_for_non_encoding_sinks() {
        let sink = make_sink(BLACKHOLE_SINK, DataType::Variant);
        reject_variant_sink(&sink).unwrap();

        let mut sink_into_table = make_sink("table", DataType::Variant);
        sink_into_table.target_table = Some(1.into());
        reject_variant_sink(&sink_into_table).unwrap();
    }

    fn column(name: &str, data_type: DataType) -> ColumnCatalog {
        ColumnCatalog::visible(ColumnDesc::named(name, ColumnId::new(0), data_type))
    }

    #[test]
    fn test_first_variant_column() {
        assert!(first_variant_column(&[]).is_none());
        assert!(
            first_variant_column(&[column("a", DataType::Int32), column("b", DataType::Jsonb)])
                .is_none()
        );

        for data_type in [
            DataType::Variant,
            DataType::list(DataType::Variant),
            DataType::Struct(StructType::new(vec![("v", DataType::Variant)])),
        ] {
            let columns = [
                column("a", DataType::Int32),
                column("v", data_type.clone()),
                column("z", DataType::Variant),
            ];
            let found = first_variant_column(&columns).expect("should find variant column");
            assert_eq!(found.name(), "v", "{data_type:?}");
        }
    }
}
