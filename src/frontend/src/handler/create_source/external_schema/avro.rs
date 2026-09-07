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

use super::*;

/// Map an Avro schema to a relational schema.
pub async fn extract_avro_table_schema(
    info: &StreamSourceInfo,
    with_properties: &WithOptionsSecResolved,
    format_encode_options: &mut BTreeMap<String, String>,
    is_debezium: bool,
) -> Result<Vec<ColumnCatalog>> {
    let parser_config = SpecificParserConfig::new(info, with_properties)?;
    try_consume_schema_registry_config_from_options(format_encode_options);
    try_consume_pulsar_schema_config_from_options(format_encode_options);
    consume_aws_config_from_options(format_encode_options);

    let vec_column_desc = if is_debezium {
        let conf = DebeziumAvroParserConfig::new(parser_config.encoding_config).await?;
        conf.map_to_columns()?
    } else {
        if let risingwave_connector::parser::EncodingProperties::Avro(avro_props) =
            &parser_config.encoding_config
            && matches!(avro_props.schema_location, SchemaLocation::File { .. })
            && format_encode_options
                .get("with_deprecated_file_header")
                .is_none_or(|v| v != "true")
        {
            bail_not_implemented!(issue = 12871, "avro without schema registry");
        }
        let conf = AvroParserConfig::new(parser_config.encoding_config).await?;
        conf.map_to_columns()?
    };
    Ok(vec_column_desc
        .into_iter()
        .map(|col| ColumnCatalog {
            column_desc: ColumnDesc::from_field_without_column_id(&col),
            is_hidden: false,
        })
        .collect_vec())
}
