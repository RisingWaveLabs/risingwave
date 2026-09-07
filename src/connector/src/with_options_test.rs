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

use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::{env, fs};

use itertools::Itertools;
use proc_macro2::TokenTree;
use quote::ToTokens;
use serde::{Deserialize, Serialize};
use syn::{Attribute, Field, Item, ItemFn, Lit, LitStr, Meta, Type, parse_file};
use thiserror_ext::AsReport;
use walkdir::{DirEntry, WalkDir};

fn connector_crate_path() -> PathBuf {
    let connector_crate_path = env::var("CARGO_MANIFEST_DIR").unwrap();
    Path::new(&connector_crate_path).to_path_buf()
}

fn common_files() -> impl IntoIterator<Item = walkdir::Result<DirEntry>> {
    WalkDir::new(
        connector_crate_path()
            .join("src")
            .join("connector_common")
            .join("common.rs"),
    )
    .into_iter()
    .chain(WalkDir::new(
        connector_crate_path()
            .join("src")
            .join("connector_common")
            .join("mqtt_common.rs"),
    ))
    .chain(WalkDir::new(
        connector_crate_path()
            .join("src")
            .join("connector_common")
            .join("iceberg")
            .join("mod.rs"),
    ))
}

fn print_struct_info(struct_infos: &BTreeMap<String, StructInfo>) -> String {
    // Generate the output
    format!(
        "# THIS FILE IS AUTO_GENERATED. DO NOT EDIT\n# UPDATE WITH: ./risedev generate-with-options\n\n{}",
        serde_yaml::to_string(&struct_infos).unwrap()
    )
}

pub fn generate_with_options_yaml_source() -> String {
    let struct_infos =
        generate_with_options_yaml_inner(&connector_crate_path().join("src").join("source"));
    print_struct_info(&struct_infos)
}

pub fn generate_with_options_yaml_connection() -> String {
    let mut struct_infos = generate_with_options_yaml_inner(
        &connector_crate_path()
            .join("src")
            .join("connector_common")
            .join("connection.rs"),
    );

    // the struct name should contain `Connection` to be collected
    struct_infos.retain(|name, _| name.contains("Connection"));

    print_struct_info(&struct_infos)
}

pub fn generate_with_options_yaml_sink() -> String {
    let struct_infos =
        generate_with_options_yaml_inner(&connector_crate_path().join("src").join("sink"));
    print_struct_info(&struct_infos)
}

pub fn generate_allow_alter_on_fly_fields_combined() -> String {
    let source_info = extract_allow_alter_on_fly_fields_from_yaml(
        &connector_crate_path().join("with_options_source.yaml"),
    );
    let sink_info = extract_allow_alter_on_fly_fields_from_yaml(
        &connector_crate_path().join("with_options_sink.yaml"),
    );
    let connection_info = extract_allow_alter_on_fly_fields_from_yaml(
        &connector_crate_path().join("with_options_connection.yaml"),
    );

    generate_rust_allow_alter_on_fly_fields_code_separate(source_info, sink_info, connection_info)
}

pub fn generate_iceberg_engine_fields() -> String {
    let fields = extract_iceberg_engine_fields_from_yaml(
        &connector_crate_path().join("with_options_sink.yaml"),
    );
    generate_rust_iceberg_engine_fields_code(fields)
}

/// Collect all structs with `#[derive(WithOptions)]` in the `.rs` files in `path` (plus `common.rs`),
/// and generate a YAML file.
///
/// Note: here we assumes the struct is parsed by `serde`. If it's not the case,
/// the generated `yaml` might be inconsistent with the actual parsing logic.
/// TODO: improve the test to check whether serde is used.
///
/// - For sources, the parsing logic is in `TryFromBTreeMap`.
/// - For sinks, the parsing logic is in `TryFrom<SinkParam>`.
fn generate_with_options_yaml_inner(path: &Path) -> BTreeMap<String, StructInfo> {
    let mut structs = vec![];
    let mut functions = BTreeMap::<String, FunctionInfo>::new();

    // Recursively list all the .rs files
    for entry in walkdir::WalkDir::new(path)
        .into_iter()
        .chain(common_files())
    {
        let entry = entry.expect("Failed to read directory entry");
        if entry.path().extension() == Some("rs".as_ref()) {
            // Parse the content of the .rs file
            let content = fs::read_to_string(entry.path()).expect("Failed to read file");
            let file = parse_file(&content).expect("Failed to parse file");

            // Process each item in the file
            for item in file.items {
                if let Item::Struct(struct_item) = item {
                    // Check if the struct has the #[with_options] attribute
                    if has_with_options_attribute(&struct_item.attrs) {
                        structs.push(struct_item);
                    }
                } else if let Item::Fn(func_item) = item {
                    let (func_name, func_body) = extract_function_body(func_item);
                    functions.insert(func_name, func_body);
                }
            }
        }
    }

    let mut struct_infos: BTreeMap<String, StructInfo> = BTreeMap::new();

    // Process each struct
    for struct_item in structs {
        let struct_name = struct_item.ident.to_string();

        let mut struct_info = StructInfo::default();
        for field in struct_item.fields {
            // Process each field
            if let Some(field_name) = &field.ident {
                if field_name == "unknown_fields" {
                    continue;
                }

                let SerdeProperties {
                    default_func,
                    rename,
                    alias,
                } = extract_serde_properties(&field);

                let allow_alter_on_fly = extract_with_option_flag(&field, "allow_alter_on_fly");
                let iceberg_engine = extract_with_option_flag(&field, "iceberg_engine");

                let field_type = field.ty;
                let mut required = match extract_type_name(&field_type).as_str() {
                    // Fields of type Option<T> or HashMap<K, V> are always considered optional.
                    "HashMap" | "Option" => false,
                    _ => true,
                };
                let mut field_type = quote::quote!(#field_type)
                    .to_token_stream()
                    .into_iter()
                    .join("");
                // Option<T> -> T
                if field_type.starts_with("Option") {
                    field_type = field_type[7..field_type.len() - 1].to_string();
                }
                let comments = extract_comments(&field.attrs);

                // Replace the function name with the function body.
                let mut default = default_func.clone();
                if let Some(default_func) = default_func {
                    if let Some(fn_info) = functions.get(&default_func) {
                        default = Some(fn_info.body.clone());
                    }
                    // If the field has a default value, it must be optional.
                    required = false;
                }

                let name = rename.unwrap_or_else(|| field_name.to_string());

                // Assemble the information
                struct_info.fields.push(FieldInfo {
                    name,
                    field_type,
                    comments,
                    required,
                    default,
                    alias,
                    allow_alter_on_fly,
                    iceberg_engine,
                });
            } else {
                panic!("Unexpected tuple struct: {}", struct_name);
            }
        }
        if struct_infos
            .insert(struct_name.clone(), struct_info)
            .is_some()
        {
            panic!("Duplicate struct: {}", struct_name);
        };
    }

    // Flatten the nested options.
    flatten_nested_options(struct_infos)
}

#[derive(Debug, Serialize, Clone)]
struct FieldInfo {
    // If specified with serde(rename), use the renamed name instead of the Rust name.
    name: String,

    /// For `Option<T>`, it'll be the T.
    field_type: String,

    #[serde(skip_serializing_if = "String::is_empty")]
    comments: String,

    required: bool,

    #[serde(skip_serializing_if = "Option::is_none")]
    default: Option<String>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    alias: Vec<String>,

    #[serde(skip_serializing_if = "std::ops::Not::not")]
    allow_alter_on_fly: bool,

    #[serde(skip_serializing_if = "std::ops::Not::not")]
    iceberg_engine: bool,
}

#[derive(Default)]
struct SerdeProperties {
    default_func: Option<String>,
    rename: Option<String>,
    alias: Vec<String>,
}

#[derive(Debug, Serialize, Default)]
struct StructInfo {
    fields: Vec<FieldInfo>,
}

#[derive(Debug)]
struct FunctionInfo {
    body: String,
}

#[derive(Debug, Deserialize)]
struct YamlFieldInfo {
    name: String,
    #[serde(default)]
    allow_alter_on_fly: bool,
    #[serde(default)]
    iceberg_engine: bool,
    #[serde(default)]
    alias: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct YamlStructInfo {
    fields: Vec<YamlFieldInfo>,
}

/// Has `#[derive(WithOptions)]`
fn has_with_options_attribute(attrs: &[Attribute]) -> bool {
    attrs.iter().any(|attr| {
        if let Meta::List(meta_list) = &attr.meta {
            return meta_list.path.is_ident("derive")
                && meta_list.tokens.clone().into_iter().any(|token| {
                    if let TokenTree::Ident(ident) = token {
                        ident == "WithOptions"
                    } else {
                        false
                    }
                });
        }
        false
    })
}

fn extract_comments(attrs: &[Attribute]) -> String {
    attrs
        .iter()
        .filter_map(|attr| {
            if let Meta::NameValue(mnv) = &attr.meta
                && mnv.path.is_ident("doc")
                && let syn::Expr::Lit(syn::ExprLit {
                    lit: Lit::Str(lit_str),
                    ..
                }) = &mnv.value
            {
                return Some(lit_str.value().trim().to_owned());
            }
            None
        })
        .collect::<Vec<_>>()
        .join("\n")
        .trim()
        .to_owned()
}

fn extract_serde_properties(field: &Field) -> SerdeProperties {
    for attr in &field.attrs {
        if let Meta::List(meta_list) = &attr.meta
            && meta_list.path.is_ident("serde")
        {
            // Initialize the values to be extracted
            let mut serde_props = SerdeProperties::default();

            // Iterate over nested meta items (e.g., rename = "abc")
            meta_list
                .parse_nested_meta(|meta| {
                    if meta.path.is_ident("rename") {
                        if let Ok(value) = meta.value()?.parse::<LitStr>() {
                            serde_props.rename = Some(value.value());
                        }
                    } else if meta.path.is_ident("alias") {
                        if let Ok(value) = meta.value()?.parse::<LitStr>() {
                            serde_props.alias.push(value.value());
                        }
                    } else if meta.path.is_ident("default") {
                        if let Ok(value) = meta.value().and_then(|v| v.parse::<LitStr>()) {
                            serde_props.default_func = Some(value.value());
                        } else {
                            serde_props.default_func = Some("Default::default".to_owned());
                        }
                    }
                    // drain the remaining meta. Otherwise parse_nested_meta returns err
                    // <https://github.com/dtolnay/syn/issues/1426>
                    _ = meta.value();
                    _ = meta.input.parse::<LitStr>();
                    Ok(())
                })
                .unwrap_or_else(|err| {
                    panic!(
                        "Failed to parse serde properties for field: {:?}, err: {}",
                        field.ident,
                        err.to_report_string(),
                    )
                });

            // Return the extracted values
            return serde_props;
        }
    }
    SerdeProperties::default()
}

/// Flatten the nested options, e.g.,
/// ```ignore
/// pub struct KafkaConfig {
///     #[serde(flatten)]
///     pub common: KafkaCommon,
///     #[serde(flatten)]
///     pub rdkafka_properties: RdKafkaPropertiesProducer,
///     // ...
/// }
/// ```
///
/// Note: here we assumes `#[serde(flatten)]` is used for struct fields. If it's not the case,
/// the generated `yaml` might be inconsistent with the actual parsing logic.
fn flatten_nested_options(options: BTreeMap<String, StructInfo>) -> BTreeMap<String, StructInfo> {
    let mut deleted_keys = HashSet::new();

    let mut new_options: BTreeMap<String, StructInfo> = options
        .iter()
        .map(|(name, struct_info)| {
            (
                name.clone(),
                StructInfo {
                    fields: flatten_struct(struct_info, &options, &mut deleted_keys),
                },
            )
        })
        .collect();

    for key in deleted_keys {
        new_options.remove(&key);
    }

    new_options
}

// Get all fields recursively from a WithOptions struct.
fn flatten_struct(
    struct_info: &StructInfo,
    options: &BTreeMap<String, StructInfo>,
    deleted_keys: &mut HashSet<String>,
) -> Vec<FieldInfo> {
    let mut fields = Vec::new();
    for field in &struct_info.fields {
        if let Some(nested_struct_info) = options.get(&field.field_type) {
            fields.append(&mut flatten_struct(
                nested_struct_info,
                options,
                deleted_keys,
            ));
            deleted_keys.insert(field.field_type.clone());
        } else {
            fields.push(field.clone());
        }
    }
    fields
}

// If the type is Option<T>, return Option.
// For HashMap<K, V>, return HashMap.
fn extract_type_name(ty: &Type) -> String {
    if let Type::Path(typepath) = ty
        && let Some(segment) = typepath.path.segments.last()
    {
        return segment.ident.to_string();
    }
    panic!("Failed to extract type name: {}", quote::quote!(#ty));
}

/// Extract the return expression from the body of a single-expression function,
/// like `123` from `fn default_func() { 123 }`
/// or `u64::MAX` from `fn default_func() -> u64 { u64::MAX }`
fn extract_function_body(func: ItemFn) -> (String, FunctionInfo) {
    // The function body is a Block, which contains a vector of Stmts (statements)
    let body = func.block;
    let body = quote::quote!(#body)
        .to_string()
        .trim_start_matches('{')
        .trim_end_matches('}')
        .trim()
        .to_owned();

    (func.sig.ident.to_string(), FunctionInfo { body })
}

fn extract_with_option_flag(field: &Field, flag: &str) -> bool {
    field.attrs.iter().any(|attr| {
        if let Meta::List(meta_list) = &attr.meta {
            return meta_list.path.is_ident("with_option")
                && meta_list.tokens.clone().into_iter().any(|token| {
                    if let TokenTree::Ident(ident) = token {
                        ident == flag
                    } else {
                        false
                    }
                });
        }
        false
    })
}

fn extract_allow_alter_on_fly_fields_from_yaml(yaml_path: &Path) -> BTreeMap<String, Vec<String>> {
    let content = fs::read_to_string(yaml_path)
        .unwrap_or_else(|_| panic!("Failed to read YAML file: {}", yaml_path.display()));

    let yaml_data: BTreeMap<String, YamlStructInfo> = serde_yaml::from_str(&content)
        .unwrap_or_else(|_| panic!("Failed to parse YAML file: {}", yaml_path.display()));

    let mut allow_alter_on_fly_fields: BTreeMap<String, Vec<String>> = BTreeMap::new();

    for (struct_name, struct_info) in yaml_data {
        let mut allow_alter_on_fly_field_names = Vec::new();

        for field in struct_info.fields {
            if field.allow_alter_on_fly {
                allow_alter_on_fly_field_names.push(field.name);
                for alias in &field.alias {
                    allow_alter_on_fly_field_names.push(alias.clone());
                }
            }
        }

        if !allow_alter_on_fly_field_names.is_empty() {
            allow_alter_on_fly_fields.insert(struct_name, allow_alter_on_fly_field_names);
        }
    }

    allow_alter_on_fly_fields
}

fn extract_iceberg_engine_fields_from_yaml(yaml_path: &Path) -> Vec<String> {
    let content = fs::read_to_string(yaml_path)
        .unwrap_or_else(|_| panic!("Failed to read YAML file: {}", yaml_path.display()));

    let yaml_data: BTreeMap<String, YamlStructInfo> = serde_yaml::from_str(&content)
        .unwrap_or_else(|_| panic!("Failed to parse YAML file: {}", yaml_path.display()));

    yaml_data
        .get("IcebergConfig")
        .expect("IcebergConfig must exist in sink WITH options")
        .fields
        .iter()
        .filter(|field| field.iceberg_engine)
        .flat_map(|field| std::iter::once(&field.name).chain(field.alias.iter()))
        .cloned()
        .collect()
}

fn generate_rust_iceberg_engine_fields_code(fields: Vec<String>) -> String {
    let fields = fields
        .iter()
        .map(|field| format!("    \"{field}\","))
        .join("\n");

    format!(
        r#"// Copyright 2026 RisingWave Labs
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

// THIS FILE IS AUTO_GENERATED. DO NOT EDIT.
// UPDATE WITH: ./risedev generate-with-options

pub const ICEBERG_ENGINE_OPTION_KEYS: &[&str] = &[
{fields}
];

pub fn is_iceberg_engine_option(key: &str) -> bool {{
    ICEBERG_ENGINE_OPTION_KEYS.contains(&key)
}}
"#
    )
}

fn generate_rust_allow_alter_on_fly_fields_code_separate(
    source_info: BTreeMap<String, Vec<String>>,
    sink_info: BTreeMap<String, Vec<String>>,
    connection_info: BTreeMap<String, Vec<String>>,
) -> String {
    // Helper function to generate field entries for a single struct
    let generate_struct_entries = |info: &BTreeMap<String, Vec<String>>| -> String {
        info.iter()
                .filter_map(|(struct_name, field_names)| {
                    let key = format!("std::any::type_name::<{}>().to_owned()", struct_name);
                    if field_names.is_empty() {
                        None
                    } else {
                        let fields = field_names
                            .iter()
                            .map(|field| format!("            \"{}\".to_owned(),", field))
                            .collect::<Vec<_>>()
                            .join("\n");
                        Some(format!(
                            "\n    // {}\n    map.try_insert(\n        {},\n        [\n{}\n        ].into_iter().collect(),\n    ).unwrap();",
                            struct_name, key, fields
                        ))
                    }
                })
                .collect::<Vec<_>>()
                .join("")
    };

    let source_entries = generate_struct_entries(&source_info);
    let sink_entries = generate_struct_entries(&sink_info);
    let connection_entries = generate_struct_entries(&connection_info);

    format!(
        r#"// Copyright 2025 RisingWave Labs
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

// THIS FILE IS AUTO_GENERATED. DO NOT EDIT
// UPDATE WITH: ./risedev generate-with-options
// This file is rewritten by `tests::test_allow_alter_on_fly_fields_rust_up_to_date` with
// `UPDATE_EXPECT=1`.
// To update content, change source/sink/connection WITH options definitions (for example,
// `#[with_option(allow_alter_on_fly)]` on struct fields), then run `./risedev generate-with-options`.
// `./risedev generate-with-options` runs three UPDATE_EXPECT tests:
// 1) refresh `with_options_{{source,sink,connection}}.yaml`;
// 2) regenerate this file from those YAML files;
// 3) regenerate the Iceberg Engine option classifier.

#![rustfmt::skip]

use std::collections::{{HashMap, HashSet}};
use std::sync::LazyLock;
use crate::error::ConnectorError;
use crate::sink::remote::JdbcSink;
use crate::sink::Sink;

macro_rules! use_source_properties {{
    ({{ $({{ $variant_name:ident, $prop_name:ty, $split:ty }}),* }}) => {{
        $(
            #[allow(unused_imports)]
            pub(super) use $prop_name;
        )*
    }};
}}

mod source_properties {{
    use crate::for_all_sources;
    use crate::source::base::SourceProperties;

    for_all_sources!(use_source_properties);

    /// Implements a function that maps a source name string to the Rust type name of the corresponding property type.
    /// Usage: `impl_source_name_to_prop_type_name!();` will generate:
    /// ```ignore
    /// pub fn source_name_to_prop_type_name(source_name: &str) -> Option<&'static str>
    /// ```
    macro_rules! impl_source_name_to_prop_type_name_inner {{
        ({{ $({{$variant:ident, $prop_name:ty, $split:ty}}),* }}) => {{
            pub fn source_name_to_prop_type_name(source_name: &str) -> Option<&'static str> {{
                match source_name {{
                    $(
                        <$prop_name>::SOURCE_NAME => Some(std::any::type_name::<$prop_name>()),
                    )*
                    _ => None,
                }}
            }}
        }};
    }}

    macro_rules! impl_source_name_to_prop_type_name {{
        () => {{
            $crate::for_all_sources! {{ impl_source_name_to_prop_type_name_inner }}
        }};
    }}

    impl_source_name_to_prop_type_name!();
}}

mod sink_properties {{
    use crate::use_all_sink_configs;
    use crate::sink::Sink;
    use crate::sink::file_sink::fs::FsSink;

    use_all_sink_configs!();

    macro_rules! impl_sink_name_to_config_type_name_inner {{
        ({{ $({{ $variant_name:ident, $sink_type:ty, $config_type:ty }}),* }}) => {{
            pub fn sink_name_to_config_type_name(sink_name: &str) -> Option<&'static str> {{
                match sink_name {{
                $(
                    <$sink_type>::SINK_NAME => Some(std::any::type_name::<$config_type>()),
                )*
                    _ => None,
                }}
            }}
        }};
    }}

    macro_rules! impl_sink_name_to_config_type_name {{
        () => {{
            $crate::for_all_sinks! {{ impl_sink_name_to_config_type_name_inner }}
        }};
    }}

    impl_sink_name_to_config_type_name!();
}}

/// Map of source connector names to their `allow_alter_on_fly` field names
pub static SOURCE_ALLOW_ALTER_ON_FLY_FIELDS: LazyLock<HashMap<String, HashSet<String>>> = LazyLock::new(|| {{
    use source_properties::*;
    let mut map = HashMap::new();
    // CDC Properties - added for schema.change.failure.policy
    map.try_insert(
        std::any::type_name::<MysqlCdcProperties>().to_owned(),
        [
            "cdc.source.wait.streaming.start.timeout".to_owned(),
            "debezium.max.queue.size".to_owned(),
            "debezium.queue.memory.ratio".to_owned(),
            "hostname".to_owned(),
            "port".to_owned(),
            "password".to_owned(),
        ].into_iter().collect(),
    ).unwrap();
    map.try_insert(
        std::any::type_name::<PostgresCdcProperties>().to_owned(),
        [
            "cdc.source.wait.streaming.start.timeout".to_owned(),
            "debezium.max.queue.size".to_owned(),
            "debezium.queue.memory.ratio".to_owned(),
            "debezium.heartbeat.interval.ms".to_owned(),
            "hostname".to_owned(),
            "port".to_owned(),
            "password".to_owned(),
        ].into_iter().collect(),
    ).unwrap();
    map.try_insert(
        std::any::type_name::<SqlServerCdcProperties>().to_owned(),
        [
            "cdc.source.wait.streaming.start.timeout".to_owned(),
            "debezium.max.queue.size".to_owned(),
            "debezium.queue.memory.ratio".to_owned(),
            "hostname".to_owned(),
            "port".to_owned(),
            "password".to_owned(),
        ].into_iter().collect(),
    ).unwrap();

    map.try_insert(
        std::any::type_name::<MongodbCdcProperties>().to_owned(),
        [
            "cdc.source.wait.streaming.start.timeout".to_owned(),
            "debezium.max.queue.size".to_owned(),
            "debezium.queue.memory.ratio".to_owned(),
        ].into_iter().collect(),
    ).unwrap();{source_entries}
    map
}});

/// Map of sink connector names to their `allow_alter_on_fly` field names
pub static SINK_ALLOW_ALTER_ON_FLY_FIELDS: LazyLock<HashMap<String, HashSet<String>>> = LazyLock::new(|| {{
    use sink_properties::*;
    let mut map = HashMap::new();{sink_entries}
    // Jdbc
    map.try_insert(
        JdbcSink::SINK_NAME.to_owned(),
        [
            "jdbc.url".to_owned(),
            "user".to_owned(),
            "password".to_owned(),
        ].into_iter().collect(),
    ).unwrap();
    map
}});

/// Map of connection names to their `allow_alter_on_fly` field names
pub static CONNECTION_ALLOW_ALTER_ON_FLY_FIELDS: LazyLock<HashMap<String, HashSet<String>>> = LazyLock::new(|| {{
    use crate::connector_common::*;
    let mut map = HashMap::new();{connection_entries}
    // Jdbc
    map.try_insert(
        JdbcSink::SINK_NAME.to_owned(),
        [
            "jdbc.url".to_owned(),
            "user".to_owned(),
            "password".to_owned(),
        ].into_iter().collect(),
    ).unwrap();
    map
}});

/// Get all source connector names that have `allow_alter_on_fly` fields
pub fn get_source_connectors_with_allow_alter_on_fly_fields() -> Vec<&'static str> {{
    SOURCE_ALLOW_ALTER_ON_FLY_FIELDS.keys().map(|s| s.as_str()).collect()
}}

/// Get all sink connector names that have `allow_alter_on_fly` fields
pub fn get_sink_connectors_with_allow_alter_on_fly_fields() -> Vec<&'static str> {{
    SINK_ALLOW_ALTER_ON_FLY_FIELDS.keys().map(|s| s.as_str()).collect()
}}

/// Get all connection names that have `allow_alter_on_fly` fields
pub fn get_connection_names_with_allow_alter_on_fly_fields() -> Vec<&'static str> {{
    CONNECTION_ALLOW_ALTER_ON_FLY_FIELDS.keys().map(|s| s.as_str()).collect()
}}

/// Checks if all given fields are allowed to be altered on the fly for the specified source connector.
/// Returns Ok(()) if all fields are allowed, otherwise returns a `ConnectorError`.
pub fn check_source_allow_alter_on_fly_fields(
    connector_name: &str,
    fields: &[String],
) -> crate::error::ConnectorResult<()> {{
    // Convert connector name to the type name key
    let Some(type_name) = source_properties::source_name_to_prop_type_name(connector_name) else {{
        return Err(ConnectorError::from(anyhow::anyhow!(
            "Unknown source connector: {{connector_name}}"
        )));
    }};
    let Some(allowed_fields) = SOURCE_ALLOW_ALTER_ON_FLY_FIELDS.get(type_name) else {{
    return Err(ConnectorError::from(anyhow::anyhow!(
        "No allow_alter_on_fly fields registered for connector: {{connector_name}}"
    )));
    }};
    for field in fields {{
        if !allowed_fields.contains(field) {{
            return Err(ConnectorError::from(anyhow::anyhow!(
                "Field '{{field}}' is not allowed to be altered on the fly for connector: {{connector_name}}"
            )));
        }}
    }}
    Ok(())
}}

pub fn check_connection_allow_alter_on_fly_fields(
    connection_name: &str,
    fields: &[String],
) -> crate::error::ConnectorResult<()> {{
    use crate::source::connection_name_to_prop_type_name;

    // Convert connection name to the type name key
    let Some(type_name) = connection_name_to_prop_type_name(connection_name) else {{
        return Err(ConnectorError::from(anyhow::anyhow!(
            "Unknown connection: {{connection_name}}"
        )));
    }};
    let Some(allowed_fields) = CONNECTION_ALLOW_ALTER_ON_FLY_FIELDS.get(type_name) else {{
        return Err(ConnectorError::from(anyhow::anyhow!(
            "No allow_alter_on_fly fields registered for connection: {{connection_name}}"
        )));
    }};
    for field in fields {{
        if !allowed_fields.contains(field) {{
            return Err(ConnectorError::from(anyhow::anyhow!(
                "Field '{{field}}' is not allowed to be altered on the fly for connection: {{connection_name}}"
            )));
        }}
    }}
    Ok(())
}}

/// Checks if all given fields are allowed to be altered on the fly for the specified sink connector.
/// Returns Ok(()) if all fields are allowed, otherwise returns a `ConnectorError`.
pub fn check_sink_allow_alter_on_fly_fields(
    sink_name: &str,
    fields: &[String],
) -> crate::error::ConnectorResult<()> {{
    // TODO(#24846): JDBC sink currently uses `()` as sink config type in `for_all_sinks!`,
    // so it cannot have an isolated key in `SINK_ALLOW_ALTER_ON_FLY_FIELDS`.
    // Reuse the JDBC entry in `CONNECTION_ALLOW_ALTER_ON_FLY_FIELDS` for now.
    // TODO(#24846): remove this special case after JDBC sink has a dedicated config type
    // and allow-alter fields are generated directly into `SINK_ALLOW_ALTER_ON_FLY_FIELDS`.
    let allowed_fields = if sink_name == JdbcSink::SINK_NAME {{
        CONNECTION_ALLOW_ALTER_ON_FLY_FIELDS.get(JdbcSink::SINK_NAME)
    }} else {{
        // Convert sink name to the type name key
        let Some(type_name) = sink_properties::sink_name_to_config_type_name(sink_name) else {{
            return Err(ConnectorError::from(anyhow::anyhow!(
                "Unknown sink connector: {{sink_name}}"
            )));
        }};
        SINK_ALLOW_ALTER_ON_FLY_FIELDS.get(type_name)
    }};
    let Some(allowed_fields) = allowed_fields else {{
        return Err(ConnectorError::from(anyhow::anyhow!(
            "No allow_alter_on_fly fields registered for sink: {{sink_name}}"
        )));
    }};
    for field in fields {{
        if !allowed_fields.contains(field) {{
            return Err(ConnectorError::from(anyhow::anyhow!(
                "Field '{{field}}' is not allowed to be altered on the fly for sink: {{sink_name}}"
            )));
        }}
    }}
    Ok(())
}}
"#,
        source_entries = source_entries,
        sink_entries = sink_entries
    )
}
