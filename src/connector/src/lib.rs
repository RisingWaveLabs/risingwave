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

#![expect(clippy::derive_partial_eq_without_eq)]
#![warn(clippy::large_futures, clippy::large_stack_frames)]
#![feature(coroutines)]
#![feature(proc_macro_hygiene)]
#![feature(stmt_expr_attributes)]
#![feature(trait_alias)]
#![feature(type_alias_impl_trait)]
#![feature(associated_type_defaults)]
#![feature(iter_from_coroutine)]
#![feature(iterator_try_collect)]
#![feature(try_blocks)]
#![feature(error_generic_member_access)]
#![feature(negative_impls)]
#![feature(register_tool)]
#![feature(never_type)]
#![feature(map_try_insert)]
#![register_tool(rw)]
#![recursion_limit = "256"]
#![feature(min_specialization)]
#![feature(custom_inner_attributes)]
#![feature(iter_array_chunks)]

use std::time::Duration;

use duration_str::parse_std;
use serde::de;

pub mod aws_utils;

pub mod allow_alter_on_fly_fields;

mod enforce_secret;
pub mod error;
mod macros;

pub mod parser;
pub mod schema;
pub mod sink;
pub mod source;

pub mod connector_common;

pub use paste::paste;
pub use risingwave_jni_core::{call_method, call_static_method, jvm_runtime};

mod with_options;
pub use with_options::{Get, GetKeyIter, WithOptionsSecResolved, WithPropertiesExt};

#[cfg(test)]
mod with_options_test;

pub const AUTO_SCHEMA_CHANGE_KEY: &str = "auto.schema.change";
pub const SINK_CREATE_TABLE_IF_NOT_EXISTS_KEY: &str = "create_table_if_not_exists";
pub const SINK_TARGET_TABLE_NAME: &str = "table.name";
pub const SINK_INTERMEDIATE_TABLE_NAME: &str = "intermediate.table.name";

pub(crate) fn deserialize_u32_from_string<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: de::Deserializer<'de>,
{
    let s: String = de::Deserialize::deserialize(deserializer)?;
    s.parse().map_err(|_| {
        de::Error::invalid_value(
            de::Unexpected::Str(&s),
            &"integer greater than or equal to 0",
        )
    })
}

pub(crate) fn deserialize_optional_string_seq_from_string<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<Vec<String>>, D::Error>
where
    D: de::Deserializer<'de>,
{
    let s: Option<String> = de::Deserialize::deserialize(deserializer)?;
    if let Some(s) = s {
        let s = s.to_ascii_lowercase();
        let s = s.split(',').map(|s| s.trim().to_owned()).collect();
        Ok(Some(s))
    } else {
        Ok(None)
    }
}

pub(crate) fn deserialize_optional_u64_seq_from_string<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<Vec<u64>>, D::Error>
where
    D: de::Deserializer<'de>,
{
    let s: Option<String> = de::Deserialize::deserialize(deserializer)?;
    if let Some(s) = s {
        let numbers = s
            .split(',')
            .map(|s| s.trim().parse())
            .collect::<Result<Vec<u64>, _>>()
            .map_err(|_| de::Error::invalid_value(de::Unexpected::Str(&s), &"invalid number"));
        Ok(Some(numbers?))
    } else {
        Ok(None)
    }
}

pub(crate) fn deserialize_bool_from_string<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: de::Deserializer<'de>,
{
    let s: String = de::Deserialize::deserialize(deserializer)?;
    let s = s.to_ascii_lowercase();
    match s.as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(de::Error::invalid_value(
            de::Unexpected::Str(&s),
            &"true or false",
        )),
    }
}

pub(crate) fn deserialize_optional_bool_from_string<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<bool>, D::Error>
where
    D: de::Deserializer<'de>,
{
    let s: Option<String> = de::Deserialize::deserialize(deserializer)?;
    if let Some(s) = s {
        let s = s.to_ascii_lowercase();
        match s.as_str() {
            "true" => Ok(Some(true)),
            "false" => Ok(Some(false)),
            _ => Err(de::Error::invalid_value(
                de::Unexpected::Str(&s),
                &"true or false",
            )),
        }
    } else {
        Ok(None)
    }
}

pub(crate) fn deserialize_duration_from_string<'de, D>(
    deserializer: D,
) -> Result<Duration, D::Error>
where
    D: de::Deserializer<'de>,
{
    let s: String = de::Deserialize::deserialize(deserializer)?;
    parse_std(&s).map_err(|_| de::Error::invalid_value(
        de::Unexpected::Str(&s),
        &"The String value unit support for one of:[“y”,“mon”,“w”,“d”,“h”,“m”,“s”, “ms”, “µs”, “ns”]",
    ))
}

pub(crate) fn deserialize_optional_duration_from_string<'de, D>(
    deserializer: D,
) -> Result<Option<Duration>, D::Error>
where
    D: de::Deserializer<'de>,
{
    let s: Option<String> = de::Deserialize::deserialize(deserializer)?;
    if let Some(s) = s {
        let duration = parse_std(&s).map_err(|_| de::Error::invalid_value(
            de::Unexpected::Str(&s),
            &"The String value unit support for one of:[“y”,“mon”,“w”,“d”,“h”,“m”,“s”, “ms”, “µs”, “ns”]",
        ))?;
        Ok(Some(duration))
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use expect_test::expect_file;

    use crate::with_options_test::{
        generate_allow_alter_on_fly_fields_combined, generate_iceberg_engine_fields,
        generate_with_options_yaml_connection, generate_with_options_yaml_sink,
        generate_with_options_yaml_source,
    };

    /// This test ensures that `src/connector/with_options.yaml` is up-to-date with the default values specified
    /// in this file. Developer should run `./risedev generate-with-options` to update it if this
    /// test fails.
    #[test]
    fn test_with_options_yaml_up_to_date() {
        expect_file!("../with_options_source.yaml").assert_eq(&generate_with_options_yaml_source());

        expect_file!("../with_options_sink.yaml").assert_eq(&generate_with_options_yaml_sink());

        expect_file!("../with_options_connection.yaml")
            .assert_eq(&generate_with_options_yaml_connection());
    }

    /// This test ensures that the `allow_alter_on_fly` fields Rust file is up-to-date.
    #[test]
    fn test_allow_alter_on_fly_fields_rust_up_to_date() {
        expect_file!("../src/allow_alter_on_fly_fields.rs")
            .assert_eq(&generate_allow_alter_on_fly_fields_combined());
    }

    /// This test ensures that Iceberg Engine sink fields are up-to-date.
    #[test]
    fn test_iceberg_engine_fields_rust_up_to_date() {
        expect_file!("../src/sink/iceberg/engine_options.rs")
            .assert_eq(&generate_iceberg_engine_fields());
    }

    /// Test some serde behavior we rely on.
    mod serde {
        #![expect(dead_code)]

        use std::collections::BTreeMap;

        use expect_test::expect;
        use serde::Deserialize;

        // test deny_unknown_fields and flatten

        // TL;DR: deny_unknown_fields
        // - doesn't work with flatten map
        // - can work with flatten struct
        // - doesn't work with nested flatten struct (This makes a flatten struct behave like a flatten map)

        #[test]
        fn test_outer_deny() {
            #[derive(Deserialize, Debug)]
            #[serde(deny_unknown_fields)]
            struct FlattenMap {
                #[serde(flatten)]
                flatten: BTreeMap<String, String>,
            }
            #[derive(Deserialize, Debug)]
            #[serde(deny_unknown_fields)]
            struct FlattenStruct {
                #[serde(flatten)]
                flatten_struct: Inner,
            }

            #[derive(Deserialize, Debug)]
            #[serde(deny_unknown_fields)]
            struct FlattenBoth {
                #[serde(flatten)]
                flatten: BTreeMap<String, String>,
                #[serde(flatten)]
                flatten_struct: Inner,
            }

            #[derive(Deserialize, Debug)]
            struct Inner {
                a: Option<String>,
                b: Option<String>,
            }

            let json = r#"{
                "a": "b"
            }"#;
            let foo: Result<FlattenMap, _> = serde_json::from_str(json);
            let foo1: Result<FlattenStruct, _> = serde_json::from_str(json);
            let foo2: Result<FlattenBoth, _> = serde_json::from_str(json);

            // with `deny_unknown_fields`, we can't flatten ONLY a map
            expect![[r#"
                Err(
                    Error("unknown field `a`", line: 3, column: 13),
                )
            "#]]
            .assert_debug_eq(&foo);

            // but can flatten a struct!
            expect![[r#"
                Ok(
                    FlattenStruct {
                        flatten_struct: Inner {
                            a: Some(
                                "b",
                            ),
                            b: None,
                        },
                    },
                )
            "#]]
            .assert_debug_eq(&foo1);
            // unknown fields can be denied.
            let foo11: Result<FlattenStruct, _> =
                serde_json::from_str(r#"{ "a": "b", "unknown":1 }"#);
            expect_test::expect![[r#"
                Err(
                    Error("unknown field `unknown`", line: 1, column: 25),
                )
            "#]]
            .assert_debug_eq(&foo11);

            // When both struct and map are flattened, the map also works...
            expect![[r#"
                Ok(
                    FlattenBoth {
                        flatten: {
                            "a": "b",
                        },
                        flatten_struct: Inner {
                            a: Some(
                                "b",
                            ),
                            b: None,
                        },
                    },
                )
            "#]]
            .assert_debug_eq(&foo2);

            let foo21: Result<FlattenBoth, _> =
                serde_json::from_str(r#"{ "a": "b", "unknown":1 }"#);
            expect_test::expect![[r#"
                Err(
                    Error("invalid type: integer `1`, expected a string", line: 1, column: 25),
                )
            "#]]
            .assert_debug_eq(&foo21);
            // This error above is a little funny, since even if we use string, it will still fail.
            let foo22: Result<FlattenBoth, _> =
                serde_json::from_str(r#"{ "a": "b", "unknown":"1" }"#);
            expect_test::expect![[r#"
                Err(
                    Error("unknown field `unknown`", line: 1, column: 27),
                )
            "#]]
            .assert_debug_eq(&foo22);
        }

        #[test]
        fn test_inner_deny() {
            // no outer deny now.
            #[derive(Deserialize, Debug)]
            struct FlattenStruct {
                #[serde(flatten)]
                flatten_struct: Inner,
            }
            #[derive(Deserialize, Debug)]
            #[serde(deny_unknown_fields)]
            struct Inner {
                a: Option<String>,
                b: Option<String>,
            }

            let json = r#"{
                "a": "b", "unknown":1
            }"#;
            let foo: Result<FlattenStruct, _> = serde_json::from_str(json);
            // unknown fields cannot be denied.
            // I think this is because `deserialize_struct` is called, and required fields are passed.
            // Other fields are left for the outer struct to consume.
            expect_test::expect![[r#"
                Ok(
                    FlattenStruct {
                        flatten_struct: Inner {
                            a: Some(
                                "b",
                            ),
                            b: None,
                        },
                    },
                )
            "#]]
            .assert_debug_eq(&foo);
        }

        #[test]
        fn test_multiple_flatten() {
            #[derive(Deserialize, Debug)]
            struct Foo {
                /// struct will "consume" the used fields!
                #[serde(flatten)]
                flatten_struct: Inner1,

                /// map will keep the unknown fields!
                #[serde(flatten)]
                flatten_map1: BTreeMap<String, String>,

                #[serde(flatten)]
                flatten_map2: BTreeMap<String, String>,

                #[serde(flatten)]
                flatten_struct2: Inner2,
            }

            #[derive(Deserialize, Debug)]
            #[serde(deny_unknown_fields)]
            struct Inner1 {
                a: Option<String>,
                b: Option<String>,
            }
            #[derive(Deserialize, Debug)]
            struct Inner11 {
                c: Option<String>,
            }
            #[derive(Deserialize, Debug)]
            #[serde(deny_unknown_fields)]
            struct Inner2 {
                c: Option<String>,
            }

            let json = r#"{
                "a": "b", "c":"d"
            }"#;
            let foo2: Result<Foo, _> = serde_json::from_str(json);

            // When there are multiple flatten, all of them will be used.
            // Also, with outer `flatten``, the inner `deny_unknown_fields` is ignored.
            expect![[r#"
            Ok(
                Foo {
                    flatten_struct: Inner1 {
                        a: Some(
                            "b",
                        ),
                        b: None,
                    },
                    flatten_map1: {
                        "c": "d",
                    },
                    flatten_map2: {
                        "c": "d",
                    },
                    flatten_struct2: Inner2 {
                        c: Some(
                            "d",
                        ),
                    },
                },
            )
        "#]]
            .assert_debug_eq(&foo2);
        }

        #[test]
        fn test_nested_flatten() {
            #[derive(Deserialize, Debug)]
            #[serde(deny_unknown_fields)]
            struct Outer {
                #[serde(flatten)]
                inner: Inner,
            }

            #[derive(Deserialize, Debug)]
            struct Inner {
                a: Option<String>,
                b: Option<String>,
                #[serde(flatten)]
                nested: InnerInner,
            }

            #[derive(Deserialize, Debug)]
            struct InnerInner {
                c: Option<String>,
            }

            let json = r#"{ "a": "b", "unknown":"1" }"#;

            let foo: Result<Outer, _> = serde_json::from_str(json);

            // This is very unfortunate...
            expect_test::expect![[r#"
            Err(
                Error("unknown field `a`", line: 1, column: 27),
            )
        "#]]
            .assert_debug_eq(&foo);

            // Actually, the nested `flatten` will makes the struct behave like a map.
            // Let's remove `deny_unknown_fields` and see
            #[derive(Deserialize, Debug)]
            struct Outer2 {
                #[serde(flatten)]
                inner: Inner,
                /// We can see the fields of `inner` are not consumed.
                #[serde(flatten)]
                map: BTreeMap<String, String>,
            }
            let foo2: Result<Outer2, _> = serde_json::from_str(json);
            expect_test::expect![[r#"
                Ok(
                    Outer2 {
                        inner: Inner {
                            a: Some(
                                "b",
                            ),
                            b: None,
                            nested: InnerInner {
                                c: None,
                            },
                        },
                        map: {
                            "a": "b",
                            "unknown": "1",
                        },
                    },
                )
            "#]]
            .assert_debug_eq(&foo2);
        }

        #[test]
        fn test_flatten_option() {
            #[derive(Deserialize, Debug)]
            struct Foo {
                /// flatten option struct can still consume the field
                #[serde(flatten)]
                flatten_struct: Option<Inner1>,

                /// flatten option map is always `Some`
                #[serde(flatten)]
                flatten_map1: Option<BTreeMap<String, String>>,

                /// flatten option struct is `None` if the required field is absent
                #[serde(flatten)]
                flatten_struct2: Option<Inner2>,

                /// flatten option struct is `Some` if the required field is present and optional field is absent.
                /// Note: if all fields are optional, the struct is always `Some`
                #[serde(flatten)]
                flatten_struct3: Option<Inner3>,
            }

            #[derive(Deserialize, Debug)]
            struct Inner1 {
                a: Option<String>,
                b: Option<String>,
            }
            #[derive(Deserialize, Debug)]
            struct Inner11 {
                c: Option<String>,
            }

            #[derive(Deserialize, Debug)]
            struct Inner2 {
                c: Option<String>,
                d: String,
            }

            #[derive(Deserialize, Debug)]
            struct Inner3 {
                e: Option<String>,
                f: String,
            }

            let json = r#"{
        "a": "b", "c": "d", "f": "g"
     }"#;
            let foo: Result<Foo, _> = serde_json::from_str(json);
            expect![[r#"
                Ok(
                    Foo {
                        flatten_struct: Some(
                            Inner1 {
                                a: Some(
                                    "b",
                                ),
                                b: None,
                            },
                        ),
                        flatten_map1: Some(
                            {
                                "c": "d",
                                "f": "g",
                            },
                        ),
                        flatten_struct2: None,
                        flatten_struct3: Some(
                            Inner3 {
                                e: None,
                                f: "g",
                            },
                        ),
                    },
                )
            "#]]
            .assert_debug_eq(&foo);
        }
    }
}
