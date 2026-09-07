// Copyright 2025 RisingWave Labs
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

use anyhow::Context;
use pgwire::pg_response::StatementType;
use risingwave_sqlparser::ast::{ObjectName, SqlOption, SqlOptionValue, Value as AstValue};
use toml::Value as TomlValue;
use toml::map::Map as TomlMap;

use crate::error::{Result, bail_invalid_input_syntax};
use crate::handler::alter_utils::resolve_streaming_job_id_for_alter;
use crate::handler::{HandlerArgs, RwPgResponse};

/// A diff of a TOML map. `None` means the key should be removed.
type TomlMapDiff = TomlMap<String, Option<TomlValue>>;

const STREAMING_CACHE_REFILL_POLICY_CONFIG_PATH: &str = "streaming.developer.cache_refill_policy";
const ALTER_CONFIG_RECOVER_NOTICE: &str =
    "ALTER CONFIG requires a RECOVER on the specified streaming job to take effect.";

fn alter_config_requires_recover(map_diff: &TomlMapDiff) -> bool {
    map_diff
        .keys()
        .any(|key| key != STREAMING_CACHE_REFILL_POLICY_CONFIG_PATH)
}

fn collect_options(entries: Vec<SqlOption>) -> Result<TomlMapDiff> {
    let mut map = TomlMap::new();

    for SqlOption { name, value } in entries {
        let name = name.real_value();
        if !name.starts_with("streaming.") {
            bail_invalid_input_syntax!(
                "ALTER CONFIG only accepts options starting with `streaming.`"
            );
        }
        let SqlOptionValue::Value(value) = value else {
            bail_invalid_input_syntax!("ALTER CONFIG only accepts value options");
        };

        let value = match value {
            AstValue::Number(n) => {
                let n: TomlValue = n.parse().context("Invalid number for ALTER CONFIG")?;
                Some(n)
            }
            AstValue::SingleQuotedString(s) | AstValue::DoubleQuotedString(s) => {
                Some(TomlValue::String(s))
            }
            AstValue::Boolean(b) => Some(TomlValue::Boolean(b)),
            AstValue::Null => None,
            _ => bail_invalid_input_syntax!("Unsupported value for ALTER CONFIG: {}", value),
        };

        let old = map.insert(name.clone(), value);
        if old.is_some() {
            bail_invalid_input_syntax!("Duplicate option for ALTER CONFIG: {}", name);
        }
    }

    Ok(map)
}

pub async fn handle_alter_streaming_set_config(
    handler_args: HandlerArgs,
    obj_name: ObjectName,
    entries: Vec<SqlOption>,
    stmt_type: StatementType,
) -> Result<RwPgResponse> {
    let session = handler_args.session;

    let job_id = resolve_streaming_job_id_for_alter(&session, obj_name, stmt_type, "config")?;
    let map_diff = collect_options(entries)?;
    let requires_recover = alter_config_requires_recover(&map_diff);

    let mut entries_to_add = HashMap::new();
    let mut keys_to_remove = Vec::new();

    for (k, v) in map_diff {
        if let Some(v) = v {
            entries_to_add.insert(k, v.to_string());
        } else {
            keys_to_remove.push(k);
        }
    }

    let catalog_writer = session.catalog_writer()?;
    catalog_writer
        .alter_config(job_id, entries_to_add, keys_to_remove)
        .await?;

    let mut builder = RwPgResponse::builder(stmt_type);
    if requires_recover {
        builder = builder.notice(ALTER_CONFIG_RECOVER_NOTICE);
    }
    Ok(builder.into())
}

pub async fn handle_alter_streaming_reset_config(
    handler_args: HandlerArgs,
    obj_name: ObjectName,
    keys: Vec<ObjectName>,
    stmt_type: StatementType,
) -> Result<RwPgResponse> {
    let entries = keys
        .into_iter()
        .map(|k| SqlOption {
            name: k,
            value: SqlOptionValue::null(),
        })
        .collect();

    // Simply delegate to `handle_alter_streaming_set_config` with all values set to `NULL`.
    handle_alter_streaming_set_config(handler_args, obj_name, entries, stmt_type).await
}

#[cfg(test)]
mod tests {
    use toml::Value as TomlValue;

    use super::{
        STREAMING_CACHE_REFILL_POLICY_CONFIG_PATH, TomlMapDiff, alter_config_requires_recover,
    };

    #[test]
    fn test_cache_refill_policy_config_does_not_require_recover() {
        let mut map_diff = TomlMapDiff::new();
        map_diff.insert(
            STREAMING_CACHE_REFILL_POLICY_CONFIG_PATH.to_owned(),
            Some(TomlValue::String("both".to_owned())),
        );
        assert!(!alter_config_requires_recover(&map_diff));

        map_diff.insert(STREAMING_CACHE_REFILL_POLICY_CONFIG_PATH.to_owned(), None);
        assert!(!alter_config_requires_recover(&map_diff));
    }

    #[test]
    fn test_other_streaming_config_still_requires_recover() {
        let mut map_diff = TomlMapDiff::new();
        map_diff.insert(
            "streaming.developer.some_other_config".to_owned(),
            Some(TomlValue::Boolean(true)),
        );
        assert!(alter_config_requires_recover(&map_diff));
    }

    #[test]
    fn test_mixed_streaming_config_requires_recover() {
        let mut map_diff = TomlMapDiff::new();
        map_diff.insert(
            STREAMING_CACHE_REFILL_POLICY_CONFIG_PATH.to_owned(),
            Some(TomlValue::String("both".to_owned())),
        );
        map_diff.insert(
            "streaming.developer.some_other_config".to_owned(),
            Some(TomlValue::Boolean(true)),
        );
        assert!(alter_config_requires_recover(&map_diff));
    }
}
