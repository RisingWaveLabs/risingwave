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

mod client;
mod util;
pub use client::*;
use risingwave_pb::catalog::SchemaRegistryNameStrategy as PbSchemaRegistryNameStrategy;
pub(crate) use util::*;

use super::{InvalidOptionError, invalid_option_error};

pub fn name_strategy_from_str(value: &str) -> Option<PbSchemaRegistryNameStrategy> {
    match value {
        "topic_name_strategy" => Some(PbSchemaRegistryNameStrategy::Unspecified),
        "record_name_strategy" => Some(PbSchemaRegistryNameStrategy::RecordNameStrategy),
        "topic_record_name_strategy" => Some(PbSchemaRegistryNameStrategy::TopicRecordNameStrategy),
        _ => None,
    }
}

pub fn get_subject_by_strategy(
    name_strategy: &PbSchemaRegistryNameStrategy,
    topic: &str,
    record: Option<&str>,
    is_key: bool,
) -> Result<String, InvalidOptionError> {
    let record_option_name = if is_key { "key.message" } else { "message" };
    let build_error_lack_field = || {
        invalid_option_error!(
            "{} requires a non-empty `{}` field",
            name_strategy.as_str_name(),
            record_option_name,
        )
    };
    match name_strategy {
        PbSchemaRegistryNameStrategy::Unspecified => {
            // default behavior
            let suffix = if is_key { "key" } else { "value" };
            Ok(format!("{topic}-{suffix}",))
        }
        PbSchemaRegistryNameStrategy::RecordNameStrategy => {
            let record_name = record.ok_or_else(build_error_lack_field)?;
            Ok(record_name.to_owned())
        }
        PbSchemaRegistryNameStrategy::TopicRecordNameStrategy => {
            let record_name = record.ok_or_else(build_error_lack_field)?;
            Ok(format!("{topic}-{record_name}"))
        }
    }
}
