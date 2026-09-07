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

pub mod parser;
use std::collections::HashSet;
use std::sync::LazyLock;

use prost_reflect::{DynamicMessage, MessageDescriptor, ReflectMessage};
use risingwave_common::log::LogSuppressor;
use risingwave_common::types::{DataType, DatumCow};
use thiserror_ext::AsReport;

use super::{Access, AccessResult, uncategorized};

pub struct ProtobufAccess<'a> {
    message: DynamicMessage,
    reader_descriptor: MessageDescriptor,
    messages_as_jsonb: &'a HashSet<String>,
}

impl<'a> ProtobufAccess<'a> {
    pub fn new(
        message: DynamicMessage,
        reader_descriptor: MessageDescriptor,
        messages_as_jsonb: &'a HashSet<String>,
    ) -> Self {
        Self {
            message,
            reader_descriptor,
            messages_as_jsonb,
        }
    }

    #[cfg(test)]
    pub fn descriptor(&self) -> prost_reflect::MessageDescriptor {
        self.message.descriptor()
    }
}

impl Access for ProtobufAccess<'_> {
    fn access<'a>(&'a self, path: &[&str], type_expected: &DataType) -> AccessResult<DatumCow<'a>> {
        debug_assert_eq!(1, path.len());
        let reader_field = self.reader_descriptor.get_field_by_name(path[0]);
        let writer_descriptor = self.message.descriptor();
        let writer_field = match &reader_field {
            Some(reader_field) => writer_descriptor.get_field(reader_field.number()),
            // The catalog and the descriptor loaded when the parser starts can temporarily be
            // out of sync. Preserve the previous name-based behavior when there is no reader field
            // to provide a stable field number.
            None => writer_descriptor.get_field_by_name(path[0]),
        };

        let Some(writer_field) = writer_field else {
            if reader_field.is_some() {
                return Ok(DatumCow::NULL);
            }
            let error = uncategorized!("protobuf schema don't have field {}", path[0]);
            {
                static LOG_SUPPRESSOR: LazyLock<LogSuppressor> =
                    LazyLock::new(LogSuppressor::default);
                if let Ok(suppressed_count) = LOG_SUPPRESSOR.check() {
                    tracing::error!(suppressed_count, "{}", error.as_report());
                }
            }
            return Err(error);
        };

        parser::from_protobuf_message_field(
            &writer_field,
            reader_field.as_ref(),
            &self.message,
            type_expected,
            self.messages_as_jsonb,
        )
    }
}
