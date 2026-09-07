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

use std::collections::BTreeMap;

use anyhow::Context as _;
use prost_reflect::{DescriptorPool, FileDescriptor, MessageDescriptor};
use prost_types::FileDescriptorSet;
use risingwave_connector_codec::common::protobuf::compile_pb;

use super::loader::{LoadedSchema, SchemaLoader};
use super::schema_registry::Subject;
use super::{
    InvalidOptionError, MESSAGE_NAME_KEY, SCHEMA_LOCATION_KEY, SCHEMA_REGISTRY_KEY,
    SchemaFetchError, invalid_option_error,
};
use crate::connector_common::AwsAuthProps;
use crate::parser::{EncodingProperties, ProtobufParserConfig, ProtobufProperties};

/// `aws_auth_props` is only required when reading `s3://` URL.
pub async fn fetch_descriptor(
    format_options: &BTreeMap<String, String>,
    topic: &str,
    aws_auth_props: Option<&AwsAuthProps>,
) -> Result<(MessageDescriptor, Option<i32>), SchemaFetchError> {
    let message_name = format_options
        .get(MESSAGE_NAME_KEY)
        .ok_or_else(|| invalid_option_error!("`{MESSAGE_NAME_KEY}` is required"))?
        .clone();
    let schema_location = format_options.get(SCHEMA_LOCATION_KEY);
    let schema_registry = format_options.get(SCHEMA_REGISTRY_KEY);
    let row_schema_location = match (schema_location, schema_registry) {
        (Some(_), Some(_)) => {
            return Err(invalid_option_error!(
                "cannot use {SCHEMA_LOCATION_KEY} and {SCHEMA_REGISTRY_KEY} together"
            )
            .into());
        }
        (None, None) => {
            return Err(invalid_option_error!(
                "requires one of {SCHEMA_LOCATION_KEY} or {SCHEMA_REGISTRY_KEY}"
            )
            .into());
        }
        (None, Some(_)) => {
            let (md, sid) = fetch_from_registry(&message_name, format_options, topic).await?;
            return Ok((md, Some(sid)));
        }
        (Some(url), None) => url.clone(),
    };

    if row_schema_location.starts_with("s3") && aws_auth_props.is_none() {
        return Err(invalid_option_error!("`s3://` schema locations are not supported yet").into());
    }

    let enc = EncodingProperties::Protobuf(ProtobufProperties {
        schema_location: crate::parser::SchemaLocation::File {
            url: row_schema_location,
            aws_auth_props: aws_auth_props.cloned(),
        },
        message_name,
        // name_strategy, topic, key_message_name, enable_upsert, client_config
        ..Default::default()
    });
    // Ideally, we should extract the schema loading logic from source parser to this place,
    // and call this in both source and sink.
    // But right now this function calls into source parser for its schema loading functionality.
    // This reversed dependency will be fixed when we support schema registry.
    let conf = ProtobufParserConfig::new(enc)
        .await
        .map_err(SchemaFetchError::YetToMigrate)?;
    Ok((conf.message_descriptor, None))
}

pub async fn fetch_from_registry(
    message_name: &str,
    format_options: &BTreeMap<String, String>,
    topic: &str,
) -> Result<(MessageDescriptor, i32), SchemaFetchError> {
    let loader = SchemaLoader::from_format_options(topic, format_options).await?;

    let (vid, vpb) = loader.load_val_schema::<FileDescriptor>().await?;
    let vid = match vid {
        super::SchemaVersion::Confluent(vid) => vid,
        super::SchemaVersion::Glue(_) => {
            return Err(invalid_option_error!(
                "Protobuf with Glue Schema Registry is not supported"
            )
            .into());
        }
    };
    let message_descriptor = vpb
        .parent_pool()
        .get_message_by_name(message_name)
        .ok_or_else(|| {
            invalid_option_error!("message `{message_name}` is not defined in the Protobuf schema")
        })?;

    Ok((message_descriptor, vid))
}

impl LoadedSchema for FileDescriptor {
    fn compile(primary: Subject, references: Vec<Subject>) -> Result<Self, SchemaFetchError> {
        let primary_name = primary.name.clone();

        match compile_pb_subject(primary, references)
            .context("failed to compile protobuf schema into fd set")
        {
            Err(e) => Err(SchemaFetchError::SchemaCompile(e.into())),
            Ok(fd_set) => DescriptorPool::from_file_descriptor_set(fd_set)
                .context("failed to convert fd set to descriptor pool")
                .and_then(|pool| {
                    pool.get_file_by_name(&primary_name)
                        .context("file lost after compilation")
                })
                .map_err(|e| SchemaFetchError::SchemaCompile(e.into())),
        }
    }
}

fn compile_pb_subject(
    primary_subject: Subject,
    dependency_subjects: Vec<Subject>,
) -> Result<FileDescriptorSet, SchemaFetchError> {
    compile_pb(
        (primary_subject.name.clone(), primary_subject.schema.content),
        dependency_subjects
            .into_iter()
            .map(|s| (s.name.clone(), s.schema.content)),
    )
    .map_err(|e| SchemaFetchError::SchemaCompile(e.into()))
}
