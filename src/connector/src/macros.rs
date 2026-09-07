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

#[macro_export]
macro_rules! for_all_classified_sources {
    ($macro:path $(, $extra_args:tt)*) => {
        $macro! {
            // cdc sources
            {
                { Mysql },
                { Postgres },
                { Citus },
                { Mongodb },
                { SqlServer }
            },
            // other sources
            // todo: file source do not nest with mq source.
            {
                { Kafka, $crate::source::kafka::KafkaProperties, $crate::source::kafka::KafkaSplit },
                { Pulsar, $crate::source::pulsar::PulsarProperties, $crate::source::pulsar::PulsarSplit },
                { Kinesis, $crate::source::kinesis::KinesisProperties, $crate::source::kinesis::split::KinesisSplit },
                { Nexmark, $crate::source::nexmark::NexmarkProperties, $crate::source::nexmark::NexmarkSplit },
                { Datagen, $crate::source::datagen::DatagenProperties, $crate::source::datagen::DatagenSplit },
                { GooglePubsub, $crate::source::google_pubsub::PubsubProperties, $crate::source::google_pubsub::PubsubSplit },
                { Mqtt, $crate::source::mqtt::MqttProperties, $crate::source::mqtt::split::MqttSplit },
                { Nats, $crate::source::nats::NatsProperties, $crate::source::nats::split::NatsSplit },
                { S3, $crate::source::filesystem::LegacyS3Properties, $crate::source::filesystem::LegacyFsSplit },
                { Gcs, $crate::source::filesystem::opendal_source::GcsProperties , $crate::source::filesystem::OpendalFsSplit<$crate::source::filesystem::opendal_source::OpendalGcs> },
                { OpendalS3, $crate::source::filesystem::opendal_source::OpendalS3Properties, $crate::source::filesystem::OpendalFsSplit<$crate::source::filesystem::opendal_source::OpendalS3> },
                { PosixFs, $crate::source::filesystem::opendal_source::PosixFsProperties, $crate::source::filesystem::OpendalFsSplit<$crate::source::filesystem::opendal_source::OpendalPosixFs> },
                { BatchPosixFs, $crate::source::filesystem::opendal_source::BatchPosixFsProperties, $crate::source::filesystem::opendal_source::BatchPosixFsSplit },
                { Azblob, $crate::source::filesystem::opendal_source::AzblobProperties, $crate::source::filesystem::OpendalFsSplit<$crate::source::filesystem::opendal_source::OpendalAzblob> },
                { Test, $crate::source::test_source::TestSourceProperties, $crate::source::test_source::TestSourceSplit},
                { Iceberg, $crate::source::iceberg::IcebergProperties, $crate::source::iceberg::IcebergSplit},
                { AdbcSnowflake, $crate::source::adbc_snowflake::AdbcSnowflakeProperties, $crate::source::adbc_snowflake::AdbcSnowflakeSplit}
            }
            $(
                ,$extra_args
            )*
        }
    };
}

#[macro_export]
macro_rules! for_all_connections {
    ($macro:path $(, $extra_args:tt)*) => {
        $macro! {
            {
                { Kafka, $crate::connector_common::KafkaConnection, risingwave_pb::catalog::connection_params::PbConnectionType, "kafka"},
                { Iceberg, $crate::connector_common::IcebergConnection, risingwave_pb::catalog::connection_params::PbConnectionType, "iceberg"},
                { SchemaRegistry, $crate::connector_common::ConfluentSchemaRegistryConnection, risingwave_pb::catalog::connection_params::PbConnectionType, "schema_registry"},
                { Elasticsearch, $crate::connector_common::ElasticsearchConnection, risingwave_pb::catalog::connection_params::PbConnectionType, "elasticsearch"}
            }
            $(,$extra_args)*
        }
    };
}

#[macro_export]
macro_rules! for_all_sources_inner {
    (
        { $({ $cdc_source_type:ident }),* },
        { $({ $source_variant:ident, $prop_name:ty, $split:ty }),* },
        $macro:tt $(, $extra_args:tt)*
    ) => {
        $crate::paste! {
            $macro! {
                {
                    $(
                        {
                            [< $cdc_source_type Cdc >],
                            $crate::source::cdc::[< $cdc_source_type CdcProperties >],
                            $crate::source::cdc::DebeziumCdcSplit<$crate::source::cdc::$cdc_source_type>
                        }
                    ),*
                    ,
                    $(
                        { $source_variant, $prop_name, $split }
                    ),*
                }
                $(, $extra_args)*
            }
        }
    };
}

#[macro_export]
macro_rules! for_all_sources {
    ($macro:path $(, $arg:tt )*) => {
        $crate::for_all_classified_sources! {$crate::for_all_sources_inner, $macro $(,$arg)* }
    };
}

/// The invocation:
/// ```ignore
/// dispatch_source_enum_inner!(
///     {
///         {A1,B1,C1},
///         {A2,B2,C2}
///     },
///     EnumType, enum_value, inner_ident, body
/// );
/// ```
/// expands to:
/// ```ignore
/// match enum_value {
///     EnumType::A1(inner_ident) => {
///         #[allow(dead_code)]
///         type PropType = B1;
///         #[allow(dead_code)]
///         type SplitType = C1;
///         {
///             body
///         }
///     }
///     EnumType::A2(inner_ident) => {
///         #[allow(dead_code)]
///         type PropType = B2;
///         #[allow(dead_code)]
///         type SplitType = C2;
///         {
///             body
///         }
///     }
/// }
/// ```
#[macro_export]
macro_rules! dispatch_source_enum_inner {
    (
        {$({$source_variant:ident, $prop_name:ty, $split:ty }),*},
        $enum_type:ident,
        $enum_value:expr,
        $inner_name:ident,
        $body:expr
    ) => {{
        match $enum_value {
            $(
                $enum_type::$source_variant($inner_name) => {
                    #[allow(dead_code)]
                    type PropType = $prop_name;
                    #[allow(dead_code)]
                    type SplitType = $split;
                    {
                        $body
                    }
                },
            )*
        }
    }}
}

/// Usage: `dispatch_source_enum!(EnumType, enum_value, |inner_ident| body)`.
///
/// Inside `body`:
/// - use `inner_ident` to represent the matched variant.
/// - use `PropType` to represent the concrete property type.
/// - use `SplitType` to represent the concrete split type.
///
/// Expands to:
/// ```ignore
/// match enum_value {
///     EnumType::Variant1(inner_ident) => {
///         body
///     }
///     ...
/// }
/// ```
///
/// Note: `inner_ident` must be passed as an argument due to macro hygiene.
#[macro_export]
macro_rules! dispatch_source_enum {
    ($enum_type:ident, $enum_value:expr, |$inner_name:ident| $body:expr) => {{
        $crate::for_all_sources! {$crate::dispatch_source_enum_inner, $enum_type, { $enum_value }, $inner_name, $body}
    }};
}

#[macro_export]
macro_rules! match_source_name_str_inner {
    (
        {$({$source_variant:ident, $prop_name:ty, $split:ty }),*},
        $source_name_str:expr,
        $prop_type_name:ident,
        $body:expr,
        $on_other_closure:expr
    ) => {{
        match $source_name_str {
            $(
                <$prop_name>::SOURCE_NAME => {
                    type $prop_type_name = $prop_name;
                    {
                        $body
                    }
                },
            )*
            other => ($on_other_closure)(other),
        }
    }}
}

/// Matches against `SourceProperties::SOURCE_NAME` to dispatch logic.
#[macro_export]
macro_rules! match_source_name_str {
    ($source_name_str:expr, $prop_type_name:ident, $body:expr, $on_other_closure:expr) => {{
        $crate::for_all_sources! {
            $crate::match_source_name_str_inner,
            { $source_name_str },
            $prop_type_name,
            { $body },
            { $on_other_closure }
        }
    }};
}

/// [`dispatch_source_enum`] with `SplitImpl` as the enum type.
#[macro_export]
macro_rules! dispatch_split_impl {
    ($impl:expr, | $inner_name:ident | $body:expr) => {{
        use $crate::source::SplitImpl;
        $crate::dispatch_source_enum! {SplitImpl, { $impl }, |$inner_name| $body}
    }};
}

#[macro_export]
macro_rules! impl_connection {
    ({$({ $variant_name:ident, $connection_type:ty, $pb_connection_path:path, $connection_type_name:literal }),*}) => {
        // impl the connection name for all connections
        pub fn connection_name_to_prop_type_name(connection_name: &str) -> Option<&'static str> {
            match connection_name {
                $(
                    $connection_type_name => Some(
                        std::any::type_name::<$connection_type>()
                    ),
                )*
                _ => None,
            }
        }

        pub fn pb_connection_type_to_connection_type(pb_connection_type: &risingwave_pb::catalog::connection_params::PbConnectionType) -> Option<&'static str> {
            match pb_connection_type {
                $(
                    risingwave_pb::catalog::connection_params::PbConnectionType::$variant_name => Some($connection_type_name),
                )*
                _ => None,
            }
        }

        pub fn build_connection(
            pb_connection_type: risingwave_pb::catalog::connection_params::PbConnectionType,
            value_secret_filled: std::collections::BTreeMap<String, String>
        ) -> $crate::error::ConnectorResult<Box<dyn $crate::connector_common::Connection>> {
            match pb_connection_type {
                $(
                    <$pb_connection_path>::$variant_name => {
                        let c: Box<$connection_type> = serde_json::from_value(json!(value_secret_filled)).map_err($crate::error::ConnectorError::from)?;
                        Ok(c)
                    },
                )*
                risingwave_pb::catalog::connection_params::PbConnectionType::Unspecified => unreachable!(),
            }
        }

        pub fn enforce_secret_connection<'a>(
            pb_connection_type: &risingwave_pb::catalog::connection_params::PbConnectionType,
            prop_iter: impl Iterator<Item = &'a str>,
        ) -> $crate::error::ConnectorResult<()> {
            match pb_connection_type {
                $(
                    <$pb_connection_path>::$variant_name => {
                        <$connection_type>::enforce_secret(prop_iter)
                    },
                )*
                risingwave_pb::catalog::connection_params::PbConnectionType::Unspecified => unreachable!(),
            }
        }
    }
}

#[macro_export]
macro_rules! impl_split {
    ({$({ $variant_name:ident, $prop_name:ty, $split:ty}),*}) => {

        #[derive(Debug, Clone, EnumAsInner, PartialEq)]
        pub enum SplitImpl {
            $(
                $variant_name($split),
            )*
        }

        $(
            impl TryFrom<SplitImpl> for $split {
                type Error = $crate::error::ConnectorError;

                fn try_from(split: SplitImpl) -> std::result::Result<Self, Self::Error> {
                    match split {
                        SplitImpl::$variant_name(inner) => Ok(inner),
                        other => risingwave_common::bail!(
                            "expected {} but got {:?}",
                            stringify!($split),
                            other
                        ),
                    }
                }
            }

            impl From<$split> for SplitImpl {
                fn from(split: $split) -> SplitImpl {
                    SplitImpl::$variant_name(split)
                }
            }

        )*
    }
}

/// [`dispatch_source_enum`] with `ConnectorProperties` as the enum type.
#[macro_export]
macro_rules! dispatch_source_prop {
    ($connector_properties:expr, |$inner_ident:ident| $body:expr) => {{
        use $crate::source::ConnectorProperties;
        $crate::dispatch_source_enum! {ConnectorProperties, { $connector_properties }, |$inner_ident| {$body}}
    }};
}

#[macro_export]
macro_rules! impl_connector_properties {
    ({$({ $variant_name:ident, $prop_name:ty, $split:ty}),*}) => {
        #[derive(Clone, Debug)]
        pub enum ConnectorProperties {
            $(
                $variant_name(Box<$prop_name>),
            )*
        }

        impl ConnectorProperties {
            pub fn kind(&self) -> &'static str {
                match self {
                    $(
                        ConnectorProperties::$variant_name(_) => stringify!($variant_name),
                    )*
                }
            }
        }

        $(
            impl From<$prop_name> for ConnectorProperties {
                fn from(prop: $prop_name) -> ConnectorProperties {
                    ConnectorProperties::$variant_name(Box::new(prop))
                }
            }
        )*
    }
}

#[macro_export]
macro_rules! impl_cdc_source_type {
    (
        {$({$cdc_source_type:tt}),*},
        {$($_ignore:tt),*}
    ) => {
        $(
            $crate::paste!{
                #[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
                pub struct $cdc_source_type;
                impl CdcSourceTypeTrait for $cdc_source_type {
                    const CDC_CONNECTOR_NAME: &'static str = concat!(stringify!([<$cdc_source_type:lower>]), "-cdc");
                    fn source_type() -> CdcSourceType {
                        CdcSourceType::$cdc_source_type
                    }
                }
                pub type [<$cdc_source_type CdcProperties>] = CdcProperties<$cdc_source_type>;
            }
        )*

        #[derive(Clone)]
        pub enum CdcSourceType {
            $(
                $cdc_source_type,
            )*
            Unspecified,
        }

        impl From<PbSourceType> for CdcSourceType {
            fn from(value: PbSourceType) -> Self {
                match value {
                    PbSourceType::Unspecified => CdcSourceType::Unspecified,
                    $(
                        PbSourceType::$cdc_source_type => CdcSourceType::$cdc_source_type,
                    )*
                }
            }
        }

        impl From<CdcSourceType> for PbSourceType {
            fn from(this: CdcSourceType) -> PbSourceType {
                match this {
                    $(
                        CdcSourceType::$cdc_source_type => PbSourceType::$cdc_source_type,
                    )*
                   CdcSourceType::Unspecified => PbSourceType::Unspecified,
                }
            }
        }

    }
}
