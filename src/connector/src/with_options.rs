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

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::marker::PhantomData;
use std::time::Duration;

use risingwave_pb::id::SecretId;
use risingwave_pb::secret::PbSecretRef;

use crate::error::ConnectorResult;
use crate::sink::catalog::SinkFormatDesc;
use crate::source::cdc::MYSQL_CDC_CONNECTOR;
use crate::source::cdc::external::ExternalCdcTableType;
use crate::source::iceberg::ICEBERG_CONNECTOR;
use crate::source::{
    ADBC_SNOWFLAKE_CONNECTOR, AZBLOB_CONNECTOR, BATCH_POSIX_FS_CONNECTOR, GCS_CONNECTOR,
    KAFKA_CONNECTOR, LEGACY_S3_CONNECTOR, OPENDAL_S3_CONNECTOR, POSIX_FS_CONNECTOR,
    PULSAR_CONNECTOR, UPSTREAM_SOURCE_KEY,
};

/// Marker trait for `WITH` options. Only for `#[derive(WithOptions)]`, should not be used manually.
///
/// This is used to ensure the `WITH` options types have reasonable structure.
///
/// TODO: add this bound for sink. There's a `SourceProperties` trait for sources, but no similar
/// things for sinks.
pub trait WithOptions {
    #[doc(hidden)]
    #[inline(always)]
    fn assert_receiver_is_with_options(&self) {}
}

// Currently CDC properties are handled specially.
// - It simply passes HashMap to Java DBZ.
// - It's not handled by serde.
// - It contains fields other than WITH options.
// TODO: remove the workaround here. And also use #[derive] for it.

impl<T: crate::source::cdc::CdcSourceTypeTrait> WithOptions
    for crate::source::cdc::CdcProperties<T>
{
}

// impl the trait for value types

impl<T: WithOptions> WithOptions for Option<T> {}
impl WithOptions for Vec<String> {}
impl WithOptions for Vec<u64> {}
impl WithOptions for HashMap<String, String> {}
impl WithOptions for BTreeMap<String, String> {}

impl WithOptions for String {}
impl WithOptions for bool {}
impl WithOptions for usize {}
impl WithOptions for u8 {}
impl WithOptions for u16 {}
impl WithOptions for u32 {}
impl WithOptions for u64 {}
impl WithOptions for i32 {}
impl WithOptions for i64 {}
impl WithOptions for f64 {}
impl WithOptions for std::time::Duration {}
impl WithOptions for crate::connector_common::MqttQualityOfService {}
impl WithOptions for crate::connector_common::SslMode {}
impl WithOptions for crate::sink::file_sink::opendal_sink::PathPartitionPrefix {}
impl WithOptions for crate::sink::kafka::CompressionCodec {}
impl WithOptions for crate::sink::pulsar::PulsarRoutingMode {}
impl WithOptions for crate::source::filesystem::file_common::CompressionFormat {}
impl WithOptions for nexmark::config::RateShape {}
impl WithOptions for nexmark::event::EventType {}
impl<T> WithOptions for PhantomData<T> {}

pub trait Get {
    fn get(&self, key: &str) -> Option<&String>;
}

pub trait GetKeyIter {
    fn key_iter(&self) -> impl Iterator<Item = &str>;
}

impl GetKeyIter for HashMap<String, String> {
    fn key_iter(&self) -> impl Iterator<Item = &str> {
        self.keys().map(|s| s.as_str())
    }
}

impl Get for HashMap<String, String> {
    fn get(&self, key: &str) -> Option<&String> {
        self.get(key)
    }
}

impl Get for BTreeMap<String, String> {
    fn get(&self, key: &str) -> Option<&String> {
        self.get(key)
    }
}

impl GetKeyIter for BTreeMap<String, String> {
    fn key_iter(&self) -> impl Iterator<Item = &str> {
        self.keys().map(|s| s.as_str())
    }
}

/// Utility methods for `WITH` properties (`HashMap` and `BTreeMap`).
pub trait WithPropertiesExt: Get + GetKeyIter + Sized {
    #[inline(always)]
    fn get_connector(&self) -> Option<String> {
        self.get(UPSTREAM_SOURCE_KEY).map(|s| s.to_lowercase())
    }

    #[inline(always)]
    fn is_kafka_connector(&self) -> bool {
        let Some(connector) = self.get_connector() else {
            return false;
        };
        connector == KAFKA_CONNECTOR
    }

    #[inline(always)]
    fn is_pulsar_connector(&self) -> bool {
        let Some(connector) = self.get_connector() else {
            return false;
        };
        connector == PULSAR_CONNECTOR
    }

    #[inline(always)]
    fn is_mysql_cdc_connector(&self) -> bool {
        let Some(connector) = self.get_connector() else {
            return false;
        };
        connector == MYSQL_CDC_CONNECTOR
    }

    #[inline(always)]
    fn get_sync_call_timeout(&self) -> Option<Duration> {
        const SYNC_CALL_TIMEOUT_KEY: &str = "properties.sync.call.timeout"; // only from kafka props, add more if needed
        self.get(SYNC_CALL_TIMEOUT_KEY)
            // ignore the error is ok here, because we will parse the field again when building the properties and has more precise error message
            .and_then(|s| duration_str::parse_std(s).ok())
    }

    #[inline(always)]
    fn is_cdc_connector(&self) -> bool {
        let Some(connector) = self.get_connector() else {
            return false;
        };
        connector.contains("-cdc")
    }

    /// It is shared when `CREATE SOURCE`, and not shared when `CREATE TABLE`. So called "shareable".
    fn is_shareable_cdc_connector(&self) -> bool {
        self.is_cdc_connector() && ExternalCdcTableType::from_properties(self).can_backfill()
    }

    fn enable_transaction_metadata(&self) -> bool {
        ExternalCdcTableType::from_properties(self).enable_transaction_metadata()
    }

    fn is_shareable_non_cdc_connector(&self) -> bool {
        self.is_kafka_connector()
    }

    #[inline(always)]
    fn is_iceberg_connector(&self) -> bool {
        let Some(connector) = self.get_connector() else {
            return false;
        };
        connector == ICEBERG_CONNECTOR
    }

    fn connector_need_pk(&self) -> bool {
        // Currently only iceberg connector doesn't need primary key
        // introduced in https://github.com/risingwavelabs/risingwave/pull/14971
        // XXX: This seems not the correct way. Iceberg doesn't necessarily lack a PK.
        // "batch source" doesn't need a PK?
        // For streaming, if it has a PK, do we want to use it? It seems not safe.
        !self.is_iceberg_connector()
    }

    fn is_legacy_fs_connector(&self) -> bool {
        self.get(UPSTREAM_SOURCE_KEY)
            .map(|s| s.eq_ignore_ascii_case(LEGACY_S3_CONNECTOR))
            .unwrap_or(false)
    }

    fn is_new_fs_connector(&self) -> bool {
        self.get(UPSTREAM_SOURCE_KEY)
            .map(|s| {
                s.eq_ignore_ascii_case(OPENDAL_S3_CONNECTOR)
                    || s.eq_ignore_ascii_case(POSIX_FS_CONNECTOR)
                    || s.eq_ignore_ascii_case(GCS_CONNECTOR)
                    || s.eq_ignore_ascii_case(AZBLOB_CONNECTOR)
            })
            .unwrap_or(false)
    }

    fn is_batch_connector(&self) -> bool {
        self.get(UPSTREAM_SOURCE_KEY)
            .map(|s| {
                s.eq_ignore_ascii_case(BATCH_POSIX_FS_CONNECTOR)
                    || s.eq_ignore_ascii_case(ADBC_SNOWFLAKE_CONNECTOR)
            })
            .unwrap_or(false)
    }

    fn supports_full_reload_refresh(&self) -> bool {
        self.get(UPSTREAM_SOURCE_KEY)
            .map(|s| {
                s.eq_ignore_ascii_case(OPENDAL_S3_CONNECTOR)
                    || s.eq_ignore_ascii_case(GCS_CONNECTOR)
                    || s.eq_ignore_ascii_case(BATCH_POSIX_FS_CONNECTOR)
                    || s.eq_ignore_ascii_case(ICEBERG_CONNECTOR)
                    || s.eq_ignore_ascii_case(ADBC_SNOWFLAKE_CONNECTOR)
            })
            .unwrap_or(false)
    }

    fn requires_singleton(&self) -> bool {
        self.is_new_fs_connector() || self.is_iceberg_connector() || self.is_batch_connector()
    }
}

impl<T: Get + GetKeyIter> WithPropertiesExt for T {}

/// Options or properties extracted from the `WITH` clause of DDLs.
#[derive(Default, Clone, Debug, PartialEq, Eq, Hash)]
pub struct WithOptionsSecResolved {
    inner: BTreeMap<String, String>,
    secret_ref: BTreeMap<String, PbSecretRef>,
}

impl std::ops::Deref for WithOptionsSecResolved {
    type Target = BTreeMap<String, String>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl std::ops::DerefMut for WithOptionsSecResolved {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl WithOptionsSecResolved {
    /// Create a new [`WithOptions`] from a option [`BTreeMap`] and resolved secret ref.
    pub fn new(inner: BTreeMap<String, String>, secret_ref: BTreeMap<String, PbSecretRef>) -> Self {
        Self { inner, secret_ref }
    }

    pub fn as_plaintext(&self) -> &BTreeMap<String, String> {
        &self.inner
    }

    pub fn as_secret(&self) -> &BTreeMap<String, PbSecretRef> {
        &self.secret_ref
    }

    pub fn handle_update(
        &mut self,
        update_alter_props: BTreeMap<String, String>,
        update_alter_secret_refs: BTreeMap<String, PbSecretRef>,
    ) -> ConnectorResult<(Vec<SecretId>, Vec<SecretId>)> {
        let old_secret_ids = self
            .secret_ref
            .values()
            .map(|secret_ref| secret_ref.secret_id)
            .collect::<BTreeSet<_>>();

        // make sure the key in update_alter_props and update_alter_secret_refs not collide
        for key in update_alter_props.keys() {
            if update_alter_secret_refs.contains_key(key) {
                return Err(
                    anyhow::anyhow!("the key {} is set both in plaintext and secret", key).into(),
                );
            }
        }

        // remove legacy key if it's set in both plaintext and secret
        // When a property changes from secret to plaintext, remove the old secret dependency
        for k in update_alter_props.keys() {
            self.secret_ref.remove(k);
        }

        // Handle secret ref updates
        for k in update_alter_secret_refs.keys() {
            // Remove any plaintext value for this key
            self.inner.remove(k);
        }

        self.inner.extend(update_alter_props);
        self.secret_ref.extend(update_alter_secret_refs);

        let new_secret_ids = self
            .secret_ref
            .values()
            .map(|secret_ref| secret_ref.secret_id)
            .collect::<BTreeSet<_>>();
        let to_add_secret_dep = new_secret_ids
            .difference(&old_secret_ids)
            .copied()
            .collect();
        let to_remove_secret_dep = old_secret_ids
            .difference(&new_secret_ids)
            .copied()
            .collect();

        Ok((to_add_secret_dep, to_remove_secret_dep))
    }

    /// Create a new [`WithOptions`] from a [`BTreeMap`].
    pub fn without_secrets(inner: BTreeMap<String, String>) -> Self {
        Self {
            inner,
            secret_ref: Default::default(),
        }
    }

    /// Take the value of the option map and secret refs.
    pub fn into_parts(self) -> (BTreeMap<String, String>, BTreeMap<String, PbSecretRef>) {
        (self.inner, self.secret_ref)
    }

    pub fn value_eq_ignore_case(&self, key: &str, val: &str) -> bool {
        if let Some(inner_val) = self.inner.get(key)
            && inner_val.eq_ignore_ascii_case(val)
        {
            return true;
        }
        false
    }
}

/// For `planner_test` crate so that it does not depend directly on `connector` crate just for `SinkFormatDesc`.
impl TryFrom<&WithOptionsSecResolved> for Option<SinkFormatDesc> {
    type Error = crate::sink::SinkError;

    fn try_from(value: &WithOptionsSecResolved) -> std::result::Result<Self, Self::Error> {
        let connector = value.get(crate::sink::CONNECTOR_TYPE_KEY);
        let r#type = value.get(crate::sink::SINK_TYPE_OPTION);
        match (connector, r#type) {
            (Some(c), Some(t)) => SinkFormatDesc::from_legacy_type(c, t),
            _ => Ok(None),
        }
    }
}

impl Get for WithOptionsSecResolved {
    fn get(&self, key: &str) -> Option<&String> {
        self.inner.get(key)
    }
}

impl GetKeyIter for WithOptionsSecResolved {
    fn key_iter(&self) -> impl Iterator<Item = &str> {
        self.inner.keys().map(|s| s.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source_options(connector: &str) -> WithOptionsSecResolved {
        WithOptionsSecResolved::without_secrets(BTreeMap::from([(
            UPSTREAM_SOURCE_KEY.to_owned(),
            connector.to_owned(),
        )]))
    }

    #[test]
    fn test_full_reload_refresh_connector_whitelist() {
        for connector in [
            OPENDAL_S3_CONNECTOR,
            GCS_CONNECTOR,
            BATCH_POSIX_FS_CONNECTOR,
            ICEBERG_CONNECTOR,
            ADBC_SNOWFLAKE_CONNECTOR,
        ] {
            assert!(
                source_options(connector).supports_full_reload_refresh(),
                "{connector} should support FULL_RELOAD refresh"
            );
        }

        for connector in [KAFKA_CONNECTOR, POSIX_FS_CONNECTOR, AZBLOB_CONNECTOR] {
            assert!(
                !source_options(connector).supports_full_reload_refresh(),
                "{connector} should not support FULL_RELOAD refresh"
            );
        }
    }
}
