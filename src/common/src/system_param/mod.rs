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

//! This module defines utilities to work with system parameters ([`PbSystemParams`] in
//! `meta.proto`).
//!
//! To add a new system parameter:
//! - Add a new field to [`PbSystemParams`] in `meta.proto`.
//! - Add a new entry to `for_all_params` in this file.
//! - Add a new method to [`reader::SystemParamsReader`].

pub mod adaptive_parallelism_strategy;
pub mod common;
pub mod diff;
pub mod local_manager;
pub mod reader;
mod state_store_url;

use std::fmt::Debug;
use std::ops::RangeBounds;
use std::str::FromStr;

use paste::paste;
use risingwave_license::{LicenseKey, LicenseKeyRef};
use risingwave_pb::meta::PbSystemParams;
pub use state_store_url::{StateStoreUrl, StateStoreUrlRef};

use self::diff::SystemParamsDiff;
pub use crate::system_param::adaptive_parallelism_strategy::AdaptiveParallelismStrategy;

pub type SystemParamsError = String;

type Result<T> = core::result::Result<T, SystemParamsError>;

/// The trait for the value type of a system parameter.
pub trait ParamValue: ToString + FromStr {
    type Borrowed<'a>;
}

macro_rules! impl_param_value {
    ($type:ty) => {
        impl_param_value!($type => $type);
    };
    ($type:ty => $borrowed:ty) => {
        impl ParamValue for $type {
            type Borrowed<'a> = $borrowed;
        }
    };
}

impl_param_value!(bool);
impl_param_value!(u32);
impl_param_value!(u64);
impl_param_value!(f64);
impl_param_value!(String => &'a str);
impl_param_value!(LicenseKey => LicenseKeyRef<'a>);
impl_param_value!(StateStoreUrl => StateStoreUrlRef<'a>);

/// Define all system parameters here.
///
/// To match all these information, write the match arm as follows:
/// ```text
/// ($({ $field:ident, $type:ty, $default:expr, $is_mutable:expr, $doc:literal, $($rest:tt)* },)*) => {
/// ```
///
/// Note:
/// - Having `None` as default value means the parameter must be initialized.
#[macro_export]
macro_rules! for_all_params {
    ($macro:ident) => {
        $macro! {
            // name                                     type                            default value                   mut?    doc
            { barrier_interval_ms,                      u32,                            Some(1000_u32),                 true,   "The interval of periodic barrier.", },
            { checkpoint_frequency,                     u64,                            Some(1_u64),                    true,   "There will be a checkpoint for every n barriers.", },
            { sstable_size_mb,                          u32,                            Some(256_u32),                  false,  "Target size of the Sstable.", },
            { parallel_compact_size_mb,                 u32,                            Some(512_u32),                  false,  "The size of parallel task for one compact/flush job.", },
            { block_size_kb,                            u32,                            Some(64_u32),                   false,  "Size of each block in bytes in SST.", },
            { bloom_false_positive,                     f64,                            Some(0.001_f64),                false,  "DEPRECATED: Bloom filter is no longer a supported SST filter implementation. This field is kept for backward compatibility and no longer controls whether SST filters are emitted.", },
            { state_store,                              $crate::system_param::StateStoreUrl, None,                       false,  "URL for the state store", },
            { data_directory,                           String,                         None,                           false,  "Remote directory for storing data and metadata objects.", },
            { backup_storage_url,                       String,                         None,                           true,   "Remote storage url for storing snapshots.", },
            { backup_storage_directory,                 String,                         None,                           true,   "Remote directory for storing snapshots.", },
            { max_concurrent_creating_streaming_jobs,   u32,                            Some(1_u32),                    true,   "Max number of concurrent creating streaming jobs.", },
            { pause_on_next_bootstrap,                  bool,                           Some(false),                    true,   "Whether to pause all data sources on next bootstrap.", },
            { enable_tracing,                           bool,                           Some(false),                    true,   "Whether to enable distributed tracing.", },
            { use_new_object_prefix_strategy,           bool,                           None,                           false,  "Whether to split object prefix.", },
            { license_key,                              risingwave_license::LicenseKey, Some(Default::default()),       true,   "The license key to activate enterprise features.", },
            { time_travel_retention_ms,                 u64,                            Some(600000_u64),               true,   "The data retention period for time travel.", },
            { per_database_isolation,                   bool,                           Some(true),                     true,   "Whether per database isolation is enabled", },
            { enforce_secret,                  bool,                           Some(false),                    true,   "Whether to enforce secret on cloud.", },
        }
    };
}

// Warn user if barrier_interval_ms is set above 5mins.
pub const NOTICE_BARRIER_INTERVAL_MS: u32 = 300000;
// Warn user if checkpoint_frequency is set above 60.
pub const NOTICE_CHECKPOINT_FREQUENCY: u64 = 60;

/// Convert field name to string.
#[macro_export]
macro_rules! key_of {
    ($field:ident) => {
        stringify!($field)
    };
}

/// Define key constants for fields in `PbSystemParams` for use of other modules.
macro_rules! def_key {
    ($({ $field:ident, $($rest:tt)* },)*) => {
        paste! {
            $(
                pub const [<$field:upper _KEY>]: &str = key_of!($field);
            )*
        }
    };
}

for_all_params!(def_key);

/// Define default value functions returning `Option`.
macro_rules! def_default_opt {
    ($({ $field:ident, $type:ty, $default: expr, $($rest:tt)* },)*) => {
        $(
            paste::paste!(
                pub fn [<$field _opt>]() -> Option<$type> {
                    $default
                }
            );
        )*
    };
}

/// Define default value functions for those with `Some` default values.
macro_rules! def_default {
    ($({ $field:ident, $type:ty, $default: expr, $($rest:tt)* },)*) => {
        $(
            def_default!(@ $field, $type, $default);
        )*
    };
    (@ $field:ident, $type:ty, None) => {};
    (@ $field:ident, $type:ty, $default: expr) => {
        pub fn $field() -> $type {
            $default.unwrap()
        }
        paste::paste!(
            pub static [<$field:upper>]: LazyLock<$type> = LazyLock::new($field);
        );
    };
}

/// Default values for all parameters.
pub mod default {
    use std::sync::LazyLock;

    for_all_params!(def_default_opt);
    for_all_params!(def_default);
}

macro_rules! impl_check_missing_fields {
    ($({ $field:ident, $($rest:tt)* },)*) => {
        /// Check if any undeprecated fields are missing.
        #[expect(deprecated)]
        pub fn check_missing_params(params: &PbSystemParams) -> Result<()> {
            $(
                if params.$field.is_none() {
                    return Err(format!("missing system param {:?}", key_of!($field)));
                }
            )*
            Ok(())
        }
    };
}

/// Derive serialization to kv pairs.
macro_rules! impl_system_params_to_kv {
    ($({ $field:ident, $($rest:tt)* },)*) => {
        /// The returned map only contains undeprecated fields.
        /// Return error if there are missing fields.
        #[allow(clippy::vec_init_then_push)]
        #[expect(deprecated)]
        pub fn system_params_to_kv(params: &PbSystemParams) -> Result<Vec<(String, String)>> {
            check_missing_params(params)?;
            let mut ret = Vec::new();
            $(ret.push((
                key_of!($field).to_owned(),
                params.$field.as_ref().unwrap().to_string(),
            ));)*
            Ok(ret)
        }
    };
}

macro_rules! impl_derive_missing_fields {
    ($({ $field:ident, $($rest:tt)* },)*) => {
        #[expect(deprecated)]
        pub fn derive_missing_fields(params: &mut PbSystemParams) {
            $(
                if params.$field.is_none() && let Some(v) = OverrideFromParams::$field(params) {
                    params.$field = Some(v.into());
                }
            )*
        }
    };
}

/// Derive deserialization from kv pairs.
macro_rules! impl_system_params_from_kv {
    ($({ $field:ident, $($rest:tt)* },)*) => {
        /// Try to deserialize deprecated fields as well.
        /// Return error if there are unrecognized fields.
        #[expect(deprecated)]
        pub fn system_params_from_kv<K, V>(mut kvs: Vec<(K, V)>) -> Result<PbSystemParams>
        where
            K: AsRef<[u8]> + Debug,
            V: AsRef<[u8]> + Debug,
        {
            let mut ret = PbSystemParams::default();
            kvs.retain(|(k,v)| {
                let k = std::str::from_utf8(k.as_ref()).unwrap();
                let v = std::str::from_utf8(v.as_ref()).unwrap();
                match k {
                    $(
                        key_of!($field) => {
                            ret.$field = Some(v.parse().unwrap());
                            false
                        }
                    )*
                    _ => {
                        true
                    }
                }
            });
            derive_missing_fields(&mut ret);
            if !kvs.is_empty() {
                let unrecognized_params = kvs.into_iter().map(|(k, v)| {
                    (
                        std::str::from_utf8(k.as_ref()).unwrap().to_owned(),
                        std::str::from_utf8(v.as_ref()).unwrap().to_owned(),
                    )
                }).collect::<Vec<_>>();
                tracing::warn!("unrecognized system params {:?}", unrecognized_params);
            }
            Ok(ret)
        }
    };
}

/// Define check rules when a field is changed.
/// If you want custom rules, please override the default implementation in
/// `OverrideValidateOnSet` below.
macro_rules! impl_default_validation {
    ($({ $field:ident, $type:ty, $default:expr, $is_mutable:expr, $($rest:tt)* },)*) => {
        #[allow(clippy::ptr_arg)]
        pub trait Validate {
            $(
                /// Default implementation does nothing.
                /// Specific checks are implemented in `OverrideValidate`.
                fn $field(_v: &$type) -> Result<()> {
                    Ok(())
                }
            )*

            fn expect_range<T, R>(v: T, range: R) -> Result<()>
            where
                T: Debug + PartialOrd,
                R: RangeBounds<T> + Debug,
            {
                if !range.contains::<T>(&v) {
                    Err(format!("value {:?} out of range, expect {:?}", v, range))
                } else {
                    Ok(())
                }
            }
        }
    }
}

/// Define rules to derive a parameter from others. This is useful for parameter type change or
/// semantic change, where a new parameter has to be introduced. When the cluster upgrades to a
/// newer version, we need to ensure the effect of the new parameter is equal to its older versions.
/// For example, if you had `interval_sec` and now you want finer granularity, you can introduce a
/// new param `interval_ms` and try to derive it from `interval_sec` by overriding `FromParams`
/// trait in `OverrideFromParams`:
///
/// ```ignore
/// impl FromParams for OverrideFromParams {
///     fn interval_ms(params: &PbSystemParams) -> Option<u64> {
///         if let Some(sec) = params.interval_sec {
///             Some(sec * 1000)
///         } else {
///             None
///         }
///     }
/// }
/// ```
///
/// Note that newer versions must be prioritized during derivation.
macro_rules! impl_default_from_other_params {
    ($({ $field:ident, $type:ty, $($rest:tt)* },)*) => {
        trait FromParams {
            $(
                fn $field(_params: &PbSystemParams) -> Option<$type> {
                    None
                }
            )*
        }
    };
}

macro_rules! impl_set_system_param {
    ($({ $field:ident, $type:ty, $default:expr, $is_mutable:expr, $($rest:tt)* },)*) => {
        /// Set a system parameter with the given value or default one.
        ///
        /// Returns the new value if changed, or an error if the parameter is unrecognized,
        /// immutable, or the value is invalid.
        #[expect(deprecated)]
        pub fn set_system_param(
            params: &mut PbSystemParams,
            key: &str,
            value: Option<impl AsRef<str>>,
        ) -> Result<Option<(String, SystemParamsDiff)>> {
            use crate::system_param::reader::{SystemParamsReader, SystemParamsRead};

            match key {
                $(
                    key_of!($field) => {
                        if !$is_mutable {
                            return Err(format!("{:?} is immutable", key_of!($field)));
                        }

                        let v: $type = if let Some(v) = value {
                            #[allow(rw::format_error)]
                            v.as_ref().parse().map_err(|e| format!("cannot parse parameter value: {e}"))?
                        } else {
                            $default.ok_or_else(|| format!("{} does not have a default value", key))?
                        };
                        OverrideValidate::$field(&v)?;

                        let changed = SystemParamsReader::new(&*params).$field() != v;
                        if changed {
                            let diff = SystemParamsDiff {
                                $field: Some(v.to_owned()),
                                ..Default::default()
                            };
                            params.$field = Some(v.into());                                 // do not use `to_string` to avoid writing redacted values
                            let new_value = params.$field.as_ref().unwrap().to_string();    // can now use `to_string` on protobuf primitive types
                            Ok(Some((new_value, diff)))
                        } else {
                            Ok(None)
                        }
                    },
                )*
                _ => {
                    Err(format!(
                        "unrecognized system parameter {:?}",
                        key
                    ))
                }
            }
        }
    };
}

macro_rules! impl_is_mutable {
    ($({ $field:ident, $type:ty, $default:expr, $is_mutable:expr, $($rest:tt)* },)*) => {
        pub fn is_mutable(field: &str) -> Result<bool> {
            match field {
                $(
                    key_of!($field) => Ok($is_mutable),
                )*
                _ => Err(format!("{:?} is not a system parameter", field))
            }
        }
    }
}

macro_rules! impl_system_params_for_test {
    ($({ $field:ident, $type:ty, $default:expr, $($rest:tt)* },)*) => {
        #[allow(clippy::needless_update)]
        #[expect(deprecated)]
        pub fn system_params_for_test() -> PbSystemParams {
            let mut ret = PbSystemParams {
                $(
                    $field: ($default as Option<$type>).map(Into::into),
                )*
                ..Default::default() // `None` for deprecated params
            };
            ret.data_directory = Some("hummock_001".to_owned());
            ret.state_store = Some("hummock+memory-isolated-for-test".to_owned());
            ret.backup_storage_url = Some("memory-isolated-for-test".into());
            ret.backup_storage_directory = Some("backup".into());
            ret.use_new_object_prefix_strategy = Some(false);
            ret.time_travel_retention_ms = Some(0);
            ret
        }
    };
}

macro_rules! impl_validate_all_params {
    ($({ $field:ident, $type:ty, $($rest:tt)* },)*) => {
        /// Validates all present parameters in a `PbSystemParams`.
        ///
        /// This function checks the validity of values against the rules in `OverrideValidate`,
        /// regardless of whether a parameter is mutable. It is suitable for validating
        /// initial parameters.
        #[allow(rw::format_error)]
        #[expect(deprecated)]
        pub fn validate_init_system_params(params: &PbSystemParams) -> Result<()> {
            $(
                if let Some(ref v_pb) = params.$field {
                    // 1. Convert the protobuf value (`v_pb`) to a string. `v_pb` could be &u32, &String, etc.
                    //    `to_string()` works for all of them.
                    // 2. Parse the string into the target logical type (`$type`), e.g., `LicenseKey`.
                    //    This relies on the `FromStr` bound on `ParamValue`.
                    let logical_v: $type = v_pb.to_string().parse()
                        .map_err(|e| format!("cannot parse value for parameter '{}': {}", key_of!($field), e))?;
                    // 3. Pass a reference to the correctly-typed logical value to the validator.
                    OverrideValidate::$field(&logical_v)
                        .map_err(|e| format!("invalid value for parameter '{}': {}", key_of!($field), e))?;
                }
            )*
            Ok(())
        }
    };
}

for_all_params!(impl_system_params_from_kv);
for_all_params!(impl_is_mutable);
for_all_params!(impl_derive_missing_fields);
for_all_params!(impl_check_missing_fields);
for_all_params!(impl_system_params_to_kv);
for_all_params!(impl_set_system_param);
for_all_params!(impl_default_validation);
for_all_params!(impl_validate_all_params);
for_all_params!(impl_system_params_for_test);

pub struct OverrideValidate;
impl Validate for OverrideValidate {
    fn barrier_interval_ms(v: &u32) -> Result<()> {
        Self::expect_range(*v, 50..)
    }

    fn checkpoint_frequency(v: &u64) -> Result<()> {
        Self::expect_range(*v, 1..)
    }

    fn backup_storage_directory(v: &String) -> Result<()> {
        if v.trim().is_empty() {
            return Err("backup_storage_directory cannot be empty".into());
        }
        Ok(())
    }

    fn backup_storage_url(v: &String) -> Result<()> {
        if v.trim().is_empty() {
            return Err("backup_storage_url cannot be empty".into());
        }
        Ok(())
    }

    fn time_travel_retention_ms(v: &u64) -> Result<()> {
        // This is intended to guarantee that non-time-travel batch query can still function even compute node's recent versions doesn't include the desired version.
        let min_retention_ms = 600_000;
        // 0 is used to disable time travel.
        if *v != 0 && *v < min_retention_ms {
            return Err(format!(
                "time_travel_retention_ms cannot be less than {min_retention_ms}"
            ));
        }
        Ok(())
    }
}

for_all_params!(impl_default_from_other_params);

struct OverrideFromParams;
impl FromParams for OverrideFromParams {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::system_param::reader::{SystemParamsRead, SystemParamsReader};

    #[test]
    fn test_to_from_kv() {
        // Include all fields (deprecated also).
        let kvs = vec![
            (BARRIER_INTERVAL_MS_KEY, "1"),
            (CHECKPOINT_FREQUENCY_KEY, "1"),
            (SSTABLE_SIZE_MB_KEY, "1"),
            (PARALLEL_COMPACT_SIZE_MB_KEY, "2"),
            (BLOCK_SIZE_KB_KEY, "1"),
            (BLOOM_FALSE_POSITIVE_KEY, "1"),
            (STATE_STORE_KEY, "a"),
            (DATA_DIRECTORY_KEY, "a"),
            (BACKUP_STORAGE_URL_KEY, "a"),
            (BACKUP_STORAGE_DIRECTORY_KEY, "a"),
            (MAX_CONCURRENT_CREATING_STREAMING_JOBS_KEY, "1"),
            (PAUSE_ON_NEXT_BOOTSTRAP_KEY, "false"),
            (ENABLE_TRACING_KEY, "true"),
            (USE_NEW_OBJECT_PREFIX_STRATEGY_KEY, "false"),
            (LICENSE_KEY_KEY, "foo"),
            (TIME_TRAVEL_RETENTION_MS_KEY, "0"),
            (PER_DATABASE_ISOLATION_KEY, "true"),
            (ENFORCE_SECRET_KEY, "false"),
            ("a_deprecated_param", "foo"),
        ];

        // To kv - missing field.
        let p = PbSystemParams::default();
        assert!(system_params_to_kv(&p).is_err());

        // From kv - unrecognized field should be ignored
        assert!(system_params_from_kv(vec![("?", "?")]).is_ok());

        // Deser & ser.
        let p = system_params_from_kv(kvs).unwrap();
        assert_eq!(
            p,
            system_params_from_kv(system_params_to_kv(&p).unwrap()).unwrap()
        );
    }

    #[test]
    fn test_set() {
        let mut p = system_params_for_test();
        // Unrecognized param.
        assert!(set_system_param(&mut p, "?", Some("?".to_owned())).is_err());
        // Value out of range.
        assert!(set_system_param(&mut p, CHECKPOINT_FREQUENCY_KEY, Some("-1".to_owned())).is_err());
        // Set immutable.
        assert!(set_system_param(&mut p, STATE_STORE_KEY, Some("?".to_owned())).is_err());
        // Parse error.
        assert!(set_system_param(&mut p, CHECKPOINT_FREQUENCY_KEY, Some("?".to_owned())).is_err());
        // Normal set.
        assert!(set_system_param(&mut p, CHECKPOINT_FREQUENCY_KEY, Some("500".to_owned())).is_ok());
        assert_eq!(p.checkpoint_frequency, Some(500));
    }

    #[test]
    fn test_init() {
        let mut p = system_params_for_test();
        // Validate all params.
        assert!(validate_init_system_params(&p).is_ok());
        p.barrier_interval_ms = Some(10);
        assert!(validate_init_system_params(&p).is_err());
        p.barrier_interval_ms = Some(1000);
        assert!(validate_init_system_params(&p).is_ok());
    }

    // Test that we always redact the value of the license key when displaying it, but when it comes to
    // persistency, we still write and get the real value.
    #[test]
    fn test_redacted_type() {
        let mut p = system_params_for_test();

        let new_license_key_value = "new_license_key_value";
        assert_ne!(p.license_key(), new_license_key_value);

        let (new_string_value, diff) =
            set_system_param(&mut p, LICENSE_KEY_KEY, Some(new_license_key_value))
                .expect("should succeed")
                .expect("should changed");

        // New string value should be the same as what we set.
        // This should not be redacted.
        assert_eq!(new_string_value, new_license_key_value);

        let new_value = diff.license_key.unwrap();
        // `to_string` repr will be redacted.
        assert_eq!(new_value.to_string(), "<redacted>");
        // while `Into<String>` still shows the real value.
        assert_eq!(String::from(new_value.as_ref()), new_license_key_value);
    }

    #[test]
    fn test_state_store_credentials_are_redacted_for_display() {
        let mut p = system_params_for_test();
        let state_store = "hummock+minio://minioadmin:minioadmin@localhost:9000/risingwave-hummock";
        p.state_store = Some(state_store.to_owned());

        let reader = SystemParamsReader::new(&p);
        let state_store_param = reader
            .get_all()
            .into_iter()
            .find(|param| param.name == STATE_STORE_KEY)
            .unwrap();

        assert_eq!(
            state_store_param.value,
            "hummock+minio://****:****@localhost:9000/risingwave-hummock"
        );
        assert_eq!(reader.state_store().expose(), state_store);
        assert_eq!(String::from(reader.state_store()), state_store);
    }
}
