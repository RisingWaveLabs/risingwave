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

use std::borrow::Borrow;

use risingwave_license::LicenseKeyRef;
use risingwave_pb::meta::PbSystemParams;

use super::{ParamValue, StateStoreUrlRef, default};
use crate::for_all_params;

/// Information about a system parameter.
///
/// Used to display to users through `pg_settings` system table and `SHOW PARAMETERS` command.
pub struct ParameterInfo {
    pub name: &'static str,
    pub mutable: bool,
    /// The [`ToString`] representation of the parameter value.
    ///
    /// Certain parameters, such as `license_key`, may be sensitive, and redaction is applied to them in their newtypes.
    /// As a result, we get the redacted value here for display.
    pub value: String,
    pub description: &'static str,
}

macro_rules! define_system_params_read_trait {
    ($({ $field:ident, $type:ty, $default:expr, $is_mutable:expr, $doc:literal, $($rest:tt)* },)*) => {
        /// The trait delegating reads on [`risingwave_pb::meta::SystemParams`].
        ///
        /// Purposes:
        /// - Avoid misuse of deprecated fields by hiding their getters.
        /// - Abstract fallback logic for fields that might not be provided by meta service due to backward
        ///   compatibility.
        /// - Redact sensitive fields by returning a newtype around the original value.
        pub trait SystemParamsRead {
            $(
                #[doc = $doc]
                fn $field(&self) -> <$type as ParamValue>::Borrowed<'_>;
            )*

            /// Return the information of all parameters.
            fn get_all(&self) -> Vec<ParameterInfo> {
                vec![
                    $(
                        ParameterInfo {
                            name: stringify!($field),
                            mutable: $is_mutable,
                            value: self.$field().to_string(), // use `to_string` to get displayable (maybe redacted) value
                            description: $doc,
                        },
                    )*
                ]
            }
        }
    };
}

for_all_params!(define_system_params_read_trait);

/// The wrapper delegating reads on [`risingwave_pb::meta::SystemParams`].
///
/// See [`SystemParamsRead`] for more details.
// TODO: should we manually impl `PartialEq` by comparing each field with read functions?
#[derive(Clone, PartialEq)]
pub struct SystemParamsReader<I = PbSystemParams> {
    inner: I,
}

// TODO: should ban `Debug` for inner `SystemParams`.
// https://github.com/risingwavelabs/risingwave/pull/17906#discussion_r1705056943
macro_rules! impl_system_params_reader_debug {
    ($({ $field:ident, $($rest:tt)* },)*) => {
        impl<I> std::fmt::Debug for SystemParamsReader<I>
        where
            I: Borrow<PbSystemParams>,
        {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct("SystemParamsReader")
                    $(
                        .field(stringify!($field), &self.$field())
                    )*
                    .finish()
            }
        }
    };
}

for_all_params!(impl_system_params_reader_debug);

impl<I> From<I> for SystemParamsReader<I>
where
    I: Borrow<PbSystemParams>,
{
    fn from(inner: I) -> Self {
        Self { inner }
    }
}

impl<I> SystemParamsReader<I>
where
    I: Borrow<PbSystemParams>,
{
    pub fn new(inner: I) -> Self {
        Self { inner }
    }

    /// Return a new reader with the reference to the inner system params.
    pub fn as_ref(&self) -> SystemParamsReader<&PbSystemParams> {
        SystemParamsReader {
            inner: self.inner(),
        }
    }

    fn inner(&self) -> &PbSystemParams {
        self.inner.borrow()
    }
}

/// - Unwrap the field if it always exists.
///   For example, if a parameter is introduced before the initial public release.
///
/// - Otherwise, specify the fallback logic when the field is missing.
impl<I> SystemParamsRead for SystemParamsReader<I>
where
    I: Borrow<PbSystemParams>,
{
    fn barrier_interval_ms(&self) -> u32 {
        self.inner().barrier_interval_ms.unwrap()
    }

    fn enforce_secret(&self) -> bool {
        self.inner()
            .enforce_secret
            .unwrap_or_else(default::enforce_secret)
    }

    fn checkpoint_frequency(&self) -> u64 {
        self.inner().checkpoint_frequency.unwrap()
    }

    fn parallel_compact_size_mb(&self) -> u32 {
        self.inner().parallel_compact_size_mb.unwrap()
    }

    fn sstable_size_mb(&self) -> u32 {
        self.inner().sstable_size_mb.unwrap()
    }

    fn block_size_kb(&self) -> u32 {
        self.inner().block_size_kb.unwrap()
    }

    #[expect(deprecated)]
    fn bloom_false_positive(&self) -> f64 {
        self.inner().bloom_false_positive.unwrap()
    }

    fn state_store(&self) -> StateStoreUrlRef<'_> {
        self.inner().state_store.as_deref().unwrap().into()
    }

    fn data_directory(&self) -> &str {
        self.inner().data_directory.as_ref().unwrap()
    }

    fn use_new_object_prefix_strategy(&self) -> bool {
        *self
            .inner()
            .use_new_object_prefix_strategy
            .as_ref()
            .unwrap()
    }

    fn backup_storage_url(&self) -> &str {
        self.inner().backup_storage_url.as_ref().unwrap()
    }

    fn backup_storage_directory(&self) -> &str {
        self.inner().backup_storage_directory.as_ref().unwrap()
    }

    fn max_concurrent_creating_streaming_jobs(&self) -> u32 {
        self.inner().max_concurrent_creating_streaming_jobs.unwrap()
    }

    fn pause_on_next_bootstrap(&self) -> bool {
        self.inner()
            .pause_on_next_bootstrap
            .unwrap_or_else(default::pause_on_next_bootstrap)
    }

    fn enable_tracing(&self) -> bool {
        self.inner()
            .enable_tracing
            .unwrap_or_else(default::enable_tracing)
    }

    fn license_key(&self) -> LicenseKeyRef<'_> {
        self.inner()
            .license_key
            .as_deref()
            .unwrap_or_default()
            .into()
    }

    fn time_travel_retention_ms(&self) -> u64 {
        self.inner()
            .time_travel_retention_ms
            .unwrap_or_else(default::time_travel_retention_ms)
    }

    fn per_database_isolation(&self) -> <bool as ParamValue>::Borrowed<'_> {
        self.inner()
            .per_database_isolation
            .unwrap_or_else(default::per_database_isolation)
    }
}
