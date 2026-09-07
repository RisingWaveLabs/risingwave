// Copyright 2026 RisingWave Labs
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

use std::str::FromStr;

use parse_display::Display;
use risingwave_pb::meta::table_cache_refill_policies::table_cache_refill_policy::PbCacheRefillPolicy;
use serde_with::{DeserializeFromStr, SerializeDisplay};

#[derive(Copy, Debug, Clone, PartialEq, Eq, Display, SerializeDisplay, DeserializeFromStr)]
#[display(style = "snake_case")]
pub enum CacheRefillPolicy {
    /// Enable normal cache refill for the table.
    Enabled,
    /// Disable cache refill for the table.
    Disabled,
    /// Enable cache refill optimized for streaming workloads for this table.
    Streaming,
    /// Enable cache refill optimized for serving workloads for this table.
    Serving,
    /// Enable cache refill optimized for both streaming and serving workloads for this table.
    Both,
}

impl FromStr for CacheRefillPolicy {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.to_ascii_lowercase().replace('-', "_");
        match s.as_str() {
            "enabled" => Ok(Self::Enabled),
            "disabled" => Ok(Self::Disabled),
            "streaming" => Ok(Self::Streaming),
            "serving" => Ok(Self::Serving),
            "both" => Ok(Self::Both),
            _ => Err("expect one of [enabled, disabled, streaming, serving, both]"),
        }
    }
}

impl CacheRefillPolicy {
    pub fn to_protobuf(self) -> PbCacheRefillPolicy {
        match self {
            Self::Enabled => PbCacheRefillPolicy::Enabled,
            Self::Disabled => PbCacheRefillPolicy::Disabled,
            Self::Streaming => PbCacheRefillPolicy::Streaming,
            Self::Serving => PbCacheRefillPolicy::Serving,
            Self::Both => PbCacheRefillPolicy::Both,
        }
    }

    pub fn from_protobuf(pb: PbCacheRefillPolicy) -> Option<Self> {
        match pb {
            PbCacheRefillPolicy::Unspecified => None,
            PbCacheRefillPolicy::Enabled => Some(Self::Enabled),
            PbCacheRefillPolicy::Disabled => Some(Self::Disabled),
            PbCacheRefillPolicy::Streaming => Some(Self::Streaming),
            PbCacheRefillPolicy::Serving => Some(Self::Serving),
            PbCacheRefillPolicy::Both => Some(Self::Both),
        }
    }

    /// Legacy/default table-level allow policy. Normal refill is not scoped by
    /// streaming or serving vnode ownership under this policy.
    pub fn is_unscoped_enabled(self) -> bool {
        matches!(self, Self::Enabled)
    }

    pub fn is_streaming_scoped(self) -> bool {
        matches!(self, Self::Streaming | Self::Both)
    }

    pub fn is_serving_scoped(self) -> bool {
        matches!(self, Self::Serving | Self::Both)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_refill_policy_contract() {
        for (policy, name, unscoped, streaming, serving) in [
            (CacheRefillPolicy::Enabled, "enabled", true, false, false),
            (CacheRefillPolicy::Disabled, "disabled", false, false, false),
            (
                CacheRefillPolicy::Streaming,
                "streaming",
                false,
                true,
                false,
            ),
            (CacheRefillPolicy::Serving, "serving", false, false, true),
            (CacheRefillPolicy::Both, "both", false, true, true),
        ] {
            assert_eq!(policy.to_string(), name);
            assert_eq!(name.parse(), Ok(policy));
            assert_eq!(name.to_ascii_uppercase().parse(), Ok(policy));
            assert_eq!(
                CacheRefillPolicy::from_protobuf(policy.to_protobuf()),
                Some(policy)
            );
            assert_eq!(policy.is_unscoped_enabled(), unscoped);
            assert_eq!(policy.is_streaming_scoped(), streaming);
            assert_eq!(policy.is_serving_scoped(), serving);
        }

        assert_eq!(
            CacheRefillPolicy::from_protobuf(PbCacheRefillPolicy::Unspecified),
            None
        );
        assert!("unknown".parse::<CacheRefillPolicy>().is_err());
    }
}
