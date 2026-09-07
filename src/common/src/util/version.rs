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

use crate::RW_VERSION;

#[cfg(madsim)]
const SIMULATED_RW_VERSION_ENV: &str = "RW_SIMULATED_RW_VERSION";
#[cfg(madsim)]
const SIMULATED_RW_VERSION_NODES_ENV: &str = "RW_SIMULATED_RW_VERSION_NODES";

pub fn current_rw_version() -> String {
    #[cfg(madsim)]
    {
        if let Ok(version) = std::env::var(SIMULATED_RW_VERSION_ENV)
            && let Ok(nodes) = std::env::var(SIMULATED_RW_VERSION_NODES_ENV)
        {
            let hostname = crate::util::resource_util::hostname();
            if nodes.split(',').any(|node| node.trim() == hostname) {
                return version;
            }
        }
    }

    RW_VERSION.to_owned()
}

pub fn is_compatible_rw_version(rw_version: &str) -> bool {
    rw_version == current_rw_version()
}
