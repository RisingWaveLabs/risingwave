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

//! Role for compute node and hummock.

use clap::ValueEnum;
use serde::{Deserialize, Serialize};

/// Role of the compute node or hummock storage.
#[derive(Copy, Clone, Debug, Default, ValueEnum, Serialize, Deserialize)]
pub enum Role {
    /// Serving role
    Serving,
    /// Streaming role
    Streaming,
    /// Both serving and streaming role
    #[default]
    Both,
    /// No specific role, mostly for temporary usage like risectl or testing.
    #[value(skip)]
    None,
}

impl Role {
    pub fn for_streaming(&self) -> bool {
        match self {
            Role::Serving => false,
            Role::Streaming => true,
            Role::Both => true,
            Role::None => false,
        }
    }

    pub fn for_serving(&self) -> bool {
        match self {
            Role::Serving => true,
            Role::Streaming => false,
            Role::Both => true,
            Role::None => false,
        }
    }
}
