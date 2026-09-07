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

//! Streaming `MATCH_RECOGNIZE` (SQL:2016 row pattern recognition).
//!
//! The ordered-input matcher: the executor consumes rows already in `ORDER BY` order (see the
//! planner's `WatermarkSort` insertion), feeding a per-partition incremental NFA matcher and
//! emitting matches on completion. `nfa` carries the pattern-matching core with its
//! catastrophic-backtracking defenses; `incremental` the appended-rows matcher; `proto` the
//! structured pattern decoding.

pub mod executor;
pub mod incremental;
pub mod nfa;
pub mod proto;
