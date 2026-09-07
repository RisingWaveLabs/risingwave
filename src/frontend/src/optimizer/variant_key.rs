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

//! Policy for rejecting `VARIANT` as a key.
//!
//! Variant equality, hashing and ordering are byte-wise over the canonical encoding and carry no
//! stable SQL semantics, so a variant value must never become a key. `DataType::contains_variant`
//! is the only predicate; the checks are layered by how much they can be trusted.
//!
//! Required to be complete, because they inspect the final key definition rather than a plan shape:
//! `StreamMaterialize` (a table's, index's or MV's own pk) and
//! `reject_variant_in_internal_storage_key` (every streaming state table pk).
//!
//! Last line of defense for batch, which has no state tables: `reject_variant_keys`. It runs after
//! logical optimization so rewrites are already resolved, but it is still shape-based — a new batch
//! operator that keys on a column needs an arm there.
//!
//! Best-effort: `StreamKeyChecker`, which names the offending SQL clause early. Whatever it misses
//! is still caught above, only with a less specific message.

use crate::error::{ErrorCode, RwError};

/// Shared hint for every `VARIANT`-as-key rejection, so a test can assert one stable string
/// regardless of which layer fired.
pub const VARIANT_KEY_HINT: &str = "VARIANT values only have byte-wise equality and ordering, so \
    they cannot be used as a key: in a primary key, index, grouping or join key, ORDER BY, or set \
    operation.";

/// Builds the rejection error so that every layer reports the same shape.
pub fn variant_key_error(message: impl Into<String>) -> RwError {
    ErrorCode::NotSupported(message.into(), VARIANT_KEY_HINT.to_owned()).into()
}
