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

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};

static BATCH_CREATE_TASK_COUNTS: LazyLock<Mutex<HashMap<String, u64>>> =
    LazyLock::new(Default::default);

pub fn record_create_task() {
    let hostname = risingwave_common::util::resource_util::hostname();
    *BATCH_CREATE_TASK_COUNTS
        .lock()
        .unwrap()
        .entry(hostname)
        .or_default() += 1;
}

pub fn create_task_count(node: &str) -> u64 {
    *BATCH_CREATE_TASK_COUNTS
        .lock()
        .unwrap()
        .get(node)
        .unwrap_or(&0)
}

pub fn reset_create_task_counts() {
    BATCH_CREATE_TASK_COUNTS.lock().unwrap().clear();
}
