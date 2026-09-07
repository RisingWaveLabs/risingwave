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

#![allow(clippy::derive_partial_eq_without_eq)]
#![feature(trait_alias)]
#![feature(type_alias_impl_trait)]
#![feature(map_try_insert)]
#![feature(error_generic_member_access)]
#![feature(try_blocks)]
#![feature(stmt_expr_attributes)]
#![feature(proc_macro_hygiene)]
#![feature(custom_test_frameworks)]
#![test_runner(risingwave_test_runner::test_runner::run_failpont_tests)]
#![feature(impl_trait_in_assoc_type)]
#![feature(anonymous_lifetime_in_impl_trait)]
#![feature(duration_millis_float)]
#![feature(iterator_try_reduce)]
#![feature(iterator_try_collect)]

pub mod backup_restore;
pub mod barrier;
pub mod controller;
#[cfg(not(madsim))] // no need in simulation test
pub mod dashboard;
pub mod error;
pub mod hummock;
pub mod manager;
pub mod model;
pub mod rpc;
pub mod serving;
pub mod stream;
pub mod table_refill;
pub mod telemetry;

pub use error::{MetaError, MetaResult};
use risingwave_common::config::MetaStoreConfig;
pub use rpc::{ElectionClient, ElectionMember};

use crate::manager::MetaOpts;

#[derive(Debug, Clone)]
pub enum MetaStoreBackend {
    Mem,
    Sql {
        endpoint: String,
        config: MetaStoreConfig,
    },
}
