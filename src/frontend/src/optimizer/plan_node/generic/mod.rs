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

//! This module contains the generic plan nodes that are shared by all the plan nodes.
//! They are meant to reuse the common fields between logical, batch and stream nodes.

use std::borrow::Cow;
use std::hash::Hash;

use pretty_xmlish::XmlNode;
use risingwave_common::catalog::Schema;

use super::{EqJoinPredicate, PlanNodeId};
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::property::{Distribution, FunctionalDependencySet};

pub mod dynamic_filter;
pub use dynamic_filter::*;
mod hop_window;
pub use hop_window::*;
mod agg;
pub use agg::*;
mod project_set;
pub use project_set::*;
mod get_channel_delta_stats;
pub use get_channel_delta_stats::*;
mod table_function;
pub use table_function::*;
mod values;
pub use values::*;
mod join;
pub use join::*;
mod project;
pub use project::*;
mod filter;
pub use filter::*;
mod gap_fill;
pub use gap_fill::*;
mod match_recognize;
pub use match_recognize::*;
mod expand;
pub use expand::*;
mod source;
pub use source::*;
mod table_scan;
pub use table_scan::*;
mod sys_scan;
pub use sys_scan::*;
mod log_scan;
pub use log_scan::*;

mod cdc_scan;
pub use cdc_scan::*;

mod union;
pub use union::*;
mod top_n;
pub use top_n::*;
mod share;
pub use share::*;
mod dedup;
pub use dedup::*;
mod intersect;
pub use intersect::*;
mod over_window;
pub use over_window::*;
mod except;
pub use except::*;
mod update;
pub use update::*;
mod delete;
pub use delete::*;
mod insert;
pub use insert::*;
mod limit;
pub use limit::*;
mod max_one_row;
pub use max_one_row::*;
mod changelog;
pub use changelog::*;
mod now;
pub use now::*;

mod file_scan;
pub use file_scan::*;

mod iceberg_metadata_scan;
pub use iceberg_metadata_scan::*;

mod postgres_query;
pub use postgres_query::*;

mod mysql_query;

pub use mysql_query::*;

mod locality_provider;
pub use locality_provider::*;

mod vector_index_lookup_join;
pub use vector_index_lookup_join::*;

pub trait DistillUnit {
    fn distill_with_name<'a>(&self, name: impl Into<Cow<'a, str>>) -> XmlNode<'a>;
}

macro_rules! impl_distill_unit_from_fields {
    ($name:ident, $bound:path) => {
        use std::borrow::Cow;

        use pretty_xmlish::XmlNode;
        use $crate::optimizer::plan_node::generic::DistillUnit;
        impl<PlanRef: $bound> DistillUnit for $name<PlanRef> {
            fn distill_with_name<'a>(&self, name: impl Into<Cow<'a, str>>) -> XmlNode<'a> {
                XmlNode::simple_record(name, self.fields_pretty(), vec![])
            }
        }
    };
}
pub(super) use impl_distill_unit_from_fields;

#[auto_impl::auto_impl(&)]
pub trait GenericPlanRef: Eq + Hash {
    fn id(&self) -> PlanNodeId;
    fn schema(&self) -> &Schema;
    fn stream_key(&self) -> Option<&[usize]>;
    fn functional_dependency(&self) -> &FunctionalDependencySet;
    fn ctx(&self) -> OptimizerContextRef;
}

#[auto_impl::auto_impl(&)]
pub trait PhysicalPlanRef: GenericPlanRef {
    fn distribution(&self) -> &Distribution;
}

pub trait GenericPlanNode {
    fn functional_dependency(&self) -> FunctionalDependencySet;
    fn schema(&self) -> Schema;
    fn stream_key(&self) -> Option<Vec<usize>>;
    fn ctx(&self) -> OptimizerContextRef;
}
