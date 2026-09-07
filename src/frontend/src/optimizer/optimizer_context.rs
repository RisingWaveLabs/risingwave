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

use core::fmt::Formatter;
use std::cell::{Cell, RefCell, RefMut};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::rc::{Rc, Weak};
use std::sync::Arc;

use risingwave_common::id::SourceId;
use risingwave_sqlparser::ast::{ExplainFormat, ExplainOptions, ExplainType};

use super::property::WatermarkGroupId;
use crate::expr::{CorrelatedId, SessionTimezone};
use crate::handler::HandlerArgs;
use crate::optimizer::plan_node::generic::Share;
use crate::optimizer::plan_node::{LogicalPlanRef, PlanNodeId, StreamPlanRef};
use crate::session::SessionImpl;
use crate::utils::{OverwriteOptions, WithOptions};
use crate::{Explain, TableCatalog};

const RESERVED_ID_NUM: u16 = 10000;

type PhantomUnsend = PhantomData<Rc<()>>;

/// The stable identity of a shared subplan.
///
/// Unlike [`PlanNodeId`], a `ShareId` survives rebuilding the wrapper plan node around a share.
/// [`OptimizerContext`] tracks the current input used to rebuild wrappers after a DAG pass.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ShareId(u32);

#[derive(Debug)]
pub(in crate::optimizer) struct ShareEntry<P> {
    current: RefCell<P>,
    plan_node_id: PlanNodeId,
}

impl<P> ShareEntry<P> {
    fn update_current(&self, current: P) {
        *self.current.borrow_mut() = current;
    }

    pub(in crate::optimizer) fn plan_node_id(&self) -> PlanNodeId {
        self.plan_node_id
    }
}

impl<P: Clone> ShareEntry<P> {
    fn current(&self) -> P {
        self.current.borrow().clone()
    }
}

pub(in crate::optimizer) type ShareEntryRef<P> = Rc<ShareEntry<P>>;

struct ShareTable<P> {
    /// Weak entries avoid a reference cycle through `PlanBase::ctx` in the stored plans. A live
    /// share handle owns the corresponding strong entry.
    entries: HashMap<ShareId, Weak<ShareEntry<P>>>,
}

impl<P> Default for ShareTable<P> {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }
}

pub struct OptimizerContext {
    session_ctx: Arc<SessionImpl>,
    /// The original SQL string, used for debugging.
    sql: Arc<str>,
    /// Normalized SQL string. See [`HandlerArgs::normalize_sql`].
    normalized_sql: String,
    /// Explain options
    explain_options: ExplainOptions,
    /// Store the trace of optimizer
    optimizer_trace: RefCell<Vec<String>>,
    /// Store the optimized logical plan of optimizer
    logical_explain: RefCell<Option<String>>,
    /// Store options or properties from the `with` clause
    with_options: WithOptions,
    /// Store the Session Timezone and whether it was used.
    session_timezone: RefCell<SessionTimezone>,
    /// Total number of optimization rules have been applied.
    total_rule_applied: RefCell<usize>,
    /// Store the configs can be overwritten in with clause
    /// if not specified, use the value from session variable.
    overwrite_options: OverwriteOptions,
    /// Mapping from Iceberg table identifier to the current snapshot and its RisingWave commit
    /// boundary. Used to keep multiple scans of the same table consistent within one query.
    iceberg_snapshot_info_map: RefCell<HashMap<SourceId, Option<IcebergSnapshotInfo>>>,
    /// Batch materialized view candidates for exact-match rewriting.
    batch_mview_candidates: RefCell<Vec<MaterializedViewCandidate>>,

    /// Last assigned plan node ID.
    last_plan_node_id: Cell<i32>,
    /// Last assigned share ID.
    last_share_id: Cell<u32>,
    /// Last assigned correlated ID.
    last_correlated_id: Cell<u32>,
    /// Last assigned expr display ID.
    last_expr_display_id: Cell<usize>,
    /// Last assigned watermark group ID.
    last_watermark_group_id: Cell<u32>,

    /// Tracks the current input of each live share while DAG-aware passes rebuild wrappers.
    logical_share_table: RefCell<ShareTable<LogicalPlanRef>>,
    stream_share_table: RefCell<ShareTable<StreamPlanRef>>,

    _phantom: PhantomUnsend,
}

#[derive(Clone, Debug)]
pub struct MaterializedViewCandidate {
    pub plan: LogicalPlanRef,
    pub table: Arc<TableCatalog>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct IcebergSnapshotInfo {
    pub(crate) snapshot_id: i64,
    pub(crate) commit_epoch: Option<u64>,
}

pub(in crate::optimizer) struct LastAssignedIds {
    last_plan_node_id: i32,
    last_correlated_id: u32,
    last_expr_display_id: usize,
    last_watermark_group_id: u32,
}

pub type OptimizerContextRef = Rc<OptimizerContext>;

impl OptimizerContext {
    /// Create a new [`OptimizerContext`] from the given [`HandlerArgs`], with empty
    /// [`ExplainOptions`].
    pub fn from_handler_args(handler_args: HandlerArgs) -> Self {
        Self::new(handler_args, ExplainOptions::default())
    }

    /// Create a new [`OptimizerContext`] from the given [`HandlerArgs`] and [`ExplainOptions`].
    pub fn new(mut handler_args: HandlerArgs, explain_options: ExplainOptions) -> Self {
        let session_timezone = RefCell::new(SessionTimezone::new(
            handler_args.session.config().timezone(),
        ));
        let overwrite_options = OverwriteOptions::new(&mut handler_args);
        Self {
            session_ctx: handler_args.session,
            sql: handler_args.sql,
            normalized_sql: handler_args.normalized_sql,
            explain_options,
            optimizer_trace: RefCell::new(vec![]),
            logical_explain: RefCell::new(None),
            with_options: handler_args.with_options,
            session_timezone,
            total_rule_applied: RefCell::new(0),
            overwrite_options,
            iceberg_snapshot_info_map: RefCell::new(HashMap::new()),
            batch_mview_candidates: RefCell::new(Vec::new()),

            last_plan_node_id: Cell::new(RESERVED_ID_NUM.into()),
            last_share_id: Cell::new(0),
            last_correlated_id: Cell::new(0),
            last_expr_display_id: Cell::new(RESERVED_ID_NUM.into()),
            last_watermark_group_id: Cell::new(RESERVED_ID_NUM.into()),

            logical_share_table: RefCell::new(ShareTable::default()),
            stream_share_table: RefCell::new(ShareTable::default()),

            _phantom: Default::default(),
        }
    }

    #[cfg(test)]
    pub fn mock() -> OptimizerContextRef {
        Self {
            session_ctx: Arc::new(SessionImpl::mock()),
            sql: Arc::from(""),
            normalized_sql: "".to_owned(),
            explain_options: ExplainOptions::default(),
            optimizer_trace: RefCell::new(vec![]),
            logical_explain: RefCell::new(None),
            with_options: Default::default(),
            session_timezone: RefCell::new(SessionTimezone::new("UTC".into())),
            total_rule_applied: RefCell::new(0),
            overwrite_options: OverwriteOptions::default(),
            iceberg_snapshot_info_map: RefCell::new(HashMap::new()),
            batch_mview_candidates: RefCell::new(Vec::new()),

            last_plan_node_id: Cell::new(0),
            last_share_id: Cell::new(0),
            last_correlated_id: Cell::new(0),
            last_expr_display_id: Cell::new(0),
            last_watermark_group_id: Cell::new(0),

            logical_share_table: RefCell::new(ShareTable::default()),
            stream_share_table: RefCell::new(ShareTable::default()),

            _phantom: Default::default(),
        }
        .into()
    }

    pub fn next_plan_node_id(&self) -> PlanNodeId {
        self.last_plan_node_id.update(|id| id + 1);
        PlanNodeId(self.last_plan_node_id.get())
    }

    fn next_share_id(&self) -> ShareId {
        self.last_share_id.update(|id| id + 1);
        ShareId(self.last_share_id.get())
    }

    fn share_entry<P>(
        table: &RefCell<ShareTable<P>>,
        share_id: ShareId,
        convention: &str,
    ) -> ShareEntryRef<P> {
        table
            .borrow()
            .entries
            .get(&share_id)
            .unwrap_or_else(|| panic!("{convention} share {share_id:?} is not registered"))
            .upgrade()
            .unwrap_or_else(|| panic!("{convention} share {share_id:?} is no longer live"))
    }

    fn register_share<P: Clone>(&self, table: &RefCell<ShareTable<P>>, input: P) -> Share<P> {
        let share_id = self.next_share_id();
        let entry = Rc::new(ShareEntry {
            current: RefCell::new(input.clone()),
            plan_node_id: self.next_plan_node_id(),
        });
        table
            .borrow_mut()
            .entries
            .try_insert(share_id, Rc::downgrade(&entry))
            .expect("share id must be unique");
        Share::new(share_id, input, entry)
    }

    fn share<P: Clone>(
        &self,
        table: &RefCell<ShareTable<P>>,
        share_id: ShareId,
        convention: &str,
    ) -> Share<P> {
        let entry = Self::share_entry(table, share_id, convention);
        let input = entry.current();
        Share::new(share_id, input, entry)
    }

    fn update_share<P>(
        table: &RefCell<ShareTable<P>>,
        share_id: ShareId,
        new_input: P,
        convention: &str,
    ) {
        let entry = Self::share_entry(table, share_id, convention);
        entry.update_current(new_input);
    }

    pub(in crate::optimizer) fn register_logical_share(
        &self,
        input: LogicalPlanRef,
    ) -> Share<LogicalPlanRef> {
        self.register_share(&self.logical_share_table, input)
    }

    pub(in crate::optimizer) fn logical_share(&self, share_id: ShareId) -> Share<LogicalPlanRef> {
        self.share(&self.logical_share_table, share_id, "logical")
    }

    pub(in crate::optimizer) fn update_logical_share(
        &self,
        share_id: ShareId,
        new_input: LogicalPlanRef,
    ) {
        Self::update_share(&self.logical_share_table, share_id, new_input, "logical");
    }

    pub(in crate::optimizer) fn register_stream_share(
        &self,
        input: StreamPlanRef,
    ) -> Share<StreamPlanRef> {
        self.register_share(&self.stream_share_table, input)
    }

    pub(in crate::optimizer) fn update_stream_share(
        &self,
        share_id: ShareId,
        new_input: StreamPlanRef,
    ) {
        Self::update_share(&self.stream_share_table, share_id, new_input, "stream");
    }

    pub fn next_correlated_id(&self) -> CorrelatedId {
        self.last_correlated_id.update(|id| id + 1);
        self.last_correlated_id.get()
    }

    pub fn next_expr_display_id(&self) -> usize {
        self.last_expr_display_id.update(|id| id + 1);
        self.last_expr_display_id.get()
    }

    pub fn next_watermark_group_id(&self) -> WatermarkGroupId {
        self.last_watermark_group_id.update(|id| id + 1);
        self.last_watermark_group_id.get()
    }

    pub(in crate::optimizer) fn backup_elem_ids(&self) -> LastAssignedIds {
        LastAssignedIds {
            last_plan_node_id: self.last_plan_node_id.get(),
            last_correlated_id: self.last_correlated_id.get(),
            last_expr_display_id: self.last_expr_display_id.get(),
            last_watermark_group_id: self.last_watermark_group_id.get(),
        }
    }

    /// This should only be called in [`crate::optimizer::plan_node::reorganize_elements_id`].
    pub(in crate::optimizer) fn reset_elem_ids(&self) {
        self.last_plan_node_id.set(0);
        self.last_correlated_id.set(0);
        self.last_expr_display_id.set(0);
        self.last_watermark_group_id.set(0);
    }

    pub(in crate::optimizer) fn restore_elem_ids(&self, backup: LastAssignedIds) {
        self.last_plan_node_id.set(backup.last_plan_node_id);
        self.last_correlated_id.set(backup.last_correlated_id);
        self.last_expr_display_id.set(backup.last_expr_display_id);
        self.last_watermark_group_id
            .set(backup.last_watermark_group_id);
    }

    pub fn add_rule_applied(&self, num: usize) {
        *self.total_rule_applied.borrow_mut() += num;
    }

    pub fn total_rule_applied(&self) -> usize {
        *self.total_rule_applied.borrow()
    }

    pub fn is_explain_verbose(&self) -> bool {
        self.explain_options.verbose
    }

    pub fn is_explain_trace(&self) -> bool {
        self.explain_options.trace
    }

    fn is_explain_logical(&self) -> bool {
        self.explain_options.explain_type == ExplainType::Logical
    }

    pub fn trace(&self, str: impl Into<String>) {
        // If explain type is logical, do not store the trace for any optimizations beyond logical.
        if self.is_explain_logical() && self.logical_explain.borrow().is_some() {
            return;
        }
        let mut optimizer_trace = self.optimizer_trace.borrow_mut();
        let string = str.into();
        tracing::info!(target: "explain_trace", "\n{}", string);
        optimizer_trace.push(string);
        optimizer_trace.push("\n".to_owned());
    }

    pub fn warn_to_user(&self, str: impl Into<String>) {
        self.session_ctx().notice_to_user(str);
    }

    fn explain_plan_impl(&self, plan: &impl Explain) -> String {
        match self.explain_options.explain_format {
            ExplainFormat::Text => plan.explain_to_string(),
            ExplainFormat::Json => plan.explain_to_json(),
            ExplainFormat::Xml => plan.explain_to_xml(),
            ExplainFormat::Yaml => plan.explain_to_yaml(),
            ExplainFormat::Dot => plan.explain_to_dot(),
        }
    }

    pub fn may_store_explain_logical(&self, plan: &LogicalPlanRef) {
        if self.is_explain_logical() {
            let str = self.explain_plan_impl(plan);
            *self.logical_explain.borrow_mut() = Some(str);
        }
    }

    pub fn take_logical(&self) -> Option<String> {
        self.logical_explain.borrow_mut().take()
    }

    pub fn take_trace(&self) -> Vec<String> {
        self.optimizer_trace.borrow_mut().drain(..).collect()
    }

    pub fn with_options(&self) -> &WithOptions {
        &self.with_options
    }

    pub fn overwrite_options(&self) -> &OverwriteOptions {
        &self.overwrite_options
    }

    pub fn add_batch_mview_candidate(&self, table: Arc<TableCatalog>, plan: LogicalPlanRef) {
        self.batch_mview_candidates
            .borrow_mut()
            .push(MaterializedViewCandidate { plan, table });
    }

    pub fn batch_mview_candidates(&self) -> std::cell::Ref<'_, Vec<MaterializedViewCandidate>> {
        self.batch_mview_candidates.borrow()
    }

    pub fn session_ctx(&self) -> &Arc<SessionImpl> {
        &self.session_ctx
    }

    pub fn batch_plan_dml_wait_persistence(&self) -> bool {
        let session_config = self.session_ctx.config();
        !session_config.implicit_flush() && session_config.dml_wait_persistence()
    }

    /// Return the original SQL.
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// Return the normalized SQL.
    pub fn normalized_sql(&self) -> &str {
        &self.normalized_sql
    }

    pub fn session_timezone(&self) -> RefMut<'_, SessionTimezone> {
        self.session_timezone.borrow_mut()
    }

    pub fn get_session_timezone(&self) -> String {
        self.session_timezone.borrow().timezone()
    }

    pub(crate) fn iceberg_snapshot_info_map(
        &self,
    ) -> RefMut<'_, HashMap<SourceId, Option<IcebergSnapshotInfo>>> {
        self.iceberg_snapshot_info_map.borrow_mut()
    }
}

impl std::fmt::Debug for OptimizerContext {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "QueryContext {{ sql = {}, explain_options = {}, with_options = {:?}, last_plan_node_id = {}, last_correlated_id = {} }}",
            self.sql,
            self.explain_options,
            self.with_options,
            self.last_plan_node_id.get(),
            self.last_correlated_id.get(),
        )
    }
}
