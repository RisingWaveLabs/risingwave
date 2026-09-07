// Copyright 2025 RisingWave Labs
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

use risingwave_pb::common::Uint32Vector;
use risingwave_pb::id::RelationId;
use risingwave_pb::stream_plan::BackfillOrder;
use risingwave_sqlparser::ast::BackfillOrderStrategy;

use crate::error::Result;
use crate::optimizer::backfill_order_strategy::auto::plan_auto_strategy;
use crate::optimizer::backfill_order_strategy::fixed::plan_fixed_strategy;
use crate::optimizer::plan_node::StreamPlanRef;
use crate::session::SessionImpl;

pub mod auto {
    use std::collections::{HashMap, HashSet};

    use risingwave_pb::id::RelationId;

    use crate::optimizer::backfill_order_strategy::common::has_cycle;
    use crate::optimizer::plan_node::{StreamPlanNodeType, StreamPlanRef};
    use crate::session::SessionImpl;

    #[derive(Debug)]
    pub(super) enum BackfillTreeNode {
        Join {
            lhs: Box<BackfillTreeNode>,
            rhs: Box<BackfillTreeNode>,
        },
        Scan {
            id: RelationId,
        },
        Union {
            children: Vec<BackfillTreeNode>,
        },
        Ignored,
    }

    /// TODO: Handle stream share
    fn plan_graph_to_backfill_tree(
        session: &SessionImpl,
        plan: StreamPlanRef,
    ) -> Option<BackfillTreeNode> {
        match plan.node_type() {
            StreamPlanNodeType::StreamHashJoin => {
                assert_eq!(plan.inputs().len(), 2);
                let mut inputs = plan.inputs().into_iter();
                let l = inputs.next().unwrap();
                let r = inputs.next().unwrap();
                Some(BackfillTreeNode::Join {
                    lhs: Box::new(plan_graph_to_backfill_tree(session, l)?),
                    rhs: Box::new(plan_graph_to_backfill_tree(session, r)?),
                })
            }
            StreamPlanNodeType::StreamTableScan => {
                let table_scan = plan.as_stream_table_scan().expect("table scan");
                let relation_id = table_scan.core().table_catalog.id().as_relation_id();
                Some(BackfillTreeNode::Scan { id: relation_id })
            }
            StreamPlanNodeType::StreamSourceScan => {
                let source_scan = plan.as_stream_source_scan().expect("source scan");
                let relation_id = source_scan.source_catalog().id.as_relation_id();
                Some(BackfillTreeNode::Scan { id: relation_id })
            }
            StreamPlanNodeType::StreamUnion => {
                let inputs = plan.inputs();
                let mut children = Vec::with_capacity(inputs.len());
                for child in inputs {
                    let subtree = plan_graph_to_backfill_tree(session, child)?;
                    if matches!(subtree, BackfillTreeNode::Ignored) {
                        continue;
                    }
                    children.push(subtree);
                }
                Some(BackfillTreeNode::Union { children })
            }
            node_type => {
                let inputs = plan.inputs();
                match inputs.len() {
                    0 => Some(BackfillTreeNode::Ignored),
                    1 => {
                        let mut inputs = inputs.into_iter();
                        let child = inputs.next().unwrap();
                        plan_graph_to_backfill_tree(session, child)
                    }
                    _ => {
                        session.notice_to_user(format!(
                            "Backfill order strategy is not supported for {:?}",
                            node_type
                        ));
                        None
                    }
                }
            }
        }
    }

    /// For a given subtree, all the leaf nodes in the leftmost leaf-node node
    /// must come _after_ all other leaf nodes in the subtree.
    /// For example, for the following tree:
    ///
    /// ```text
    ///       JOIN (A)
    ///      /        \
    ///     JOIN (B)   SCAN (C)
    ///    /        \
    ///   /         \
    /// /           \
    /// SCAN (D)    SCAN (E)
    /// ```
    ///
    /// D is the leftmost leaf node.
    /// {C, E} are the other leaf nodes.
    ///
    /// So the partial order is:
    /// {C, E} -> {D}
    /// Expanded:
    /// C -> D
    /// E -> D
    ///
    /// Next, we have to consider UNION as well.
    /// If a UNION node is the leftmost child,
    /// then for all subtrees in the UNION,
    /// their leftmost leaf nodes must come after
    /// all other leaf nodes in the subtree.
    ///
    /// ``` text
    ///         JOIN (A)
    ///        /        \
    ///       JOIN (B)   SCAN (C)
    ///       /       \
    ///      /         \
    ///     UNION (D)   SCAN (E)
    ///    /        \
    ///   /         \
    /// SCAN (F)    JOIN (G)
    ///            /        \
    ///           /          \
    ///          SCAN (H)   SCAN (I)
    /// ```
    ///
    /// In this case, {F, H} are the leftmost leaf nodes.
    /// {C, E} -> {F, H}
    /// I -> H
    /// Expanded:
    /// C -> F
    /// E -> F
    /// C -> H
    /// E -> H
    /// I -> H
    fn fold_backfill_tree_to_partial_order(
        tree: BackfillTreeNode,
    ) -> HashMap<RelationId, HashSet<RelationId>> {
        let mut order: HashMap<RelationId, HashSet<RelationId>> = HashMap::new();

        // Returns terminal nodes of the subtree
        // This is recursive algorithm we use to traverse the tree and compute partial orders.
        fn traverse_backfill_tree(
            tree: BackfillTreeNode,
            order: &mut HashMap<RelationId, HashSet<RelationId>>,
            is_leftmost_child: bool,
            mut prior_terminal_nodes: HashSet<RelationId>,
        ) -> HashSet<RelationId> {
            match tree {
                BackfillTreeNode::Ignored => HashSet::new(),
                BackfillTreeNode::Scan { id } => {
                    if is_leftmost_child {
                        for prior_terminal_node in prior_terminal_nodes {
                            order.entry(prior_terminal_node).or_default().insert(id);
                        }
                    }
                    HashSet::from([id])
                }
                BackfillTreeNode::Union { children } => {
                    let mut terminal_nodes = HashSet::new();
                    for child in children {
                        let child_terminal_nodes = traverse_backfill_tree(
                            child,
                            order,
                            is_leftmost_child,
                            prior_terminal_nodes.clone(),
                        );
                        terminal_nodes.extend(child_terminal_nodes);
                    }
                    terminal_nodes
                }
                BackfillTreeNode::Join { lhs, rhs } => {
                    let rhs_terminal_nodes =
                        traverse_backfill_tree(*rhs, order, false, HashSet::new());
                    prior_terminal_nodes.extend(rhs_terminal_nodes.iter().cloned());
                    traverse_backfill_tree(*lhs, order, true, prior_terminal_nodes)
                }
            }
        }

        traverse_backfill_tree(tree, &mut order, false, HashSet::new());

        order
    }

    pub(super) fn plan_auto_strategy(
        session: &SessionImpl,
        plan: StreamPlanRef,
    ) -> HashMap<RelationId, HashSet<RelationId>> {
        if let Some(tree) = plan_graph_to_backfill_tree(session, plan) {
            let order = fold_backfill_tree_to_partial_order(tree);
            if has_cycle(&order) {
                tracing::warn!(?order, "Backfill order strategy has a cycle");
                session.notice_to_user("Backfill order strategy has a cycle");
                return Default::default();
            }
            return order;
        }
        Default::default()
    }
}

mod fixed {
    use std::collections::{HashMap, HashSet};

    use risingwave_common::bail;
    use risingwave_pb::id::RelationId;
    use risingwave_sqlparser::ast::ObjectName;

    use crate::error::Result;
    use crate::optimizer::backfill_order_strategy::common::{
        bind_backfill_relation_id_by_name, has_cycle,
    };
    use crate::optimizer::plan_node::{StreamPlanNodeType, StreamPlanRef};
    use crate::session::SessionImpl;

    /// Collect a mapping from bindable relation IDs to relation IDs actually scanned in the plan.
    ///
    /// Index selection may replace a scan on a base table with a scan on one of its index tables.
    /// In that case, both the base table ID and the index table ID map to the actual index table
    /// ID, so a user can still specify the base table in a fixed backfill order.
    fn collect_scanned_relation_ids(
        session: &SessionImpl,
        plan: StreamPlanRef,
    ) -> HashMap<RelationId, HashSet<RelationId>> {
        let mut relation_ids: HashMap<RelationId, HashSet<RelationId>> = HashMap::new();

        fn visit(
            session: &SessionImpl,
            plan: StreamPlanRef,
            relation_ids: &mut HashMap<RelationId, HashSet<RelationId>>,
        ) {
            match plan.node_type() {
                StreamPlanNodeType::StreamTableScan => {
                    let table_scan = plan.as_stream_table_scan().expect("table scan");
                    let table_catalog = &table_scan.core().table_catalog;
                    let scanned_relation_id = table_catalog.id().as_relation_id();
                    relation_ids
                        .entry(scanned_relation_id)
                        .or_default()
                        .insert(scanned_relation_id);

                    if table_catalog.is_index() {
                        let reader = session.env().catalog_reader().read_guard();
                        let schema_catalog = reader
                            .get_schema_by_id(table_catalog.database_id, table_catalog.schema_id)
                            .expect("schema of an index table should exist");
                        let index_catalog = schema_catalog
                            .iter_index()
                            .find(|index| index.index_table().id == table_catalog.id)
                            .expect("index catalog of an index table should exist");
                        relation_ids
                            .entry(index_catalog.primary_table.id().as_relation_id())
                            .or_default()
                            .insert(scanned_relation_id);
                    }
                }
                StreamPlanNodeType::StreamSourceScan => {
                    let source_scan = plan.as_stream_source_scan().expect("source scan");
                    let relation_id = source_scan.source_catalog().id.as_relation_id();
                    relation_ids
                        .entry(relation_id)
                        .or_default()
                        .insert(relation_id);
                }
                _ => {}
            }

            // Recursively visit all inputs
            for child in plan.inputs() {
                visit(session, child, relation_ids);
            }
        }

        visit(session, plan, &mut relation_ids);
        relation_ids
    }

    pub(super) fn plan_fixed_strategy(
        session: &SessionImpl,
        orders: Vec<(ObjectName, ObjectName)>,
        plan: StreamPlanRef,
    ) -> Result<HashMap<RelationId, HashSet<RelationId>>> {
        // Collect all scanned relation IDs from the plan.
        let scanned_relation_ids = collect_scanned_relation_ids(session, plan);

        let mut order: HashMap<RelationId, HashSet<RelationId>> = HashMap::new();
        for (start_name, end_name) in orders {
            let start_relation_id = bind_backfill_relation_id_by_name(session, start_name.clone())?;
            let end_relation_id = bind_backfill_relation_id_by_name(session, end_name.clone())?;

            // Validate that both relations are present in the query plan
            let Some(start_scanned_relation_ids) = scanned_relation_ids.get(&start_relation_id)
            else {
                bail!(
                    "Table or source '{}' specified in backfill_order is not used in the query",
                    start_name
                );
            };
            let Some(end_scanned_relation_ids) = scanned_relation_ids.get(&end_relation_id) else {
                bail!(
                    "Table or source '{}' specified in backfill_order is not used in the query",
                    end_name
                );
            };

            for start_scanned_relation_id in start_scanned_relation_ids {
                order
                    .entry(*start_scanned_relation_id)
                    .or_default()
                    .extend(end_scanned_relation_ids.iter().copied());
            }
        }
        if has_cycle(&order) {
            bail!("Backfill order strategy has a cycle");
        }
        Ok(order)
    }
}

mod common {
    use std::collections::{HashMap, HashSet};

    use risingwave_pb::id::RelationId;
    use risingwave_sqlparser::ast::ObjectName;

    use crate::Binder;
    use crate::catalog::CatalogError;
    use crate::catalog::root_catalog::SchemaPath;
    use crate::catalog::schema_catalog::SchemaCatalog;
    use crate::error::Result;
    use crate::session::SessionImpl;

    /// Check if the backfill order has a cycle.
    pub(super) fn has_cycle(order: &HashMap<RelationId, HashSet<RelationId>>) -> bool {
        fn dfs(
            node: RelationId,
            order: &HashMap<RelationId, HashSet<RelationId>>,
            visited: &mut HashSet<RelationId>,
            stack: &mut HashSet<RelationId>,
        ) -> bool {
            if stack.contains(&node) {
                return true; // Cycle detected
            }

            if visited.insert(node) {
                stack.insert(node);
                if let Some(downstreams) = order.get(&node) {
                    for neighbor in downstreams {
                        if dfs(*neighbor, order, visited, stack) {
                            return true;
                        }
                    }
                }
                stack.remove(&node);
            }
            false
        }

        let mut visited = HashSet::new();
        let mut stack = HashSet::new();
        for &start in order.keys() {
            if dfs(start, order, &mut visited, &mut stack) {
                return true;
            }
        }

        false
    }

    pub(super) fn bind_backfill_relation_id_by_name(
        session: &SessionImpl,
        name: ObjectName,
    ) -> Result<RelationId> {
        let (db_name, schema_name, rel_name) = Binder::resolve_db_schema_qualified_name(&name)?;
        let db_name = db_name.unwrap_or(session.database());

        let reader = session.env().catalog_reader().read_guard();

        match schema_name {
            Some(name) => {
                let schema_catalog = reader.get_schema_by_name(&db_name, &name)?;
                bind_table(schema_catalog, &rel_name)
            }
            None => {
                let search_path = session.config().search_path();
                let user_name = session.user_name();
                let schema_path = SchemaPath::Path(&search_path, &user_name);
                let result: crate::error::Result<Option<(RelationId, &str)>> = schema_path
                    .try_find(|schema_name| {
                        if let Ok(schema_catalog) = reader.get_schema_by_name(&db_name, schema_name)
                            && let Ok(relation_id) = bind_table(schema_catalog, &rel_name)
                        {
                            Ok(Some(relation_id))
                        } else {
                            Ok(None)
                        }
                    });
                if let Some((relation_id, _schema_name)) = result? {
                    return Ok(relation_id);
                }
                Err(CatalogError::not_found("table", &rel_name).into())
            }
        }
    }

    fn bind_table(
        schema_catalog: &SchemaCatalog,
        name: &String,
    ) -> crate::error::Result<RelationId> {
        if let Some(table) = schema_catalog.get_created_table_by_name(name) {
            Ok(table.id().as_relation_id())
        } else if let Some(source) = schema_catalog.get_source_by_name(name) {
            Ok(source.id.as_relation_id())
        } else {
            Err(CatalogError::not_found("table or source", name).into())
        }
    }
}

pub mod display {
    use risingwave_pb::stream_plan::BackfillOrder;

    use crate::session::SessionImpl;

    fn get_table_name(session: &SessionImpl, id: u32) -> crate::error::Result<String> {
        let catalog_reader = session.env().catalog_reader().read_guard();
        let table_catalog = catalog_reader.get_any_table_by_id(id.into())?;
        let table_name = table_catalog.name();
        let db_id = table_catalog.database_id;
        let schema_id = table_catalog.schema_id;
        let schema_catalog = catalog_reader.get_schema_by_id(db_id, schema_id)?;
        let schema_name = schema_catalog.name();
        let name = format!("{}.{}", schema_name, table_name);
        Ok(name)
    }

    pub(super) fn print_backfill_order_in_dot_format(
        session: &SessionImpl,
        order: BackfillOrder,
    ) -> crate::error::Result<String> {
        let mut result = String::new();
        result.push_str("digraph G {\n");
        // NOTE(kwannoel): This is a hack to make the edge ordering deterministic.
        // so our planner tests are deterministic.
        let mut edges = vec![];
        for (start, end) in order.order {
            let start_name = get_table_name(session, start.as_raw_id())?;
            for end in end.data {
                let end_name = get_table_name(session, end)?;
                edges.push(format!("  \"{}\" -> \"{}\";\n", start_name, end_name));
            }
        }
        edges.sort();
        for edge in edges {
            result.push_str(&edge);
        }
        result.push_str("}\n");
        Ok(result)
    }
}

/// We only bind tables and materialized views.
/// We need to bind sources and indices in the future as well.
/// For auto backfill strategy,
/// if a cycle forms due to the same relation being scanned twice in the derived order,
/// we won't generate any backfill order strategy.
/// For fixed backfill strategy,
/// for scans on the same relation id, even though they may be in different fragments,
/// they will all share the same backfill order.
pub fn plan_backfill_order(
    session: &SessionImpl,
    backfill_order_strategy: BackfillOrderStrategy,
    plan: StreamPlanRef,
) -> Result<BackfillOrder> {
    let order = match backfill_order_strategy {
        BackfillOrderStrategy::Default | BackfillOrderStrategy::None => Default::default(),
        BackfillOrderStrategy::Auto => plan_auto_strategy(session, plan),
        BackfillOrderStrategy::Fixed(orders) => plan_fixed_strategy(session, orders, plan)?,
    };
    Ok(BackfillOrder {
        order: order
            .into_iter()
            .map(|(relation_id, dependencies)| {
                (
                    relation_id,
                    Uint32Vector {
                        data: dependencies
                            .into_iter()
                            .map(RelationId::as_raw_id)
                            .collect(),
                    },
                )
            })
            .collect(),
    })
}

/// Plan the backfill order, and also output the backfill tree.
pub fn explain_backfill_order_in_dot_format(
    session: &SessionImpl,
    backfill_order_strategy: BackfillOrderStrategy,
    plan: StreamPlanRef,
) -> Result<String> {
    let order = plan_backfill_order(session, backfill_order_strategy, plan)?;
    let dot_formatted_backfill_order = display::print_backfill_order_in_dot_format(session, order)?;
    Ok(dot_formatted_backfill_order)
}
