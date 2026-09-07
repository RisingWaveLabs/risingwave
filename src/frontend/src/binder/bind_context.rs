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

use std::cell::RefCell;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::rc::Rc;

use parse_display::Display;
use risingwave_common::catalog::Field;
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::{TableAlias, WindowSpec};

use crate::binder::Relation;
use crate::error::{ErrorCode, Result};
use crate::expr::ExprImpl;

type LiteResult<T> = std::result::Result<T, ErrorCode>;

use crate::binder::{BoundQuery, COLUMN_GROUP_PREFIX, ShareId};

#[derive(Debug, Clone)]
pub struct ColumnBinding {
    pub table_name: String,
    pub schema_name: Option<String>,
    /// if the table has table alias, `table_alias` store the original table name
    pub table_alias: Option<String>,
    pub index: usize,
    pub is_hidden: bool,
    pub field: Field,
}

impl ColumnBinding {
    pub fn new(
        table_name: String,
        schema_name: Option<String>,
        table_alias: Option<String>,
        index: usize,
        is_hidden: bool,
        field: Field,
    ) -> Self {
        ColumnBinding {
            table_name,
            schema_name,
            table_alias,
            index,
            is_hidden,
            field,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Display)]
#[display(style = "TITLE CASE")]
pub enum Clause {
    Where,
    Values,
    GroupBy,
    JoinOn,
    Having,
    Filter,
    From,
    GeneratedColumn,
    Insert,
}

/// A `BindContext` that is only visible if the `LATERAL` keyword
/// is provided.
pub struct LateralBindContext {
    pub is_visible: bool,
    pub context: BindContext,
}

#[derive(Debug, Clone)]
pub enum BindingCteState {
    /// We get the whole bound result of the CTE.
    Bound {
        query: BoundQuery,
    },

    ChangeLog {
        table: Relation,
    },
}

#[derive(Clone, Debug)]
pub struct BindingCte {
    pub share_id: ShareId,
    pub state: BindingCteState,
    pub alias: TableAlias,
}

#[derive(Default, Debug, Clone)]
pub struct BindContext {
    // Columns of all tables.
    pub columns: Vec<ColumnBinding>,
    // Mapping column name to indices in `columns`.
    pub indices_of: HashMap<String, Vec<usize>>,
    // Mapping (schema name, table name) to [begin, end) of its columns.
    pub range_of: HashMap<(Option<String>, String), (usize, usize)>,
    // `clause` identifies in what clause we are binding.
    pub clause: Option<Clause>,
    // The `BindContext`'s data on its column groups
    pub column_group_context: ColumnGroupContext,
    /// Map the cte's name to its binding state.
    /// The `ShareId` in `BindingCte` of the value is used to help the planner identify the share plan.
    pub cte_to_relation: HashMap<String, Rc<RefCell<BindingCte>>>,
    /// Exposed relation names of CTE references in the current FROM scope.
    pub cte_relation_names: HashSet<String>,
    /// Current lambda functions's arguments
    pub lambda_args: Option<HashMap<String, (usize, DataType)>>,
    /// Whether the security invoker is set, currently only used for views.
    pub disable_security_invoker: bool,
    /// Named window definitions from the `WINDOW` clause
    pub named_windows: HashMap<String, WindowSpec>,
    /// Bound arguments for the current SQL UDF, if any.
    // TODO: use enum for named or positional arguments
    pub sql_udf_arguments: Option<HashMap<String, ExprImpl>>,
}

/// Holds the context for the `BindContext`'s `ColumnGroup`s.
#[derive(Default, Debug, Clone)]
pub struct ColumnGroupContext {
    // Maps naturally-joined/USING columns to their column group id
    pub mapping: HashMap<usize, u32>,
    // Maps column group ids to their column group data
    // We use a BTreeMap to ensure that iteration over the groups is ordered.
    pub groups: BTreeMap<u32, ColumnGroup>,

    next_group_id: u32,
}

/// When binding a natural join or a join with USING, a `ColumnGroup` contains the columns with the
/// same name.
#[derive(Default, Debug, Clone)]
pub struct ColumnGroup {
    /// Indices of the columns in the group
    pub indices: HashSet<usize>,
    /// A non-nullable column is never NULL.
    /// If `None`, ambiguous references to the column name will be resolved to a `COALESCE(col1,
    /// col2, ..., coln)` over each column in the group
    pub non_nullable_column: Option<usize>,

    pub column_name: Option<String>,
}

impl BindContext {
    pub fn get_column_binding_index(
        &self,
        schema_name: &Option<String>,
        table_name: &Option<String>,
        column_name: &String,
    ) -> LiteResult<usize> {
        match &self.get_column_binding_indices(schema_name, table_name, column_name)?[..] {
            [] => unreachable!(),
            [idx] => Ok(*idx),
            _ => Err(ErrorCode::InternalError(format!(
                "Ambiguous column name: {}",
                column_name
            ))),
        }
    }

    /// If return Vec has len > 1, it means we have an unqualified reference to a column which has
    /// been naturally joined upon, wherein none of the columns are min-nullable. This will be
    /// handled in downstream as a `COALESCE` expression
    pub fn get_column_binding_indices(
        &self,
        schema_name: &Option<String>,
        table_name: &Option<String>,
        column_name: &String,
    ) -> LiteResult<Vec<usize>> {
        match table_name {
            Some(table_name) => {
                if let Some(group_id_str) = table_name.strip_prefix(COLUMN_GROUP_PREFIX) {
                    let group_id = group_id_str.parse::<u32>().map_err(|_|ErrorCode::InternalError(
                        format!("Could not parse {:?} as virtual table name `{COLUMN_GROUP_PREFIX}[group_id]`", table_name)))?;
                    self.get_indices_with_group_id(group_id, column_name)
                } else {
                    Ok(vec![self.get_index_with_table_name(
                        column_name,
                        table_name,
                        schema_name,
                    )?])
                }
            }
            None => self.get_unqualified_indices(column_name),
        }
    }

    pub fn get_table_alias(
        &self,
        schema_name: &String,
        table_name: &String,
        column_name: &String,
    ) -> LiteResult<Option<usize>> {
        let column_indexes = self
            .indices_of
            .get(column_name)
            .ok_or_else(|| ErrorCode::ItemNotFound(format!("Invalid column: {}", column_name)))?;
        for index in column_indexes {
            let column = &self.columns[*index];
            if let (Some(schema), Some(table_alias)) = (&column.schema_name, &column.table_alias)
                && schema == schema_name
                && table_alias == table_name
            {
                return Ok(Some(*index));
            }
        }
        Ok(None)
    }

    fn get_indices_with_group_id(
        &self,
        group_id: u32,
        column_name: &String,
    ) -> LiteResult<Vec<usize>> {
        let group = self.column_group_context.groups.get(&group_id).unwrap();
        if let Some(name) = &group.column_name {
            debug_assert_eq!(name, column_name);
        }
        if let Some(non_nullable) = &group.non_nullable_column {
            Ok(vec![*non_nullable])
        } else {
            // These will be converted to a `COALESCE(col1, col2, ..., coln)`
            let mut indices: Vec<_> = group.indices.iter().copied().collect();
            indices.sort(); // ensure a deterministic result
            Ok(indices)
        }
    }

    pub fn get_unqualified_indices(&self, column_name: &String) -> LiteResult<Vec<usize>> {
        let columns = self
            .indices_of
            .get(column_name)
            .ok_or_else(|| ErrorCode::ItemNotFound(format!("Invalid column: {column_name}")))?;
        if columns.len() > 1 {
            // If there is some group containing the columns and the ambiguous columns are all in
            // the group
            if let Some(group_id) = self.column_group_context.mapping.get(&columns[0]) {
                let group = self.column_group_context.groups.get(group_id).unwrap();
                if columns.iter().all(|idx| group.indices.contains(idx)) {
                    if let Some(non_nullable) = &group.non_nullable_column {
                        return Ok(vec![*non_nullable]);
                    } else {
                        // These will be converted to a `COALESCE(col1, col2, ..., coln)`
                        return Ok(columns.clone());
                    }
                }
            }
            Err(ErrorCode::InternalError(format!(
                "Ambiguous column name: {}",
                column_name
            )))
        } else {
            Ok(columns.clone())
        }
    }

    pub fn check_catalog_name(&self, exposed_relation_name: &str) -> Result<()> {
        if self.cte_relation_names.contains(exposed_relation_name) {
            return Err(ErrorCode::DuplicateRelationName(format!(
                "table name \"{}\" specified more than once",
                exposed_relation_name
            ))
            .into());
        }
        Ok(())
    }

    pub fn add_cte_name(&mut self, exposed_relation_name: String) {
        self.cte_relation_names.insert(exposed_relation_name);
    }

    fn has_relation_name(&self, relation_name: &str) -> bool {
        self.range_of
            .keys()
            .any(|(_, existing_name)| existing_name == relation_name)
    }

    pub fn check_relation_name_conflict(&self, relation_name: &str) -> Result<()> {
        if self.has_relation_name(relation_name) {
            return Err(ErrorCode::DuplicateRelationName(format!(
                "table name \"{}\" specified more than once",
                relation_name
            ))
            .into());
        }
        Ok(())
    }

    fn check_cte_relation_name_conflict(&self, other: &Self) -> Result<()> {
        // `self` may bind a CTE in a separate context, while `other` already has a catalog
        // relation with the same exposed name.
        for cte_relation_name in &self.cte_relation_names {
            other.check_relation_name_conflict(cte_relation_name)?;
        }
        // `other` may bind a CTE in a separate context, while `self` already has a catalog
        // relation with the same exposed name.
        for cte_relation_name in &other.cte_relation_names {
            self.check_relation_name_conflict(cte_relation_name)?;
        }
        Ok(())
    }

    /// Identifies two columns as being in the same group. Additionally, possibly provides one of
    /// the columns as being `non_nullable`
    pub fn add_natural_columns(
        &mut self,
        left: usize,
        right: usize,
        non_nullable_column: Option<usize>,
    ) {
        match (
            self.column_group_context.mapping.get(&left).copied(),
            self.column_group_context.mapping.get(&right).copied(),
        ) {
            (None, None) => {
                let group_id = self.column_group_context.next_group_id;
                self.column_group_context.next_group_id += 1;

                let group = ColumnGroup {
                    indices: HashSet::from([left, right]),
                    non_nullable_column,
                    column_name: Some(self.columns[left].field.name.clone()),
                };
                self.column_group_context.groups.insert(group_id, group);
                self.column_group_context.mapping.insert(left, group_id);
                self.column_group_context.mapping.insert(right, group_id);
            }
            (Some(group_id), None) => {
                let group = self.column_group_context.groups.get_mut(&group_id).unwrap();
                group.indices.insert(right);
                if group.non_nullable_column.is_none() {
                    group.non_nullable_column = non_nullable_column;
                }
                self.column_group_context.mapping.insert(right, group_id);
            }
            (None, Some(group_id)) => {
                let group = self.column_group_context.groups.get_mut(&group_id).unwrap();
                group.indices.insert(left);
                if group.non_nullable_column.is_none() {
                    group.non_nullable_column = non_nullable_column;
                }
                self.column_group_context.mapping.insert(left, group_id);
            }
            (Some(l_group_id), Some(r_group_id)) => {
                if r_group_id == l_group_id {
                    return;
                }

                let r_group = self
                    .column_group_context
                    .groups
                    .remove(&r_group_id)
                    .unwrap();
                let l_group = self
                    .column_group_context
                    .groups
                    .get_mut(&l_group_id)
                    .unwrap();

                for idx in &r_group.indices {
                    *self.column_group_context.mapping.get_mut(idx).unwrap() = l_group_id;
                    l_group.indices.insert(*idx);
                }
                if l_group.non_nullable_column.is_none() {
                    l_group.non_nullable_column = if r_group.non_nullable_column.is_none() {
                        non_nullable_column
                    } else {
                        r_group.non_nullable_column
                    };
                }
            }
        }
    }

    /// Resolve a qualified column reference (`table.column` or `schema.table.column`)
    /// to one concrete column index in the current bind scope.
    ///
    /// This function intentionally uses a two-step strategy:
    /// 1. Resolve relation first (`resolve_relation_range`), including ambiguity checks.
    /// 2. Resolve column inside that unique relation range (`resolve_column_in_range`).
    fn get_index_with_table_name(
        &self,
        column_name: &String,
        table_name: &String,
        schema_name: &Option<String>,
    ) -> LiteResult<usize> {
        let chosen_range = self.resolve_relation_range(table_name, schema_name)?;
        self.resolve_column_in_range(column_name, table_name, chosen_range)
    }

    /// Resolve the relation range for a `table_name` in the current bind scope.
    ///
    /// Alias matches are prioritized over base relation name matches.
    pub(super) fn resolve_relation_range(
        &self,
        table_name: &String,
        schema_name: &Option<String>,
    ) -> LiteResult<(usize, usize)> {
        let mut alias_ranges = Vec::new();
        let mut base_ranges = Vec::new();

        for ((rel_schema, rel_name), range) in &self.range_of {
            if rel_name != table_name
                || !schema_name
                    .as_deref()
                    .is_none_or(|s| rel_schema.as_deref() == Some(s))
            {
                continue;
            }

            // Empty ranges have no column binding from which to read `table_alias`. Explicit
            // aliases are stored without a schema in `range_of`.
            let is_alias = if range.0 == range.1 {
                rel_schema.is_none()
            } else {
                self.columns[range.0].table_alias.is_some()
            };
            if is_alias {
                alias_ranges.push(*range);
            } else {
                base_ranges.push(*range);
            }
        }

        let chosen_ranges = if alias_ranges.is_empty() {
            base_ranges
        } else {
            alias_ranges
        };

        match chosen_ranges.as_slice() {
            [] => Err(ErrorCode::ItemNotFound(format!(
                "missing FROM-clause entry for table \"{}\"",
                table_name
            ))),
            [range] => Ok(*range),
            _ => Err(ErrorCode::InvalidReference(format!(
                "table reference \"{}\" is ambiguous",
                table_name
            ))),
        }
    }

    /// Resolve `column_name` inside a pre-resolved relation range.
    fn resolve_column_in_range(
        &self,
        column_name: &String,
        table_name: &String,
        range: (usize, usize),
    ) -> LiteResult<usize> {
        let idxs = self
            .indices_of
            .get(column_name)
            .ok_or_else(|| ErrorCode::ItemNotFound(format!("Invalid column: {}", column_name)))?;

        let matched: Vec<_> = idxs
            .iter()
            .copied()
            .filter(|index| (range.0..range.1).contains(index))
            .collect();

        match matched.as_slice() {
            [] => Err(ErrorCode::ItemNotFound(format!(
                "missing FROM-clause entry for table \"{}\"",
                table_name
            ))),
            [column_index] => Ok(*column_index),
            _ => Err(ErrorCode::InvalidReference(format!(
                "column reference \"{}\" is ambiguous",
                column_name
            ))),
        }
    }

    /// Merges two `BindContext`s which are adjacent. For instance, the `BindContext` of two
    /// adjacent cross-joined tables.
    pub fn merge_context(&mut self, other: Self) -> Result<()> {
        self.check_cte_relation_name_conflict(&other)?;

        let begin = self.columns.len();
        self.columns.extend(other.columns.into_iter().map(|mut c| {
            c.index += begin;
            c
        }));
        for (k, v) in other.indices_of {
            let entry = self.indices_of.entry(k).or_default();
            entry.extend(v.into_iter().map(|x| x + begin));
        }
        for (k, (x, y)) in other.range_of {
            match self.range_of.entry(k) {
                Entry::Occupied(e) => {
                    return Err(ErrorCode::InternalError(format!(
                        "Duplicated table name while merging adjacent contexts: {}",
                        e.key().1
                    ))
                    .into());
                }
                Entry::Vacant(entry) => {
                    entry.insert((begin + x, begin + y));
                }
            }
        }
        self.cte_relation_names.extend(other.cte_relation_names);
        // To merge the column_group_contexts, we just need to offset RHS
        // with the next_group_id of LHS.
        let ColumnGroupContext {
            mapping,
            groups,
            next_group_id,
        } = other.column_group_context;

        let offset = self.column_group_context.next_group_id;
        for (idx, group_id) in mapping {
            self.column_group_context
                .mapping
                .insert(begin + idx, offset + group_id);
        }
        for (group_id, mut group) in groups {
            group.indices = group.indices.into_iter().map(|idx| idx + begin).collect();
            if let Some(col) = &mut group.non_nullable_column {
                *col += begin;
            }
            self.column_group_context
                .groups
                .insert(offset + group_id, group);
        }
        self.column_group_context.next_group_id += next_group_id;

        // we assume that the clause is contained in the outer-level context
        Ok(())
    }
}

impl BindContext {
    pub fn new() -> Self {
        Self::default()
    }
}
