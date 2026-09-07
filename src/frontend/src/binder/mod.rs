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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use itertools::Itertools;
use parking_lot::RwLock;
use risingwave_common::catalog::FunctionId;
use risingwave_common::session_config::{SearchPath, SessionConfig};
use risingwave_common::types::DataType;
use risingwave_common::util::iter_util::ZipEqDebug;
use risingwave_sqlparser::ast::Statement;

use crate::error::Result;

mod bind_context;
mod bind_param;
mod create;
mod create_view;
mod declare_cursor;
mod delete;
mod expr;
pub mod fetch_cursor;
mod for_system;
mod gap_fill_binder;
mod insert;
mod query;
mod relation;
mod select;
mod set_expr;
mod statement;
mod struct_field;
mod update;
mod values;

pub use bind_context::{BindContext, Clause, LateralBindContext};
pub use create_view::BoundCreateView;
pub use delete::BoundDelete;
pub use expr::bind_data_type;
pub use gap_fill_binder::BoundFillStrategy;
pub use insert::BoundInsert;
use pgwire::pg_server::{Session, SessionId};
pub use query::BoundQuery;
pub use relation::{
    BoundBaseTable, BoundGapFill, BoundIcebergMetadataTable, BoundJoin, BoundMatchRecognize,
    BoundMeasure, BoundShare, BoundShareInput, BoundSource, BoundSymbolDefinition,
    BoundSystemTable, BoundWatermark, BoundWindowTableFunction, MeasureSlotKind, Relation,
    ResolveQualifiedNameError, WindowTableFunctionKind,
};
// Re-export common types
pub use risingwave_common::gap_fill::FillStrategy;
use risingwave_common::id::ObjectId;
pub use select::{BoundDistinct, BoundSelect};
pub use set_expr::*;
pub use statement::BoundStatement;
pub use update::{BoundUpdate, UpdateProject};
pub use values::BoundValues;

use crate::catalog::catalog_service::CatalogReadGuard;
use crate::catalog::root_catalog::SchemaPath;
use crate::catalog::schema_catalog::SchemaCatalog;
use crate::catalog::{CatalogResult, DatabaseId, SecretId, ViewId};
use crate::error::ErrorCode;
use crate::session::{AuthContext, SessionImpl, StagingCatalogManager, TemporarySourceManager};
use crate::user::user_service::UserInfoReadGuard;

pub type ShareId = usize;

/// The type of binding statement.
enum BindFor {
    /// Binding MV/SINK
    Stream,
    /// Binding a batch query
    Batch,
    /// Binding a DDL (e.g. CREATE TABLE/SOURCE)
    Ddl,
    /// Binding a system query (e.g. SHOW)
    System,
}

/// `Binder` binds the identifiers in AST to columns in relations
pub struct Binder {
    // TODO: maybe we can only lock the database, but not the whole catalog.
    catalog: CatalogReadGuard,
    user: UserInfoReadGuard,
    db_name: String,
    database_id: DatabaseId,
    session_id: SessionId,
    context: BindContext,
    auth_context: Arc<AuthContext>,
    /// A stack holding contexts of outer queries when binding a subquery.
    /// It also holds all of the lateral contexts for each respective
    /// subquery.
    ///
    /// See [`Binder::bind_subquery_expr`] for details.
    upper_subquery_contexts: Vec<(BindContext, Vec<LateralBindContext>)>,

    /// A stack holding contexts of left-lateral `TableFactor`s.
    ///
    /// We need a separate stack as `CorrelatedInputRef` depth is
    /// determined by the upper subquery context depth, not the lateral context stack depth.
    lateral_contexts: Vec<LateralBindContext>,

    next_subquery_id: usize,
    next_values_id: usize,
    /// The `ShareId` is used to identify the share relation which could be a CTE, a source, a view
    /// and so on.
    next_share_id: ShareId,

    session_config: Arc<RwLock<SessionConfig>>,

    search_path: SearchPath,
    /// The type of binding statement.
    bind_for: BindFor,

    /// `ShareId`s identifying shared views.
    shared_views: HashMap<ViewId, ShareId>,

    /// The included relations while binding a query.
    included_relations: HashSet<ObjectId>,

    /// The included user-defined functions while binding a query.
    included_udfs: HashSet<FunctionId>,

    /// The included secrets while binding a query (e.g., secret refs in UDF arguments).
    included_secrets: HashSet<SecretId>,

    param_types: ParameterTypes,

    /// The temporary sources that will be used during binding phase
    temporary_source_manager: TemporarySourceManager,

    /// The staging catalogs that will be used during binding phase
    staging_catalog_manager: StagingCatalogManager,

    /// Information for `secure_compare` function. It's ONLY available when binding the
    /// `VALIDATE` clause of Webhook source i.e. `VALIDATE SECRET ... AS SECURE_COMPARE(...)`.
    secure_compare_context: Option<SecureCompareContext>,
}

pub const WEBHOOK_PAYLOAD_FIELD_NAME: &str = "payload";

// There are hidden names reserved for webhook validation expressions:
// - `headers`, whose type is `JSONB`
// - `payload`, whose type is `BYTEA`
#[derive(Default, Clone, Debug)]
pub struct SecureCompareContext {
    /// The identifier used to reference the raw webhook payload during validation.
    pub payload_name: String,
    /// The secret (usually a token provided by the webhook source user) to validate the calls
    pub secret_name: Option<String>,
}

/// `ParameterTypes` is used to record the types of the parameters during binding prepared stataments.
/// It works by following the rules:
/// 1. At the beginning, it contains the user specified parameters type.
/// 2. When the binder encounters a parameter, it will record it as unknown(call `record_new_param`)
///    if it didn't exist in `ParameterTypes`.
/// 3. When the binder encounters a cast on parameter, if it's a unknown type, the cast function
///    will record the target type as infer type for that parameter(call `record_infer_type`). If the
///    parameter has been inferred, the cast function will act as a normal cast.
/// 4. After bind finished:
///    (a) parameter not in `ParameterTypes` means that the user didn't specify it and it didn't
///    occur in the query. `export` will return error if there is a kind of
///    parameter. This rule is compatible with PostgreSQL
///    (b) parameter is None means that it's a unknown type. The user didn't specify it
///    and we can't infer it in the query. We will treat it as VARCHAR type finally. This rule is
///    compatible with PostgreSQL.
///    (c) parameter is Some means that it's a known type.
#[derive(Clone, Debug)]
pub struct ParameterTypes(Arc<RwLock<HashMap<u64, Option<DataType>>>>);

impl ParameterTypes {
    pub fn new(specified_param_types: Vec<Option<DataType>>) -> Self {
        let map = specified_param_types
            .into_iter()
            .enumerate()
            .map(|(index, data_type)| ((index + 1) as u64, data_type))
            .collect::<HashMap<u64, Option<DataType>>>();
        Self(Arc::new(RwLock::new(map)))
    }

    pub fn has_infer(&self, index: u64) -> bool {
        self.0.read().get(&index).unwrap().is_some()
    }

    pub fn read_type(&self, index: u64) -> Option<DataType> {
        self.0.read().get(&index).unwrap().clone()
    }

    pub fn record_new_param(&mut self, index: u64) {
        self.0.write().entry(index).or_insert(None);
    }

    pub fn record_infer_type(&mut self, index: u64, data_type: &DataType) {
        assert!(
            !self.has_infer(index),
            "The parameter has been inferred, should not be inferred again."
        );
        self.0
            .write()
            .get_mut(&index)
            .unwrap()
            .replace(data_type.clone());
    }

    pub fn export(&self) -> Result<Vec<DataType>> {
        let types = self
            .0
            .read()
            .clone()
            .into_iter()
            .sorted_by_key(|(index, _)| *index)
            .collect::<Vec<_>>();

        // Check if all the parameters have been inferred.
        for ((index, _), expect_index) in types.iter().zip_eq_debug(1_u64..=types.len() as u64) {
            if *index != expect_index {
                return Err(ErrorCode::InvalidInputSyntax(format!(
                    "Cannot infer the type of the parameter {}.",
                    expect_index
                ))
                .into());
            }
        }

        Ok(types
            .into_iter()
            .map(|(_, data_type)| data_type.unwrap_or(DataType::Varchar))
            .collect::<Vec<_>>())
    }
}

impl Binder {
    fn new(session: &SessionImpl, bind_for: BindFor) -> Binder {
        Binder {
            catalog: session.env().catalog_reader().read_guard(),
            user: session.env().user_info_reader().read_guard(),
            db_name: session.database(),
            database_id: session.database_id(),
            session_id: session.id(),
            context: BindContext::new(),
            auth_context: session.auth_context(),
            upper_subquery_contexts: vec![],
            lateral_contexts: vec![],
            next_subquery_id: 0,
            next_values_id: 0,
            next_share_id: 0,
            session_config: session.shared_config(),
            search_path: session.config().search_path(),
            bind_for,
            shared_views: HashMap::new(),
            included_relations: HashSet::new(),
            included_udfs: HashSet::new(),
            included_secrets: HashSet::new(),
            param_types: ParameterTypes::new(vec![]),
            temporary_source_manager: session.temporary_source_manager(),
            staging_catalog_manager: session.staging_catalog_manager(),
            secure_compare_context: None,
        }
    }

    pub fn new_for_batch(session: &SessionImpl) -> Binder {
        Self::new(session, BindFor::Batch)
    }

    pub fn new_for_stream(session: &SessionImpl) -> Binder {
        Self::new(session, BindFor::Stream)
    }

    pub fn new_for_ddl(session: &SessionImpl) -> Binder {
        Self::new(session, BindFor::Ddl)
    }

    pub fn new_for_system(session: &SessionImpl) -> Binder {
        Self::new(session, BindFor::System)
    }

    /// Set the specified parameter types.
    pub fn with_specified_params_types(mut self, param_types: Vec<Option<DataType>>) -> Self {
        self.param_types = ParameterTypes::new(param_types);
        self
    }

    /// Set the secure compare context.
    pub fn with_secure_compare(mut self, ctx: SecureCompareContext) -> Self {
        self.secure_compare_context = Some(ctx);
        self
    }

    fn is_for_stream(&self) -> bool {
        matches!(self.bind_for, BindFor::Stream)
    }

    #[expect(dead_code)]
    fn is_for_batch(&self) -> bool {
        matches!(self.bind_for, BindFor::Batch)
    }

    fn is_for_ddl(&self) -> bool {
        matches!(self.bind_for, BindFor::Ddl)
    }

    /// Bind a [`Statement`].
    pub fn bind(&mut self, stmt: Statement) -> Result<BoundStatement> {
        self.bind_statement(stmt)
    }

    pub fn export_param_types(&self) -> Result<Vec<DataType>> {
        self.param_types.export()
    }

    /// Get included relations in the query after binding. This is used for resolving relation
    /// dependencies. Note that it only contains referenced relations discovered during binding.
    /// After the plan is built, the referenced relations may be changed. We cannot rely on the
    /// collection result of plan, because we still need to record the dependencies that have been
    /// optimised away.
    pub fn included_relations(&self) -> &HashSet<ObjectId> {
        &self.included_relations
    }

    /// Get included user-defined functions in the query after binding.
    pub fn included_udfs(&self) -> &HashSet<FunctionId> {
        &self.included_udfs
    }

    /// Get included secrets in the query after binding (e.g., secret refs in UDF arguments).
    pub fn included_secrets(&self) -> &HashSet<SecretId> {
        &self.included_secrets
    }

    fn push_context(&mut self) {
        let new_context = std::mem::take(&mut self.context);
        self.context
            .cte_to_relation
            .clone_from(&new_context.cte_to_relation);
        self.context.disable_security_invoker = new_context.disable_security_invoker;
        let new_lateral_contexts = std::mem::take(&mut self.lateral_contexts);
        self.upper_subquery_contexts
            .push((new_context, new_lateral_contexts));
    }

    fn pop_context(&mut self) -> Result<()> {
        let (old_context, old_lateral_contexts) = self
            .upper_subquery_contexts
            .pop()
            .ok_or_else(|| ErrorCode::InternalError("Popping non-existent context".to_owned()))?;
        self.context = old_context;
        self.lateral_contexts = old_lateral_contexts;
        Ok(())
    }

    fn push_lateral_context(&mut self) {
        let new_context = std::mem::take(&mut self.context);
        self.context
            .cte_to_relation
            .clone_from(&new_context.cte_to_relation);
        self.context.disable_security_invoker = new_context.disable_security_invoker;
        self.lateral_contexts.push(LateralBindContext {
            is_visible: false,
            context: new_context,
        });
    }

    fn pop_and_merge_lateral_context(&mut self) -> Result<()> {
        let mut old_context = self
            .lateral_contexts
            .pop()
            .ok_or_else(|| ErrorCode::InternalError("Popping non-existent context".to_owned()))?
            .context;
        old_context.merge_context(self.context.clone())?;
        self.context = old_context;
        Ok(())
    }

    /// Make every enclosing left-hand `FROM` context visible while binding a lateral table
    /// factor. A lateral factor nested in a join tree may refer not only to its immediate left
    /// sibling, but also to left inputs of enclosing joins.
    fn mark_lateral_contexts_visible(&mut self) -> Vec<bool> {
        self.lateral_contexts
            .iter_mut()
            .map(|ctx| std::mem::replace(&mut ctx.is_visible, true))
            .collect()
    }

    fn restore_lateral_contexts_visibility(&mut self, visibility: Vec<bool>) {
        // Some table-factor binders return early on an error without unwinding their temporary
        // query context. The whole binder is discarded in that case, so there is no visibility
        // state to restore on the active stack.
        if self.lateral_contexts.len() != visibility.len() {
            return;
        }
        for (ctx, is_visible) in self.lateral_contexts.iter_mut().zip_eq_debug(visibility) {
            ctx.is_visible = is_visible;
        }
    }

    /// Returns a reverse iterator over the upper subquery contexts that are visible to the current
    /// context. Not to be confused with `is_visible` in [`LateralBindContext`].
    ///
    /// In most cases, this should include all the upper subquery contexts. However, when binding
    /// SQL UDFs, we should avoid resolving the context outside the UDF for hygiene.
    fn visible_upper_subquery_contexts_rev(
        &self,
    ) -> impl Iterator<Item = &(BindContext, Vec<LateralBindContext>)> + '_ {
        self.upper_subquery_contexts
            .iter()
            .rev()
            .take_while(|(context, _)| context.sql_udf_arguments.is_none())
    }

    fn next_subquery_id(&mut self) -> usize {
        let id = self.next_subquery_id;
        self.next_subquery_id += 1;
        id
    }

    fn next_values_id(&mut self) -> usize {
        let id = self.next_values_id;
        self.next_values_id += 1;
        id
    }

    fn next_share_id(&mut self) -> ShareId {
        let id = self.next_share_id;
        self.next_share_id += 1;
        id
    }

    fn first_valid_schema(&self) -> CatalogResult<&SchemaCatalog> {
        self.catalog.first_valid_schema(
            &self.db_name,
            &self.search_path,
            &self.auth_context.user_name,
        )
    }

    fn bind_schema_path<'a>(&'a self, schema_name: Option<&'a str>) -> SchemaPath<'a> {
        SchemaPath::new(schema_name, &self.search_path, &self.auth_context.user_name)
    }

    pub fn set_clause(&mut self, clause: Option<Clause>) {
        self.context.clause = clause;
    }
}

/// The column name stored in [`BindContext`] for a column without an alias.
pub const UNNAMED_COLUMN: &str = "?column?";
/// The table name stored in [`BindContext`] for a subquery without an alias.
const UNNAMED_SUBQUERY: &str = "?subquery?";
/// The table name stored in [`BindContext`] for a column group.
const COLUMN_GROUP_PREFIX: &str = "?column_group_id?";

#[cfg(test)]
pub mod test_utils {
    use risingwave_common::types::DataType;

    use super::Binder;
    use crate::session::SessionImpl;

    pub fn mock_binder() -> Binder {
        mock_binder_with_param_types(vec![])
    }

    pub fn mock_binder_with_param_types(param_types: Vec<Option<DataType>>) -> Binder {
        Binder::new_for_batch(&SessionImpl::mock()).with_specified_params_types(param_types)
    }
}

#[cfg(test)]
mod tests {
    use expect_test::expect;

    use super::test_utils::*;

    #[tokio::test]
    async fn test_bind_approx_percentile() {
        let stmt = risingwave_sqlparser::parser::Parser::parse_sql(
            "SELECT approx_percentile(0.5, 0.01) WITHIN GROUP (ORDER BY generate_series) FROM generate_series(1, 100)",
        ).unwrap().into_iter().next().unwrap();
        let parse_expected = expect![[r#"
            Query(
                Query {
                    with: None,
                    body: Select(
                        Select {
                            distinct: All,
                            projection: [
                                UnnamedExpr(
                                    Function(
                                        Function {
                                            scalar_as_agg: false,
                                            name: ObjectName(
                                                [
                                                    Ident {
                                                        value: "approx_percentile",
                                                        quote_style: None,
                                                    },
                                                ],
                                            ),
                                            arg_list: FunctionArgList {
                                                distinct: false,
                                                args: [
                                                    Unnamed(
                                                        Expr(
                                                            Value(
                                                                Number(
                                                                    "0.5",
                                                                ),
                                                            ),
                                                        ),
                                                    ),
                                                    Unnamed(
                                                        Expr(
                                                            Value(
                                                                Number(
                                                                    "0.01",
                                                                ),
                                                            ),
                                                        ),
                                                    ),
                                                ],
                                                variadic: false,
                                                order_by: [],
                                                ignore_nulls: false,
                                            },
                                            within_group: Some(
                                                OrderByExpr {
                                                    expr: Identifier(
                                                        Ident {
                                                            value: "generate_series",
                                                            quote_style: None,
                                                        },
                                                    ),
                                                    asc: None,
                                                    nulls_first: None,
                                                },
                                            ),
                                            filter: None,
                                            over: None,
                                        },
                                    ),
                                ),
                            ],
                            from: [
                                TableWithJoins {
                                    relation: TableFunction {
                                        name: ObjectName(
                                            [
                                                Ident {
                                                    value: "generate_series",
                                                    quote_style: None,
                                                },
                                            ],
                                        ),
                                        alias: None,
                                        args: [
                                            Unnamed(
                                                Expr(
                                                    Value(
                                                        Number(
                                                            "1",
                                                        ),
                                                    ),
                                                ),
                                            ),
                                            Unnamed(
                                                Expr(
                                                    Value(
                                                        Number(
                                                            "100",
                                                        ),
                                                    ),
                                                ),
                                            ),
                                        ],
                                        with_ordinality: false,
                                    },
                                    joins: [],
                                },
                            ],
                            lateral_views: [],
                            selection: None,
                            group_by: [],
                            having: None,
                            window: [],
                        },
                    ),
                    order_by: [],
                    limit: None,
                    offset: None,
                    fetch: None,
                },
            )"#]];
        parse_expected.assert_eq(&format!("{:#?}", stmt));

        let mut binder = mock_binder();
        let bound = binder.bind(stmt).unwrap();

        let expected = expect![[r#"
            Query(
                BoundQuery {
                    body: Select(
                        BoundSelect {
                            distinct: All,
                            select_items: [
                                AggCall(
                                    AggCall {
                                        agg_type: Builtin(
                                            ApproxPercentile,
                                        ),
                                        return_type: Float64,
                                        args: [
                                            FunctionCall(
                                                FunctionCall {
                                                    func_type: Cast,
                                                    return_type: Float64,
                                                    inputs: [
                                                        InputRef(
                                                            InputRef {
                                                                index: 0,
                                                                data_type: Int32,
                                                            },
                                                        ),
                                                    ],
                                                },
                                            ),
                                        ],
                                        filter: Condition {
                                            conjunctions: [],
                                        },
                                        distinct: false,
                                        order_by: OrderBy {
                                            sort_exprs: [
                                                OrderByExpr {
                                                    expr: InputRef(
                                                        InputRef {
                                                            index: 0,
                                                            data_type: Int32,
                                                        },
                                                    ),
                                                    order_type: OrderType {
                                                        direction: Ascending,
                                                        nulls_are: Largest,
                                                    },
                                                },
                                            ],
                                        },
                                        direct_args: [
                                            Literal {
                                                data: Some(
                                                    Float64(
                                                        0.5,
                                                    ),
                                                ),
                                                data_type: Some(
                                                    Float64,
                                                ),
                                            },
                                            Literal {
                                                data: Some(
                                                    Float64(
                                                        0.01,
                                                    ),
                                                ),
                                                data_type: Some(
                                                    Float64,
                                                ),
                                            },
                                        ],
                                    },
                                ),
                            ],
                            aliases: [
                                Some(
                                    "approx_percentile",
                                ),
                            ],
                            from: Some(
                                TableFunction {
                                    expr: TableFunction(
                                        FunctionCall {
                                            function_type: GenerateSeries,
                                            return_type: Int32,
                                            args: [
                                                Literal(
                                                    Literal {
                                                        data: Some(
                                                            Int32(
                                                                1,
                                                            ),
                                                        ),
                                                        data_type: Some(
                                                            Int32,
                                                        ),
                                                    },
                                                ),
                                                Literal(
                                                    Literal {
                                                        data: Some(
                                                            Int32(
                                                                100,
                                                            ),
                                                        ),
                                                        data_type: Some(
                                                            Int32,
                                                        ),
                                                    },
                                                ),
                                            ],
                                        },
                                    ),
                                    with_ordinality: false,
                                },
                            ),
                            where_clause: None,
                            group_by: GroupKey(
                                [],
                            ),
                            having: None,
                            window: {},
                            schema: Schema {
                                fields: [
                                    approx_percentile:Float64,
                                ],
                            },
                        },
                    ),
                    order: [],
                    limit: None,
                    offset: None,
                    with_ties: false,
                    extra_order_exprs: [],
                },
            )"#]];

        expected.assert_eq(&format!("{:#?}", bound));
    }
}
