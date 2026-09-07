// Copyright 2023 RisingWave Labs
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

use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::catalog::{FunctionId, Schema};
use risingwave_common::types::DataType;

use super::{Expr, ExprDisplay, ExprImpl};
use crate::catalog::function_catalog::{FunctionCatalog, FunctionKind};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UserDefinedFunction {
    pub args: Vec<ExprImpl>,
    pub catalog: Arc<FunctionCatalog>,
}

impl UserDefinedFunction {
    pub fn new(catalog: Arc<FunctionCatalog>, args: Vec<ExprImpl>) -> Self {
        Self { args, catalog }
    }

    pub(super) fn from_expr_proto(
        udf: &risingwave_pb::expr::UserDefinedFunction,
        return_type: DataType,
    ) -> crate::error::Result<Self> {
        let args: Vec<_> = udf
            .get_children()
            .iter()
            .map(ExprImpl::from_expr_proto)
            .try_collect()?;

        // function catalog
        let arg_types = udf.get_arg_types().iter().map_into().collect_vec();
        let catalog = FunctionCatalog {
            // FIXME(yuhao): function id is not in udf proto.
            id: FunctionId::placeholder(),
            name: udf.name.clone(),
            // FIXME(yuhao): owner is not in udf proto.
            owner: (u32::MAX - 1).into(),
            kind: FunctionKind::Scalar,
            arg_names: udf.arg_names.clone(),
            arg_types,
            return_type,
            language: udf.language.clone(),
            runtime: udf.runtime.clone(),
            name_in_runtime: udf.name_in_runtime().map(|x| x.to_owned()),
            body: udf.body.clone(),
            link: udf.link.clone(),
            compressed_binary: udf.compressed_binary.clone(),
            always_retry_on_network_error: udf.always_retry_on_network_error,
            unsafe_skip_materializing_exprs: false,
            is_batched: udf.is_batched,
            is_async: udf.is_async,
            created_at_epoch: None,
            created_at_cluster_version: None,
        };

        Ok(Self {
            args,
            catalog: Arc::new(catalog),
        })
    }
}

impl Expr for UserDefinedFunction {
    fn return_type(&self) -> DataType {
        self.catalog.return_type.clone()
    }

    fn try_to_expr_proto(&self) -> Result<risingwave_pb::expr::ExprNode, String> {
        use risingwave_pb::expr::expr_node::*;
        use risingwave_pb::expr::*;

        let children = self
            .args
            .iter()
            .map(|arg| arg.try_to_expr_proto())
            .try_collect()?;

        Ok(ExprNode {
            function_type: Type::Unspecified.into(),
            return_type: Some(self.return_type().to_protobuf()),
            rex_node: Some(RexNode::Udf(Box::new(UserDefinedFunction {
                children,
                name: self.catalog.name.clone(),
                arg_names: self.catalog.arg_names.clone(),
                arg_types: self
                    .catalog
                    .arg_types
                    .iter()
                    .map(|t| t.to_protobuf())
                    .collect(),
                language: self.catalog.language.clone(),
                runtime: self.catalog.runtime.clone(),
                identifier: self.catalog.name_in_runtime.clone(),
                link: self.catalog.link.clone(),
                body: self.catalog.body.clone(),
                compressed_binary: self.catalog.compressed_binary.clone(),
                always_retry_on_network_error: self.catalog.always_retry_on_network_error,
                is_async: self.catalog.is_async,
                is_batched: self.catalog.is_batched,
                version: PbUdfExprVersion::LATEST as _,
            }))),
        })
    }
}

pub struct UserDefinedFunctionDisplay<'a> {
    pub func_call: &'a UserDefinedFunction,
    pub input_schema: &'a Schema,
}

impl std::fmt::Debug for UserDefinedFunctionDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let that = self.func_call;
        let mut builder = f.debug_tuple(&that.catalog.name);
        for arg in &that.args {
            builder.field(&ExprDisplay {
                expr: arg,
                input_schema: self.input_schema,
            });
        }
        builder.finish()
    }
}
