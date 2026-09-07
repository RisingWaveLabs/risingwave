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

use risingwave_common::types::{DataType, Datum, ToText, literal_type_match};
use risingwave_common::util::value_encoding::{DatumFromProtoExt, DatumToProtoExt};
use risingwave_pb::expr::expr_node::RexNode;

use super::Expr;
use crate::expr::ExprType;
#[derive(Clone, Eq, PartialEq, Hash)]
pub struct Literal {
    data: Datum,
    // `null` or `'foo'` is of `unknown` type until used in a typed context (e.g. func arg)
    data_type: Option<DataType>,
}

impl std::fmt::Debug for Literal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if f.alternate() {
            f.debug_struct("Literal")
                .field("data", &self.data)
                .field("data_type", &self.data_type)
                .finish()
        } else {
            let data_type = self.return_type();
            match &self.data {
                None => write!(f, "null"),
                Some(v) => match data_type {
                    DataType::Boolean => write!(f, "{}", v.as_bool()),
                    DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::Serial
                    | DataType::Decimal
                    | DataType::Float32
                    | DataType::Float64 => write!(f, "{}", v.as_scalar_ref_impl().to_text()),
                    DataType::Varchar
                    | DataType::Bytea
                    | DataType::Date
                    | DataType::Timestamp
                    | DataType::Timestamptz
                    | DataType::Time
                    | DataType::Interval
                    | DataType::Jsonb
                    | DataType::Variant
                    | DataType::Int256
                    | DataType::Struct(_)
                    | DataType::Map(_)
                    | DataType::Vector(_) => write!(
                        f,
                        "'{}'",
                        v.as_scalar_ref_impl().to_text_with_type(&data_type)
                    ),
                    DataType::List { .. } => write!(f, "{}", v.as_list().display_for_explain()),
                },
            }?;
            write!(f, ":{:?}", data_type)
        }
    }
}

impl Literal {
    pub fn new(data: Datum, data_type: DataType) -> Self {
        assert!(
            literal_type_match(&data_type, data.as_ref()),
            "data_type: {:?}, data: {:?}",
            data_type,
            data
        );
        Literal {
            data,
            data_type: Some(data_type),
        }
    }

    pub fn new_untyped(data: Option<String>) -> Self {
        Literal {
            data: data.map(Into::into),
            data_type: None,
        }
    }

    pub fn get_data(&self) -> &Datum {
        &self.data
    }

    pub fn get_data_type(&self) -> &Option<DataType> {
        &self.data_type
    }

    pub fn is_untyped(&self) -> bool {
        self.data_type.is_none()
    }

    pub(super) fn from_expr_proto(
        proto: &risingwave_pb::expr::ExprNode,
    ) -> crate::error::Result<Self> {
        let data_type = proto.get_return_type()?;
        Ok(Self {
            data: value_encoding_to_literal(&proto.rex_node, &data_type.into())?,
            data_type: Some(data_type.into()),
        })
    }
}

impl Expr for Literal {
    fn return_type(&self) -> DataType {
        self.data_type.clone().unwrap_or(DataType::Varchar)
    }

    fn try_to_expr_proto(&self) -> Result<risingwave_pb::expr::ExprNode, String> {
        use risingwave_pb::expr::*;

        Ok(ExprNode {
            function_type: ExprType::Unspecified as i32,
            return_type: Some(self.return_type().to_protobuf()),
            rex_node: Some(literal_to_value_encoding(self.get_data())),
        })
    }
}

/// Convert a literal value (datum) into protobuf.
pub fn literal_to_value_encoding(d: &Datum) -> RexNode {
    RexNode::Constant(d.to_protobuf())
}

/// Convert protobuf into a literal value (datum).
fn value_encoding_to_literal(
    proto: &Option<RexNode>,
    ty: &DataType,
) -> crate::error::Result<Datum> {
    if let Some(rex_node) = proto {
        if let RexNode::Constant(prost_datum) = rex_node {
            let datum = Datum::from_protobuf(prost_datum, ty)?;
            Ok(datum)
        } else {
            unreachable!()
        }
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::{ListValue, StructValue};
    use risingwave_common::types::{DataType, Datum, ScalarImpl, StructType};
    use risingwave_common::util::value_encoding::DatumFromProtoExt;
    use risingwave_pb::expr::expr_node::RexNode;

    use crate::expr::literal::literal_to_value_encoding;

    #[test]
    fn test_struct_to_value_encoding() {
        let value = StructValue::new(vec![
            Some(ScalarImpl::Utf8("".into())),
            Some(2.into()),
            Some(3.into()),
        ]);
        let data = Some(ScalarImpl::Struct(value.clone()));
        let node = literal_to_value_encoding(&data);
        if let RexNode::Constant(prost) = node {
            let data2 = Datum::from_protobuf(
                &prost,
                &StructType::unnamed(vec![DataType::Varchar, DataType::Int32, DataType::Int32])
                    .into(),
            )
            .unwrap()
            .unwrap();
            assert_eq!(ScalarImpl::Struct(value), data2);
        }
    }

    #[test]
    fn test_list_to_value_encoding() {
        let value = ListValue::from_iter(["1", "2", ""]);
        let data = Some(ScalarImpl::List(value.clone()));
        let node = literal_to_value_encoding(&data);
        if let RexNode::Constant(prost) = node {
            let data2 = Datum::from_protobuf(&prost, &DataType::Varchar.list())
                .unwrap()
                .unwrap();
            assert_eq!(ScalarImpl::List(value), data2);
        }
    }
}
