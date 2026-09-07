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

use std::fmt::Debug;

use jsonbb::Builder;
use risingwave_common::types::{
    DataType, Date, Decimal, F32, F64, Int256Ref, Interval, JsonbRef, ListRef, MapRef, Scalar,
    ScalarRefImpl, Serial, StructRef, Time, Timestamp, Timestamptz, ToText, VariantRef, VectorRef,
};
use risingwave_common::util::iter_util::ZipEqDebug;
use risingwave_expr::expr::Context;
use risingwave_expr::{ExprError, Result, function};
use thiserror_ext::AsReport;

#[function("to_jsonb(*) -> jsonb")]
fn to_jsonb(
    input: Option<impl ToJsonb>,
    ctx: &Context,
    writer: &mut jsonbb::Builder,
) -> Result<()> {
    input.add_to(&ctx.arg_types[0], writer)?;
    Ok(())
}

/// Values that can be converted to JSONB.
///
/// This trait is implemented for all scalar reference types.
pub trait ToJsonb {
    fn add_to(self, data_type: &DataType, builder: &mut Builder) -> Result<()>;
}

impl<T: ToJsonb> ToJsonb for Option<T> {
    fn add_to(self, data_type: &DataType, builder: &mut Builder) -> Result<()> {
        match self {
            Some(inner) => inner.add_to(data_type, builder),
            None => {
                builder.add_null();
                Ok(())
            }
        }
    }
}

impl ToJsonb for ScalarRefImpl<'_> {
    fn add_to(self, ty: &DataType, builder: &mut Builder) -> Result<()> {
        use ScalarRefImpl::*;
        match self {
            Int16(v) => v.add_to(ty, builder),
            Int32(v) => v.add_to(ty, builder),
            Int64(v) => v.add_to(ty, builder),
            Int256(v) => v.add_to(ty, builder),
            Float32(v) => v.add_to(ty, builder),
            Float64(v) => v.add_to(ty, builder),
            Utf8(v) => v.add_to(ty, builder),
            Bool(v) => v.add_to(ty, builder),
            Decimal(v) => v.add_to(ty, builder),
            Interval(v) => v.add_to(ty, builder),
            Date(v) => v.add_to(ty, builder),
            Time(v) => v.add_to(ty, builder),
            Timestamp(v) => v.add_to(ty, builder),
            Jsonb(v) => v.add_to(ty, builder),
            Variant(v) => v.add_to(ty, builder),
            Serial(v) => v.add_to(ty, builder),
            Bytea(v) => v.add_to(ty, builder),
            Timestamptz(v) => v.add_to(ty, builder),
            Struct(v) => v.add_to(ty, builder),
            List(v) => v.add_to(ty, builder),
            Map(v) => v.add_to(ty, builder),
            Vector(v) => v.add_to(ty, builder),
        }
    }
}

impl ToJsonb for bool {
    fn add_to(self, _: &DataType, builder: &mut Builder) -> Result<()> {
        builder.add_bool(self);
        Ok(())
    }
}

impl ToJsonb for i16 {
    fn add_to(self, _: &DataType, builder: &mut Builder) -> Result<()> {
        builder.add_i64(self as _);
        Ok(())
    }
}

impl ToJsonb for i32 {
    fn add_to(self, _: &DataType, builder: &mut Builder) -> Result<()> {
        builder.add_i64(self as _);
        Ok(())
    }
}

impl ToJsonb for i64 {
    fn add_to(self, _: &DataType, builder: &mut Builder) -> Result<()> {
        builder.add_i64(self as _);
        Ok(())
    }
}

impl ToJsonb for F32 {
    fn add_to(self, _: &DataType, builder: &mut Builder) -> Result<()> {
        if self.0 == f32::INFINITY {
            builder.add_string("Infinity");
        } else if self.0 == f32::NEG_INFINITY {
            builder.add_string("-Infinity");
        } else if self.0.is_nan() {
            builder.add_string("NaN");
        } else {
            builder.add_f64(self.0 as f64);
        }
        Ok(())
    }
}

impl ToJsonb for F64 {
    fn add_to(self, _: &DataType, builder: &mut Builder) -> Result<()> {
        if self.0 == f64::INFINITY {
            builder.add_string("Infinity");
        } else if self.0 == f64::NEG_INFINITY {
            builder.add_string("-Infinity");
        } else if self.0.is_nan() {
            builder.add_string("NaN");
        } else {
            builder.add_f64(self.0);
        }
        Ok(())
    }
}

impl ToJsonb for Decimal {
    fn add_to(self, t: &DataType, builder: &mut Builder) -> Result<()> {
        let res: F64 = self
            .try_into()
            .map_err(|_| ExprError::CastOutOfRange("IEEE 754 double"))?;
        res.add_to(t, builder)?;
        Ok(())
    }
}

impl ToJsonb for Int256Ref<'_> {
    fn add_to(self, _: &DataType, builder: &mut Builder) -> Result<()> {
        builder.display(ToTextDisplay(self));
        Ok(())
    }
}

impl ToJsonb for &str {
    fn add_to(self, _: &DataType, builder: &mut Builder) -> Result<()> {
        builder.add_string(self);
        Ok(())
    }
}

impl ToJsonb for &[u8] {
    fn add_to(self, _: &DataType, builder: &mut Builder) -> Result<()> {
        builder.display(ToTextDisplay(self));
        Ok(())
    }
}

impl ToJsonb for Date {
    fn add_to(self, _: &DataType, builder: &mut Builder) -> Result<()> {
        builder.display(ToTextDisplay(self));
        Ok(())
    }
}

impl ToJsonb for Time {
    fn add_to(self, _: &DataType, builder: &mut Builder) -> Result<()> {
        builder.display(ToTextDisplay(self));
        Ok(())
    }
}

impl ToJsonb for Interval {
    fn add_to(self, _: &DataType, builder: &mut Builder) -> Result<()> {
        builder.display(ToTextDisplay(self));
        Ok(())
    }
}

impl ToJsonb for Timestamp {
    fn add_to(self, _: &DataType, builder: &mut Builder) -> Result<()> {
        builder.display(format_args!("{}T{}", self.0.date(), self.0.time()));
        Ok(())
    }
}

impl ToJsonb for Timestamptz {
    fn add_to(self, _: &DataType, builder: &mut Builder) -> Result<()> {
        let instant_local = self.to_datetime_utc();
        builder.display(instant_local.to_rfc3339().as_str());
        Ok(())
    }
}

impl ToJsonb for Serial {
    fn add_to(self, _: &DataType, builder: &mut Builder) -> Result<()> {
        builder.display(ToTextDisplay(self));
        Ok(())
    }
}

impl ToJsonb for VectorRef<'_> {
    fn add_to(self, _: &DataType, builder: &mut Builder) -> Result<()> {
        builder.display(ToTextDisplay(self));
        Ok(())
    }
}

impl ToJsonb for JsonbRef<'_> {
    fn add_to(self, _: &DataType, builder: &mut Builder) -> Result<()> {
        builder.add_value(self.into());
        Ok(())
    }
}

impl ToJsonb for VariantRef<'_> {
    fn add_to(self, _: &DataType, builder: &mut Builder) -> Result<()> {
        let jsonb = self
            .to_jsonb()
            .map_err(|e| ExprError::Parse(e.to_report_string().into()))?;
        builder.add_value(jsonb.as_scalar_ref().into());
        Ok(())
    }
}

impl ToJsonb for ListRef<'_> {
    fn add_to(self, data_type: &DataType, builder: &mut Builder) -> Result<()> {
        let elem_type = data_type.as_list_elem();
        builder.begin_array();
        for value in self.iter() {
            value.add_to(elem_type, builder)?;
        }
        builder.end_array();
        Ok(())
    }
}

impl ToJsonb for MapRef<'_> {
    fn add_to(self, data_type: &DataType, builder: &mut Builder) -> Result<()> {
        let value_type = data_type.as_map().value();
        builder.begin_object();
        for (k, v) in self.iter() {
            // XXX: is to_text here reasonable?
            builder.add_string(&k.to_text());
            v.add_to(value_type, builder)?;
        }
        builder.end_object();
        Ok(())
    }
}

impl ToJsonb for StructRef<'_> {
    fn add_to(self, data_type: &DataType, builder: &mut Builder) -> Result<()> {
        let struct_type = data_type.as_struct();
        if let Some(field_name) = struct_type.find_duplicate_field_name() {
            return Err(ExprError::InvalidParam {
                name: "to_jsonb",
                reason: format!("struct type has duplicate field name `{field_name}`").into(),
            });
        }

        builder.begin_object();
        for (value, (field_name, field_type)) in
            self.iter_fields_ref().zip_eq_debug(struct_type.iter())
        {
            builder.add_string(field_name);
            value.add_to(field_type, builder)?;
        }
        builder.end_object();
        Ok(())
    }
}

/// A wrapper type to implement `Display` for `ToText`.
pub struct ToTextDisplay<T>(pub T);

impl<T: ToText> std::fmt::Display for ToTextDisplay<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.write(f)
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::types::{ScalarImpl, StructType, StructValue};

    use super::*;

    fn assert_duplicate_error(value: &StructValue, data_type: &DataType, field_name: &str) {
        let mut builder = Builder::<Vec<u8>>::new();
        let error = value
            .as_scalar_ref()
            .add_to(data_type, &mut builder)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains(&format!(
                "struct type has duplicate field name `{field_name}`"
            )),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn struct_to_jsonb_rejects_duplicate_field_names() {
        let data_type = DataType::Struct(StructType::new([
            ("a", DataType::Int32),
            ("a", DataType::Varchar),
        ]));
        let value = StructValue::new(vec![
            Some(ScalarImpl::Int32(1)),
            Some(ScalarImpl::Utf8("hello".into())),
        ]);

        assert_duplicate_error(&value, &data_type, "a");
    }

    #[test]
    fn struct_to_jsonb_rejects_nested_duplicate_field_names() {
        let nested_type = DataType::Struct(StructType::new([
            ("a", DataType::Int32),
            ("a", DataType::Varchar),
        ]));
        let data_type = DataType::Struct(StructType::new([("nested", nested_type)]));
        let nested_value = StructValue::new(vec![
            Some(ScalarImpl::Int32(1)),
            Some(ScalarImpl::Utf8("hello".into())),
        ]);
        let value = StructValue::new(vec![Some(ScalarImpl::Struct(nested_value))]);

        assert_duplicate_error(&value, &data_type, "a");
    }

    #[test]
    fn struct_to_jsonb_preserves_all_unique_fields() {
        let data_type = DataType::Struct(StructType::new([
            ("a", DataType::Int32),
            ("A", DataType::Varchar),
        ]));
        let value = StructValue::new(vec![
            Some(ScalarImpl::Int32(1)),
            Some(ScalarImpl::Utf8("hello".into())),
        ]);
        let mut builder = Builder::<Vec<u8>>::new();

        value
            .as_scalar_ref()
            .add_to(&data_type, &mut builder)
            .unwrap();

        assert_eq!(builder.finish().to_string(), r#"{"A":"hello","a":1}"#);
    }
}
