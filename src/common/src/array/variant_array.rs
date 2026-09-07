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

use std::sync::LazyLock;

use risingwave_common_estimate_size::EstimateSize;
use risingwave_pb::data::{PbArray, PbArrayType};

use super::{Array, ArrayBuilder, BytesArray, BytesArrayBuilder};
use crate::bitmap::Bitmap;
use crate::types::{DataType, Scalar, VariantRef, VariantVal};

/// Returned by raw iteration for NULL entries, whose empty buffer is not a valid variant.
static NULL_VARIANT_PLACEHOLDER: LazyLock<VariantVal> = LazyLock::new(VariantVal::null);

/// `VariantArray` is a collection of Parquet Variant values. It's a wrapper of [`BytesArray`],
/// and every non-null slot holds bytes accepted by [`VariantRef::from_serialized`].
#[derive(Debug, Clone, PartialEq, Eq, EstimateSize)]
pub struct VariantArray {
    bytes: BytesArray,
}

impl Array for VariantArray {
    type Builder = VariantArrayBuilder;
    type OwnedItem = VariantVal;
    type RefItem<'a> = VariantRef<'a>;

    unsafe fn raw_value_at_unchecked(&self, idx: usize) -> Self::RefItem<'_> {
        if unsafe { !self.bytes.null_bitmap().is_set_unchecked(idx) } {
            return NULL_VARIANT_PLACEHOLDER.as_scalar_ref();
        }
        VariantRef::from_serialized_unchecked(unsafe { self.bytes.raw_value_at_unchecked(idx) })
    }

    #[inline]
    fn len(&self) -> usize {
        self.bytes.len()
    }

    #[inline]
    fn to_protobuf(&self) -> PbArray {
        PbArray {
            array_type: PbArrayType::Variant as i32,
            ..self.bytes.to_protobuf()
        }
    }

    fn null_bitmap(&self) -> &Bitmap {
        self.bytes.null_bitmap()
    }

    fn into_null_bitmap(self) -> Bitmap {
        self.bytes.into_null_bitmap()
    }

    fn set_bitmap(&mut self, bitmap: Bitmap) {
        self.bytes.set_bitmap(bitmap);
    }

    fn data_type(&self) -> DataType {
        DataType::Variant
    }
}

#[derive(Debug, Clone, EstimateSize)]
pub struct VariantArrayBuilder {
    bytes: BytesArrayBuilder,
}

impl ArrayBuilder for VariantArrayBuilder {
    type ArrayType = VariantArray;

    fn new(capacity: usize) -> Self {
        Self {
            bytes: BytesArrayBuilder::new(capacity),
        }
    }

    fn with_type(capacity: usize, ty: DataType) -> Self {
        assert_eq!(ty, DataType::Variant);
        Self::new(capacity)
    }

    #[inline]
    fn append_n(&mut self, n: usize, value: Option<VariantRef<'_>>) {
        self.bytes.append_n(n, value.map(|v| v.as_bytes()));
    }

    #[inline]
    fn append_array(&mut self, other: &VariantArray) {
        self.bytes.append_array(&other.bytes);
    }

    #[inline]
    fn pop(&mut self) -> Option<()> {
        self.bytes.pop()
    }

    fn len(&self) -> usize {
        self.bytes.len()
    }

    fn finish(self) -> VariantArray {
        VariantArray {
            bytes: self.bytes.finish(),
        }
    }
}

impl FromIterator<Option<VariantVal>> for VariantArray {
    fn from_iter<I: IntoIterator<Item = Option<VariantVal>>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let mut builder = <Self as Array>::Builder::new(iter.size_hint().0);
        for i in iter {
            builder.append(i.as_ref().map(|v| v.as_scalar_ref()));
        }
        builder.finish()
    }
}

impl FromIterator<VariantVal> for VariantArray {
    fn from_iter<I: IntoIterator<Item = VariantVal>>(iter: I) -> Self {
        iter.into_iter().map(Some).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::ArrayImpl;

    fn variant(text: &str) -> VariantVal {
        text.parse().unwrap()
    }

    #[test]
    fn raw_iter_tolerates_null_slots() {
        let array: VariantArray = [Some(variant("1")), None].into_iter().collect();

        assert!(array.value_at(0).is_some());
        assert!(array.value_at(1).is_none());

        let texts: Vec<_> = array.raw_iter().map(|v| v.to_string()).collect();
        assert_eq!(texts, ["1", "null"]);
    }

    #[test]
    fn protobuf_round_trip_preserves_values_and_nulls() {
        let array: VariantArray = [
            Some(variant("1")),
            None,
            Some(variant(r#"{"a":[1,2],"b":"x"}"#)),
            Some(variant("null")),
        ]
        .into_iter()
        .collect();

        let decoded = ArrayImpl::from_protobuf(&array.to_protobuf(), array.len()).unwrap();
        assert_eq!(ArrayImpl::from(array), decoded);
    }

    #[test]
    fn rejects_invalid_serialized_values_from_protobuf() {
        let array: VariantArray = [Some(variant("1"))].into_iter().collect();
        let mut proto = array.to_protobuf();
        // Keep the original length, so the failure comes from validating the bytes rather than
        // from the data buffer running short.
        proto.values[1].body = vec![0xFF; proto.values[1].body.len()];

        let err = ArrayImpl::from_protobuf(&proto, 1).unwrap_err();
        assert!(
            err.to_string()
                .contains("failed to read variant from bytes"),
            "{err:?}"
        );
    }

    #[test]
    fn append_array_concatenates() {
        let left: VariantArray = [Some(variant("1")), None].into_iter().collect();
        let right: VariantArray = [Some(variant(r#""x""#))].into_iter().collect();

        let mut builder = VariantArrayBuilder::new(3);
        builder.append_array(&left);
        builder.append_array(&right);
        let joined = builder.finish();

        assert_eq!(joined.len(), 3);
        let texts: Vec<_> = joined
            .iter()
            .map(|v| v.map(|v| v.to_string()))
            .collect::<Vec<_>>();
        assert_eq!(
            texts,
            [Some("1".to_owned()), None, Some("\"x\"".to_owned())]
        );
    }
}
