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

/// `for_all_variants` includes all variants of our type system. If you introduced a new array type
/// (also known as scalar type or physical type), be sure to add a variant here.
///
/// It is used to simplify the boilerplate code of repeating all array types, while each type
/// has exactly the same code.
///
/// Take `Utf8` as an example, the layout of the variant is:
/// - `$data_type: Varchar` data type variant name, e.g. `DataType::Varchar`
/// - `$variant_name: Utf8` array type variant name, e.g. `ArrayImpl::Utf8`, `ScalarImpl::Utf8`
/// - `$suffix_name: utf8` the suffix of some functions, e.g. `ArrayImpl::as_utf8`
/// - `$scalar: Box<str>` the scalar type, e.g. `ScalarImpl::Utf8(Box<str>)`
/// - `$scalar_ref: &'scalar str` the scalar reference type, e.g. `ScalarRefImpl::Utf8(&'scalar
///   str)`
/// - `$array: Utf8Array` the array type, e.g. `ArrayImpl::Utf8(Utf8Array)`
/// - `$builder: Utf8ArrayBuilder` the array builder type, e.g.
///   `ArrayBuilderImpl::Utf8(Utf8ArrayBuilder)`
///
/// To use it, one need to provide another macro which accepts arguments in the layout described
/// above. Refer to the following implementations as examples.
///
/// **Note**: See also `dispatch_xx_variants` and `dispatch_data_types` which doesn't require
/// another macro for the implementation and can be easier to use in most cases.
#[macro_export]
macro_rules! for_all_variants {
    ($macro:ident $(, $x:tt)*) => {
        $macro! {
            $($x, )*
            //data_type     variant_name  suffix_name   scalar                      scalar_ref                          array                               builder
            { Int16,        Int16,        int16,        i16,                        i16,                                $crate::array::I16Array,            $crate::array::I16ArrayBuilder          },
            { Int32,        Int32,        int32,        i32,                        i32,                                $crate::array::I32Array,            $crate::array::I32ArrayBuilder          },
            { Int64,        Int64,        int64,        i64,                        i64,                                $crate::array::I64Array,            $crate::array::I64ArrayBuilder          },
            { Int256,       Int256,       int256,       $crate::types::Int256,      $crate::types::Int256Ref<'scalar>,  $crate::array::Int256Array,         $crate::array::Int256ArrayBuilder       },
            { Float32,      Float32,      float32,      $crate::types::F32,         $crate::types::F32,                 $crate::array::F32Array,            $crate::array::F32ArrayBuilder          },
            { Float64,      Float64,      float64,      $crate::types::F64,         $crate::types::F64,                 $crate::array::F64Array,            $crate::array::F64ArrayBuilder          },
            { Varchar,      Utf8,         utf8,         Box<str>,                   &'scalar str,                       $crate::array::Utf8Array,           $crate::array::Utf8ArrayBuilder         },
            { Boolean,      Bool,         bool,         bool,                       bool,                               $crate::array::BoolArray,           $crate::array::BoolArrayBuilder         },
            { Decimal,      Decimal,      decimal,      $crate::types::Decimal,     $crate::types::Decimal,             $crate::array::DecimalArray,        $crate::array::DecimalArrayBuilder      },
            { Interval,     Interval,     interval,     $crate::types::Interval,    $crate::types::Interval,            $crate::array::IntervalArray,       $crate::array::IntervalArrayBuilder     },
            { Date,         Date,         date,         $crate::types::Date,        $crate::types::Date,                $crate::array::DateArray,           $crate::array::DateArrayBuilder         },
            { Time,         Time,         time,         $crate::types::Time,        $crate::types::Time,                $crate::array::TimeArray,           $crate::array::TimeArrayBuilder         },
            { Timestamp,    Timestamp,    timestamp,    $crate::types::Timestamp,   $crate::types::Timestamp,           $crate::array::TimestampArray,      $crate::array::TimestampArrayBuilder    },
            { Timestamptz,  Timestamptz,  timestamptz,  $crate::types::Timestamptz, $crate::types::Timestamptz,         $crate::array::TimestamptzArray,    $crate::array::TimestamptzArrayBuilder  },
            { Jsonb,        Jsonb,        jsonb,        $crate::types::JsonbVal,    $crate::types::JsonbRef<'scalar>,   $crate::array::JsonbArray,          $crate::array::JsonbArrayBuilder        },
            { Variant,      Variant,      variant,      $crate::types::VariantVal,  $crate::types::VariantRef<'scalar>, $crate::array::VariantArray,        $crate::array::VariantArrayBuilder      },
            { Serial,       Serial,       serial,       $crate::types::Serial,      $crate::types::Serial,              $crate::array::SerialArray,         $crate::array::SerialArrayBuilder       },
            { Struct,       Struct,       struct,       $crate::types::StructValue, $crate::types::StructRef<'scalar>,  $crate::array::StructArray,         $crate::array::StructArrayBuilder       },
            { List,         List,         list,         $crate::types::ListValue,   $crate::types::ListRef<'scalar>,    $crate::array::ListArray,           $crate::array::ListArrayBuilder         },
            { Map,          Map,          map,          $crate::types::MapValue,    $crate::types::MapRef<'scalar>,     $crate::array::MapArray,            $crate::array::MapArrayBuilder          },
            { Vector,       Vector,       vector,       $crate::types::VectorVal,   $crate::types::VectorRef<'scalar>,  $crate::array::VectorArray,         $crate::array::VectorArrayBuilder       },
            { Bytea,        Bytea,        bytea,        Box<[u8]>,                  &'scalar [u8],                      $crate::array::BytesArray,          $crate::array::BytesArrayBuilder        }
        }
    };
}

/// Helper macro for expanding type aliases and constants. Internally used by `dispatch_` macros.
#[macro_export]
macro_rules! do_expand_alias {
    ($array:ty, $variant_name:ident, (
        $(Array, $array_alias:ident,)?
        $(ArrayBuilder, $array_builder_alias:ident,)?
        $(Scalar, $scalar_alias:ident,)?
        $(ScalarRef, $scalar_ref_alias:ident,)?
        $(VARIANT_NAME, $variant_name_alias:ident,)?
    )) => {
        $(type $array_alias = $array;)?
        $(type $array_builder_alias = <$array as $crate::array::Array>::Builder;)?
        $(type $scalar_alias = <$array as $crate::array::Array>::OwnedItem;)?
        $(type $scalar_ref_alias<'scalar> = <$array as $crate::array::Array>::RefItem<'scalar>;)?
        $(const $variant_name_alias: &'static str = stringify!($variant_name);)?
    };
}

/// Helper macro for generating dispatching code. Internally used by `dispatch_xx_variants` macros.
#[macro_export(local_inner_macros)]
macro_rules! do_dispatch_variants {
    // Use `tt` for `$alias` as a workaround of nested repetition.
    ($impl:expr, $type:ident, $inner:pat, [$alias:tt], $body:tt, $( { $data_type:ident, $variant_name:ident, $suffix_name:ident, $scalar:ty, $scalar_ref:ty, $array:ty, $builder:ty } ),*) => {
        match $impl {
            $( $type::$variant_name($inner) => {
                do_expand_alias!($array, $variant_name, $alias);
                #[allow(unused_braces)]
                $body
            }, )*
        }
    };
}

/// Dispatch the code block to all variants of `ArrayImpl`.
///
/// # Usage
///
/// The basic usage to access the inner concrete `impl Array` value is:
///
/// ```ignore
/// fn do_stuff<A: Array>(array: &A) { .. }
///
/// fn do_stuff_dispatch(array_impl: &ArrayImpl) {
///     dispatch_array_variants!(array_impl, array, {
///         do_stuff(array)
///     })
/// }
/// ```
///
/// One can also bind the inner concrete `impl Array` type to an alias:
///
/// ```ignore
/// fn do_stuff<A: Array>() { .. }
///
/// fn do_stuff_dispatch(array_impl: &ArrayImpl) {
///     dispatch_array_variants!(array_impl, [A = Array], {
///         do_stuff::<A>()
///     })
/// }
/// ```
///
/// There're more to bind, including type aliases of associated `ArrayBuilder`, `Scalar`, and
/// `ScalarRef`, or even the constant string of the variant name `VARIANT_NAME`. This can be
/// achieved by writing one or more of them in the square brackets. Due to the limitation of macro,
/// the order of the bindings matters.
///
/// ```ignore
/// fn do_stuff_dispatch(array_impl: &ArrayImpl) {
///     dispatch_array_variants!(
///         array_impl,
///         [A = Array, B = ArrayBuilder, S = Scalar, R = ScalarRef, N = VARIANT_NAME],
///         { .. }
///     )
/// }
/// ```
///
/// Alias bindings can also be used along with the inner value accessing:
///
/// ```ignore
/// fn do_stuff<A: Array>(array: &A) { .. }
///
/// fn do_stuff_dispatch(array_impl: &ArrayImpl) {
///     dispatch_array_variants!(array_impl, array, [A = Array], {
///         do_stuff::<A>(array)
///     })
/// }
/// ```
#[macro_export(local_inner_macros)]
macro_rules! dispatch_array_variants {
    ($impl:expr, [$($k:ident = $v:ident),*], $body:tt) => {
        dispatch_array_variants!($impl, _, [$($k = $v),*], $body)
    };
    ($impl:expr, $inner:pat, $body:tt) => {
        dispatch_array_variants!($impl, $inner, [], $body)
    };
    // Switch the order of alias bindings to avoid ambiguousness.
    ($impl:expr, $inner:pat, [$($k:ident = $v:ident),*], $body:tt) => {{
        use $crate::array::ArrayImpl;
        for_all_variants! { do_dispatch_variants, $impl, ArrayImpl, $inner, [($($v, $k,)*)], $body }
    }};
}

/// Dispatch the code block to all variants of `ArrayBuilderImpl`.
///
/// Refer to [`dispatch_array_variants`] for usage.
// TODO: avoid duplication by `macro_metavar_expr` feature
#[macro_export(local_inner_macros)]
macro_rules! dispatch_array_builder_variants {
    ($impl:expr, [$($k:ident = $v:ident),*], $body:tt) => {
        dispatch_array_builder_variants!($impl, _, [$($k = $v),*], $body)
    };
    ($impl:expr, $inner:pat, $body:tt) => {
        dispatch_array_builder_variants!($impl, $inner, [], $body)
    };
    ($impl:expr, $inner:pat, [$($k:ident = $v:ident),*], $body:tt) => {{
        use $crate::array::ArrayBuilderImpl;
        for_all_variants! { do_dispatch_variants, $impl, ArrayBuilderImpl, $inner, [($($v, $k,)*)], $body }
    }};
}

/// Dispatch the code block to all variants of `ScalarImpl`.
///
/// Refer to [`dispatch_array_variants`] for usage.
// TODO: avoid duplication by `macro_metavar_expr` feature
#[macro_export(local_inner_macros)]
macro_rules! dispatch_scalar_variants {
    ($impl:expr, [$($k:ident = $v:ident),*], $body:tt) => {
        dispatch_scalar_variants!($impl, _, [$($k = $v),*], $body)
    };
    ($impl:expr, $inner:pat, $body:tt) => {
        dispatch_scalar_variants!($impl, $inner, [], $body)
    };
    ($impl:expr, $inner:pat, [$($k:ident = $v:ident),*], $body:tt) => {{
        use $crate::types::ScalarImpl;
        for_all_variants! { do_dispatch_variants, $impl, ScalarImpl, $inner, [($($v, $k,)*)], $body }
    }};
}

/// Dispatch the code block to all variants of `ScalarRefImpl`.
///
/// Refer to [`dispatch_array_variants`] for usage.
// TODO: avoid duplication by `macro_metavar_expr` feature
#[macro_export(local_inner_macros)]
macro_rules! dispatch_scalar_ref_variants {
    ($impl:expr, [$($k:ident = $v:ident),*], $body:tt) => {
        dispatch_scalar_ref_variants!($impl, _, [$($k = $v),*], $body)
    };
    ($impl:expr, $inner:pat, $body:tt) => {
        dispatch_scalar_ref_variants!($impl, $inner, [], $body)
    };
    ($impl:expr, $inner:pat, [$($k:ident = $v:ident),*], $body:tt) => {{
        use $crate::types::ScalarRefImpl;
        for_all_variants! { do_dispatch_variants, $impl, ScalarRefImpl, $inner, [($($v, $k,)*)], $body }
    }};
}

/// Helper macro for generating dispatching code. Internally used by `dispatch_data_types` macros.
#[macro_export(local_inner_macros)]
macro_rules! do_dispatch_data_types {
    ($impl:expr, [$alias:tt], $body:tt, $( { $data_type:ident, $variant_name:ident, $suffix_name:ident, $scalar:ty, $scalar_ref:ty, $array:ty, $builder:ty } ),*) => {
        match $impl {
            $( $crate::types::DataType::$data_type { .. } => {
                do_expand_alias!($array, $variant_name, $alias);
                #[allow(unused_braces)]
                $body
            }, )*
        }
    };
}

/// Dispatch the code block to all variants of `DataType`.
///
/// There's no inner value to access, so only alias bindings are supported. Refer to
/// [`dispatch_array_variants`] for usage.
#[macro_export(local_inner_macros)]
macro_rules! dispatch_data_types {
    ($impl:expr, [$($k:ident = $v:ident),*], $body:tt) => {
        for_all_variants! { do_dispatch_data_types, $impl, [($($v, $k,)*)], $body }
    };
}
