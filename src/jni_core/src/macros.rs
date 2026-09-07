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

// Utils
/// A macro that splits the input by comma and calls the callback `$macro` with the split result.
/// The each part of the split result is within a bracket, and the callback `$macro` can have its
/// first parameter as `{$({$($args:tt)+})*}` to match the split result.
///
/// ```
/// macro_rules! call_split_by_comma {
///     ({$({$($args:tt)+})*}) => {
///         [$(
///            stringify! {$($args)+}
///         ),*]
///     };
///     ($($input:tt)*) => {{
///         risingwave_jni_core::split_by_comma! {
///             {$($input)*},
///             call_split_by_comma
///         }
///     }};
/// }
///
/// let expected_result = [
///     "hello",
///     "my friend",
/// ];
///
/// assert_eq!(expected_result, {call_split_by_comma!(hello, my friend)});
/// ```
#[macro_export]
macro_rules! split_by_comma {
    // Entry of macro. Put input as the first parameter and initialize empty
    // second and third parameter.
    (
        {$($input:tt)*},
        $macro:path $(,$args:tt)*
    ) => {
        $crate::split_by_comma! {
            {$($input)*},
            {}, // previous splits
            {}, // current split
            $macro $(,$args)*
        }
    };
    // When the first token is comma, move the tokens of current split to the
    // list of previous splits with a bracket wrapped, and then clear the current
    // split.
    (
        {
            ,
            $($rest:tt)*
        },
        {
            $(
                {$($prev_split:tt)*}
            )*
        },
        {
            $($current_split:tt)*
        },
        $macro:path $(,$args:tt)*
    ) => {
        $crate::split_by_comma! {
            {
                $($rest)*
            },
            {
                $(
                    {$($prev_split)*}
                )*
                {
                    $($current_split)*
                }
            },
            {},
            $macro $(,$args)*
        }
    };
    // Do the same as the previous match when we just reach the end of input,
    // with the content of last split not added to the list of previous split yet.
    (
        {},
        {
            $(
                {$($prev_split:tt)*}
            )*
        },
        {
            $($current_split:tt)+
        },
        $macro:path $(,$args:tt)*
    ) => {
        $crate::split_by_comma! {
            {},
            {
                $(
                    {$($prev_split)*}
                )*
                {
                    $($current_split)+
                }
            },
            {},
            $macro $(,$args)*
        }
    };
    // For a token that is not comma, add to the list of current staging split.
    (
        {
            $first:tt
            $($rest:tt)*
        },
        {
            $(
                {$($prev_split:tt)*}
            )*
        },
        {
            $($current_split:tt)*
        },
        $macro:path $(,$args:tt)*
    ) => {
        $crate::split_by_comma! {
            {
                $($rest)*
            },
            {
                $(
                    {$($prev_split)*}
                )*
            },
            {
                $($current_split)* $first
            },
            $macro $(,$args)*
        }
    };
    // When all split result are added to the list, call the callback `$macro` with the result.
    (
        {},
        {
            $(
                {$($prev_split:tt)*}
            )*
        },
        {},
        $macro:path $(,$args:tt)*
    ) => {
        $macro! {
            {
                $(
                    {$($prev_split)*}
                )*
            }
            $(,$args)*
        }
    };
}
// End of utils part

/// Generate the dot separated java class name to the slash separated name.
///
/// ```
/// assert_eq!(
///     "java/lang/String",
///     risingwave_jni_core::gen_class_name!(java.lang.String)
/// );
/// assert_eq!(
///     "java/lang/String",
///     risingwave_jni_core::gen_class_name!(String)
/// );
/// ```
#[macro_export]
macro_rules! gen_class_name {
    // A single part class name will be prefixed with `java.lang.`
    ($single_part_class:ident $($param_name:ident)?) => {
        $crate::gen_class_name! { @inner java.lang.$single_part_class }
    };
    ($($class:ident).+ $($param_name:ident)?) => {
        $crate::gen_class_name! { @inner $($class).+ }
    };
    (@inner $last:ident) => {
        stringify! {$last}
    };
    (@inner $first:ident . $($rest:ident).+) => {
        concat! {stringify! {$first}, "/", $crate::gen_class_name! {@inner $($rest).+} }
    }
}

/// Generate the type signature of a single type
/// ```
/// use risingwave_jni_core::gen_jni_type_sig;
/// assert_eq!("Z", gen_jni_type_sig!(boolean));
/// assert_eq!("B", gen_jni_type_sig!(byte));
/// assert_eq!("C", gen_jni_type_sig!(char));
/// assert_eq!("S", gen_jni_type_sig!(short));
/// assert_eq!("I", gen_jni_type_sig!(int));
/// assert_eq!("J", gen_jni_type_sig!(long));
/// assert_eq!("F", gen_jni_type_sig!(float));
/// assert_eq!("D", gen_jni_type_sig!(double));
/// assert_eq!("V", gen_jni_type_sig!(void));
/// assert_eq!("Ljava/lang/Class;", gen_jni_type_sig!(Class<?>));
/// assert_eq!("Ljava/lang/String;", gen_jni_type_sig!(String));
/// assert_eq!("[B", gen_jni_type_sig!(byte[]));
/// ```
#[macro_export]
macro_rules! gen_jni_type_sig {
    (boolean $($param_name:ident)?) => {
        "Z"
    };
    (byte $($param_name:ident)?) => {
        "B"
    };
    (char $($param_name:ident)?) => {
        "C"
    };
    (short $($param_name:ident)?) => {
        "S"
    };
    (int $($param_name:ident)?) => {
        "I"
    };
    (long $($param_name:ident)?) => {
        "J"
    };
    (float $($param_name:ident)?) => {
        "F"
    };
    (double $($param_name:ident)?) => {
        "D"
    };
    (void) => {
        "V"
    };
    (Class $(< ? >)? $($param_name:ident)?) => {
        $crate::gen_jni_type_sig! { java.lang.Class }
    };
    ($($class_part:ident).+ $($param_name:ident)?) => {
        concat! {"L", $crate::gen_class_name! {$($class_part).+}, ";"}
    };
    ($($class_part:ident).+ [] $($param_name:ident)?) => {
        concat! { "[", $crate::gen_jni_type_sig! {$($class_part).+}}
    };
    ($($invalid:tt)*) => {
        compile_error!(concat!("unsupported type `", stringify!($($invalid)*), "`"))
    };
}

/// Cast a `JValueGen` to a concrete type by the given type
///
/// ```
/// use jni::objects::JValue;
/// use jni::sys::JNI_TRUE;
/// use risingwave_jni_core::cast_jvalue;
/// assert_eq!(cast_jvalue!({boolean}, JValue::Bool(JNI_TRUE)), true);
/// assert_eq!(cast_jvalue!({byte}, JValue::Byte(10 as i8)), 10);
/// assert_eq!(cast_jvalue!({char}, JValue::Char('c' as u16)), 'c' as u16);
/// assert_eq!(cast_jvalue!({double}, JValue::Double(3.14)), 3.14);
/// assert_eq!(cast_jvalue!({float}, JValue::Float(3.14)), 3.14);
/// assert_eq!(cast_jvalue!({int}, JValue::Int(10)), 10);
/// assert_eq!(cast_jvalue!({long}, JValue::Long(10)), 10);
/// assert_eq!(cast_jvalue!({short}, JValue::Short(10)), 10);
/// cast_jvalue!({void}, JValue::Void);
/// let null = jni::objects::JObject::null();
/// let _: &jni::objects::JObject<'_> = cast_jvalue!({String}, JValue::Object(&null));
/// let _: jni::objects::JByteArray<'_> = cast_jvalue!({byte[]}, jni::objects::JValueOwned::Object(null));
/// ```
#[macro_export]
macro_rules! cast_jvalue {
    ({ boolean }, $value:expr) => {{ $value.z().expect("should be bool") }};
    ({ byte }, $value:expr) => {{ $value.b().expect("should be byte") }};
    ({ char }, $value:expr) => {{ $value.c().expect("should be char") }};
    ({ double }, $value:expr) => {{ $value.d().expect("should be double") }};
    ({ float }, $value:expr) => {{ $value.f().expect("should be float") }};
    ({ int }, $value:expr) => {{ $value.i().expect("should be int") }};
    ({ long }, $value:expr) => {{ $value.j().expect("should be long") }};
    ({ short }, $value:expr) => {{ $value.s().expect("should be short") }};
    ({ void }, $value:expr) => {{ $value.v().expect("should be void") }};
    ({ byte[] }, $value:expr) => {{
        let obj = $value.l().expect("should be object");
        unsafe { jni::objects::JByteArray::from_raw(obj.into_raw()) }
    }};
    ({ $($class:tt)+ }, $value:expr) => {{ $value.l().expect("should be object") }};
}

/// Cast a `JValueGen` to a concrete type by the given type
///
/// ```
/// use jni::sys::JNI_TRUE;
/// use risingwave_jni_core::{cast_jvalue, to_jvalue};
/// assert_eq!(
///     cast_jvalue!({ boolean }, to_jvalue!({ boolean }, JNI_TRUE)),
///     true
/// );
/// assert_eq!(cast_jvalue!({ byte }, to_jvalue!({ byte }, 10)), 10);
/// assert_eq!(
///     cast_jvalue!({ char }, to_jvalue!({ char }, 'c')),
///     'c' as u16
/// );
/// assert_eq!(cast_jvalue!({ double }, to_jvalue!({ double }, 3.14)), 3.14);
/// assert_eq!(cast_jvalue!({ float }, to_jvalue!({ float }, 3.14)), 3.14);
/// assert_eq!(cast_jvalue!({ int }, to_jvalue!({ int }, 10)), 10);
/// assert_eq!(cast_jvalue!({ long }, to_jvalue!({ long }, 10)), 10);
/// assert_eq!(cast_jvalue!({ short }, to_jvalue!({ short }, 10)), 10);
/// let obj = jni::objects::JObject::null();
/// cast_jvalue!({ String }, to_jvalue!({ String }, &obj));
/// ```
#[macro_export]
macro_rules! to_jvalue {
    ({ boolean $($param_name:ident)? }, $value:expr) => {{ jni::objects::JValue::Bool($value as _) }};
    ({ byte $($param_name:ident)? }, $value:expr) => {{ jni::objects::JValue::Byte($value as _) }};
    ({ char $($param_name:ident)? }, $value:expr) => {{ jni::objects::JValue::Char($value as _) }};
    ({ double $($param_name:ident)? }, $value:expr) => {{ jni::objects::JValue::Double($value as _) }};
    ({ float $($param_name:ident)? }, $value:expr) => {{ jni::objects::JValue::Float($value as _) }};
    ({ int $($param_name:ident)? }, $value:expr) => {{ jni::objects::JValue::Int($value as _) }};
    ({ long $($param_name:ident)? }, $value:expr) => {{ jni::objects::JValue::Long($value as _) }};
    ({ short $($param_name:ident)? }, $value:expr) => {{ jni::objects::JValue::Short($value as _) }};
    ({ void }, $value:expr) => {{
        compile_error! {concat! {"unlike to pass void value: ", stringify! {$value} }}
    }};
    ({ $($class:ident)+ $([])? $($param_name:ident)? }, $value:expr) => {{ jni::objects::JValue::Object($value as _) }};
}

/// Generate the jni signature of a given function
/// ```
/// use risingwave_jni_core::gen_jni_sig;
/// assert_eq!(gen_jni_sig!(boolean f(int, short, byte[])), "(IS[B)Z");
/// assert_eq!(
///     gen_jni_sig!(boolean f(int, short, byte[], java.lang.String)),
///     "(IS[BLjava/lang/String;)Z"
/// );
/// assert_eq!(
///     gen_jni_sig!(boolean f(int, java.lang.String)),
///     "(ILjava/lang/String;)Z"
/// );
/// assert_eq!(gen_jni_sig!(public static native int defaultVnodeCount()), "()I");
/// assert_eq!(
///     gen_jni_sig!(long hummockIteratorNew(byte[] readPlan)),
///     "([B)J"
/// );
/// assert_eq!(gen_jni_sig!(long hummockIteratorNext(long pointer)), "(J)J");
/// assert_eq!(
///     gen_jni_sig!(void hummockIteratorClose(long pointer)),
///     "(J)V"
/// );
/// assert_eq!(gen_jni_sig!(byte[] rowGetKey(long pointer)), "(J)[B");
/// assert_eq!(
///     gen_jni_sig!(java.sql.Timestamp rowGetTimestampValue(long pointer, int index)),
///     "(JI)Ljava/sql/Timestamp;"
/// );
/// assert_eq!(
///     gen_jni_sig!(String rowGetStringValue(long pointer, int index)),
///     "(JI)Ljava/lang/String;"
/// );
/// assert_eq!(
///     gen_jni_sig!(static native Object rowGetArrayValue(long pointer, int index, Class clazz)),
///     "(JILjava/lang/Class;)Ljava/lang/Object;"
/// );
/// ```
#[macro_export]
macro_rules! gen_jni_sig {
    // handle the result of `split_by_comma`
    ({$({$($args:tt)+})*}, {return {$($ret:tt)*}}) => {{
        concat! {
            "(", $($crate::gen_jni_type_sig!{ $($args)+ },)* ")",
            $crate::gen_jni_type_sig! {$($ret)+}
        }
    }};
    ({$($ret:tt)*}, {$($args:tt)*}) => {{
        $crate::split_by_comma! {
            {$($args)*},
            $crate::gen_jni_sig,
            {return {$($ret)*}}
        }
    }};
    // handle the result of `split_extract_plain_native_methods`
    ({{$func_name:ident, {$($ret:tt)*}, {$($args:tt)*}}}) => {{
        $crate::gen_jni_sig! {
            {$($ret)*}, {$($args)*}
        }
    }};
    ($($input:tt)*) => {{
        $crate::split_extract_plain_native_methods! {{$($input)*;}, $crate::gen_jni_sig}
    }}
}

#[macro_export]
macro_rules! for_all_plain_native_methods {
    ($macro:path $(,$args:tt)*) => {
        $macro! {
            {
                public static native void tracingSlf4jEvent(String threadName, String name, int level, String message, String stackTrace);

                public static native boolean tracingSlf4jEventEnabled(int level);

                public static native int defaultVnodeCount();

                public static native boolean validateCdcSourceColumnType(
                    int cdcTableType,
                    String upstreamTypeName,
                    int rwTypeName,
                    long charMaxLength,
                    boolean isUnsigned,
                    String postgresUdtName);

                static native long iteratorNewStreamChunk(long pointer);

                static native boolean iteratorNext(long pointer);

                public static native void initObjectStoreForTest(String stateStoreUrl, String dataDirectory);

                public static native void putObject(String objectName, byte[] data);

                public static native String getObjectStoreType();

                public static native void deleteObjects(String dir);

                public static native byte[] getObject(String objectName);

                public static native String[] listObject(String dir);

                static native void iteratorClose(long pointer);

                static native long newStreamChunkFromPayload(byte[] streamChunkPayload);

                static native long newStreamChunkFromPretty(String str);

                static native void streamChunkClose(long pointer);

                static native byte[] iteratorGetKey(long pointer);

                static native int iteratorGetOp(long pointer);

                static native boolean iteratorIsNull(long pointer, int index);

                static native short iteratorGetInt16Value(long pointer, int index);

                static native int iteratorGetInt32Value(long pointer, int index);

                static native long iteratorGetInt64Value(long pointer, int index);

                static native float iteratorGetFloatValue(long pointer, int index);

                static native double iteratorGetDoubleValue(long pointer, int index);

                static native boolean iteratorGetBooleanValue(long pointer, int index);

                static native String iteratorGetStringValue(long pointer, int index);

                static native java.time.LocalDateTime iteratorGetTimestampValue(long pointer, int index);

                static native java.time.OffsetDateTime iteratorGetTimestamptzValue(long pointer, int index);

                static native java.math.BigDecimal iteratorGetDecimalValue(long pointer, int index);

                static native java.time.LocalTime iteratorGetTimeValue(long pointer, int index);

                static native java.time.LocalDate iteratorGetDateValue(long pointer, int index);

                static native String iteratorGetIntervalValue(long pointer, int index);

                static native String iteratorGetJsonbValue(long pointer, int index);

                static native byte[] iteratorGetByteaValue(long pointer, int index);

                // TODO: object or object array?
                static native Object iteratorGetArrayValue(long pointer, int index, Class<?> clazz);

                public static native boolean sendCdcSourceMsgToChannel(long channelPtr, byte[] msg);

                public static native boolean sendCdcSourceErrorToChannel(long channelPtr, String errorMsg);

                public static native void cdcSourceSenderClose(long channelPtr);

                public static native com.risingwave.java.binding.JniSinkWriterStreamRequest
                    recvSinkWriterRequestFromChannel(long channelPtr);

                public static native boolean sendSinkWriterResponseToChannel(long channelPtr, byte[] msg);

                public static native boolean sendSinkWriterErrorToChannel(long channelPtr, String msg);

                public static native byte[] recvSinkCoordinatorRequestFromChannel(long channelPtr);

                public static native boolean sendSinkCoordinatorResponseToChannel(long channelPtr, byte[] msg);
            }
            $(,$args)*
        }
    };
}

/// Given the plain text of a list native methods, split the methods by semicolon (;), extract
/// the return type, argument list and name of the methods and pass the result to the callback
/// `$macro` with the extracted result as the first parameter. The result can be matched with
/// pattern `{$({$func_name:ident, {$($ret:tt)*}, {$($args:tt)*}})*}`
///
/// ```
/// macro_rules! call_split_extract_plain_native_methods {
///     ({$({$func_name:ident, {$($ret:tt)*}, {$($args:tt)*}})*}) => {
///         [$(
///             (stringify! {$func_name}, stringify!{$($ret)*}, stringify!{($($args)*)})
///         ),*]
///     };
///     ($($input:tt)*) => {{
///         risingwave_jni_core::split_extract_plain_native_methods! {
///             {$($input)*},
///             call_split_extract_plain_native_methods
///         }
///     }}
/// }
/// assert_eq!([
///     ("f", "int", "(int param1, boolean param2)"),
///     ("f2", "boolean[]", "()"),
///     ("f3", "java.lang.String", "(byte[] param)")
/// ], call_split_extract_plain_native_methods!(
///     int f(int param1, boolean param2);
///     boolean[] f2();
///     java.lang.String f3(byte[] param);
/// ))
/// ```
#[macro_export]
macro_rules! split_extract_plain_native_methods {
    (
        {$($input:tt)*},
        $macro:path
        $(,$extra_args:tt)*
    ) => {{
        $crate::split_extract_plain_native_methods! {
            {$($input)*},
            {},
            $macro
            $(,$extra_args)*
        }
    }};
    (
        {
            $(public)? static native $($first:tt)*
        },
        {
            $($second:tt)*
        },
        $macro:path
        $(,$extra_args:tt)*
    ) => {
        $crate::split_extract_plain_native_methods! {
            {
                $($first)*
            },
            {
                $($second)*
            },
            $macro
            $(,$extra_args)*
        }
    };
    (
        {
            $($ret:tt).+ $func_name:ident($($args:tt)*); $($rest:tt)*
        },
        {
            $({$prev_func_name:ident, {$($prev_ret:tt)*}, {$($prev_args:tt)*}})*
        },
        $macro:path
        $(,$extra_args:tt)*
    ) => {
        $crate::split_extract_plain_native_methods! {
            {$($rest)*},
            {
                $({$prev_func_name, {$($prev_ret)*}, {$($prev_args)*}})*
                {$func_name, {$($ret).+}, {$($args)*}}
            },
            $macro
            $(,$extra_args)*
        }
    };
    (
        {
            $($ret:tt).+ [] $func_name:ident($($args:tt)*); $($rest:tt)*
        },
        {
            $({$prev_func_name:ident, {$($prev_ret:tt)*}, {$($prev_args:tt)*}})*
        },
        $macro:path
        $(,$extra_args:tt)*
    ) => {
        $crate::split_extract_plain_native_methods! {
            {$($rest)*},
            {
                $({$prev_func_name, {$($prev_ret)*}, {$($prev_args)*}})*
                {$func_name, {$($ret).+ []}, {$($args)*}}
            },
            $macro
            $(,$extra_args)*
        }
    };
    (
        {},
        {
            $({$func_name:ident, {$($ret:tt)*}, {$($args:tt)*}})*
        },
        $macro:path
        $(,$extra_args:tt)*
    ) => {
        $macro! {
            {
                $({$func_name, {$($ret)*}, {$($args)*}})*
            }
            $(,$extra_args)*
        }
    };
    ($($invalid:tt)*) => {
        compile_error!(concat!("unable to split extract `", stringify!($($invalid)*), "`"))
    };
}

/// Pass the information of all native methods to the callback `$macro`. The input can be matched
/// with pattern `{$({$func_name:ident, {$($ret:tt)*}, {$($args:tt)*}})*}`
#[macro_export]
macro_rules! for_all_native_methods {
    ($macro:path $(,$args:tt)*) => {{
        $crate::for_all_plain_native_methods! {
            $crate::for_all_native_methods,
            $macro
            $(,$args)*
        }
    }};
    (
        {
            $({$func_name:ident, {$($ret:tt)*}, {$($args:tt)*}})*
        },
        $macro:path
        $(,$extra_args:tt)*
    ) => {{
        $macro! {
            {$({$func_name, {$($ret)*}, {$($args)*}})*}
            $(,$extra_args)*
        }
    }};
    (
        {$($input:tt)*},
        $macro:path
        $(,$extra_args:tt)*
    ) => {{
        $crate::split_extract_plain_native_methods! {
            {$($input)*},
            $crate::for_all_native_methods,
            $macro
            $(,$extra_args)*
        }
    }};
}

/// Convert the argument value list when invoking a method to a list of `JValue` by the argument type list in the signature
/// ```
/// use risingwave_jni_core::convert_args_list;
/// use jni::objects::{JObject, JValue};
/// use jni::sys::JNI_TRUE;
/// let list: [JValue<'static, 'static>; 3] = convert_args_list!(
///     {boolean first, int second, byte third},
///     {true, 10, 20}
/// );
/// match &list[0] {
///     JValue::Bool(b) => assert_eq!(*b, JNI_TRUE),
///     value => unreachable!("wrong value: {:?}", value),
/// }
/// match &list[1] {
///     JValue::Int(v) => assert_eq!(*v, 10),
///     value => unreachable!("wrong value: {:?}", value),
/// }
/// match &list[2] {
///     JValue::Byte(v) => assert_eq!(*v, 20),
///     value => unreachable!("wrong value: {:?}", value),
/// }
/// ```
#[macro_export]
macro_rules! convert_args_list {
    (
        {
            {$($first_args:tt)+}
            $({$($args:tt)+})*
        },
        {
            {$first_value:expr}
            $({$value:expr})*
        },
        {
            $({$converted:expr})*
        }) => {
        $crate::convert_args_list! {
            {$({$($args)+})*},
            {$({$value})*},
            {
                $({$converted})*
                {
                    $crate::to_jvalue! {
                        {$($first_args)+},
                        {$first_value}
                    }
                }
            }
        }
    };
    ({$($args:tt)+}, {}, {$({$converted:expr})*}) => {
        compile_error! {concat!{"trailing argument not passed: ", stringify!{$($args)+}}}
    };
    ({}, {$($value:tt)+}, {$({$converted:expr})*}) => {
        compile_error! {concat!{"trailing extra value passed: ", stringify!{$($value)+}}}
    };
    ({}, {}, {$({$converted:expr})*}) => {
        [$(
            $converted
        ),*]
    };
    ({$($args:tt)*}, {$($value:expr),*}) => {{
        $crate::split_by_comma! {
            {$($args)*},
            $crate::convert_args_list,
            {$({$value})*},
            {}
        }
    }};
    ($($invalid:tt)*) => {
        compile_error!(concat!("failed to convert `", stringify!($($invalid)*), "`"))
    };
}

#[macro_export]
macro_rules! call_static_method {
    (
        {{$func_name:ident, {$($ret:tt)*}, {$($args:tt)*}}},
        {$($class:ident).+},
        {$env:expr} $(, $method_args:expr)*
    ) => {{
        $crate::call_static_method! {
            $env,
            $crate::gen_class_name!($($class).+),
            stringify! {$func_name},
            {{$($ret)*}, {$($args)*}}
            $(, $method_args)*
        }
    }};
    ($env:expr, {$($class:ident).+}, {$($method:tt)*} $(, $args:expr)*) => {{
        $crate::split_extract_plain_native_methods! {
            {$($method)*;},
            $crate::call_static_method,
            {$($class).+},
            {$env} $(, $args)*
        }
    }};
    (
        $env:expr,
        $class_name:expr,
        $func_name:expr,
        {{$($ret:tt)*}, {$($args:tt)*}}
        $(, $method_args:expr)*
    ) => {{
        $env.call_static_method(
            $class_name,
            $func_name,
            $crate::gen_jni_sig! { {$($ret)+}, {$($args)*}},
            &{
                $crate::convert_args_list! {
                    {$($args)*},
                    {$($method_args),*}
                }
            },
        ).map(|jvalue| {
            $crate::cast_jvalue! {
                {$($ret)*},
                jvalue
            }
        })
    }};
}

#[macro_export]
macro_rules! call_method {
    (
        {{$func_name:ident, {$($ret:tt)*}, {$($args:tt)*}}},
        $env:expr, $obj:expr $(, $method_args:expr)*
    ) => {{
        $env.call_method(
            $obj,
            stringify!{$func_name},
            $crate::gen_jni_sig! { {$($ret)+}, {$($args)*}},
            &{
                $crate::convert_args_list! {
                    {$($args)*},
                    {$($method_args),*}
                }
            },
        ).map(|jvalue| {
            $crate::cast_jvalue! {
                {$($ret)*},
                jvalue
            }
        })
    }};
    ($env:expr, $obj:expr, {$($method:tt)*} $(, $args:expr)*) => {{
        $crate::split_extract_plain_native_methods! {
            {$($method)*;},
            $crate::call_method,
            $env, $obj $(, $args)*
        }
    }};
}

#[macro_export]
macro_rules! gen_native_method_entry {
    (
        $class_prefix:ident, $func_name:ident, {$($ret:tt)+}, {$($args:tt)*}
    ) => {{
        {
            let fn_ptr = $crate::paste! {[<$class_prefix $func_name> ]} as *mut c_void;
            let sig = $crate::gen_jni_sig! { {$($ret)+}, {$($args)*}};
            jni::NativeMethod {
                name: jni::strings::JNIString::from(stringify! {$func_name}),
                sig: jni::strings::JNIString::from(sig),
                fn_ptr,
            }
        }
    }};
}

#[cfg(test)]
mod tests {
    use std::fmt::Formatter;

    #[test]
    fn test_for_all_gen() {
        macro_rules! gen_array {
            (test) => {{
                for_all_native_methods! {
                    {
                        public static native int defaultVnodeCount();
                        static native long hummockIteratorNew(byte[] readPlan);
                        public static native byte[] rowGetKey(long pointer);
                    },
                    gen_array
                }
            }};
            (all) => {{
                for_all_native_methods! {
                    gen_array
                }
            }};
            ({$({ $func_name:ident, {$($ret:tt)+}, {$($args:tt)*} })*}) => {{
                [
                    $(
                        (stringify! {$func_name}, gen_jni_sig! { {$($ret)+}, {$($args)*}}),
                    )*
                ]
            }};
        }
        let sig: [(_, _); 3] = gen_array!(test);
        assert_eq!(
            sig,
            [
                ("defaultVnodeCount", "()I"),
                ("hummockIteratorNew", "([B)J"),
                ("rowGetKey", "(J)[B")
            ]
        );

        let sig = gen_array!(all);
        assert!(!sig.is_empty());
    }

    #[test]
    fn test_all_native_methods() {
        // This test shows the signature of all native methods
        let expected = expect_test::expect![[r#"
            [
                tracingSlf4jEvent                        (Ljava/lang/String;Ljava/lang/String;ILjava/lang/String;Ljava/lang/String;)V,
                tracingSlf4jEventEnabled                 (I)Z,
                defaultVnodeCount                        ()I,
                validateCdcSourceColumnType              (ILjava/lang/String;IJZLjava/lang/String;)Z,
                iteratorNewStreamChunk                   (J)J,
                iteratorNext                             (J)Z,
                initObjectStoreForTest                   (Ljava/lang/String;Ljava/lang/String;)V,
                putObject                                (Ljava/lang/String;[B)V,
                getObjectStoreType                       ()Ljava/lang/String;,
                deleteObjects                            (Ljava/lang/String;)V,
                getObject                                (Ljava/lang/String;)[B,
                listObject                               (Ljava/lang/String;)[Ljava/lang/String;,
                iteratorClose                            (J)V,
                newStreamChunkFromPayload                ([B)J,
                newStreamChunkFromPretty                 (Ljava/lang/String;)J,
                streamChunkClose                         (J)V,
                iteratorGetKey                           (J)[B,
                iteratorGetOp                            (J)I,
                iteratorIsNull                           (JI)Z,
                iteratorGetInt16Value                    (JI)S,
                iteratorGetInt32Value                    (JI)I,
                iteratorGetInt64Value                    (JI)J,
                iteratorGetFloatValue                    (JI)F,
                iteratorGetDoubleValue                   (JI)D,
                iteratorGetBooleanValue                  (JI)Z,
                iteratorGetStringValue                   (JI)Ljava/lang/String;,
                iteratorGetTimestampValue                (JI)Ljava/time/LocalDateTime;,
                iteratorGetTimestamptzValue              (JI)Ljava/time/OffsetDateTime;,
                iteratorGetDecimalValue                  (JI)Ljava/math/BigDecimal;,
                iteratorGetTimeValue                     (JI)Ljava/time/LocalTime;,
                iteratorGetDateValue                     (JI)Ljava/time/LocalDate;,
                iteratorGetIntervalValue                 (JI)Ljava/lang/String;,
                iteratorGetJsonbValue                    (JI)Ljava/lang/String;,
                iteratorGetByteaValue                    (JI)[B,
                iteratorGetArrayValue                    (JILjava/lang/Class;)Ljava/lang/Object;,
                sendCdcSourceMsgToChannel                (J[B)Z,
                sendCdcSourceErrorToChannel              (JLjava/lang/String;)Z,
                cdcSourceSenderClose                     (J)V,
                recvSinkWriterRequestFromChannel         (J)Lcom/risingwave/java/binding/JniSinkWriterStreamRequest;,
                sendSinkWriterResponseToChannel          (J[B)Z,
                sendSinkWriterErrorToChannel             (JLjava/lang/String;)Z,
                recvSinkCoordinatorRequestFromChannel    (J)[B,
                sendSinkCoordinatorResponseToChannel     (J[B)Z,
            ]
        "#]];

        struct MethodInfo {
            name: &'static str,
            sig: &'static str,
        }

        impl std::fmt::Debug for MethodInfo {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "{:40} {}", self.name, self.sig)
            }
        }

        macro_rules! gen_all_native_method_info {
            () => {{
                $crate::for_all_native_methods! {
                    gen_all_native_method_info
                }
            }};
            ({$({$func_name:ident, {$($ret:tt)*}, {$($args:tt)*}})*}) => {
                [$(
                    (
                        MethodInfo {
                            name: stringify! {$func_name},
                            sig: $crate::gen_jni_sig! {
                                {$($ret)*}, {$($args)*}
                            },
                        }
                    )
                ),*]
            }
        }
        let info = gen_all_native_method_info!();
        expected.assert_debug_eq(&info);
    }
}
