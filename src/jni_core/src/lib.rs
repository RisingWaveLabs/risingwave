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

#![feature(error_generic_member_access)]
#![feature(once_cell_try)]
#![feature(type_alias_impl_trait)]
#![feature(try_blocks)]
#![feature(used_with_arg)]

pub mod jvm_runtime;
mod macros;
mod opendal_schema_history;
mod tracing_slf4j;

use std::backtrace::Backtrace;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::slice::from_raw_parts;
use std::sync::{LazyLock, OnceLock};

use anyhow::anyhow;
use bytes::Bytes;
use cfg_or_panic::cfg_or_panic;
use chrono::{DateTime, Datelike, Timelike};
use futures::TryStreamExt;
use futures::stream::BoxStream;
use jni::JNIEnv;
use jni::objects::{
    AutoElements, GlobalRef, JByteArray, JClass, JMethodID, JObject, JStaticMethodID, JString,
    JValueOwned, ReleaseMode,
};
use jni::signature::ReturnType;
use jni::sys::{
    JNI_FALSE, JNI_TRUE, jboolean, jbyte, jdouble, jfloat, jint, jlong, jshort, jsize, jvalue,
};
pub use paste::paste;
use prost::{DecodeError, Message};
use risingwave_common::array::{ArrayError, StreamChunk};
use risingwave_common::catalog::cdc_type_compatibility::cdc_source_column_type_compatible;
use risingwave_common::hash::VirtualNode;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::test_prelude::StreamChunkTestExt;
use risingwave_common::types::{Decimal, ScalarRefImpl};
use risingwave_common::util::panic::rw_catch_unwind;
use risingwave_pb::catalog::table::CdcTableType as PbCdcTableType;
use risingwave_pb::connector_service::{
    GetEventStreamResponse, SinkCoordinatorStreamRequest, SinkCoordinatorStreamResponse,
    SinkWriterStreamRequest, SinkWriterStreamResponse,
};
use risingwave_pb::data::Op;
use risingwave_pb::data::data_type::TypeName as PbTypeName;
use thiserror::Error;
use thiserror_ext::AsReport;
use tokio::runtime::Runtime;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing_slf4j::*;

/// Enable JVM and Java libraries.
///
/// This macro forces this crate to be linked, which registers the JVM builder.
#[macro_export]
macro_rules! enable {
    () => {
        use risingwave_jni_core as _;
    };
}

pub static JAVA_BINDING_ASYNC_RUNTIME: LazyLock<Runtime> =
    LazyLock::new(|| tokio::runtime::Runtime::new().unwrap());

#[derive(Error, Debug)]
pub enum BindingError {
    #[error("JniError {error}")]
    Jni {
        #[from]
        error: jni::errors::Error,
        backtrace: Backtrace,
    },

    #[error("StorageError {error}")]
    Storage {
        #[from]
        error: anyhow::Error,
        backtrace: Backtrace,
    },

    #[error("DecodeError {error}")]
    Decode {
        #[from]
        error: DecodeError,
        backtrace: Backtrace,
    },

    #[error("StreamChunkArrayError {error}")]
    StreamChunkArray {
        #[from]
        error: ArrayError,
        backtrace: Backtrace,
    },
}

type Result<T> = std::result::Result<T, BindingError>;

pub fn to_guarded_slice<'array, 'env>(
    array: &'array JByteArray<'env>,
    env: &'array mut JNIEnv<'env>,
) -> Result<SliceGuard<'env, 'array>> {
    unsafe {
        let array = env.get_array_elements(array, ReleaseMode::NoCopyBack)?;
        let slice = from_raw_parts(array.as_ptr() as *mut u8, array.len());

        Ok(SliceGuard {
            _array: array,
            slice,
        })
    }
}

/// Wrapper around `&[u8]` derived from `jbyteArray` to prevent it from being auto-released.
pub struct SliceGuard<'env, 'array> {
    _array: AutoElements<'env, 'env, 'array, jbyte>,
    slice: &'array [u8],
}

impl Deref for SliceGuard<'_, '_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.slice
    }
}

#[repr(transparent)]
pub struct Pointer<'a, T> {
    pointer: jlong,
    _phantom: PhantomData<&'a T>,
}

impl<T> Default for Pointer<'_, T> {
    fn default() -> Self {
        Self {
            pointer: 0,
            _phantom: Default::default(),
        }
    }
}

impl<T> From<T> for Pointer<'static, T> {
    fn from(value: T) -> Self {
        Pointer {
            pointer: Box::into_raw(Box::new(value)) as jlong,
            _phantom: PhantomData,
        }
    }
}

impl<'a, T> Pointer<'a, T> {
    fn as_ref(&self) -> &'a T {
        assert!(self.pointer != 0);
        unsafe { &*(self.pointer as *const T) }
    }

    fn as_mut(&mut self) -> &'a mut T {
        assert!(self.pointer != 0);
        unsafe { &mut *(self.pointer as *mut T) }
    }
}

/// A pointer that owns the object it points to.
///
/// Note that dropping an `OwnedPointer` does not release the object.
/// Instead, you should call [`OwnedPointer::release`] manually.
pub type OwnedPointer<T> = Pointer<'static, T>;

impl<T> OwnedPointer<T> {
    /// Consume `self` and return the pointer value. Used for passing to JNI.
    pub fn into_pointer(self) -> jlong {
        self.pointer
    }

    /// Release the object behind the pointer.
    fn release(self) {
        tracing::debug!(
            type_name = std::any::type_name::<T>(),
            address = %format_args!("{:x}", self.pointer),
            "release jni OwnedPointer"
        );
        assert!(self.pointer != 0);
        unsafe { drop(Box::from_raw(self.pointer as *mut T)) }
    }
}

/// In most Jni interfaces, the first parameter is `JNIEnv`, and the second parameter is `JClass`.
/// This struct simply encapsulates the two common parameters into a single struct for simplicity.
#[repr(C)]
pub struct EnvParam<'a> {
    env: JNIEnv<'a>,
    class: JClass<'a>,
}

impl<'a> Deref for EnvParam<'a> {
    type Target = JNIEnv<'a>;

    fn deref(&self) -> &Self::Target {
        &self.env
    }
}

impl DerefMut for EnvParam<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.env
    }
}

impl<'a> EnvParam<'a> {
    pub fn get_class(&self) -> &JClass<'a> {
        &self.class
    }
}

pub fn execute_and_catch<'env, F, Ret>(mut env: EnvParam<'env>, inner: F) -> Ret
where
    F: FnOnce(&mut EnvParam<'env>) -> Result<Ret>,
    Ret: Default + 'env,
{
    match rw_catch_unwind(std::panic::AssertUnwindSafe(|| inner(&mut env))) {
        Ok(Ok(ret)) => ret,
        Ok(Err(e)) => {
            match e {
                BindingError::Jni {
                    error: jni::errors::Error::JavaException,
                    backtrace,
                } => {
                    tracing::error!("get JavaException thrown from: {:?}", backtrace);
                    // the exception is already thrown. No need to throw again
                }
                _ => {
                    env.throw(format!("get error while processing: {:?}", e.as_report()))
                        .expect("should be able to throw");
                }
            }
            Ret::default()
        }
        Err(e) => {
            env.throw(format!("panic while processing: {:?}", e))
                .expect("should be able to throw");
            Ret::default()
        }
    }
}

#[derive(Default)]
struct JavaClassMethodCache {
    big_decimal_ctor: OnceLock<(GlobalRef, JMethodID)>,

    timestamp_ctor: OnceLock<(GlobalRef, JStaticMethodID)>,
    timestamptz_ctor: OnceLock<(GlobalRef, JStaticMethodID)>,
    date_ctor: OnceLock<(GlobalRef, JStaticMethodID)>,
    time_ctor: OnceLock<(GlobalRef, JStaticMethodID)>,
    instant_ctor: OnceLock<(GlobalRef, JStaticMethodID)>,
    utc: OnceLock<GlobalRef>,
}

mod opaque_type {
    use super::*;
    // TODO: may only return a RowRef
    pub type StreamChunkRowIterator<'a> = impl Iterator<Item = (Op, OwnedRow)> + 'a;

    impl<'a> JavaBindingIteratorInner<'a> {
        #[define_opaque(StreamChunkRowIterator)]
        pub(super) fn from_chunk(chunk: &'a StreamChunk) -> JavaBindingIteratorInner<'a> {
            JavaBindingIteratorInner::StreamChunk(
                chunk
                    .rows()
                    .map(|(op, row)| (op.to_protobuf(), row.to_owned_row())),
            )
        }
    }
}
pub use opaque_type::StreamChunkRowIterator;
pub type HummockJavaBindingIterator = BoxStream<'static, anyhow::Result<(Bytes, OwnedRow)>>;
pub enum JavaBindingIteratorInner<'a> {
    Hummock(HummockJavaBindingIterator),
    StreamChunk(StreamChunkRowIterator<'a>),
}

enum RowExtra {
    Op(Op),
    Key(Bytes),
}

impl RowExtra {
    fn as_op(&self) -> Op {
        match self {
            RowExtra::Op(op) => *op,
            RowExtra::Key(_) => unreachable!("should be op"),
        }
    }

    fn as_key(&self) -> &Bytes {
        match self {
            RowExtra::Key(key) => key,
            RowExtra::Op(_) => unreachable!("should be key"),
        }
    }
}

struct RowCursor {
    row: OwnedRow,
    extra: RowExtra,
}

pub struct JavaBindingIterator<'a> {
    inner: JavaBindingIteratorInner<'a>,
    cursor: Option<RowCursor>,
    class_cache: JavaClassMethodCache,
}

impl JavaBindingIterator<'static> {
    pub fn new_hummock_iter(iter: HummockJavaBindingIterator) -> Self {
        Self {
            inner: JavaBindingIteratorInner::Hummock(iter),
            cursor: None,
            class_cache: Default::default(),
        }
    }
}

impl Deref for JavaBindingIterator<'_> {
    type Target = OwnedRow;

    fn deref(&self) -> &Self::Target {
        &self
            .cursor
            .as_ref()
            .expect("should exist when call row methods")
            .row
    }
}

#[unsafe(no_mangle)]
extern "system" fn Java_com_risingwave_java_binding_Binding_defaultVnodeCount(
    _env: EnvParam<'_>,
) -> jint {
    VirtualNode::COUNT_FOR_COMPAT as jint
}

fn jstring_to_option(env: &mut EnvParam<'_>, value: JString<'_>) -> Result<Option<String>> {
    if value.is_null() {
        return Ok(None);
    }

    Ok(Some(env.get_string(&value)?.into()))
}

#[unsafe(no_mangle)]
extern "system" fn Java_com_risingwave_java_binding_Binding_validateCdcSourceColumnType<'a>(
    env: EnvParam<'a>,
    cdc_table_type: jint,
    upstream_type_name: JString<'a>,
    rw_type_name: jint,
    char_max_length: jlong,
    is_unsigned: jboolean,
    postgres_udt_name: JString<'a>,
) -> jboolean {
    execute_and_catch(env, move |env| {
        let cdc_table_type =
            PbCdcTableType::try_from(cdc_table_type).unwrap_or(PbCdcTableType::Unspecified);
        let rw_type_name =
            PbTypeName::try_from(rw_type_name).unwrap_or(PbTypeName::TypeUnspecified);
        let upstream_type_name = jstring_to_option(env, upstream_type_name)?.unwrap_or_default();
        let postgres_udt_name = jstring_to_option(env, postgres_udt_name)?;
        let char_max_length = (char_max_length >= 0).then_some(char_max_length);

        Ok(
            if cdc_source_column_type_compatible(
                cdc_table_type,
                &upstream_type_name,
                rw_type_name,
                char_max_length,
                is_unsigned != JNI_FALSE,
                postgres_udt_name.as_deref(),
            ) {
                JNI_TRUE
            } else {
                JNI_FALSE
            },
        )
    })
}

#[cfg_or_panic(not(madsim))]
#[unsafe(no_mangle)]
extern "system" fn Java_com_risingwave_java_binding_Binding_iteratorNewStreamChunk<'a>(
    env: EnvParam<'a>,
    chunk: Pointer<'a, StreamChunk>,
) -> Pointer<'static, JavaBindingIterator<'a>> {
    execute_and_catch(env, move |_env| {
        let iter = JavaBindingIterator {
            inner: JavaBindingIteratorInner::from_chunk(chunk.as_ref()),
            cursor: None,
            class_cache: Default::default(),
        };
        Ok(iter.into())
    })
}

#[cfg_or_panic(not(madsim))]
#[unsafe(no_mangle)]
extern "system" fn Java_com_risingwave_java_binding_Binding_iteratorNext<'a>(
    env: EnvParam<'a>,
    mut pointer: Pointer<'a, JavaBindingIterator<'a>>,
) -> jboolean {
    execute_and_catch(env, move |_env| {
        let iter = pointer.as_mut();
        match &mut iter.inner {
            JavaBindingIteratorInner::Hummock(hummock_iter) => {
                match JAVA_BINDING_ASYNC_RUNTIME.block_on(hummock_iter.try_next())? {
                    None => {
                        iter.cursor = None;
                        Ok(JNI_FALSE)
                    }
                    Some((key, row)) => {
                        iter.cursor = Some(RowCursor {
                            row,
                            extra: RowExtra::Key(key),
                        });
                        Ok(JNI_TRUE)
                    }
                }
            }
            JavaBindingIteratorInner::StreamChunk(stream_chunk_iter) => {
                match stream_chunk_iter.next() {
                    None => {
                        iter.cursor = None;
                        Ok(JNI_FALSE)
                    }
                    Some((op, row)) => {
                        iter.cursor = Some(RowCursor {
                            row,
                            extra: RowExtra::Op(op),
                        });
                        Ok(JNI_TRUE)
                    }
                }
            }
        }
    })
}

#[unsafe(no_mangle)]
extern "system" fn Java_com_risingwave_java_binding_Binding_iteratorClose<'a>(
    _env: EnvParam<'a>,
    pointer: OwnedPointer<JavaBindingIterator<'a>>,
) {
    pointer.release()
}

#[unsafe(no_mangle)]
extern "system" fn Java_com_risingwave_java_binding_Binding_newStreamChunkFromPayload<'a>(
    env: EnvParam<'a>,
    stream_chunk_payload: JByteArray<'a>,
) -> Pointer<'static, StreamChunk> {
    execute_and_catch(env, move |env| {
        let prost_stream_chumk =
            Message::decode(to_guarded_slice(&stream_chunk_payload, env)?.deref())?;
        Ok(StreamChunk::from_protobuf(&prost_stream_chumk)?.into())
    })
}

#[unsafe(no_mangle)]
extern "system" fn Java_com_risingwave_java_binding_Binding_newStreamChunkFromPretty<'a>(
    env: EnvParam<'a>,
    str: JString<'a>,
) -> Pointer<'static, StreamChunk> {
    execute_and_catch(env, move |env: &mut EnvParam<'_>| {
        Ok(StreamChunk::from_pretty(env.get_string(&str)?.to_str().unwrap()).into())
    })
}

#[unsafe(no_mangle)]
extern "system" fn Java_com_risingwave_java_binding_Binding_streamChunkClose(
    _env: EnvParam<'_>,
    chunk: OwnedPointer<StreamChunk>,
) {
    chunk.release()
}

#[unsafe(no_mangle)]
extern "system" fn Java_com_risingwave_java_binding_Binding_iteratorGetKey<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingIterator<'a>>,
) -> JByteArray<'a> {
    execute_and_catch(env, move |env: &mut EnvParam<'_>| {
        Ok(env.byte_array_from_slice(
            pointer
                .as_ref()
                .cursor
                .as_ref()
                .expect("should exists when call get key")
                .extra
                .as_key()
                .as_ref(),
        )?)
    })
}

#[unsafe(no_mangle)]
extern "system" fn Java_com_risingwave_java_binding_Binding_iteratorGetOp<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingIterator<'a>>,
) -> jint {
    execute_and_catch(env, move |_env| {
        Ok(pointer
            .as_ref()
            .cursor
            .as_ref()
            .expect("should exist when call get op")
            .extra
            .as_op() as jint)
    })
}

#[unsafe(no_mangle)]
extern "system" fn Java_com_risingwave_java_binding_Binding_iteratorIsNull<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingIterator<'a>>,
    idx: jint,
) -> jboolean {
    execute_and_catch(env, move |_env| {
        Ok(pointer.as_ref().datum_at(idx as usize).is_none() as jboolean)
    })
}

#[unsafe(no_mangle)]
extern "system" fn Java_com_risingwave_java_binding_Binding_iteratorGetInt16Value<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingIterator<'a>>,
    idx: jint,
) -> jshort {
    execute_and_catch(env, move |_env| {
        Ok(pointer
            .as_ref()
            .datum_at(idx as usize)
            .unwrap()
            .into_int16())
    })
}

#[unsafe(no_mangle)]
extern "system" fn Java_com_risingwave_java_binding_Binding_iteratorGetInt32Value<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingIterator<'a>>,
    idx: jint,
) -> jint {
    execute_and_catch(env, move |_env| {
        Ok(pointer
            .as_ref()
            .datum_at(idx as usize)
            .unwrap()
            .into_int32())
    })
}

#[unsafe(no_mangle)]
extern "system" fn Java_com_risingwave_java_binding_Binding_iteratorGetInt64Value<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingIterator<'a>>,
    idx: jint,
) -> jlong {
    execute_and_catch(env, move |_env| {
        Ok(pointer
            .as_ref()
            .datum_at(idx as usize)
            .unwrap()
            .into_int64())
    })
}

#[unsafe(no_mangle)]
extern "system" fn Java_com_risingwave_java_binding_Binding_iteratorGetFloatValue<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingIterator<'a>>,
    idx: jint,
) -> jfloat {
    execute_and_catch(env, move |_env| {
        Ok(pointer
            .as_ref()
            .datum_at(idx as usize)
            .unwrap()
            .into_float32()
            .into())
    })
}

#[unsafe(no_mangle)]
extern "system" fn Java_com_risingwave_java_binding_Binding_iteratorGetDoubleValue<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingIterator<'a>>,
    idx: jint,
) -> jdouble {
    execute_and_catch(env, move |_env| {
        Ok(pointer
            .as_ref()
            .datum_at(idx as usize)
            .unwrap()
            .into_float64()
            .into())
    })
}

#[unsafe(no_mangle)]
extern "system" fn Java_com_risingwave_java_binding_Binding_iteratorGetBooleanValue<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingIterator<'a>>,
    idx: jint,
) -> jboolean {
    execute_and_catch(env, move |_env| {
        Ok(pointer.as_ref().datum_at(idx as usize).unwrap().into_bool() as jboolean)
    })
}

#[unsafe(no_mangle)]
extern "system" fn Java_com_risingwave_java_binding_Binding_iteratorGetStringValue<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingIterator<'a>>,
    idx: jint,
) -> JString<'a> {
    execute_and_catch(env, move |env: &mut EnvParam<'a>| {
        Ok(env.new_string(pointer.as_ref().datum_at(idx as usize).unwrap().into_utf8())?)
    })
}

#[unsafe(no_mangle)]
extern "system" fn Java_com_risingwave_java_binding_Binding_iteratorGetIntervalValue<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingIterator<'a>>,
    idx: jint,
) -> JString<'a> {
    execute_and_catch(env, move |env: &mut EnvParam<'a>| {
        let interval = pointer
            .as_ref()
            .datum_at(idx as usize)
            .unwrap()
            .into_interval()
            .as_iso_8601();
        Ok(env.new_string(interval)?)
    })
}

#[unsafe(no_mangle)]
extern "system" fn Java_com_risingwave_java_binding_Binding_iteratorGetJsonbValue<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingIterator<'a>>,
    idx: jint,
) -> JString<'a> {
    execute_and_catch(env, move |env: &mut EnvParam<'_>| {
        let jsonb = pointer
            .as_ref()
            .datum_at(idx as usize)
            .unwrap()
            .into_jsonb()
            .to_string();
        Ok(env.new_string(jsonb)?)
    })
}

#[unsafe(no_mangle)]
extern "system" fn Java_com_risingwave_java_binding_Binding_iteratorGetTimestampValue<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingIterator<'a>>,
    idx: jint,
) -> JObject<'a> {
    execute_and_catch(env, move |env: &mut EnvParam<'_>| {
        let value = pointer
            .as_ref()
            .datum_at(idx as usize)
            .unwrap()
            .into_timestamp();

        let sig = gen_jni_sig!(java.time.LocalDateTime of(int year, int month, int dayOfMonth, int hour, int minute, int second, int nanoOfSecond));

        let (timestamp_class_ref, constructor) = pointer
            .as_ref()
            .class_cache
            .timestamp_ctor
            .get_or_try_init(|| {
                let cls = env.find_class(gen_class_name!(java.time.LocalDateTime))?;
                let init_method = env.get_static_method_id(&cls, "of", sig)?;
                Ok::<_, jni::errors::Error>((env.new_global_ref(cls)?, init_method))
            })?;
        unsafe {
            let JValueOwned::Object(timestamp_obj) = env.call_static_method_unchecked(
                <&JClass<'_>>::from(timestamp_class_ref.as_obj()),
                *constructor,
                ReturnType::Object,
                &[
                    jvalue { i: value.0.year() },
                    jvalue {
                        i: value.0.month() as i32,
                    },
                    jvalue {
                        i: value.0.day() as i32,
                    },
                    jvalue {
                        i: value.0.hour() as i32,
                    },
                    jvalue {
                        i: value.0.minute() as i32,
                    },
                    jvalue {
                        i: value.0.second() as i32,
                    },
                    jvalue {
                        i: value.0.nanosecond() as i32,
                    },
                ],
            )?
            else {
                return Err(BindingError::from(jni::errors::Error::MethodNotFound {
                    name: "of".to_owned(),
                    sig: sig.into(),
                }));
            };
            Ok(timestamp_obj)
        }
    })
}

#[unsafe(no_mangle)]
extern "system" fn Java_com_risingwave_java_binding_Binding_iteratorGetTimestamptzValue<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingIterator<'a>>,
    idx: jint,
) -> JObject<'a> {
    execute_and_catch(env, move |env: &mut EnvParam<'_>| {
        let value = pointer
            .as_ref()
            .datum_at(idx as usize)
            .unwrap()
            .into_timestamptz();

        let instant_sig =
            gen_jni_sig!(java.time.Instant ofEpochSecond(long epochSecond, long nanoAdjustment));

        let (instant_class_ref, instant_constructor) = pointer
            .as_ref()
            .class_cache
            .instant_ctor
            .get_or_try_init(|| {
                let cls = env.find_class(gen_class_name!(java.time.Instant))?;
                let init_method = env.get_static_method_id(&cls, "ofEpochSecond", instant_sig)?;
                Ok::<_, jni::errors::Error>((env.new_global_ref(cls)?, init_method))
            })?;
        let instant_obj = unsafe {
            let JValueOwned::Object(instant_obj) = env.call_static_method_unchecked(
                <&JClass<'_>>::from(instant_class_ref.as_obj()),
                *instant_constructor,
                ReturnType::Object,
                &[
                    jvalue {
                        j: value.timestamp(),
                    },
                    jvalue {
                        j: value.timestamp_subsec_nanos() as i64,
                    },
                ],
            )?
            else {
                return Err(BindingError::from(jni::errors::Error::MethodNotFound {
                    name: "ofEpochSecond".to_owned(),
                    sig: instant_sig.into(),
                }));
            };
            instant_obj
        };

        let utc_ref = pointer.as_ref().class_cache.utc.get_or_try_init(|| {
            let cls = env.find_class(gen_class_name!(java.time.ZoneOffset))?;
            let utc = env
                .get_static_field(&cls, "UTC", gen_jni_type_sig!(java.time.ZoneOffset))?
                .l()?;
            env.new_global_ref(utc)
        })?;

        let sig = gen_jni_sig!(java.time.OffsetDateTime ofInstant(java.time.Instant instant, java.time.ZoneId zone));

        let (timestamptz_class_ref, constructor) = pointer
            .as_ref()
            .class_cache
            .timestamptz_ctor
            .get_or_try_init(|| {
                let cls = env.find_class(gen_class_name!(java.time.OffsetDateTime))?;
                let init_method = env.get_static_method_id(&cls, "ofInstant", sig)?;
                Ok::<_, jni::errors::Error>((env.new_global_ref(cls)?, init_method))
            })?;
        unsafe {
            let JValueOwned::Object(timestamptz_obj) = env.call_static_method_unchecked(
                <&JClass<'_>>::from(timestamptz_class_ref.as_obj()),
                *constructor,
                ReturnType::Object,
                &[
                    jvalue {
                        l: instant_obj.as_raw(),
                    },
                    jvalue {
                        l: utc_ref.as_obj().as_raw(),
                    },
                ],
            )?
            else {
                return Err(BindingError::from(jni::errors::Error::MethodNotFound {
                    name: "ofInstant".to_owned(),
                    sig: sig.into(),
                }));
            };
            Ok(timestamptz_obj)
        }
    })
}

#[unsafe(no_mangle)]
extern "system" fn Java_com_risingwave_java_binding_Binding_iteratorGetDecimalValue<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingIterator<'a>>,
    idx: jint,
) -> JObject<'a> {
    execute_and_catch(env, move |env: &mut EnvParam<'_>| {
        let decimal_value = pointer
            .as_ref()
            .datum_at(idx as usize)
            .unwrap()
            .into_decimal();

        match decimal_value {
            Decimal::NaN | Decimal::NegativeInf | Decimal::PositiveInf => {
                return Ok(JObject::null());
            }
            Decimal::Normalized(_) => {}
        };

        let value = decimal_value.to_string();
        let string_value = env.new_string(value)?;
        let (decimal_class_ref, constructor) = pointer
            .as_ref()
            .class_cache
            .big_decimal_ctor
            .get_or_try_init(|| {
                let cls = env.find_class("java/math/BigDecimal")?;
                let init_method = env.get_method_id(&cls, "<init>", "(Ljava/lang/String;)V")?;
                Ok::<_, jni::errors::Error>((env.new_global_ref(cls)?, init_method))
            })?;
        unsafe {
            let decimal_class = <&JClass<'_>>::from(decimal_class_ref.as_obj());
            let date_obj = env.new_object_unchecked(
                decimal_class,
                *constructor,
                &[jvalue {
                    l: string_value.into_raw(),
                }],
            )?;
            Ok(date_obj)
        }
    })
}

#[unsafe(no_mangle)]
extern "system" fn Java_com_risingwave_java_binding_Binding_iteratorGetDateValue<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingIterator<'a>>,
    idx: jint,
) -> JObject<'a> {
    execute_and_catch(env, move |env: &mut EnvParam<'_>| {
        let value = pointer.as_ref().datum_at(idx as usize).unwrap().into_date();
        let epoch_days = (value.0 - DateTime::UNIX_EPOCH.date_naive()).num_days();

        let sig = gen_jni_sig!(java.time.LocalDate ofEpochDay(long));

        let (date_class_ref, constructor) =
            pointer.as_ref().class_cache.date_ctor.get_or_try_init(|| {
                let cls = env.find_class(gen_class_name!(java.time.LocalDate))?;
                let init_method = env.get_static_method_id(&cls, "ofEpochDay", sig)?;
                Ok::<_, jni::errors::Error>((env.new_global_ref(cls)?, init_method))
            })?;
        unsafe {
            let JValueOwned::Object(date_obj) = env.call_static_method_unchecked(
                <&JClass<'_>>::from(date_class_ref.as_obj()),
                *constructor,
                ReturnType::Object,
                &[jvalue { j: epoch_days }],
            )?
            else {
                return Err(BindingError::from(jni::errors::Error::MethodNotFound {
                    name: "ofEpochDay".to_owned(),
                    sig: sig.into(),
                }));
            };
            Ok(date_obj)
        }
    })
}

#[unsafe(no_mangle)]
extern "system" fn Java_com_risingwave_java_binding_Binding_iteratorGetTimeValue<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingIterator<'a>>,
    idx: jint,
) -> JObject<'a> {
    execute_and_catch(env, move |env: &mut EnvParam<'_>| {
        let value = pointer.as_ref().datum_at(idx as usize).unwrap().into_time();

        let sig = gen_jni_sig!(java.time.LocalTime of(int hour, int minute, int second, int nanoOfSecond));

        let (time_class_ref, constructor) =
            pointer.as_ref().class_cache.time_ctor.get_or_try_init(|| {
                let cls = env.find_class(gen_class_name!(java.time.LocalTime))?;
                let init_method = env.get_static_method_id(&cls, "of", sig)?;
                Ok::<_, jni::errors::Error>((env.new_global_ref(cls)?, init_method))
            })?;
        unsafe {
            let JValueOwned::Object(time_obj) = env.call_static_method_unchecked(
                <&JClass<'_>>::from(time_class_ref.as_obj()),
                *constructor,
                ReturnType::Object,
                &[
                    jvalue {
                        i: value.0.hour() as i32,
                    },
                    jvalue {
                        i: value.0.minute() as i32,
                    },
                    jvalue {
                        i: value.0.second() as i32,
                    },
                    jvalue {
                        i: value.0.nanosecond() as i32,
                    },
                ],
            )?
            else {
                return Err(BindingError::from(jni::errors::Error::MethodNotFound {
                    name: "of".to_owned(),
                    sig: sig.into(),
                }));
            };
            Ok(time_obj)
        }
    })
}

#[unsafe(no_mangle)]
extern "system" fn Java_com_risingwave_java_binding_Binding_iteratorGetByteaValue<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingIterator<'a>>,
    idx: jint,
) -> JByteArray<'a> {
    execute_and_catch(env, move |env: &mut EnvParam<'_>| {
        let bytes = pointer
            .as_ref()
            .datum_at(idx as usize)
            .unwrap()
            .into_bytea();
        Ok(env.byte_array_from_slice(bytes)?)
    })
}

#[unsafe(no_mangle)]
extern "system" fn Java_com_risingwave_java_binding_Binding_iteratorGetArrayValue<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingIterator<'a>>,
    idx: jint,
    class: JClass<'a>,
) -> JObject<'a> {
    execute_and_catch(env, move |env: &mut EnvParam<'_>| {
        let elems = pointer
            .as_ref()
            .datum_at(idx as usize)
            .unwrap()
            .into_list()
            .iter();

        // convert the Rust elements to a Java object array (Object[])
        let jarray = env.new_object_array(elems.len() as jsize, &class, JObject::null())?;

        for (i, ele) in elems.enumerate() {
            let index = i as jsize;
            match ele {
                None => env.set_object_array_element(&jarray, i as jsize, JObject::null())?,
                Some(val) => match val {
                    ScalarRefImpl::Int16(v) => {
                        let o = call_static_method!(
                            env,
                            {Short},
                            {Short	valueOf(short s)},
                            v
                        )?;
                        env.set_object_array_element(&jarray, index, &o)?;
                    }
                    ScalarRefImpl::Int32(v) => {
                        let o = call_static_method!(
                            env,
                            {Integer},
                            {Integer	valueOf(int i)},
                            v
                        )?;
                        env.set_object_array_element(&jarray, index, &o)?;
                    }
                    ScalarRefImpl::Int64(v) => {
                        let o = call_static_method!(
                            env,
                            {Long},
                            {Long	valueOf(long l)},
                            v
                        )?;
                        env.set_object_array_element(&jarray, index, &o)?;
                    }
                    ScalarRefImpl::Float32(v) => {
                        let o = call_static_method!(
                            env,
                            {Float},
                            {Float	valueOf(float f)},
                            v.into_inner()
                        )?;
                        env.set_object_array_element(&jarray, index, &o)?;
                    }
                    ScalarRefImpl::Float64(v) => {
                        let o = call_static_method!(
                            env,
                            {Double},
                            {Double	valueOf(double d)},
                            v.into_inner()
                        )?;
                        env.set_object_array_element(&jarray, index, &o)?;
                    }
                    ScalarRefImpl::Utf8(v) => {
                        let obj = env.new_string(v)?;
                        env.set_object_array_element(&jarray, index, obj)?
                    }
                    _ => env.set_object_array_element(&jarray, index, JObject::null())?,
                },
            }
        }
        let output = unsafe { JObject::from_raw(jarray.into_raw()) };
        Ok(output)
    })
}

pub type JniSenderType<T> = Sender<anyhow::Result<T>>;
pub type JniReceiverType<T> = Receiver<T>;

/// Send messages to the channel received by `CdcSplitReader`.
/// If msg is null, just check whether the channel is closed.
/// Return true if sending is successful, otherwise, return false so that caller can stop
/// gracefully.
#[unsafe(no_mangle)]
extern "system" fn Java_com_risingwave_java_binding_Binding_sendCdcSourceMsgToChannel<'a>(
    env: EnvParam<'a>,
    channel: Pointer<'a, JniSenderType<GetEventStreamResponse>>,
    msg: JByteArray<'a>,
) -> jboolean {
    execute_and_catch(env, move |env| {
        // If msg is null means just check whether channel is closed.
        if msg.is_null() {
            if channel.as_ref().is_closed() {
                return Ok(JNI_FALSE);
            } else {
                return Ok(JNI_TRUE);
            }
        }

        let get_event_stream_response: GetEventStreamResponse =
            Message::decode(to_guarded_slice(&msg, env)?.deref())?;

        match channel
            .as_ref()
            .blocking_send(Ok(get_event_stream_response))
        {
            Ok(_) => Ok(JNI_TRUE),
            Err(e) => {
                tracing::info!(error = %e.as_report(), "send error");
                Ok(JNI_FALSE)
            }
        }
    })
}

#[unsafe(no_mangle)]
extern "system" fn Java_com_risingwave_java_binding_Binding_sendCdcSourceErrorToChannel<'a>(
    env: EnvParam<'a>,
    channel: Pointer<'a, JniSenderType<GetEventStreamResponse>>,
    msg: JString<'a>,
) -> jboolean {
    execute_and_catch(env, move |env| {
        let ret = env.get_string(&msg);
        match ret {
            Ok(str) => {
                let err_msg: String = str.into();
                match channel.as_ref().blocking_send(Err(anyhow!(err_msg))) {
                    Ok(_) => Ok(JNI_TRUE),
                    Err(e) => {
                        tracing::info!(error = ?e.as_report(), "send error");
                        Ok(JNI_FALSE)
                    }
                }
            }
            Err(err) => {
                if msg.is_null() {
                    tracing::warn!("source error message is null");
                    Ok(JNI_TRUE)
                } else {
                    tracing::error!(error = ?err.as_report(), "source error message should be a java string");
                    Ok(JNI_FALSE)
                }
            }
        }
    })
}

#[unsafe(no_mangle)]
extern "system" fn Java_com_risingwave_java_binding_Binding_cdcSourceSenderClose(
    _env: EnvParam<'_>,
    channel: OwnedPointer<JniSenderType<GetEventStreamResponse>>,
) {
    channel.release();
}

pub enum JniSinkWriterStreamRequest {
    PbRequest(SinkWriterStreamRequest),
    Chunk {
        epoch: u64,
        batch_id: u64,
        chunk: StreamChunk,
    },
}

impl From<SinkWriterStreamRequest> for JniSinkWriterStreamRequest {
    fn from(value: SinkWriterStreamRequest) -> Self {
        Self::PbRequest(value)
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_recvSinkWriterRequestFromChannel<
    'a,
>(
    env: EnvParam<'a>,
    mut channel: Pointer<'a, JniReceiverType<JniSinkWriterStreamRequest>>,
) -> JObject<'a> {
    execute_and_catch(env, move |env| match channel.as_mut().blocking_recv() {
        Some(msg) => {
            let obj = match msg {
                JniSinkWriterStreamRequest::PbRequest(request) => {
                    let bytes = env.byte_array_from_slice(&Message::encode_to_vec(&request))?;
                    let jobj = JObject::from(bytes);
                    call_static_method!(
                        env,
                        {com.risingwave.java.binding.JniSinkWriterStreamRequest},
                        {com.risingwave.java.binding.JniSinkWriterStreamRequest fromSerializedPayload(byte[] payload)},
                        &jobj
                    )?
                }
                JniSinkWriterStreamRequest::Chunk {
                    epoch,
                    batch_id,
                    chunk,
                } => {
                    let pointer = Box::into_raw(Box::new(chunk));
                    call_static_method!(
                        env,
                        {com.risingwave.java.binding.JniSinkWriterStreamRequest},
                        {com.risingwave.java.binding.JniSinkWriterStreamRequest fromStreamChunkOwnedPointer(long pointer, long epoch, long batchId)},
                        pointer as u64, epoch, batch_id
                    )
                    .inspect_err(|_| unsafe {
                        // release the stream chunk on err
                        drop(Box::from_raw(pointer));
                    })?
                }
            };
            Ok(obj)
        }
        None => Ok(JObject::null()),
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_sendSinkWriterResponseToChannel<
    'a,
>(
    env: EnvParam<'a>,
    channel: Pointer<'a, JniSenderType<SinkWriterStreamResponse>>,
    msg: JByteArray<'a>,
) -> jboolean {
    execute_and_catch(env, move |env| {
        let sink_writer_stream_response: SinkWriterStreamResponse =
            Message::decode(to_guarded_slice(&msg, env)?.deref())?;

        match channel
            .as_ref()
            .blocking_send(Ok(sink_writer_stream_response))
        {
            Ok(_) => Ok(JNI_TRUE),
            Err(e) => {
                tracing::info!(error = ?e.as_report(), "send error");
                Ok(JNI_FALSE)
            }
        }
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_sendSinkWriterErrorToChannel<'a>(
    env: EnvParam<'a>,
    channel: Pointer<'a, Sender<anyhow::Result<SinkWriterStreamResponse>>>,
    msg: JString<'a>,
) -> jboolean {
    execute_and_catch(env, move |env| {
        let ret = env.get_string(&msg);
        match ret {
            Ok(str) => {
                let err_msg: String = str.into();
                match channel.as_ref().blocking_send(Err(anyhow!(err_msg))) {
                    Ok(_) => Ok(JNI_TRUE),
                    Err(e) => {
                        tracing::info!(error = ?e.as_report(), "send error");
                        Ok(JNI_FALSE)
                    }
                }
            }
            Err(err) => {
                if msg.is_null() {
                    tracing::warn!("sink error message is null");
                    Ok(JNI_TRUE)
                } else {
                    tracing::error!(error = ?err.as_report(), "sink error message should be a java string");
                    Ok(JNI_FALSE)
                }
            }
        }
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_recvSinkCoordinatorRequestFromChannel<
    'a,
>(
    env: EnvParam<'a>,
    mut channel: Pointer<'a, JniReceiverType<SinkCoordinatorStreamRequest>>,
) -> JByteArray<'a> {
    execute_and_catch(env, move |env| match channel.as_mut().blocking_recv() {
        Some(msg) => {
            let bytes = env
                .byte_array_from_slice(&Message::encode_to_vec(&msg))
                .unwrap();
            Ok(bytes)
        }
        None => Ok(JObject::null().into()),
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_sendSinkCoordinatorResponseToChannel<
    'a,
>(
    env: EnvParam<'a>,
    channel: Pointer<'a, JniSenderType<SinkCoordinatorStreamResponse>>,
    msg: JByteArray<'a>,
) -> jboolean {
    execute_and_catch(env, move |env| {
        let sink_coordinator_stream_response: SinkCoordinatorStreamResponse =
            Message::decode(to_guarded_slice(&msg, env)?.deref())?;

        match channel
            .as_ref()
            .blocking_send(Ok(sink_coordinator_stream_response))
        {
            Ok(_) => Ok(JNI_TRUE),
            Err(e) => {
                tracing::info!(error = ?e.as_report(), "send error");
                Ok(JNI_FALSE)
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use risingwave_common::types::Timestamptz;

    /// make sure that the [`ScalarRefImpl::Int64`] received by
    /// [`Java_com_risingwave_java_binding_Binding_iteratorGetTimestampValue`]
    /// is of type [`DataType::Timestamptz`] stored in microseconds
    #[test]
    fn test_timestamptz_to_i64() {
        assert_eq!(
            "2023-06-01 09:45:00+08:00".parse::<Timestamptz>().unwrap(),
            Timestamptz::from_micros(1_685_583_900_000_000).unwrap()
        );
    }
}
