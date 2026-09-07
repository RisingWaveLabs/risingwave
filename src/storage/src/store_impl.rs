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

use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use enum_as_inner::EnumAsInner;
use foyer::{
    BlockEngineConfig, CacheBuilder, DeviceBuilder, FifoPicker, FsDeviceBuilder, HybridCacheBuilder,
};
use futures::FutureExt;
use futures::future::BoxFuture;
use mixtrics::registry::prometheus::PrometheusMetricsRegistry;
use risingwave_common::catalog::TableId;
use risingwave_common::config::Role;
use risingwave_common::config::storage::FileCacheRuntimeConfig;
use risingwave_common::license::Feature;
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;
use risingwave_common_service::RpcNotificationClient;
use risingwave_hummock_sdk::{HummockEpoch, HummockSstableObjectId, SyncResult};
use risingwave_object_store::object::build_remote_object_store;
use thiserror_ext::AsReport;

use crate::StateStore;
use crate::compaction_catalog_manager::{CompactionCatalogManager, RemoteTableAccessor};
use crate::error::StorageResult;
use crate::hummock::all::AllRecentFilter;
use crate::hummock::hummock_meta_client::MonitoredHummockMetaClient;
use crate::hummock::none::NoneRecentFilter;
use crate::hummock::sharded::ShardedRecentFilter;
use crate::hummock::simple::SimpleRecentFilter;
use crate::hummock::{
    Block, BlockCacheEventListener, HummockError, HummockStorage, Sstable, SstableBlockIndex,
    SstableStore, SstableStoreConfig,
};
use crate::memory::MemoryStateStore;
use crate::memory::sled::SledStateStore;
use crate::monitor::{
    CompactorMetrics, HummockStateStoreMetrics, MonitoredStateStore, MonitoredStorageMetrics,
    ObjectStoreMetrics,
};
use crate::opts::StorageOpts;

fn build_file_cache_spawner(
    name: &str,
    config: &FileCacheRuntimeConfig,
) -> StorageResult<foyer::Spawner> {
    let config = match config {
        FileCacheRuntimeConfig::Disabled => return Ok(foyer::Spawner::current()),
        FileCacheRuntimeConfig::Unified(config) => config.clone(),
        FileCacheRuntimeConfig::Separated { .. } => {
            return Err(HummockError::other(format!(
                "{name} runtime_config.Separated is unsupported with Foyer 0.22; use runtime_config.Unified instead"
            ))
            .into());
        }
    };

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    #[cfg(madsim)]
    let _ = &config;
    #[cfg(not(madsim))]
    if config.worker_threads != 0 {
        builder.worker_threads(config.worker_threads);
    }
    #[cfg(not(madsim))]
    if config.max_blocking_threads != 0 {
        builder.max_blocking_threads(config.max_blocking_threads);
    }
    builder.thread_name(format!("{name}-unified"));
    let runtime = builder.enable_all().build().map_err(|error| {
        HummockError::other(format!(
            "failed to build {name} dedicated runtime: {}",
            error.as_report()
        ))
    })?;
    Ok(runtime.into())
}

#[cfg(test)]
mod tests {
    use risingwave_common::config::storage::FileCacheTokioRuntimeConfig;

    use super::*;

    #[test]
    fn test_build_file_cache_spawner_rejects_separated_runtime() {
        let runtime_options = FileCacheTokioRuntimeConfig {
            worker_threads: 1,
            max_blocking_threads: 1,
        };
        let error = build_file_cache_spawner(
            "foyer.test",
            &FileCacheRuntimeConfig::Separated {
                read_runtime_options: runtime_options.clone(),
                write_runtime_options: runtime_options,
            },
        )
        .unwrap_err()
        .to_string();

        assert!(error.contains("foyer.test runtime_config.Separated"));
        assert!(error.contains("use runtime_config.Unified instead"));
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn test_build_file_cache_spawner_runtime_modes() {
        let current_runtime_id = tokio::runtime::Handle::current().id();

        let disabled =
            build_file_cache_spawner("foyer.test", &FileCacheRuntimeConfig::Disabled).unwrap();
        let disabled_runtime_id = disabled
            .spawn(async { tokio::runtime::Handle::current().id() })
            .await
            .unwrap();
        assert_eq!(disabled_runtime_id, current_runtime_id);

        let unified = build_file_cache_spawner(
            "foyer.test",
            &FileCacheRuntimeConfig::Unified(FileCacheTokioRuntimeConfig {
                worker_threads: 1,
                max_blocking_threads: 1,
            }),
        )
        .unwrap();
        let (unified_runtime_id, thread_name) = unified
            .spawn(async {
                (
                    tokio::runtime::Handle::current().id(),
                    std::thread::current().name().map(str::to_owned),
                )
            })
            .await
            .unwrap();

        assert_ne!(unified_runtime_id, current_runtime_id);
        assert_eq!(thread_name.as_deref(), Some("foyer.test-unified"));
    }
}

static FOYER_METRICS_REGISTRY: LazyLock<Box<PrometheusMetricsRegistry>> = LazyLock::new(|| {
    Box::new(PrometheusMetricsRegistry::new(
        GLOBAL_METRICS_REGISTRY.clone(),
    ))
});

mod opaque_type {
    use super::*;

    pub type HummockStorageType = impl StateStore + AsHummock;
    pub type MemoryStateStoreType = impl StateStore + AsHummock;
    pub type SledStateStoreType = impl StateStore + AsHummock;

    #[define_opaque(MemoryStateStoreType)]
    pub fn in_memory(state_store: MemoryStateStore) -> MemoryStateStoreType {
        may_dynamic_dispatch(state_store)
    }

    #[define_opaque(HummockStorageType)]
    pub fn hummock(state_store: HummockStorage) -> HummockStorageType {
        may_dynamic_dispatch(may_verify(state_store))
    }

    #[define_opaque(SledStateStoreType)]
    pub fn sled(state_store: SledStateStore) -> SledStateStoreType {
        may_dynamic_dispatch(state_store)
    }
}
pub use opaque_type::{HummockStorageType, MemoryStateStoreType, SledStateStoreType};
use opaque_type::{hummock, in_memory, sled};

#[cfg(feature = "hm-trace")]
type Monitored<S> = MonitoredStateStore<crate::monitor::traced_store::TracedStateStore<S>>;

#[cfg(not(feature = "hm-trace"))]
type Monitored<S> = MonitoredStateStore<S>;

fn monitored<S: StateStore>(
    state_store: S,
    storage_metrics: Arc<MonitoredStorageMetrics>,
) -> Monitored<S> {
    let inner = {
        #[cfg(feature = "hm-trace")]
        {
            crate::monitor::traced_store::TracedStateStore::new_global(state_store)
        }
        #[cfg(not(feature = "hm-trace"))]
        {
            state_store
        }
    };
    inner.monitored(storage_metrics)
}

fn inner<S>(state_store: &Monitored<S>) -> &S {
    let inner = state_store.inner();
    {
        #[cfg(feature = "hm-trace")]
        {
            inner.inner()
        }
        #[cfg(not(feature = "hm-trace"))]
        {
            inner
        }
    }
}

/// The type erased [`StateStore`].
#[derive(Clone, EnumAsInner)]
#[expect(clippy::enum_variant_names)]
pub enum StateStoreImpl {
    /// The Hummock state store, which operates on an S3-like service. URLs beginning with
    /// `hummock` will be automatically recognized as Hummock state store.
    ///
    /// Example URLs:
    ///
    /// * `hummock+s3://bucket`
    /// * `hummock+minio://KEY:SECRET@minio-ip:port`
    /// * `hummock+memory` (should only be used in 1 compute node mode)
    HummockStateStore(Monitored<HummockStorageType>),
    /// In-memory B-Tree state store. Should only be used in unit and integration tests. If you
    /// want speed up e2e test, you should use Hummock in-memory mode instead. Also, this state
    /// store misses some critical implementation to ensure the correctness of persisting streaming
    /// state. (e.g., no `read_epoch` support, no async checkpoint)
    MemoryStateStore(Monitored<MemoryStateStoreType>),
    SledStateStore(Monitored<SledStateStoreType>),
}

fn may_dynamic_dispatch(state_store: impl StateStore + AsHummock) -> impl StateStore + AsHummock {
    #[cfg(not(debug_assertions))]
    {
        state_store
    }
    #[cfg(debug_assertions)]
    {
        use crate::store_impl::dyn_state_store::StateStorePointer;
        StateStorePointer(Arc::new(state_store) as _)
    }
}

fn may_verify(state_store: impl StateStore + AsHummock) -> impl StateStore + AsHummock {
    #[cfg(not(debug_assertions))]
    {
        state_store
    }
    #[cfg(debug_assertions)]
    {
        use std::marker::PhantomData;

        use risingwave_common::util::env_var::env_var_is_true;
        use tracing::info;

        use crate::store_impl::verify::VerifyStateStore;

        let expected = if env_var_is_true("ENABLE_STATE_STORE_VERIFY") {
            info!("enable verify state store");
            Some(SledStateStore::new_temp())
        } else {
            info!("verify state store is not enabled");
            None
        };
        VerifyStateStore {
            actual: state_store,
            expected,
            _phantom: PhantomData::<()>,
        }
    }
}

impl StateStoreImpl {
    fn in_memory(
        state_store: MemoryStateStore,
        storage_metrics: Arc<MonitoredStorageMetrics>,
    ) -> Self {
        // The specific type of MemoryStateStoreType in deducted here.
        Self::MemoryStateStore(monitored(in_memory(state_store), storage_metrics))
    }

    pub fn hummock(
        state_store: HummockStorage,
        storage_metrics: Arc<MonitoredStorageMetrics>,
    ) -> Self {
        // The specific type of HummockStateStoreType in deducted here.
        Self::HummockStateStore(monitored(hummock(state_store), storage_metrics))
    }

    pub fn sled(
        state_store: SledStateStore,
        storage_metrics: Arc<MonitoredStorageMetrics>,
    ) -> Self {
        Self::SledStateStore(monitored(sled(state_store), storage_metrics))
    }

    pub fn shared_in_memory_store(storage_metrics: Arc<MonitoredStorageMetrics>) -> Self {
        Self::in_memory(MemoryStateStore::shared(), storage_metrics)
    }

    pub fn for_test() -> Self {
        Self::in_memory(
            MemoryStateStore::new(),
            Arc::new(MonitoredStorageMetrics::unused()),
        )
    }

    pub fn as_hummock(&self) -> Option<&HummockStorage> {
        match self {
            StateStoreImpl::HummockStateStore(hummock) => {
                Some(inner(hummock).as_hummock().expect("should be hummock"))
            }
            _ => None,
        }
    }
}

impl Debug for StateStoreImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StateStoreImpl::HummockStateStore(_) => write!(f, "HummockStateStore"),
            StateStoreImpl::MemoryStateStore(_) => write!(f, "MemoryStateStore"),
            StateStoreImpl::SledStateStore(_) => write!(f, "SledStateStore"),
        }
    }
}

#[macro_export]
macro_rules! dispatch_state_store {
    ($impl:expr, $store:ident, $body:tt) => {{
        use $crate::store_impl::StateStoreImpl;

        match $impl {
            StateStoreImpl::MemoryStateStore($store) => {
                // WARNING: don't change this. Enabling memory backend will cause monomorphization
                // explosion and thus slow compile time in release mode.
                #[cfg(debug_assertions)]
                {
                    $body
                }
                #[cfg(not(debug_assertions))]
                {
                    let _store = $store;
                    unimplemented!("memory state store should never be used in release mode");
                }
            }

            StateStoreImpl::SledStateStore($store) => {
                // WARNING: don't change this. Enabling memory backend will cause monomorphization
                // explosion and thus slow compile time in release mode.
                #[cfg(debug_assertions)]
                {
                    $body
                }
                #[cfg(not(debug_assertions))]
                {
                    let _store = $store;
                    unimplemented!("sled state store should never be used in release mode");
                }
            }

            StateStoreImpl::HummockStateStore($store) => $body,
        }
    }};
}

#[cfg(any(debug_assertions, test, feature = "test"))]
pub mod verify {
    use std::fmt::Debug;
    use std::future::Future;
    use std::marker::PhantomData;
    use std::ops::Deref;
    use std::sync::Arc;

    use bytes::Bytes;
    use risingwave_common::array::VectorRef;
    use risingwave_common::bitmap::Bitmap;
    use risingwave_common::hash::VirtualNode;
    use risingwave_hummock_sdk::HummockReadEpoch;
    use risingwave_hummock_sdk::key::{FullKey, TableKey, TableKeyRange};
    use tracing::log::warn;

    use crate::error::StorageResult;
    use crate::hummock::HummockStorage;
    use crate::store::*;
    use crate::store_impl::AsHummock;

    #[expect(dead_code)]
    fn assert_result_eq<Item: PartialEq + Debug, E>(
        first: &std::result::Result<Item, E>,
        second: &std::result::Result<Item, E>,
    ) {
        match (first, second) {
            (Ok(first), Ok(second)) => {
                if first != second {
                    warn!("result different: {:?} {:?}", first, second);
                }
                assert_eq!(first, second);
            }
            (Err(_), Err(_)) => {}
            _ => {
                warn!("one success and one failed");
                panic!("result not equal");
            }
        }
    }

    #[derive(Clone)]
    pub struct VerifyStateStore<A, E, T = ()> {
        pub actual: A,
        pub expected: Option<E>,
        pub _phantom: PhantomData<T>,
    }

    impl<A: AsHummock, E: AsHummock> AsHummock for VerifyStateStore<A, E> {
        fn as_hummock(&self) -> Option<&HummockStorage> {
            self.actual.as_hummock()
        }
    }

    impl<A: StateStoreGet, E: StateStoreGet> StateStoreGet for VerifyStateStore<A, E> {
        async fn on_key_value<'a, O: Send + 'a>(
            &'a self,
            key: TableKey<Bytes>,
            read_options: ReadOptions,
            on_key_value_fn: impl KeyValueFn<'a, O>,
        ) -> StorageResult<Option<O>> {
            let actual: Option<(FullKey<Bytes>, Bytes)> = self
                .actual
                .on_key_value(key.clone(), read_options.clone(), |key, value| {
                    Ok((key.copy_into(), Bytes::copy_from_slice(value)))
                })
                .await?;
            if let Some(expected) = &self.expected {
                let expected: Option<(FullKey<Bytes>, Bytes)> = expected
                    .on_key_value(key, read_options, |key, value| {
                        Ok((key.copy_into(), Bytes::copy_from_slice(value)))
                    })
                    .await?;
                assert_eq!(
                    actual
                        .as_ref()
                        .map(|item| (item.0.epoch_with_gap.pure_epoch(), item)),
                    expected
                        .as_ref()
                        .map(|item| (item.0.epoch_with_gap.pure_epoch(), item))
                );
            }

            actual
                .map(|(key, value)| on_key_value_fn(key.to_ref(), value.as_ref()))
                .transpose()
        }
    }

    impl<A: StateStoreReadVector, E: StateStoreReadVector> StateStoreReadVector
        for VerifyStateStore<A, E>
    {
        fn nearest<'a, O: Send + 'a>(
            &'a self,
            vec: VectorRef<'a>,
            options: VectorNearestOptions,
            on_nearest_item_fn: impl OnNearestItemFn<'a, O>,
        ) -> impl StorageFuture<'a, Vec<O>> {
            self.actual.nearest(vec, options, on_nearest_item_fn)
        }
    }

    impl<A: StateStoreRead, E: StateStoreRead> StateStoreRead for VerifyStateStore<A, E> {
        type Iter = impl StateStoreReadIter;
        type RevIter = impl StateStoreReadIter;

        // TODO: may avoid manual async fn when the bug of rust compiler is fixed. Currently it will
        // fail to compile.
        #[expect(clippy::manual_async_fn)]
        fn iter(
            &self,
            key_range: TableKeyRange,

            read_options: ReadOptions,
        ) -> impl Future<Output = StorageResult<Self::Iter>> + '_ {
            async move {
                let actual = self
                    .actual
                    .iter(key_range.clone(), read_options.clone())
                    .await?;
                let expected = if let Some(expected) = &self.expected {
                    Some(expected.iter(key_range, read_options).await?)
                } else {
                    None
                };

                Ok(verify_iter::<StateStoreKeyedRow>(actual, expected))
            }
        }

        #[expect(clippy::manual_async_fn)]
        fn rev_iter(
            &self,
            key_range: TableKeyRange,

            read_options: ReadOptions,
        ) -> impl Future<Output = StorageResult<Self::RevIter>> + '_ {
            async move {
                let actual = self
                    .actual
                    .rev_iter(key_range.clone(), read_options.clone())
                    .await?;
                let expected = if let Some(expected) = &self.expected {
                    Some(expected.rev_iter(key_range, read_options).await?)
                } else {
                    None
                };

                Ok(verify_iter::<StateStoreKeyedRow>(actual, expected))
            }
        }
    }

    impl<A: StateStoreReadLog, E: StateStoreReadLog> StateStoreReadLog for VerifyStateStore<A, E> {
        type ChangeLogIter = impl StateStoreReadChangeLogIter;

        async fn next_epoch(&self, epoch: u64, options: NextEpochOptions) -> StorageResult<u64> {
            let actual = self.actual.next_epoch(epoch, options.clone()).await?;
            if let Some(expected) = &self.expected {
                assert_eq!(actual, expected.next_epoch(epoch, options).await?);
            }
            Ok(actual)
        }

        async fn iter_log(
            &self,
            epoch_range: (u64, u64),
            key_range: TableKeyRange,
            options: ReadLogOptions,
        ) -> StorageResult<Self::ChangeLogIter> {
            let actual = self
                .actual
                .iter_log(epoch_range, key_range.clone(), options.clone())
                .await?;
            let expected = if let Some(expected) = &self.expected {
                Some(expected.iter_log(epoch_range, key_range, options).await?)
            } else {
                None
            };

            Ok(verify_iter::<StateStoreReadLogItem>(actual, expected))
        }
    }

    impl<A: StateStoreIter<T>, E: StateStoreIter<T>, T: IterItem> StateStoreIter<T>
        for VerifyStateStore<A, E, T>
    where
        for<'a> T::ItemRef<'a>: PartialEq + Debug,
    {
        async fn try_next(&mut self) -> StorageResult<Option<T::ItemRef<'_>>> {
            let actual = self.actual.try_next().await?;
            if let Some(expected) = self.expected.as_mut() {
                let expected = expected.try_next().await?;
                assert_eq!(actual, expected);
            }
            Ok(actual)
        }
    }

    fn verify_iter<T: IterItem>(
        actual: impl StateStoreIter<T>,
        expected: Option<impl StateStoreIter<T>>,
    ) -> impl StateStoreIter<T>
    where
        for<'a> T::ItemRef<'a>: PartialEq + Debug,
    {
        VerifyStateStore {
            actual,
            expected,
            _phantom: PhantomData::<T>,
        }
    }

    impl<A: LocalStateStore, E: LocalStateStore> LocalStateStore for VerifyStateStore<A, E> {
        type FlushedSnapshotReader =
            VerifyStateStore<A::FlushedSnapshotReader, E::FlushedSnapshotReader>;

        type Iter<'a> = impl StateStoreIter + 'a;
        type RevIter<'a> = impl StateStoreIter + 'a;

        #[expect(clippy::manual_async_fn)]
        fn iter(
            &self,
            key_range: TableKeyRange,
            read_options: ReadOptions,
        ) -> impl Future<Output = StorageResult<Self::Iter<'_>>> + Send + '_ {
            async move {
                let actual = self
                    .actual
                    .iter(key_range.clone(), read_options.clone())
                    .await?;
                let expected = if let Some(expected) = &self.expected {
                    Some(expected.iter(key_range, read_options).await?)
                } else {
                    None
                };

                Ok(verify_iter::<StateStoreKeyedRow>(actual, expected))
            }
        }

        #[expect(clippy::manual_async_fn)]
        fn rev_iter(
            &self,
            key_range: TableKeyRange,
            read_options: ReadOptions,
        ) -> impl Future<Output = StorageResult<Self::RevIter<'_>>> + Send + '_ {
            async move {
                let actual = self
                    .actual
                    .rev_iter(key_range.clone(), read_options.clone())
                    .await?;
                let expected = if let Some(expected) = &self.expected {
                    Some(expected.rev_iter(key_range, read_options).await?)
                } else {
                    None
                };

                Ok(verify_iter::<StateStoreKeyedRow>(actual, expected))
            }
        }

        fn insert(
            &mut self,
            key: TableKey<Bytes>,
            new_val: Bytes,
            old_val: Option<Bytes>,
        ) -> StorageResult<()> {
            if let Some(expected) = &mut self.expected {
                expected.insert(key.clone(), new_val.clone(), old_val.clone())?;
            }
            self.actual.insert(key, new_val, old_val)?;

            Ok(())
        }

        fn delete(&mut self, key: TableKey<Bytes>, old_val: Bytes) -> StorageResult<()> {
            if let Some(expected) = &mut self.expected {
                expected.delete(key.clone(), old_val.clone())?;
            }
            self.actual.delete(key, old_val)?;
            Ok(())
        }

        async fn update_vnode_bitmap(&mut self, vnodes: Arc<Bitmap>) -> StorageResult<Arc<Bitmap>> {
            let ret = self.actual.update_vnode_bitmap(vnodes.clone()).await?;
            if let Some(expected) = &mut self.expected {
                assert_eq!(ret, expected.update_vnode_bitmap(vnodes).await?);
            }
            Ok(ret)
        }

        fn get_table_watermark(&self, vnode: VirtualNode) -> Option<Bytes> {
            let ret = self.actual.get_table_watermark(vnode);
            if let Some(expected) = &self.expected {
                assert_eq!(ret, expected.get_table_watermark(vnode));
            }
            ret
        }

        fn new_flushed_snapshot_reader(&self) -> Self::FlushedSnapshotReader {
            VerifyStateStore {
                actual: self.actual.new_flushed_snapshot_reader(),
                expected: self.expected.as_ref().map(E::new_flushed_snapshot_reader),
                _phantom: Default::default(),
            }
        }
    }

    impl<A: StateStoreWriteEpochControl, E: StateStoreWriteEpochControl> StateStoreWriteEpochControl
        for VerifyStateStore<A, E>
    {
        async fn flush(&mut self) -> StorageResult<usize> {
            if let Some(expected) = &mut self.expected {
                expected.flush().await?;
            }
            self.actual.flush().await
        }

        async fn try_flush(&mut self) -> StorageResult<()> {
            if let Some(expected) = &mut self.expected {
                expected.try_flush().await?;
            }
            self.actual.try_flush().await
        }

        async fn init(&mut self, options: InitOptions) -> StorageResult<()> {
            self.actual.init(options.clone()).await?;
            if let Some(expected) = &mut self.expected {
                expected.init(options).await?;
            }
            Ok(())
        }

        fn seal_current_epoch(&mut self, next_epoch: u64, opts: SealCurrentEpochOptions) {
            if let Some(expected) = &mut self.expected {
                expected.seal_current_epoch(next_epoch, opts.clone());
            }
            self.actual.seal_current_epoch(next_epoch, opts);
        }
    }

    impl<A: StateStore, E: StateStore> StateStore for VerifyStateStore<A, E> {
        type Local = VerifyStateStore<A::Local, E::Local>;
        type ReadSnapshot = VerifyStateStore<A::ReadSnapshot, E::ReadSnapshot>;
        type VectorWriter = A::VectorWriter;

        fn try_wait_epoch(
            &self,
            epoch: HummockReadEpoch,
            options: TryWaitEpochOptions,
        ) -> impl Future<Output = StorageResult<()>> + Send + '_ {
            self.actual.try_wait_epoch(epoch, options)
        }

        async fn new_local(&self, option: NewLocalOptions) -> Self::Local {
            let expected = if let Some(expected) = &self.expected {
                Some(expected.new_local(option.clone()).await)
            } else {
                None
            };
            VerifyStateStore {
                actual: self.actual.new_local(option).await,
                expected,
                _phantom: PhantomData::<()>,
            }
        }

        async fn new_read_snapshot(
            &self,
            epoch: HummockReadEpoch,
            options: NewReadSnapshotOptions,
        ) -> StorageResult<Self::ReadSnapshot> {
            let expected = if let Some(expected) = &self.expected {
                Some(expected.new_read_snapshot(epoch, options).await?)
            } else {
                None
            };
            Ok(VerifyStateStore {
                actual: self.actual.new_read_snapshot(epoch, options).await?,
                expected,
                _phantom: PhantomData::<()>,
            })
        }

        fn new_vector_writer(
            &self,
            options: NewVectorWriterOptions,
        ) -> impl Future<Output = Self::VectorWriter> + Send + '_ {
            self.actual.new_vector_writer(options)
        }
    }

    impl<A, E> Deref for VerifyStateStore<A, E> {
        type Target = A;

        fn deref(&self) -> &Self::Target {
            &self.actual
        }
    }
}

impl StateStoreImpl {
    #[cfg_attr(not(target_os = "linux"), allow(unused_variables))]
    #[expect(clippy::borrowed_box)]
    pub async fn new(
        s: &str,
        role: Role,
        opts: Arc<StorageOpts>,
        hummock_meta_client: Arc<MonitoredHummockMetaClient>,
        state_store_metrics: Arc<HummockStateStoreMetrics>,
        object_store_metrics: Arc<ObjectStoreMetrics>,
        storage_metrics: Arc<MonitoredStorageMetrics>,
        compactor_metrics: Arc<CompactorMetrics>,
        await_tree_config: Option<await_tree::Config>,
        use_new_object_prefix_strategy: bool,
    ) -> StorageResult<Self> {
        const KB: usize = 1 << 10;
        const MB: usize = 1 << 20;

        let meta_cache = {
            let mut builder = HybridCacheBuilder::new()
                .with_name("foyer.meta")
                .with_metrics_registry(FOYER_METRICS_REGISTRY.clone())
                .memory(opts.meta_cache_capacity_mb * MB)
                .with_shards(opts.meta_cache_shard_num)
                .with_eviction_config(opts.meta_cache_eviction_config.clone())
                .with_weighter(|_: &HummockSstableObjectId, value: &Box<Sstable>| {
                    std::mem::size_of::<HummockSstableObjectId>()
                        + value.estimated_meta_cache_memory_weight()
                })
                .storage();

            if !opts.meta_file_cache_dir.is_empty() {
                if let Err(e) = Feature::ElasticDiskCache.check_available() {
                    tracing::warn!(error = %e.as_report(), "ElasticDiskCache is not available.");
                } else {
                    let device = FsDeviceBuilder::new(&opts.meta_file_cache_dir)
                        .with_capacity(opts.meta_file_cache_capacity_mb * MB)
                        .with_throttle(opts.meta_file_cache_throttle.clone())
                        .build()
                        .map_err(HummockError::foyer_error)?;
                    let engine_builder = BlockEngineConfig::new(device)
                        .with_block_size(opts.meta_file_cache_file_capacity_mb * MB)
                        .with_indexer_shards(opts.meta_file_cache_indexer_shards)
                        .with_flushers(opts.meta_file_cache_flushers)
                        .with_reclaimers(opts.meta_file_cache_reclaimers)
                        .with_buffer_pool_size(opts.meta_file_cache_flush_buffer_threshold_mb * MB)
                        .with_submit_queue_size_threshold(
                            opts.meta_file_cache_submit_queue_size_threshold_mb * MB,
                        )
                        .with_clean_block_threshold(
                            opts.meta_file_cache_reclaimers + opts.meta_file_cache_reclaimers / 2,
                        )
                        .with_recover_concurrency(opts.meta_file_cache_recover_concurrency)
                        .with_blob_index_size(opts.meta_file_cache_blob_index_size_kb * KB)
                        .with_eviction_pickers(vec![Box::new(FifoPicker::new(
                            opts.meta_file_cache_fifo_probation_ratio,
                        ))]);
                    builder = builder
                        .with_engine_config(engine_builder)
                        .with_recover_mode(opts.meta_file_cache_recover_mode)
                        .with_compression(opts.meta_file_cache_compression);
                    builder = builder.with_spawner(build_file_cache_spawner(
                        "foyer.meta",
                        &opts.meta_file_cache_runtime_config,
                    )?);
                }
            }

            builder.build().await.map_err(HummockError::foyer_error)?
        };

        let block_cache = {
            let mut builder = HybridCacheBuilder::new()
                .with_name("foyer.data")
                .with_metrics_registry(FOYER_METRICS_REGISTRY.clone())
                .with_event_listener(Arc::new(BlockCacheEventListener::new(
                    state_store_metrics.clone(),
                )))
                .memory(opts.block_cache_capacity_mb * MB)
                .with_shards(opts.block_cache_shard_num)
                .with_eviction_config(opts.block_cache_eviction_config.clone())
                .with_weighter(|_: &SstableBlockIndex, value: &Box<Block>| {
                    std::mem::size_of::<SstableBlockIndex>() + value.estimated_memory_weight()
                })
                .storage();

            if !opts.data_file_cache_dir.is_empty() {
                if let Err(e) = Feature::ElasticDiskCache.check_available() {
                    tracing::warn!(error = %e.as_report(), "ElasticDiskCache is not available.");
                } else {
                    let device = FsDeviceBuilder::new(&opts.data_file_cache_dir)
                        .with_capacity(opts.data_file_cache_capacity_mb * MB)
                        .with_throttle(opts.data_file_cache_throttle.clone())
                        .build()
                        .map_err(HummockError::foyer_error)?;
                    let engine_builder = BlockEngineConfig::new(device)
                        .with_block_size(opts.data_file_cache_file_capacity_mb * MB)
                        .with_indexer_shards(opts.data_file_cache_indexer_shards)
                        .with_flushers(opts.data_file_cache_flushers)
                        .with_reclaimers(opts.data_file_cache_reclaimers)
                        .with_buffer_pool_size(opts.data_file_cache_flush_buffer_threshold_mb * MB)
                        .with_submit_queue_size_threshold(
                            opts.data_file_cache_submit_queue_size_threshold_mb * MB,
                        )
                        .with_clean_block_threshold(
                            opts.data_file_cache_reclaimers + opts.data_file_cache_reclaimers / 2,
                        )
                        .with_recover_concurrency(opts.data_file_cache_recover_concurrency)
                        .with_blob_index_size(opts.data_file_cache_blob_index_size_kb * KB)
                        .with_eviction_pickers(vec![Box::new(FifoPicker::new(
                            opts.data_file_cache_fifo_probation_ratio,
                        ))]);
                    builder = builder
                        .with_engine_config(engine_builder)
                        .with_recover_mode(opts.data_file_cache_recover_mode)
                        .with_compression(opts.data_file_cache_compression);
                    builder = builder.with_spawner(build_file_cache_spawner(
                        "foyer.data",
                        &opts.data_file_cache_runtime_config,
                    )?);
                }
            }

            builder.build().await.map_err(HummockError::foyer_error)?
        };

        let vector_meta_cache = CacheBuilder::new(opts.vector_meta_cache_capacity_mb * MB)
            .with_shards(opts.vector_meta_cache_shard_num)
            .with_eviction_config(opts.vector_meta_cache_eviction_config.clone())
            .build();

        let vector_block_cache = CacheBuilder::new(opts.vector_block_cache_capacity_mb * MB)
            .with_shards(opts.vector_block_cache_shard_num)
            .with_eviction_config(opts.vector_block_cache_eviction_config.clone())
            .build();

        let recent_filter = if opts.data_file_cache_dir.is_empty() {
            Arc::new(NoneRecentFilter::default().into())
        } else if opts.cache_refill_recent_filter_shards == 1 {
            Arc::new(
                SimpleRecentFilter::new(
                    opts.cache_refill_recent_filter_layers,
                    Duration::from_millis(
                        opts.cache_refill_recent_filter_rotate_interval_ms as u64,
                    ),
                )
                .into(),
            )
        } else if opts.cache_refill_skip_recent_filter {
            Arc::new(AllRecentFilter::default().into())
        } else {
            Arc::new(
                ShardedRecentFilter::new(
                    opts.cache_refill_recent_filter_layers,
                    Duration::from_millis(
                        opts.cache_refill_recent_filter_rotate_interval_ms as u64,
                    ),
                    opts.cache_refill_recent_filter_shards,
                )
                .into(),
            )
        };

        let store = match s {
            hummock if hummock.starts_with("hummock+") => {
                let object_store = build_remote_object_store(
                    hummock.strip_prefix("hummock+").unwrap(),
                    object_store_metrics.clone(),
                    "Hummock",
                    Arc::new(opts.object_store_config.clone()),
                    &opts.data_directory,
                )
                .await;

                let sstable_store = Arc::new(SstableStore::new(SstableStoreConfig {
                    store: Arc::new(object_store),
                    path: opts.data_directory.clone(),
                    prefetch_buffer_capacity: opts.prefetch_buffer_capacity_mb * (1 << 20),
                    max_prefetch_block_number: opts.max_prefetch_block_number,
                    recent_filter,
                    state_store_metrics: state_store_metrics.clone(),
                    use_new_object_prefix_strategy,
                    skip_bloom_filter_in_serde: opts.sst_skip_bloom_filter_in_serde,

                    meta_cache,
                    block_cache,
                    vector_meta_cache,
                    vector_block_cache,
                }));
                let notification_client =
                    RpcNotificationClient::new(hummock_meta_client.get_inner().clone());
                let compaction_catalog_manager_ref =
                    Arc::new(CompactionCatalogManager::new(Box::new(
                        RemoteTableAccessor::new(hummock_meta_client.get_inner().clone()),
                    )));

                let inner = HummockStorage::new(
                    role,
                    opts.clone(),
                    sstable_store,
                    hummock_meta_client.clone(),
                    notification_client,
                    compaction_catalog_manager_ref,
                    state_store_metrics.clone(),
                    compactor_metrics.clone(),
                    await_tree_config,
                )
                .await?;

                StateStoreImpl::hummock(inner, storage_metrics)
            }

            "in_memory" | "in-memory" => {
                tracing::warn!(
                    "In-memory state store should never be used in end-to-end benchmarks or production environment. Scaling and recovery are not supported."
                );
                StateStoreImpl::shared_in_memory_store(storage_metrics.clone())
            }

            sled if sled.starts_with("sled://") => {
                tracing::warn!(
                    "sled state store should never be used in end-to-end benchmarks or production environment. Scaling and recovery are not supported."
                );
                let path = sled.strip_prefix("sled://").unwrap();
                StateStoreImpl::sled(SledStateStore::new(path), storage_metrics.clone())
            }

            other => unimplemented!("{} state store is not supported", other),
        };

        Ok(store)
    }
}

pub trait AsHummock: Send + Sync {
    fn as_hummock(&self) -> Option<&HummockStorage>;

    fn sync(
        &self,
        sync_table_epochs: Vec<(HummockEpoch, HashSet<TableId>)>,
    ) -> BoxFuture<'_, StorageResult<SyncResult>> {
        async move {
            if let Some(hummock) = self.as_hummock() {
                hummock.sync(sync_table_epochs).await
            } else {
                Ok(SyncResult::default())
            }
        }
        .boxed()
    }
}

impl AsHummock for HummockStorage {
    fn as_hummock(&self) -> Option<&HummockStorage> {
        Some(self)
    }
}

impl AsHummock for MemoryStateStore {
    fn as_hummock(&self) -> Option<&HummockStorage> {
        None
    }
}

impl AsHummock for SledStateStore {
    fn as_hummock(&self) -> Option<&HummockStorage> {
        None
    }
}

#[cfg(debug_assertions)]
mod dyn_state_store {
    use std::future::Future;
    use std::ops::DerefMut;
    use std::sync::Arc;

    use bytes::Bytes;
    use risingwave_common::array::VectorRef;
    use risingwave_common::bitmap::Bitmap;
    use risingwave_common::hash::VirtualNode;
    use risingwave_hummock_sdk::HummockReadEpoch;
    use risingwave_hummock_sdk::key::{TableKey, TableKeyRange};

    use crate::error::StorageResult;
    use crate::hummock::HummockStorage;
    use crate::store::*;
    use crate::store_impl::AsHummock;
    use crate::vector::VectorDistance;

    #[async_trait::async_trait]
    pub trait DynStateStoreIter<T: IterItem>: Send {
        async fn try_next(&mut self) -> StorageResult<Option<T::ItemRef<'_>>>;
    }

    #[async_trait::async_trait]
    impl<T: IterItem, I: StateStoreIter<T>> DynStateStoreIter<T> for I {
        async fn try_next(&mut self) -> StorageResult<Option<T::ItemRef<'_>>> {
            self.try_next().await
        }
    }

    pub type BoxStateStoreIter<'a, T> = Box<dyn DynStateStoreIter<T> + 'a>;
    impl<T: IterItem> StateStoreIter<T> for BoxStateStoreIter<'_, T> {
        fn try_next(
            &mut self,
        ) -> impl Future<Output = StorageResult<Option<T::ItemRef<'_>>>> + Send + '_ {
            self.deref_mut().try_next()
        }
    }

    // For StateStoreRead

    pub type BoxStateStoreReadIter = BoxStateStoreIter<'static, StateStoreKeyedRow>;
    pub type BoxStateStoreReadChangeLogIter = BoxStateStoreIter<'static, StateStoreReadLogItem>;

    #[async_trait::async_trait]
    pub trait DynStateStoreGet: StaticSendSync {
        async fn get_keyed_row(
            &self,
            key: TableKey<Bytes>,
            read_options: ReadOptions,
        ) -> StorageResult<Option<StateStoreKeyedRow>>;
    }

    #[async_trait::async_trait]
    pub trait DynStateStoreRead: DynStateStoreGet + StaticSendSync {
        async fn iter(
            &self,
            key_range: TableKeyRange,

            read_options: ReadOptions,
        ) -> StorageResult<BoxStateStoreReadIter>;

        async fn rev_iter(
            &self,
            key_range: TableKeyRange,

            read_options: ReadOptions,
        ) -> StorageResult<BoxStateStoreReadIter>;
    }

    #[async_trait::async_trait]
    pub trait DynStateStoreReadLog: StaticSendSync {
        async fn next_epoch(&self, epoch: u64, options: NextEpochOptions) -> StorageResult<u64>;
        async fn iter_log(
            &self,
            epoch_range: (u64, u64),
            key_range: TableKeyRange,
            options: ReadLogOptions,
        ) -> StorageResult<BoxStateStoreReadChangeLogIter>;
    }

    pub type StateStoreReadDynRef = StateStorePointer<Arc<dyn DynStateStoreRead>>;

    #[async_trait::async_trait]
    impl<S: StateStoreGet> DynStateStoreGet for S {
        async fn get_keyed_row(
            &self,
            key: TableKey<Bytes>,
            read_options: ReadOptions,
        ) -> StorageResult<Option<StateStoreKeyedRow>> {
            self.on_key_value(key, read_options, move |key, value| {
                Ok((key.copy_into(), Bytes::copy_from_slice(value)))
            })
            .await
        }
    }

    #[async_trait::async_trait]
    impl<S: StateStoreRead> DynStateStoreRead for S {
        async fn iter(
            &self,
            key_range: TableKeyRange,

            read_options: ReadOptions,
        ) -> StorageResult<BoxStateStoreReadIter> {
            Ok(Box::new(self.iter(key_range, read_options).await?))
        }

        async fn rev_iter(
            &self,
            key_range: TableKeyRange,

            read_options: ReadOptions,
        ) -> StorageResult<BoxStateStoreReadIter> {
            Ok(Box::new(self.rev_iter(key_range, read_options).await?))
        }
    }

    #[async_trait::async_trait]
    impl<S: StateStoreReadLog> DynStateStoreReadLog for S {
        async fn next_epoch(&self, epoch: u64, options: NextEpochOptions) -> StorageResult<u64> {
            self.next_epoch(epoch, options).await
        }

        async fn iter_log(
            &self,
            epoch_range: (u64, u64),
            key_range: TableKeyRange,
            options: ReadLogOptions,
        ) -> StorageResult<BoxStateStoreReadChangeLogIter> {
            Ok(Box::new(
                self.iter_log(epoch_range, key_range, options).await?,
            ))
        }
    }

    // For LocalStateStore
    pub type BoxLocalStateStoreIterStream<'a> = BoxStateStoreIter<'a, StateStoreKeyedRow>;
    #[async_trait::async_trait]
    pub trait DynLocalStateStore:
        DynStateStoreGet + DynStateStoreWriteEpochControl + StaticSendSync
    {
        async fn iter(
            &self,
            key_range: TableKeyRange,
            read_options: ReadOptions,
        ) -> StorageResult<BoxLocalStateStoreIterStream<'_>>;

        async fn rev_iter(
            &self,
            key_range: TableKeyRange,
            read_options: ReadOptions,
        ) -> StorageResult<BoxLocalStateStoreIterStream<'_>>;

        fn new_flushed_snapshot_reader(&self) -> StateStoreReadDynRef;

        fn insert(
            &mut self,
            key: TableKey<Bytes>,
            new_val: Bytes,
            old_val: Option<Bytes>,
        ) -> StorageResult<()>;

        fn delete(&mut self, key: TableKey<Bytes>, old_val: Bytes) -> StorageResult<()>;

        async fn update_vnode_bitmap(&mut self, vnodes: Arc<Bitmap>) -> StorageResult<Arc<Bitmap>>;

        fn get_table_watermark(&self, vnode: VirtualNode) -> Option<Bytes>;
    }

    #[async_trait::async_trait]
    pub trait DynStateStoreWriteEpochControl: StaticSendSync {
        async fn flush(&mut self) -> StorageResult<usize>;

        async fn try_flush(&mut self) -> StorageResult<()>;

        async fn init(&mut self, epoch: InitOptions) -> StorageResult<()>;

        fn seal_current_epoch(&mut self, next_epoch: u64, opts: SealCurrentEpochOptions);
    }

    #[async_trait::async_trait]
    impl<S: LocalStateStore> DynLocalStateStore for S {
        async fn iter(
            &self,
            key_range: TableKeyRange,
            read_options: ReadOptions,
        ) -> StorageResult<BoxLocalStateStoreIterStream<'_>> {
            Ok(Box::new(self.iter(key_range, read_options).await?))
        }

        async fn rev_iter(
            &self,
            key_range: TableKeyRange,
            read_options: ReadOptions,
        ) -> StorageResult<BoxLocalStateStoreIterStream<'_>> {
            Ok(Box::new(self.rev_iter(key_range, read_options).await?))
        }

        fn new_flushed_snapshot_reader(&self) -> StateStoreReadDynRef {
            StateStorePointer(Arc::new(self.new_flushed_snapshot_reader()) as _)
        }

        fn insert(
            &mut self,
            key: TableKey<Bytes>,
            new_val: Bytes,
            old_val: Option<Bytes>,
        ) -> StorageResult<()> {
            self.insert(key, new_val, old_val)
        }

        fn delete(&mut self, key: TableKey<Bytes>, old_val: Bytes) -> StorageResult<()> {
            self.delete(key, old_val)
        }

        async fn update_vnode_bitmap(&mut self, vnodes: Arc<Bitmap>) -> StorageResult<Arc<Bitmap>> {
            self.update_vnode_bitmap(vnodes).await
        }

        fn get_table_watermark(&self, vnode: VirtualNode) -> Option<Bytes> {
            self.get_table_watermark(vnode)
        }
    }

    #[async_trait::async_trait]
    impl<S: StateStoreWriteEpochControl> DynStateStoreWriteEpochControl for S {
        async fn flush(&mut self) -> StorageResult<usize> {
            self.flush().await
        }

        async fn try_flush(&mut self) -> StorageResult<()> {
            self.try_flush().await
        }

        async fn init(&mut self, options: InitOptions) -> StorageResult<()> {
            self.init(options).await
        }

        fn seal_current_epoch(&mut self, next_epoch: u64, opts: SealCurrentEpochOptions) {
            self.seal_current_epoch(next_epoch, opts)
        }
    }

    pub type BoxDynLocalStateStore = StateStorePointer<Box<dyn DynLocalStateStore>>;

    impl LocalStateStore for BoxDynLocalStateStore {
        type FlushedSnapshotReader = StateStoreReadDynRef;
        type Iter<'a> = BoxLocalStateStoreIterStream<'a>;
        type RevIter<'a> = BoxLocalStateStoreIterStream<'a>;

        fn iter(
            &self,
            key_range: TableKeyRange,
            read_options: ReadOptions,
        ) -> impl Future<Output = StorageResult<Self::Iter<'_>>> + Send + '_ {
            (*self.0).iter(key_range, read_options)
        }

        fn rev_iter(
            &self,
            key_range: TableKeyRange,
            read_options: ReadOptions,
        ) -> impl Future<Output = StorageResult<Self::RevIter<'_>>> + Send + '_ {
            (*self.0).rev_iter(key_range, read_options)
        }

        fn new_flushed_snapshot_reader(&self) -> Self::FlushedSnapshotReader {
            (*self.0).new_flushed_snapshot_reader()
        }

        fn get_table_watermark(&self, vnode: VirtualNode) -> Option<Bytes> {
            (*self.0).get_table_watermark(vnode)
        }

        fn insert(
            &mut self,
            key: TableKey<Bytes>,
            new_val: Bytes,
            old_val: Option<Bytes>,
        ) -> StorageResult<()> {
            (*self.0).insert(key, new_val, old_val)
        }

        fn delete(&mut self, key: TableKey<Bytes>, old_val: Bytes) -> StorageResult<()> {
            (*self.0).delete(key, old_val)
        }

        async fn update_vnode_bitmap(&mut self, vnodes: Arc<Bitmap>) -> StorageResult<Arc<Bitmap>> {
            (*self.0).update_vnode_bitmap(vnodes).await
        }
    }

    impl<P> StateStoreWriteEpochControl for StateStorePointer<P>
    where
        StateStorePointer<P>: AsMut<dyn DynStateStoreWriteEpochControl> + StaticSendSync,
    {
        fn flush(&mut self) -> impl Future<Output = StorageResult<usize>> + Send + '_ {
            self.as_mut().flush()
        }

        fn try_flush(&mut self) -> impl Future<Output = StorageResult<()>> + Send + '_ {
            self.as_mut().try_flush()
        }

        fn init(
            &mut self,
            options: InitOptions,
        ) -> impl Future<Output = StorageResult<()>> + Send + '_ {
            self.as_mut().init(options)
        }

        fn seal_current_epoch(&mut self, next_epoch: u64, opts: SealCurrentEpochOptions) {
            self.as_mut().seal_current_epoch(next_epoch, opts)
        }
    }

    #[async_trait::async_trait]
    pub trait DynStateStoreWriteVector: DynStateStoreWriteEpochControl + StaticSendSync {
        fn insert(&mut self, vec: VectorRef<'_>, info: Bytes) -> StorageResult<()>;
    }

    #[async_trait::async_trait]
    impl<S: StateStoreWriteVector> DynStateStoreWriteVector for S {
        fn insert(&mut self, vec: VectorRef<'_>, info: Bytes) -> StorageResult<()> {
            self.insert(vec, info)
        }
    }

    pub type BoxDynStateStoreWriteVector = StateStorePointer<Box<dyn DynStateStoreWriteVector>>;

    impl StateStoreWriteVector for BoxDynStateStoreWriteVector {
        fn insert(&mut self, vec: VectorRef<'_>, info: Bytes) -> StorageResult<()> {
            self.0.insert(vec, info)
        }
    }

    // For global StateStore

    #[async_trait::async_trait]
    pub trait DynStateStoreReadVector: StaticSendSync {
        async fn nearest(
            &self,
            vec: VectorRef<'_>,
            options: VectorNearestOptions,
        ) -> StorageResult<Vec<(Vector, VectorDistance, Bytes)>>;
    }

    #[async_trait::async_trait]
    impl<S: StateStoreReadVector> DynStateStoreReadVector for S {
        async fn nearest(
            &self,
            vec: VectorRef<'_>,
            options: VectorNearestOptions,
        ) -> StorageResult<Vec<(Vector, VectorDistance, Bytes)>> {
            use risingwave_common::types::ScalarRef;
            self.nearest(vec, options, |vec, distance, info| {
                (
                    vec.to_owned_scalar(),
                    distance,
                    Bytes::copy_from_slice(info),
                )
            })
            .await
        }
    }

    impl<P> StateStoreReadVector for StateStorePointer<P>
    where
        StateStorePointer<P>: AsRef<dyn DynStateStoreReadVector> + StaticSendSync,
    {
        async fn nearest<'a, O: Send + 'a>(
            &'a self,
            vec: VectorRef<'a>,
            options: VectorNearestOptions,
            on_nearest_item_fn: impl OnNearestItemFn<'a, O>,
        ) -> StorageResult<Vec<O>> {
            let output = self.as_ref().nearest(vec, options).await?;
            Ok(output
                .into_iter()
                .map(|(vec, distance, info)| {
                    on_nearest_item_fn(vec.to_ref(), distance, info.as_ref())
                })
                .collect())
        }
    }

    pub trait DynStateStoreReadSnapshot:
        DynStateStoreRead + DynStateStoreReadVector + StaticSendSync
    {
    }

    impl<S: DynStateStoreRead + DynStateStoreReadVector + StaticSendSync> DynStateStoreReadSnapshot
        for S
    {
    }

    pub type StateStoreReadSnapshotDynRef = StateStorePointer<Arc<dyn DynStateStoreReadSnapshot>>;
    #[async_trait::async_trait]
    pub trait DynStateStoreExt: StaticSendSync {
        async fn try_wait_epoch(
            &self,
            epoch: HummockReadEpoch,
            options: TryWaitEpochOptions,
        ) -> StorageResult<()>;

        async fn new_local(&self, option: NewLocalOptions) -> BoxDynLocalStateStore;
        async fn new_read_snapshot(
            &self,
            epoch: HummockReadEpoch,
            options: NewReadSnapshotOptions,
        ) -> StorageResult<StateStoreReadSnapshotDynRef>;
        async fn new_vector_writer(
            &self,
            options: NewVectorWriterOptions,
        ) -> BoxDynStateStoreWriteVector;
    }

    #[async_trait::async_trait]
    impl<S: StateStore> DynStateStoreExt for S {
        async fn try_wait_epoch(
            &self,
            epoch: HummockReadEpoch,
            options: TryWaitEpochOptions,
        ) -> StorageResult<()> {
            self.try_wait_epoch(epoch, options).await
        }

        async fn new_local(&self, option: NewLocalOptions) -> BoxDynLocalStateStore {
            StateStorePointer(Box::new(self.new_local(option).await))
        }

        async fn new_read_snapshot(
            &self,
            epoch: HummockReadEpoch,
            options: NewReadSnapshotOptions,
        ) -> StorageResult<StateStoreReadSnapshotDynRef> {
            Ok(StateStorePointer(Arc::new(
                self.new_read_snapshot(epoch, options).await?,
            )))
        }

        async fn new_vector_writer(
            &self,
            options: NewVectorWriterOptions,
        ) -> BoxDynStateStoreWriteVector {
            StateStorePointer(Box::new(self.new_vector_writer(options).await))
        }
    }

    pub type StateStoreDynRef = StateStorePointer<Arc<dyn DynStateStore>>;

    macro_rules! state_store_pointer_dyn_as_ref {
        ($pointer:ident < dyn $source_dyn_trait:ident > , $target_dyn_trait:ident) => {
            impl AsRef<dyn $target_dyn_trait>
                for StateStorePointer<$pointer<dyn $source_dyn_trait>>
            {
                fn as_ref(&self) -> &dyn $target_dyn_trait {
                    (&*self.0) as _
                }
            }
        };
    }

    state_store_pointer_dyn_as_ref!(Arc<dyn DynStateStoreReadSnapshot>, DynStateStoreRead);
    state_store_pointer_dyn_as_ref!(Arc<dyn DynStateStoreReadSnapshot>, DynStateStoreGet);
    state_store_pointer_dyn_as_ref!(Arc<dyn DynStateStoreReadSnapshot>, DynStateStoreReadVector);
    state_store_pointer_dyn_as_ref!(Arc<dyn DynStateStoreRead>, DynStateStoreRead);
    state_store_pointer_dyn_as_ref!(Arc<dyn DynStateStoreRead>, DynStateStoreGet);
    state_store_pointer_dyn_as_ref!(Box<dyn DynLocalStateStore>, DynStateStoreGet);

    macro_rules! state_store_pointer_dyn_as_mut {
        ($pointer:ident < dyn $source_dyn_trait:ident > , $target_dyn_trait:ident) => {
            impl AsMut<dyn $target_dyn_trait>
                for StateStorePointer<$pointer<dyn $source_dyn_trait>>
            {
                fn as_mut(&mut self) -> &mut dyn $target_dyn_trait {
                    (&mut *self.0) as _
                }
            }
        };
    }

    state_store_pointer_dyn_as_mut!(Box<dyn DynLocalStateStore>, DynStateStoreWriteEpochControl);
    state_store_pointer_dyn_as_mut!(
        Box<dyn DynStateStoreWriteVector>,
        DynStateStoreWriteEpochControl
    );

    #[derive(Clone)]
    pub struct StateStorePointer<P>(pub(crate) P);

    impl<P> StateStoreGet for StateStorePointer<P>
    where
        StateStorePointer<P>: AsRef<dyn DynStateStoreGet> + StaticSendSync,
    {
        async fn on_key_value<'a, O: Send + 'a>(
            &'a self,
            key: TableKey<Bytes>,
            read_options: ReadOptions,
            on_key_value_fn: impl KeyValueFn<'a, O>,
        ) -> StorageResult<Option<O>> {
            let option = self.as_ref().get_keyed_row(key, read_options).await?;
            option
                .map(|(key, value)| on_key_value_fn(key.to_ref(), value.as_ref()))
                .transpose()
        }
    }

    impl<P> StateStoreRead for StateStorePointer<P>
    where
        StateStorePointer<P>: AsRef<dyn DynStateStoreRead> + StateStoreGet + StaticSendSync,
    {
        type Iter = BoxStateStoreReadIter;
        type RevIter = BoxStateStoreReadIter;

        fn iter(
            &self,
            key_range: TableKeyRange,

            read_options: ReadOptions,
        ) -> impl Future<Output = StorageResult<Self::Iter>> + '_ {
            self.as_ref().iter(key_range, read_options)
        }

        fn rev_iter(
            &self,
            key_range: TableKeyRange,

            read_options: ReadOptions,
        ) -> impl Future<Output = StorageResult<Self::RevIter>> + '_ {
            self.as_ref().rev_iter(key_range, read_options)
        }
    }

    impl StateStoreReadLog for StateStoreDynRef {
        type ChangeLogIter = BoxStateStoreReadChangeLogIter;

        async fn next_epoch(&self, epoch: u64, options: NextEpochOptions) -> StorageResult<u64> {
            (*self.0).next_epoch(epoch, options).await
        }

        fn iter_log(
            &self,
            epoch_range: (u64, u64),
            key_range: TableKeyRange,
            options: ReadLogOptions,
        ) -> impl Future<Output = StorageResult<Self::ChangeLogIter>> + Send + '_ {
            (*self.0).iter_log(epoch_range, key_range, options)
        }
    }

    pub trait DynStateStore: DynStateStoreReadLog + DynStateStoreExt + AsHummock {}

    impl AsHummock for StateStoreDynRef {
        fn as_hummock(&self) -> Option<&HummockStorage> {
            (*self.0).as_hummock()
        }
    }

    impl<S: DynStateStoreReadLog + DynStateStoreExt + AsHummock> DynStateStore for S {}

    impl StateStore for StateStoreDynRef {
        type Local = BoxDynLocalStateStore;
        type ReadSnapshot = StateStoreReadSnapshotDynRef;
        type VectorWriter = BoxDynStateStoreWriteVector;

        fn try_wait_epoch(
            &self,
            epoch: HummockReadEpoch,
            options: TryWaitEpochOptions,
        ) -> impl Future<Output = StorageResult<()>> + Send + '_ {
            (*self.0).try_wait_epoch(epoch, options)
        }

        fn new_local(
            &self,
            option: NewLocalOptions,
        ) -> impl Future<Output = Self::Local> + Send + '_ {
            (*self.0).new_local(option)
        }

        async fn new_read_snapshot(
            &self,
            epoch: HummockReadEpoch,
            options: NewReadSnapshotOptions,
        ) -> StorageResult<Self::ReadSnapshot> {
            (*self.0).new_read_snapshot(epoch, options).await
        }

        fn new_vector_writer(
            &self,
            options: NewVectorWriterOptions,
        ) -> impl Future<Output = Self::VectorWriter> + Send + '_ {
            (*self.0).new_vector_writer(options)
        }
    }
}
