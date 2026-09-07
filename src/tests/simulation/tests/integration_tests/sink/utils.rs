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

use std::collections::HashMap;
use std::io::Write;
use std::iter::once;
use std::mem::take;
use std::num::NonZero;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use anyhow::Result;
use async_trait::async_trait;
use futures::future::pending;
use futures::stream::{BoxStream, empty, select_all};
use futures::{FutureExt, StreamExt, TryFuture, stream};
use itertools::Itertools;
use rand::prelude::SliceRandom;
use rand::{Rng, rng as thread_rng};
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::catalog::Field;
use risingwave_common::row::Row;
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_connector::sink::boxed::{BoxLogSinker, DynLogReader};
use risingwave_connector::sink::coordinate::CoordinatedLogSinker;
use risingwave_connector::sink::log_store::{
    DeliveryFutureManagerAddFuture, LogStoreReadItem, TruncateOffset,
};
use risingwave_connector::sink::test_sink::{
    TestSinkRegistryGuard, register_build_coordinated_sink, register_build_sink,
};
use risingwave_connector::sink::writer::{
    AsyncTruncateSinkWriter, AsyncTruncateSinkWriterExt, SinkWriter,
};
use risingwave_connector::sink::{
    LogSinker, SinglePhaseCommitCoordinator, SinkCommitCoordinator, SinkCommittedEpochSubscriber,
    SinkError, SinkLogReader, TwoPhaseCommitCoordinator,
};
use risingwave_connector::source::test_source::{
    BoxSource, TestSourceRegistryGuard, TestSourceSplit, register_test_source,
};
use risingwave_pb::connector_service::SinkMetadata;
use risingwave_pb::connector_service::sink_metadata::{Metadata, SerializedMetadata};
use risingwave_pb::stream_plan::PbSinkSchemaChange;
use risingwave_simulation::cluster::{Cluster, ConfigPath, Configuration};
use tokio::task::yield_now;
use tokio::time::sleep;

use crate::{assert_eq_with_err_returned as assert_eq, assert_with_err_returned as assert};

pub const CREATE_SOURCE: &str = "create source test_source (id int, name varchar) with (connector = 'test') FORMAT PLAIN ENCODE JSON";
pub const CREATE_SINK: &str =
    "create sink test_sink from test_source with (connector = 'test', type = 'upsert')";
pub const DROP_SINK: &str = "drop sink test_sink";
pub const DROP_SOURCE: &str = "drop source test_source";

pub struct TestSinkStoreInner {
    id_name: HashMap<i32, Vec<String>>,
    epochs: Vec<u64>,
    checkpoint_count: usize,
    err_count: usize,
}

#[derive(Clone)]
pub struct TestSinkStore {
    inner: Arc<Mutex<TestSinkStoreInner>>,
}

impl TestSinkStore {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(TestSinkStoreInner {
                id_name: HashMap::new(),
                epochs: Vec::new(),
                checkpoint_count: 0,
                err_count: 0,
            })),
        }
    }

    pub fn insert_many(&self, pairs: impl Iterator<Item = (i32, Vec<String>)>) {
        let mut inner = self.inner();
        for (id, names) in pairs {
            inner.id_name.entry(id).or_default().extend(names);
        }
    }

    pub fn insert(&self, id: i32, name: String) {
        self.inner().id_name.entry(id).or_default().push(name);
    }

    pub fn begin_epoch(&self, epoch: u64) {
        self.inner().epochs.push(epoch)
    }

    fn inner(&self) -> MutexGuard<'_, TestSinkStoreInner> {
        self.inner.lock().unwrap()
    }

    pub fn check_simple_result(&self, id_list: &[i32]) -> anyhow::Result<()> {
        let inner = self.inner();
        assert_eq!(inner.id_name.len(), id_list.len());
        for id in id_list {
            let names = inner.id_name.get(id).unwrap();
            assert!(!names.is_empty());
            for name in names {
                assert_eq!(name, &simple_name_of_id(*id));
            }
        }
        Ok(())
    }

    pub fn check_result_rows(&self, rows: &[(i32, &str)]) -> anyhow::Result<()> {
        let inner = self.inner();
        let actual = inner
            .id_name
            .iter()
            .flat_map(|(id, names)| names.iter().map(|name| (*id, name.as_str())))
            .sorted()
            .collect_vec();
        let expected = rows.iter().copied().sorted().collect_vec();
        assert_eq!(actual, expected);
        Ok(())
    }

    pub fn id_count(&self) -> usize {
        self.inner().id_name.len()
    }

    pub fn inc_checkpoint(&self) {
        self.inner().checkpoint_count += 1;
    }

    pub fn checkpoint_count(&self) -> usize {
        self.inner().checkpoint_count
    }

    pub fn inc_err(&self) {
        self.inner().err_count += 1;
    }

    pub fn err_count(&self) -> usize {
        self.inner().err_count
    }

    pub async fn wait_for_count(&self, count: usize) -> anyhow::Result<()> {
        let mut prev_count = 0;
        let mut has_printed = false;
        loop {
            sleep(Duration::from_secs(1)).await;
            let curr_count = self.id_count();
            if curr_count >= count {
                assert_eq!(count, curr_count);
                break;
            }
            if curr_count == prev_count {
                if !has_printed {
                    println!("not making progress: curr {}", curr_count);
                    has_printed = true;
                }
                sleep(Duration::from_secs(3)).await;
            } else {
                prev_count = curr_count;
                has_printed = false;
            }
        }
        Ok(())
    }

    pub async fn wait_for_err(&self, count: usize) -> anyhow::Result<()> {
        loop {
            sleep(Duration::from_secs(1)).await;
            if self.err_count() >= count {
                break;
            }
        }
        Ok(())
    }
}

pub struct TestWriter {
    store: TestSinkStore,
    parallelism_counter: Arc<AtomicUsize>,
    err_rate: Arc<AtomicU32>,
    barrier_delay_ms: Option<Arc<AtomicU64>>,
}

#[async_trait]
impl SinkWriter for TestWriter {
    async fn begin_epoch(&mut self, epoch: u64) -> risingwave_connector::sink::Result<()> {
        self.store.begin_epoch(epoch);
        Ok(())
    }

    async fn write_batch(&mut self, chunk: StreamChunk) -> risingwave_connector::sink::Result<()> {
        if SimulationTestSink::random_err(&self.err_rate) {
            println!("write with err");
            self.store.inc_err();
            return Err(SinkError::Internal(anyhow::anyhow!("fail to write")));
        }
        for (op, row) in chunk.rows() {
            assert_eq!(op, Op::Insert);
            assert_eq!(row.len(), 2);
            let id = row.datum_at(0).unwrap().into_int32();
            let name = row.datum_at(1).unwrap().into_utf8().to_string();
            self.store.insert(id, name);
        }
        Ok(())
    }

    async fn barrier(
        &mut self,
        is_checkpoint: bool,
    ) -> risingwave_connector::sink::Result<Self::CommitMetadata> {
        if let Some(barrier_delay_ms) = &self.barrier_delay_ms {
            let barrier_delay_ms = barrier_delay_ms.load(Relaxed);
            if barrier_delay_ms > 0 {
                sleep(Duration::from_millis(barrier_delay_ms)).await;
            }
        } else if is_checkpoint {
            self.store.inc_checkpoint();
            sleep(Duration::from_millis(100)).await;
        }
        Ok(())
    }
}

impl Drop for TestWriter {
    fn drop(&mut self) {
        self.parallelism_counter.fetch_sub(1, Relaxed);
    }
}

#[derive(Default)]
struct StagingDataStoreInner {
    next_file_id: usize,
    files: HashMap<usize, HashMap<i32, Vec<String>>>,
}

#[derive(Default, Clone)]
struct StagingDataStore {
    inner: Arc<Mutex<StagingDataStoreInner>>,
}

impl StagingDataStore {
    fn add(&self, id_names: HashMap<i32, Vec<String>>) -> SinkMetadata {
        let mut inner = self.inner.lock().unwrap();
        let file_id = inner.next_file_id;
        inner.next_file_id += 1;
        inner.files.insert(file_id, id_names);
        SinkMetadata {
            metadata: Some(Metadata::Serialized(SerializedMetadata {
                metadata: file_id.to_le_bytes().into(),
            })),
        }
    }

    fn get(&self, file_id: usize) -> HashMap<i32, Vec<String>> {
        self.inner.lock().unwrap().files[&file_id].clone()
    }
}

pub struct CoordinatedTestWriter {
    store: TestSinkStore,
    parallelism_counter: Arc<AtomicUsize>,
    err_rate: Arc<AtomicU32>,
    staging: HashMap<i32, Vec<String>>,
    staging_store: StagingDataStore,
}

#[async_trait]
impl SinkWriter for CoordinatedTestWriter {
    type CommitMetadata = Option<SinkMetadata>;

    async fn begin_epoch(&mut self, epoch: u64) -> risingwave_connector::sink::Result<()> {
        self.store.begin_epoch(epoch);
        Ok(())
    }

    async fn write_batch(&mut self, chunk: StreamChunk) -> risingwave_connector::sink::Result<()> {
        if SimulationTestSink::random_err(&self.err_rate) {
            println!("write with err");
            self.store.inc_err();
            return Err(SinkError::Internal(anyhow::anyhow!("fail to write")));
        }
        for (op, row) in chunk.rows() {
            assert_eq!(op, Op::Insert);
            assert_eq!(row.len(), 2);
            let id = row.datum_at(0).unwrap().into_int32();
            let name = row.datum_at(1).unwrap().into_utf8().to_string();
            self.staging.entry(id).or_default().push(name);
        }
        Ok(())
    }

    async fn barrier(
        &mut self,
        is_checkpoint: bool,
    ) -> risingwave_connector::sink::Result<Self::CommitMetadata> {
        let metadata = if is_checkpoint {
            self.store.inc_checkpoint();
            sleep(Duration::from_millis(100)).await;
            Some(self.staging_store.add(take(&mut self.staging)))
        } else {
            None
        };
        Ok(metadata)
    }
}

impl Drop for CoordinatedTestWriter {
    fn drop(&mut self) {
        self.parallelism_counter.fetch_sub(1, Relaxed);
    }
}

pub struct TestCoordinator {
    store: TestSinkStore,
    err_rate: Arc<AtomicU32>,
    staging_store: StagingDataStore,
}

#[async_trait]
impl SinglePhaseCommitCoordinator for TestCoordinator {
    async fn init(&mut self) -> risingwave_connector::sink::Result<()> {
        Ok(())
    }

    /// After collecting the metadata from each sink writer, a coordinator will call `commit` with
    /// the set of metadata. The metadata is serialized into bytes, because the metadata is expected
    /// to be passed between different gRPC node, so in this general trait, the metadata is
    /// serialized bytes.
    async fn commit_data(
        &mut self,
        epoch: u64,
        metadata: Vec<SinkMetadata>,
    ) -> risingwave_connector::sink::Result<()> {
        if SimulationTestSink::random_err(&self.err_rate) {
            println!("commit with err");
            self.store.inc_err();
            return Err(SinkError::Internal(anyhow::anyhow!("fail to commit")));
        }
        let file_ids = metadata.into_iter().map(|metadata| {
            let Metadata::Serialized(serialized) = metadata.metadata.unwrap();
            usize::from_le_bytes((serialized.metadata.as_slice()).try_into().unwrap())
        });
        self.store.insert_many(
            file_ids
                .into_iter()
                .map(|file_id| self.staging_store.get(file_id))
                .flatten(),
        );
        Ok(())
    }
}

#[async_trait]
impl TwoPhaseCommitCoordinator for TestCoordinator {
    async fn init(&mut self) -> risingwave_connector::sink::Result<()> {
        Ok(())
    }

    async fn pre_commit(
        &mut self,
        epoch: u64,
        metadata: Vec<SinkMetadata>,
        _schema_change: Option<PbSinkSchemaChange>,
    ) -> risingwave_connector::sink::Result<Option<Vec<u8>>> {
        if SimulationTestSink::random_err(&self.err_rate) {
            println!("pre-commit with err");
            self.store.inc_err();
            return Err(SinkError::Internal(anyhow::anyhow!("fail to pre-commit")));
        }

        if metadata.is_empty() {
            return Ok(None);
        }

        let mut pre_commit_metadata_bytes = Vec::new();
        for metadata in metadata {
            let Metadata::Serialized(serialized) = metadata.metadata.unwrap();
            pre_commit_metadata_bytes.push(serialized.metadata);
        }

        let pre_commit_metadata_bytes: Vec<u8> =
            serde_json::to_vec(&pre_commit_metadata_bytes).unwrap();

        Ok(Some(pre_commit_metadata_bytes))
    }

    async fn commit_data(
        &mut self,
        epoch: u64,
        commit_metadata: Vec<u8>,
    ) -> risingwave_connector::sink::Result<()> {
        if commit_metadata.is_empty() {
            return Ok(());
        }

        if SimulationTestSink::random_err(&self.err_rate) {
            println!("commit with err");
            self.store.inc_err();
            return Err(SinkError::Internal(anyhow::anyhow!("fail to commit")));
        }

        let write_results_bytes: Vec<Vec<u8>> = serde_json::from_slice(&commit_metadata).unwrap();

        let file_ids = write_results_bytes
            .into_iter()
            .map(|metadata_bytes: Vec<u8>| {
                usize::from_le_bytes((metadata_bytes.as_slice()).try_into().unwrap())
            });

        self.store.insert_many(
            file_ids
                .into_iter()
                .map(|file_id| self.staging_store.get(file_id))
                .flatten(),
        );
        Ok(())
    }

    async fn abort(&mut self, epoch: u64, commit_metadata: Vec<u8>) {
        // No-op
    }
}

pub struct AsyncTruncateTestWriter {
    store: TestSinkStore,
    parallelism_counter: Arc<AtomicUsize>,
    err_rate: Arc<AtomicU32>,
}

impl AsyncTruncateSinkWriter for AsyncTruncateTestWriter {
    type DeliveryFuture = impl TryFuture<Ok = (), Error = SinkError> + Unpin + Send + 'static;

    async fn write_chunk<'a>(
        &'a mut self,
        chunk: StreamChunk,
        mut add_future: DeliveryFutureManagerAddFuture<'a, Self::DeliveryFuture>,
    ) -> risingwave_connector::sink::Result<()> {
        if SimulationTestSink::random_err(&self.err_rate) {
            println!("write with err");
            self.store.inc_err();
            return Err(SinkError::Internal(anyhow::anyhow!("fail to write")));
        }
        for (op, row) in chunk.rows() {
            assert_eq!(op, Op::Insert);
            assert_eq!(row.len(), 2);
            let id = row.datum_at(0).unwrap().into_int32();
            let name = row.datum_at(1).unwrap().into_utf8().to_string();
            let store = self.store.clone();
            let ack_err = SimulationTestSink::random_err(&self.err_rate);
            add_future
                .add_future_may_await(
                    async move {
                        if thread_rng().random_bool(0.5) {
                            let micro = (thread_rng().random::<u32>() as f64 * 30.0
                                / (u32::MAX as f64)) as _;
                            sleep(Duration::from_micros(micro)).await;
                        } else if ack_err {
                            println!("ack with err");
                            store.inc_err();
                            return Err(SinkError::Internal(anyhow::anyhow!("fail to ack")));
                        }
                        store.insert(id, name);
                        Ok(())
                    }
                    .boxed(),
                )
                .await?;
        }
        Ok(())
    }

    async fn barrier(&mut self, is_checkpoint: bool) -> risingwave_connector::sink::Result<()> {
        if is_checkpoint {
            self.store.inc_checkpoint();
        }
        Ok(())
    }
}

impl Drop for AsyncTruncateTestWriter {
    fn drop(&mut self) {
        self.parallelism_counter.fetch_sub(1, Relaxed);
    }
}

pub struct DelayedAsyncTruncateTestWriter {
    store: TestSinkStore,
    parallelism_counter: Arc<AtomicUsize>,
}

impl AsyncTruncateSinkWriter for DelayedAsyncTruncateTestWriter {
    type DeliveryFuture = impl TryFuture<Ok = (), Error = SinkError> + Unpin + Send + 'static;

    async fn write_chunk<'a>(
        &'a mut self,
        chunk: StreamChunk,
        mut add_future: DeliveryFutureManagerAddFuture<'a, Self::DeliveryFuture>,
    ) -> risingwave_connector::sink::Result<()> {
        for (op, row) in chunk.rows() {
            assert_eq!(op, Op::Insert);
            assert!(row.len() >= 2);
            let id = row.datum_at(0).unwrap().into_int32();
            let name = row.datum_at(1).unwrap().into_utf8().to_string();
            let store = self.store.clone();
            add_future
                .add_future_may_await(
                    async move {
                        sleep(Duration::from_secs(2)).await;
                        store.insert(id, name);
                        Ok(())
                    }
                    .boxed(),
                )
                .await?;
        }
        Ok(())
    }

    async fn barrier(&mut self, is_checkpoint: bool) -> risingwave_connector::sink::Result<()> {
        if is_checkpoint {
            self.store.inc_checkpoint();
        }
        Ok(())
    }
}

impl Drop for DelayedAsyncTruncateTestWriter {
    fn drop(&mut self) {
        self.parallelism_counter.fetch_sub(1, Relaxed);
    }
}

pub fn simple_name_of_id(id: i32) -> String {
    format!("name-{}", id)
}

pub struct SimulationTestSink {
    _sink_guard: TestSinkRegistryGuard,
    pub store: TestSinkStore,
    pub parallelism_counter: Arc<AtomicUsize>,
    pub err_rate: Arc<AtomicU32>,
    barrier_delay_ms: Option<(Arc<AtomicU64>, u64)>,
}

pub enum TestSinkType {
    SinkWriter,
    SlowBarrier(Duration),
    SinglePhaseCoordinatedSink,
    TwoPhaseCoordinatedSink,
    AsyncTruncate,
    DelayedAsyncTruncate,
    RetainLog,
}

#[macro_export]
macro_rules! for_all_sink_types {
    ($macro:path) => {
        $macro! {
            SinkWriter,
            SinglePhaseCoordinatedSink,
            TwoPhaseCoordinatedSink,
            AsyncTruncate,
        }
    };
}

impl SimulationTestSink {
    pub fn register_new(test_type: TestSinkType) -> Self {
        let parallelism_counter = Arc::new(AtomicUsize::new(0));
        let err_rate = Arc::new(AtomicU32::new(0));
        let store = TestSinkStore::new();
        let barrier_delay_ms = match &test_type {
            TestSinkType::SlowBarrier(delay) => Some((
                Arc::new(AtomicU64::new(0)),
                delay.as_millis().try_into().unwrap(),
            )),
            _ => None,
        };

        let _sink_guard = match test_type {
            TestSinkType::SinglePhaseCoordinatedSink => {
                let staging_store = StagingDataStore::default();
                register_build_coordinated_sink(
                    {
                        let parallelism_counter = parallelism_counter.clone();
                        let err_rate = err_rate.clone();
                        let store = store.clone();
                        let staging_store = staging_store.clone();
                        use risingwave_connector::sink::SinkWriterMetrics;
                        use risingwave_connector::sink::writer::SinkWriterExt;
                        move |param, writer_param| {
                            parallelism_counter.fetch_add(1, Relaxed);
                            let metrics = SinkWriterMetrics::new(&writer_param);
                            let writer = CoordinatedTestWriter {
                                store: store.clone(),
                                parallelism_counter: parallelism_counter.clone(),
                                err_rate: err_rate.clone(),
                                staging_store: staging_store.clone(),
                                staging: Default::default(),
                            };
                            async move {
                                let log_sinker =
                                    risingwave_connector::sink::boxed::boxed_log_sinker(
                                        CoordinatedLogSinker::new(
                                            &writer_param,
                                            param,
                                            writer,
                                            NonZero::new(1).unwrap(),
                                        )
                                        .await?,
                                    );
                                Ok(log_sinker)
                            }
                            .boxed()
                        }
                    },
                    {
                        let err_rate = err_rate.clone();
                        let store = store.clone();
                        let staging_store = staging_store.clone();
                        move |_, _| {
                            let coordinator = TestCoordinator {
                                err_rate: err_rate.clone(),
                                store: store.clone(),
                                staging_store: staging_store.clone(),
                            };
                            SinkCommitCoordinator::SinglePhase(Box::new(coordinator))
                        }
                    },
                )
            }
            TestSinkType::TwoPhaseCoordinatedSink => {
                let staging_store = StagingDataStore::default();
                register_build_coordinated_sink(
                    {
                        let parallelism_counter = parallelism_counter.clone();
                        let err_rate = err_rate.clone();
                        let store = store.clone();
                        let staging_store = staging_store.clone();
                        use risingwave_connector::sink::SinkWriterMetrics;
                        use risingwave_connector::sink::writer::SinkWriterExt;
                        move |param, writer_param| {
                            parallelism_counter.fetch_add(1, Relaxed);
                            let metrics = SinkWriterMetrics::new(&writer_param);
                            let writer = CoordinatedTestWriter {
                                store: store.clone(),
                                parallelism_counter: parallelism_counter.clone(),
                                err_rate: err_rate.clone(),
                                staging_store: staging_store.clone(),
                                staging: Default::default(),
                            };
                            async move {
                                let log_sinker =
                                    risingwave_connector::sink::boxed::boxed_log_sinker(
                                        CoordinatedLogSinker::new(
                                            &writer_param,
                                            param,
                                            writer,
                                            NonZero::new(1).unwrap(),
                                        )
                                        .await?,
                                    );
                                Ok(log_sinker)
                            }
                            .boxed()
                        }
                    },
                    {
                        let err_rate = err_rate.clone();
                        let store = store.clone();
                        let staging_store = staging_store.clone();
                        move |_, _| {
                            let coordinator = TestCoordinator {
                                err_rate: err_rate.clone(),
                                store: store.clone(),
                                staging_store: staging_store.clone(),
                            };
                            SinkCommitCoordinator::TwoPhase(Box::new(coordinator))
                        }
                    },
                )
            }
            TestSinkType::SinkWriter => register_build_sink({
                let parallelism_counter = parallelism_counter.clone();
                let err_rate = err_rate.clone();
                let store = store.clone();
                use risingwave_connector::sink::SinkWriterMetrics;
                use risingwave_connector::sink::writer::SinkWriterExt;
                move |_, writer_param| {
                    parallelism_counter.fetch_add(1, Relaxed);
                    let metrics = SinkWriterMetrics::new(&writer_param);
                    let log_sinker = risingwave_connector::sink::boxed::boxed_log_sinker(
                        TestWriter {
                            store: store.clone(),
                            parallelism_counter: parallelism_counter.clone(),
                            err_rate: err_rate.clone(),
                            barrier_delay_ms: None,
                        }
                        .into_log_sinker(metrics),
                    );
                    async move { Ok(log_sinker) }.boxed()
                }
            }),
            TestSinkType::SlowBarrier(_) => register_build_sink({
                let parallelism_counter = parallelism_counter.clone();
                let err_rate = err_rate.clone();
                let store = store.clone();
                let barrier_delay_ms = barrier_delay_ms.as_ref().unwrap().0.clone();
                use risingwave_connector::sink::SinkWriterMetrics;
                use risingwave_connector::sink::writer::SinkWriterExt;
                move |_, writer_param| {
                    parallelism_counter.fetch_add(1, Relaxed);
                    let metrics = SinkWriterMetrics::new(&writer_param);
                    let log_sinker = risingwave_connector::sink::boxed::boxed_log_sinker(
                        TestWriter {
                            store: store.clone(),
                            parallelism_counter: parallelism_counter.clone(),
                            err_rate: err_rate.clone(),
                            barrier_delay_ms: Some(barrier_delay_ms.clone()),
                        }
                        .into_log_sinker(metrics),
                    );
                    async move { Ok(log_sinker) }.boxed()
                }
            }),
            TestSinkType::AsyncTruncate => register_build_sink({
                let parallelism_counter = parallelism_counter.clone();
                let err_rate = err_rate.clone();
                let store = store.clone();
                use risingwave_connector::sink::SinkWriterMetrics;
                use risingwave_connector::sink::writer::SinkWriterExt;
                move |_, writer_param| {
                    parallelism_counter.fetch_add(1, Relaxed);
                    let metrics = SinkWriterMetrics::new(&writer_param);
                    let log_sinker = risingwave_connector::sink::boxed::boxed_log_sinker(
                        AsyncTruncateTestWriter {
                            store: store.clone(),
                            parallelism_counter: parallelism_counter.clone(),
                            err_rate: err_rate.clone(),
                        }
                        .into_log_sinker(10),
                    );
                    async move { Ok(log_sinker) }.boxed()
                }
            }),
            TestSinkType::DelayedAsyncTruncate => register_build_sink({
                let parallelism_counter = parallelism_counter.clone();
                let store = store.clone();
                move |_, _| {
                    parallelism_counter.fetch_add(1, Relaxed);
                    let log_sinker = risingwave_connector::sink::boxed::boxed_log_sinker(
                        DelayedAsyncTruncateTestWriter {
                            store: store.clone(),
                            parallelism_counter: parallelism_counter.clone(),
                        }
                        .into_log_sinker(10),
                    );
                    async move { Ok(log_sinker) }.boxed()
                }
            }),
            TestSinkType::RetainLog => register_build_sink({
                let parallelism_counter = parallelism_counter.clone();
                let store = store.clone();
                move |_, _| {
                    parallelism_counter.fetch_add(1, Relaxed);
                    let store = store.clone();
                    let parallelism_counter = parallelism_counter.clone();
                    let log_sinker: BoxLogSinker =
                        Box::new(move |mut log_reader: &mut dyn DynLogReader| {
                            async move {
                                struct ParallelismGuard(Arc<AtomicUsize>);

                                impl Drop for ParallelismGuard {
                                    fn drop(&mut self) {
                                        self.0.fetch_sub(1, Relaxed);
                                    }
                                }

                                let _parallelism_guard = ParallelismGuard(parallelism_counter);

                                log_reader.start_from(None).await?;
                                loop {
                                    let (epoch, item) = log_reader.next_item().await?;
                                    match item {
                                        LogStoreReadItem::StreamChunk { chunk, .. } => {
                                            for (op, row) in chunk.rows() {
                                                assert_eq!(op, Op::Insert);
                                                assert!(row.len() >= 2);
                                                let id = row.datum_at(0).unwrap().into_int32();
                                                let name = row
                                                    .datum_at(1)
                                                    .unwrap()
                                                    .into_utf8()
                                                    .to_string();
                                                store.insert(id, name);
                                            }
                                        }
                                        LogStoreReadItem::Barrier {
                                            is_checkpoint,
                                            schema_change,
                                            ..
                                        } => {
                                            if is_checkpoint {
                                                store.inc_checkpoint();
                                            }
                                            if schema_change.is_some() {
                                                log_reader
                                                    .truncate(TruncateOffset::Barrier { epoch })?;
                                            }
                                        }
                                    }
                                }
                            }
                            .boxed()
                        });
                    async move { Ok(log_sinker) }.boxed()
                }
            }),
        };

        Self {
            _sink_guard,
            parallelism_counter,
            store,
            err_rate,
            barrier_delay_ms,
        }
    }

    pub fn enable_barrier_delay(&self) {
        let (barrier_delay_ms, delay) = self.barrier_delay_ms.as_ref().unwrap();
        barrier_delay_ms.store(*delay, Relaxed);
    }

    pub fn set_err_rate(&self, err_rate: f64) {
        let err_rate = u32::MAX as f64 * err_rate;
        self.err_rate.store(err_rate as _, Relaxed);
    }

    fn random_err(err_rate: &AtomicU32) -> bool {
        thread_rng().random_ratio(err_rate.load(Relaxed), u32::MAX)
    }

    pub async fn wait_initial_parallelism(&self, parallelism: usize) -> Result<()> {
        while self.parallelism_counter.load(Relaxed) < parallelism {
            yield_now().await;
        }
        assert_eq!(self.parallelism_counter.load(Relaxed), parallelism);
        Ok(())
    }
}

pub fn build_stream_chunk(
    row_iter: impl Iterator<Item = (i32, String, String, String)>,
) -> StreamChunk {
    let mut builder = DataChunkBuilder::new(
        vec![
            DataType::Int32,
            DataType::Varchar,
            DataType::Serial,
            DataType::Varchar,
            DataType::Varchar,
        ],
        100000,
    );
    for (id, name, split_id, offset) in row_iter {
        std::assert!(
            builder
                .append_one_row([
                    Some(ScalarImpl::Int32(id)),
                    Some(ScalarImpl::Utf8(name.into())),
                    // Row id columns are filled with `NULL` here and will be filled with the real
                    // row id generated by `RowIdGenExecutor` later.
                    None,
                    Some(ScalarImpl::Utf8(split_id.into())),
                    Some(ScalarImpl::Utf8(offset.into())),
                ])
                .is_none()
        );
    }
    let chunk = builder.consume_all().unwrap();
    let ops = (0..chunk.cardinality()).map(|_| Op::Insert).collect_vec();
    StreamChunk::from_parts(ops, chunk)
}

pub struct SimulationTestSource {
    _source_guard: TestSourceRegistryGuard,
    pub id_list: Vec<i32>,
    pub create_stream_count: Arc<AtomicUsize>,
}

impl SimulationTestSource {
    pub fn register_new(
        source_parallelism: usize,
        id_list: impl Iterator<Item = i32>,
        sample_rate: f32,
        pause_interval: usize,
    ) -> Self {
        let mut id_list: Vec<i32> = id_list.collect_vec();
        let count = (id_list.len() as f32 * sample_rate) as usize;
        id_list.shuffle(&mut rand::rng());
        let id_list = id_list[0..count].iter().cloned().collect_vec();
        let mut id_lists = vec![vec![]; source_parallelism];
        for id in &id_list {
            id_lists[*id as usize % source_parallelism].push(*id);
        }
        let create_stream_count = Arc::new(AtomicUsize::new(0));
        let id_lists_clone = id_lists.iter().map(|l| Arc::new(l.clone())).collect_vec();
        let box_source_create_stream_count = create_stream_count.clone();
        let _source_guard = register_test_source(BoxSource::new(
            move |_, _| {
                Ok((0..source_parallelism)
                    .map(|i: usize| TestSourceSplit {
                        id: format!("{}", i).as_str().into(),
                        properties: Default::default(),
                        offset: "".to_string(),
                    })
                    .collect_vec())
            },
            move |_, splits, _, _, _| {
                box_source_create_stream_count.fetch_add(1, Relaxed);
                select_all(splits.into_iter().map(|split| {
                    let split_id: usize = split.id.parse().unwrap();
                    let id_list = id_lists_clone[split_id].clone();
                    let mut offset = if split.offset == "" {
                        0
                    } else {
                        split.offset.parse::<usize>().unwrap() + 1
                    };

                    let mut stream: BoxStream<'static, StreamChunk> = empty().boxed();

                    while offset < id_list.len() {
                        let mut chunks = Vec::new();
                        while offset < id_list.len() && chunks.len() < pause_interval {
                            let id = id_list[offset];
                            let chunk = build_stream_chunk(once((
                                id,
                                simple_name_of_id(id),
                                split.id.to_string(),
                                offset.to_string(),
                            )));
                            chunks.push(chunk);

                            offset += 1;
                        }

                        stream = stream
                            .chain(
                                async move { stream::iter(chunks) }
                                    .into_stream()
                                    .chain(
                                        async move {
                                            sleep(Duration::from_millis(100)).await;
                                            stream::iter(Vec::new())
                                        }
                                        .into_stream(),
                                    )
                                    .flatten(),
                            )
                            .boxed();
                    }

                    stream
                        .chain(async { pending::<StreamChunk>().await }.into_stream())
                        .map(|chunk| Ok(chunk))
                        .boxed()
                }))
                .boxed()
            },
        ));

        Self {
            _source_guard,
            id_list,
            create_stream_count,
        }
    }
}

pub async fn start_sink_test_cluster() -> Result<Cluster> {
    let config_path = {
        let mut file = tempfile::NamedTempFile::new().expect("failed to create temp config file");
        file.write_all(include_bytes!("../../../../../config/ci-sim.toml"))
            .expect("failed to write config file");
        file.into_temp_path()
    };

    Cluster::start(Configuration {
        config_path: ConfigPath::Temp(config_path.into()),
        frontend_nodes: 1,
        compute_nodes: 3,
        meta_nodes: 1,
        compactor_nodes: 1,
        compute_node_cores: 2,
        ..Default::default()
    })
    .await
}
