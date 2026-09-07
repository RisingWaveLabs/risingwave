// Copyright 2025 RisingWave Labs
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
use std::marker::PhantomData;
use std::ops::Bound;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use either::Either;
use futures::TryStreamExt;
use futures::stream::{self, StreamExt};
use futures_async_stream::try_stream;
use pin_project::pin_project;
use risingwave_common::catalog::ColumnId;
use risingwave_common::hash::VnodeBitmapExt;
use risingwave_common::id::SourceId;
use risingwave_common::metrics::GLOBAL_ERROR_METRICS;
use risingwave_common::types::ScalarRef;
use risingwave_connector::source::filesystem::OpendalFsSplit;
use risingwave_connector::source::filesystem::opendal_source::{
    OpendalAzblob, OpendalGcs, OpendalPosixFs, OpendalS3, OpendalSource,
};
use risingwave_connector::source::reader::desc::SourceDesc;
use risingwave_connector::source::{
    BoxStreamingFileSourceChunkStream, SourceContext, SourceCtrlOpts, SplitImpl, SplitMetaData,
};
use risingwave_pb::common::ThrottleType;
use risingwave_storage::store::PrefetchOptions;
use thiserror_ext::AsReport;

use super::{
    SourceStateTableHandler, StreamSourceCore,
    apply_rate_limit_with_for_streaming_file_source_reader, get_split_offset_col_idx,
    get_split_offset_mapping_from_chunk, prune_additional_cols,
    source_reader_event_to_chunk_stream,
};
use crate::common::rate_limit::limited_chunk_size;
use crate::executor::prelude::*;
use crate::executor::stream_reader::StreamReaderWithPause;

const SPLIT_BATCH_SIZE: usize = 1000;
const MAX_RETRIES_PER_SPLIT: u32 = 3;
const RETRY_BASE_BACKOFF: Duration = Duration::from_millis(200);

type SplitBatch = Option<Vec<SplitImpl>>;

struct ReplaceReaderArgs<'a, S: StateStore, const BIASED: bool> {
    splits_on_fetch: &'a mut usize,
    state_store_handler: &'a SourceStateTableHandler<S>,
    dirty_splits: &'a HashSet<Arc<str>>,
    column_ids: Vec<ColumnId>,
    source_ctx: SourceContext,
    source_desc: &'a SourceDesc,
    stream: &'a mut StreamReaderWithPause<BIASED, Option<StreamChunk>>,
    rate_limit_rps: Option<u32>,
    reading_file: Arc<Mutex<Option<Arc<str>>>>,
}

/// A stream wrapper that sets `reading_file` before polling the underlying stream for the first time.
///
/// This helps attribute errors that happen before the first chunk is produced (e.g., invalid UTF-8)
/// to the corresponding split.
#[pin_project]
struct SetReadingFileOnPoll<S> {
    #[pin]
    inner: S,
    reading_file: Arc<Mutex<Option<Arc<str>>>>,
    split_id: Arc<str>,
    is_set: bool,
}

impl<S> SetReadingFileOnPoll<S> {
    fn new(inner: S, reading_file: Arc<Mutex<Option<Arc<str>>>>, split_id: Arc<str>) -> Self {
        Self {
            inner,
            reading_file,
            split_id,
            is_set: false,
        }
    }
}

impl<S> futures::Stream for SetReadingFileOnPoll<S>
where
    S: futures::Stream,
{
    type Item = S::Item;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.project();
        if !*this.is_set {
            *this.reading_file.lock().expect("mutex poisoned") = Some(this.split_id.clone());
            *this.is_set = true;
        }
        this.inner.poll_next(cx)
    }
}

pub struct FsFetchExecutor<S: StateStore, Src: OpendalSource> {
    actor_ctx: ActorContextRef,

    /// Streaming source for external
    stream_source_core: Option<StreamSourceCore<S>>,

    /// Upstream list executor.
    upstream: Option<Executor>,

    /// Rate limit in rows/s.
    rate_limit_rps: Option<u32>,

    _marker: PhantomData<Src>,
}

impl<S: StateStore, Src: OpendalSource> FsFetchExecutor<S, Src> {
    pub fn new(
        actor_ctx: ActorContextRef,
        stream_source_core: StreamSourceCore<S>,
        upstream: Executor,
        rate_limit_rps: Option<u32>,
    ) -> Self {
        Self {
            actor_ctx,
            stream_source_core: Some(stream_source_core),
            upstream: Some(upstream),
            rate_limit_rps,
            _marker: PhantomData,
        }
    }

    async fn replace_with_new_batch_reader<const BIASED: bool>(
        args: ReplaceReaderArgs<'_, S, BIASED>,
    ) -> StreamExecutorResult<()> {
        let ReplaceReaderArgs {
            splits_on_fetch,
            state_store_handler,
            dirty_splits,
            column_ids,
            source_ctx,
            source_desc,
            stream,
            rate_limit_rps,
            reading_file,
        } = args;
        let mut batch = Vec::with_capacity(SPLIT_BATCH_SIZE);
        let state_table = state_store_handler.state_table();
        'vnodes: for vnode in state_table.vnodes().iter_vnodes() {
            let table_iter = state_table
                .iter_with_vnode(
                    vnode,
                    &(Bound::<OwnedRow>::Unbounded, Bound::<OwnedRow>::Unbounded),
                    // This usage is similar with `backfill`. So we only need to fetch a large data rather than establish a connection for a whole object.
                    PrefetchOptions::prefetch_for_small_range_scan(),
                )
                .await?;
            pin_mut!(table_iter);
            while let Some(item) = table_iter.next().await {
                let row = item?;
                let split = match row.datum_at(1) {
                    Some(ScalarRefImpl::Jsonb(jsonb_ref)) => match &source_desc.source.config {
                        risingwave_connector::source::ConnectorProperties::Gcs(_) => {
                            let split: OpendalFsSplit<OpendalGcs> =
                                OpendalFsSplit::restore_from_json(jsonb_ref.to_owned_scalar())?;
                            SplitImpl::from(split)
                        }
                        risingwave_connector::source::ConnectorProperties::OpendalS3(_) => {
                            let split: OpendalFsSplit<OpendalS3> =
                                OpendalFsSplit::restore_from_json(jsonb_ref.to_owned_scalar())?;
                            SplitImpl::from(split)
                        }
                        risingwave_connector::source::ConnectorProperties::Azblob(_) => {
                            let split: OpendalFsSplit<OpendalAzblob> =
                                OpendalFsSplit::restore_from_json(jsonb_ref.to_owned_scalar())?;
                            SplitImpl::from(split)
                        }
                        risingwave_connector::source::ConnectorProperties::PosixFs(_) => {
                            let split: OpendalFsSplit<OpendalPosixFs> =
                                OpendalFsSplit::restore_from_json(jsonb_ref.to_owned_scalar())?;
                            SplitImpl::from(split)
                        }
                        _ => unreachable!(),
                    },
                    _ => unreachable!(),
                };
                let split_id = split.id();
                if dirty_splits.contains(&split_id) {
                    continue;
                }
                batch.push(split);

                if batch.len() >= SPLIT_BATCH_SIZE {
                    break 'vnodes;
                }
            }
        }
        if batch.is_empty() {
            stream.replace_data_stream(stream::pending().boxed());
        } else {
            *splits_on_fetch += batch.len();

            let mut merged_stream =
                stream::empty::<StreamExecutorResult<Option<StreamChunk>>>().boxed();
            // Change the previous implementation where multiple files shared a single SourceReader
            // to a new approach where each SourceReader reads only one file.
            // Then, merge the streams of multiple files serially here.
            for split in batch {
                let split_id = split.id();
                let single_file_stream = Self::build_single_file_stream_reader(
                    column_ids.clone(),
                    source_ctx.clone(),
                    source_desc,
                    Some(vec![split]),
                    rate_limit_rps,
                )
                .await?
                .map_err(StreamExecutorError::connector_error);
                let single_file_stream =
                    SetReadingFileOnPoll::new(single_file_stream, reading_file.clone(), split_id)
                        .boxed();
                merged_stream = merged_stream.chain(single_file_stream).boxed();
            }

            stream.replace_data_stream(merged_stream);
        }

        Ok(())
    }

    // Note: This change applies only to the file source.
    //
    // Each SourceReader (for the streaming file source, this is the `OpendalReader` struct)
    // reads only one file. After the chunk stream returned by the SourceReader,
    // chain a None to indicate that the file has been fully read.
    async fn build_single_file_stream_reader(
        column_ids: Vec<ColumnId>,
        source_ctx: SourceContext,
        source_desc: &SourceDesc,
        batch: SplitBatch,
        rate_limit_rps: Option<u32>,
    ) -> StreamExecutorResult<BoxStreamingFileSourceChunkStream> {
        let (stream, _) = source_desc
            .source
            .build_stream(batch, column_ids, Arc::new(source_ctx), false)
            .await
            .map_err(StreamExecutorError::connector_error)?;
        let optional_stream: BoxStreamingFileSourceChunkStream =
            source_reader_event_to_chunk_stream(stream)
                .boxed()
                .map(|item| item.map(Some))
                .chain(stream::once(async { Ok(None) }))
                .boxed();
        Ok(
            apply_rate_limit_with_for_streaming_file_source_reader(optional_stream, rate_limit_rps)
                .boxed(),
        )
    }

    fn build_source_ctx(
        &self,
        source_desc: &SourceDesc,
        source_id: SourceId,
        source_name: &str,
    ) -> SourceContext {
        SourceContext::new(
            self.actor_ctx.id,
            source_id,
            self.actor_ctx.fragment_id,
            source_name.to_owned(),
            source_desc.metrics.clone(),
            SourceCtrlOpts {
                chunk_size: limited_chunk_size(self.rate_limit_rps),
                split_txn: self.rate_limit_rps.is_some(), // when rate limiting, we may split txn
            },
            source_desc.source.config.clone(),
            None,
        )
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn into_stream(mut self) {
        let mut upstream = self.upstream.take().unwrap().execute();
        let barrier = expect_first_barrier(&mut upstream).await?;
        let first_epoch = barrier.epoch;
        let is_pause_on_startup = barrier.is_pause_on_startup();
        yield Message::Barrier(barrier);

        let mut core = self.stream_source_core.take().unwrap();
        let mut state_store_handler = core.split_state_store;

        // Build source description from the builder.
        let source_desc_builder = core.source_desc_builder.take().unwrap();

        let source_desc = source_desc_builder
            .build()
            .map_err(StreamExecutorError::connector_error)?;
        let actor_id = self.actor_ctx.id.to_string();
        let fragment_id = self.actor_ctx.fragment_id.to_string();
        let source_id = core.source_id.to_string();
        let source_name = core.source_name.clone();
        let dirty_split_count_metrics = source_desc
            .metrics
            .file_source_dirty_split_count
            .with_guarded_label_values(&[&actor_id, &source_id, &source_name, &fragment_id]);
        let failed_split_count_metrics = source_desc
            .metrics
            .file_source_failed_split_count
            .with_guarded_label_values(&[&actor_id, &source_id, &source_name, &fragment_id]);
        dirty_split_count_metrics.set(0);

        // pulsar's `message_id_data_idx` is not used in this executor, so we don't need to get it.
        let (Some(split_idx), Some(offset_idx), _) = get_split_offset_col_idx(&source_desc.columns)
        else {
            unreachable!("Partition and offset columns must be set.");
        };
        // Initialize state table.
        state_store_handler.init_epoch(first_epoch).await?;

        let reading_file: Arc<Mutex<Option<Arc<str>>>> = Arc::new(Mutex::new(None));
        let mut retry_counts: HashMap<Arc<str>, u32> = HashMap::new();
        let mut dirty_splits: HashSet<Arc<str>> = HashSet::new();

        let mut splits_on_fetch: usize = 0;
        let mut stream = StreamReaderWithPause::<true, Option<StreamChunk>>::new(
            upstream,
            stream::pending().boxed(),
        );
        if is_pause_on_startup {
            stream.pause_stream();
        }

        // If it is a recovery startup,
        // there can be file assignments in the state table.
        // Hence we try building a reader first.
        Self::replace_with_new_batch_reader(ReplaceReaderArgs {
            splits_on_fetch: &mut splits_on_fetch,
            state_store_handler: &state_store_handler,
            dirty_splits: &dirty_splits,
            column_ids: core.column_ids.clone(),
            source_ctx: self.build_source_ctx(&source_desc, core.source_id, &core.source_name),
            source_desc: &source_desc,
            stream: &mut stream,
            rate_limit_rps: self.rate_limit_rps,
            reading_file: reading_file.clone(),
        })
        .await?;

        while let Some(msg) = stream.next().await {
            match msg {
                Err(e) => {
                    let cur_file = reading_file.lock().expect("mutex poisoned").clone();
                    let Some(split_id) = cur_file else {
                        tracing::error!(
                            source_id = %core.source_id,
                            source_name = %core.source_name,
                            fragment_id = %self.actor_ctx.fragment_id,
                            error = %e.as_report(),
                            "Fetch Error but failed to infer reading file; aborting actor"
                        );
                        return Err(e);
                    };

                    let retries_done = retry_counts.entry(split_id.clone()).or_insert(0);
                    if *retries_done < MAX_RETRIES_PER_SPLIT {
                        *retries_done = retries_done.saturating_add(1);
                        let backoff = RETRY_BASE_BACKOFF
                            .checked_mul(1u32 << (*retries_done - 1))
                            .unwrap_or(Duration::from_secs(60));
                        tracing::warn!(
                            source_id = %core.source_id,
                            source_name = %core.source_name,
                            fragment_id = %self.actor_ctx.fragment_id,
                            reading_file = %split_id,
                            retries_done = *retries_done,
                            max_retries = MAX_RETRIES_PER_SPLIT,
                            error = %e.as_report(),
                            "Fetch Error, retrying file split"
                        );
                        tokio::time::sleep(backoff).await;
                    } else {
                        // Exceeded max retries: mark dirty in memory and skip this split afterwards.
                        dirty_splits.insert(split_id.clone());
                        dirty_split_count_metrics.set(dirty_splits.len() as i64);
                        failed_split_count_metrics.inc();
                        retry_counts.remove(&split_id);
                        tracing::error!(
                            source_id = %core.source_id,
                            source_name = %core.source_name,
                            fragment_id = %self.actor_ctx.fragment_id,
                            reading_file = %split_id,
                            max_retries = MAX_RETRIES_PER_SPLIT,
                            error = %e.as_report(),
                            "Fetch Error, exceeded max retries; marking split dirty and skipping"
                        );
                        GLOBAL_ERROR_METRICS.user_source_error.report([
                            "File source dirty split".to_owned(),
                            core.source_id.to_string(),
                            core.source_name.clone(),
                            self.actor_ctx.fragment_id.to_string(),
                        ]);
                    }

                    // Clear current reading file and rebuild reader to continue with other splits.
                    *reading_file.lock().expect("mutex poisoned") = None;
                    splits_on_fetch = 0;
                    Self::replace_with_new_batch_reader(ReplaceReaderArgs {
                        splits_on_fetch: &mut splits_on_fetch,
                        state_store_handler: &state_store_handler,
                        dirty_splits: &dirty_splits,
                        column_ids: core.column_ids.clone(),
                        source_ctx: self.build_source_ctx(
                            &source_desc,
                            core.source_id,
                            &core.source_name,
                        ),
                        source_desc: &source_desc,
                        stream: &mut stream,
                        rate_limit_rps: self.rate_limit_rps,
                        reading_file: reading_file.clone(),
                    })
                    .await?;
                    continue;
                }
                Ok(msg) => {
                    match msg {
                        // This branch will be preferred.
                        Either::Left(msg) => {
                            match msg {
                                Message::Barrier(barrier) => {
                                    if let Some(mutation) = barrier.mutation.as_deref() {
                                        match mutation {
                                            Mutation::Pause => stream.pause_stream(),
                                            Mutation::Resume => stream.resume_stream(),
                                            Mutation::Throttle(fragment_to_apply) => {
                                                if let Some(entry) = fragment_to_apply
                                                    .get(&self.actor_ctx.fragment_id)
                                                    && entry.throttle_type() == ThrottleType::Source
                                                    && entry.rate_limit != self.rate_limit_rps
                                                {
                                                    tracing::info!(
                                                        "updating rate limit from {:?} to {:?}",
                                                        self.rate_limit_rps,
                                                        entry.rate_limit
                                                    );
                                                    self.rate_limit_rps = entry.rate_limit;
                                                    splits_on_fetch = 0;
                                                    *reading_file.lock().expect("mutex poisoned") =
                                                        None;
                                                }
                                            }
                                            _ => (),
                                        }
                                    }

                                    let post_commit = state_store_handler
                                        .commit_may_update_vnode_bitmap(barrier.epoch)
                                        .await?;

                                    let update_vnode_bitmap =
                                        barrier.as_update_vnode_bitmap(self.actor_ctx.id);
                                    // Propagate the barrier.
                                    yield Message::Barrier(barrier);

                                    if post_commit
                                        .post_yield_barrier(update_vnode_bitmap)
                                        .await?
                                        .is_some()
                                    {
                                        // Vnode bitmap update changes which file assignments this executor
                                        // should read. Rebuild the reader to avoid reading splits that no
                                        // longer belong to this actor (e.g., during scale-out).
                                        splits_on_fetch = 0;
                                        *reading_file.lock().expect("mutex poisoned") = None;
                                    }

                                    if splits_on_fetch == 0 {
                                        Self::replace_with_new_batch_reader(ReplaceReaderArgs {
                                            splits_on_fetch: &mut splits_on_fetch,
                                            state_store_handler: &state_store_handler,
                                            dirty_splits: &dirty_splits,
                                            column_ids: core.column_ids.clone(),
                                            source_ctx: self.build_source_ctx(
                                                &source_desc,
                                                core.source_id,
                                                &core.source_name,
                                            ),
                                            source_desc: &source_desc,
                                            stream: &mut stream,
                                            rate_limit_rps: self.rate_limit_rps,
                                            reading_file: reading_file.clone(),
                                        })
                                        .await?;
                                    }
                                }
                                // Receiving file assignments from upstream list executor,
                                // store into state table.
                                Message::Chunk(chunk) => {
                                    // For Parquet encoding, the offset indicates the current row being read.
                                    let file_assignment: Vec<OpendalFsSplit<Src>> = chunk
                                        .data_chunk()
                                        .rows()
                                        .filter_map(|row| {
                                            let filename = row.datum_at(0).unwrap().into_utf8();
                                            let size = row.datum_at(2).unwrap().into_int64();

                                            if size > 0 {
                                                Some(OpendalFsSplit::<Src>::new(
                                                    filename.to_owned(),
                                                    0,
                                                    size as usize,
                                                ))
                                            } else {
                                                None
                                            }
                                        })
                                        .collect();

                                    state_store_handler.set_states(file_assignment).await?;
                                    state_store_handler.try_flush().await?;
                                }
                                Message::Watermark(_) => unreachable!(),
                            }
                        }
                        // StreamChunk from FsSourceReader, and the reader reads only one file.
                        // Motivation for the changes:
                        //
                        // Previously, the fetch executor determined whether a file was fully read by checking if the
                        // offset reached the size of the file. However, this approach had some issues related to
                        // maintaining the offset:
                        //
                        // 1. For files compressed with gzip, the size reported corresponds to the original uncompressed
                        //    size, not the actual size of the compressed file.
                        //
                        // 2. For Parquet files, the offset represents the number of rows. Therefore, when listing each
                        //    file, it was necessary to read the metadata to obtain the total number of rows in the file,
                        //    which is an expensive operation.
                        //
                        // To address these issues, we changes the approach to determining whether a file is fully
                        // read by moving the check outside the reader. The fetch executor's right stream has been
                        // changed from a chunk stream to an `Option<Chunk>` stream. When a file is completely read,
                        // a `None` value is added at the end of the file stream to signal to the fetch executor that the
                        // file has been fully read. Upon encountering `None`, the file is deleted.
                        Either::Right(optional_chunk) => match optional_chunk {
                            Some(chunk) => {
                                let mapping = get_split_offset_mapping_from_chunk(
                                    &chunk, split_idx, offset_idx,
                                )
                                .unwrap();
                                debug_assert_eq!(mapping.len(), 1);
                                if let Some((split_id, offset)) = mapping.into_iter().next() {
                                    *reading_file.lock().expect("mutex poisoned") =
                                        Some(split_id.clone());
                                    retry_counts.remove(&split_id);
                                    let row = state_store_handler.get(&split_id).await?
                                        .unwrap_or_else(|| {
                                            panic!("The fs_split (file_name) {:?} should be in the state table.",
                                        split_id)
                                        });
                                    let mut fs_split = match row.datum_at(1) {
                                        Some(ScalarRefImpl::Jsonb(jsonb_ref)) => {
                                            OpendalFsSplit::<Src>::restore_from_json(
                                                jsonb_ref.to_owned_scalar(),
                                            )?
                                        }
                                        _ => unreachable!(),
                                    };
                                    fs_split.update_offset(offset)?;

                                    state_store_handler
                                        .set(&split_id, fs_split.encode_to_json())
                                        .await?;
                                }
                                let chunk = prune_additional_cols(
                                    &chunk,
                                    &[split_idx, offset_idx],
                                    &source_desc.columns,
                                );
                                yield Message::Chunk(chunk);
                            }
                            None => {
                                let cur_file = reading_file.lock().expect("mutex poisoned").clone();
                                tracing::debug!("Deleting file: {:?}", cur_file);
                                if let Some(ref delete_file_name) = cur_file {
                                    splits_on_fetch -= 1;
                                    state_store_handler.delete(delete_file_name).await?;
                                    // Clean up in-memory retry count.
                                    retry_counts.remove(delete_file_name);
                                }
                            }
                        },
                    }
                }
            }
        }
    }
}

impl<S: StateStore, Src: OpendalSource> Execute for FsFetchExecutor<S, Src> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.into_stream().boxed()
    }
}

impl<S: StateStore, Src: OpendalSource> Debug for FsFetchExecutor<S, Src> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(core) = &self.stream_source_core {
            f.debug_struct("FsFetchExecutor")
                .field("source_id", &core.source_id)
                .field("column_ids", &core.column_ids)
                .finish()
        } else {
            f.debug_struct("FsFetchExecutor").finish()
        }
    }
}
