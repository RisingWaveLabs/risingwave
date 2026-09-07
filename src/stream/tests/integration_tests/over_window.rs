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

use std::sync::atomic::Ordering;

use risingwave_common::config::streaming::OverWindowCachePolicy;
use risingwave_common::util::epoch::test_epoch;
use risingwave_expr::aggregate::{AggArgs, PbAggKind};
use risingwave_expr::window_function::{
    Frame, FrameBound, FrameExclusion, WindowFuncCall, WindowFuncKind,
};
use risingwave_stream::common::table::test_utils::gen_pbtable;
use risingwave_stream::executor::monitor::StreamingMetrics;
use risingwave_stream::executor::{OverWindowExecutor, OverWindowExecutorArgs};

use crate::prelude::*;

async fn create_executor<S: StateStore>(
    calls: Vec<WindowFuncCall>,
    store: S,
) -> (MessageSender, BoxedMessageStream) {
    create_executor_with_watermark(
        calls,
        store,
        Arc::new(AtomicU64::new(0)),
        Arc::new(StreamingMetrics::unused()),
    )
    .await
}

async fn create_executor_with_watermark<S: StateStore>(
    calls: Vec<WindowFuncCall>,
    store: S,
    watermark_epoch: Arc<AtomicU64>,
    metrics: Arc<StreamingMetrics>,
) -> (MessageSender, BoxedMessageStream) {
    let input_schema = Schema::new(vec![
        Field::unnamed(DataType::Int64),   // order key
        Field::unnamed(DataType::Varchar), // partition key
        Field::unnamed(DataType::Int64),   // pk
        Field::unnamed(DataType::Int32),   // x
    ]);
    let input_stream_key = vec![2];
    let partition_key_indices = vec![1];
    let order_key_indices = vec![0];
    let order_key_order_types = vec![OrderType::ascending()];

    let mut table_columns = vec![
        ColumnDesc::unnamed(ColumnId::new(0), DataType::Int64), // order key
        ColumnDesc::unnamed(ColumnId::new(1), DataType::Varchar), // partition key
        ColumnDesc::unnamed(ColumnId::new(2), DataType::Int64), // pk
        ColumnDesc::unnamed(ColumnId::new(3), DataType::Int32), // x
    ];
    for call in &calls {
        table_columns.push(ColumnDesc::unnamed(
            ColumnId::new(table_columns.len() as i32),
            call.return_type.clone(),
        ));
    }
    let table_pk_indices = vec![1, 0, 2];
    let table_order_types = vec![
        OrderType::ascending(),
        OrderType::ascending(),
        OrderType::ascending(),
    ];

    let output_schema = {
        let mut fields = input_schema.fields.clone();
        calls.iter().for_each(|call| {
            fields.push(Field::unnamed(call.return_type.clone()));
        });
        Schema { fields }
    };

    let state_table = StateTable::from_table_catalog(
        &gen_pbtable(
            TableId::new(1),
            table_columns,
            table_order_types,
            table_pk_indices,
            0,
        ),
        store,
        None,
    )
    .await;

    let (tx, source) = MockSource::channel();
    let source = source.into_executor(input_schema, input_stream_key.clone());
    let executor = OverWindowExecutor::new(OverWindowExecutorArgs {
        actor_ctx: ActorContext::for_test(123),

        input: source,

        schema: output_schema,
        calls,
        partition_key_indices,
        order_key_indices,
        order_key_order_types,
        state_table,
        watermark_epoch,
        metrics,
        chunk_size: 1024,
        cache_policy: OverWindowCachePolicy::Recent,
    });
    (tx, executor.boxed().execute())
}

fn snapshot_options() -> SnapshotOptions {
    SnapshotOptions::default().include_applied_result(true)
}

#[tokio::test]
async fn test_over_window_lag_lead_append_only() {
    let store = MemoryStateStore::new();
    let calls = vec![
        // lag(x, 1)
        WindowFuncCall {
            kind: WindowFuncKind::Aggregate(PbAggKind::FirstValue.into()),
            return_type: DataType::Int32,
            args: AggArgs::from_iter([(DataType::Int32, 3)]),
            ignore_nulls: false,
            frame: Frame::rows(FrameBound::Preceding(1), FrameBound::Preceding(1)),
        },
        // lead(x, 1)
        WindowFuncCall {
            kind: WindowFuncKind::Aggregate(PbAggKind::FirstValue.into()),
            return_type: DataType::Int32,
            args: AggArgs::from_iter([(DataType::Int32, 3)]),
            ignore_nulls: false,
            frame: Frame::rows(FrameBound::Following(1), FrameBound::Following(1)),
        },
    ];

    check_with_script(
        || create_executor(calls.clone(), store.clone()),
        r###"
        - !barrier 1
        - !chunk |2
              I T  I   i
            + 1 p1 100 10
            + 1 p1 101 16
        - !chunk |2
              I T  I   i
            + 5 p1 102 18
        - !barrier 2
        - recovery
        - !barrier 2
        - !chunk |2
              I  T  I   i
            + 10 p1 103 13
        - !barrier 3
        "###,
        expect![[r#"
            - input: !barrier 1
              output:
              - !barrier 1
            - input: !chunk |-
                +---+---+----+-----+----+
                | + | 1 | p1 | 100 | 10 |
                | + | 1 | p1 | 101 | 16 |
                +---+---+----+-----+----+
              output:
              - !chunk |-
                +---+---+----+-----+----+----+----+
                | + | 1 | p1 | 100 | 10 |    | 16 |
                | + | 1 | p1 | 101 | 16 | 10 |    |
                +---+---+----+-----+----+----+----+
                applied result:
                +---+----+-----+----+----+----+
                | 1 | p1 | 100 | 10 |    | 16 |
                | 1 | p1 | 101 | 16 | 10 |    |
                +---+----+-----+----+----+----+
            - input: !chunk |-
                +---+---+----+-----+----+
                | + | 5 | p1 | 102 | 18 |
                +---+---+----+-----+----+
              output:
              - !chunk |-
                +----+---+----+-----+----+----+----+
                | U- | 1 | p1 | 101 | 16 | 10 |    |
                | U+ | 1 | p1 | 101 | 16 | 10 | 18 |
                |  + | 5 | p1 | 102 | 18 | 16 |    |
                +----+---+----+-----+----+----+----+
                applied result:
                +---+----+-----+----+----+----+
                | 1 | p1 | 100 | 10 |    | 16 |
                | 1 | p1 | 101 | 16 | 10 | 18 |
                | 5 | p1 | 102 | 18 | 16 |    |
                +---+----+-----+----+----+----+
            - input: !barrier 2
              output:
              - !barrier 2
            - input: recovery
              output: []
            - input: !barrier 2
              output:
              - !barrier 2
            - input: !chunk |-
                +---+----+----+-----+----+
                | + | 10 | p1 | 103 | 13 |
                +---+----+----+-----+----+
              output:
              - !chunk |-
                +----+----+----+-----+----+----+----+
                | U- | 5  | p1 | 102 | 18 | 16 |    |
                | U+ | 5  | p1 | 102 | 18 | 16 | 13 |
                |  + | 10 | p1 | 103 | 13 | 18 |    |
                +----+----+----+-----+----+----+----+
                applied result:
                +----+----+-----+----+----+----+
                | 1  | p1 | 100 | 10 |    | 16 |
                | 1  | p1 | 101 | 16 | 10 | 18 |
                | 5  | p1 | 102 | 18 | 16 | 13 |
                | 10 | p1 | 103 | 13 | 18 |    |
                +----+----+-----+----+----+----+
            - input: !barrier 3
              output:
              - !barrier 3
        "#]],
        snapshot_options(),
    )
    .await;
}

#[tokio::test]
async fn test_over_window_lag_lead_with_updates() {
    let store = MemoryStateStore::new();
    let calls = vec![
        // lag(x, 1)
        WindowFuncCall {
            kind: WindowFuncKind::Aggregate(PbAggKind::FirstValue.into()),
            return_type: DataType::Int32,
            args: AggArgs::from_iter([(DataType::Int32, 3)]),
            ignore_nulls: false,
            frame: Frame::rows(FrameBound::Preceding(1), FrameBound::Preceding(1)),
        },
        // lead(x, 1)
        WindowFuncCall {
            kind: WindowFuncKind::Aggregate(PbAggKind::FirstValue.into()),
            return_type: DataType::Int32,
            args: AggArgs::from_iter([(DataType::Int32, 3)]),
            ignore_nulls: false,
            frame: Frame::rows(FrameBound::Following(1), FrameBound::Following(1)),
        },
    ];

    check_with_script(
        || create_executor(calls.clone(), store.clone()),
        r###"
        - !barrier 1
        - !chunk |2
              I T  I   i
            + 1 p1 100 10
            + 1 p2 200 20
            + 2 p1 101 16
            + 3 p1 103 13
            - 3 p1 103 13 // deletes the above row
        - !chunk |2
              I T  I   i
           U- 1 p1 100 10
           U+ 3 p1 100 13 // an order-change update, `x` also changed
            + 5 p1 105 18
            + 6 p2 203 23
        - !barrier 2
        - recovery
        - !barrier 2
        - !chunk |2
              I T  I   i
            - 6 p2 203 23
           U- 2 p1 101 16
           U+ 2 p2 101 16 // a partition-change update
        - !barrier 3
        - recovery
        - !barrier 3
        - !chunk |2
              I  T  I   i
            + 10 p3 300 30
        - !barrier 4
        "###,
        expect![[r#"
            - input: !barrier 1
              output:
              - !barrier 1
            - input: !chunk |-
                +---+---+----+-----+----+
                | + | 1 | p1 | 100 | 10 |
                | + | 1 | p2 | 200 | 20 |
                | + | 2 | p1 | 101 | 16 |
                | + | 3 | p1 | 103 | 13 |
                | - | 3 | p1 | 103 | 13 |
                +---+---+----+-----+----+
              output:
              - !chunk |-
                +---+---+----+-----+----+----+----+
                | + | 1 | p1 | 100 | 10 |    | 16 |
                | + | 2 | p1 | 101 | 16 | 10 |    |
                | + | 1 | p2 | 200 | 20 |    |    |
                +---+---+----+-----+----+----+----+
                applied result:
                +---+----+-----+----+----+----+
                | 1 | p1 | 100 | 10 |    | 16 |
                | 1 | p2 | 200 | 20 |    |    |
                | 2 | p1 | 101 | 16 | 10 |    |
                +---+----+-----+----+----+----+
            - input: !chunk |-
                +----+---+----+-----+----+
                | U- | 1 | p1 | 100 | 10 |
                | U+ | 3 | p1 | 100 | 13 |
                |  + | 5 | p1 | 105 | 18 |
                |  + | 6 | p2 | 203 | 23 |
                +----+---+----+-----+----+
              output:
              - !chunk |-
                +----+---+----+-----+----+----+----+
                | U- | 2 | p1 | 101 | 16 | 10 |    |
                | U+ | 2 | p1 | 101 | 16 |    | 13 |
                | U- | 1 | p1 | 100 | 10 |    | 16 |
                | U+ | 3 | p1 | 100 | 13 | 16 | 18 |
                |  + | 5 | p1 | 105 | 18 | 13 |    |
                | U- | 1 | p2 | 200 | 20 |    |    |
                | U+ | 1 | p2 | 200 | 20 |    | 23 |
                |  + | 6 | p2 | 203 | 23 | 20 |    |
                +----+---+----+-----+----+----+----+
                applied result:
                +---+----+-----+----+----+----+
                | 1 | p2 | 200 | 20 |    | 23 |
                | 2 | p1 | 101 | 16 |    | 13 |
                | 3 | p1 | 100 | 13 | 16 | 18 |
                | 5 | p1 | 105 | 18 | 13 |    |
                | 6 | p2 | 203 | 23 | 20 |    |
                +---+----+-----+----+----+----+
            - input: !barrier 2
              output:
              - !barrier 2
            - input: recovery
              output: []
            - input: !barrier 2
              output:
              - !barrier 2
            - input: !chunk |-
                +----+---+----+-----+----+
                |  - | 6 | p2 | 203 | 23 |
                | U- | 2 | p1 | 101 | 16 |
                | U+ | 2 | p2 | 101 | 16 |
                +----+---+----+-----+----+
              output:
              - !chunk |-
                +----+---+----+-----+----+----+----+
                |  - | 2 | p1 | 101 | 16 |    | 13 |
                | U- | 3 | p1 | 100 | 13 | 16 | 18 |
                | U+ | 3 | p1 | 100 | 13 |    | 18 |
                | U- | 1 | p2 | 200 | 20 |    | 23 |
                | U+ | 1 | p2 | 200 | 20 |    | 16 |
                |  + | 2 | p2 | 101 | 16 | 20 |    |
                |  - | 6 | p2 | 203 | 23 | 20 |    |
                +----+---+----+-----+----+----+----+
                applied result:
                +---+----+-----+----+----+----+
                | 1 | p2 | 200 | 20 |    | 16 |
                | 2 | p2 | 101 | 16 | 20 |    |
                | 3 | p1 | 100 | 13 |    | 18 |
                | 5 | p1 | 105 | 18 | 13 |    |
                +---+----+-----+----+----+----+
            - input: !barrier 3
              output:
              - !barrier 3
            - input: recovery
              output: []
            - input: !barrier 3
              output:
              - !barrier 3
            - input: !chunk |-
                +---+----+----+-----+----+
                | + | 10 | p3 | 300 | 30 |
                +---+----+----+-----+----+
              output:
              - !chunk |-
                +---+----+----+-----+----+---+---+
                | + | 10 | p3 | 300 | 30 |   |   |
                +---+----+----+-----+----+---+---+
                applied result:
                +----+----+-----+----+----+----+
                | 1  | p2 | 200 | 20 |    | 16 |
                | 2  | p2 | 101 | 16 | 20 |    |
                | 3  | p1 | 100 | 13 |    | 18 |
                | 5  | p1 | 105 | 18 | 13 |    |
                | 10 | p3 | 300 | 30 |    |    |
                +----+----+-----+----+----+----+
            - input: !barrier 4
              output:
              - !barrier 4
        "#]],
        snapshot_options(),
    )
    .await;
}

/// Test that force-evicting the partition cache between chunks (in the middle of an epoch)
/// doesn't affect the results. The unbounded frame start forces `CachePolicy::Full`, and the
/// evicted partition must be reloaded from the state table on the next access.
#[tokio::test]
async fn test_over_window_evict_between_chunks() {
    let store = MemoryStateStore::new();
    let watermark = Arc::new(AtomicU64::new(0));
    let metrics = Arc::new(StreamingMetrics::unused());
    let ow_metrics = metrics.new_over_window_metrics(TableId::new(1), 123.into(), 0.into());
    let initial_lookup_count = ow_metrics.over_window_cache_lookup_count.get();
    let initial_miss_count = ow_metrics.over_window_cache_miss_count.get();
    let calls = vec![
        // sum(x) over (partition by .. order by .. rows unbounded preceding)
        WindowFuncCall {
            kind: WindowFuncKind::Aggregate(PbAggKind::Sum.into()),
            return_type: DataType::Int64,
            args: AggArgs::from_iter([(DataType::Int32, 3)]),
            ignore_nulls: false,
            frame: Frame::rows(FrameBound::UnboundedPreceding, FrameBound::CurrentRow),
        },
    ];
    let (mut tx, mut stream) =
        create_executor_with_watermark(calls, store, watermark.clone(), metrics.clone()).await;

    tx.push_barrier(test_epoch(1), false);
    stream.expect_barrier().await;

    tx.push_chunk(StreamChunk::from_pretty(
        " I T  I   i
        + 1 p1 100 10
        + 2 p1 101 16",
    ));
    assert_eq!(
        stream.expect_chunk().await,
        StreamChunk::from_pretty(
            " I T  I   i  I
            + 1 p1 100 10 10
            + 2 p1 101 16 26",
        )
    );

    tx.push_barrier(test_epoch(2), false);
    stream.expect_barrier().await;

    // Demand eviction of all cache entries. `p1` is not accessed in the current epoch, so
    // it will be evicted at the next chunk boundary.
    watermark.store(u64::MAX, Ordering::Relaxed);

    tx.push_chunk(StreamChunk::from_pretty(
        " I T  I   i
        + 1 p2 200 5",
    ));
    assert_eq!(
        stream.expect_chunk().await,
        StreamChunk::from_pretty(
            " I T  I   i I
            + 1 p2 200 5 5",
        )
    );

    // Still in the same epoch, `p1` must be reloaded from the state table and produce
    // correct running sums.
    tx.push_chunk(StreamChunk::from_pretty(
        " I T  I   i
        + 3 p1 102 4",
    ));
    assert_eq!(
        stream.expect_chunk().await,
        StreamChunk::from_pretty(
            " I T  I   i I
            + 3 p1 102 4 30",
        )
    );

    // Append to `p1` again in the same epoch. The partition, now containing a row not
    // yet committed, is evicted again at the last chunk boundary, and the reload must
    // see the uncommitted row through the state table.
    tx.push_chunk(StreamChunk::from_pretty(
        " I T  I   i
        + 4 p1 103 2",
    ));
    assert_eq!(
        stream.expect_chunk().await,
        StreamChunk::from_pretty(
            " I T  I   i I
            + 4 p1 103 2 32",
        )
    );

    tx.push_barrier(test_epoch(3), false);
    stream.expect_barrier().await;
    // Poll once more so that the executor runs the post-barrier code, which flushes
    // cache stats to metrics.
    stream.next_unwrap_pending();

    // 4 partition lookups (p1, p2, p1, p1), all missing the cache: `p1` was evicted at
    // each chunk boundary in the second epoch. Without chunk-boundary eviction, the
    // last two lookups would hit the cache and the miss count would be 2.
    assert_eq!(
        ow_metrics.over_window_cache_lookup_count.get(),
        initial_lookup_count + 4
    );
    assert_eq!(
        ow_metrics.over_window_cache_miss_count.get(),
        initial_miss_count + 4
    );
}

/// Regression test: deleting all rows of a fully-cached partition (no sentinels in the
/// cache) leaves the range cache with zero entries. `PartitionCache::shrink` with
/// `CachePolicy::Recent` used to panic on such an empty cache at the next barrier.
///
/// Note that the insert and the delete must happen within the same barrier interval:
/// `shrink` only runs for partitions registered in `recently_accessed_ranges`, and a
/// chunk that deletes all rows of a partition doesn't register it (empty affected
/// ranges), so the registration must come from the insert chunk.
#[tokio::test]
async fn test_over_window_delete_all_rows_of_partition() {
    let store = MemoryStateStore::new();
    let calls = vec![
        // lag(x, 1), doesn't force `CachePolicy::Full`
        WindowFuncCall {
            kind: WindowFuncKind::Aggregate(PbAggKind::FirstValue.into()),
            return_type: DataType::Int32,
            args: AggArgs::from_iter([(DataType::Int32, 3)]),
            ignore_nulls: false,
            frame: Frame::rows(FrameBound::Preceding(1), FrameBound::Preceding(1)),
        },
    ];
    let (mut tx, mut stream) = create_executor(calls, store).await;

    tx.push_barrier(test_epoch(1), false);
    stream.expect_barrier().await;

    tx.push_chunk(StreamChunk::from_pretty(
        " I T  I   i
        + 1 p1 100 10
        + 2 p1 101 16",
    ));
    assert_eq!(
        stream.expect_chunk().await,
        StreamChunk::from_pretty(
            " I T  I   i  i
            + 1 p1 100 10 .
            + 2 p1 101 16 10",
        )
    );

    // Delete all rows of `p1` in the same epoch, emptying the range cache.
    tx.push_chunk(StreamChunk::from_pretty(
        " I T  I   i
        - 1 p1 100 10
        - 2 p1 101 16",
    ));
    assert_eq!(
        stream.expect_chunk().await,
        StreamChunk::from_pretty(
            " I T  I   i  i
            - 1 p1 100 10 .
            - 2 p1 101 16 10",
        )
    );

    tx.push_barrier(test_epoch(2), false);
    stream.expect_barrier().await;
    // Poll once more to run the post-barrier cache shrinking, which used to panic on
    // the emptied `p1` cache.
    stream.next_unwrap_pending();
}

#[tokio::test]
async fn test_over_window_sum() {
    let store = MemoryStateStore::new();
    let calls = vec![
        // sum(x) over (
        //   partition by ..
        //   order by ..
        //   rows between 1 preceding and 2 following exclude current row
        // )
        WindowFuncCall {
            kind: WindowFuncKind::Aggregate(PbAggKind::Sum.into()),
            return_type: DataType::Int64,
            args: AggArgs::from_iter([(DataType::Int32, 3)]),
            ignore_nulls: false,
            frame: Frame::rows_with_exclusion(
                FrameBound::Preceding(1),
                FrameBound::Following(2),
                FrameExclusion::CurrentRow,
            ),
        },
    ];

    check_with_script(
        || create_executor(calls.clone(), store.clone()),
        r###"
        - !barrier 1
        - !chunk |2
              I T  I   i
            + 1 p1 100 10
            + 1 p2 200 20
            + 2 p1 101 16
            + 2 p1 102 17
        - !chunk |2
              I T  I   i
           U- 1 p1 100 10
           U+ 3 p1 100 13 // an order-change update, `x` also changed
            + 5 p1 105 18
            + 6 p2 203 23
        - !barrier 2
        - recovery
        - !barrier 2
        - !chunk |2
              I T  I   i
            - 6 p2 203 23
           U- 2 p1 101 16
           U+ 2 p2 101 16 // a partition-change update
        - !barrier 3
        - recovery
        - !barrier 3
        - !chunk |2
              I  T  I   i
            + 10 p3 300 30
        - !barrier 4
        "###,
        expect![[r#"
            - input: !barrier 1
              output:
              - !barrier 1
            - input: !chunk |-
                +---+---+----+-----+----+
                | + | 1 | p1 | 100 | 10 |
                | + | 1 | p2 | 200 | 20 |
                | + | 2 | p1 | 101 | 16 |
                | + | 2 | p1 | 102 | 17 |
                +---+---+----+-----+----+
              output:
              - !chunk |-
                +---+---+----+-----+----+----+
                | + | 1 | p1 | 100 | 10 | 33 |
                | + | 2 | p1 | 101 | 16 | 27 |
                | + | 2 | p1 | 102 | 17 | 16 |
                | + | 1 | p2 | 200 | 20 |    |
                +---+---+----+-----+----+----+
                applied result:
                +---+----+-----+----+----+
                | 1 | p1 | 100 | 10 | 33 |
                | 1 | p2 | 200 | 20 |    |
                | 2 | p1 | 101 | 16 | 27 |
                | 2 | p1 | 102 | 17 | 16 |
                +---+----+-----+----+----+
            - input: !chunk |-
                +----+---+----+-----+----+
                | U- | 1 | p1 | 100 | 10 |
                | U+ | 3 | p1 | 100 | 13 |
                |  + | 5 | p1 | 105 | 18 |
                |  + | 6 | p2 | 203 | 23 |
                +----+---+----+-----+----+
              output:
              - !chunk |-
                +----+---+----+-----+----+----+
                | U- | 2 | p1 | 101 | 16 | 27 |
                | U+ | 2 | p1 | 101 | 16 | 30 |
                | U- | 2 | p1 | 102 | 17 | 16 |
                | U+ | 2 | p1 | 102 | 17 | 47 |
                | U- | 1 | p1 | 100 | 10 | 33 |
                | U+ | 3 | p1 | 100 | 13 | 35 |
                |  + | 5 | p1 | 105 | 18 | 13 |
                | U- | 1 | p2 | 200 | 20 |    |
                | U+ | 1 | p2 | 200 | 20 | 23 |
                |  + | 6 | p2 | 203 | 23 | 20 |
                +----+---+----+-----+----+----+
                applied result:
                +---+----+-----+----+----+
                | 1 | p2 | 200 | 20 | 23 |
                | 2 | p1 | 101 | 16 | 30 |
                | 2 | p1 | 102 | 17 | 47 |
                | 3 | p1 | 100 | 13 | 35 |
                | 5 | p1 | 105 | 18 | 13 |
                | 6 | p2 | 203 | 23 | 20 |
                +---+----+-----+----+----+
            - input: !barrier 2
              output:
              - !barrier 2
            - input: recovery
              output: []
            - input: !barrier 2
              output:
              - !barrier 2
            - input: !chunk |-
                +----+---+----+-----+----+
                |  - | 6 | p2 | 203 | 23 |
                | U- | 2 | p1 | 101 | 16 |
                | U+ | 2 | p2 | 101 | 16 |
                +----+---+----+-----+----+
              output:
              - !chunk |-
                +----+---+----+-----+----+----+
                |  - | 2 | p1 | 101 | 16 | 30 |
                | U- | 2 | p1 | 102 | 17 | 47 |
                | U+ | 2 | p1 | 102 | 17 | 31 |
                | U- | 1 | p2 | 200 | 20 | 23 |
                | U+ | 1 | p2 | 200 | 20 | 16 |
                |  + | 2 | p2 | 101 | 16 | 20 |
                |  - | 6 | p2 | 203 | 23 | 20 |
                +----+---+----+-----+----+----+
                applied result:
                +---+----+-----+----+----+
                | 1 | p2 | 200 | 20 | 16 |
                | 2 | p1 | 102 | 17 | 31 |
                | 2 | p2 | 101 | 16 | 20 |
                | 3 | p1 | 100 | 13 | 35 |
                | 5 | p1 | 105 | 18 | 13 |
                +---+----+-----+----+----+
            - input: !barrier 3
              output:
              - !barrier 3
            - input: recovery
              output: []
            - input: !barrier 3
              output:
              - !barrier 3
            - input: !chunk |-
                +---+----+----+-----+----+
                | + | 10 | p3 | 300 | 30 |
                +---+----+----+-----+----+
              output:
              - !chunk |-
                +---+----+----+-----+----+---+
                | + | 10 | p3 | 300 | 30 |   |
                +---+----+----+-----+----+---+
                applied result:
                +----+----+-----+----+----+
                | 1  | p2 | 200 | 20 | 16 |
                | 2  | p1 | 102 | 17 | 31 |
                | 2  | p2 | 101 | 16 | 20 |
                | 3  | p1 | 100 | 13 | 35 |
                | 5  | p1 | 105 | 18 | 13 |
                | 10 | p3 | 300 | 30 |    |
                +----+----+-----+----+----+
            - input: !barrier 4
              output:
              - !barrier 4
        "#]],
        snapshot_options(),
    )
    .await;
}
