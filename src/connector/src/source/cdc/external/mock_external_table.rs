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

use std::sync::atomic::{AtomicUsize, Ordering};

use futures::stream::BoxStream;
use futures_async_stream::try_stream;
use risingwave_common::catalog::Field;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_common::util::sort_util::{OrderType, cmp_datum};

use crate::error::{ConnectorError, ConnectorResult};
use crate::source::CdcTableSnapshotSplit;
use crate::source::cdc::external::{
    CdcOffset, CdcOffsetParseFunc, CdcTableSnapshotSplitOption, ExternalTableReader, MySqlOffset,
    SchemaTableName,
};
#[derive(Debug)]
pub struct MockExternalTableReader {
    binlog_watermarks: Vec<MySqlOffset>,
    snapshot_cnt: AtomicUsize,
    snapshot_errors_remaining: AtomicUsize,
    cdc_offset_idx: AtomicUsize,
    parallel_backfill_snapshots: Vec<OwnedRow>,
}

impl MockExternalTableReader {
    pub fn new() -> Self {
        let binlog_file = String::from("1.binlog");
        // mock binlog watermarks for backfill
        // initial low watermark: 1.binlog, pos=2 and expected behaviors:
        // - ignore events before (1.binlog, pos=2);
        // - apply events in the range of (1.binlog, pos=2, 1.binlog, pos=4) to the snapshot
        let binlog_watermarks = vec![
            MySqlOffset::new(binlog_file.clone(), 2), // binlog low watermark
            MySqlOffset::new(binlog_file.clone(), 4),
            MySqlOffset::new(binlog_file.clone(), 6),
            MySqlOffset::new(binlog_file.clone(), 8),
            MySqlOffset::new(binlog_file, 10),
        ];
        let parallel_backfill_snapshots = vec![
            OwnedRow::new(vec![
                Some(ScalarImpl::Int64(1)),
                Some(ScalarImpl::Float64(1.0001.into())),
            ]),
            OwnedRow::new(vec![
                Some(ScalarImpl::Int64(1)),
                Some(ScalarImpl::Float64(11.00.into())),
            ]),
            OwnedRow::new(vec![
                Some(ScalarImpl::Int64(2)),
                Some(ScalarImpl::Float64(22.00.into())),
            ]),
            OwnedRow::new(vec![
                Some(ScalarImpl::Int64(5)),
                Some(ScalarImpl::Float64(1.0005.into())),
            ]),
            OwnedRow::new(vec![
                Some(ScalarImpl::Int64(6)),
                Some(ScalarImpl::Float64(1.0006.into())),
            ]),
            OwnedRow::new(vec![
                Some(ScalarImpl::Int64(900)),
                Some(ScalarImpl::Float64(900.1.into())),
            ]),
            OwnedRow::new(vec![
                Some(ScalarImpl::Int64(8)),
                Some(ScalarImpl::Float64(1.0008.into())),
            ]),
            OwnedRow::new(vec![
                Some(ScalarImpl::Int64(400)),
                Some(ScalarImpl::Float64(400.1.into())),
            ]),
        ];
        Self {
            binlog_watermarks,
            snapshot_cnt: AtomicUsize::new(0),
            snapshot_errors_remaining: AtomicUsize::new(0),
            parallel_backfill_snapshots,
            cdc_offset_idx: AtomicUsize::new(0),
        }
    }

    pub fn new_with_snapshot_errors(snapshot_errors: usize) -> Self {
        Self {
            snapshot_errors_remaining: AtomicUsize::new(snapshot_errors),
            ..Self::new()
        }
    }

    fn take_snapshot_error(&self) -> ConnectorResult<()> {
        if self
            .snapshot_errors_remaining
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            return Err(anyhow::anyhow!("mock snapshot read error").into());
        }
        Ok(())
    }

    pub fn get_normalized_table_name(_table_name: &SchemaTableName) -> String {
        "`mock_table`".to_owned()
    }

    pub fn get_cdc_offset_parser() -> CdcOffsetParseFunc {
        Box::new(move |offset| {
            Ok(CdcOffset::MySql(MySqlOffset::parse_debezium_offset(
                offset,
            )?))
        })
    }

    /// The snapshot will emit to downstream all in once, because it is too small.
    /// After that we will emit the buffered upstream chunks all in one.
    #[try_stream(boxed, ok = OwnedRow, error = ConnectorError)]
    async fn snapshot_read_inner(&self) {
        self.take_snapshot_error()?;

        let snap_idx = self.snapshot_cnt.fetch_add(1, Ordering::Relaxed);
        println!("snapshot read: idx {}", snap_idx);

        let snap0 = vec![
            OwnedRow::new(vec![
                Some(ScalarImpl::Int64(1)),
                Some(ScalarImpl::Float64(1.0001.into())),
            ]),
            OwnedRow::new(vec![
                Some(ScalarImpl::Int64(1)),
                Some(ScalarImpl::Float64(11.00.into())),
            ]),
            OwnedRow::new(vec![
                Some(ScalarImpl::Int64(2)),
                Some(ScalarImpl::Float64(22.00.into())),
            ]),
            OwnedRow::new(vec![
                Some(ScalarImpl::Int64(5)),
                Some(ScalarImpl::Float64(1.0005.into())),
            ]),
            OwnedRow::new(vec![
                Some(ScalarImpl::Int64(6)),
                Some(ScalarImpl::Float64(1.0006.into())),
            ]),
            OwnedRow::new(vec![
                Some(ScalarImpl::Int64(8)),
                Some(ScalarImpl::Float64(1.0008.into())),
            ]),
        ];

        let snapshots = [snap0];
        if snap_idx >= snapshots.len() {
            return Ok(());
        }

        for row in &snapshots[snap_idx] {
            yield row.clone();
        }
    }

    #[try_stream(boxed, ok = OwnedRow, error = ConnectorError)]
    async fn split_snapshot_read_inner(&self, left: OwnedRow, right: OwnedRow) {
        self.take_snapshot_error()?;
        for row in &self.parallel_backfill_snapshots {
            if (left[0].is_none()
                || cmp_datum(&row[0], &left[0], OrderType::ascending_nulls_first()).is_ge())
                && (right[0].is_none()
                    || cmp_datum(&row[0], &right[0], OrderType::ascending_nulls_first()).is_lt())
            {
                yield row.clone();
            }
        }
    }
}

impl ExternalTableReader for MockExternalTableReader {
    async fn current_cdc_offset(&self) -> ConnectorResult<CdcOffset> {
        let idx = self.cdc_offset_idx.fetch_add(1, Ordering::Relaxed);
        if idx < self.binlog_watermarks.len() {
            Ok(CdcOffset::MySql(self.binlog_watermarks[idx].clone()))
        } else {
            Ok(CdcOffset::MySql(MySqlOffset {
                filename: "1.binlog".to_owned(),
                position: u64::MAX,
            }))
        }
    }

    fn snapshot_read(
        &self,
        _table_name: SchemaTableName,
        _start_pk: Option<OwnedRow>,
        _primary_keys: Vec<String>,
        _limit: u32,
    ) -> BoxStream<'_, ConnectorResult<OwnedRow>> {
        self.snapshot_read_inner()
    }

    fn get_parallel_cdc_splits(
        &self,
        _options: CdcTableSnapshotSplitOption,
    ) -> BoxStream<'_, ConnectorResult<CdcTableSnapshotSplit>> {
        unreachable!()
    }

    fn split_snapshot_read(
        &self,
        _table_name: SchemaTableName,
        left: OwnedRow,
        right: OwnedRow,
        split_columns: Vec<Field>,
    ) -> BoxStream<'_, ConnectorResult<OwnedRow>> {
        assert_eq!(split_columns.len(), 1);
        assert_eq!(split_columns[0].data_type, DataType::Int64);
        self.split_snapshot_read_inner(left, right)
    }
}
