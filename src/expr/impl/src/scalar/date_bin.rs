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

use risingwave_common::types::{Interval, Timestamp, Timestamptz};
use risingwave_expr::{ExprError, Result, function};

#[function("date_bin(interval, timestamp, timestamp) -> timestamp")]
pub fn date_bin_ts(stride: Interval, source: Timestamp, origin: Timestamp) -> Result<Timestamp> {
    let source_us = source.0.and_utc().timestamp_micros(); // source to microseconds
    let origin_us = origin.0.and_utc().timestamp_micros(); // origin to microseconds

    let binned_source_us = date_bin_inner(stride, source_us, origin_us)?;
    Timestamp::with_micros(binned_source_us).map_err(|_| ExprError::NumericOutOfRange)
}

#[function("date_bin(interval, timestamptz, timestamptz) -> timestamptz")]
pub fn date_bin_tstz(
    stride: Interval,
    source: Timestamptz,
    origin: Timestamptz,
) -> Result<Timestamptz> {
    let source_us = source.timestamp_micros(); // source to microseconds
    let origin_us = origin.timestamp_micros(); // origin to microseconds

    let binned_source_us = date_bin_inner(stride, source_us, origin_us)?;
    Timestamptz::from_micros(binned_source_us).ok_or(ExprError::NumericOutOfRange)
}

fn date_bin_inner(stride: Interval, source_us: i64, origin_us: i64) -> Result<i64> {
    if stride.months() != 0 {
        // PostgreSQL doesn't allow months in the interval for date_bin.
        return Err(ExprError::InvalidParam {
            name: "stride",
            reason: "stride interval with months not supported in date_bin".into(),
        });
    }
    let stride_us = stride.usecs() + (stride.days() as i64) * Interval::USECS_PER_DAY; // stride width in microseconds

    if stride_us <= 0 {
        return Err(ExprError::InvalidParam {
            name: "stride",
            reason: "stride interval must be positive".into(),
        });
    }

    // Compute how far ts is from the origin
    let delta = source_us - origin_us;

    // Floor the delta to the nearest stride
    let bucket = delta.div_euclid(stride_us) * stride_us;

    // Add back to origin to get stridened timestamp
    let binned_source_us = origin_us + bucket;
    Ok(binned_source_us)
}
