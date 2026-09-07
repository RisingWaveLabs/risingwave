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

use risingwave_common::types::{Interval, Timestamp, Timestamptz};
use risingwave_expr::{ExprError, Result, function};

use super::timestamptz::timestamp_at_time_zone;

// TODO(xiangjinwu): parse into an enum
const MICROSECONDS: &str = "microseconds";
const MILLISECONDS: &str = "milliseconds";
const SECOND: &str = "second";
const MINUTE: &str = "minute";
const HOUR: &str = "hour";
const DAY: &str = "day";
const WEEK: &str = "week";
const MONTH: &str = "month";
const QUARTER: &str = "quarter";
const YEAR: &str = "year";
const DECADE: &str = "decade";
const CENTURY: &str = "century";
const MILLENNIUM: &str = "millennium";

#[function("date_trunc(varchar, timestamp) -> timestamp")]
pub fn date_trunc_timestamp(field: &str, ts: Timestamp) -> Result<Timestamp> {
    Ok(match field.to_ascii_lowercase().as_str() {
        MICROSECONDS => ts.truncate_micros(),
        MILLISECONDS => ts.truncate_millis(),
        SECOND => ts.truncate_second(),
        MINUTE => ts.truncate_minute(),
        HOUR => ts.truncate_hour(),
        DAY => ts.truncate_day(),
        WEEK => ts.truncate_week(),
        MONTH => ts.truncate_month(),
        QUARTER => ts.truncate_quarter(),
        YEAR => ts.truncate_year(),
        DECADE => ts.truncate_decade(),
        CENTURY => ts.truncate_century(),
        MILLENNIUM => ts.truncate_millennium(),
        _ => return Err(invalid_field_error(field)),
    })
}

#[function("date_trunc(varchar, timestamptz) -> timestamptz", rewritten)]
fn _date_trunc_timestamptz() {}

#[function("date_trunc(varchar, timestamptz, varchar) -> timestamptz")]
pub fn date_trunc_timestamptz_at_timezone(
    field: &str,
    tsz: Timestamptz,
    timezone: &str,
) -> Result<Timestamptz> {
    use chrono::Offset as _;

    use super::timestamptz::time_zone_err;

    let tz = Timestamptz::lookup_time_zone(timezone).map_err(time_zone_err)?;
    let instant_local = tsz.to_datetime_in_zone(tz);

    let truncated_naive = date_trunc_timestamp(field, Timestamp(instant_local.naive_local()))?;

    match field.to_ascii_lowercase().as_str() {
        MICROSECONDS | MILLISECONDS | SECOND | MINUTE | HOUR => {
            // When unit < day, follow PostgreSQL to use old timezone offset.
            // rather than reinterpret it in the timezone.
            // https://github.com/postgres/postgres/blob/REL_16_0/src/backend/utils/adt/timestamp.c#L4270
            // See `e2e_test/batch/functions/issue_12072.slt.part` for the difference.
            let fixed = instant_local.offset().fix();
            // `unwrap` is okay because `FixedOffset` always returns single unique conversion result.
            let truncated_local = truncated_naive.0.and_local_timezone(fixed).unwrap();
            Ok(Timestamptz::from_micros_uncheck(
                truncated_local.timestamp_micros(),
            ))
        }
        _ => timestamp_at_time_zone(truncated_naive, timezone),
    }
}

#[function("date_trunc(varchar, interval) -> interval")]
pub fn date_trunc_interval(field: &str, interval: Interval) -> Result<Interval> {
    Ok(match field.to_ascii_lowercase().as_str() {
        MICROSECONDS => interval,
        MILLISECONDS => interval.truncate_millis(),
        SECOND => interval.truncate_second(),
        MINUTE => interval.truncate_minute(),
        HOUR => interval.truncate_hour(),
        DAY => interval.truncate_day(),
        WEEK => return Err(ExprError::UnsupportedFunction(
            "interval units \"week\" not supported because months usually have fractional weeks"
                .into(),
        )),
        MONTH => interval.truncate_month(),
        QUARTER => interval.truncate_quarter(),
        YEAR => interval.truncate_year(),
        DECADE => interval.truncate_decade(),
        CENTURY => interval.truncate_century(),
        MILLENNIUM => interval.truncate_millennium(),
        _ => return Err(invalid_field_error(field)),
    })
}

#[inline]
fn invalid_field_error(field: &str) -> ExprError {
    ExprError::InvalidParam {
        name: "field",
        reason: format!("invalid field {field:?}. must be one of: microseconds, milliseconds, second, minute, hour, day, week, month, quarter, year, decade, century, millennium").into(),
    }
}
