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

use num_traits::Zero;
use risingwave_common::types::{Date, Interval, Timestamp, Timestamptz};
use risingwave_expr::{ExprError, Result, function};

#[inline(always)]
fn interval_to_micro_second(t: Interval) -> Result<i64> {
    let checked_interval_to_micro_second = || {
        (t.months() as i64)
            .checked_mul(Interval::USECS_PER_MONTH)?
            .checked_add(
                (t.days() as i64)
                    .checked_mul(Interval::USECS_PER_DAY)?
                    .checked_add(t.usecs())?,
            )
    };

    checked_interval_to_micro_second().ok_or(ExprError::NumericOutOfRange)
}

#[function("tumble_start(date, interval) -> timestamp")]
pub fn tumble_start_date(timestamp: Date, window_size: Interval) -> Result<Timestamp> {
    tumble_start_date_time(timestamp.into(), window_size)
}

#[function("tumble_start(timestamp, interval) -> timestamp")]
pub fn tumble_start_date_time(timestamp: Timestamp, window_size: Interval) -> Result<Timestamp> {
    let timestamp_micro_second = timestamp.0.and_utc().timestamp_micros();
    let window_start_micro_second = get_window_start(timestamp_micro_second, window_size)?;
    Timestamp::with_micros(window_start_micro_second).map_err(|_| ExprError::NumericOutOfRange)
}

#[function("tumble_start(timestamptz, interval) -> timestamptz")]
pub fn tumble_start_timestamptz(tz: Timestamptz, window_size: Interval) -> Result<Timestamptz> {
    get_window_start(tz.timestamp_micros(), window_size)
        .and_then(|us| Timestamptz::from_micros(us).ok_or(ExprError::NumericOutOfRange))
}

/// The common part of PostgreSQL function `timestamp_bin` and `timestamptz_bin`.
#[inline(always)]
fn get_window_start(timestamp_micro_second: i64, window_size: Interval) -> Result<i64> {
    get_window_start_with_offset(timestamp_micro_second, window_size, Interval::zero())
}

#[function("tumble_start(date, interval, interval) -> timestamp")]
pub fn tumble_start_offset_date(
    timestamp_date: Date,
    window_size: Interval,
    offset: Interval,
) -> Result<Timestamp> {
    tumble_start_offset_date_time(timestamp_date.into(), window_size, offset)
}

#[function("tumble_start(timestamp, interval, interval) -> timestamp")]
pub fn tumble_start_offset_date_time(
    time: Timestamp,
    window_size: Interval,
    offset: Interval,
) -> Result<Timestamp> {
    let timestamp_micro_second = time.0.and_utc().timestamp_micros();
    let window_start_micro_second =
        get_window_start_with_offset(timestamp_micro_second, window_size, offset)?;

    Timestamp::with_micros(window_start_micro_second).map_err(|_| ExprError::NumericOutOfRange)
}

#[inline(always)]
fn get_window_start_with_offset(
    timestamp_micro_second: i64,
    window_size: Interval,
    offset: Interval,
) -> Result<i64> {
    let window_size_micro_second = interval_to_micro_second(window_size)?;
    let offset_micro_second = interval_to_micro_second(offset)?;

    // Inspired by https://issues.apache.org/jira/browse/FLINK-26334
    let remainder = timestamp_micro_second
        .checked_sub(offset_micro_second)
        .ok_or(ExprError::NumericOutOfRange)?
        .checked_rem(window_size_micro_second)
        .ok_or(ExprError::DivisionByZero)?;
    if remainder < 0 {
        timestamp_micro_second
            .checked_sub(remainder + window_size_micro_second)
            .ok_or(ExprError::NumericOutOfRange)
    } else {
        timestamp_micro_second
            .checked_sub(remainder)
            .ok_or(ExprError::NumericOutOfRange)
    }
}

#[function("tumble_start(timestamptz, interval, interval) -> timestamptz")]
pub fn tumble_start_offset_timestamptz(
    tz: Timestamptz,
    window_size: Interval,
    offset: Interval,
) -> Result<Timestamptz> {
    get_window_start_with_offset(tz.timestamp_micros(), window_size, offset)
        .and_then(|us| Timestamptz::from_micros(us).ok_or(ExprError::NumericOutOfRange))
}

#[cfg(test)]
mod tests {
    use chrono::{Datelike, Timelike};
    use risingwave_common::types::test_utils::IntervalTestExt;
    use risingwave_common::types::{Date, Interval, Timestamp};

    use super::tumble_start_offset_date_time;
    use crate::scalar::tumble::{
        get_window_start, interval_to_micro_second, tumble_start_date_time,
    };

    #[test]
    fn test_tumble_start_date_time() {
        let dt = Date::from_ymd_uncheck(2022, 2, 22).and_hms_uncheck(22, 22, 22);
        let interval = Interval::from_minutes(30);
        let w = tumble_start_date_time(dt, interval).unwrap().0;
        assert_eq!(w.year(), 2022);
        assert_eq!(w.month(), 2);
        assert_eq!(w.day(), 22);
        assert_eq!(w.hour(), 22);
        assert_eq!(w.minute(), 0);
        assert_eq!(w.second(), 0);
    }

    #[test]
    fn test_tumble_start_negative_fractional() {
        // A pre-1970 sub-second window start used to wrap `as u32` and panic.
        let dt = "1969-12-31 23:59:58.450".parse::<Timestamp>().unwrap();
        let w = tumble_start_date_time(dt, Interval::from_millis(100)).unwrap();
        assert_eq!(w, "1969-12-31 23:59:58.400".parse::<Timestamp>().unwrap());
    }

    #[test]
    fn test_tumble_start_offset_date_time() {
        let dt = Date::from_ymd_uncheck(2022, 2, 22).and_hms_uncheck(22, 22, 22);
        let window_size = 30;
        for offset in 0..window_size {
            for coefficient in 0..5 {
                let w = tumble_start_date_time(dt, Interval::from_minutes(window_size))
                    .unwrap()
                    .0;
                println!("{}", w);
                let w = tumble_start_offset_date_time(
                    dt,
                    Interval::from_minutes(window_size),
                    Interval::from_minutes(coefficient * window_size + offset),
                )
                .unwrap()
                .0;
                assert_eq!(w.year(), 2022);
                assert_eq!(w.month(), 2);
                assert_eq!(w.day(), 22);
                if offset > 22 {
                    assert_eq!(w.hour(), 21);
                    assert_eq!(w.minute(), 30 + offset as u32);
                } else {
                    assert_eq!(w.hour(), 22);
                    assert_eq!(w.minute(), offset as u32);
                }

                assert_eq!(w.second(), 0);
            }
        }
    }

    #[test]
    fn test_remainder_necessary() {
        let mut wrong_cnt = 0;
        for i in -30..30 {
            let timestamp_micro_second = Interval::from_minutes(i).usecs();
            let window_size = Interval::from_minutes(5);
            let window_start = get_window_start(timestamp_micro_second, window_size).unwrap();

            let window_size_micro_second = interval_to_micro_second(window_size).unwrap();
            let default_window_start = timestamp_micro_second
                - (timestamp_micro_second + window_size_micro_second) % window_size_micro_second;

            if timestamp_micro_second < default_window_start {
                // which is wrong
                wrong_cnt += 1;
            }

            assert!(timestamp_micro_second >= window_start)
        }
        assert_ne!(wrong_cnt, 0);
    }

    #[test]
    fn test_window_start_overflow() {
        get_window_start(i64::MIN, Interval::from_millis(20)).unwrap_err();
        interval_to_micro_second(Interval::from_month_day_usec(1, 1, i64::MAX)).unwrap_err();
    }
}
