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

use risingwave_common::types::{F64, Interval};
use risingwave_expr::{ExprError, Result, function};

// empty
#[function("make_interval() -> interval")]
pub fn make_interval_empty() -> Result<Interval> {
    Ok(Interval::from_month_day_usec(0, 0, 0))
}

// years
#[function("make_interval(int4) -> interval")]
pub fn make_interval_years(years: i32) -> Result<Interval> {
    make_interval(years, 0, 0, 0, 0, 0, F64::from(0.0))
}

// years, months
#[function("make_interval(int4, int4) -> interval")]
pub fn make_interval_years_months(years: i32, months: i32) -> Result<Interval> {
    make_interval(years, months, 0, 0, 0, 0, F64::from(0.0))
}

// years, months, weeks
#[function("make_interval(int4, int4, int4) -> interval")]
pub fn make_interval_years_months_weeks(years: i32, months: i32, weeks: i32) -> Result<Interval> {
    make_interval(years, months, weeks, 0, 0, 0, F64::from(0.0))
}

// years, months, weeks, days
#[function("make_interval(int4, int4, int4, int4) -> interval")]
pub fn make_interval_years_months_weeks_days(
    years: i32,
    months: i32,
    weeks: i32,
    days: i32,
) -> Result<Interval> {
    make_interval(years, months, weeks, days, 0, 0, F64::from(0.0))
}

// years, months, weeks, days, hours
#[function("make_interval(int4, int4, int4, int4, int4) -> interval")]
pub fn make_interval_years_months_weeks_days_hours(
    years: i32,
    months: i32,
    weeks: i32,
    days: i32,
    hours: i32,
) -> Result<Interval> {
    make_interval(years, months, weeks, days, hours, 0, F64::from(0.0))
}

// years, months, weeks, days, hours, mins
#[function("make_interval(int4, int4, int4, int4, int4, int4) -> interval")]
pub fn make_interval_years_months_weeks_days_hours_mins(
    years: i32,
    months: i32,
    weeks: i32,
    days: i32,
    hours: i32,
    mins: i32,
) -> Result<Interval> {
    make_interval(years, months, weeks, days, hours, mins, F64::from(0.0))
}

// years, months, weeks, days, hours, mins, secs
#[function("make_interval(int4, int4, int4, int4, int4, int4, float8) -> interval")]
pub fn make_interval_years_months_weeks_days_hours_mins_secs(
    years: i32,
    months: i32,
    weeks: i32,
    days: i32,
    hours: i32,
    mins: i32,
    secs: F64,
) -> Result<Interval> {
    make_interval(years, months, weeks, days, hours, mins, secs)
}

fn make_interval(
    years: i32,
    months: i32,
    weeks: i32,
    days: i32,
    hours: i32,
    mins: i32,
    secs: F64,
) -> Result<Interval> {
    if !secs.0.is_finite() {
        return Err(ExprError::NumericOutOfRange);
    }

    // years and months -> months
    let total_months = years
        .checked_mul(12)
        .and_then(|m| m.checked_add(months))
        .ok_or(ExprError::NumericOutOfRange)?;

    // weeks and days -> days
    let total_days = weeks
        .checked_mul(7)
        .and_then(|d| d.checked_add(days))
        .ok_or(ExprError::NumericOutOfRange)?;

    // hours and mins -> usecs (cannot overflow 64-bit)
    let hours_in_usecs = hours as i64 * Interval::USECS_PER_HOUR;
    let mins_in_usecs = mins as i64 * Interval::USECS_PER_MINUTE;
    let hours_mins_in_usecs = hours_in_usecs + mins_in_usecs;

    // secs -> usecs
    let secs_in_usecs = (secs.0 * (Interval::USECS_PER_SEC as f64)).round_ties_even();
    let secs_in_usecs = try_into_i64(secs_in_usecs)?;

    // hours and mins and secs -> usecs
    let usecs = hours_mins_in_usecs
        .checked_add(secs_in_usecs)
        .ok_or(ExprError::NumericOutOfRange)?;

    Ok(Interval::from_month_day_usec(
        total_months,
        total_days,
        usecs,
    ))
}

fn try_into_i64(value: f64) -> Result<i64> {
    if value.is_finite() && value >= i64::MIN as f64 && value <= i64::MAX as f64 {
        Ok(value as i64)
    } else {
        Err(ExprError::NumericOutOfRange)
    }
}
