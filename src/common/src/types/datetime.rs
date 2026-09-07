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

//! Date, time, and timestamp types.

use std::error::Error;
use std::fmt::Display;
use std::hash::Hash;
use std::io::{Cursor, Write};
use std::str::FromStr;

use anyhow::Context;
use byteorder::{BigEndian, ReadBytesExt};
use bytes::BytesMut;
use chrono::{
    DateTime, Datelike, Days, Duration, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Weekday,
};
use postgres_types::{FromSql, IsNull, ToSql, Type, accepts, to_sql_checked};
use risingwave_common_estimate_size::ZeroHeapSize;
use thiserror::Error;

use super::to_text::ToText;
use super::{CheckedAdd, DataType, Interval};
use crate::array::{ArrayError, ArrayResult};

/// The same as `NaiveDate::from_ymd(1970, 1, 1).num_days_from_ce()`.
/// Minus this magic number to store the number of days since 1970-01-01.
const UNIX_EPOCH_DAYS: i32 = 719_163;
const LEAP_DAYS: &[i32] = &[0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
const NORMAL_DAYS: &[i32] = &[0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

macro_rules! impl_chrono_wrapper {
    ($variant_name:ident, $chrono:ty, $pg_type:ident) => {
        #[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
        #[repr(transparent)]
        pub struct $variant_name(pub $chrono);

        impl $variant_name {
            pub const MIN: Self = Self(<$chrono>::MIN);

            pub fn new(data: $chrono) -> Self {
                $variant_name(data)
            }
        }

        impl Display for $variant_name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                ToText::write(self, f)
            }
        }

        impl From<$chrono> for $variant_name {
            fn from(data: $chrono) -> Self {
                $variant_name(data)
            }
        }

        impl ZeroHeapSize for $variant_name {}

        impl ToSql for $variant_name {
            accepts!($pg_type);

            to_sql_checked!();

            fn to_sql(
                &self,
                ty: &Type,
                out: &mut BytesMut,
            ) -> std::result::Result<IsNull, Box<dyn Error + Sync + Send>>
            where
                Self: Sized,
            {
                self.0.to_sql(ty, out)
            }
        }

        impl<'a> FromSql<'a> for $variant_name {
            fn from_sql(
                ty: &Type,
                raw: &'a [u8],
            ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
                let instant = <$chrono>::from_sql(ty, raw)?;
                Ok(Self::from(instant))
            }

            fn accepts(ty: &Type) -> bool {
                matches!(*ty, Type::$pg_type)
            }
        }
    };
}

impl_chrono_wrapper!(Date, NaiveDate, DATE);
impl_chrono_wrapper!(Timestamp, NaiveDateTime, TIMESTAMP);
impl_chrono_wrapper!(Time, NaiveTime, TIME);

/// Parse a date from varchar.
///
/// # Example
/// ```
/// use std::str::FromStr;
///
/// use risingwave_common::types::Date;
///
/// Date::from_str("1999-01-08").unwrap();
/// ```
impl FromStr for Date {
    type Err = InvalidParamsError;

    fn from_str(s: &str) -> Result<Self> {
        let date = speedate::Date::parse_str_rfc3339(s).map_err(|_| ErrorKind::ParseDate)?;
        Ok(Date::new(
            Date::from_ymd_uncheck(date.year as i32, date.month as u32, date.day as u32).0,
        ))
    }
}

/// Parse a time from varchar.
///
/// # Example
/// ```
/// use std::str::FromStr;
///
/// use risingwave_common::types::Time;
///
/// Time::from_str("04:05").unwrap();
/// Time::from_str("04:05:06").unwrap();
/// ```
impl FromStr for Time {
    type Err = InvalidParamsError;

    fn from_str(s: &str) -> Result<Self> {
        let s_without_zone = s.trim_end_matches('Z');
        let res = speedate::Time::parse_str(s_without_zone).map_err(|_| ErrorKind::ParseTime)?;
        Ok(Time::from_hms_micro_uncheck(
            res.hour as u32,
            res.minute as u32,
            res.second as u32,
            res.microsecond,
        ))
    }
}

/// Parse a timestamp from varchar.
///
/// # Example
/// ```
/// use std::str::FromStr;
///
/// use risingwave_common::types::Timestamp;
///
/// Timestamp::from_str("1999-01-08 04:02").unwrap();
/// Timestamp::from_str("1999-01-08 04:05:06").unwrap();
/// Timestamp::from_str("1999-01-08T04:05:06").unwrap();
/// ```
impl FromStr for Timestamp {
    type Err = InvalidParamsError;

    fn from_str(s: &str) -> Result<Self> {
        let dt = s
            .parse::<jiff::civil::DateTime>()
            .map_err(|_| ErrorKind::ParseTimestamp)?;
        Ok(
            Date::from_ymd_uncheck(dt.year() as i32, dt.month() as u32, dt.day() as u32)
                .and_hms_nano_uncheck(
                    dt.hour() as u32,
                    dt.minute() as u32,
                    dt.second() as u32,
                    dt.subsec_nanosecond() as u32,
                ),
        )
    }
}

/// In `PostgreSQL`, casting from timestamp to date discards the time part.
///
/// # Example
/// ```
/// use std::str::FromStr;
///
/// use risingwave_common::types::{Date, Timestamp};
///
/// let ts = Timestamp::from_str("1999-01-08 04:02").unwrap();
/// let date = Date::from(ts);
/// assert_eq!(date, Date::from_str("1999-01-08").unwrap());
/// ```
impl From<Timestamp> for Date {
    fn from(ts: Timestamp) -> Self {
        Date::new(ts.0.date())
    }
}

/// In `PostgreSQL`, casting from timestamp to time discards the date part.
///
/// # Example
/// ```
/// use std::str::FromStr;
///
/// use risingwave_common::types::{Time, Timestamp};
///
/// let ts = Timestamp::from_str("1999-01-08 04:02").unwrap();
/// let time = Time::from(ts);
/// assert_eq!(time, Time::from_str("04:02").unwrap());
/// ```
impl From<Timestamp> for Time {
    fn from(ts: Timestamp) -> Self {
        Time::new(ts.0.time())
    }
}

/// In `PostgreSQL`, casting from interval to time discards the days part.
///
/// # Example
/// ```
/// use std::str::FromStr;
///
/// use risingwave_common::types::{Interval, Time};
///
/// let interval = Interval::from_month_day_usec(1, 2, 61000003);
/// let time = Time::from(interval);
/// assert_eq!(time, Time::from_str("00:01:01.000003").unwrap());
///
/// let interval = Interval::from_month_day_usec(0, 0, -61000003);
/// let time = Time::from(interval);
/// assert_eq!(time, Time::from_str("23:58:58.999997").unwrap());
/// ```
impl From<Interval> for Time {
    fn from(interval: Interval) -> Self {
        let usecs = interval.usecs_of_day();
        let secs = (usecs / 1_000_000) as u32;
        let nano = (usecs % 1_000_000 * 1000) as u32;
        Time::from_num_seconds_from_midnight_uncheck(secs, nano)
    }
}

#[derive(Copy, Clone, Debug, Error)]
enum ErrorKind {
    #[error("Invalid date: days: {days}")]
    Date { days: i32 },
    #[error("Invalid time: secs: {secs}, nanoseconds: {nsecs}")]
    Time { secs: u32, nsecs: u32 },
    #[error("Invalid time: {value} {unit} is out of range for a time of day")]
    TimeOfDay { value: u64, unit: &'static str },
    #[error("Invalid datetime: seconds: {secs}, nanoseconds: {nsecs}")]
    DateTime { secs: i64, nsecs: u32 },
    #[error("Invalid datetime: {value} {unit} is out of range")]
    Timestamp { value: i64, unit: &'static str },
    #[error("Can't cast string to date (expected format is YYYY-MM-DD)")]
    ParseDate,
    #[error(
        "Can't cast string to time (expected format is HH:MM:SS[.D+{{up to 6 digits}}][Z] or HH:MM)"
    )]
    ParseTime,
    #[error(
        "Can't cast string to timestamp (expected format is YYYY-MM-DD HH:MM:SS[.D+{{up to 9 digits}}] or YYYY-MM-DD HH:MM or YYYY-MM-DD or ISO 8601 format)"
    )]
    ParseTimestamp,
}

#[derive(Debug, Error)]
#[error(transparent)]
pub struct InvalidParamsError(#[from] ErrorKind);

impl InvalidParamsError {
    pub fn date(days: i32) -> Self {
        ErrorKind::Date { days }.into()
    }

    pub fn time(secs: u32, nsecs: u32) -> Self {
        ErrorKind::Time { secs, nsecs }.into()
    }

    pub fn time_of_day(value: u64, unit: &'static str) -> Self {
        ErrorKind::TimeOfDay { value, unit }.into()
    }

    pub fn datetime(secs: i64, nsecs: u32) -> Self {
        ErrorKind::DateTime { secs, nsecs }.into()
    }

    pub fn timestamp(value: i64, unit: &'static str) -> Self {
        ErrorKind::Timestamp { value, unit }.into()
    }
}

impl From<InvalidParamsError> for ArrayError {
    fn from(e: InvalidParamsError) -> Self {
        ArrayError::internal(e)
    }
}

type Result<T> = std::result::Result<T, InvalidParamsError>;

impl ToText for Date {
    /// ```
    /// # use risingwave_common::types::Date;
    /// let date = Date::from_ymd_uncheck(2001, 5, 16);
    /// assert_eq!(date.to_string(), "2001-05-16");
    ///
    /// let date = Date::from_ymd_uncheck(1, 10, 26);
    /// assert_eq!(date.to_string(), "0001-10-26");
    ///
    /// let date = Date::from_ymd_uncheck(0, 10, 26);
    /// assert_eq!(date.to_string(), "0001-10-26 BC");
    /// ```
    fn write<W: std::fmt::Write>(&self, f: &mut W) -> std::fmt::Result {
        let (ce, year) = self.0.year_ce();
        let suffix = if ce { "" } else { " BC" };
        write!(
            f,
            "{:04}-{:02}-{:02}{}",
            year,
            self.0.month(),
            self.0.day(),
            suffix
        )
    }

    fn write_with_type<W: std::fmt::Write>(&self, ty: &DataType, f: &mut W) -> std::fmt::Result {
        match ty {
            super::DataType::Date => self.write(f),
            _ => unreachable!(),
        }
    }
}

impl ToText for Time {
    fn write<W: std::fmt::Write>(&self, f: &mut W) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }

    fn write_with_type<W: std::fmt::Write>(&self, ty: &DataType, f: &mut W) -> std::fmt::Result {
        match ty {
            super::DataType::Time => self.write(f),
            _ => unreachable!(),
        }
    }
}

impl ToText for Timestamp {
    fn write<W: std::fmt::Write>(&self, f: &mut W) -> std::fmt::Result {
        let (ce, year) = self.0.year_ce();
        let suffix = if ce { "" } else { " BC" };
        write!(
            f,
            "{:04}-{:02}-{:02} {}{}",
            year,
            self.0.month(),
            self.0.day(),
            self.0.time(),
            suffix
        )
    }

    fn write_with_type<W: std::fmt::Write>(&self, ty: &DataType, f: &mut W) -> std::fmt::Result {
        match ty {
            super::DataType::Timestamp => self.write(f),
            _ => unreachable!(),
        }
    }
}

impl Date {
    pub fn with_days_since_ce(days: i32) -> Result<Self> {
        Ok(Date::new(
            NaiveDate::from_num_days_from_ce_opt(days)
                .ok_or_else(|| InvalidParamsError::date(days))?,
        ))
    }

    pub fn with_days_since_unix_epoch(days: i32) -> Result<Self> {
        Ok(Date::new(
            NaiveDate::from_num_days_from_ce_opt(days)
                .ok_or_else(|| InvalidParamsError::date(days))?
                .checked_add_days(Days::new(UNIX_EPOCH_DAYS as u64))
                .ok_or_else(|| InvalidParamsError::date(days))?,
        ))
    }

    pub fn get_nums_days_unix_epoch(&self) -> i32 {
        self.0
            .checked_sub_days(Days::new(UNIX_EPOCH_DAYS as u64))
            .unwrap()
            .num_days_from_ce()
    }

    pub fn from_protobuf(cur: &mut Cursor<&[u8]>) -> ArrayResult<Date> {
        let days = cur
            .read_i32::<BigEndian>()
            .context("failed to read i32 from Date buffer")?;

        Ok(Date::with_days_since_ce(days)?)
    }

    pub fn to_protobuf<T: Write>(self, output: &mut T) -> ArrayResult<usize> {
        output
            .write(&(self.0.num_days_from_ce()).to_be_bytes())
            .map_err(Into::into)
    }

    pub fn from_ymd_uncheck(year: i32, month: u32, day: u32) -> Self {
        Self::new(NaiveDate::from_ymd_opt(year, month, day).unwrap())
    }

    pub fn from_num_days_from_ce_uncheck(days: i32) -> Self {
        Self::with_days_since_ce(days).unwrap()
    }

    pub fn and_hms_uncheck(self, hour: u32, min: u32, sec: u32) -> Timestamp {
        self.and_hms_micro_uncheck(hour, min, sec, 0)
    }

    pub fn and_hms_micro_uncheck(self, hour: u32, min: u32, sec: u32, micro: u32) -> Timestamp {
        Timestamp::new(
            self.0
                .and_time(Time::from_hms_micro_uncheck(hour, min, sec, micro).0),
        )
    }

    pub fn and_hms_nano_uncheck(self, hour: u32, min: u32, sec: u32, nano: u32) -> Timestamp {
        Timestamp::new(
            self.0
                .and_time(Time::from_hms_nano_uncheck(hour, min, sec, nano).0),
        )
    }
}

/// Exclusive upper bounds of a time of day.
const NANOS_PER_DAY: u64 = 86_400 * 1_000_000_000;
const MICROS_PER_DAY: u64 = 86_400 * 1_000_000;

impl Time {
    pub fn with_secs_nano(secs: u32, nano: u32) -> Result<Self> {
        Ok(Time::new(
            NaiveTime::from_num_seconds_from_midnight_opt(secs, nano)
                .ok_or_else(|| InvalidParamsError::time(secs, nano))?,
        ))
    }

    pub fn from_protobuf(cur: &mut Cursor<&[u8]>) -> ArrayResult<Time> {
        let nano = cur
            .read_u64::<BigEndian>()
            .context("failed to read u64 from Time buffer")?;

        Ok(Time::with_nano(nano)?)
    }

    pub fn to_protobuf<T: Write>(self, output: &mut T) -> ArrayResult<usize> {
        output
            .write(&self.nanos_of_day().to_be_bytes())
            .map_err(Into::into)
    }

    pub fn with_nano(nano: u64) -> Result<Self> {
        // Rejecting out-of-day values here also keeps the casts below lossless.
        if nano >= NANOS_PER_DAY {
            return Err(InvalidParamsError::time_of_day(nano, "nanoseconds"));
        }
        Self::with_secs_nano((nano / 1_000_000_000) as u32, (nano % 1_000_000_000) as u32)
    }

    pub fn with_micro(micro: u64) -> Result<Self> {
        if micro >= MICROS_PER_DAY {
            return Err(InvalidParamsError::time_of_day(micro, "microseconds"));
        }
        Self::with_secs_nano(
            (micro / 1_000_000) as u32,
            ((micro % 1_000_000) * 1_000) as u32,
        )
    }

    /// Nanoseconds since midnight, the inverse of [`Time::with_nano`].
    pub fn nanos_of_day(self) -> u64 {
        self.0.num_seconds_from_midnight() as u64 * 1_000_000_000 + self.0.nanosecond() as u64
    }

    /// Microseconds since midnight, truncating sub-microsecond precision.
    pub fn micros_of_day(self) -> u64 {
        self.0.num_seconds_from_midnight() as u64 * 1_000_000 + self.0.nanosecond() as u64 / 1_000
    }

    pub fn with_milli(milli: u32) -> Result<Self> {
        let secs = milli / 1_000;
        let nano = (milli % 1_000) * 1_000_000;
        Self::with_secs_nano(secs, nano)
    }

    pub fn from_hms_uncheck(hour: u32, min: u32, sec: u32) -> Self {
        Self::from_hms_nano_uncheck(hour, min, sec, 0)
    }

    pub fn from_hms_micro_uncheck(hour: u32, min: u32, sec: u32, micro: u32) -> Self {
        Self::new(NaiveTime::from_hms_micro_opt(hour, min, sec, micro).unwrap())
    }

    pub fn from_hms_nano_uncheck(hour: u32, min: u32, sec: u32, nano: u32) -> Self {
        Self::new(NaiveTime::from_hms_nano_opt(hour, min, sec, nano).unwrap())
    }

    pub fn from_num_seconds_from_midnight_uncheck(secs: u32, nano: u32) -> Self {
        Self::new(NaiveTime::from_num_seconds_from_midnight_opt(secs, nano).unwrap())
    }
}

// The first 64 bits of protobuf encoding for `Timestamp` type has 2 possible meanings.
// * When the highest 2 bits are `11` or `00` (i.e. values ranging from `0b1100...00` to `0b0011..11`),
//   it is *microseconds* since 1970-01-01 midnight. 2^62 microseconds covers 146235 years.
// * When the highest 2 bits are `10` or `01`, we flip the second bit to get values from `0b1100...00` to `0b0011..11` again.
//   It is *seconds* since 1970-01-01 midnight. It is then followed by another 32 bits as nanoseconds within a second.
// Since timestamp is negative when it is less than 1970-1-1, you need to take both cases into account(`11+00`` or `01+10``).
enum FirstI64 {
    V0 { usecs: i64 },
    V1 { secs: i64 },
}
impl FirstI64 {
    pub fn to_protobuf(&self) -> i64 {
        match self {
            FirstI64::V0 { usecs } => *usecs,
            FirstI64::V1 { secs } => secs ^ (0b01 << 62),
        }
    }

    pub fn from_protobuf(cur: &mut Cursor<&[u8]>) -> ArrayResult<FirstI64> {
        let value = cur
            .read_i64::<BigEndian>()
            .context("failed to read i64 from Time buffer")?;
        if Self::is_v1_format_state(value) {
            let secs = value ^ (0b01 << 62);
            Ok(FirstI64::V1 { secs })
        } else {
            Ok(FirstI64::V0 { usecs: value })
        }
    }

    fn is_v1_format_state(value: i64) -> bool {
        let state = (value >> 62) & 0b11;
        state == 0b10 || state == 0b01
    }
}

impl Timestamp {
    pub fn with_secs_nsecs(secs: i64, nsecs: u32) -> Result<Self> {
        DateTime::from_timestamp(secs, nsecs)
            .map(|t| Timestamp(t.naive_utc()))
            .ok_or_else(|| InvalidParamsError::datetime(secs, nsecs))
    }

    pub fn from_protobuf(cur: &mut Cursor<&[u8]>) -> ArrayResult<Timestamp> {
        match FirstI64::from_protobuf(cur)? {
            FirstI64::V0 { usecs } => Ok(Timestamp::with_micros(usecs)?),
            FirstI64::V1 { secs } => {
                let nsecs = cur
                    .read_u32::<BigEndian>()
                    .context("failed to read u32 from Time buffer")?;
                Ok(Timestamp::with_secs_nsecs(secs, nsecs)?)
            }
        }
    }

    // Since timestamp secs is much smaller than i64, we use the highest 2 bit to store the format information, which is compatible with the old format.
    // New format: secs(i64) + nsecs(u32)
    // Old format: micros(i64)
    pub fn to_protobuf<T: Write>(self, output: &mut T) -> ArrayResult<usize> {
        let timestamp_size = output
            .write(
                &(FirstI64::V1 {
                    secs: self.0.and_utc().timestamp(),
                }
                .to_protobuf())
                .to_be_bytes(),
            )
            .map_err(Into::<ArrayError>::into)?;
        let timestamp_subsec_nanos_size = output
            .write(&(self.0.and_utc().timestamp_subsec_nanos()).to_be_bytes())
            .map_err(Into::<ArrayError>::into)?;
        Ok(timestamp_subsec_nanos_size + timestamp_size)
    }

    pub fn get_timestamp_nanos(&self) -> i64 {
        self.0.and_utc().timestamp_nanos_opt().unwrap()
    }

    pub fn with_millis(timestamp_millis: i64) -> Result<Self> {
        DateTime::from_timestamp_millis(timestamp_millis)
            .map(|t| Timestamp(t.naive_utc()))
            .ok_or_else(|| InvalidParamsError::timestamp(timestamp_millis, "milliseconds"))
    }

    pub fn with_micros(timestamp_micros: i64) -> Result<Self> {
        DateTime::from_timestamp_micros(timestamp_micros)
            .map(|t| Timestamp(t.naive_utc()))
            .ok_or_else(|| InvalidParamsError::timestamp(timestamp_micros, "microseconds"))
    }

    /// An `i64` nanosecond count is always representable, so this cannot fail.
    pub fn with_nanos(timestamp_nanos: i64) -> Self {
        Timestamp(DateTime::from_timestamp_nanos(timestamp_nanos).naive_utc())
    }

    pub fn from_timestamp_uncheck(secs: i64, nsecs: u32) -> Self {
        Self::new(DateTime::from_timestamp(secs, nsecs).unwrap().naive_utc())
    }

    /// Truncate the timestamp to the precision of microseconds.
    ///
    /// # Example
    /// ```
    /// # use risingwave_common::types::Timestamp;
    /// let ts = "2001-05-16T20:38:40.123456789".parse().unwrap();
    /// assert_eq!(
    ///     Timestamp::new(ts).truncate_micros().to_string(),
    ///     "2001-05-16 20:38:40.123456"
    /// );
    /// ```
    pub fn truncate_micros(self) -> Self {
        Self::new(
            self.0
                .with_nanosecond(self.0.nanosecond() / 1000 * 1000)
                .unwrap(),
        )
    }

    /// Truncate the timestamp to the precision of milliseconds.
    ///
    /// # Example
    /// ```
    /// # use risingwave_common::types::Timestamp;
    /// let ts = "2001-05-16T20:38:40.123456789".parse().unwrap();
    /// assert_eq!(
    ///     Timestamp::new(ts).truncate_millis().to_string(),
    ///     "2001-05-16 20:38:40.123"
    /// );
    /// ```
    pub fn truncate_millis(self) -> Self {
        Self::new(
            self.0
                .with_nanosecond(self.0.nanosecond() / 1_000_000 * 1_000_000)
                .unwrap(),
        )
    }

    /// Truncate the timestamp to the precision of seconds.
    ///
    /// # Example
    /// ```
    /// # use risingwave_common::types::Timestamp;
    /// let ts = "2001-05-16T20:38:40.123456789".parse().unwrap();
    /// assert_eq!(
    ///     Timestamp::new(ts).truncate_second().to_string(),
    ///     "2001-05-16 20:38:40"
    /// );
    /// ```
    pub fn truncate_second(self) -> Self {
        Self::new(self.0.with_nanosecond(0).unwrap())
    }

    /// Truncate the timestamp to the precision of minutes.
    ///
    /// # Example
    /// ```
    /// # use risingwave_common::types::Timestamp;
    /// let ts = "2001-05-16T20:38:40.123456789".parse().unwrap();
    /// assert_eq!(
    ///     Timestamp::new(ts).truncate_minute().to_string(),
    ///     "2001-05-16 20:38:00"
    /// );
    /// ```
    pub fn truncate_minute(self) -> Self {
        Date::new(self.0.date()).and_hms_uncheck(self.0.hour(), self.0.minute(), 0)
    }

    /// Truncate the timestamp to the precision of hours.
    ///
    /// # Example
    /// ```
    /// # use risingwave_common::types::Timestamp;
    /// let ts = "2001-05-16T20:38:40.123456789".parse().unwrap();
    /// assert_eq!(
    ///     Timestamp::new(ts).truncate_hour().to_string(),
    ///     "2001-05-16 20:00:00"
    /// );
    /// ```
    pub fn truncate_hour(self) -> Self {
        Date::new(self.0.date()).and_hms_uncheck(self.0.hour(), 0, 0)
    }

    /// Truncate the timestamp to the precision of days.
    ///
    /// # Example
    /// ```
    /// # use risingwave_common::types::Timestamp;
    /// let ts = "2001-05-16T20:38:40.123456789".parse().unwrap();
    /// assert_eq!(
    ///     Timestamp::new(ts).truncate_day().to_string(),
    ///     "2001-05-16 00:00:00"
    /// );
    /// ```
    pub fn truncate_day(self) -> Self {
        Date::new(self.0.date()).into()
    }

    /// Truncate the timestamp to the precision of weeks.
    ///
    /// # Example
    /// ```
    /// # use risingwave_common::types::Timestamp;
    /// let ts = "2001-05-16T20:38:40.123456789".parse().unwrap();
    /// assert_eq!(
    ///     Timestamp::new(ts).truncate_week().to_string(),
    ///     "2001-05-14 00:00:00"
    /// );
    /// ```
    pub fn truncate_week(self) -> Self {
        Date::new(self.0.date().week(Weekday::Mon).first_day()).into()
    }

    /// Truncate the timestamp to the precision of months.
    ///
    /// # Example
    /// ```
    /// # use risingwave_common::types::Timestamp;
    /// let ts = "2001-05-16T20:38:40.123456789".parse().unwrap();
    /// assert_eq!(
    ///     Timestamp::new(ts).truncate_month().to_string(),
    ///     "2001-05-01 00:00:00"
    /// );
    /// ```
    pub fn truncate_month(self) -> Self {
        Date::new(self.0.date().with_day(1).unwrap()).into()
    }

    /// Truncate the timestamp to the precision of quarters.
    ///
    /// # Example
    /// ```
    /// # use risingwave_common::types::Timestamp;
    /// let ts = "2001-05-16T20:38:40.123456789".parse().unwrap();
    /// assert_eq!(
    ///     Timestamp::new(ts).truncate_quarter().to_string(),
    ///     "2001-04-01 00:00:00"
    /// );
    /// ```
    pub fn truncate_quarter(self) -> Self {
        Date::from_ymd_uncheck(self.0.year(), self.0.month0() / 3 * 3 + 1, 1).into()
    }

    /// Truncate the timestamp to the precision of years.
    ///
    /// # Example
    /// ```
    /// # use risingwave_common::types::Timestamp;
    /// let ts = "2001-05-16T20:38:40.123456789".parse().unwrap();
    /// assert_eq!(
    ///     Timestamp::new(ts).truncate_year().to_string(),
    ///     "2001-01-01 00:00:00"
    /// );
    /// ```
    pub fn truncate_year(self) -> Self {
        Date::from_ymd_uncheck(self.0.year(), 1, 1).into()
    }

    /// Truncate the timestamp to the precision of decades.
    ///
    /// # Example
    /// ```
    /// # use risingwave_common::types::Timestamp;
    /// let ts = "2001-05-16T20:38:40.123456789".parse().unwrap();
    /// assert_eq!(
    ///     Timestamp::new(ts).truncate_decade().to_string(),
    ///     "2000-01-01 00:00:00"
    /// );
    /// ```
    pub fn truncate_decade(self) -> Self {
        Date::from_ymd_uncheck(self.0.year() / 10 * 10, 1, 1).into()
    }

    /// Truncate the timestamp to the precision of centuries.
    ///
    /// # Example
    /// ```
    /// # use risingwave_common::types::Timestamp;
    /// let ts = "3202-05-16T20:38:40.123456789".parse().unwrap();
    /// assert_eq!(
    ///     Timestamp::new(ts).truncate_century().to_string(),
    ///     "3201-01-01 00:00:00"
    /// );
    /// ```
    pub fn truncate_century(self) -> Self {
        Date::from_ymd_uncheck((self.0.year() - 1) / 100 * 100 + 1, 1, 1).into()
    }

    /// Truncate the timestamp to the precision of millenniums.
    ///
    /// # Example
    /// ```
    /// # use risingwave_common::types::Timestamp;
    /// let ts = "3202-05-16T20:38:40.123456789".parse().unwrap();
    /// assert_eq!(
    ///     Timestamp::new(ts).truncate_millennium().to_string(),
    ///     "3001-01-01 00:00:00"
    /// );
    /// ```
    pub fn truncate_millennium(self) -> Self {
        Date::from_ymd_uncheck((self.0.year() - 1) / 1000 * 1000 + 1, 1, 1).into()
    }
}

impl From<Date> for Timestamp {
    fn from(date: Date) -> Self {
        date.and_hms_uncheck(0, 0, 0)
    }
}

/// return the days of the `year-month`
fn get_mouth_days(year: i32, month: usize) -> i32 {
    if is_leap_year(year) {
        LEAP_DAYS[month]
    } else {
        NORMAL_DAYS[month]
    }
}

fn is_leap_year(year: i32) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

impl CheckedAdd<Interval> for Timestamp {
    type Output = Timestamp;

    fn checked_add(self, rhs: Interval) -> Option<Timestamp> {
        let mut date = self.0.date();
        if rhs.months() != 0 {
            // NaiveDate don't support add months. We need calculate manually
            let mut day = date.day() as i32;
            let mut month = date.month() as i32;
            let mut year = date.year();
            // Calculate the number of year in this interval
            let interval_months = rhs.months();
            let year_diff = interval_months / 12;
            year += year_diff;

            // Calculate the number of month in this interval except the added year
            // The range of month_diff is (-12, 12) (The month is negative when the interval is
            // negative)
            let month_diff = interval_months - year_diff * 12;
            // The range of new month is (-12, 24) ( original month:[1, 12] + month_diff:(-12, 12) )
            month += month_diff;
            // Process the overflow months
            if month > 12 {
                year += 1;
                month -= 12;
            } else if month <= 0 {
                year -= 1;
                month += 12;
            }

            // Fix the days after changing date.
            // For example, 1970.1.31 + 1 month = 1970.2.28
            day = day.min(get_mouth_days(year, month as usize));
            date = NaiveDate::from_ymd_opt(year, month as u32, day as u32)?;
        }
        let mut datetime = NaiveDateTime::new(date, self.0.time());
        datetime = datetime.checked_add_signed(Duration::days(rhs.days().into()))?;
        datetime = datetime.checked_add_signed(Duration::microseconds(rhs.usecs()))?;

        Some(Timestamp::new(datetime))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse() {
        assert_eq!(
            Timestamp::from_str("2022-08-03T10:34:02").unwrap(),
            Timestamp::from_str("2022-08-03 10:34:02").unwrap()
        );
        let ts = Timestamp::from_str("0001-11-15 07:35:40.999999").unwrap();
        assert_eq!(ts.0.and_utc().timestamp_micros(), -62108094259000001);

        let ts = Timestamp::from_str("1969-12-31 23:59:59.999999").unwrap();
        assert_eq!(ts.0.and_utc().timestamp_micros(), -1);

        // invalid datetime
        Date::from_str("1999-01-08AA").unwrap_err();
        Time::from_str("AA04:05:06").unwrap_err();
        Timestamp::from_str("1999-01-08 04:05:06AA").unwrap_err();
    }

    #[test]
    fn time_of_day_bounds() {
        assert_eq!(
            Time::with_micro(86_399_999_999).unwrap(),
            Time::from_hms_micro_uncheck(23, 59, 59, 999_999)
        );
        assert_eq!(
            Time::with_nano(86_399_999_999_999).unwrap(),
            Time::from_hms_nano_uncheck(23, 59, 59, 999_999_999)
        );

        Time::with_micro(86_400_000_000).unwrap_err();
        Time::with_nano(86_400_000_000_000).unwrap_err();

        // A seconds count at a multiple of 2^32 used to wrap into the valid range.
        Time::with_micro(4_294_967_296_000_000).unwrap_err();
        Time::with_nano(4_294_967_296_000_000_000).unwrap_err();
    }
}
