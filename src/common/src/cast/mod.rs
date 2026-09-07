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

use chrono::NaiveDateTime;
use itertools::Itertools;
use thiserror_ext::AsReport;

use crate::types::{Timestamp, Timestamptz};

type Result<T> = std::result::Result<T, String>;

pub const PARSE_ERROR_STR_TO_BYTEA: &str = "Invalid Bytea syntax";

/// Parse a string into a bool.
///
/// See `https://www.postgresql.org/docs/9.5/datatype-boolean.html`
pub fn str_to_bool(input: &str) -> Result<bool> {
    /// String literals for bool type.
    const TRUE_BOOL_LITERALS: [&str; 9] = ["true", "tru", "tr", "t", "on", "1", "yes", "ye", "y"];
    const FALSE_BOOL_LITERALS: [&str; 10] = [
        "false", "fals", "fal", "fa", "f", "off", "of", "0", "no", "n",
    ];

    let trimmed_input = input.trim();
    if TRUE_BOOL_LITERALS
        .iter()
        .any(|s| s.eq_ignore_ascii_case(trimmed_input))
    {
        Ok(true)
    } else if FALSE_BOOL_LITERALS
        .iter()
        .any(|s| trimmed_input.eq_ignore_ascii_case(s))
    {
        Ok(false)
    } else {
        Err("Invalid bool".into())
    }
}

/// Converts UNIX epoch time to timestamptz.
///
/// The input UNIX epoch time is interpreted as follows:
///
/// - [0, 1e11) are assumed to be in seconds.
/// - [1e11, 1e14) are assumed to be in milliseconds.
/// - [1e14, 1e17) are assumed to be in microseconds.
/// - [1e17, upper) are assumed to be in nanoseconds.
///
/// This would cause no problem for timestamp in [1973-03-03 09:46:40, 5138-11-16 09:46:40).
#[inline]
pub fn i64_to_timestamptz(t: i64) -> Result<Timestamptz> {
    const E11: u64 = 100_000_000_000;
    const E14: u64 = 100_000_000_000_000;
    const E17: u64 = 100_000_000_000_000_000;
    match t.abs_diff(0) {
        0..E11 => Ok(Timestamptz::from_secs(t).unwrap()), // s
        E11..E14 => Ok(Timestamptz::from_millis(t).unwrap()), // ms
        E14..E17 => Ok(Timestamptz::from_micros(t).unwrap()), // us
        E17.. => Ok(Timestamptz::from_micros(t / 1000).unwrap()), // ns
    }
}

/// Converts UNIX epoch time to timestamp.
///
/// The input UNIX epoch time is interpreted as follows:
///
/// - [0, 1e11) are assumed to be in seconds.
/// - [1e11, 1e14) are assumed to be in milliseconds.
/// - [1e14, 1e17) are assumed to be in microseconds.
/// - [1e17, upper) are assumed to be in nanoseconds.
///
/// This would cause no problem for timestamp in [1973-03-03 09:46:40, 5138-11-16 09:46:40).
///
/// # Example
/// ```
/// # use risingwave_common::cast::i64_to_timestamp;
/// assert_eq!(
///     i64_to_timestamp(1_666_666_666).unwrap().to_string(),
///     "2022-10-25 02:57:46"
/// );
/// assert_eq!(
///     i64_to_timestamp(1_666_666_666_666).unwrap().to_string(),
///     "2022-10-25 02:57:46.666"
/// );
/// assert_eq!(
///     i64_to_timestamp(1_666_666_666_666_666).unwrap().to_string(),
///     "2022-10-25 02:57:46.666666"
/// );
/// assert_eq!(
///     i64_to_timestamp(1_666_666_666_666_666_666)
///         .unwrap()
///         .to_string(),
///     // note that we only support microseconds precision
///     "2022-10-25 02:57:46.666666"
/// );
/// ```
#[inline]
pub fn i64_to_timestamp(t: i64) -> Result<Timestamp> {
    let tz = i64_to_timestamptz(t)?;
    Ok(Timestamp::from_timestamp_uncheck(
        tz.timestamp(),
        tz.timestamp_subsec_nanos(),
    ))
}

/// Refer to PostgreSQL's implementation <https://github.com/postgres/postgres/blob/5cb54fc310fb84287cbdc74533f3420490a2f63a/src/backend/utils/adt/varlena.c#L276-L288>
pub fn str_to_bytea(elem: &str, writer: &mut impl std::io::Write) -> Result<()> {
    if let Some(remainder) = elem.strip_prefix(r"\x") {
        Ok(parse_bytes_hex(remainder, writer)?)
    } else {
        Ok(parse_bytes_traditional(elem, writer)?)
    }
}

/// Ref: <https://docs.rs/hex/0.4.3/src/hex/lib.rs.html#175-185>
fn get_hex(c: u8) -> Result<u8> {
    match c {
        b'A'..=b'F' => Ok(c - b'A' + 10),
        b'a'..=b'f' => Ok(c - b'a' + 10),
        b'0'..=b'9' => Ok(c - b'0'),
        _ => Err(format!("invalid hexadecimal digit: \"{}\"", c as char)),
    }
}

/// Refer to <https://www.postgresql.org/docs/current/datatype-binary.html#id-1.5.7.12.10> for specification.
pub fn parse_bytes_hex(s: &str, writer: &mut impl std::io::Write) -> Result<()> {
    let mut bytes = s.bytes();
    while let Some(c) = bytes.next() {
        // white spaces are tolerated
        if c == b' ' || c == b'\n' || c == b'\t' || c == b'\r' {
            continue;
        }
        let v1 = get_hex(c)?;

        match bytes.next() {
            Some(c) => {
                let v2 = get_hex(c)?;
                writer
                    .write_all(&[(v1 << 4) | v2])
                    .map_err(|e| e.to_report_string())?;
            }
            None => return Err("invalid hexadecimal data: odd number of digits".to_owned()),
        }
    }

    Ok(())
}

/// Refer to <https://www.postgresql.org/docs/current/datatype-binary.html#id-1.5.7.12.10> for specification.
pub fn parse_bytes_traditional(s: &str, writer: &mut impl std::io::Write) -> Result<()> {
    let mut bytes = s.bytes();

    while let Some(b) = bytes.next() {
        if b != b'\\' {
            writer.write_all(&[b]).map_err(|e| e.to_report_string())?;
        } else {
            match bytes.next() {
                Some(b'\\') => {
                    writer.write_all(b"\\").map_err(|e| e.to_report_string())?;
                }
                Some(b1 @ b'0'..=b'3') => match bytes.next_tuple() {
                    Some((b2 @ b'0'..=b'7', b3 @ b'0'..=b'7')) => {
                        let byte = (b1 - b'0') << 6 | (b2 - b'0') << 3 | (b3 - b'0');
                        writer
                            .write_all(&[byte])
                            .map_err(|e| e.to_report_string())?;
                    }
                    _ => {
                        // one backslash, not followed by another or ### valid octal
                        return Err("invalid input syntax for type bytea".to_owned());
                    }
                },
                _ => {
                    // one backslash, not followed by another or ### valid octal
                    return Err("invalid input syntax for type bytea".to_owned());
                }
            }
        }
    }

    Ok(())
}

pub fn datetime_to_timestamp_millis(datetime: NaiveDateTime) -> i64 {
    datetime.and_utc().timestamp_millis()
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};

    use super::*;

    #[test]
    fn test_negative_int_to_timestamptz() {
        let x = i64_to_timestamptz(-2208988800000000000)
            .unwrap()
            .to_datetime_utc();
        let ans: DateTime<Utc> = "1900-01-01T00:00:00Z".parse().unwrap();
        assert_eq!(x, ans);
    }

    #[test]
    fn test_bytea() {
        use crate::types::ToText;

        fn str_to_bytea(s: &str) -> Result<Box<[u8]>> {
            let mut buf = Vec::new();
            super::str_to_bytea(s, &mut buf)?;
            Ok(buf.into())
        }

        assert_eq!(str_to_bytea("fgo").unwrap().as_ref().to_text(), r"\x66676f");
        assert_eq!(
            str_to_bytea(r"\xDeadBeef").unwrap().as_ref().to_text(),
            r"\xdeadbeef"
        );
        assert_eq!(
            str_to_bytea("12CD").unwrap().as_ref().to_text(),
            r"\x31324344"
        );
        assert_eq!(
            str_to_bytea("1234").unwrap().as_ref().to_text(),
            r"\x31323334"
        );
        assert_eq!(
            str_to_bytea(r"\x12CD").unwrap().as_ref().to_text(),
            r"\x12cd"
        );
        assert_eq!(
            str_to_bytea(r"\x De Ad Be Ef ").unwrap().as_ref().to_text(),
            r"\xdeadbeef"
        );
        assert_eq!(
            str_to_bytea("x De Ad Be Ef ").unwrap().as_ref().to_text(),
            r"\x7820446520416420426520456620"
        );
        assert_eq!(
            str_to_bytea(r"De\\123dBeEf").unwrap().as_ref().to_text(),
            r"\x44655c3132336442654566"
        );
        assert_eq!(
            str_to_bytea(r"De\123dBeEf").unwrap().as_ref().to_text(),
            r"\x4465536442654566"
        );
        assert_eq!(
            str_to_bytea(r"De\\000dBeEf").unwrap().as_ref().to_text(),
            r"\x44655c3030306442654566"
        );

        assert_eq!(str_to_bytea(r"\123").unwrap().as_ref().to_text(), r"\x53");
        assert_eq!(str_to_bytea(r"\\").unwrap().as_ref().to_text(), r"\x5c");
        assert_eq!(
            str_to_bytea(r"123").unwrap().as_ref().to_text(),
            r"\x313233"
        );
        assert_eq!(
            str_to_bytea(r"\\123").unwrap().as_ref().to_text(),
            r"\x5c313233"
        );

        assert!(str_to_bytea(r"\1").is_err());
        assert!(str_to_bytea(r"\12").is_err());
        assert!(str_to_bytea(r"\400").is_err());
        assert!(str_to_bytea(r"\378").is_err());
        assert!(str_to_bytea(r"\387").is_err());
        assert!(str_to_bytea(r"\377").is_ok());
    }
}
