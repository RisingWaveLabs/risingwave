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

use std::borrow::Cow;

use const_currying::const_currying;
use itertools::Itertools;
use risingwave_common::util::recursive::{self, Recurse};
use risingwave_expr::function;

use crate::{ExprError, Result};

/// Converts a SQL `LIKE` pattern to the internal backslash escape convention.
///
/// This mirrors PostgreSQL's `do_like_escape`: `ESCAPE ''` disables escaping and
/// doubles backslashes, `ESCAPE '\'` leaves the pattern unchanged, and any other
/// escape character is rewritten to the internal `\` escape marker.
fn normalize_pattern(p: &str, escape: Option<u8>) -> Cow<'_, [u8]> {
    if escape == Some(b'\\') {
        return Cow::Borrowed(p.as_bytes());
    }

    let mut normalized = Vec::with_capacity(p.len());
    let mut after_escape = false;

    for c in p.bytes() {
        if Some(c) == escape && !after_escape {
            normalized.push(b'\\');
            after_escape = true;
        } else if c == b'\\' {
            normalized.push(b'\\');
            if !after_escape {
                normalized.push(b'\\');
            }
            after_escape = false;
        } else {
            normalized.push(c);
            after_escape = false;
        }
    }

    Cow::Owned(normalized)
}

fn like_impl_inner<const CASE_INSENSITIVE: bool>(
    s: &str,
    p: &str,
    escape: Option<u8>,
) -> Result<bool> {
    let pattern = normalize_pattern(p, escape);
    Ok(match_text::<CASE_INSENSITIVE>(s.as_bytes(), pattern.as_ref())?.into_bool())
}

#[const_currying]
fn like_impl<const CASE_INSENSITIVE: bool>(
    s: &str,
    p: &str,
    #[maybe_const(consts = [b'\\'])] escape: u8,
) -> Result<bool> {
    like_impl_inner::<CASE_INSENSITIVE>(s, p, Some(escape))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchResult {
    True,
    False,
    Abort,
}

// PostgreSQL checks the remaining native stack space with `check_stack_depth`.
// Use a conservative recursion limit because Rust does not expose an equivalent
// portable stack-depth check.
const MAX_RECURSION_DEPTH: usize = 4096;

impl MatchResult {
    #[inline]
    fn into_bool(self) -> bool {
        matches!(self, MatchResult::True)
    }
}

#[inline]
fn normalize<const CASE_INSENSITIVE: bool>(b: u8) -> u8 {
    if CASE_INSENSITIVE {
        b.to_ascii_lowercase()
    } else {
        b
    }
}

#[inline]
fn byte_eq<const CASE_INSENSITIVE: bool>(a: u8, b: u8) -> bool {
    normalize::<CASE_INSENSITIVE>(a) == normalize::<CASE_INSENSITIVE>(b)
}

pub fn match_text<const CASE_INSENSITIVE: bool>(
    text: &[u8],
    pattern: &[u8],
) -> Result<MatchResult, ExprError> {
    recursive::tracker!().recurse(|tracker| {
        if tracker.depth() > MAX_RECURSION_DEPTH {
            return Err(ExprError::InvalidParam {
                name: "pattern",
                reason: "stack depth limit exceeded".into(),
            });
        }

        match_text_inner::<CASE_INSENSITIVE>(text, pattern)
    })
}

fn match_text_inner<const CASE_INSENSITIVE: bool>(
    mut text: &[u8],
    mut pattern: &[u8],
) -> Result<MatchResult, ExprError> {
    // Fast path for match-everything pattern
    if pattern == b"%" {
        return Ok(MatchResult::True);
    }

    while let (Some((&p, rest_pattern)), Some((&t, rest_text))) =
        (pattern.split_first(), text.split_first())
    {
        match p {
            b'%' => {
                pattern = rest_pattern;

                while let Some((&p, rest_pattern)) = pattern.split_first() {
                    match p {
                        b'%' => {
                            // N's % == one's %
                            pattern = rest_pattern;
                        }
                        b'_' => {
                            let Some((_, rest_text)) = text.split_first() else {
                                // If not enough text left to match the pattern, ABORT
                                return Ok(MatchResult::Abort);
                            };

                            text = rest_text;
                            pattern = rest_pattern;
                        }
                        _ => break, // Reached a non-wildcard pattern char
                    }
                }

                // If we're at end of pattern, match: we have a trailing % which
                // matches any remaining text string.
                let Some((&first, _)) = pattern.split_first() else {
                    return Ok(MatchResult::True);
                };

                // Get the first literal character in the remaining pattern for fast text scanning.
                let first = if first == b'\\' {
                    let Some(&escaped) = pattern.get(1) else {
                        return Err(ExprError::InvalidParam {
                            name: "pattern",
                            reason: "LIKE pattern must not end with escape character".into(),
                        });
                    };
                    escaped
                } else {
                    first
                };

                while let Some((&t, rest_text)) = text.split_first() {
                    if byte_eq::<CASE_INSENSITIVE>(t, first) {
                        let matched = match_text::<CASE_INSENSITIVE>(text, pattern)?;

                        if matched != MatchResult::False {
                            return Ok(matched); /* TRUE or ABORT */
                        }
                    }

                    text = rest_text;
                }

                // End of text with no match, so no point in trying later places
                // to start matching this pattern.
                return Ok(MatchResult::Abort);
            }
            b'_' => {
                text = rest_text;
                pattern = rest_pattern;
            }
            b'\\' => {
                let Some((&escaped, rest_pattern)) = rest_pattern.split_first() else {
                    return Err(ExprError::InvalidParam {
                        name: "pattern",
                        reason: "LIKE pattern must not end with escape character".into(),
                    });
                };
                if !byte_eq::<CASE_INSENSITIVE>(t, escaped) {
                    return Ok(MatchResult::False);
                }
                text = rest_text;
                pattern = rest_pattern;
            }
            literal => {
                if !byte_eq::<CASE_INSENSITIVE>(t, literal) {
                    // non-wildcard pattern char fails to match text char
                    return Ok(MatchResult::False);
                }

                text = rest_text;
                pattern = rest_pattern;
            }
        }
    }

    if !text.is_empty() {
        // end of pattern, but not of text
        return Ok(MatchResult::False);
    }

    // End of text, but perhaps not of pattern.
    // Match iff the remaining pattern can match a zero-length string,
    // ie, it's zero or more %'s.
    while pattern.first() == Some(&b'%') {
        pattern = &pattern[1..];
    }

    // End of text with no match, so no point in trying later places to start
    // matching this pattern.
    if pattern.is_empty() {
        Ok(MatchResult::True)
    } else {
        Ok(MatchResult::Abort)
    }
}

#[function("like(varchar, varchar) -> boolean")]
pub fn like_default(s: &str, p: &str) -> Result<bool> {
    like_impl_escape::<false, b'\\'>(s, p)
}

#[function("i_like(varchar, varchar) -> boolean")]
pub fn i_like_default(s: &str, p: &str) -> Result<bool> {
    like_impl_escape::<true, b'\\'>(s, p)
}

#[function(
    "like(varchar, varchar, varchar) -> boolean",
    prebuild = "EscapeChar::from_str($2)?"
)]
fn like(s: &str, p: &str, escape: &EscapeChar) -> Result<bool> {
    like_impl_inner::<false>(s, p, escape.0)
}

#[function(
    "i_like(varchar, varchar, varchar) -> boolean",
    prebuild = "EscapeChar::from_str($2)?"
)]
fn i_like(s: &str, p: &str, escape: &EscapeChar) -> Result<bool> {
    like_impl_inner::<true>(s, p, escape.0)
}

// TODO: We should support any UTF-8 character as escape character.
#[derive(Copy, Clone, Debug)]
struct EscapeChar(Option<u8>);

impl EscapeChar {
    fn from_str(escape: &str) -> Result<Self> {
        if escape.is_empty() {
            return Ok(Self(None));
        }

        Itertools::exactly_one(escape.chars())
            .ok()
            .and_then(|c| c.as_ascii().map(|c| c.to_u8()))
            .map(Some)
            .ok_or_else(|| ExprError::InvalidParam {
                name: "escape",
                reason: "only empty or single ascii character is supported now".into(),
            })
            .map(Self)
    }
}

#[cfg(test)]
mod tests {
    use risingwave_expr::scalar::like::EscapeChar;

    use super::{
        MAX_RECURSION_DEPTH, i_like, i_like_default, like, like_default, normalize_pattern,
    };

    static CASES: &[(&str, &str, bool, bool)] = &[
        (r#"ABCDE"#, r#"%abcde%"#, false, false),
        (r#"Like, expression"#, r#"Like, expression"#, false, true),
        (r#"Like, expression"#, r#"Like, %"#, false, true),
        (r#"Like, expression"#, r#"%, expression"#, false, true),
        (r#"like"#, r#"li%ke"#, false, true),
        (r#"like"#, r#"l%ik%e"#, false, true),
        (r#"like"#, r#"%like%"#, false, true),
        (r#"like"#, r#"l%i%k%e%"#, false, true),
        (r#"like"#, r#"_%_e"#, false, true),
        (r#"like"#, r#"l%__"#, false, true),
        (r#"like"#, r#"_%_%_%_"#, false, true),
        (r#"abctest"#, r#"__test"#, false, false),
        (r#"abctest"#, r#"%_test"#, false, true),
        (r#"aaaaabbb"#, r#"a%a%a%a%a%a%b"#, false, false),
        (
            r#"blush thistle blue yellow saddle"#,
            r#"%yellow%"#,
            false,
            true,
        ),
        (r#"ABC_123"#, r#"ABC_123"#, false, true),
        (r#"ABCD123"#, r#"ABC_123"#, false, true),
        (r#"ABC_123"#, r"ABC\_123", false, true),
        (r#"ABCD123"#, r"ABC\_123", false, false),
        (r"ABC\123", r#"ABC_123"#, false, true),
        (r"ABC\123", r"ABC\\123", false, true),
        (r"ABC\123", r"ABC\123", false, false),
        ("apple", r#"App%"#, true, true),
        ("banana", r#"B%nana"#, true, true),
        ("apple", r#"B%nana"#, true, false),
        ("grape", "Gr_P_", true, true),
    ];

    #[test]
    fn test_like() {
        for (target, pattern, case_insensitive, expected) in CASES {
            let output = if *case_insensitive {
                i_like_default(target, pattern)
            } else {
                like_default(target, pattern)
            };
            assert!(output.is_ok());
            assert_eq!(
                output.unwrap(),
                *expected,
                "target={}, pattern={}, case_insensitive={}",
                target,
                pattern,
                case_insensitive
            );
        }
    }

    fn assert_normalized_pattern(pattern: &str, escape: &str, expected: &str) {
        let escape = EscapeChar::from_str(escape).unwrap();
        let normalized = normalize_pattern(pattern, escape.0);
        assert_eq!(
            normalized,
            expected.as_bytes(),
            "pattern={pattern}, escape={escape:?}"
        );
    }

    #[test]
    fn test_normalize_pattern() {
        let testcases = [
            // Default PostgreSQL escape behavior is already the internal representation.
            (r"ABC\_123", r"\", r"ABC\_123"),
            (r"ABC\\123", r"\", r"ABC\\123"),
            // Unused escape characters keep wildcard semantics.
            ("h%", "#", "h%"),
            ("ind_o", "$", "ind_o"),
            // Escaped percent and underscore become internal backslash escapes.
            ("h#%", "#", r"h\%"),
            ("h#%%", "#", r"h\%%"),
            ("i$_d_o", "$", r"i\_d_o"),
            ("i$_d%o", "$", r"i\_d%o"),
            // PostgreSQL regression cases where the escape is also a wildcard.
            ("m%aca", "%", r"m\aca"),
            ("m%a%%a", "%", r"m\a\%a"),
            ("b_ear", "_", r"b\ear"),
            ("b_e__r", "_", r"b\e\_r"),
            ("__e__r", "_", r"\_e\_r"),
            ("____r", "_", r"\_\_r"),
            // ESCAPE '' disables escaping, including backslash escaping.
            ("a_c", "", "a_c"),
            (r"a\_c", "", r"a\\_c"),
        ];

        for (pattern, escape, expected) in testcases {
            assert_normalized_pattern(pattern, escape, expected);
        }
    }

    #[test]
    fn test_normalize_pattern_ends_with_escape() {
        let testcases = [
            (r"abc\", r"\", r"abc\"),
            ("h#", "#", r"h\"),
            ("_____", "_", r"\_\_\"),
            ("m%", "%", r"m\"),
        ];

        for (pattern, escape, expected) in testcases {
            assert_normalized_pattern(pattern, escape, expected);
        }

        assert_normalized_pattern(r"abc\", "", r"abc\\");
    }

    static ESCAPE_CASES: &[(&str, &str, &str, bool)] = &[
        (r"bear", r"b_ear", r"_", true),
        (r"be_r", r"b_e__r", r"_", true),
        (r"be__r", r"b_e___r", r"_", false),
        (r"be__r", r"b_e____r", r"_", true),
        (r"be___r", r"b_e_____r", r"_", false),
        (r"be___r", r"b_e______r", r"_", true),
        (r"be_r", r"__e__r", r"_", false),
        (r"___r", r"____r", r"_", false),
        (r"__r", r"____r", r"_", true),
        (r"maca", r"m%aca", r"%", true),
        (r"ma%a", r"m%a%%a", r"%", true),
        (r"abc", r"a_c", r"", true),
        (r"a_c", r"a\_c", r"", false),
        (r"a\_c", r"a\_c", r"", true),
        (r"abc", r"abc\", r"\", false),
        (r"h", r"h#", "#", false),
        (r"__", r"_____", "_", false),
        (r"m", r"m%", "%", false),
    ];

    #[test]
    fn test_escape_like() {
        for (target, pattern, escape, expected) in ESCAPE_CASES {
            let output = like(target, pattern, &EscapeChar::from_str(escape).unwrap());
            assert!(output.is_ok());
            assert_eq!(
                output.unwrap(),
                *expected,
                "target={}, pattern={}, escape={}",
                target,
                pattern,
                escape
            );
        }
    }

    #[test]
    fn test_like_pattern_ends_with_escape() {
        let escape = EscapeChar::from_str("_").unwrap();
        assert!(like("____", "_____", &escape).is_err());
        assert!(i_like("____", "_____", &escape).is_err());
    }

    #[test]
    fn test_like_recursion_depth_limit() {
        let text = "a".repeat(MAX_RECURSION_DEPTH - 1);
        let pattern = "%a".repeat(MAX_RECURSION_DEPTH - 1);
        assert!(like_default(&text, &pattern).unwrap());

        let text = "a".repeat(MAX_RECURSION_DEPTH);
        let pattern = "%a".repeat(MAX_RECURSION_DEPTH);

        assert!(like_default(&text, &pattern).is_err());
    }
}
