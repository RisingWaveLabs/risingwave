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

//! Parsers for data types.
//!
//! This module contains parsers for data types. To handle the anbiguity of `>>` and `> >` in struct definition,
//! we need to use a stateful parser here. See [`with_state`] for more information.

use std::cell::RefCell;
use std::rc::Rc;

use winnow::combinator::{
    alt, cut_err, delimited, dispatch, empty, fail, opt, preceded, repeat, separated, seq,
    terminated, trace,
};
use winnow::error::{ContextError, ErrMode, FromExternalError, StrContext};
use winnow::{ModalResult, Parser, Stateful};

use super::{
    TokenStream, identifier_non_reserved, keyword, literal_u64, object_name, precision_in_range,
    with_state,
};
use crate::ast::{DataType, StructField};
use crate::keywords::Keyword;
use crate::tokenizer::Token;

#[derive(Default, Debug)]
struct DataTypeParsingState {
    /// Since we can't distinguish between `>>` and `> >` in tokenizer, we need to handle this case in the parser.
    /// When we want a [`>`][Token::Gt] but actually consumed a `>>` (ShiftRight), we set this to true.
    /// When the value was true and we want a [`>`][Token::Gt], we just set this to false instead of really consume it.
    remaining_close: Rc<RefCell<usize>>,
}

type StatefulStream<S> = Stateful<S, DataTypeParsingState>;

/// Consume struct type definitions
fn struct_data_type<S>(input: &mut StatefulStream<S>) -> ModalResult<Vec<StructField>>
where
    S: TokenStream,
{
    let remaining_close1 = input.state.remaining_close.clone();
    let remaining_close2 = input.state.remaining_close.clone();

    // Consume an abstract `>`, it may be the `remaining_close1` flag set by previous `>>`.
    let consume_close = trace(
        "consume_struct_close",
        alt((
            trace(
                "consume_remaining_close",
                move |input: &mut StatefulStream<S>| -> ModalResult<()> {
                    let rem = *remaining_close1.borrow();
                    if let Some(sub1) = rem.checked_sub(1) {
                        *remaining_close1.borrow_mut() = sub1;
                        Ok(())
                    } else {
                        fail(input)
                    }
                },
            )
            .void(),
            trace(
                "produce_remaining_close",
                super::token
                    .verify(|t| match &t.token {
                        Token::Op(op) if op.chars().all(|c| c == '>') => {
                            *remaining_close2.borrow_mut() = op.len() - 1;
                            true
                        }
                        _ => false,
                    })
                    .void(),
            ),
            Token::Gt.void(),
        )),
    );

    // If there is an `over-consumed' `>`, we shouldn't handle `,`.
    let sep = |input: &mut StatefulStream<S>| -> ModalResult<()> {
        if *input.state.remaining_close.borrow() > 0 {
            fail(input)
        } else {
            Token::Comma.void().parse_next(input)
        }
    };

    // Note: although we support empty(zero-field) struct and we allow `0` occurrences here, users
    // still need to leave a whitespace between `<` and `>` to prevent it from being tokenized as `Neq`.
    delimited(
        Token::Lt,
        cut_err(separated(
            0..,
            trace(
                "struct_field",
                seq! {
                    StructField {
                        name: identifier_non_reserved,
                        data_type: data_type_stateful,
                    }
                },
            ),
            sep,
        )),
        cut_err(consume_close),
    )
    .context(StrContext::Label("struct_data_type"))
    .parse_next(input)
}

/// Consume a data type definition.
///
/// The parser is the main entry point for data type parsing.
///
/// Note: in recursion, we should use `data_type_stateful` instead of `data_type`,
/// otherwise the type parameter will recurse like `Stateful<Stateful<Stateful<...>>>`.
/// Also note that we cannot use `Parser<'_>` directly to avoid misuse, because we need
/// generics `<S>` to parameterize over `Parser<'_>` and `Stateful<Parser<'_>>`.
pub fn data_type<S>(input: &mut S) -> ModalResult<DataType>
where
    S: TokenStream,
{
    #[derive(Debug, thiserror::Error)]
    #[error("unconsumed `>>`")]
    struct UnconsumedShiftRight;

    with_state::<S, DataTypeParsingState, _, _, _>(terminated(
        data_type_stateful,
        trace("data_type_verify_state", |input: &mut StatefulStream<S>| {
            // If there is remaining `>`, we should fail.
            if *input.state.remaining_close.borrow() > 0 {
                Err(ErrMode::Cut(ContextError::from_external_error(
                    input,
                    UnconsumedShiftRight,
                )))
            } else {
                Ok(())
            }
        }),
    ))
    .context(StrContext::Label("data_type"))
    .parse_next(input)
}

/// Data type parsing with stateful stream.
fn data_type_stateful<S>(input: &mut StatefulStream<S>) -> ModalResult<DataType>
where
    S: TokenStream,
{
    let base = data_type_stateful_inner.parse_next(input)?;
    // Shall not peek for `Token::LBracket` when `>>` is partially consumed.
    if *input.state.remaining_close.borrow() > 0 {
        return Ok(base);
    }
    (
        empty.value(base),
        repeat(0.., (Token::LBracket, cut_err(Token::RBracket))),
    )
        .map(|(mut dt, depth)| {
            let depth: usize = depth;
            for _ in 0..depth {
                dt = DataType::Array(Box::new(dt));
            }
            dt
        })
        .parse_next(input)
}

/// Consume a data type except [`DataType::Array`].
fn data_type_stateful_inner<S>(input: &mut StatefulStream<S>) -> ModalResult<DataType>
where
    S: TokenStream,
{
    trace(
        "data_type_inner",
        alt((keyword_datatype, non_keyword_datatype)),
    )
    .parse_next(input)
}

fn keyword_datatype<S: TokenStream>(input: &mut StatefulStream<S>) -> ModalResult<DataType> {
    let with_time_zone = || {
        opt(alt((
            (Keyword::WITH, Keyword::TIME, Keyword::ZONE).value(true),
            (Keyword::WITHOUT, Keyword::TIME, Keyword::ZONE).value(false),
        )))
        .map(|x| x.unwrap_or(false))
    };

    let precision_and_scale = || {
        opt(delimited(
            Token::LParen,
            (literal_u64, opt(preceded(Token::Comma, literal_u64))),
            Token::RParen,
        ))
        .map(|p| match p {
            Some((x, y)) => (Some(x), y),
            None => (None, None),
        })
    };

    let mut ty = dispatch! {keyword;
        Keyword::BOOLEAN | Keyword::BOOL => empty.value(DataType::Boolean),
        Keyword::FLOAT => opt(precision_in_range(1..54)).map(DataType::Float),
        Keyword::REAL => empty.value(DataType::Real),
        Keyword::DOUBLE => opt(Keyword::PRECISION).value(DataType::Double),
        Keyword::SMALLINT => empty.value(DataType::SmallInt),
        Keyword::INT | Keyword::INTEGER => empty.value(DataType::Int),
        Keyword::BIGINT => empty.value(DataType::BigInt),
        Keyword::STRING | Keyword::VARCHAR => empty.value(DataType::Varchar),
        Keyword::CHAR | Keyword::CHARACTER => alt((
            Keyword::VARYING.value(DataType::Varchar),
            opt(precision_in_range(..)).map(DataType::Char),
        )),
        Keyword::UUID => empty.value(DataType::Uuid),
        Keyword::DATE => empty.value(DataType::Date),
        Keyword::TIMESTAMP => with_time_zone().map(DataType::Timestamp),
        Keyword::TIME => with_time_zone().map(DataType::Time),
        // TODO: Support complex interval type parsing.
        Keyword::INTERVAL => empty.value(DataType::Interval),
        Keyword::TEXT => empty.value(DataType::Text),
        Keyword::STRUCT => cut_err(struct_data_type).map(DataType::Struct),
        Keyword::BYTEA => empty.value(DataType::Bytea),
        Keyword::NUMERIC | Keyword::DECIMAL | Keyword::DEC => cut_err(precision_and_scale()).map(|(precision, scale)| {
            DataType::Decimal(precision, scale)
        }),
        _ => fail
    };

    ty.parse_next(input)
}

fn non_keyword_datatype<S: TokenStream>(input: &mut StatefulStream<S>) -> ModalResult<DataType> {
    let type_name = object_name.parse_next(input)?;
    match type_name.to_string().to_ascii_lowercase().as_str() {
        // PostgreSQL built-in data types that are not keywords.
        "jsonb" => Ok(DataType::Jsonb),
        "variant" => Ok(DataType::Variant),
        "regclass" => Ok(DataType::Regclass),
        "regproc" => Ok(DataType::Regproc),
        "map" => cut_err(map_type_arguments).parse_next(input),
        "vector" => precision_in_range(..)
            .map(DataType::Vector)
            .parse_next(input),
        _ => Ok(DataType::Custom(type_name)),
    }
}

fn map_type_arguments<S: TokenStream>(input: &mut StatefulStream<S>) -> ModalResult<DataType> {
    delimited(
        Token::LParen,
        // key is string or integral type. value is arbitrary type.
        // We don't validate here, but in binder bind_data_type
        seq!(keyword_datatype, _:Token::Comma, data_type_stateful),
        Token::RParen,
    )
    .map(|(k, v)| DataType::Map(Box::new((k, v))))
    .parse_next(input)
}
