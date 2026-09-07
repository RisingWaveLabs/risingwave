// Copyright 2022 RisingWave Labs
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

use std::vec::IntoIter;

use futures::stream::FusedStream;
use futures::{StreamExt, TryStreamExt};
use postgres_types::FromSql;

use crate::error::{PsqlError, PsqlResult};
use crate::pg_message::{BeCommandCompleteMessage, BeMessage};
use crate::pg_protocol::{PgByteStream, PgStream};
use crate::pg_response::{PgResponse, ValuesStream};
use crate::types::{Format, Row};

pub struct ResultCache<VS>
where
    VS: ValuesStream,
{
    result: PgResponse<VS>,
    row_cache: IntoIter<Row>,
}

impl<VS> ResultCache<VS>
where
    VS: ValuesStream,
{
    pub fn new(result: PgResponse<VS>) -> Self {
        ResultCache {
            result,
            row_cache: vec![].into_iter(),
        }
    }

    /// Return indicate whether the result is consumed completely.
    pub async fn consume<S: PgByteStream>(
        &mut self,
        row_limit: usize,
        msg_stream: &mut PgStream<S>,
    ) -> PsqlResult<bool> {
        for notice in self.result.notices() {
            msg_stream.write_no_flush(BeMessage::NoticeResponse(notice))?;
        }

        let status = self.result.status();
        if let Some(ref application_name) = status.application_name {
            msg_stream.write_no_flush(BeMessage::ParameterStatus(
                crate::pg_message::BeParameterStatusMessage::ApplicationName(application_name),
            ))?;
        }

        if self.result.is_empty() {
            // Run the callback before sending the response.
            self.result.run_callback().await?;

            msg_stream.write_no_flush(BeMessage::EmptyQueryResponse)?;
            return Ok(true);
        }

        let mut query_end = false;
        if self.result.is_copy_query_to_stdout() {
            msg_stream.write_no_flush(BeMessage::CopyOutResponse(self.result.row_desc().len()))?;

            let mut count = 0;
            while let Some(row_set) = self.result.values_stream().next().await {
                let row_set = row_set.map_err(PsqlError::SimpleQueryError)?;
                for row in row_set {
                    msg_stream
                        .write_streaming(BeMessage::CopyData(&row))
                        .await?;
                    count += 1;
                }
            }

            msg_stream.write_no_flush(BeMessage::CopyDone)?;

            // Run the callback before sending the `CommandComplete` message.
            self.result.run_callback().await?;

            msg_stream.write_no_flush(BeMessage::CommandComplete(BeCommandCompleteMessage {
                stmt_type: self.result.stmt_type(),
                rows_cnt: count,
            }))?;

            query_end = true;
        } else if self.result.is_query() {
            let mut query_row_count = 0;

            // fetch row data
            // if row_limit is 0, fetch all rows
            // if row_limit > 0, fetch row_limit rows
            while row_limit == 0 || query_row_count < row_limit {
                if self.row_cache.len() > 0 {
                    for row in self.row_cache.by_ref() {
                        msg_stream.write_streaming(BeMessage::DataRow(&row)).await?;
                        query_row_count += 1;
                        if row_limit > 0 && query_row_count >= row_limit {
                            break;
                        }
                    }
                } else {
                    self.row_cache = match self
                        .result
                        .values_stream()
                        .try_next()
                        .await
                        .map_err(PsqlError::ExtendedExecuteError)?
                    {
                        Some(rows) => rows.into_iter(),
                        _ => {
                            query_end = true;
                            break;
                        }
                    };
                }
            }

            // Check if the result is consumed completely.
            // If not, cache the result.
            if self.row_cache.len() == 0 && self.result.values_stream().peekable().is_terminated() {
                query_end = true;
            }
            if query_end {
                // Run the callback before sending the `CommandComplete` message.
                self.result.run_callback().await?;

                msg_stream.write_no_flush(BeMessage::CommandComplete(
                    BeCommandCompleteMessage {
                        stmt_type: self.result.stmt_type(),
                        rows_cnt: query_row_count as i32,
                    },
                ))?;
            } else {
                msg_stream.write_no_flush(BeMessage::PortalSuspended)?;
            }
        } else if self.result.stmt_type().is_dml() && !self.result.stmt_type().is_returning() {
            let first_row_set = self.result.values_stream().next().await;
            let first_row_set = match first_row_set {
                None => {
                    return Err(PsqlError::Uncategorized(
                        "no affected rows in output".into(),
                    ));
                }
                Some(row) => row.map_err(PsqlError::SimpleQueryError)?,
            };
            let affected_rows_str = first_row_set[0].values()[0]
                .as_ref()
                .expect("compute node should return affected rows in output");

            let affected_rows_cnt: i32 = match self.result.row_cnt_format() {
                Some(Format::Binary) => {
                    i64::from_sql(&postgres_types::Type::INT8, affected_rows_str)
                        .unwrap()
                        .try_into()
                        .expect("affected rows count large than i64")
                }
                Some(Format::Text) => String::from_utf8(affected_rows_str.to_vec())
                    .unwrap()
                    .parse()
                    .unwrap_or_default(),
                None => panic!("affected rows count should be set"),
            };

            // Run the callback before sending the `CommandComplete` message.
            self.result.run_callback().await?;

            msg_stream.write_no_flush(BeMessage::CommandComplete(BeCommandCompleteMessage {
                stmt_type: self.result.stmt_type(),
                rows_cnt: affected_rows_cnt,
            }))?;

            query_end = true;
        } else {
            // Run the callback before sending the `CommandComplete` message.
            self.result.run_callback().await?;

            msg_stream.write_no_flush(BeMessage::CommandComplete(BeCommandCompleteMessage {
                stmt_type: self.result.stmt_type(),
                rows_cnt: self
                    .result
                    .affected_rows_cnt()
                    .expect("row count should be set"),
            }))?;

            query_end = true;
        }

        Ok(query_end)
    }
}
