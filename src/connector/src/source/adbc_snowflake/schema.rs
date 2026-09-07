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

use adbc_core::Statement as _;
use anyhow::Context;
use risingwave_common::array::arrow::arrow_schema_58 as arrow_schema;

use super::AdbcSnowflakeProperties;
use crate::error::ConnectorResult;

impl AdbcSnowflakeProperties {
    /// Get the Arrow schema from the Snowflake table.
    /// This is used for schema inference when creating tables.
    ///
    /// **Important**: We use a `LIMIT 0` query instead of ADBC's `get_table_schema` API
    /// because `get_table_schema` may return different types than the actual query results.
    /// For example, Snowflake NUMBER columns may be reported as Int64 by `get_table_schema`
    /// but returned as Decimal128 in actual query results. Using a real query ensures
    /// consistency between schema inference (used by frontend) and data fetching (used by executor).
    ///
    /// The column order in the returned schema matches the column order in the Snowflake table.
    pub fn get_arrow_schema(&self) -> ConnectorResult<arrow_schema::Schema> {
        let database = self.create_database()?;
        let mut connection = self.create_connection(&database)?;

        // Use LIMIT 0 query to get the actual schema that will be returned by queries.
        // This ensures schema inference matches actual data types returned by Snowflake.
        let query = format!("SELECT * FROM {} LIMIT 0", self.table_ref());
        let mut statement = self.create_statement(&mut connection, &query)?;

        let reader = statement
            .execute()
            .context("Failed to execute schema query")?;

        let schema = reader.schema();

        Ok((*schema).clone())
    }
}
