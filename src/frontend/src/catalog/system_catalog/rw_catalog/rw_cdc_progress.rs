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

use risingwave_common::id::JobId;
use risingwave_common::types::Fields;
use risingwave_frontend_macro::system_catalog;

use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::error::Result;

#[derive(Fields)]
struct RwCdcProgress {
    #[primary_key]
    job_id: JobId,
    split_total_count: i64,
    split_backfilled_count: i64,
    split_completed_count: i64,
    backfilled_row_count: Option<i64>,
    estimated_row_count: Option<i64>,
}

#[system_catalog(table, "rw_catalog.rw_cdc_progress")]
async fn read_rw_cdc_progress(reader: &SysCatalogReaderImpl) -> Result<Vec<RwCdcProgress>> {
    let progress = reader.meta_client.list_cdc_progress().await?;

    Ok(progress
        .into_iter()
        .map(|(job_id, p)| RwCdcProgress {
            job_id,
            split_total_count: p.split_total_count as _,
            split_backfilled_count: p.split_backfilled_count as _,
            split_completed_count: p.split_completed_count as _,
            backfilled_row_count: p.backfilled_row_count.map(|count| count as _),
            estimated_row_count: p.estimated_row_count.map(|count| count as _),
        })
        .collect())
}
