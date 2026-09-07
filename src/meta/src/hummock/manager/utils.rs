// Copyright 2024 RisingWave Labs
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

/// Commit multiple `ValTransaction`s to state store and upon success update the local in-mem state
/// by the way
/// After called, the `ValTransaction` will be dropped.
macro_rules! commit_multi_var {
    ($meta_store:expr, $($val_txn:expr),*) => {
        {
            async {
                use crate::model::{MetadataModelError, InMemValTransaction, ValTransaction};
                use sea_orm::TransactionTrait;
                let mut txn = $meta_store.conn.begin().await.map_err(MetadataModelError::from)?;
                $(
                    $val_txn.apply_to_txn(&mut txn).await?;
                )*
                txn.commit().await.map_err(MetadataModelError::from)?;
                $(
                    $val_txn.commit();
                )*
                Result::Ok(())
            }.await
        }
    };
}

macro_rules! commit_multi_var_with_provided_txn {
    ($txn:expr, $($val_txn:expr),*) => {
        {
            async {
                use crate::model::{InMemValTransaction, ValTransaction};
                use crate::model::MetadataModelError;
                $(
                    $val_txn.apply_to_txn(&mut $txn).await?;
                )*
                $txn.commit().await.map_err(MetadataModelError::from)?;
                $(
                    $val_txn.commit();
                )*
                Result::Ok(())
            }.await
        }
    };
}

pub(crate) use commit_multi_var;
pub(crate) use commit_multi_var_with_provided_txn;
use risingwave_hummock_sdk::ObjectIdRange;

use crate::hummock::HummockManager;
use crate::hummock::error::Result;
use crate::hummock::sequence::next_raw_object_id;

impl HummockManager {
    #[cfg(test)]
    pub(super) async fn check_state_consistency(&self) {
        use crate::hummock::manager::compaction::Compaction;
        use crate::hummock::manager::context::ContextInfo;
        use crate::hummock::manager::versioning::Versioning;
        let mut compaction_guard = self
            .compaction
            .write_with_process_name("check_state_consistency")
            .await;
        let mut versioning_guard = self
            .versioning
            .write_with_process_name("check_state_consistency")
            .await;
        let mut context_info_guard = self
            .context_info
            .write_with_process_name("check_state_consistency")
            .await;
        // We don't check `checkpoint` because it's allowed to update its in memory state without
        // persisting to object store.
        let get_state = |compaction_guard: &mut Compaction,
                         versioning_guard: &mut Versioning,
                         context_info_guard: &mut ContextInfo| {
            let compact_statuses_copy = compaction_guard.compaction_statuses.clone();
            let compact_task_assignment_copy = compaction_guard.compact_task_assignment.clone();
            let pinned_versions_copy = context_info_guard.pinned_versions.clone();
            let hummock_version_deltas_copy = versioning_guard.hummock_version_deltas.clone();
            let version_stats_copy = versioning_guard.version_stats.clone();
            ((
                compact_statuses_copy,
                compact_task_assignment_copy,
                pinned_versions_copy,
                hummock_version_deltas_copy,
                version_stats_copy,
            ),)
        };
        let mem_state = get_state(
            &mut compaction_guard,
            &mut versioning_guard,
            &mut context_info_guard,
        );
        self.load_meta_store_state_impl(
            &mut compaction_guard,
            &mut versioning_guard,
            &mut context_info_guard,
        )
        .await
        .expect("Failed to load state from meta store");
        let loaded_state = get_state(
            &mut compaction_guard,
            &mut versioning_guard,
            &mut context_info_guard,
        );
        assert_eq!(
            mem_state, loaded_state,
            "hummock in-mem state is inconsistent with meta store state",
        );
    }

    pub async fn get_new_object_ids(&self, number: u32) -> Result<ObjectIdRange> {
        let start_id = next_raw_object_id(&self.env, number).await?;
        Ok(ObjectIdRange::new(start_id, start_id + number as u64))
    }
}
