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

use std::time::Instant;

use anyhow::anyhow;
use futures::StreamExt;
use futures::future::try_join_all;
use risingwave_common::config::ObjectStoreConfig;
use risingwave_hummock_sdk::{
    VALID_OBJECT_ID_SUFFIXES, get_object_data_path, get_object_id_from_path,
};
use risingwave_object_store::object::object_metrics::ObjectStoreMetrics;
use risingwave_object_store::object::prefix::opendal_engine::get_object_prefix;
use risingwave_object_store::object::{
    ObjectStoreImpl, OpendalObjectStore, build_remote_object_store,
};

pub async fn migrate_legacy_object(
    url: String,
    source_dir: String,
    target_dir: String,
    concurrency: u32,
) -> anyhow::Result<()> {
    let source_dir = source_dir.trim_end_matches('/');
    let target_dir = target_dir.trim_end_matches('/');
    println!("Normalized source_dir: {source_dir}.");
    println!("Normalized target_dir: {target_dir}.");
    if source_dir.is_empty() || target_dir.is_empty() {
        return Err(anyhow!("the source_dir and target_dir must not be empty"));
    }
    if target_dir.starts_with(source_dir) {
        return Err(anyhow!("the target_dir must not include source_dir"));
    }
    let mut config = ObjectStoreConfig::default();
    config.s3.developer.use_opendal = true;
    let store = build_remote_object_store(
        &url,
        ObjectStoreMetrics::unused().into(),
        "migrate_legacy_object",
        config.into(),
        "",
    )
    .await;
    let ObjectStoreImpl::Opendal(opendal) = store else {
        return Err(anyhow!("OpenDAL is required"));
    };
    let mut iter = opendal.list(source_dir, None, None).await?;
    let mut count = 0;
    println!("Migration is started: from {source_dir} to {target_dir}.");
    let mut from_to = Vec::with_capacity(concurrency as usize);
    let timer = Instant::now();
    while let Some(object) = iter.next().await {
        let object = object?;
        if !VALID_OBJECT_ID_SUFFIXES
            .iter()
            .any(|suffix| object.key.ends_with(suffix))
        {
            let legacy_path = object.key;
            assert_eq!(
                &legacy_path[..source_dir.len()],
                source_dir,
                "{legacy_path} versus {source_dir}"
            );
            if legacy_path.ends_with('/') {
                tracing::warn!(legacy_path, "skip directory");
                continue;
            }
            let new_path = format!("{}{}", target_dir, &legacy_path[source_dir.len()..]);
            from_to.push((legacy_path, new_path));
        } else {
            let object_id = get_object_id_from_path(&object.key);
            let legacy_prefix = get_object_prefix(object_id.as_raw().as_raw_id(), false);
            let legacy_path = get_object_data_path(&legacy_prefix, source_dir, object_id);
            if object.key != legacy_path {
                return Err(anyhow!(format!(
                    "the source object store does not appear to be legacy: {} versus {}",
                    object.key, legacy_path
                )));
            }
            let new_path = get_object_data_path(
                &get_object_prefix(object_id.as_raw().as_raw_id(), true),
                target_dir,
                object_id,
            );
            from_to.push((legacy_path, new_path));
        }
        count += 1;
        if from_to.len() >= concurrency as usize {
            copy(std::mem::take(&mut from_to).into_iter(), opendal.inner()).await?;
        }
    }
    if !from_to.is_empty() {
        copy(from_to.into_iter(), opendal.inner()).await?;
    }
    let cost = timer.elapsed();
    println!(
        "Migration is finished in {} seconds. {count} objects have been migrated from {source_dir} to {target_dir}.",
        cost.as_secs()
    );
    Ok(())
}

async fn copy(
    from_to: impl Iterator<Item = (String, String)>,
    opendal: &OpendalObjectStore,
) -> anyhow::Result<()> {
    let futures = from_to.map(|(from_path, to_path)| async move {
        println!("From {from_path} to {to_path}");
        opendal.copy(&from_path, &to_path).await
    });
    try_join_all(futures).await?;
    Ok(())
}
