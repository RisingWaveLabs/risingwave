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

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::{FragmentTypeFlag, FragmentTypeMask};
    use risingwave_common::hash::VirtualNode;
    use risingwave_meta_model::FragmentId;
    use risingwave_meta_model::fragment::DistributionType;
    use risingwave_meta_model::table::HandleConflictBehavior;
    use risingwave_pb::catalog::subscription::SubscriptionState;
    use risingwave_pb::catalog::{PbSinkType, StreamSourceInfo};
    use risingwave_pb::common::{HostAddress, WorkerNode, WorkerType, worker_node};
    use risingwave_pb::meta::SubscribeType;
    use risingwave_pb::meta::table_fragments::fragment::PbFragmentDistributionType;
    use risingwave_pb::stream_plan::stream_node::PbNodeBody;
    use risingwave_pb::stream_plan::{PbStreamNode, StreamScanNode, StreamScanType};
    use tokio::sync::{mpsc, oneshot};

    use crate::barrier::Command;
    use crate::controller::catalog::*;
    use crate::manager::{LocalNotification, MetaOpts, WorkerKey};
    use crate::model::{Fragment, FragmentDownstreamRelation};
    use crate::serving::ServingVnodeMapping;

    const TEST_DATABASE_ID: DatabaseId = DatabaseId::new(1);
    const TEST_SCHEMA_ID: SchemaId = SchemaId::new(2);
    const TEST_OWNER_ID: UserId = UserId::new(1);

    async fn insert_test_table(
        txn: &DatabaseTransaction,
        table_id: TableId,
        name: &str,
        table_type: TableType,
        belongs_to_job_id: Option<JobId>,
        definition: &str,
    ) -> MetaResult<()> {
        table::ActiveModel {
            table_id: Set(table_id),
            name: Set(name.to_owned()),
            optional_associated_source_id: Set(None),
            table_type: Set(table_type),
            belongs_to_job_id: Set(belongs_to_job_id),
            columns: Set(vec![].into()),
            pk: Set(vec![].into()),
            distribution_key: Set(Vec::<i32>::new().into()),
            stream_key: Set(Vec::<i32>::new().into()),
            append_only: Set(false),
            fragment_id: Set(None),
            vnode_col_index: Set(None),
            row_id_index: Set(None),
            value_indices: Set(Vec::<i32>::new().into()),
            definition: Set(definition.to_owned()),
            handle_pk_conflict_behavior: Set(HandleConflictBehavior::NoCheck),
            version_column_indices: Set(None),
            read_prefix_len_hint: Set(0),
            watermark_indices: Set(Vec::<i32>::new().into()),
            dist_key_in_pk: Set(Vec::<i32>::new().into()),
            dml_fragment_id: Set(None),
            cardinality: Set(None),
            cleaned_by_watermark: Set(false),
            description: Set(None),
            version: Set(None),
            retention_seconds: Set(None),
            cdc_table_id: Set(None),
            vnode_count: Set(1),
            webhook_info: Set(None),
            engine: Set(None),
            clean_watermark_index_in_pk: Set(None),
            clean_watermark_indices: Set(None),
            refreshable: Set(false),
            vector_index_info: Set(None),
            cdc_table_type: Set(None),
        }
        .insert(txn)
        .await?;
        Ok(())
    }

    async fn insert_test_fragment(
        txn: &DatabaseTransaction,
        fragment_id: FragmentId,
        job_id: JobId,
        state_table_ids: TableIdArray,
    ) -> MetaResult<()> {
        fragment::ActiveModel {
            fragment_id: Set(fragment_id),
            job_id: Set(job_id),
            fragment_type_mask: Set(0),
            distribution_type: Set(fragment::DistributionType::Hash),
            stream_node: Set(StreamNode::from(&PbStreamNode::default())),
            state_table_ids: Set(state_table_ids),
            upstream_fragment_id: Set(I32Array::default()),
            vnode_count: Set(1),
            parallelism: Set(None),
        }
        .insert(txn)
        .await?;
        Ok(())
    }

    async fn insert_test_streaming_job(
        txn: &DatabaseTransaction,
        name: &str,
        has_result_table: bool,
        policy: Option<CacheRefillPolicy>,
    ) -> MetaResult<(JobId, Option<TableId>, TableId)> {
        let object_type = if has_result_table {
            ObjectType::Table
        } else {
            ObjectType::Sink
        };
        let job_id = CatalogController::create_object(
            txn,
            object_type,
            TEST_OWNER_ID,
            Some(TEST_SCHEMA_ID.as_object_id()),
        )
        .await?
        .oid
        .as_job_id();
        let result_table_id = has_result_table.then_some(job_id.as_mv_table_id());
        if let Some(table_id) = result_table_id {
            insert_test_table(txn, table_id, name, TableType::MaterializedView, None, "").await?;
        }

        let internal_table_id = CatalogController::create_object(
            txn,
            ObjectType::Table,
            TEST_OWNER_ID,
            Some(job_id.as_object_id()),
        )
        .await?
        .oid
        .as_table_id();
        insert_test_table(
            txn,
            internal_table_id,
            &format!("__internal_{name}"),
            TableType::Internal,
            Some(job_id),
            "",
        )
        .await?;

        insert_test_streaming_job_model(txn, job_id, policy).await?;

        Ok((job_id, result_table_id, internal_table_id))
    }

    async fn insert_test_streaming_job_model(
        txn: &DatabaseTransaction,
        job_id: JobId,
        policy: Option<CacheRefillPolicy>,
    ) -> MetaResult<()> {
        streaming_job::ActiveModel {
            job_id: Set(job_id),
            job_status: Set(JobStatus::Created),
            create_type: Set(CreateType::Foreground),
            timezone: Set(None),
            config_override: Set(policy.map(|policy| {
                format!(
                    "[streaming.developer]\ncache_refill_policy = \"{}\"\n",
                    policy
                )
            })),
            adaptive_parallelism_strategy: Set(None),
            parallelism: Set(StreamingParallelism::Adaptive),
            backfill_parallelism: Set(None),
            backfill_adaptive_parallelism_strategy: Set(None),
            backfill_orders: Set(None),
            max_parallelism: Set(1),
            specific_resource_group: Set(None),
            is_serverless_backfill: Set(false),
            refresh_interval_sec: Set(None),
        }
        .insert(txn)
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_cancel_creating_job_includes_belonging_streaming_jobs() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;
        let mut inner = mgr.inner.write().await;
        let txn = inner.db.begin().await?;

        let table_job_id = CatalogController::create_object(
            &txn,
            ObjectType::Table,
            TEST_OWNER_ID,
            Some(TEST_SCHEMA_ID.as_object_id()),
        )
        .await?
        .oid
        .as_job_id();
        insert_test_table(
            &txn,
            table_job_id.as_mv_table_id(),
            "cancel_table",
            TableType::Table,
            None,
            "",
        )
        .await?;
        let sink_job_id = CatalogController::create_object(
            &txn,
            ObjectType::Sink,
            TEST_OWNER_ID,
            Some(table_job_id.as_object_id()),
        )
        .await?
        .oid
        .as_job_id();
        Sink::insert(sink::ActiveModel::from(PbSink {
            id: sink_job_id.as_sink_id(),
            schema_id: TEST_SCHEMA_ID,
            database_id: TEST_DATABASE_ID,
            name: "cancel_sink".to_owned(),
            owner: TEST_OWNER_ID as _,
            sink_type: PbSinkType::AppendOnly as i32,
            ..Default::default()
        }))
        .exec(&txn)
        .await?;
        for job_id in [table_job_id, sink_job_id] {
            insert_test_streaming_job_model(&txn, job_id, None).await?;
            StreamingJob::update(streaming_job::ActiveModel {
                job_id: Set(job_id),
                job_status: Set(JobStatus::Creating),
                ..Default::default()
            })
            .exec(&txn)
            .await?;
        }

        let table_state_id = TableId::new(1000);
        let sink_state_id = TableId::new(1001);
        insert_test_fragment(
            &txn,
            FragmentId::new(100),
            table_job_id,
            TableIdArray(vec![table_state_id]),
        )
        .await?;
        insert_test_fragment(
            &txn,
            FragmentId::new(101),
            sink_job_id,
            TableIdArray(vec![sink_state_id]),
        )
        .await?;
        let (table_finish_tx, table_finish_rx) = oneshot::channel();
        inner.register_finish_notifier(TEST_DATABASE_ID, table_job_id, table_finish_tx);
        let (sink_finish_tx, sink_finish_rx) = oneshot::channel();
        inner.register_finish_notifier(TEST_DATABASE_ID, sink_job_id, sink_finish_tx);
        txn.commit().await?;
        drop(inner);

        let abort_result = mgr
            .try_abort_creating_streaming_job(table_job_id, true)
            .await?;
        assert!(abort_result.aborted);
        assert_eq!(
            abort_result.aborted_sink_ids,
            vec![sink_job_id.as_sink_id()]
        );
        let cancel_info = abort_result
            .cancel_info
            .expect("cancelled table job should have cleanup information");
        assert_eq!(
            cancel_info
                .streaming_job_ids
                .iter()
                .copied()
                .collect::<HashSet<_>>(),
            HashSet::from([table_job_id, sink_job_id])
        );
        assert_eq!(
            cancel_info
                .state_table_ids
                .iter()
                .copied()
                .collect::<HashSet<_>>(),
            HashSet::from([table_state_id, sink_state_id])
        );
        let Command::DropStreamingJobs {
            streaming_job_ids,
            unregistered_state_table_ids,
            ..
        } = cancel_info.command
        else {
            unreachable!()
        };
        assert_eq!(
            streaming_job_ids,
            HashSet::from([table_job_id, sink_job_id])
        );
        assert_eq!(
            unregistered_state_table_ids,
            HashSet::from([table_state_id, sink_state_id])
        );

        for finish_rx in [table_finish_rx, sink_finish_rx] {
            let err = finish_rx
                .await
                .expect("aborted job should notify its finish waiter")
                .expect_err("aborted job should not finish successfully");
            assert!(err.contains("cancelled"));
        }
        let db = &mgr.inner.read().await.db;
        assert!(Object::find_by_id(table_job_id).one(db).await?.is_none());
        assert!(Object::find_by_id(sink_job_id).one(db).await?.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_create_multiple_sinks_into_same_table_and_drop_table() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;
        let inner = mgr.inner.write().await;
        let txn = inner.db.begin().await?;
        let (_, Some(target_table_id), _) =
            insert_test_streaming_job(&txn, "mvt", true, None).await?
        else {
            unreachable!()
        };
        let (mv1_id, Some(_), _) = insert_test_streaming_job(&txn, "mv1", true, None).await? else {
            unreachable!()
        };
        let (mv2_id, Some(_), _) = insert_test_streaming_job(&txn, "mv2", true, None).await? else {
            unreachable!()
        };
        txn.commit().await?;
        drop(inner);

        let mut sink_ids = Vec::new();
        let test_sink_tuples = [("s1", mv1_id), ("s2", mv2_id)];

        fn assert_incoming_sink_drop_error<T>(error: &MetaError, test_sink_tuples: &[(&str, T)]) {
            let message = error.to_string();

            assert!(
                message.contains("sink") && message.contains("depends on it"),
                "expected an incoming-sink dependency error, got: {message}"
            );

            assert!(
                test_sink_tuples
                    .iter()
                    .all(|(sink_name, _)| message.contains(*sink_name)),
                "expected the error to mention all incoming sinks, got: {message}"
            );
        }

        for (name, source) in test_sink_tuples {
            let mut job = crate::manager::StreamingJob::Sink(
                PbSink {
                    name: name.to_owned(),
                    database_id: TEST_DATABASE_ID,
                    schema_id: TEST_SCHEMA_ID,
                    owner: TEST_OWNER_ID as _,
                    target_table: Some(target_table_id),
                    sink_type: PbSinkType::AppendOnly as i32,
                    ..Default::default()
                },
                None,
            );
            // Use create_job_catalog to trigger construct_sink_cycle_check_query for regression
            // testing purpose to ensure no circular issue causing infinite recursion when
            // cte_referencing
            tokio::time::timeout(
                std::time::Duration::from_secs(3),
                mgr.create_job_catalog(
                    &mut job,
                    &crate::model::StreamContext::default(),
                    &None,
                    1,
                    HashSet::from([source.as_object_id()]),
                    risingwave_pb::ddl_service::streaming_job_resource_type::ResourceType::Regular(
                        true,
                    ),
                    &None,
                    None,
                    None,
                    None,
                    None,
                ),
            )
            .await
            .expect("creating a second sink into the same table should not hang")?;
            sink_ids.push(job.id().as_object_id());
        }
        let owned_sink_name = "owned_sink_without_iceberg_prefix";
        let mut owned_sink = crate::manager::StreamingJob::Sink(
            PbSink {
                name: owned_sink_name.to_owned(),
                database_id: TEST_DATABASE_ID,
                schema_id: TEST_SCHEMA_ID,
                owner: TEST_OWNER_ID as _,
                target_table: Some(target_table_id),
                sink_type: PbSinkType::AppendOnly as i32,
                ..Default::default()
            },
            Some(target_table_id),
        );
        mgr.create_job_catalog(
            &mut owned_sink,
            &crate::model::StreamContext::default(),
            &None,
            1,
            HashSet::from([mv1_id.as_object_id()]),
            risingwave_pb::ddl_service::streaming_job_resource_type::ResourceType::Regular(true),
            &None,
            None,
            None,
            None,
            None,
        )
        .await?;
        let owned_sink_id = owned_sink.id().as_object_id();
        sink_ids.push(owned_sink_id);

        // Ensure the test sinks were created
        assert_eq!(sink_ids.len(), test_sink_tuples.len() + 1);

        let inner = mgr.inner.read().await;
        for sink_id in &sink_ids {
            streaming_job::ActiveModel {
                job_id: Set(sink_id.as_job_id()),
                job_status: Set(JobStatus::Created),
                ..Default::default()
            }
            .update(&inner.db)
            .await?;
        }
        // Ensure no object_dependency created for (target_table, sink) which could cause circular
        // issue
        let object_dependency_count = ObjectDependency::find()
            .filter(object_dependency::Column::Oid.eq(target_table_id.as_object_id()))
            .filter(object_dependency::Column::UsedBy.is_in(sink_ids.clone()))
            .count(&inner.db)
            .await?;
        assert_eq!(object_dependency_count, 0);
        assert_eq!(
            Object::find_by_id(owned_sink_id)
                .one(&inner.db)
                .await?
                .unwrap()
                .belong_to_oid,
            Some(target_table_id.as_object_id())
        );
        drop(inner);

        let error = mgr
            .drop_object(ObjectType::Table, target_table_id, DropMode::Restrict)
            .await
            .expect_err("RESTRICT drop should fail for a table with incoming sinks");
        assert_incoming_sink_drop_error(&error, &test_sink_tuples);
        assert!(
            !error.to_string().contains(owned_sink_name),
            "an owned incoming sink should not prevent a RESTRICT drop"
        );
        mgr.drop_object(ObjectType::Table, target_table_id, DropMode::Cascade)
            .await
            .expect("CASCADE drop should succeed");

        let inner = mgr.inner.read().await;
        let db = &inner.db;
        // Check that the cascade drop successfully dropped
        assert!(Object::find_by_id(target_table_id).one(db).await?.is_none());
        assert_eq!(
            Object::find()
                .filter(object::Column::Oid.is_in(sink_ids))
                .count(db)
                .await?,
            0
        );
        // Sanity checks that sources were not dropped
        assert!(Object::find_by_id(mv1_id).one(db).await?.is_some());
        assert!(Object::find_by_id(mv2_id).one(db).await?.is_some());

        Ok(())
    }

    #[tokio::test]
    async fn test_replace_upstream_object_rejects_creating_incoming_sink() -> MetaResult<()> {
        fn assert_replace_concurrency_error(error: &MetaError) {
            let message = error.to_string();
            // Ensures the replacement failed because a referring streaming job is still creating.
            assert!(
                message.contains("referenced by some creating jobs"),
                "expected a replace concurrency error, got: {message}"
            );
        }

        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;
        let inner = mgr.inner.write().await;
        let txn = inner.db.begin().await?;
        let (target_mv_id, Some(target_mv_table_id), _) =
            insert_test_streaming_job(&txn, "target_mv", true, None).await?
        else {
            unreachable!()
        };
        let (source_mv_id, Some(_), _) =
            insert_test_streaming_job(&txn, "source_mv", true, None).await?
        else {
            unreachable!()
        };
        txn.commit().await?;
        drop(inner);

        let mut sink = crate::manager::StreamingJob::Sink(
            PbSink {
                name: "creating_sink".to_owned(),
                database_id: TEST_DATABASE_ID,
                schema_id: TEST_SCHEMA_ID,
                owner: TEST_OWNER_ID as _,
                target_table: Some(target_mv_table_id),
                sink_type: PbSinkType::AppendOnly as i32,
                ..Default::default()
            },
            None,
        );
        let creating_sink = mgr
            .create_job_catalog(
                &mut sink,
                &crate::model::StreamContext::default(),
                &None,
                1,
                HashSet::from([source_mv_id.as_object_id()]),
                risingwave_pb::ddl_service::streaming_job_resource_type::ResourceType::Regular(
                    true,
                ),
                &None,
                None,
                None,
                None,
                None,
            )
            .await?;
        // Ensures the incoming sink is still creating, which should block replacement.
        assert_ne!(creating_sink.job_status, JobStatus::Created);

        let replacement = crate::manager::StreamingJob::MaterializedView(PbTable {
            id: target_mv_table_id,
            name: "target_mv".to_owned(),
            database_id: TEST_DATABASE_ID,
            schema_id: TEST_SCHEMA_ID,
            owner: TEST_OWNER_ID as _,
            ..Default::default()
        });
        // Ensures the replacement targets the upstream MV that the creating sink depends on.
        assert_eq!(replacement.id(), target_mv_id);

        // Ensures replacement rejects the upstream MV while its referring sink is creating.
        let error = mgr
            .create_job_catalog_for_replace(&replacement, None, None, None)
            .await
            .expect_err("replacement should reject a creating incoming sink");
        // Ensures the rejection error reports the expected concurrency reason.
        assert_replace_concurrency_error(&error);

        Ok(())
    }

    #[tokio::test]
    async fn test_replace_upstream_object_with_created_incoming_sink() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;
        let inner = mgr.inner.write().await;
        let txn = inner.db.begin().await?;
        let (_, Some(target_table_id), _) =
            insert_test_streaming_job(&txn, "target_table", true, None).await?
        else {
            unreachable!()
        };
        let (source_mv_id, Some(source_mv_table_id), _) =
            insert_test_streaming_job(&txn, "source_mv", true, None).await?
        else {
            unreachable!()
        };
        txn.commit().await?;
        drop(inner);

        let mut sink = crate::manager::StreamingJob::Sink(
            PbSink {
                name: "created_sink".to_owned(),
                database_id: TEST_DATABASE_ID,
                schema_id: TEST_SCHEMA_ID,
                owner: TEST_OWNER_ID as _,
                target_table: Some(target_table_id),
                sink_type: PbSinkType::AppendOnly as i32,
                ..Default::default()
            },
            None,
        );
        let creating_sink = mgr
            .create_job_catalog(
                &mut sink,
                &crate::model::StreamContext::default(),
                &None,
                1,
                HashSet::from([source_mv_id.as_object_id()]),
                risingwave_pb::ddl_service::streaming_job_resource_type::ResourceType::Regular(
                    true,
                ),
                &None,
                None,
                None,
                None,
                None,
            )
            .await?;
        // Ensures the incoming sink starts as a creating job before the test marks it created.
        assert_ne!(creating_sink.job_status, JobStatus::Created);

        let sink_id = sink.id();
        let inner = mgr.inner.read().await;
        streaming_job::ActiveModel {
            job_id: Set(sink_id),
            job_status: Set(JobStatus::Created),
            ..Default::default()
        }
        .update(&inner.db)
        .await?;
        let sink_model = risingwave_meta_model::prelude::StreamingJob::find_by_id(sink_id)
            .one(&inner.db)
            .await?
            .expect("sink should exist");
        // Ensures the persisted sink row is the sink created by this test.
        assert_eq!(sink_model.job_id, sink_id);
        // Ensures a fully created incoming sink does not block upstream MV replacement.
        assert_eq!(sink_model.job_status, JobStatus::Created);
        drop(inner);

        let replacement = crate::manager::StreamingJob::MaterializedView(PbTable {
            id: source_mv_table_id,
            name: "source_mv".to_owned(),
            database_id: TEST_DATABASE_ID,
            schema_id: TEST_SCHEMA_ID,
            owner: TEST_OWNER_ID as _,
            ..Default::default()
        });
        // Ensures the replacement targets the upstream MV that the created sink depends on.
        assert_eq!(replacement.id(), source_mv_id);

        let tmp_model = mgr
            .create_job_catalog_for_replace(&replacement, None, None, None)
            .await?;

        // Ensures replacement creates a distinct temporary job instead of reusing the original MV id.
        assert_ne!(tmp_model.job_id, source_mv_id);
        // Ensures the temporary replacement job is created but not finished yet.
        assert_eq!(tmp_model.job_status, JobStatus::Initial);

        let inner = mgr.inner.read().await;
        let db = &inner.db;
        // Ensures the created sink still depends on the upstream MV being replaced.
        assert_eq!(
            ObjectDependency::find()
                .filter(object_dependency::Column::Oid.eq(source_mv_id.as_object_id()))
                .filter(object_dependency::Column::UsedBy.eq(sink_id.as_object_id()))
                .count(db)
                .await?,
            1
        );
        // Ensures replacement records the temporary job as a dependent of the original MV.
        assert_eq!(
            ObjectDependency::find()
                .filter(object_dependency::Column::Oid.eq(source_mv_id.as_object_id()))
                .filter(object_dependency::Column::UsedBy.eq(tmp_model.job_id.as_object_id()))
                .count(db)
                .await?,
            1
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_table_refill_catalog_snapshot_classifies_table_identity() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;
        let inner = mgr.inner.write().await;
        let txn = inner.db.begin().await?;

        let (mv_job, Some(mv_result), mv_internal) =
            insert_test_streaming_job(&txn, "mv_both", true, Some(CacheRefillPolicy::Both)).await?
        else {
            unreachable!()
        };
        let (default_job, Some(_default_result), default_internal) =
            insert_test_streaming_job(&txn, "mv_default", true, None).await?
        else {
            unreachable!()
        };
        let (sink_job, None, sink_internal) = insert_test_streaming_job(
            &txn,
            "sink_streaming",
            false,
            Some(CacheRefillPolicy::Streaming),
        )
        .await?
        else {
            unreachable!()
        };

        let result_fragment = FragmentId::new(100);
        let internal_fragment = FragmentId::new(101);
        let sink_fragment = FragmentId::new(102);
        for (fragment_id, job_id, table_ids) in [
            (result_fragment, mv_job, vec![mv_result, mv_internal]),
            (internal_fragment, default_job, vec![default_internal]),
            (sink_fragment, sink_job, vec![sink_internal]),
        ] {
            insert_test_fragment(&txn, fragment_id, job_id, TableIdArray(table_ids)).await?;
        }
        txn.commit().await?;
        drop(inner);

        let serving_infos = mgr.fragment_serving_infos().await?;
        assert_eq!(serving_infos.len(), 3);
        assert_eq!(
            serving_infos[&result_fragment].result_table_id,
            Some(mv_result)
        );
        assert_eq!(serving_infos[&internal_fragment].result_table_id, None);
        assert_eq!(serving_infos[&sink_fragment].result_table_id, None);

        let policies = mgr.table_cache_refill_policies_snapshot().await?;
        assert_eq!(
            policies
                .table_policies
                .into_iter()
                .map(|policy| (policy.table_id, policy.policy))
                .collect::<HashMap<_, _>>(),
            HashMap::from([(
                mv_result.as_raw_id(),
                CacheRefillPolicy::Both.to_protobuf() as i32,
            )])
        );
        assert_eq!(
            policies
                .internal_table_policies
                .into_iter()
                .map(|policy| (policy.table_id, policy.policy))
                .collect::<HashMap<_, _>>(),
            HashMap::from([
                (
                    mv_internal.as_raw_id(),
                    CacheRefillPolicy::Both.to_protobuf() as i32,
                ),
                (
                    sink_internal.as_raw_id(),
                    CacheRefillPolicy::Streaming.to_protobuf() as i32,
                ),
            ])
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_foreground_creating_catalog_lifecycle() -> MetaResult<()> {
        let env = MetaSrvEnv::for_test().await;
        let (tx, mut notification_rx) = mpsc::unbounded_channel();
        env.notification_manager().insert_sender(
            SubscribeType::Frontend,
            WorkerKey(HostAddress {
                host: "localhost".to_owned(),
                port: 1234,
            }),
            tx,
        );
        let mgr = CatalogController::new(env).await?;
        let inner = mgr.inner.write().await;
        let txn = inner.db.begin().await?;

        // A foreground table and its internal table are both visible while creating.
        let (job_id, Some(table_id), internal_table_id) =
            insert_test_streaming_job(&txn, "creating_table", true, None).await?
        else {
            unreachable!()
        };
        let associated_source_id = CatalogController::create_object(
            &txn,
            ObjectType::Source,
            TEST_OWNER_ID,
            Some(TEST_SCHEMA_ID.as_object_id()),
        )
        .await?
        .oid
        .as_source_id();
        Source::insert(source::ActiveModel::from(PbSource {
            id: associated_source_id,
            schema_id: TEST_SCHEMA_ID,
            database_id: TEST_DATABASE_ID,
            name: "creating_table_source".to_owned(),
            owner: TEST_OWNER_ID as _,
            optional_associated_table_id: Some(
                risingwave_pb::catalog::source::OptionalAssociatedTableId::AssociatedTableId(
                    table_id,
                ),
            ),
            ..Default::default()
        }))
        .exec(&txn)
        .await?;
        table::ActiveModel {
            table_id: Set(table_id),
            table_type: Set(TableType::Table),
            optional_associated_source_id: Set(Some(associated_source_id)),
            ..Default::default()
        }
        .update(&txn)
        .await?;
        streaming_job::ActiveModel {
            job_id: Set(job_id),
            job_status: Set(JobStatus::Initial),
            ..Default::default()
        }
        .update(&txn)
        .await?;

        // A foreground index and its index table are both visible while creating.
        let (_primary_job_id, Some(primary_table_id), _) =
            insert_test_streaming_job(&txn, "primary_table", true, None).await?
        else {
            unreachable!()
        };
        let index_job_id = CatalogController::create_object(
            &txn,
            ObjectType::Index,
            TEST_OWNER_ID,
            Some(TEST_SCHEMA_ID.as_object_id()),
        )
        .await?
        .oid
        .as_job_id();
        let index_table_id = index_job_id.as_mv_table_id();
        insert_test_table(
            &txn,
            index_table_id,
            "creating_index",
            TableType::Index,
            None,
            "",
        )
        .await?;
        index::ActiveModel {
            index_id: Set(index_job_id.as_index_id()),
            name: Set("creating_index".to_owned()),
            index_table_id: Set(index_table_id),
            primary_table_id: Set(primary_table_id),
            index_items: Set(vec![].into()),
            index_column_properties: Set(None),
            index_columns_len: Set(0),
        }
        .insert(&txn)
        .await?;
        insert_test_streaming_job_model(&txn, index_job_id, None).await?;
        streaming_job::ActiveModel {
            job_id: Set(index_job_id),
            job_status: Set(JobStatus::Initial),
            ..Default::default()
        }
        .update(&txn)
        .await?;

        // A creating shared source is included in restart snapshots as well.
        let source_job_id = CatalogController::create_object(
            &txn,
            ObjectType::Source,
            TEST_OWNER_ID,
            Some(TEST_SCHEMA_ID.as_object_id()),
        )
        .await?
        .oid
        .as_job_id();
        Source::insert(source::ActiveModel::from(PbSource {
            id: source_job_id.as_shared_source_id(),
            schema_id: TEST_SCHEMA_ID,
            database_id: TEST_DATABASE_ID,
            name: "creating_shared_source".to_owned(),
            owner: TEST_OWNER_ID as _,
            info: Some(StreamSourceInfo {
                cdc_source_job: true,
                ..Default::default()
            }),
            ..Default::default()
        }))
        .exec(&txn)
        .await?;
        insert_test_streaming_job_model(&txn, source_job_id, None).await?;
        streaming_job::ActiveModel {
            job_id: Set(source_job_id),
            job_status: Set(JobStatus::Initial),
            ..Default::default()
        }
        .update(&txn)
        .await?;

        let sink_job_id = CatalogController::create_object(
            &txn,
            ObjectType::Sink,
            TEST_OWNER_ID,
            Some(TEST_SCHEMA_ID.as_object_id()),
        )
        .await?
        .oid
        .as_job_id();
        Sink::insert(sink::ActiveModel::from(PbSink {
            id: sink_job_id.as_sink_id(),
            schema_id: TEST_SCHEMA_ID,
            database_id: TEST_DATABASE_ID,
            name: "creating_sink".to_owned(),
            owner: TEST_OWNER_ID as _,
            sink_type: PbSinkType::AppendOnly as i32,
            ..Default::default()
        }))
        .exec(&txn)
        .await?;
        insert_test_streaming_job_model(&txn, sink_job_id, None).await?;
        streaming_job::ActiveModel {
            job_id: Set(sink_job_id),
            job_status: Set(JobStatus::Initial),
            ..Default::default()
        }
        .update(&txn)
        .await?;

        txn.commit().await?;

        let (catalog, _) = inner.snapshot().await?;
        assert!(!catalog.2.iter().any(|table| table.id == table_id));
        assert!(!catalog.2.iter().any(|table| table.id == internal_table_id));
        assert!(!catalog.2.iter().any(|table| table.id == index_table_id));
        assert!(
            !catalog
                .3
                .iter()
                .any(|source| source.id == associated_source_id)
        );
        assert!(
            !catalog
                .3
                .iter()
                .any(|source| source.id == source_job_id.as_shared_source_id())
        );
        assert!(
            !catalog
                .6
                .iter()
                .any(|index| index.id == index_job_id.as_index_id())
        );
        assert!(
            !catalog
                .4
                .iter()
                .any(|sink| sink.id == sink_job_id.as_sink_id())
        );

        drop(inner);

        let downstreams = FragmentDownstreamRelation::new();
        let mut add_notifications = vec![];
        for creating_job_id in [job_id, index_job_id, source_job_id, sink_job_id] {
            mgr.post_collect_job_fragments(creating_job_id, &downstreams, None, None, None, true)
                .await?;
            let notification = notification_rx
                .recv()
                .await
                .expect("frontend should receive a creating notification")
                .expect("creating notification should be valid");
            assert_eq!(notification.operation(), NotificationOperation::Add);
            let object_group = match notification.info {
                Some(NotificationInfo::ObjectGroup(object_group)) => object_group,
                other => panic!("unexpected notification: {other:?}"),
            };
            add_notifications.push(object_group);
        }

        assert!(add_notifications[0].objects.iter().any(|object| matches!(
            &object.object_info,
            Some(PbObjectInfo::Table(table)) if table.id == table_id
        )));
        assert!(add_notifications[0].objects.iter().any(|object| matches!(
            &object.object_info,
            Some(PbObjectInfo::Table(table)) if table.id == internal_table_id
        )));
        assert!(add_notifications[0].objects.iter().any(|object| matches!(
            &object.object_info,
            Some(PbObjectInfo::Source(source)) if source.id == associated_source_id
        )));
        assert!(add_notifications[1].objects.iter().any(|object| matches!(
            &object.object_info,
            Some(PbObjectInfo::Table(table)) if table.id == index_table_id
        )));
        assert!(add_notifications[1].objects.iter().any(|object| matches!(
            &object.object_info,
            Some(PbObjectInfo::Index(index)) if index.id == index_job_id.as_index_id()
        )));
        assert!(add_notifications[2].objects.iter().any(|object| matches!(
            &object.object_info,
            Some(PbObjectInfo::Source(source)) if source.id == source_job_id.as_shared_source_id()
        )));
        assert!(add_notifications[3].objects.iter().any(|object| matches!(
            &object.object_info,
            Some(PbObjectInfo::Sink(sink)) if sink.id == sink_job_id.as_sink_id()
        )));

        let inner = mgr.inner.write().await;
        let (catalog, _) = inner.snapshot().await?;
        assert!(catalog.2.iter().any(|table| table.id == table_id));
        assert!(catalog.2.iter().any(|table| table.id == internal_table_id));
        assert!(catalog.2.iter().any(|table| table.id == index_table_id));
        assert!(
            catalog
                .3
                .iter()
                .any(|source| source.id == source_job_id.as_shared_source_id())
        );
        assert!(
            catalog
                .6
                .iter()
                .any(|index| index.id == index_job_id.as_index_id())
        );
        assert!(
            catalog
                .4
                .iter()
                .any(|sink| sink.id == sink_job_id.as_sink_id())
        );

        let txn = inner.db.begin().await?;
        let (operation, _, _, _) = mgr.finish_streaming_job_inner(&txn, job_id).await?;
        assert_eq!(operation, NotificationOperation::Update);
        let (operation, _, _, _) = mgr.finish_streaming_job_inner(&txn, index_job_id).await?;
        assert_eq!(operation, NotificationOperation::Update);
        let (operation, _, _, _) = mgr.finish_streaming_job_inner(&txn, source_job_id).await?;
        assert_eq!(operation, NotificationOperation::Update);
        let (operation, _, _, _) = mgr.finish_streaming_job_inner(&txn, sink_job_id).await?;
        assert_eq!(operation, NotificationOperation::Update);
        txn.commit().await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_alter_streaming_job_cache_refill_policy_notifies_hummock() -> MetaResult<()> {
        let env = MetaSrvEnv::for_test().await;
        let (tx, mut rx) = mpsc::unbounded_channel();
        env.notification_manager().insert_sender(
            SubscribeType::Hummock,
            WorkerKey(HostAddress {
                host: "localhost".to_owned(),
                port: 1234,
            }),
            tx,
        );
        let mgr = CatalogController::new(env).await?;

        let inner = mgr.inner.write().await;
        let txn = inner.db.begin().await?;
        let (_job, Some(result_table_id), internal_table_id) =
            insert_test_streaming_job(&txn, "mv_cache_refill", true, None).await?
        else {
            unreachable!()
        };
        insert_test_fragment(
            &txn,
            FragmentId::new(200),
            result_table_id.as_job_id(),
            TableIdArray(vec![result_table_id, internal_table_id]),
        )
        .await?;
        txn.commit().await?;
        drop(inner);

        mgr.alter_streaming_job_config(
            result_table_id.as_job_id(),
            HashMap::from([(
                STREAMING_CACHE_REFILL_POLICY_CONFIG_PATH.to_owned(),
                "\"both\"".to_owned(),
            )]),
            vec![],
        )
        .await?;

        let response = rx
            .recv()
            .await
            .expect("should receive hummock notification")
            .expect("notification should be ok");
        assert_eq!(response.operation(), NotificationOperation::Update);
        let info = response.info;
        let Some(NotificationInfo::TableRefillRuntimeConfig(config)) = info else {
            panic!("unexpected notification: {:?}", info);
        };
        assert!(config.serving_table_vnode_mappings.is_none());
        let policies = config
            .table_cache_refill_policies
            .expect("policy snapshot should be present");
        assert_eq!(
            policies
                .table_policies
                .into_iter()
                .map(|policy| (policy.table_id, policy.policy))
                .collect::<HashMap<_, _>>(),
            HashMap::from([(
                result_table_id.as_raw_id(),
                CacheRefillPolicy::Both.to_protobuf() as i32,
            )])
        );
        assert_eq!(
            policies
                .internal_table_policies
                .into_iter()
                .map(|policy| (policy.table_id, policy.policy))
                .collect::<HashMap<_, _>>(),
            HashMap::from([(
                internal_table_id.as_raw_id(),
                CacheRefillPolicy::Both.to_protobuf() as i32,
            )])
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_prepare_streaming_job_cache_refill_policy_notifies_hummock() -> MetaResult<()> {
        let env = MetaSrvEnv::for_test().await;
        let (tx, mut rx) = mpsc::unbounded_channel();
        env.notification_manager().insert_sender(
            SubscribeType::Hummock,
            WorkerKey(HostAddress {
                host: "localhost".to_owned(),
                port: 1234,
            }),
            tx,
        );
        let (local_notification_tx, mut local_notification_rx) = mpsc::unbounded_channel();
        env.notification_manager()
            .insert_local_sender(local_notification_tx);
        let mgr = CatalogController::new(env).await?;

        let inner = mgr.inner.write().await;
        let txn = inner.db.begin().await?;
        let (job_id, Some(result_table_id), internal_table_id) = insert_test_streaming_job(
            &txn,
            "mv_initial_cache_refill",
            true,
            Some(CacheRefillPolicy::Both),
        )
        .await?
        else {
            unreachable!()
        };
        let (unprepared_job_id, Some(unprepared_result_table_id), unprepared_internal_table_id) =
            insert_test_streaming_job(
                &txn,
                "mv_unprepared_cache_refill",
                true,
                Some(CacheRefillPolicy::Serving),
            )
            .await?
        else {
            unreachable!()
        };
        for job_id in [job_id, unprepared_job_id] {
            streaming_job::ActiveModel {
                job_id: Set(job_id),
                job_status: Set(JobStatus::Initial),
                ..Default::default()
            }
            .update(&txn)
            .await?;
        }
        txn.commit().await?;
        drop(inner);

        let fragments = [Fragment {
            fragment_id: FragmentId::new(300),
            fragment_type_mask: FragmentTypeMask::default(),
            distribution_type: PbFragmentDistributionType::Hash,
            state_table_ids: vec![],
            maybe_vnode_count: Some(1),
            nodes: PbStreamNode::default(),
        }];
        mgr.prepare_streaming_job(
            job_id,
            || fragments.iter(),
            &FragmentDownstreamRelation::default(),
            true,
            None,
            None,
        )
        .await?;

        let local_notification = local_notification_rx
            .try_recv()
            .expect("should receive serving fragment mapping notification");
        let LocalNotification::ServingFragmentMappingsUpsert(fragment_ids) = local_notification
        else {
            panic!(
                "unexpected local notification before hummock notification: {:?}",
                local_notification
            );
        };
        assert_eq!(fragment_ids, vec![FragmentId::new(300).as_raw_id()]);

        let response = rx
            .recv()
            .await
            .expect("should receive hummock notification")
            .expect("notification should be ok");
        assert_eq!(response.operation(), NotificationOperation::Update);
        let info = response.info;
        let Some(NotificationInfo::TableRefillRuntimeConfig(config)) = info else {
            panic!("unexpected notification: {:?}", info);
        };
        assert!(config.serving_table_vnode_mappings.is_none());
        let policies = config
            .table_cache_refill_policies
            .expect("policy snapshot should be present");
        let table_policies = policies
            .table_policies
            .into_iter()
            .map(|policy| (policy.table_id, policy.policy))
            .collect::<HashMap<_, _>>();
        assert_eq!(
            table_policies,
            HashMap::from([(
                result_table_id.as_raw_id(),
                CacheRefillPolicy::Both.to_protobuf() as i32,
            )])
        );
        assert!(!table_policies.contains_key(&unprepared_result_table_id.as_raw_id()));
        let internal_table_policies = policies
            .internal_table_policies
            .into_iter()
            .map(|policy| (policy.table_id, policy.policy))
            .collect::<HashMap<_, _>>();
        assert_eq!(
            internal_table_policies,
            HashMap::from([(
                internal_table_id.as_raw_id(),
                CacheRefillPolicy::Both.to_protobuf() as i32,
            )])
        );
        assert!(!internal_table_policies.contains_key(&unprepared_internal_table_id.as_raw_id()));

        Ok(())
    }

    async fn insert_dirty_creating_job_with_fragment(
        mgr: &CatalogController,
        fragment_id: FragmentId,
        vnode_count: i32,
        fragment_type_mask: FragmentTypeMask,
    ) -> MetaResult<(JobId, TableId)> {
        let inner = mgr.inner.write().await;
        let txn = inner.db.begin().await?;
        let job_obj = CatalogController::create_object(
            &txn,
            ObjectType::Table,
            TEST_OWNER_ID,
            Some(TEST_SCHEMA_ID.as_object_id()),
        )
        .await?;
        let job_id = job_obj.oid.as_job_id();
        let table_id = job_id.as_mv_table_id();
        insert_test_table(
            &txn,
            table_id,
            "mv_dirty_serving_mapping",
            TableType::MaterializedView,
            None,
            "CREATE MATERIALIZED VIEW mv_dirty_serving_mapping AS SELECT 1",
        )
        .await?;
        table::ActiveModel {
            table_id: Set(table_id),
            engine: Set(Some(table::Engine::Hummock)),
            ..Default::default()
        }
        .update(&txn)
        .await?;
        streaming_job::ActiveModel {
            job_id: Set(job_id),
            job_status: Set(JobStatus::Creating),
            create_type: Set(CreateType::Foreground),
            timezone: Set(None),
            config_override: Set(None),
            adaptive_parallelism_strategy: Set(None),
            parallelism: Set(StreamingParallelism::Adaptive),
            backfill_parallelism: Set(None),
            backfill_adaptive_parallelism_strategy: Set(None),
            backfill_orders: Set(None),
            max_parallelism: Set(1),
            specific_resource_group: Set(None),
            is_serverless_backfill: Set(false),
            refresh_interval_sec: Set(None),
        }
        .insert(&txn)
        .await?;
        fragment::ActiveModel {
            fragment_id: Set(fragment_id),
            job_id: Set(job_id),
            fragment_type_mask: Set(fragment_type_mask.into()),
            distribution_type: Set(DistributionType::Hash),
            stream_node: Set(StreamNode::default()),
            state_table_ids: Set(Vec::<TableId>::new().into()),
            upstream_fragment_id: Set(Vec::<i32>::new().into()),
            vnode_count: Set(vnode_count),
            parallelism: Set(None),
        }
        .insert(&txn)
        .await?;
        txn.commit().await?;
        drop(inner);

        Ok((job_id, table_id))
    }

    #[tokio::test]
    async fn test_dirty_cleanup_reconcile_removes_stale_serving_vnode_mapping() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;
        let fragment_id = FragmentId::new(42);
        insert_dirty_creating_job_with_fragment(
            &mgr,
            fragment_id,
            VirtualNode::COUNT_FOR_TEST as i32,
            FragmentTypeMask::from(FragmentTypeFlag::Values as u32),
        )
        .await?;

        let worker = WorkerNode {
            id: WorkerId::new(1),
            r#type: WorkerType::ComputeNode.into(),
            host: Some(HostAddress {
                host: "localhost".to_owned(),
                port: 1,
            }),
            state: worker_node::State::Running as i32,
            property: Some(worker_node::Property {
                is_serving: true,
                parallelism: 1,
                ..Default::default()
            }),
            ..Default::default()
        };
        let serving_vnode_mapping = ServingVnodeMapping::default();
        let initial_snapshot = mgr.fragment_serving_infos().await?;
        serving_vnode_mapping.upsert(&initial_snapshot, std::slice::from_ref(&worker), None);
        assert!(serving_vnode_mapping.all().contains_key(&fragment_id));

        mgr.clean_dirty_creating_jobs(Some(TEST_DATABASE_ID))
            .await?;
        let current_snapshot = mgr.fragment_serving_infos().await?;
        assert!(!current_snapshot.contains_key(&fragment_id));

        serving_vnode_mapping.reconcile(&current_snapshot, &[worker], None);
        assert!(!serving_vnode_mapping.all().contains_key(&fragment_id));

        Ok(())
    }

    #[tokio::test]
    async fn test_clean_dirty_creating_jobs_keeps_job_without_values_fragment() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;
        let fragment_id = FragmentId::new(43);
        let (job_id, table_id) = insert_dirty_creating_job_with_fragment(
            &mgr,
            fragment_id,
            1,
            FragmentTypeMask::empty(),
        )
        .await?;

        let cleaned = mgr
            .clean_dirty_creating_jobs(Some(TEST_DATABASE_ID))
            .await?;
        assert!(cleaned.streaming_job_ids.is_empty());

        let inner = mgr.inner.read().await;
        assert!(Object::find_by_id(job_id).one(&inner.db).await?.is_some());
        assert!(
            StreamingJob::find_by_id(job_id)
                .one(&inner.db)
                .await?
                .is_some()
        );
        assert!(Table::find_by_id(table_id).one(&inner.db).await?.is_some());

        Ok(())
    }

    #[tokio::test]
    async fn test_clean_dirty_creating_jobs_cleans_foreground_job_in_legacy_mode() -> MetaResult<()>
    {
        let mut opts = MetaOpts::test(false);
        opts.clean_all_foreground_jobs_on_recovery = true;
        let mgr = CatalogController::new(MetaSrvEnv::for_test_opts(opts, |_| ()).await).await?;
        let (job_id, table_id) = insert_dirty_creating_job_with_fragment(
            &mgr,
            FragmentId::new(44),
            1,
            FragmentTypeMask::empty(),
        )
        .await?;

        let cleaned = mgr
            .clean_dirty_creating_jobs(Some(TEST_DATABASE_ID))
            .await?;
        assert_eq!(cleaned.streaming_job_ids, vec![job_id]);

        let db = &mgr.inner.read().await.db;
        assert!(Object::find_by_id(job_id).one(db).await?.is_none());
        assert!(StreamingJob::find_by_id(job_id).one(db).await?.is_none());
        assert!(Table::find_by_id(table_id).one(db).await?.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_database_func() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;
        let pb_database = PbDatabase {
            name: "db1".to_owned(),
            owner: TEST_OWNER_ID as _,
            ..Default::default()
        };
        mgr.create_database(pb_database).await?;

        let database_id: DatabaseId = Database::find()
            .select_only()
            .column(database::Column::DatabaseId)
            .filter(database::Column::Name.eq("db1"))
            .into_tuple()
            .one(&mgr.inner.read().await.db)
            .await?
            .unwrap();

        mgr.alter_name(ObjectType::Database, database_id, "db2")
            .await?;
        let database = Database::find_by_id(database_id)
            .one(&mgr.inner.read().await.db)
            .await?
            .unwrap();
        assert_eq!(database.name, "db2");

        let schema_id: SchemaId = Schema::find()
            .inner_join(Object)
            .select_only()
            .column(schema::Column::SchemaId)
            .filter(object::Column::DatabaseId.eq(database_id))
            .into_tuple()
            .one(&mgr.inner.read().await.db)
            .await?
            .unwrap();
        mgr.create_view(
            PbView {
                schema_id,
                database_id,
                name: "cross_db_upstream".to_owned(),
                owner: TEST_OWNER_ID as _,
                sql: "CREATE VIEW cross_db_upstream AS SELECT 1".to_owned(),
                ..Default::default()
            },
            HashSet::new(),
        )
        .await?;
        let upstream_id: ViewId = View::find()
            .inner_join(Object)
            .select_only()
            .column(view::Column::ViewId)
            .filter(
                object::Column::DatabaseId
                    .eq(database_id)
                    .and(view::Column::Name.eq("cross_db_upstream")),
            )
            .into_tuple()
            .one(&mgr.inner.read().await.db)
            .await?
            .unwrap();

        let inner = mgr.inner.write().await;
        let txn = inner.db.begin().await?;
        let (dependent_job_id, Some(dependent_table_id), _) =
            insert_test_streaming_job(&txn, "cross_db_dependent", true, None).await?
        else {
            unreachable!()
        };
        ObjectDependency::insert(object_dependency::ActiveModel {
            oid: Set(upstream_id.as_object_id()),
            used_by: Set(dependent_job_id.as_object_id()),
            ..Default::default()
        })
        .exec(&txn)
        .await?;
        txn.commit().await?;
        drop(inner);

        assert!(
            mgr.drop_object(ObjectType::Database, database_id, DropMode::Cascade)
                .await
                .is_err()
        );
        mgr.drop_object(ObjectType::Table, dependent_table_id, DropMode::Cascade)
            .await?;
        mgr.drop_object(ObjectType::Database, database_id, DropMode::Cascade)
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_schema_func() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;
        let pb_schema = PbSchema {
            database_id: TEST_DATABASE_ID,
            name: "schema1".to_owned(),
            owner: TEST_OWNER_ID as _,
            ..Default::default()
        };
        mgr.create_schema(pb_schema.clone()).await?;
        assert!(mgr.create_schema(pb_schema).await.is_err());

        let schema_id: SchemaId = Schema::find()
            .select_only()
            .column(schema::Column::SchemaId)
            .filter(schema::Column::Name.eq("schema1"))
            .into_tuple()
            .one(&mgr.inner.read().await.db)
            .await?
            .unwrap();

        mgr.alter_name(ObjectType::Schema, schema_id, "schema2")
            .await?;
        let schema = Schema::find_by_id(schema_id)
            .one(&mgr.inner.read().await.db)
            .await?
            .unwrap();
        assert_eq!(schema.name, "schema2");
        mgr.drop_object(ObjectType::Schema, schema_id, DropMode::Restrict)
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_create_view() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;
        let pb_view = PbView {
            schema_id: TEST_SCHEMA_ID,
            database_id: TEST_DATABASE_ID,
            name: "view".to_owned(),
            owner: TEST_OWNER_ID as _,
            sql: "CREATE VIEW view AS SELECT 1".to_owned(),
            ..Default::default()
        };
        mgr.create_view(pb_view.clone(), HashSet::new()).await?;
        assert!(mgr.create_view(pb_view, HashSet::new()).await.is_err());

        let view = View::find().one(&mgr.inner.read().await.db).await?.unwrap();
        mgr.drop_object(ObjectType::View, view.view_id, DropMode::Cascade)
            .await?;
        assert!(
            View::find_by_id(view.view_id)
                .one(&mgr.inner.read().await.db)
                .await?
                .is_none()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_object_belong_to_cascade() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;
        mgr.create_schema(PbSchema {
            database_id: TEST_DATABASE_ID,
            name: "belong_to_target".to_owned(),
            owner: TEST_OWNER_ID as _,
            ..Default::default()
        })
        .await?;
        let target_schema_id: SchemaId = Schema::find()
            .select_only()
            .column(schema::Column::SchemaId)
            .filter(schema::Column::Name.eq("belong_to_target"))
            .into_tuple()
            .one(&mgr.inner.read().await.db)
            .await?
            .unwrap();
        let txn = mgr.inner.read().await.db.begin().await?;

        let mv_obj = CatalogController::create_object(
            &txn,
            ObjectType::Table,
            TEST_OWNER_ID,
            Some(TEST_SCHEMA_ID.as_object_id()),
        )
        .await?;
        assert_eq!(mv_obj.belong_to_oid, Some(TEST_SCHEMA_ID.as_object_id()));
        assert_eq!(mv_obj.database_id, Some(TEST_DATABASE_ID));
        assert_eq!(mv_obj.schema_id, Some(TEST_SCHEMA_ID));
        let job_id = mv_obj.oid.as_job_id();
        let mv_table_id = job_id.as_mv_table_id();
        insert_test_table(
            &txn,
            mv_table_id,
            "mv_belong_to",
            TableType::MaterializedView,
            None,
            "CREATE MATERIALIZED VIEW mv_belong_to AS SELECT 1",
        )
        .await?;

        let internal_obj = CatalogController::create_object(
            &txn,
            ObjectType::Table,
            TEST_OWNER_ID,
            Some(job_id.as_object_id()),
        )
        .await?;
        assert_eq!(internal_obj.belong_to_oid, Some(job_id.as_object_id()));
        assert_eq!(internal_obj.database_id, Some(TEST_DATABASE_ID));
        assert_eq!(internal_obj.schema_id, Some(TEST_SCHEMA_ID));
        let internal_table_id = internal_obj.oid.as_table_id();
        insert_test_table(
            &txn,
            internal_table_id,
            "__internal_mv_belong_to",
            TableType::Internal,
            Some(job_id),
            "",
        )
        .await?;
        let nested_obj = CatalogController::create_object(
            &txn,
            ObjectType::Table,
            TEST_OWNER_ID,
            Some(internal_table_id.as_object_id()),
        )
        .await?;
        txn.commit().await?;

        assert!(
            mgr.alter_schema(ObjectType::Sink, job_id.as_object_id(), target_schema_id,)
                .await
                .is_err()
        );
        mgr.alter_schema(ObjectType::Table, job_id.as_object_id(), target_schema_id)
            .await?;

        let db = &mgr.inner.read().await.db;
        let belonging_object_ids = get_belong_objects(db, job_id.as_object_id())
            .await?
            .into_iter()
            .map(|object| object.oid)
            .collect::<HashSet<_>>();
        assert_eq!(
            belonging_object_ids,
            HashSet::from([internal_table_id.as_object_id(), nested_obj.oid])
        );
        let moved_objects = Object::find()
            .filter(object::Column::Oid.is_in([
                job_id.as_object_id(),
                internal_table_id.as_object_id(),
                nested_obj.oid,
            ]))
            .all(db)
            .await?;
        assert!(
            moved_objects
                .iter()
                .all(|object| object.schema_id == Some(target_schema_id))
        );
        assert_eq!(
            Object::find_by_id(internal_table_id)
                .one(db)
                .await?
                .unwrap()
                .belong_to_oid,
            Some(job_id.as_object_id())
        );
        assert_eq!(
            Object::find_by_id(nested_obj.oid)
                .one(db)
                .await?
                .unwrap()
                .belong_to_oid,
            Some(internal_table_id.as_object_id())
        );

        Object::delete_by_id(job_id).exec(db).await?;

        assert!(Object::find_by_id(job_id).one(db).await?.is_none());
        assert!(
            Object::find_by_id(internal_table_id)
                .one(db)
                .await?
                .is_none()
        );
        assert!(Table::find_by_id(mv_table_id).one(db).await?.is_none());
        assert!(
            Table::find_by_id(internal_table_id)
                .one(db)
                .await?
                .is_none()
        );
        assert!(Object::find_by_id(nested_obj.oid).one(db).await?.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_alter_internal_table_schema_rejected() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;
        mgr.create_schema(PbSchema {
            database_id: TEST_DATABASE_ID,
            name: "internal_table_alter_target".to_owned(),
            owner: TEST_OWNER_ID as _,
            ..Default::default()
        })
        .await?;
        let target_schema_id: SchemaId = Schema::find()
            .select_only()
            .column(schema::Column::SchemaId)
            .filter(schema::Column::Name.eq("internal_table_alter_target"))
            .into_tuple()
            .one(&mgr.inner.read().await.db)
            .await?
            .unwrap();

        let txn = mgr.inner.read().await.db.begin().await?;
        let parent_obj = CatalogController::create_object(
            &txn,
            ObjectType::Table,
            TEST_OWNER_ID,
            Some(TEST_SCHEMA_ID.as_object_id()),
        )
        .await?;
        let parent_job_id = parent_obj.oid.as_job_id();
        insert_test_table(
            &txn,
            parent_job_id.as_mv_table_id(),
            "internal_table_parent",
            TableType::MaterializedView,
            None,
            "",
        )
        .await?;
        let internal_obj = CatalogController::create_object(
            &txn,
            ObjectType::Table,
            TEST_OWNER_ID,
            Some(parent_job_id.as_object_id()),
        )
        .await?;
        let internal_table_id = internal_obj.oid.as_table_id();
        insert_test_table(
            &txn,
            internal_table_id,
            "__internal_table_alter_target",
            TableType::Internal,
            Some(parent_job_id),
            "",
        )
        .await?;
        txn.commit().await?;

        for new_schema in [TEST_SCHEMA_ID, target_schema_id] {
            assert!(
                mgr.alter_schema(
                    ObjectType::Table,
                    internal_table_id.as_object_id(),
                    new_schema,
                )
                .await
                .is_err()
            );
        }

        let internal_obj = Object::find_by_id(internal_table_id)
            .one(&mgr.inner.read().await.db)
            .await?
            .unwrap();
        assert_eq!(internal_obj.schema_id, Some(TEST_SCHEMA_ID));
        assert_eq!(
            internal_obj.belong_to_oid,
            Some(parent_job_id.as_object_id())
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_alter_table_schema_moves_indexes_but_not_subscriptions() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;
        mgr.create_schema(PbSchema {
            database_id: TEST_DATABASE_ID,
            name: "alter_table_target".to_owned(),
            owner: TEST_OWNER_ID as _,
            ..Default::default()
        })
        .await?;
        let target_schema_id: SchemaId = Schema::find()
            .select_only()
            .column(schema::Column::SchemaId)
            .filter(schema::Column::Name.eq("alter_table_target"))
            .into_tuple()
            .one(&mgr.inner.read().await.db)
            .await?
            .unwrap();

        let txn = mgr.inner.read().await.db.begin().await?;
        let table_obj = CatalogController::create_object(
            &txn,
            ObjectType::Table,
            TEST_OWNER_ID,
            Some(TEST_SCHEMA_ID.as_object_id()),
        )
        .await?;
        let table_id = table_obj.oid.as_table_id();
        insert_test_table(
            &txn,
            table_id,
            "mv_with_index_and_subscription",
            TableType::MaterializedView,
            None,
            "CREATE MATERIALIZED VIEW mv_with_index_and_subscription AS SELECT 1",
        )
        .await?;

        let index_obj = CatalogController::create_object(
            &txn,
            ObjectType::Index,
            TEST_OWNER_ID,
            Some(TEST_SCHEMA_ID.as_object_id()),
        )
        .await?;
        let index_id = index_obj.oid.as_index_id();
        let index_table_id = index_id.as_object_id().as_table_id();
        insert_test_table(
            &txn,
            index_table_id,
            "idx_mv_with_index_and_subscription_table",
            TableType::Index,
            None,
            "",
        )
        .await?;
        index::ActiveModel {
            index_id: Set(index_id),
            name: Set("idx_mv_with_index_and_subscription".to_owned()),
            index_table_id: Set(index_table_id),
            primary_table_id: Set(table_id),
            index_items: Set(Vec::<risingwave_pb::expr::ExprNode>::new().into()),
            index_column_properties: Set(None),
            index_columns_len: Set(0),
        }
        .insert(&txn)
        .await?;

        let index_internal_obj = CatalogController::create_object(
            &txn,
            ObjectType::Table,
            TEST_OWNER_ID,
            Some(index_id.as_object_id()),
        )
        .await?;
        let index_internal_table_id = index_internal_obj.oid.as_table_id();
        insert_test_table(
            &txn,
            index_internal_table_id,
            "__internal_idx_mv_with_index_and_subscription",
            TableType::Internal,
            Some(index_id.as_job_id()),
            "",
        )
        .await?;
        txn.commit().await?;

        let mut subscription = PbSubscription {
            name: "subscription_in_original_schema".to_owned(),
            definition: "CREATE SUBSCRIPTION subscription_in_original_schema FROM mv_with_index_and_subscription".to_owned(),
            retention_seconds: 86400,
            database_id: TEST_DATABASE_ID,
            schema_id: TEST_SCHEMA_ID,
            dependent_table_id: table_id,
            owner: TEST_OWNER_ID as _,
            subscription_state: SubscriptionState::Created as _,
            ..Default::default()
        };
        mgr.create_subscription_catalog(&mut subscription).await?;

        {
            let inner = mgr.inner.read().await;
            assert_eq!(
                Object::find_by_id(index_id)
                    .one(&inner.db)
                    .await?
                    .unwrap()
                    .belong_to_oid,
                Some(TEST_SCHEMA_ID.as_object_id())
            );
            assert_eq!(
                Object::find_by_id(subscription.id)
                    .one(&inner.db)
                    .await?
                    .unwrap()
                    .belong_to_oid,
                Some(TEST_SCHEMA_ID.as_object_id())
            );
        }

        mgr.alter_schema(ObjectType::Table, table_id.as_object_id(), target_schema_id)
            .await?;

        let db = &mgr.inner.read().await.db;
        for object_id in [table_id.as_object_id(), index_id.as_object_id()] {
            let object = Object::find_by_id(object_id).one(db).await?.unwrap();
            assert_eq!(object.schema_id, Some(target_schema_id));
            assert_eq!(object.belong_to_oid, Some(target_schema_id.as_object_id()));
        }
        let index_internal_object = Object::find_by_id(index_internal_table_id)
            .one(db)
            .await?
            .unwrap();
        assert_eq!(index_internal_object.schema_id, Some(target_schema_id));
        assert_eq!(
            index_internal_object.belong_to_oid,
            Some(index_id.as_object_id())
        );

        let subscription_object = Object::find_by_id(subscription.id).one(db).await?.unwrap();
        assert_eq!(subscription_object.schema_id, Some(TEST_SCHEMA_ID));
        assert_eq!(
            subscription_object.belong_to_oid,
            Some(TEST_SCHEMA_ID.as_object_id())
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_create_function() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;
        let test_data_type = risingwave_pb::data::DataType {
            type_name: risingwave_pb::data::data_type::TypeName::Int32 as _,
            ..Default::default()
        };
        let arg_types = vec![test_data_type.clone()];
        // This fixture represents an `IMMUTABLE` UDF. Function behavior is validated by the
        // frontend and is not persisted in `PbFunction`.
        let pb_function = PbFunction {
            schema_id: TEST_SCHEMA_ID,
            database_id: TEST_DATABASE_ID,
            name: "test_function".to_owned(),
            owner: TEST_OWNER_ID as _,
            arg_types,
            return_type: Some(test_data_type.clone()),
            language: "python".to_owned(),
            always_retry_on_network_error: true,
            unsafe_skip_materializing_exprs: true,
            kind: Some(risingwave_pb::catalog::function::Kind::Scalar(
                Default::default(),
            )),
            ..Default::default()
        };
        mgr.create_function(pb_function.clone()).await?;
        assert!(mgr.create_function(pb_function).await.is_err());

        let function = Function::find()
            .inner_join(Object)
            .filter(
                object::Column::DatabaseId
                    .eq(TEST_DATABASE_ID)
                    .and(object::Column::SchemaId.eq(TEST_SCHEMA_ID))
                    .add(function::Column::Name.eq("test_function")),
            )
            .one(&mgr.inner.read().await.db)
            .await?
            .unwrap();
        assert_eq!(function.return_type.to_protobuf(), test_data_type);
        assert_eq!(function.arg_types.to_protobuf().len(), 1);
        assert_eq!(function.language, "python");
        assert!(function.always_retry_on_network_error);
        assert!(function.unsafe_skip_materializing_exprs);

        mgr.create_schema(PbSchema {
            database_id: TEST_DATABASE_ID,
            name: "function_target".to_owned(),
            owner: TEST_OWNER_ID as _,
            ..Default::default()
        })
        .await?;
        let target_schema_id: SchemaId = Schema::find()
            .select_only()
            .column(schema::Column::SchemaId)
            .filter(schema::Column::Name.eq("function_target"))
            .into_tuple()
            .one(&mgr.inner.read().await.db)
            .await?
            .unwrap();
        mgr.alter_schema(
            ObjectType::Function,
            function.function_id.as_object_id(),
            target_schema_id,
        )
        .await?;
        assert_eq!(
            Object::find_by_id(function.function_id)
                .one(&mgr.inner.read().await.db)
                .await?
                .unwrap()
                .schema_id,
            Some(target_schema_id)
        );

        mgr.drop_object(
            ObjectType::Function,
            function.function_id,
            DropMode::Restrict,
        )
        .await?;
        assert!(
            Object::find_by_id(function.function_id)
                .one(&mgr.inner.read().await.db)
                .await?
                .is_none()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_alter_relation_rename() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;
        let pb_source = PbSource {
            schema_id: TEST_SCHEMA_ID,
            database_id: TEST_DATABASE_ID,
            name: "s1".to_owned(),
            owner: TEST_OWNER_ID as _,
            definition: r#"CREATE SOURCE s1 (v1 int) with (
  connector = 'kafka',
  topic = 'kafka_alter',
  properties.bootstrap.server = 'message_queue:29092',
  scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE JSON"#
                .to_owned(),
            info: Some(StreamSourceInfo {
                ..Default::default()
            }),
            ..Default::default()
        };
        mgr.create_source(pb_source, None).await?;
        let source_id: SourceId = Source::find()
            .select_only()
            .column(source::Column::SourceId)
            .filter(source::Column::Name.eq("s1"))
            .into_tuple()
            .one(&mgr.inner.read().await.db)
            .await?
            .unwrap();

        let pb_view = PbView {
            schema_id: TEST_SCHEMA_ID,
            database_id: TEST_DATABASE_ID,
            name: "view_1".to_owned(),
            owner: TEST_OWNER_ID as _,
            sql: "CREATE VIEW view_1 AS SELECT v1 FROM s1".to_owned(),
            ..Default::default()
        };
        mgr.create_view(pb_view, HashSet::from([source_id.as_object_id()]))
            .await?;
        let view_id: ViewId = View::find()
            .select_only()
            .column(view::Column::ViewId)
            .filter(view::Column::Name.eq("view_1"))
            .into_tuple()
            .one(&mgr.inner.read().await.db)
            .await?
            .unwrap();

        mgr.alter_name(ObjectType::Source, source_id, "s2").await?;
        let source = Source::find_by_id(source_id)
            .one(&mgr.inner.read().await.db)
            .await?
            .unwrap();
        assert_eq!(source.name, "s2");
        assert_eq!(
            source.definition,
            "CREATE SOURCE s2 (v1 INT) WITH (\
  connector = 'kafka', \
  topic = 'kafka_alter', \
  properties.bootstrap.server = 'message_queue:29092', \
  scan.startup.mode = 'earliest'\
) FORMAT PLAIN ENCODE JSON"
        );

        let view = View::find_by_id(view_id)
            .one(&mgr.inner.read().await.db)
            .await?
            .unwrap();
        assert_eq!(
            view.definition,
            "CREATE VIEW view_1 AS SELECT v1 FROM s2 AS s1"
        );

        mgr.drop_object(ObjectType::Source, source_id, DropMode::Cascade)
            .await?;
        assert!(
            View::find_by_id(view_id)
                .one(&mgr.inner.read().await.db)
                .await?
                .is_none()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_cancel_creating_table_deletes_associated_source() -> MetaResult<()> {
        let env = MetaSrvEnv::for_test().await;
        let (tx, mut notification_rx) = mpsc::unbounded_channel();
        env.notification_manager().insert_sender(
            SubscribeType::Frontend,
            WorkerKey(HostAddress {
                host: "localhost".to_owned(),
                port: 1234,
            }),
            tx,
        );
        let mgr = CatalogController::new(env).await?;

        let mut inner = mgr.inner.write().await;
        let txn = inner.db.begin().await?;
        let obj = CatalogController::create_object(
            &txn,
            ObjectType::Table,
            TEST_OWNER_ID,
            Some(TEST_SCHEMA_ID.as_object_id()),
        )
        .await?;
        let job_id = obj.oid.as_job_id();
        let source_obj = CatalogController::create_object(
            &txn,
            ObjectType::Source,
            TEST_OWNER_ID,
            Some(job_id.as_object_id()),
        )
        .await?;
        Source::insert(source::ActiveModel::from(PbSource {
            id: source_obj.oid.as_source_id(),
            schema_id: TEST_SCHEMA_ID,
            database_id: TEST_DATABASE_ID,
            name: "source_abort_initial".to_owned(),
            owner: TEST_OWNER_ID as _,
            ..Default::default()
        }))
        .exec(&txn)
        .await?;

        table::ActiveModel {
            table_id: Set(obj.oid.as_table_id()),
            name: Set("table_abort_initial".to_owned()),
            optional_associated_source_id: Set(Some(source_obj.oid.as_source_id())),
            table_type: Set(TableType::Table),
            belongs_to_job_id: Set(None),
            columns: Set(vec![].into()),
            pk: Set(vec![].into()),
            distribution_key: Set(Vec::<i32>::new().into()),
            stream_key: Set(Vec::<i32>::new().into()),
            append_only: Set(false),
            fragment_id: Set(None),
            vnode_col_index: Set(None),
            row_id_index: Set(None),
            value_indices: Set(Vec::<i32>::new().into()),
            definition: Set("CREATE TABLE table_abort_initial (v1 INT)".to_owned()),
            handle_pk_conflict_behavior: Set(HandleConflictBehavior::NoCheck),
            version_column_indices: Set(None),
            read_prefix_len_hint: Set(0),
            watermark_indices: Set(Vec::<i32>::new().into()),
            dist_key_in_pk: Set(Vec::<i32>::new().into()),
            dml_fragment_id: Set(None),
            cardinality: Set(None),
            cleaned_by_watermark: Set(false),
            description: Set(None),
            version: Set(None),
            retention_seconds: Set(None),
            cdc_table_id: Set(None),
            vnode_count: Set(1),
            webhook_info: Set(None),
            engine: Set(None),
            clean_watermark_index_in_pk: Set(None),
            clean_watermark_indices: Set(None),
            refreshable: Set(false),
            vector_index_info: Set(None),
            cdc_table_type: Set(None),
        }
        .insert(&txn)
        .await?;

        let internal_obj = CatalogController::create_object(
            &txn,
            ObjectType::Table,
            TEST_OWNER_ID,
            Some(job_id.as_object_id()),
        )
        .await?;
        let internal_table_id = internal_obj.oid.as_table_id();
        insert_test_table(
            &txn,
            internal_table_id,
            "__internal_mv_abort_initial",
            TableType::Internal,
            Some(job_id),
            "",
        )
        .await?;

        streaming_job::ActiveModel {
            job_id: Set(job_id),
            job_status: Set(JobStatus::Creating),
            create_type: Set(CreateType::Foreground),
            timezone: Set(None),
            config_override: Set(None),
            adaptive_parallelism_strategy: Set(None),
            parallelism: Set(StreamingParallelism::Adaptive),
            backfill_parallelism: Set(None),
            backfill_adaptive_parallelism_strategy: Set(None),
            backfill_orders: Set(None),
            max_parallelism: Set(1),
            specific_resource_group: Set(None),
            is_serverless_backfill: Set(false),
            refresh_interval_sec: Set(None),
        }
        .insert(&txn)
        .await?;

        let (tx, rx) = oneshot::channel();
        inner.register_finish_notifier(TEST_DATABASE_ID, job_id, tx);
        txn.commit().await?;
        drop(inner);

        let abort_result = mgr.try_abort_creating_streaming_job(job_id, true).await?;
        assert!(abort_result.aborted);
        assert_eq!(abort_result.database_id, Some(TEST_DATABASE_ID));

        let err = rx
            .await
            .expect("finish notifier should be notified")
            .expect_err("creating job cancellation should fail the create wait");
        assert!(err.contains("cancelled"));

        let db = &mgr.inner.read().await.db;
        assert!(Object::find_by_id(job_id).one(db).await?.is_none());
        assert!(StreamingJob::find_by_id(job_id).one(db).await?.is_none());
        assert!(
            Table::find_by_id(job_id.as_mv_table_id())
                .one(db)
                .await?
                .is_none()
        );
        assert!(
            Object::find_by_id(internal_table_id)
                .one(db)
                .await?
                .is_none()
        );
        assert!(
            Table::find_by_id(internal_table_id)
                .one(db)
                .await?
                .is_none()
        );
        assert!(
            mgr.inner
                .read()
                .await
                .dropped_tables
                .contains_key(&internal_table_id)
        );
        assert!(
            Source::find_by_id(source_obj.oid.as_source_id())
                .one(db)
                .await?
                .is_none()
        );

        let notification = notification_rx
            .recv()
            .await
            .expect("frontend should receive an abort notification")
            .expect("abort notification should be valid");
        assert_eq!(notification.operation(), NotificationOperation::Delete);
        let object_group = match notification.info {
            Some(NotificationInfo::ObjectGroup(object_group)) => object_group,
            other => panic!("unexpected notification: {other:?}"),
        };
        assert!(object_group.objects.iter().any(|object| matches!(
            &object.object_info,
            Some(PbObjectInfo::Table(table)) if table.id == job_id.as_mv_table_id()
        )));
        assert!(object_group.objects.iter().any(|object| matches!(
            &object.object_info,
            Some(PbObjectInfo::Source(source)) if source.id == source_obj.oid.as_source_id()
        )));

        Ok(())
    }

    #[tokio::test]
    async fn test_failed_foreground_creating_job_is_preserved() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;
        let (job_id, table_id) = insert_dirty_creating_job_with_fragment(
            &mgr,
            FragmentId::new(45),
            1,
            FragmentTypeMask::empty(),
        )
        .await?;

        let abort_result = mgr.try_abort_creating_streaming_job(job_id, false).await?;
        assert!(!abort_result.aborted);
        assert_eq!(abort_result.database_id, Some(TEST_DATABASE_ID));

        let db = &mgr.inner.read().await.db;
        assert!(Object::find_by_id(job_id).one(db).await?.is_some());
        assert!(StreamingJob::find_by_id(job_id).one(db).await?.is_some());
        assert!(Table::find_by_id(table_id).one(db).await?.is_some());

        Ok(())
    }

    #[tokio::test]
    async fn test_failed_created_job_is_preserved() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;
        let (job_id, table_id) = insert_dirty_creating_job_with_fragment(
            &mgr,
            FragmentId::new(46),
            1,
            FragmentTypeMask::empty(),
        )
        .await?;

        {
            let inner = mgr.inner.read().await;
            streaming_job::ActiveModel {
                job_id: Set(job_id),
                job_status: Set(JobStatus::Created),
                ..Default::default()
            }
            .update(&inner.db)
            .await?;
        }

        let abort_result = mgr.try_abort_creating_streaming_job(job_id, false).await?;
        assert!(!abort_result.aborted);
        assert_eq!(abort_result.database_id, Some(TEST_DATABASE_ID));
        let db = &mgr.inner.read().await.db;
        assert!(Object::find_by_id(job_id).one(db).await?.is_some());
        assert!(StreamingJob::find_by_id(job_id).one(db).await?.is_some());
        assert!(Table::find_by_id(table_id).one(db).await?.is_some());

        Ok(())
    }

    #[tokio::test]
    async fn test_clean_dirty_creating_jobs_records_dropped_tables_for_per_db_recovery()
    -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;

        let inner = mgr.inner.write().await;
        let txn = inner.db.begin().await?;
        let mv_obj = CatalogController::create_object(
            &txn,
            ObjectType::Table,
            TEST_OWNER_ID,
            Some(TEST_SCHEMA_ID.as_object_id()),
        )
        .await?;
        let job_id = mv_obj.oid.as_job_id();
        let mv_table_id = job_id.as_mv_table_id();
        insert_test_table(
            &txn,
            mv_table_id,
            "mv_dirty",
            TableType::MaterializedView,
            None,
            "CREATE MATERIALIZED VIEW mv_dirty AS SELECT 1",
        )
        .await?;

        let internal_obj = CatalogController::create_object(
            &txn,
            ObjectType::Table,
            TEST_OWNER_ID,
            Some(job_id.as_object_id()),
        )
        .await?;
        let internal_table_id = internal_obj.oid.as_table_id();
        insert_test_table(
            &txn,
            internal_table_id,
            "__internal_mv_dirty",
            TableType::Internal,
            Some(job_id),
            "",
        )
        .await?;

        streaming_job::ActiveModel {
            job_id: Set(job_id),
            job_status: Set(JobStatus::Initial),
            create_type: Set(CreateType::Foreground),
            timezone: Set(None),
            config_override: Set(None),
            adaptive_parallelism_strategy: Set(None),
            parallelism: Set(StreamingParallelism::Adaptive),
            backfill_parallelism: Set(None),
            backfill_adaptive_parallelism_strategy: Set(None),
            backfill_orders: Set(None),
            max_parallelism: Set(1),
            specific_resource_group: Set(None),
            is_serverless_backfill: Set(false),
            refresh_interval_sec: Set(None),
        }
        .insert(&txn)
        .await?;
        txn.commit().await?;
        drop(inner);

        let cleaned = mgr
            .clean_dirty_creating_jobs(Some(TEST_DATABASE_ID))
            .await?;
        assert_eq!(cleaned.streaming_job_ids, vec![job_id]);
        assert!(cleaned.source_ids.is_empty());
        let mut dropped_table_ids = cleaned.dropped_table_ids;
        dropped_table_ids.sort_unstable();
        assert_eq!(dropped_table_ids, vec![mv_table_id, internal_table_id]);

        let inner = mgr.inner.read().await;
        assert!(inner.dropped_tables.contains_key(&mv_table_id));
        assert!(inner.dropped_tables.contains_key(&internal_table_id));
        assert!(Object::find_by_id(job_id).one(&inner.db).await?.is_none());
        assert!(
            Object::find_by_id(internal_table_id)
                .one(&inner.db)
                .await?
                .is_none()
        );
        assert!(
            StreamingJob::find_by_id(job_id)
                .one(&inner.db)
                .await?
                .is_none()
        );
        assert!(
            Table::find_by_id(mv_table_id)
                .one(&inner.db)
                .await?
                .is_none()
        );
        assert!(
            Table::find_by_id(internal_table_id)
                .one(&inner.db)
                .await?
                .is_none()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_clean_dirty_creating_jobs_notifies_serving_mapping_fragment_delete()
    -> MetaResult<()> {
        let env = MetaSrvEnv::for_test().await;
        let (local_notification_tx, mut local_notification_rx) = mpsc::unbounded_channel();
        env.notification_manager()
            .insert_local_sender(local_notification_tx);
        let mgr = CatalogController::new(env).await?;
        let fragment_id = FragmentId::new(3);
        let (job_id, mv_table_id) = insert_dirty_creating_job_with_fragment(
            &mgr,
            fragment_id,
            1,
            FragmentTypeMask::from(FragmentTypeFlag::Values as u32),
        )
        .await?;

        assert!(
            mgr.fragment_serving_infos()
                .await?
                .contains_key(&fragment_id)
        );

        let cleaned = mgr
            .clean_dirty_creating_jobs(Some(TEST_DATABASE_ID))
            .await?;
        assert_eq!(cleaned.streaming_job_ids, vec![job_id]);

        let inner = mgr.inner.read().await;
        assert!(Object::find_by_id(job_id).one(&inner.db).await?.is_none());
        assert!(
            StreamingJob::find_by_id(job_id)
                .one(&inner.db)
                .await?
                .is_none()
        );
        assert!(
            Table::find_by_id(mv_table_id)
                .one(&inner.db)
                .await?
                .is_none()
        );
        drop(inner);
        assert!(
            !mgr.fragment_serving_infos()
                .await?
                .contains_key(&fragment_id)
        );

        let notification = local_notification_rx.try_recv().expect(
            "dirty-job cleanup must notify the serving mapping worker about deleted fragments",
        );
        match notification {
            LocalNotification::ServingFragmentMappingsDelete(fragment_ids) => {
                assert_eq!(fragment_ids, vec![fragment_id]);
            }
            notification => panic!("unexpected local notification: {notification:?}"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_abort_creating_subscription_commits_delete() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;
        let pb_view = PbView {
            schema_id: TEST_SCHEMA_ID,
            database_id: TEST_DATABASE_ID,
            name: "subscription_dep_view".to_owned(),
            owner: TEST_OWNER_ID as _,
            sql: "CREATE VIEW subscription_dep_view AS SELECT 1".to_owned(),
            ..Default::default()
        };
        mgr.create_view(pb_view, HashSet::new()).await?;

        let view_id: ViewId = View::find()
            .select_only()
            .column(view::Column::ViewId)
            .filter(view::Column::Name.eq("subscription_dep_view"))
            .into_tuple()
            .one(&mgr.inner.read().await.db)
            .await?
            .unwrap();

        let mut pb_subscription = PbSubscription {
            name: "subscription_to_abort".to_owned(),
            definition: "CREATE SUBSCRIPTION subscription_to_abort FROM subscription_dep_view"
                .to_owned(),
            retention_seconds: 86400,
            database_id: TEST_DATABASE_ID,
            schema_id: TEST_SCHEMA_ID,
            dependent_table_id: view_id.as_object_id().as_table_id(),
            owner: TEST_OWNER_ID as _,
            subscription_state: SubscriptionState::Init as _,
            ..Default::default()
        };
        mgr.create_subscription_catalog(&mut pb_subscription)
            .await?;

        mgr.try_abort_creating_subscription(pb_subscription.id)
            .await?;

        assert!(
            Subscription::find_by_id(pb_subscription.id)
                .one(&mgr.inner.read().await.db)
                .await?
                .is_none()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_drop_table_cascade_drops_dependent_subscription() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;

        let inner = mgr.inner.write().await;
        let txn = inner.db.begin().await?;
        let table_obj = CatalogController::create_object(
            &txn,
            ObjectType::Table,
            TEST_OWNER_ID,
            Some(TEST_SCHEMA_ID.as_object_id()),
        )
        .await?;
        let table_id = table_obj.oid.as_table_id();
        insert_test_table(
            &txn,
            table_id,
            "subscription_dep_table",
            TableType::Table,
            None,
            "CREATE TABLE subscription_dep_table (v1 INT)",
        )
        .await?;
        txn.commit().await?;
        drop(inner);

        let mut pb_subscription = PbSubscription {
            name: "subscription_to_drop_with_table".to_owned(),
            definition:
                "CREATE SUBSCRIPTION subscription_to_drop_with_table FROM subscription_dep_table"
                    .to_owned(),
            retention_seconds: 86400,
            database_id: TEST_DATABASE_ID,
            schema_id: TEST_SCHEMA_ID,
            dependent_table_id: table_id,
            owner: TEST_OWNER_ID as _,
            subscription_state: SubscriptionState::Created as _,
            ..Default::default()
        };
        mgr.create_subscription_catalog(&mut pb_subscription)
            .await?;

        mgr.drop_object(ObjectType::Table, table_id, DropMode::Cascade)
            .await?;

        let db = &mgr.inner.read().await.db;
        assert!(Table::find_by_id(table_id).one(db).await?.is_none());
        assert!(
            Object::find_by_id(table_id.as_object_id())
                .one(db)
                .await?
                .is_none()
        );
        assert!(
            Subscription::find_by_id(pb_subscription.id)
                .one(db)
                .await?
                .is_none()
        );
        assert!(
            Object::find_by_id(pb_subscription.id.as_object_id())
                .one(db)
                .await?
                .is_none()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_get_table_change_log_truncate_info() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;
        let pb_view = PbView {
            schema_id: TEST_SCHEMA_ID,
            database_id: TEST_DATABASE_ID,
            name: "change_log_upstream".to_owned(),
            owner: TEST_OWNER_ID as _,
            sql: "CREATE VIEW change_log_upstream AS SELECT 1".to_owned(),
            ..Default::default()
        };
        mgr.create_view(pb_view, HashSet::new()).await?;
        let upstream_table_id: TableId = View::find()
            .select_only()
            .column(view::Column::ViewId)
            .filter(view::Column::Name.eq("change_log_upstream"))
            .into_tuple::<ViewId>()
            .one(&mgr.inner.read().await.db)
            .await?
            .unwrap()
            .as_object_id()
            .as_table_id();
        let mut subscription = PbSubscription {
            name: "change_log_subscription".to_owned(),
            definition: "CREATE SUBSCRIPTION change_log_subscription FROM change_log_upstream"
                .to_owned(),
            retention_seconds: 123,
            database_id: TEST_DATABASE_ID,
            schema_id: TEST_SCHEMA_ID,
            dependent_table_id: upstream_table_id,
            owner: TEST_OWNER_ID as _,
            subscription_state: SubscriptionState::Created as _,
            ..Default::default()
        };
        mgr.create_subscription_catalog(&mut subscription).await?;

        let inner = mgr.inner.write().await;
        let txn = inner.db.begin().await?;
        let (job_id, _, state_table_id) =
            insert_test_streaming_job(&txn, "snapshot_job", true, None).await?;
        let mut job = streaming_job::Entity::find_by_id(job_id)
            .one(&txn)
            .await?
            .unwrap()
            .into_active_model();
        job.job_status = Set(JobStatus::Creating);
        job.update(&txn).await?;
        fragment::ActiveModel {
            fragment_id: Set(FragmentId::new(100)),
            job_id: Set(job_id),
            fragment_type_mask: Set(FragmentTypeFlag::SnapshotBackfillStreamScan as i32),
            distribution_type: Set(fragment::DistributionType::Hash),
            stream_node: Set(StreamNode::from(&PbStreamNode {
                node_body: Some(PbNodeBody::StreamScan(Box::new(StreamScanNode {
                    table_id: upstream_table_id,
                    stream_scan_type: StreamScanType::SnapshotBackfill as i32,
                    snapshot_backfill_epoch: None,
                    ..Default::default()
                }))),
                ..Default::default()
            })),
            state_table_ids: Set(vec![state_table_id].into()),
            upstream_fragment_id: Set(I32Array::default()),
            vnode_count: Set(1),
            parallelism: Set(None),
        }
        .insert(&txn)
        .await?;
        txn.commit().await?;
        drop(inner);

        let truncate_info = mgr.get_table_change_log_truncate_info().await?;
        assert_eq!(
            truncate_info.subscription_retention_seconds,
            HashMap::from([(upstream_table_id, 123)])
        );
        assert_eq!(truncate_info.independent_jobs.len(), 1);
        let independent_job = &truncate_info.independent_jobs[0];
        assert_eq!(independent_job.job_id, job_id);
        assert_eq!(
            independent_job.state_table_ids,
            HashSet::from([state_table_id])
        );
        assert_eq!(
            independent_job.upstream_table_snapshot_epochs,
            HashMap::from([(upstream_table_id, None)])
        );

        Ok(())
    }
}
