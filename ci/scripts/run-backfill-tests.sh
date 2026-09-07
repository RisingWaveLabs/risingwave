#!/usr/bin/env bash

# Runs backfill tests.

# USAGE:
# ```sh
# profile=(ci-release|ci-dev) ./ci/scripts/run-backfill-tests.sh
# ```
# Example progress:
# dev=> select * from rw_catalog.rw_ddl_progress;
# ddl_id |                 ddl_statement                  | progress |        initialized_at
#--------+------------------------------------------------+----------+-------------------------------
#   1002 | CREATE MATERIALIZED VIEW m1 AS SELECT * FROM t | 56.12%   | 2023-09-27 06:37:06.636+00:00
#(1 row)

# TODO: refactor with inline style.

set -euo pipefail

PARENT_PATH=$(dirname "${BASH_SOURCE[0]}")

TEST_DIR=$PWD/e2e_test
BACKGROUND_DDL_DIR=$TEST_DIR/background_ddl
COMMON_DIR=$BACKGROUND_DDL_DIR/common

CLUSTER_PROFILE='ci-1cn-1fe-user-kafka-with-recovery'
echo "--- Configuring cluster profiles"
if [[ -n "${BUILDKITE:-}" ]]; then
  echo "Running in buildkite"
  RUNTIME_CLUSTER_PROFILE='ci-backfill-3cn-1fe'
  MINIO_RATE_LIMIT_CLUSTER_PROFILE='ci-backfill-3cn-1fe-with-minio-rate-limit'
else
  echo "Running locally"
  RUNTIME_CLUSTER_PROFILE='ci-backfill-3cn-1fe-with-monitoring'
  MINIO_RATE_LIMIT_CLUSTER_PROFILE='ci-backfill-3cn-1fe-with-monitoring-and-minio-rate-limit'
fi
export RUST_LOG="info,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info,risingwave_stream::executor::stream_reader=warn" \

run_sql_file() {
  psql -h localhost -p 4566 -d dev -U root -f "$@"
}

run_sql() {
  psql -h localhost -p 4566 -d dev -U root -c "$@"
}

flush() {
  run_sql "FLUSH;"
}

cancel_stream_jobs() {
  ID=$(run_sql "select ddl_id from rw_catalog.rw_ddl_progress;" | tail -3 | head -1 | grep -E -o "[0-9]*")
  echo "CANCELLING STREAM_JOB: $ID"
  run_sql "CANCEL JOBS $ID;" </dev/null
}

# Prefix logs, so they don't get overridden after node restart.
rename_logs_with_prefix() {
  prefix="$1"
  pushd .risingwave/log
  for log in *.log
    do
      mv -- "$log" "${prefix}-${log}"
    done
  popd
}

kill_cluster() {
  risedev ci-kill
  wait
}

restart_cluster() {
  kill_cluster
  rename_logs_with_prefix "before-restart"
  risedev dev $CLUSTER_PROFILE
}

restart_cn() {
  tmux list-windows -t risedev | grep compute-node | grep -o "^[0-9]*" | xargs -I {} tmux send-keys -t %{} C-c C-d
  sleep 4
  mv .risingwave/log/compute-node-*.log .risingwave/log/before-restart-cn-compute-node.log
  ./.risingwave/bin/risingwave/compute-node \
  --config-path \
  ./.risingwave/config/risingwave.toml \
  --listen-addr \
  127.0.0.1:5688 \
  --prometheus-listener-addr \
  127.0.0.1:1222 \
  --advertise-addr \
  127.0.0.1:5688 \
  --async-stack-trace \
  verbose \
  --parallelism \
  4 \
  --total-memory-bytes \
  8589934592 \
  --role \
  both \
  --meta-address \
  http://127.0.0.1:5690 >.risingwave/log/compute-node.log 2>&1 &
}

# Test snapshot and upstream read.
test_snapshot_and_upstream_read() {
  echo "--- e2e, ci-backfill, test_snapshot_and_upstream_read"
  risedev ci-start ci-backfill
  run_sql_file "$PARENT_PATH"/sql/backfill/basic/create_base_table.sql

  # Provide snapshot
  run_sql_file "$PARENT_PATH"/sql/backfill/basic/insert.sql

  run_sql "alter table t1 set dml_rate_limit = 10"

  # Provide updates ...
  run_sql_file "$PARENT_PATH"/sql/backfill/basic/insert.sql &

  # ... and concurrently create mv.
  run_sql_file "$PARENT_PATH"/sql/backfill/basic/create_mv.sql && run_sql "alter table t1 set dml_rate_limit = default" &

  wait

  run_sql_file "$PARENT_PATH"/sql/backfill/basic/select.sql </dev/null

  risedev kill
  risedev wait-processes-exit
}

# Lots of upstream tombstone, backfill should still proceed.
test_backfill_tombstone() {
  echo "--- e2e, test_backfill_tombstone"
  risedev ci-start $CLUSTER_PROFILE
  risedev psql -c "
  CREATE TABLE tomb (v1 int)
  WITH (
    connector = 'datagen',
    fields.v1._.kind = 'sequence',
    datagen.rows.per.second = '2000000'
  )
  FORMAT PLAIN
  ENCODE JSON;
  "

  sleep 10

  bash -c '
    set -euo pipefail

    for i in $(seq 1 1000)
    do
      risedev psql -c "DELETE FROM tomb; FLUSH;"
      sleep 1
    done
  ' 1>deletes.log 2>&1 &

  risedev psql -c "SET streaming_use_snapshot_backfill = false; CREATE MATERIALIZED VIEW m1 as select * from tomb;"
  echo "--- Kill cluster"
  kill_cluster
  wait
}

test_replication_with_column_pruning() {
  echo "--- e2e, test_replication_with_column_pruning"
  RUST_LOG="risingwave_storage::hummock::event_handler::hummock_event_handler=error" risedev ci-start ci-backfill
  run_sql_file "$PARENT_PATH"/sql/backfill/replication_with_column_pruning/create_base_table.sql
  # Provide snapshot
  run_sql_file "$PARENT_PATH"/sql/backfill/replication_with_column_pruning/insert.sql

  run_sql_file "$PARENT_PATH"/sql/backfill/replication_with_column_pruning/create_mv.sql &

  # Provide upstream updates
  run_sql_file "$PARENT_PATH"/sql/backfill/replication_with_column_pruning/insert.sql &

  wait

  run_sql_file "$PARENT_PATH"/sql/backfill/replication_with_column_pruning/select.sql </dev/null
  run_sql_file "$PARENT_PATH"/sql/backfill/replication_with_column_pruning/drop.sql
  echo "--- Kill cluster"
  kill_cluster
}

# Test sink backfill recovery
test_sink_backfill_recovery() {
  echo "--- e2e, test_sink_backfill_recovery"
  risedev ci-start $CLUSTER_PROFILE

  # Check progress
  sqllogictest -p 4566 -d dev 'e2e_test/backfill/sink/create_sink.slt'

  # Sleep before restart cluster, to ensure the downstream sink actually gets created.
  sleep 5

  # Restart
  restart_cluster
  sleep 5

  # Sink back into rw
  run_sql "CREATE TABLE table_kafka (v1 int primary key)
    WITH (
      connector = 'kafka',
      topic = 's_kafka',
      properties.bootstrap.server = 'message_queue:29092',
  ) FORMAT DEBEZIUM ENCODE JSON;"

  sleep 10

  # Verify data matches upstream table.
  sqllogictest -p 4566 -d dev 'e2e_test/backfill/sink/validate_sink.slt'
  kill_cluster
}

# Test sink backfill order validation
test_sink_backfill_order_validation() {
  echo "--- e2e, test_sink_backfill_order_validation"
  risedev ci-start ci-backfill
  sqllogictest -p 4566 -d dev 'e2e_test/backfill/sink/test_sink_backfill_order_validation.slt'
  kill_cluster
}

test_arrangement_backfill_snapshot_and_upstream_runtime() {
  echo "--- e2e, test_arrangement_backfill_snapshot_and_upstream_runtime, $RUNTIME_CLUSTER_PROFILE"
  risedev ci-start $RUNTIME_CLUSTER_PROFILE
  sqllogictest -p 4566 -d dev 'e2e_test/backfill/runtime/create_table.slt'
  sqllogictest -p 4566 -d dev 'e2e_test/backfill/runtime/insert_snapshot.slt'
  sqllogictest -p 4566 -d dev 'e2e_test/backfill/runtime/insert_upstream.slt' 2>&1 1>out.log &
  echo "[INFO] Upstream is ingesting in background"
  sqllogictest -p 4566 -d dev 'e2e_test/backfill/runtime/create_arrangement_backfill_mv.slt'

  wait

  sqllogictest -p 4566 -d dev 'e2e_test/backfill/runtime/validate_rows_arrangement.slt'

  risedev ci-kill
}

test_no_shuffle_backfill_snapshot_and_upstream_runtime() {
  echo "--- e2e, test_no_shuffle_backfill_snapshot_and_upstream_runtime, $RUNTIME_CLUSTER_PROFILE"
  risedev ci-start $RUNTIME_CLUSTER_PROFILE
  sqllogictest -p 4566 -d dev 'e2e_test/backfill/runtime/create_table.slt'
  sqllogictest -p 4566 -d dev 'e2e_test/backfill/runtime/insert_snapshot.slt'
  sqllogictest -p 4566 -d dev 'e2e_test/backfill/runtime/insert_upstream.slt' 2>&1 1>out.log &
  echo "[INFO] Upstream is ingesting in background"
  sqllogictest -p 4566 -d dev 'e2e_test/backfill/runtime/create_no_shuffle_mv.slt'

  wait

  sqllogictest -p 4566 -d dev 'e2e_test/backfill/runtime/validate_rows_no_shuffle.slt'

  kill_cluster
}

test_backfill_snapshot_runtime() {
  echo "--- e2e, test_backfill_snapshot_runtime, $RUNTIME_CLUSTER_PROFILE"
  risedev ci-start $RUNTIME_CLUSTER_PROFILE
  sqllogictest -p 4566 -d dev 'e2e_test/backfill/runtime/create_table.slt'
  sqllogictest -p 4566 -d dev 'e2e_test/backfill/runtime/insert_snapshot.slt'
  sqllogictest -p 4566 -d dev 'e2e_test/backfill/runtime/create_arrangement_backfill_mv.slt'
  sqllogictest -p 4566 -d dev 'e2e_test/backfill/runtime/create_no_shuffle_mv.slt'
  sqllogictest -p 4566 -d dev 'e2e_test/backfill/runtime/validate_rows_no_shuffle.slt'
  sqllogictest -p 4566 -d dev 'e2e_test/backfill/runtime/validate_rows_arrangement.slt'

  kill_cluster
}

# Throttle the storage throughput.
# Arrangement Backfill should not fail because of this.
test_backfill_snapshot_with_limited_storage_throughput() {
  echo "--- e2e, test_backfill_snapshot_with_limited_storage_throughput, $MINIO_RATE_LIMIT_CLUSTER_PROFILE"
  risedev ci-start $MINIO_RATE_LIMIT_CLUSTER_PROFILE
  sqllogictest -p 4566 -d dev 'e2e_test/backfill/runtime/create_table.slt'
  sqllogictest -p 4566 -d dev 'e2e_test/backfill/runtime/insert_snapshot.slt'
  sqllogictest -p 4566 -d dev 'e2e_test/backfill/runtime/create_arrangement_backfill_mv.slt'
  sqllogictest -p 4566 -d dev 'e2e_test/backfill/runtime/create_no_shuffle_mv.slt'
  sqllogictest -p 4566 -d dev 'e2e_test/backfill/runtime/validate_rows_no_shuffle.slt'
  sqllogictest -p 4566 -d dev 'e2e_test/backfill/runtime/validate_rows_arrangement.slt'

  kill_cluster
}

# Test case where we do backfill with PK of 10 columns to measure performance impact.
test_backfill_snapshot_with_wider_rows() {
  echo "--- e2e, test_backfill_snapshot_with_wider_rows, $RUNTIME_CLUSTER_PROFILE"
  risedev ci-start $RUNTIME_CLUSTER_PROFILE
  sqllogictest -p 4566 -d dev 'e2e_test/backfill/runtime/create_wide_table.slt'
  sqllogictest -p 4566 -d dev 'e2e_test/backfill/runtime/insert_wide_snapshot.slt'
  sqllogictest -p 4566 -d dev 'e2e_test/backfill/runtime/create_arrangement_backfill_mv.slt'
  sqllogictest -p 4566 -d dev 'e2e_test/backfill/runtime/create_no_shuffle_mv.slt'
  sqllogictest -p 4566 -d dev 'e2e_test/backfill/runtime/validate_rows_no_shuffle.slt'
  sqllogictest -p 4566 -d dev 'e2e_test/backfill/runtime/validate_rows_arrangement.slt'

  kill_cluster
}

test_snapshot_backfill() {
  echo "--- e2e, snapshot backfill test, $RUNTIME_CLUSTER_PROFILE"

  risedev ci-start $RUNTIME_CLUSTER_PROFILE

  sqllogictest -p 4566 -d dev 'e2e_test/backfill/snapshot_backfill/create_nexmark_table.slt'

  # sleep for a while to let table accumulate enough data
  sleep 10

  psql -h localhost -p 4566 -d dev -U root -c 'ALTER SYSTEM SET max_concurrent_creating_streaming_jobs TO 4;'

  TEST_NAME=nexmark_q3 sqllogictest -p 4566 -d dev 'e2e_test/backfill/snapshot_backfill/nexmark/nexmark_q3.slt' &
  TEST_NAME=nexmark_q7 sqllogictest -p 4566 -d dev 'e2e_test/backfill/snapshot_backfill/nexmark/nexmark_q7.slt' &

  wait

  TEST_NAME=nexmark_q3 sqllogictest -p 4566 -d dev 'e2e_test/backfill/snapshot_backfill/scale.slt' &
  TEST_NAME=nexmark_q7 sqllogictest -p 4566 -d dev 'e2e_test/backfill/snapshot_backfill/scale.slt' &

  wait

  psql -h localhost -p 4566 -d dev -U root -c 'RECOVER'

  sleep 3

  TEST_NAME=nexmark_q3 sqllogictest -p 4566 -d dev 'e2e_test/backfill/snapshot_backfill/check_data_equal.slt.part' &
  TEST_NAME=nexmark_q7 sqllogictest -p 4566 -d dev 'e2e_test/backfill/snapshot_backfill/check_data_equal.slt.part' &

  wait

  TEST_NAME=nexmark_q3 sqllogictest -p 4566 -d dev 'e2e_test/backfill/snapshot_backfill/drop_mv.slt' &
  TEST_NAME=nexmark_q7 sqllogictest -p 4566 -d dev 'e2e_test/backfill/snapshot_backfill/drop_mv.slt' &

  wait

  sqllogictest -p 4566 -d dev 'e2e_test/backfill/snapshot_backfill/drop_nexmark_table.slt'

  sqllogictest -p 4566 -d dev 'e2e_test/backfill/snapshot_backfill/failed_tests.slt'

  sqllogictest -p 4566 -d dev 'e2e_test/backfill/snapshot_backfill/pk_predicate_pushdown.slt'

  sqllogictest -p 4566 -d dev 'e2e_test/backfill/snapshot_backfill/distinct_on_join_double_insert.slt'

  kill_cluster
}

test_scale_in() {
  echo "--- e2e, test_scale_in, 3cn -> 1cn"
  RUST_LOG=info risedev ci-start ci-3cn-1fe-with-recovery
  risedev psql-env
  source .risingwave/config/psql-env

  psql -c "alter system set per_database_isolation = false"

  psql -c "create table t(v1 int); insert into t select * from generate_series(1, 1000); flush"
  psql -c "set background_ddl=true; set backfill_rate_limit=10; set streaming_use_snapshot_backfill=false; create materialized view m1 as select * from t; flush"
  internal_table=$(psql -t -c "show internal tables;" | grep -v 'INFO')

  for i in $(seq 1 100000); do
    sleep 1
    progress=$(psql -c "select progress from rw_ddl_progress" | grep -o "[0-9]*\.[0-9]*%" |  sed 's/\([0-9]*\)\..*/\1/' )
    echo "progress ${progress}%"

    if [[ "$progress" -gt 70 ]]; then
      echo "hit 70% progress, setting backfill_rate_limit=1"
      psql -c 'alter materialized view m1 set backfill_rate_limit=1;'
      break
    fi
  done

  for i in $(seq 1 100000); do
    sleep 1
    progress=$(psql -c "select progress from rw_ddl_progress" | grep -o "[0-9]*\.[0-9]*%" |  sed 's/\([0-9]*\)\..*/\1/' )
    echo "progress ${progress}%"

    if [[ "$progress" -gt 90 ]]; then
      echo "hit 90% progress, setting backfill_rate_limit=0"
      psql -c 'alter materialized view m1 set backfill_rate_limit=0;'
      result=$(psql -t -c "select count(*) from ${internal_table} where backfill_finished=true;")
      if [[ "$result" -gt 0 ]]; then
        echo "some backfill_finished is set, breaking loop"
        break
      elif [[ "$result" -eq 0 ]]; then
        echo "backfill_finished is not set, setting backfill_rate_limit=1"
        psql -c 'alter materialized view m1 set backfill_rate_limit=1;'
      fi
    fi
  done

  # If jobs are already completed, we can't continue running this test.
  not_completed=$(psql -t -c "select count(*) from ${internal_table} where backfill_finished=false;")
  if [[ "$not_completed" -eq 0 ]]; then
    echo "All jobs are completed, can't continue running this test."
    exit 0
  elif [[ "$not_completed" -gt 0 ]]; then
    echo "Some jobs are not completed, continuing test. Remaining jobs: ${not_completed}"
  fi

  # First insert a bunch of rows to increase snapshot size.
  psql -c "insert into t select * from generate_series(1, 100000); flush"

  # Kill old cluster
  kill_cluster

  # Scale in
  RUST_LOG=info risedev d ci-1cn-1fe-with-recovery

  for i in $(seq 1 100000); do
    sleep 1
    is_recovered=$(psql -t -c "select rw_recovery_status()" | xargs | tr -s ' ')
    echo "recovery_status: $is_recovered"
    if [[ "$is_recovered" == "RUNNING" ]]; then
      break
    fi
  done

  # Resume backfill fully
  psql -c "alter materialized view m1 set backfill_rate_limit=default;"

  # wait for backfill to complete
  echo "waiting for backfill"

  psql -c "wait"

  # check rows should be 1000 + 100000 = 101000
  result=$(psql -t -c "select count(*) from m1;")
  echo "result: $result"
  if [[ "$result" -eq 101000 ]]; then
    echo "backfill is successful"
  else
    echo "backfill is not successful"
    exit 1
  fi

  # kill cluster finish test
  kill_cluster
  risedev clean-data
}

test_cross_db_snapshot_backfill() {
  echo "--- e2e, cross db snapshot backfill test, $RUNTIME_CLUSTER_PROFILE"

  risedev ci-start $RUNTIME_CLUSTER_PROFILE

  sqllogictest -p 4566 -d dev 'e2e_test/backfill/cross_db/cross_db_mv.slt'

  kill_cluster
}

test_locality_backfill() {
  echo "--- e2e, locality backfill test, $RUNTIME_CLUSTER_PROFILE"

  risedev ci-start $RUNTIME_CLUSTER_PROFILE

  sqllogictest -p 4566 -d dev 'e2e_test/backfill/locality_backfill/*.slt'

  kill_cluster
}

main() {
  set -euo pipefail
  test_snapshot_and_upstream_read
  test_backfill_tombstone
  test_replication_with_column_pruning
  test_sink_backfill_recovery
  test_sink_backfill_order_validation
  test_snapshot_backfill

  test_scale_in

  test_cross_db_snapshot_backfill
  test_locality_backfill

  # Only if profile is "ci-release", run it.
  if [[ ${profile:-} == "ci-release" ]]; then
    echo "--- Using release profile, running backfill performance tests."
    # Need separate tests, we don't want to backfill concurrently.
    # It's difficult to measure the time taken for each backfill if we do so.
    test_no_shuffle_backfill_snapshot_and_upstream_runtime
    test_arrangement_backfill_snapshot_and_upstream_runtime

    # Backfill will happen in sequence here.
    test_backfill_snapshot_runtime
    test_backfill_snapshot_with_wider_rows
    test_backfill_snapshot_with_limited_storage_throughput

    # No upstream only tests, because if there's no snapshot,
    # Backfill will complete almost immediately.
  fi
}

main
