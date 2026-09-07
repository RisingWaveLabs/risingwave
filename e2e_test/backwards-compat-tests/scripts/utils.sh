#!/usr/bin/env bash

# NOTE(kwannoel):
# Do not run this script directly, it is meant to be sourced.
# Backwards compatibility tests consist of the following parts:
#
# 1. Setup old cluster binaries.
# 2. Seed old cluster.
# 3. Start the latest stable patch of each intermediate release line and wait
#    for successful recovery.
# 4. Setup new cluster binaries and wait for successful recovery.
# 5. Run validation on new cluster.
#
# Steps 1,3,4 are specific to the execution environment, CI / Local.
# This script provides the shared version, recovery, seeding, and validation utilities.

################################### ENVIRONMENT CONFIG

# Maximum duration to wait for recovery (seconds)
RECOVERY_TIMEOUT=300
# The `RECOVER` command was introduced in this version.
RECOVER_COMMAND_MIN_VERSION=1.9.0

# Setup test directory
TEST_DIR=.risingwave/e2e_test/backwards-compat-tests/
# Keep this case on recently verified releases. It relies on Hummock system tables and
# risectl commands to build a mixed-table SST deterministically.
HUMMOCK_STALE_TABLE_IDS_MIN_VERSION=2.8.0
# v2.8.4 backported normalized compact task table ids, so old clusters at this
# version and later already clean stale table ids from SST metadata.
HUMMOCK_STALE_TABLE_IDS_FIXED_VERSION=2.8.4
# This case relies on cross-database streaming queries and subscriptions on the
# old cluster before upgrade, and only targets upgrades crossing 2.8 -> 3.0.
CROSS_DB_SUBSCRIPTION_MIN_VERSION=2.2.0
CROSS_DB_SUBSCRIPTION_MAX_OLD_VERSION=2.8.999
CROSS_DB_SUBSCRIPTION_MIN_NEW_VERSION=3.0.0
mkdir -p $TEST_DIR
cp -r e2e_test/backwards-compat-tests/slt/* $TEST_DIR

wait_for_process() {
  process_name="$1"

  while pgrep -x "$process_name" >/dev/null; do
    echo "Process $process_name is still running... Wait for 1 sec"
    sleep 1
  done
}

wait_all_process_exit() {
  wait_for_process meta-node
  wait_for_process compute-node
  wait_for_process frontend
  wait_for_process compactor
  echo "All processes has exited."
}

# Older versions of RW may not gracefully kill kafka.
# So we duplicate the definition here.
kill_cluster() {
  if tmux -L risedev ls &>/dev/null; then
    TMUX="tmux -L risedev"
  else
    TMUX="tmux"
  fi

  # Kill other components
  $TMUX list-windows -t risedev -F "#{window_name} #{pane_id}" |
    awk '{ print $2 }' |
    xargs -I {} $TMUX send-keys -t {} C-c C-d

  $TMUX kill-server
  test $? -eq 0 || {
    echo "Failed to stop all RiseDev components."
    exit 1
  }
  wait_all_process_exit
}

run_sql() {
  psql -h localhost -p 4566 -d dev -U root -c "$@"
}

run_sql_db() {
  local db="$1"
  shift
  psql -h localhost -p 4566 -d "$db" -U root -c "$@"
}

run_sql_scalar() {
  psql -h localhost -p 4566 -d dev -U root -At -c "$@" | tr -d '[:space:]'
}

run_sql_scalar_db() {
  local db="$1"
  shift
  psql -h localhost -p 4566 -d "$db" -U root -At -c "$@" | tr -d '[:space:]'
}

wait_for_recovery() {
  local version="$1"
  local deadline=$((SECONDS + RECOVERY_TIMEOUT))
  local recover_output=""

  echo "--- Wait for a successful RECOVER command on version ${version}"
  while (( SECONDS < deadline )); do
    if recover_output=$(run_sql "RECOVER;" 2>&1); then
      echo "$recover_output"
      echo "--- RECOVER succeeded on version ${version}"
      return
    fi
    echo "RECOVER is not ready on version ${version}; retry in 1 second"
    sleep 1
  done

  echo "$recover_output"
  echo "Timed out after ${RECOVERY_TIMEOUT}s waiting for RECOVER on version ${version}"
  return 1
}

run_risectl() (
  set -euo pipefail
  set -a
  source .risingwave/config/risedev-env
  set +a
  .risingwave/bin/risingwave/risectl "$@"
)

check_version() {
  local VERSION=$1
  local raw_version=$(run_sql "SELECT version();")
  echo "--- Version"
  echo "raw_version: $raw_version"
  local version=$(echo $raw_version | grep -i risingwave | sed 's/^.*risingwave-\([0-9]*\.[0-9]*\.[0-9]\).*$/\1/i')
  if [[ "$version" != "$VERSION" ]]; then
    echo "Version mismatch, expected $VERSION, got $version"
    exit 1
  fi
}

create_kafka_topic() {
  RPK_BROKERS=message_queue:29092 \
  rpk topic create backwards_compat_test_kafka_source
}

insert_json_kafka() {
  local JSON=$1

  echo "$JSON" | \
  RPK_BROKERS=message_queue:29092 \
  rpk topic produce backwards_compat_test_kafka_source -f "%k,%v"
}

seed_json_kafka() {
  insert_json_kafka '{"user_id": 1},{"timestamp": "2023-07-28 07:11:00", "user_id": 1, "page_id": 1, "action": "gtrgretrg"}'
  insert_json_kafka '{"user_id": 2},{"timestamp": "2023-07-28 07:11:00", "user_id": 2, "page_id": 1, "action": "fsdfgerrg"}'
  insert_json_kafka '{"user_id": 3},{"timestamp": "2023-07-28 07:11:00", "user_id": 3, "page_id": 1, "action": "sdfergtth"}'
  insert_json_kafka '{"user_id": 4},{"timestamp": "2023-07-28 06:54:00", "user_id": 4, "page_id": 2, "action": "erwerhghj"}'
  insert_json_kafka '{"user_id": 5},{"timestamp": "2023-07-28 06:54:00", "user_id": 5, "page_id": 2, "action": "kiku7ikkk"}'
  insert_json_kafka '{"user_id": 6},{"timestamp": "2023-07-28 06:54:00", "user_id": 6, "page_id": 3, "action": "6786745ge"}'
  insert_json_kafka '{"user_id": 7},{"timestamp": "2023-07-28 06:54:00", "user_id": 7, "page_id": 3, "action": "fgbgfnyyy"}'
  insert_json_kafka '{"user_id": 8},{"timestamp": "2023-07-28 06:54:00", "user_id": 8, "page_id": 4, "action": "werwerwwe"}'
  insert_json_kafka '{"user_id": 9},{"timestamp": "2023-07-28 06:54:00", "user_id": 9, "page_id": 4, "action": "yjtyjtyyy"}'
}

seed_hummock_stale_table_ids() {
  rm -f "$TEST_DIR/hummock-stale-table-ids/enabled" \
    "$TEST_DIR/hummock-stale-table-ids/dropped_table_id"

  if version_lt "$OLD_VERSION" "$HUMMOCK_STALE_TABLE_IDS_MIN_VERSION" ||
    version_le "$HUMMOCK_STALE_TABLE_IDS_FIXED_VERSION" "$OLD_VERSION"; then
    echo "--- HUMMOCK STALE TABLE IDS TEST: Skipped for old version ${OLD_VERSION}"
    return
  fi

  echo "--- HUMMOCK STALE TABLE IDS TEST: Seeding old cluster with mixed-table SST"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/hummock-stale-table-ids/seed.slt"

  local live_table_id
  live_table_id=$(run_sql_scalar "SELECT id FROM rw_catalog.rw_tables WHERE name = 'hummock_stale_live';")
  local dropped_table_id
  dropped_table_id=$(run_sql_scalar "SELECT id FROM rw_catalog.rw_tables WHERE name = 'hummock_stale_dropped';")
  if [[ -z "$live_table_id" || -z "$dropped_table_id" ]]; then
    echo "Failed to capture hummock stale table ids: live=${live_table_id}, dropped=${dropped_table_id}"
    exit 1
  fi

  local live_compaction_group_id
  live_compaction_group_id=$(run_sql_scalar "SELECT compaction_group_id FROM rw_catalog.rw_hummock_sstables WHERE table_ids @> '[${live_table_id}]'::jsonb LIMIT 1;")
  local dropped_compaction_group_id
  dropped_compaction_group_id=$(run_sql_scalar "SELECT compaction_group_id FROM rw_catalog.rw_hummock_sstables WHERE table_ids @> '[${dropped_table_id}]'::jsonb LIMIT 1;")
  if [[ -z "$live_compaction_group_id" || -z "$dropped_compaction_group_id" ]]; then
    echo "Failed to capture hummock compaction groups: live=${live_compaction_group_id}, dropped=${dropped_compaction_group_id}"
    exit 1
  fi

  # Put both tables into one compaction group so a manual L0 compaction can
  # rewrite their SST metadata into one mixed-table SST.
  if [[ "$live_compaction_group_id" != "$dropped_compaction_group_id" ]]; then
    run_risectl hummock merge-compaction-group \
      --left-group-id "$live_compaction_group_id" \
      --right-group-id "$dropped_compaction_group_id"
  fi

  # Target the exact L0 SSTs that contain either table id. This avoids depending
  # on the default manual selector deciding that there is enough data to compact.
  local target_sst_ids
  target_sst_ids=$(run_sql_scalar "
    SELECT string_agg(sstable_id::varchar, ',' ORDER BY sub_level_id, sstable_id)
    FROM rw_catalog.rw_hummock_sstables
    WHERE compaction_group_id = ${live_compaction_group_id}
      AND level_id = 0
      AND (
        table_ids @> '[${live_table_id}]'::jsonb
        OR table_ids @> '[${dropped_table_id}]'::jsonb
      );
  ")
  if [[ -z "$target_sst_ids" ]]; then
    echo "Failed to find L0 SSTs for hummock stale table ids test: live_table_id=${live_table_id}, dropped_table_id=${dropped_table_id}, compaction_group_id=${live_compaction_group_id}"
    exit 1
  fi

  # `risectl` does not expose a reliable shell exit status for "no task picked",
  # so the test verifies success by observing the resulting SST metadata below.
  run_risectl hummock trigger-manual-compaction \
    --compaction-group-id "$live_compaction_group_id" \
    --level 0 \
    --sst-ids "$target_sst_ids"

  # Fail closed unless the current Hummock version really contains an SST whose
  # metadata references both the live table and the table that will be dropped.
  local mixed_sst_count
  for _ in $(seq 1 60); do
    mixed_sst_count=$(run_sql_scalar "SELECT count(*) FROM rw_catalog.rw_hummock_sstables WHERE table_ids @> '[${live_table_id}]'::jsonb AND table_ids @> '[${dropped_table_id}]'::jsonb;")
    if [[ "$mixed_sst_count" != "0" ]]; then
      break
    fi
    sleep 1
  done
  if [[ "$mixed_sst_count" == "0" ]]; then
    echo "Failed to build mixed-table SST for hummock stale table ids test"
    exit 1
  fi

  echo "$dropped_table_id" > "$TEST_DIR/hummock-stale-table-ids/dropped_table_id"

  # Simulate the old-version upgrade state: the dropped table is unregistered
  # from compaction group configs, while old SST metadata can still contain it.
  run_sql "DROP TABLE hummock_stale_dropped;"
  run_sql "FLUSH;"

  local registered_stale_table_count
  registered_stale_table_count=$(run_sql_scalar "SELECT count(*) FROM rw_catalog.rw_hummock_compaction_group_configs WHERE member_tables @> '[${dropped_table_id}]'::jsonb;")
  if [[ "$registered_stale_table_count" != "0" ]]; then
    echo "Old cluster did not unregister dropped table id ${dropped_table_id}"
    exit 1
  fi

  local stale_sst_count
  stale_sst_count=$(run_sql_scalar "SELECT count(*) FROM rw_catalog.rw_hummock_sstables WHERE table_ids @> '[${dropped_table_id}]'::jsonb;")
  if [[ "$stale_sst_count" == "0" ]]; then
    echo "Old cluster did not retain stale SST table id ${dropped_table_id}; the backwards compatibility test would be ineffective"
    exit 1
  fi

  echo "--- HUMMOCK STALE TABLE IDS TEST: Validating old cluster"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/hummock-stale-table-ids/validate_original.slt"
  touch "$TEST_DIR/hummock-stale-table-ids/enabled"
}

validate_hummock_stale_table_ids() {
  if [[ ! -f "$TEST_DIR/hummock-stale-table-ids/enabled" ]]; then
    echo "--- HUMMOCK STALE TABLE IDS TEST: Skipped"
    return
  fi

  echo "--- HUMMOCK STALE TABLE IDS TEST: Validating new cluster before compaction"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/hummock-stale-table-ids/validate_restart.slt"

  local dropped_table_id
  dropped_table_id=$(cat "$TEST_DIR/hummock-stale-table-ids/dropped_table_id")
  local stale_sst_count
  stale_sst_count=$(run_sql_scalar "SELECT count(*) FROM rw_catalog.rw_hummock_sstables WHERE table_ids @> '[${dropped_table_id}]'::jsonb;")
  if [[ "$stale_sst_count" != "0" ]]; then
    echo "Recovered new cluster still has SST metadata containing dropped table id ${dropped_table_id}"
    exit 1
  fi
}

# https://stackoverflow.com/a/4024263
version_le() {
  printf '%s\n' "$1" "$2" | sort -C -V
}

version_lt() {
  ! version_le "$2" "$1"
}

seed_cross_db_subscription_validation() {
  local test_name="CROSS DATABASE SUBSCRIPTION TEST"
  local test_path="$TEST_DIR/cross-db-subscription"
  rm -rf "$test_path"
  mkdir -p "$test_path"

  if version_lt "$OLD_VERSION" "$CROSS_DB_SUBSCRIPTION_MIN_VERSION" ||
    version_lt "$CROSS_DB_SUBSCRIPTION_MAX_OLD_VERSION" "$OLD_VERSION" ||
    version_lt "$NEW_VERSION" "$CROSS_DB_SUBSCRIPTION_MIN_NEW_VERSION"; then
    echo "--- ${test_name}: Skipped for OLD_VERSION=${OLD_VERSION}, NEW_VERSION=${NEW_VERSION}"
    return
  fi

  echo "--- ${test_name}: Seeding old cluster"
  run_sql "CREATE DATABASE db1;"
  run_sql "CREATE DATABASE db2;"

  run_sql_db "db1" "
    CREATE TABLE t1 (v1 int)
    WITH (
      connector = 'datagen',
      datagen.rows.per.second = '10'
    )
    FORMAT PLAIN
    ENCODE JSON;
  "
  run_sql_db "db1" "CREATE SUBSCRIPTION sub_t1 FROM t1 WITH (retention = '1d');"
  run_sql_db "db2" "CREATE MATERIALIZED VIEW mv AS SELECT * FROM db1.public.t1;"

  echo "--- ${test_name}: Wait 60s for streaming data and changelog epochs"
  sleep 60

  local min_epoch
  local max_epoch
  min_epoch=$(run_sql_scalar_db "db1" "
    WITH epochs AS (
      SELECT
        (jsonb_array_elements_text((jsonb_array_elements(change_log->'changeLogs'))->'epochs'))::bigint AS epoch
      FROM rw_catalog.rw_hummock_table_change_log
    )
    SELECT min(epoch) FROM epochs;
  ")
  max_epoch=$(run_sql_scalar_db "db1" "
    WITH epochs AS (
      SELECT
        (jsonb_array_elements_text((jsonb_array_elements(change_log->'changeLogs'))->'epochs'))::bigint AS epoch
      FROM rw_catalog.rw_hummock_table_change_log
    )
    SELECT max(epoch) FROM epochs;
  ")

  if [[ -z "$min_epoch" || -z "$max_epoch" ]]; then
    echo "${test_name}: Failed to capture epoch window on old cluster"
    exit 1
  fi

  local epoch_count_before_upgrade
  epoch_count_before_upgrade=$(run_sql_scalar_db "db1" "
    WITH epochs AS (
      SELECT
        (jsonb_array_elements_text((jsonb_array_elements(change_log->'changeLogs'))->'epochs'))::bigint AS epoch
      FROM rw_catalog.rw_hummock_table_change_log
    )
    SELECT count(*) FROM epochs WHERE epoch >= ${min_epoch} AND epoch <= ${max_epoch};
  ")

  if [[ -z "$epoch_count_before_upgrade" ]]; then
    echo "${test_name}: Failed to capture epoch count on old cluster"
    exit 1
  fi

  echo "$min_epoch" > "$test_path/min_epoch"
  echo "$max_epoch" > "$test_path/max_epoch"
  echo "$epoch_count_before_upgrade" > "$test_path/epoch_count"
  touch "$test_path/enabled"
}

query_cross_db_epoch_count() {
  local min_epoch="$1"
  local max_epoch="$2"

  run_sql_scalar_db "db1" "
    WITH epochs AS (
      SELECT
        (jsonb_array_elements_text((jsonb_array_elements(change_log->'changeLogs'))->'epochs'))::bigint AS epoch
      FROM rw_catalog.rw_hummock_table_change_log
    )
    SELECT count(*) FROM epochs WHERE epoch >= ${min_epoch} AND epoch <= ${max_epoch};
  "
}

log_cross_db_counts() {
  run_sql "
    SELECT count(*) AS cnt, 'db1' AS src FROM db1.public.t1
    UNION ALL
    SELECT count(*) AS cnt, 'db2' AS src FROM db2.public.mv;
  "
}

assert_cross_db_counts_increasing() {
  local before_t1
  local before_mv
  local after_t1
  local after_mv

  before_t1=$(run_sql_scalar_db "db1" "SELECT count(*) FROM t1;")
  before_mv=$(run_sql_scalar_db "db2" "SELECT count(*) FROM mv;")
  log_cross_db_counts

  echo "--- CROSS DATABASE SUBSCRIPTION TEST: Wait 10s before rechecking row counts"
  sleep 10

  after_t1=$(run_sql_scalar_db "db1" "SELECT count(*) FROM t1;")
  after_mv=$(run_sql_scalar_db "db2" "SELECT count(*) FROM mv;")
  log_cross_db_counts

  if (( after_t1 <= before_t1 )); then
    echo "CROSS DATABASE SUBSCRIPTION TEST: db1.public.t1 count did not increase (${before_t1} -> ${after_t1})"
    exit 1
  fi

  if (( after_mv <= before_mv )); then
    echo "CROSS DATABASE SUBSCRIPTION TEST: db2.mv count did not increase (${before_mv} -> ${after_mv})"
    exit 1
  fi
}

log_cross_db_epoch_count() {
  local min_epoch="$1"
  local max_epoch="$2"
  local epoch_count="$3"
  local phase="$4"

  echo "CROSS DATABASE SUBSCRIPTION TEST: epoch window ${phase}, min_epoch=${min_epoch}, max_epoch=${max_epoch}, epoch_count=${epoch_count}"
}

assert_cross_db_epoch_count_matches() {
  local expected_count="$1"
  local actual_count="$2"
  local min_epoch="$3"
  local max_epoch="$4"
  local phase="$5"

  log_cross_db_epoch_count "$min_epoch" "$max_epoch" "$actual_count" "$phase"

  if [[ "$actual_count" != "$expected_count" ]]; then
    echo "CROSS DATABASE SUBSCRIPTION TEST: epoch count mismatch ${phase}, expected ${expected_count}, got ${actual_count}"
    exit 1
  fi
}

restart_meta_node() {
  local tmux_cmd="tmux"
  if tmux -L risedev ls &>/dev/null; then
    tmux_cmd="tmux -L risedev"
  fi

  local meta_window_index
  meta_window_index=$($tmux_cmd list-windows -t risedev -F "#{window_index} #{window_name}" | awk '$2 ~ /^meta-node/ {print $1; exit}')
  if [[ -z "$meta_window_index" ]]; then
    echo "Failed to find meta-node tmux window"
    exit 1
  fi

  echo "--- Restart meta node"
  $tmux_cmd respawn-window -k -t "risedev:${meta_window_index}"
  wait_for_recovery "$NEW_VERSION after meta restart"
}

validate_cross_db_subscription_after_upgrade() {
  local test_name="CROSS DATABASE SUBSCRIPTION TEST"
  local test_path="$TEST_DIR/cross-db-subscription"

  if [[ ! -f "$test_path/enabled" ]]; then
    echo "--- ${test_name}: Skipped"
    return
  fi

  local min_epoch
  local max_epoch
  local expected_epoch_count
  min_epoch=$(cat "$test_path/min_epoch")
  max_epoch=$(cat "$test_path/max_epoch")
  expected_epoch_count=$(cat "$test_path/epoch_count")

  echo "--- ${test_name}: Validate row count growth after upgrade"
  assert_cross_db_counts_increasing

  echo "--- ${test_name}: Validate epoch window after upgrade"
  local epoch_count_after_upgrade
  epoch_count_after_upgrade=$(query_cross_db_epoch_count "$min_epoch" "$max_epoch")
  assert_cross_db_epoch_count_matches "$expected_epoch_count" "$epoch_count_after_upgrade" "$min_epoch" "$max_epoch" "after upgrade"

  restart_meta_node

  echo "--- ${test_name}: Validate row count growth meta restart"
  assert_cross_db_counts_increasing

  echo "--- ${test_name}: Validate epoch window after meta restart"
  local epoch_count_after_meta_restart
  epoch_count_after_meta_restart=$(query_cross_db_epoch_count "$min_epoch" "$max_epoch")
  assert_cross_db_epoch_count_matches "$expected_epoch_count" "$epoch_count_after_meta_restart" "$min_epoch" "$max_epoch" "after meta restart"
}

################################### Entry Points

get_old_version() {
  # For backwards compat test we assume we are testing the latest version of RW (i.e. latest main commit)
  # against the Nth latest release candidate, where N > 1. N can be larger,
  # in case some old cluster did not upgrade.
  if [[ -z ${VERSION_OFFSET:-} ]]; then
    local VERSION_OFFSET=1
  fi

  # First get the new version to offset against
  get_new_version
  echo "--- NEW VERSION: $NEW_VERSION"

  # Extract X.Y.Z tags
  echo "--- VERSION BRANCHES"
  local tags=$(git tag | grep -E "^v[0-9]+\.[0-9]+\.[0-9]+$" | tr -d 'v' | tr -d ' ')
  echo "$tags"

  # Then we sort them in descending order.
  echo "--- VERSIONS"
  local sorted_versions=$(echo -e "$tags" | sort -V)
  echo "$sorted_versions"

  # Find the index of NEW_VERSION in the sorted list
  local new_version_index=$(echo -e "$sorted_versions" | grep -n "$NEW_VERSION" | cut -d: -f1)
  if [[ -z $new_version_index ]]; then
    echo "Could not find NEW_VERSION ($NEW_VERSION) in git tags, looking for latest version in same minor series"
    # Extract major.minor from NEW_VERSION
    local major_minor=$(echo "$NEW_VERSION" | cut -d. -f1,2)
    # Find the latest version in the same minor series
    local latest_in_series=$(echo -e "$sorted_versions" | grep "^$major_minor\." | tail -n1)
    if [[ -n "$latest_in_series" ]]; then
      echo "Found latest version in series $major_minor: $latest_in_series"
      new_version_index=$(echo -e "$sorted_versions" | grep -n "$latest_in_series" | cut -d: -f1)
    else
      echo "No version found in series $major_minor, using latest tag as reference"
      # Get the total number of versions
      local total_versions=$(echo -e "$sorted_versions" | wc -l)
      # Use the latest version's index + 1 as reference
      new_version_index=$((total_versions + 1))
    fi
    echo "Using reference index: $new_version_index"
  fi

  # Calculate the target index by subtracting the offset
  local target_index=$((new_version_index - VERSION_OFFSET))

  # Get the version at the target index
  OLD_VERSION=$(echo -e "$sorted_versions" | sed -n "${target_index}p")
  if [[ -z $OLD_VERSION ]]; then
    echo "Error: Could not find version at offset $VERSION_OFFSET from reference version"
    exit 1
  fi
  echo "--- OLD VERSION: $OLD_VERSION"
}

get_new_version() {
  # Next, for $NEW_VERSION we just scrape it from `workspace.package.version`.
  NEW_VERSION=$(cat Cargo.toml | grep "\[workspace\.package\]" -A 5 | sed -n 's/version = \"\([0-9]*\.[0-9]*\.[0-9]*\).*/\1/p' | tr -d ' ')
}

# Get $OLD_VERSION and $NEW_VERSION for Risingwave
get_rw_versions() {
  get_old_version
  get_new_version

  if version_le "$OLD_VERSION" "$NEW_VERSION"
  then
    echo "OLD_VERSION: $OLD_VERSION"
    echo "NEW_VERSION: $NEW_VERSION"
  else
    echo "ERROR: $OLD_VERSION >= $NEW_VERSION"
    exit 1
  fi
}

get_intermediate_versions() {
  local version
  local series
  local latest_series=""
  local latest_version=""

  while read -r version; do
    if version_lt "$OLD_VERSION" "$version" && version_lt "$version" "$NEW_VERSION"; then
      series="${version%.*}"
      if [[ -n "$latest_series" && "$series" != "$latest_series" ]]; then
        echo "$latest_version"
      fi
      latest_series="$series"
      latest_version="$version"
    fi
  done < <(git tag --list 'v[0-9]*.[0-9]*.[0-9]*' \
    | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' \
    | sed 's/^v//' \
    | sort -V)

  if [[ -n "$latest_version" ]]; then
    echo "$latest_version"
  fi
}

# Setup table and materialized view.
# Run updates and deletes on the table.
# Get the results.
seed_old_cluster() {
  echo "--- Start cluster on old_version: $OLD_VERSION"
  # Caller should make sure the test env has these.
  # They are called here because the current tests
  # may not be backwards compatible, so we need to call
  # them in old cluster environment.
  cp -r e2e_test/streaming/nexmark $TEST_DIR
  cp -r e2e_test/nexmark/* $TEST_DIR/nexmark

  cp -r e2e_test/batch/tpch $TEST_DIR
  cp -r e2e_test/tpch/* $TEST_DIR/tpch

  ./risedev clean-data
  # `ENABLE_PYTHON_UDF` and `ENABLE_JS_UDF` are set for backwards-compartibility
  ENABLE_PYTHON_UDF=1 ENABLE_JS_UDF=1 ENABLE_UDF=1 ./risedev d full-without-monitoring && rm .risingwave/log/*

  if version_le "$RECOVER_COMMAND_MIN_VERSION" "$OLD_VERSION"; then
    wait_for_recovery "$OLD_VERSION"
  fi
  check_version "$OLD_VERSION"

  echo "--- BASIC TEST: Seeding old cluster with data"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/basic/seed.slt"

  echo "--- BASIC TEST: Validating old cluster"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/basic/validate_original.slt"

  echo "--- HASH JOIN WATERMARK TEST: Seeding old cluster with data"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/hash-join-watermark/seed.slt"

  echo "--- HASH JOIN WATERMARK TEST: Validating old cluster"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/hash-join-watermark/validate_original.slt"

  echo "--- EOWC OVER WINDOW NUMBERING TEST: Seeding old cluster with data"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/eowc-over-window-numbering/seed.slt"

  echo "--- EOWC OVER WINDOW NUMBERING TEST: Validating old cluster"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/eowc-over-window-numbering/validate_original.slt"

  echo "--- NEXMARK TEST: Seeding old cluster with data"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/nexmark-backwards-compat/seed.slt"

  echo "--- NEXMARK TEST: Validating old cluster"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/nexmark-backwards-compat/validate_original.slt"

  echo "--- TPCH TEST: Seeding old cluster with data"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/tpch-backwards-compat/seed.slt"

  echo "--- TPCH TEST: Validating old cluster"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/tpch-backwards-compat/validate_original.slt"

  echo "--- KAFKA TEST: Seeding old cluster with data"
  create_kafka_topic
  seed_json_kafka
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/kafka/seed.slt"
  # use the old syntax for version at most 1.5.4
  if version_le "$OLD_VERSION" "1.5.4"; then
    sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/kafka/upsert/deprecate_upsert.slt"
  else
    sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/kafka/upsert/include_key_as.slt"
  fi

  echo "--- KAFKA TEST: wait 5s for kafka to process data"
  sleep 5

  echo "--- KAFKA TEST: Validating old cluster"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/kafka/validate_original.slt"

  # Test version columns backwards compatibility, if OLD_VERSION <= 2.6.0
  if version_le "$OLD_VERSION" "2.6.0"; then
    echo "--- VERSION COLUMNS TEST: Seeding old cluster with data"
    sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/version-columns/seed.slt"

    echo "--- VERSION COLUMNS TEST: Validating old cluster"
    sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/version-columns/validate_original.slt"
  fi

  # Test legacy streaming parallelism session parameter migration for versions in [2.8.0, 2.9.0).
  if version_le "2.8.0" "$OLD_VERSION" && version_lt "$OLD_VERSION" "2.9.0"; then
    echo "--- STREAMING PARALLELISM TEST: Seeding old cluster"
    sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/streaming-parallelism/seed.slt"

    echo "--- STREAMING PARALLELISM TEST: Validating old cluster"
    sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/streaming-parallelism/validate_original.slt"
  fi

  # Test invalid WITH options, if OLD_VERSION <= 1.5.0
  if version_le "$OLD_VERSION" "1.5.0"; then
    echo "--- KAFKA TEST (invalid options): Seeding old cluster with data"
    sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/kafka/invalid_options/seed.slt"

    echo "--- KAFKA TEST (invalid options): wait 5s for kafka to process data"
    sleep 5

    echo "--- KAFKA TEST (invalid options): Validating old cluster"
    sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/kafka/invalid_options/validate_original.slt"
  fi

  echo "--- SINK INTO TABLE TEST: Seeding old cluster with data"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/sink_into_table/seed.slt"

  echo "--- SINK INTO TABLE TEST: Validating old cluster"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/sink_into_table/validate_original.slt"

  echo "--- ASOF JOIN TEST: Seeding old cluster with data"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/asof-join/seed.slt"

  echo "--- ASOF JOIN TEST: Validating old cluster"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/asof-join/validate_original.slt"

  echo "--- CDC TEST: Seeding old cluster with data"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/cdc/seed.slt"

  echo "--- CDC TEST: Validating old cluster"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/cdc/validate_original.slt"

  # The `pending_sink_state` table and exactly-once Iceberg sinks exist since 2.8.0.
  if version_le "2.8.0" "$OLD_VERSION"; then
    echo "--- PENDING SINK STATE TEST: Seeding old cluster with a dropped Iceberg sink"
    ./risedev mc mb -p hummock-minio/icebergdata || true
    sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/pending-sink-state/seed.slt"
  fi

  # work around https://github.com/risingwavelabs/risingwave/issues/18650
  echo "--- wait for a version checkpoint"
  sleep 60

  seed_cross_db_subscription_validation

  seed_hummock_stale_table_ids

  echo "--- Killing cluster"
  kill_cluster
  echo "--- Killed cluster"
}

validate_new_cluster() {
  echo "--- Start cluster on latest"
  ENABLE_UDF=1 ./risedev d full-without-monitoring

  wait_for_recovery "$NEW_VERSION"

  check_version "$NEW_VERSION"

  echo "--- BASIC TEST: Validating new cluster"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/basic/validate_restart.slt"

  echo "--- HASH JOIN WATERMARK TEST: Validating new cluster"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/hash-join-watermark/validate_restart.slt"

  echo "--- EOWC OVER WINDOW NUMBERING TEST: Validating new cluster"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/eowc-over-window-numbering/validate_restart.slt"

  echo "--- NEXMARK TEST: Validating new cluster"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/nexmark-backwards-compat/validate_restart.slt"

  echo "--- TPCH TEST: Validating new cluster"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/tpch-backwards-compat/validate_restart.slt"

  echo "--- KAFKA TEST: Seeding new cluster with data"
  seed_json_kafka

  echo "--- KAFKA TEST: Validating new cluster"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/kafka/validate_restart.slt"

  # Test version columns backwards compatibility, if OLD_VERSION <= 2.6.0
  if version_le "$OLD_VERSION" "2.6.0"; then
    echo "--- VERSION COLUMNS TEST: Validating new cluster"
    sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/version-columns/validate_restart.slt"
  fi

  # Test legacy streaming parallelism session parameter migration for versions in [2.8.0, 2.9.0).
  if version_le "2.8.0" "$OLD_VERSION" && version_lt "$OLD_VERSION" "2.9.0"; then
    echo "--- STREAMING PARALLELISM TEST: Validating new cluster"
    sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/streaming-parallelism/validate_restart.slt"
  fi

  # Test invalid WITH options, if OLD_VERSION <= 1.5.0
  if version_le "$OLD_VERSION" "1.5.0"; then
    echo "--- KAFKA TEST (invalid options): Validating new cluster"
    sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/kafka/invalid_options/validate_restart.slt"
  fi

  echo "--- SINK INTO TABLE TEST: Validating new cluster"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/sink_into_table/validate_restart.slt"

  echo "--- ASOF JOIN TEST: Validating new cluster"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/asof-join/validate_restart.slt"

  echo "--- CDC TEST: Validating new cluster"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/cdc/validate_restart.slt"

  validate_cross_db_subscription_after_upgrade

  # The `pending_sink_state` table and exactly-once Iceberg sinks exist since 2.8.0.
  if version_le "2.8.0" "$OLD_VERSION"; then
    echo "--- PENDING SINK STATE TEST: Validating new cluster"
    sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/pending-sink-state/validate_restart.slt"
  fi

  validate_hummock_stale_table_ids

  kill_cluster
}
