#!/usr/bin/env bash
set -eo pipefail
[ -n "${BACKUP_TEST_MCLI}" ]
[ -n "${BACKUP_TEST_MCLI_CONFIG}" ]
[ -n "${BACKUP_TEST_RW_ALL_IN_ONE}" ]

function stop_cluster() {
  cargo make --allow-private k 1>/dev/null 2>&1 || true
  cargo make --allow-private wait-processes-exit 1>/dev/null 2>&1 || true
}

function clean_all_data {
  cargo make --allow-private clean-data 1>/dev/null 2>&1
}

function clean_meta_store() {
  clean_sqlite_data
}

function clean_sqlite_data() {
  tables=$(sqlite3 "${RW_SQLITE_DB}" "select name from sqlite_master where type='table';")
  while IFS= read table
  do
    if [ -z "${table}" ]; then
      break
    fi
    sqlite3 "${RW_SQLITE_DB}" "delete from [${table}]"
  done <<< "${tables}"
}

function start_cluster() {
  stop_cluster
  cargo make d ci-meta-backup-test-sql 1>/dev/null 2>&1
  sleep 5
}

function full_gc_sst() {
  ${BACKUP_TEST_RW_ALL_IN_ONE} risectl hummock trigger-full-gc -s 0 1>/dev/null 2>&1
  # TODO #6482: wait full gc finish deterministically.
  # Currently have to wait long enough.
  sleep 30
}

function manual_compaction() {
  ${BACKUP_TEST_RW_ALL_IN_ONE} risectl hummock trigger-manual-compaction "$@" 1>/dev/null 2>&1
}

function start_meta_store_minio() {
  start_sql_minio
}

function start_sql_minio() {
  cargo make d ci-meta-backup-test-restore-sql 1>/dev/null 2>&1
}

function create_mvs() {
  cargo make slt -p 4566 -d dev "e2e_test/backup_restore/tpch_snapshot_create.slt"
}

function query_mvs() {
  cargo make slt -p 4566 -d dev "e2e_test/backup_restore/tpch_snapshot_query.slt"
}

function drop_mvs() {
  cargo make slt -p 4566 -d dev "e2e_test/backup_restore/tpch_snapshot_drop.slt"
}

function backup() {
  local snapshot_id
  o=$(execute_sql_t "BACKUP;")
  snapshot_id=$(echo "${o}" | awk '{$1=$1}; NF {print $1}' | tail -n 1)
  # snapshot_id=$(awk '{$1=$1}; NF {print $1}' < <(execute_sql_t "BACKUP;") | tail -n 1)
  [ -n "${snapshot_id}" ]
  echo "${snapshot_id}"
}

function delete_snapshot() {
  local snapshot_id
  snapshot_id=$1
  execute_sql "DELETE META SNAPSHOT ${snapshot_id};" 1>/dev/null
}

function restore() {
  local job_id
  job_id=$1
  echo "try to restore snapshot ${job_id}"
  stop_cluster
  clean_meta_store
  start_meta_store_minio
  ${BACKUP_TEST_RW_ALL_IN_ONE} \
  risectl \
  meta \
  restore-meta \
  --meta-store-type "sql" \
  --meta-snapshot-id "${job_id}" \
  --sql-endpoint "sqlite://${RW_SQLITE_DB}?mode=rwc" \
  --backup-storage-url minio://hummockadmin:hummockadmin@127.0.0.1:9301/hummock001 \
  --hummock-storage-url minio://hummockadmin:hummockadmin@127.0.0.1:9301/hummock001 \
  --validate-integrity \
  1>/dev/null 2>&1
}

function create_minio_bucket() {
  local bucket_name
  bucket_name=$1
  ${BACKUP_TEST_MCLI} -C "${BACKUP_TEST_MCLI_CONFIG}" \
  mb "hummock-minio/${bucket_name}"
}

function cp_minio_bucket() {
  local from
  from=$1
  local to
  to=$2
  ${BACKUP_TEST_MCLI} -C "${BACKUP_TEST_MCLI_CONFIG}" \
  cp --recursive "hummock-minio/${from}" "hummock-minio/${to}"
}

function restore_with_overwrite() {
  local job_id
  job_id=$1
  local overwrite_hummock_storage_url
  overwrite_hummock_storage_url=$2
  local overwrite_hummock_storage_dir
  overwrite_hummock_storage_dir=$3
  local overwrite_backup_storage_url
  overwrite_backup_storage_url=$4
  local overwrite_backup_storage_dir
  overwrite_backup_storage_dir=$5
  echo "try to restore snapshot ${job_id}"
  stop_cluster
  clean_meta_store
  start_meta_store_minio
  ${BACKUP_TEST_RW_ALL_IN_ONE} \
  risectl \
  meta \
  restore-meta \
  --meta-store-type "sql" \
  --meta-snapshot-id "${job_id}" \
  --sql-endpoint "sqlite://${RW_SQLITE_DB}?mode=rwc" \
  --overwrite-hummock-storage-endpoint \
  --overwrite-backup-storage-url "${overwrite_backup_storage_url}" \
  --overwrite-backup-storage-directory "${overwrite_backup_storage_dir}" \
  --hummock-storage-url "${overwrite_hummock_storage_url}" \
  --hummock-storage-directory "${overwrite_hummock_storage_dir}" \
  --backup-storage-url minio://hummockadmin:hummockadmin@127.0.0.1:9301/hummock001 \
  --validate-integrity \
  1>/dev/null 2>&1
}

function restore_fail_integrity_validation() {
  local job_id
  job_id=$1
  echo "try to restore snapshot ${job_id}"
  stop_cluster
  clean_meta_store
  start_meta_store_minio
  set +e
  local output
  output=$(${BACKUP_TEST_RW_ALL_IN_ONE} \
  risectl \
  meta \
  restore-meta \
  --meta-store-type "sql" \
  --meta-snapshot-id "${job_id}" \
  --sql-endpoint "sqlite://${RW_SQLITE_DB}?mode=rwc" \
  --backup-storage-url minio://hummockadmin:hummockadmin@127.0.0.1:9301/hummock001 \
  --hummock-storage-url minio://hummockadmin:hummockadmin@127.0.0.1:9301/hummock001 \
  --hummock-storage-directory dummy_dir \
  --validate-integrity \
  --dry-run 2>&1 | grep "Fail integrity validation." )
  set -e
  [ -n "${output}" ]
}

function execute_sql() {
  local sql
  sql=$1
  echo "${sql}" | psql -h localhost -p 4566 -d dev -U root 2>&1
}

function execute_sql_t() {
  local sql
  sql=$1
  echo "${sql}" | psql -h localhost -p 4566 -d dev -U root -t 2>&1
}

function execute_sql_and_expect() {
  local sql
  sql=$1
  local expected
  expected=$2

  echo "execute SQL ${sql}"
  echo "expected string in result: ${expected}"
  query_result=$(execute_sql "${sql}")
  printf "actual result:\n%s\n" "${query_result}"
  result=$(echo "${query_result}" | grep -F "${expected}")
  [ -n "${result}" ]
}

function get_all_sst_paths() {
  ${BACKUP_TEST_MCLI} -C "${BACKUP_TEST_MCLI_CONFIG}" \
  find "hummock-minio/hummock001" -name "*.data" | sort
}

function get_total_sst_count() {
  get_all_sst_paths | wc -l
}

function get_table_committed_epoch_in_meta_snapshot() {
    sql="select id from rw_tables;"
    table_id=$(execute_sql_t "${sql}")
    table_id="${table_id#"${table_id%%[![:space:]]*}"}"
    table_id="${table_id%"${table_id##*[![:space:]]}"}"
    sql="select state_table_info->'${table_id}'->>'committedEpoch' from rw_meta_snapshot;"
    query_result=$(execute_sql_t "${sql}")
    echo ${query_result}
}
