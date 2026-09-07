
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. "${DIR}/common.sh"

stop_cluster
clean_all_data
start_cluster

execute_sql "CREATE TABLE t (c int);"
execute_sql "INSERT INTO t values (1),(2);"
execute_sql "FLUSH;"

execute_sql_and_expect \
"SHOW parameters;" \
"state_store                            | hummock+minio://****:****@127.0.0.1:9301/hummock001"

execute_sql_and_expect \
"SHOW parameters;" \
"data_directory                         | hummock_001"

execute_sql_and_expect \
"SHOW parameters;" \
"backup_storage_url                     | minio://hummockadmin:hummockadmin@127.0.0.1:9301/hummock001"

execute_sql_and_expect \
"SHOW parameters;" \
"backup_storage_directory               | backup"


job_id_1=$(backup)
create_minio_bucket "hummock002"
cp_minio_bucket "hummock001/hummock_001/" "hummock002/hummock_002/"
overwrite_hummock_storage_url="minio://hummockadmin:hummockadmin@127.0.0.1:9301/hummock002"
overwrite_hummock_storage_dir="hummock_002"
overwrite_backup_storage_url="minio://hummockadmin:hummockadmin@127.0.0.1:9301/hummock002"
overwrite_backup_storage_dir="backup_002"
restore_with_overwrite "${job_id_1}" "${overwrite_hummock_storage_url}" "${overwrite_hummock_storage_dir}" "${overwrite_backup_storage_url}" "${overwrite_backup_storage_dir}"

start_cluster

execute_sql_and_expect \
"SHOW parameters;" \
"state_store                            | hummock+minio://****:****@127.0.0.1:9301/hummock002"

execute_sql_and_expect \
"SHOW parameters;" \
"data_directory                         | ${overwrite_hummock_storage_dir}"

execute_sql_and_expect \
"SHOW parameters;" \
"backup_storage_url                     | ${overwrite_backup_storage_url}"

execute_sql_and_expect \
"SHOW parameters;" \
"backup_storage_directory               | ${overwrite_backup_storage_dir}"

execute_sql_and_expect \
"SELECT * FROM t ORDER BY c;" \
"1
2
(2 rows)"
