#!/usr/bin/env bash

set -euo pipefail

source ci/scripts/common.sh

while getopts 'p:' opt; do
    case ${opt} in
        p )
            profile=$OPTARG
            ;;
        \? )
            echo "Invalid Option: -$OPTARG" 1>&2
            exit 1
            ;;
        : )
            echo "Invalid option: $OPTARG requires an argument" 1>&2
            ;;
    esac
done
shift $((OPTIND -1))

source_test_env_setup "$profile" --risedev-profile ci-source-cdc-test --need-connector --need-python

install_sqlserver_client
export SQLCMDSERVER=sqlserver-server SQLCMDUSER=SA SQLCMDPASSWORD="SomeTestOnly@SA" SQLCMDDBNAME=mydb SQLCMDPORT=1433

echo "--- Install mongosh"
wget --no-verbose https://repo.mongodb.org/apt/ubuntu/dists/noble/mongodb-org/8.0/multiverse/binary-amd64/mongodb-mongosh_2.5.8_amd64.deb
dpkg -i mongodb-mongosh_2.5.8_amd64.deb

echo "--- Run inline CDC source tests"
risedev slt './e2e_test/source_inline/cdc/**/*.slt' --skip 'cron_only' -j1
risedev slt './e2e_test/source_inline/cdc/**/*.slt.serial' --skip 'cron_only'

echo "--- Run TVF source tests"
export MYSQL_HOST=mysql MYSQL_TCP_PORT=3306 MYSQL_PWD=123456
risedev slt './e2e_test/source_inline/tvf/*.slt'

echo "--- Kill cluster"
risedev ci-kill

echo "--- Prepare CDC source fixtures"
mysql --host=mysql --port=3306 -u root -p123456 < ./e2e_test/source_legacy/cdc/mysql_cdc.sql

echo "--- Run mysql-async integration test"
cargo test --package risingwave_mysql_test -- --ignored

export PGHOST=db PGPORT=5432 PGUSER=postgres PGPASSWORD='post\tgres' PGDATABASE=cdc_test
createdb
psql < ./e2e_test/source_legacy/cdc/postgres_cdc.sql

echo "--- Start RisingWave cluster"
RUST_LOG="debug,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info" \
risedev ci-start ci-1cn-1fe-with-recovery

echo "--- Run legacy CDC source tests"
source ci/scripts/e2e-source-mysql-offline-schema-change.sh
source ci/scripts/e2e-source-mysql-cdc-reset.sh

echo "--- Restart RisingWave cluster"
RUST_LOG="debug,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info" \
risedev ci-start ci-1cn-1fe-with-recovery

risedev slt './e2e_test/source_legacy/cdc_inline/**/*.slt'
risedev slt './e2e_test/source_legacy/cdc/cdc.validate.shared.slt'
risedev slt './e2e_test/source_legacy/cdc/cdc.share_stream.slt'

echo "--- Kill cluster"
risedev kill

echo "--- Prepare CDC recovery data"
mysql --protocol=tcp -u root mytest -e "INSERT INTO products
       VALUES (default,'RisingWave','Next generation Streaming Database'),
              (default,'Materialize','The Streaming Database You Already Know How to Use');
       UPDATE products SET name = 'RW' WHERE id <= 103;
       INSERT INTO orders VALUES (default, '2022-12-01 15:08:22', 'Sam', 1000.52, 110, false);"
mysql --host=mysql --port=3306 -u root -p123456 < ./e2e_test/source_legacy/cdc/mysql_cdc_insert.sql
psql < ./e2e_test/source_legacy/cdc/postgres_cdc_insert.sql

unset RISINGWAVE_CI
export RUST_LOG="risingwave_stream=debug,risingwave_batch=info,risingwave_storage=info"

risedev dev ci-1cn-1fe-with-recovery
sleep 20
risedev slt './e2e_test/source_legacy/cdc/cdc.check_new_rows.slt'
risedev slt './e2e_test/source_legacy/cdc/cdc_share_stream_drop.slt'

echo "--- Kill cluster"
risedev ci-kill
export RISINGWAVE_CI=true
