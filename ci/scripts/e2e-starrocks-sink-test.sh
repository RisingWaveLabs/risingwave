#!/usr/bin/env bash

# Exits as soon as any line fails.
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

sink_test_env_setup "$profile" --sleep-duration 0

export STARROCKS_HOST="starrocks-fe-server"
export STARROCKS_HTTP_PORT="8030"
export STARROCKS_QUERY_PORT="9030"
export STARROCKS_USER="users"
export STARROCKS_PASSWORD="123456"
export STARROCKS_DATABASE="demo"
export STARROCKS_ADMIN_USER="root"
export STARROCKS_ADMIN_PASSWORD=""
export RISEDEV_STARROCKS_WITH_OPTIONS_COMMON="connector='starrocks',starrocks.host='${STARROCKS_HOST}',starrocks.mysqlport='${STARROCKS_QUERY_PORT}',starrocks.httpport='${STARROCKS_HTTP_PORT}',starrocks.user='${STARROCKS_USER}',starrocks.password='${STARROCKS_PASSWORD}',starrocks.database='${STARROCKS_DATABASE}'"
export PATH="${PWD}/e2e_test/commands:${PATH}"

echo "--- testing sinks"
sqllogictest -p 4566 -d dev './e2e_test/sink/starrocks_sink.slt'

echo "--- Kill cluster"
risedev ci-kill
