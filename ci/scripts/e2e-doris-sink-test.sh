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

export DORIS_HOST="doris-server"
export DORIS_HTTP_PORT="8030"
export DORIS_QUERY_PORT="9030"
export DORIS_USER="users"
export DORIS_PASSWORD="123456"
export DORIS_DATABASE="demo"
export DORIS_ADMIN_USER="root"
export DORIS_ADMIN_PASSWORD=""
export RISEDEV_DORIS_WITH_OPTIONS_COMMON="connector='doris',doris.url='http://${DORIS_HOST}:${DORIS_HTTP_PORT}',doris.user='${DORIS_USER}',doris.password='${DORIS_PASSWORD}',doris.database='${DORIS_DATABASE}'"
export PATH="${PWD}/e2e_test/commands:${PATH}"

echo "--- testing sinks"
sqllogictest -p 4566 -d dev './e2e_test/sink/doris_sink.slt'

echo "--- Kill cluster"
risedev ci-kill
