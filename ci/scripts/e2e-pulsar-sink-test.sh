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

sink_test_env_setup "$profile"

echo "--- waiting until pulsar is healthy"
HTTP_CODE=404
MAX_RETRY=20
set +e
while [[ $HTTP_CODE -ne 200 && MAX_RETRY -gt 0 ]]
do
    HTTP_CODE=$(curl --connect-timeout 2 -s -o /dev/null -w ''%{http_code}'' http://pulsar-server:8080/admin/v2/clusters)
    echo Got HTTP Code: "$HTTP_CODE"
    ((MAX_RETRY--))
    sleep 5
done
set -e

echo "--- testing pulsar sink"
sqllogictest -p 4566 -d dev './e2e_test/sink/pulsar_sink.slt'
sqllogictest -p 4566 -d dev './e2e_test/sink/pulsar_sink_roundrobin.slt'

sleep 1

echo "--- Kill cluster"
risedev ci-kill
