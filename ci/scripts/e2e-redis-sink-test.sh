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
apt-get update -y && apt-get install -y redis-server

export PATH="$(pwd)/e2e_test/commands:${PATH}"
export REDIS_HOST="redis-server"
export REDIS_PORT="6379"
export RISEDEV_REDIS_URL="redis://redis-server:6379/"
export RISEDEV_REDIS_WITH_OPTIONS_COMMON="connector='redis',redis.url='${RISEDEV_REDIS_URL}'"

echo "--- testing sinks"
sqllogictest -p 4566 -d dev './e2e_test/sink/redis_sink.slt'

echo "--- testing cluster sinks"
REDIS_CLUSTER_LOG_DIR=".risingwave/log"
mkdir -p "$REDIS_CLUSTER_LOG_DIR"
redis-server ./ci/redis-conf/redis-7000.conf --daemonize yes --logfile "$REDIS_CLUSTER_LOG_DIR/redis-7000.log"
redis-server ./ci/redis-conf/redis-7001.conf --daemonize yes --logfile "$REDIS_CLUSTER_LOG_DIR/redis-7001.log"
redis-server ./ci/redis-conf/redis-7002.conf --daemonize yes --logfile "$REDIS_CLUSTER_LOG_DIR/redis-7002.log"

echo "yes" | redis-cli --cluster create 127.0.0.1:7000 127.0.0.1:7001 127.0.0.1:7002
# Wait a bit for cluster_state to become ok before running SLT.
sleep 2

sqllogictest -p 4566 -d dev './e2e_test/sink/redis_cluster_sink.slt'

redis-cli -c --cluster call 127.0.0.1:7000 keys \* >> ./query_result_1.txt

line_count=$(wc -l < query_result_1.txt)
if [ "$line_count" -eq 16 ]; then
    echo "Redis sink check passed"
else
    cat ./query_result_1.txt
    echo "The output is not as expected."
    exit 1
fi

echo "--- Kill cluster"
risedev ci-kill
