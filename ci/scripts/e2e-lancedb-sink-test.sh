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

download_and_prepare_rw "$profile" source

LANCEDB_DIR="/tmp/lancedb-e2e"

echo "--- installing Python lancedb package"
python3 -m pip install --break-system-packages --quiet 'lancedb==0.29.0'

echo "--- preparing LanceDB table"
rm -rf "$LANCEDB_DIR"
python3 ./e2e_test/sink/lancedb/lancedb_helper.py setup "$LANCEDB_DIR"

echo "--- starting risingwave cluster"
risedev ci-start ci-sink-test
sleep 1

echo "--- running LanceDB sink e2e test"
sqllogictest -p 4566 -d dev './e2e_test/sink/lancedb/lancedb_sink.slt'
sleep 3

echo "--- verifying LanceDB sink output"
actual_output=$(python3 ./e2e_test/sink/lancedb/lancedb_helper.py verify "$LANCEDB_DIR" 5)

if diff -u ./e2e_test/sink/lancedb/expected_output.csv <(echo "$actual_output"); then
    echo "LanceDB sink check passed"
else
    echo "LanceDB sink output mismatch!"
    echo "Actual output:"
    echo "$actual_output"
    exit 1
fi

echo "--- Kill cluster"
risedev ci-kill

echo "--- cleaning up"
rm -rf "$LANCEDB_DIR"
