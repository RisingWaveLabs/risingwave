#!/usr/bin/env bash

set -euo pipefail

while getopts 'p:m:' opt; do
    case ${opt} in
        p )
            profile=$OPTARG
            ;;
        m )
            mode=$OPTARG
            ;;
        \? )
            echo "Invalid Option: -$OPTARG" 1>&2
            exit 1
            ;;
        : )
            echo "Invalid option: $OPTARG requires an argument" 1>&2
            exit 1
            ;;
    esac
done
shift $((OPTIND -1))

source ci/scripts/common.sh

profile=${profile:-}
mode=${mode:-}

if [[ -z "$profile" ]]; then
    echo "Missing required option: -p <profile>" 1>&2
    exit 1
fi

if [[ -z "$mode" ]]; then
    echo "Missing required option: -m <mode>" 1>&2
    exit 1
fi

download_and_prepare_rw "$profile" common

cleanup() {
    risedev ci-kill || true
}
trap cleanup EXIT

echo "--- Install Python Dependencies"
python3 -m pip install --break-system-packages -r ./e2e_test/requirements.txt psycopg2-binary

echo "--- e2e, $mode, refill"
RUST_LOG="info,risingwave_storage=info" \
risedev ci-start "$mode"

echo "--- risedev-env"
risedev show-risedev-env

risedev slt -p 4566 -d dev './e2e_test/refill/table_refill.slt'

echo "--- Kill cluster"
risedev ci-kill
trap - EXIT
