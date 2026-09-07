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

download_and_prepare_rw "$profile" source

echo "--- e2e, generic sink test"
RUST_LOG="await_tree::future=error" risedev ci-start ci-sink-test

risedev slt './e2e_test/sink/append_only_sink.slt'
risedev slt './e2e_test/sink/blackhole_sink.slt'
risedev slt './e2e_test/sink/file_sink.slt'
risedev slt './e2e_test/sink/license.slt'

echo "--- e2e, dynamodb sink validation"
DYNAMODB_SINK_SERVER_PID=""
cleanup_dynamodb_mock_server() {
  if [[ -n "${DYNAMODB_SINK_SERVER_PID}" ]]; then
    kill "${DYNAMODB_SINK_SERVER_PID}" || true
    wait "${DYNAMODB_SINK_SERVER_PID}" || true
    DYNAMODB_SINK_SERVER_PID=""
  fi
}
trap cleanup_dynamodb_mock_server EXIT
python3 e2e_test/sink/dynamodb_mock_server.py 18082 &
DYNAMODB_SINK_SERVER_PID=$!
for i in $(seq 1 20); do curl -sf http://localhost:18082/ >/dev/null && break; sleep 0.5; done
risedev slt './e2e_test/sink/dynamodb_sink.slt'
cleanup_dynamodb_mock_server

risedev slt './e2e_test/sink/rate_limit.slt'
risedev slt './e2e_test/sink/auto_schema_change.slt'
risedev slt './e2e_test/sink/sink_into_table/*.slt'
risedev slt './e2e_test/sink/sink_vector_columns.slt'
risedev slt './e2e_test/sink/force_compaction_sink.slt'

echo "--- e2e, http sink"
HTTP_SINK_OUTPUT=$(mktemp)
HTTP_SINK_HEADERS=$(mktemp)
HTTP_SINK_METHODS=$(mktemp)
python3 e2e_test/sink/http_sink_mock_server.py "$HTTP_SINK_OUTPUT" 18081 "$HTTP_SINK_HEADERS" "" "$HTTP_SINK_METHODS" &
HTTP_SINK_SERVER_PID=$!
# Wait for the server to be ready
for i in $(seq 1 20); do curl -sf http://localhost:18081/ && break; sleep 0.5; done

risedev slt './e2e_test/sink/http_sink.slt'

# Allow a moment for in-flight requests to complete
sleep 1
# Verify bodies reached the mock server
grep -Fx 'before update' "$HTTP_SINK_OUTPUT"
grep -Fx 'hello world' "$HTTP_SINK_OUTPUT"
grep -Fx '{"key":"value"}' "$HTTP_SINK_OUTPUT"
grep -Fx 'put payload' "$HTTP_SINK_OUTPUT"
grep -Fx 'patch payload' "$HTTP_SINK_OUTPUT"
grep -q '"event"' "$HTTP_SINK_OUTPUT"
grep -Fx 'dynamic url payload' "$HTTP_SINK_OUTPUT"
grep -Fx 'dynamic url as select payload' "$HTTP_SINK_OUTPUT"
grep -Fx 'dynamic patch payload' "$HTTP_SINK_OUTPUT"
grep -Fx 'dynamic custom method payload' "$HTTP_SINK_OUTPUT"
grep -Fx 'dynamic url and method payload' "$HTTP_SINK_OUTPUT"
! grep -F 'skipped invalid method payload' "$HTTP_SINK_OUTPUT"
! grep -F 'skipped empty method payload' "$HTTP_SINK_OUTPUT"
! grep -F 'skipped null method payload' "$HTTP_SINK_OUTPUT"
# Exactly 7 existing requests + static PATCH and DELETE + 3 valid dynamic-method requests + 1 combined dynamic URL/method request.
test "$(wc -l < "$HTTP_SINK_OUTPUT")" -eq 13
# Verify static, dynamic, case-insensitive standard, and extension methods.
test "$(grep -c '^POST$' "$HTTP_SINK_METHODS")" -eq 6
test "$(grep -c '^PUT$' "$HTTP_SINK_METHODS")" -eq 2
test "$(grep -c '^PATCH$' "$HTTP_SINK_METHODS")" -eq 2
test "$(grep -c '^DELETE$' "$HTTP_SINK_METHODS")" -eq 2
test "$(grep -c '^PROPFIND$' "$HTTP_SINK_METHODS")" -eq 1
# The static and dynamic DELETE requests have no body.
test "$(paste "$HTTP_SINK_METHODS" "$HTTP_SINK_OUTPUT" | grep -c $'^DELETE\t$')" -eq 2
# Verify the custom header set via header.x_test = 'rw-http-sink' was sent
grep -q '"x_test": "rw-http-sink"' "$HTTP_SINK_HEADERS"
# Verify inferred default content types for varchar and jsonb payloads
grep -q '"content-type": "text/plain"' "$HTTP_SINK_HEADERS"
grep -q '"content-type": "application/json"' "$HTTP_SINK_HEADERS"

kill "$HTTP_SINK_SERVER_PID" || true
rm -f "$HTTP_SINK_OUTPUT" "$HTTP_SINK_HEADERS" "$HTTP_SINK_METHODS"

echo "--- e2e, turbopuffer sink"
TURBOPUFFER_SINK_OUTPUT=$(mktemp)
TURBOPUFFER_SINK_HEADERS=$(mktemp)
TURBOPUFFER_SINK_PATHS=$(mktemp)
python3 e2e_test/sink/http_sink_mock_server.py "$TURBOPUFFER_SINK_OUTPUT" 18082 "$TURBOPUFFER_SINK_HEADERS" "$TURBOPUFFER_SINK_PATHS" &
TURBOPUFFER_SINK_SERVER_PID=$!
# Wait for the server to be ready
for i in $(seq 1 20); do curl -sf http://localhost:18082/ && break; sleep 0.5; done

risedev slt './e2e_test/sink/turbopuffer_sink.slt'

# Allow a moment for in-flight requests to complete
sleep 1
python3 e2e_test/sink/turbopuffer_sink_check.py "$TURBOPUFFER_SINK_OUTPUT" "$TURBOPUFFER_SINK_HEADERS" "$TURBOPUFFER_SINK_PATHS"

kill "$TURBOPUFFER_SINK_SERVER_PID" || true
rm -f "$TURBOPUFFER_SINK_OUTPUT" "$TURBOPUFFER_SINK_HEADERS" "$TURBOPUFFER_SINK_PATHS"

echo "--- Kill cluster"
risedev ci-kill

echo "--- e2e, ci-1cn-1fe, nexmark endless"
RUST_LOG="info,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info,await_tree::future=error" \
risedev ci-start ci-1cn-1fe
risedev slt './e2e_test/sink/nexmark_endless_mvs/*.slt'
risedev slt './e2e_test/sink/nexmark_endless_sinks/*.slt'

echo "--- Kill cluster"
risedev ci-kill
