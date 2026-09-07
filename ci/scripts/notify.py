#!/usr/bin/env python3

import subprocess
import os
import sys

# Add new test keys here.
# Add their corresponding owners (by slack username) here.
# NOTE(kwannoel): we may have to migrate to use `slack_user_id`.
# I use `slack_username` since it is more readable, but not officially supported in the docs.
MAIN_CRON_TEST_MAP = {
    "test-notify": ["noelkwan", "noelkwan"],
    "test-notify-2": ["noelkwan", "noelkwan"],
    "test-notify-timeout": ["noelkwan", "noelkwan"],
    "docslt": ["tianxiao"],
    "e2e-test-release": ["zhi", "Eric"],
    "e2e-meta-backup-test-release": ["zhi", "Eric"],
    "e2e-test-release-parallel": ["zhi", "Eric"],
    "e2e-test-release-parallel-memory": ["zhi", "Eric"],
    "e2e-test-release-source": ["bohan", "siyuan"],
    "e2e-test-release-sink": ["bohan", "siyuan"],
    "fuzz-test": ["noelkwan"],
    "unit-test": ["zhi", "Eric"],
    "unit-test-deterministic": ["zhi", "Eric"],
    "integration-test-deterministic-scale": ["ziqi", "Eric"],
    "integration-test-deterministic-recovery": ["ziqi", "Eric"],
    "integration-test-deterministic-backfill": ["ziqi", "Eric"],
    "integration-test-deterministic-storage": ["ziqi", "Eric"],
    "integration-test-deterministic-sink": ["ziqi", "Eric"],
    "e2e-test-deterministic": ["runji", "noelkwan"],
    "recovery-test-deterministic": ["runji", "noelkwan"],
    "background-ddl-arrangement-backfill-recovery-test-deterministic": [
        "runji",
        "noelkwan",
    ],
    "background-ddl-recovery-test-deterministic": ["runji", "noelkwan"],
    "e2e-iceberg-test": ["zilin", "xinhao", "tianxiao"],
    "e2e-java-binding-tests": ["yiming"],
    "s3-source-check-aws": ["bohan"],
    "s3-source-check-aws-json-parser": ["bohan"],
    "s3-source-check-aws-csv-parser": ["bohan"],
    "s3-v2-source-check-aws-json-parser": ["bohan"],
    "s3-v2-source-batch-read-check-aws-json-parser": ["bohan"],
    "s3-v2-source-check-aws-csv-parser": ["bohan"],
    "s3-source-test-for-opendal-fs-engine-csv-parser": ["congyi", "kexiang"],
    "s3-source-test-for-opendal-fs-engine": ["congyi", "kexiang"],
    "pulsar-source-tests": ["bohan"],
    "run-micro-benchmarks": ["noelkwan"],
    "upload-micro-benchmarks": ["noelkwan"],
    "backwards-compat-tests": ["yuchao"],
    "sqlsmith-differential-tests": ["noelkwan"],
    "backfill-tests": ["noelkwan", "yiming"],
    "e2e-standalone-binary-tests": ["noelkwan"],
    "e2e-single-node-binary-tests": ["pin", "peng", "noelkwan"],
    "e2e-test-opendal-parallel": ["congyi"],
    "e2e-deltalake-sink-rust-tests": ["xinhao"],
    "e2e-lancedb-sink-tests": ["yiming"],
    "e2e-redis-sink-tests": ["xinhao"],
    "e2e-starrocks-sink-tests": ["xinhao"],
    "e2e-cassandra-sink-tests": ["xinhao"],
    "e2e-clickhouse-sink-tests": ["bohan", "xinhao"],
    "e2e-pulsar-sink-tests": ["bohan"],
    "e2e-mqtt-sink-tests": ["xinhao"],
    "connector-node-integration-test": ["siyuan"],
}

INTEGRATION_TEST_MAP = {
    "test-notify": ["jianwei"],
    "ad-click-json": ["bohan"],
    "ad-ctr-json": ["bohan"],
    "cdn-metrics-json": ["bohan"],
    "clickstream-json": ["bohan"],
    "livestream-json": ["bohan"],
    "livestream-protobuf": ["bohan"],
    "prometheus-json": ["bohan"],
    "schema-registry-json": ["bohan"],
    "mysql-cdc-json": ["siyuan"],
    "postgres-cdc-json": ["siyuan"],
    "mongodb-cdc-json": ["siyuan"],
    "mysql-sink-json": ["siyuan"],
    "postgres-sink-json": ["siyuan"],
    "iceberg-cdc-json": ["zilin"],
    "iceberg-sink-none": ["zilin"],
    'iceberg-source-none': ["zilin"],
    "twitter-json": ["bohan"],
    "twitter-protobuf": ["bohan"],
    "twitter-pulsar-json": ["bohan"],
    "debezium-mysql-json": ["bohan"],
    "debezium-postgres-json": ["bohan"],
    "debezium-sqlserver-json": ["bohan"],
    "tidb-cdc-sink-json": ["eric"],
    "citus-cdc-json": ["siyuan"],
    "kinesis-s3-source-json": ["bohan"],
    "clickhouse-sink-json": ["xinhao"],
    "cockroach-sink-json": ["bohan"],
    "kafka-cdc-sink-json": ["bohan"],
    "cassandra-and-scylladb-sink-json": ["xinhao"],
    "elasticsearch-sink-json": ["xinhao"],
    "redis-sink-json": ["xinhao"],
    "big-query-sink-json": ["xinhao"],
    "vector-json": ["wutao"],
    "nats-json": ["wutao"],
    "nats-protobuf": ["wutao"],
    "mqtt-json": ["bohan"],
    "doris-sink-json": ["xinhao"],
    "starrocks-sink-json": ["xinhao"],
    "deltalake-sink-json": ["xinhao"],
    "pinot-sink-json": ["yiming"],
    "presto-trino-json": ["wutao"],
    "client-library-none": ["wutao"],
    "kafka-cdc-json": ["bohan"],
}

def get_failed_tests(get_test_status, test_map):
    failed_test_map = {}
    for test in test_map.keys():
        test_status = get_test_status(test)
        if test_status == "hard_failed" or test_status == "soft_failed" or test_status == "errored":
            print(f"{test} failed with outcome: {test_status}")
            failed_test_map[test] = test_map[test]
        elif test_status == "passed":
            print(f"{test} passed with outcome: {test_status}")
        elif test_status is None or test_status == "":
            print(f"{test} no outcome, skipping")
        else:
            print(f"{test} failed with unknown outcome: {test_status}")
            failed_test_map[test] = test_map[test]
    return failed_test_map

def generate_test_status_message(failed_test_map):
    messages = []
    for test, users in failed_test_map.items():
        users = " ".join(map(lambda user: f"<@{user}>", users))
        messages.append(f"Test {test} failed {users}")
    message = "\n            ".join(messages)
    return message

def get_buildkite_test_status(test):
    result = subprocess.run(f"buildkite-agent step get \"outcome\" --step \"{test}\"", capture_output = True, text = True, shell=True)
    outcome = result.stdout.strip()
    return outcome

def get_mock_test_status(test):
    mock_test_map = {
        "test-notify": "hard_failed",
        "test-notify-2": "hard_failed",
        "backfill-tests": "",
        "backwards-compat-tests": "",
        "fuzz-test": "",
        "e2e-test-release": "",
        "e2e-iceberg-tests": "passed",
        "e2e-java-binding-tests": "soft_failed",
        "e2e-clickhouse-sink-tests": "hard_failed",
        "e2e-pulsar-sink-tests": "",
        "s3-source-test-for-opendal-fs-engine": "",
        "s3-source-tests": "",
        "pulsar-source-tests": "",
        "connector-node-integration-test": "",
    }
    return mock_test_map[test]

def format_cmd(messages):
    cmd=f"""
cat <<- YAML | buildkite-agent pipeline upload
steps:
  - label: "trigger failed test notification"
    command: echo "running failed test notification" && exit 1
    notify:
      - slack:
          channels:
            - "#notification-buildkite"
          message: |
            {messages}
YAML
        """
    return cmd

def get_test_map():
    pipeline_name = os.environ['BUILDKITE_PIPELINE_NAME']
    if pipeline_name == "main-cron":
        test_map=MAIN_CRON_TEST_MAP
    elif pipeline_name == "integration-tests":
        test_map=INTEGRATION_TEST_MAP
    else:
        print("Invaild pipeline name!")
        sys.exit(1)
    return test_map

def run_test_1():
    test_map = get_test_map()
    failed_test_map = get_failed_tests(get_mock_test_status, test_map)
    message = generate_test_status_message(failed_test_map)
    if message == "":
        print("All tests passed, no need to notify")
        return
    else:
        print("Some tests failed, notify users")
        print(message)
        cmd = format_cmd(message)
        print(cmd)

def main():
    test_map = get_test_map()
    print("--- Getting failed tests")
    failed_test_map = get_failed_tests(get_buildkite_test_status, test_map)
    message = generate_test_status_message(failed_test_map)
    if message == "":
        print("--- Tests passed, no need to notify")
        return
    else:
        print("--- Some tests failed, notify users")
        cmd = format_cmd(message)
        print(cmd)
        subprocess.run(cmd, shell=True)
        print("notification sent")

main()
