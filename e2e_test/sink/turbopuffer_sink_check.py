#!/usr/bin/env python3
# Copyright 2026 RisingWave Labs
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import sys


def read_json_lines(path):
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip()]


def read_lines(path):
    with open(path) as f:
        return [line.strip() for line in f if line.strip()]


def assert_equal(actual, expected, message):
    if actual != expected:
        raise AssertionError(f"{message}: expected {expected!r}, got {actual!r}")


def assert_float_list_close(actual, expected, message, tolerance=1e-6):
    if len(actual) != len(expected):
        raise AssertionError(f"{message}: expected {expected!r}, got {actual!r}")
    for index, (actual_value, expected_value) in enumerate(zip(actual, expected)):
        if abs(actual_value - expected_value) > tolerance:
            raise AssertionError(
                f"{message}[{index}]: expected {expected_value!r}, got {actual_value!r}"
            )


def find_body(bodies, predicate, description):
    for body in bodies:
        if predicate(body):
            return body
    raise AssertionError(f"missing turbopuffer request body: {description}")


def main():
    if len(sys.argv) != 4:
        raise SystemExit(
            "usage: turbopuffer_sink_check.py <body_file> <header_file> <path_file>"
        )

    bodies = read_json_lines(sys.argv[1])
    headers = read_json_lines(sys.argv[2])
    paths = read_lines(sys.argv[3])

    if not bodies:
        raise AssertionError("expected at least 1 turbopuffer request, got 0")
    assert_equal(len(headers), len(bodies), "header/body request count mismatch")
    assert_equal(len(paths), len(bodies), "path/body request count mismatch")

    if "/v2/namespaces/IFI-ns-1" not in paths:
        raise AssertionError(f"dynamic namespace path was not observed: {paths!r}")

    for header in headers:
        assert_equal(header.get("authorization"), "Bearer tpuf_test", "authorization header")
        assert "application/json" in header.get("content-type", ""), "content-type header"

    schema_body = find_body(
        bodies,
        lambda body: "schema" in body,
        "request with schema",
    )
    assert_equal(schema_body["distance_metric"], "cosine_distance", "distance metric")
    assert_equal(schema_body["disable_backpressure"], True, "disable_backpressure")
    assert_equal(schema_body["sharding"]["num_shards"], 8, "num_shards")

    schema = schema_body["schema"]
    assert_equal(schema["body"]["type"], "string", "body type")
    assert_equal(schema["body"]["filterable"], True, "body filterable")
    assert_equal(schema["body"]["full_text_search"], True, "body full_text_search")
    assert_equal(schema["note_contents"]["type"], "[]string", "note_contents type")
    assert_equal(
        schema["note_contents"]["full_text_search"],
        True,
        "note_contents full_text_search",
    )
    assert_equal(schema["is_starred"]["type"], "bool", "is_starred type")
    assert_equal(schema["published_at"]["type"], "datetime", "published_at type")
    assert_equal(schema["embedding"]["type"], "[3]f32", "embedding type")
    assert_equal(schema["embedding"]["ann"], True, "embedding ann")

    delete_request = find_body(
        bodies,
        lambda body: "deletes" in body and "delete-me" in body["deletes"],
        "delete request for delete-me",
    )
    assert_equal(delete_request["distance_metric"], "cosine_distance", "delete distance metric")

    upsert_request = find_body(
        bodies,
        lambda body: "upsert_rows" in body
        and any(row.get("id") == "upsert-me" for row in body["upsert_rows"]),
        "upsert request for upsert-me",
    )
    rows = {row["id"]: row for row in upsert_request["upsert_rows"]}
    assert_equal(rows["upsert-me"]["body"], "inserted after delete", "upsert body")
    assert_equal(rows["upsert-me"]["note_contents"], ["new", "row"], "upsert note contents")
    assert_equal(
        rows["upsert-me"]["published_at"],
        "2026-06-16T04:05:06.000000",
        "upsert published_at",
    )
    assert_float_list_close(rows["upsert-me"]["embedding"], [0.4, 0.5, 0.6], "upsert vector")


if __name__ == "__main__":
    main()
