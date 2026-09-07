#!/usr/bin/env python3

"""Smoke-test table-refill role projection; simulation owns exact vnode oracles."""

import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import psycopg2


JOBS = {
    "refill_streaming_mv": "streaming",
    "refill_serving_mv": "serving",
    "refill_both_mv": "both",
}
EXPECTED_ROLES = {(True, False), (False, True)}
TIMEOUT_SECONDS = 60
RETRY_INTERVAL_SECONDS = 2
RISECTL = Path(__file__).resolve().with_name("risectl")


def connect():
    return psycopg2.connect(
        host=os.environ.get("RISEDEV_RW_FRONTEND_LISTEN_ADDRESS")
        or os.environ.get("SLT_HOST", "127.0.0.1"),
        port=os.environ.get("RISEDEV_RW_FRONTEND_PORT")
        or os.environ.get("SLT_PORT", "4566"),
        dbname=os.environ.get("SLT_DB", "dev"),
        user="root",
        password="",
    )


def query(conn, sql: str, params: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
    with conn.cursor() as cursor:
        cursor.execute(sql, params)
        return cursor.fetchall()


def load_fixture(conn):
    result_rows = query(
        conn,
        "SELECT id, name FROM rw_catalog.rw_materialized_views "
        "WHERE name = ANY(%s)",
        (list(JOBS),),
    )
    result_ids = {str(name): int(table_id) for table_id, name in result_rows}
    if set(result_ids) != set(JOBS):
        raise RuntimeError(
            f"expected materialized views {sorted(JOBS)}, got {sorted(result_ids)}"
        )

    internal_rows = query(conn, "SELECT id, job_id FROM rw_catalog.rw_internal_table_info")
    all_internal_ids = {int(table_id) for table_id, _ in internal_rows}
    internal_ids_by_job = {name: set() for name in JOBS}
    job_name_by_id = {job_id: name for name, job_id in result_ids.items()}
    for table_id, job_id in internal_rows:
        if name := job_name_by_id.get(int(job_id)):
            internal_ids_by_job[name].add(int(table_id))
    missing = [name for name, ids in internal_ids_by_job.items() if not ids]
    if missing:
        raise RuntimeError(f"stateful materialized views have no internal tables: {missing}")

    expected_policy_by_table = {}
    for name, policy in JOBS.items():
        table_ids = {result_ids[name], *internal_ids_by_job[name]}
        expected_policy_by_table.update((table_id, policy) for table_id in table_ids)

    worker_rows = query(
        conn,
        "SELECT id, is_streaming, is_serving FROM rw_catalog.rw_worker_nodes "
        "WHERE is_streaming IS NOT NULL AND state = 'RUNNING'",
    )
    workers = {
        int(worker_id): (bool(is_streaming), bool(is_serving))
        for worker_id, is_streaming, is_serving in worker_rows
    }
    roles = set(workers.values())
    if roles != EXPECTED_ROLES:
        raise RuntimeError(f"expected pure streaming and serving roles, got {roles}")

    return result_ids, all_internal_ids, expected_policy_by_table, workers


def fetch_stats() -> dict[int, dict[str, Any]]:
    env = os.environ.copy()
    env.setdefault("RW_META_ADDR", "http://127.0.0.1:5690")
    result = subprocess.run(
        [str(RISECTL), "hummock", "refill", "stats"],
        capture_output=True,
        text=True,
        timeout=15,
        env=env,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip())

    payload = json.loads(result.stdout)
    stats = {int(entry["worker_id"]): entry["stats"] for entry in payload}
    if len(stats) != len(payload):
        raise RuntimeError("refill stats contain duplicate worker ids")
    return stats


def id_map(value: Any) -> dict[int, Any]:
    if not isinstance(value, dict):
        raise RuntimeError(f"expected JSON object, got {type(value).__name__}")
    return {int(table_id): entry for table_id, entry in value.items()}


def validate_worker(
    worker_id: int,
    role: tuple[bool, bool],
    stats: dict[str, Any],
    result_ids: dict[str, int],
    all_internal_ids: set[int],
    expected_policy_by_table: dict[int, str],
) -> list[str]:
    errors = []
    policies = id_map(stats.get("policies"))
    contexts = id_map(stats.get("contexts"))
    internal = stats.get("internal")
    if not isinstance(internal, dict):
        return [f"worker {worker_id}: missing internal runtime state"]
    raw_streaming = id_map(internal.get("streaming"))
    raw_serving = id_map(internal.get("serving"))

    is_streaming, is_serving = role
    expected_policies = (
        expected_policy_by_table
        if is_streaming
        else {result_ids[name]: policy for name, policy in JOBS.items()}
    )
    for table_id, expected_policy in expected_policies.items():
        if policies.get(table_id) != expected_policy:
            errors.append(
                f"worker {worker_id}: table {table_id} policy is "
                f"{policies.get(table_id)!r}, expected {expected_policy!r}"
            )
        context = contexts.get(table_id)
        if is_streaming and context is None:
            errors.append(f"worker {worker_id}: table {table_id} context is missing")
        elif context is not None and context.get("policy") != expected_policy:
            errors.append(
                f"worker {worker_id}: table {table_id} context policy is "
                f"{context.get('policy')!r}, expected {expected_policy!r}"
            )

    if is_streaming:
        if raw_serving:
            errors.append(f"worker {worker_id}: streaming-only worker has serving mapping")
        for table_id, policy in expected_policies.items():
            context = contexts.get(table_id, {})
            if policy == "serving" and context.get("streaming") is not None:
                errors.append(
                    f"worker {worker_id}: table {table_id} has unexpected streaming context "
                    f"for policy {policy!r}"
                )
            if context.get("serving") is not None:
                errors.append(f"worker {worker_id}: table {table_id} has a serving context")

    if is_serving:
        leaked = all_internal_ids & (set(policies) | set(contexts) | set(raw_serving))
        if leaked:
            errors.append(
                f"worker {worker_id}: pure-serving state contains internal tables "
                f"{sorted(leaked)}"
            )
        if raw_streaming:
            errors.append(f"worker {worker_id}: serving-only worker has streaming mapping")
        for name, policy in JOBS.items():
            context = contexts.get(result_ids[name], {})
            if policy == "streaming" and context.get("serving") is not None:
                errors.append(
                    f"worker {worker_id}: {name} has unexpected serving context "
                    f"for policy {policy!r}"
                )
            if context.get("streaming") is not None:
                errors.append(f"worker {worker_id}: {name} has a streaming context")

    return errors


def validate(stats_by_worker, fixture) -> list[str]:
    result_ids, all_internal_ids, expected_policy_by_table, workers = fixture
    if set(stats_by_worker) != set(workers):
        return [
            "refill stats worker set mismatch: "
            f"expected {sorted(workers)}, got {sorted(stats_by_worker)}"
        ]

    errors = []
    for worker_id, role in workers.items():
        errors.extend(
            validate_worker(
                worker_id,
                role,
                stats_by_worker[worker_id],
                result_ids,
                all_internal_ids,
                expected_policy_by_table,
            )
        )
    for field, role_index, scoped_policies in [
        ("streaming", 0, {"streaming", "both"}),
        ("serving", 1, {"serving", "both"}),
    ]:
        role_workers = [worker_id for worker_id, role in workers.items() if role[role_index]]
        scoped_tables = (
            expected_policy_by_table
            if field == "streaming"
            else {result_ids[name]: policy for name, policy in JOBS.items()}
        )
        for table_id, policy in scoped_tables.items():
            if policy in scoped_policies and not any(
                bool(
                    id_map(stats_by_worker[worker_id]["contexts"])
                    .get(table_id, {}).get(field)
                )
                for worker_id in role_workers
            ):
                errors.append(f"table {table_id}: no {field} worker reports scoped ownership")
    return errors


def main() -> int:
    with connect() as conn:
        fixture = load_fixture(conn)

    deadline = time.monotonic() + TIMEOUT_SECONDS
    last_failure = "no refill stats observed"
    while time.monotonic() < deadline:
        try:
            errors = validate(fetch_stats(), fixture)
            if not errors:
                print("table refill runtime state converged")
                return 0
            last_failure = "\n".join(errors)
        except Exception as error:  # Retry transient recovery and RPC failures.
            last_failure = str(error)
        time.sleep(RETRY_INTERVAL_SECONDS)

    print(f"table refill runtime state did not converge:\n{last_failure}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
