import argparse
import configparser
import os
import sys
from datetime import datetime
from typing import Optional

import tomli as toml

from test_utils import (
    log,
    LogLevel,
    TestResult,
    print_test_summary,
    init_iceberg_table,
    compare_sql,
    drop_table,
    discover_test_cases,
    execute_slt,
    prepare_test_env,
    verify_result,
)


def run_test_case(test_file: str, args: dict) -> TestResult:
    """Run a single test case."""
    log(f"\nRunning test case: {test_file}", level=LogLevel.HEADER)
    start_time = datetime.now()
    error = None
    success = True
    drop_sqls = None

    try:
        with open(test_file, "rb") as f:
            test_case = toml.load(f)

            # Extract content from testcase
            init_sqls = test_case["init_sqls"]
            slt = test_case.get("slt")
            log(f"slt: {slt}", level=LogLevel.INFO)
            verify_schema = test_case.get("verify_schema")
            log(f"verify_schema: {verify_schema}", level=LogLevel.INFO)
            verify_sql = test_case.get("verify_sql")
            log(f"verify_sql: {verify_sql}", level=LogLevel.INFO)
            verify_data = test_case.get("verify_data")
            verify_slt = test_case.get("verify_slt")
            cmp_sqls = test_case.get("cmp_sqls")
            drop_sqls = test_case["drop_sqls"]

            init_iceberg_table(args, init_sqls)
            if slt:
                execute_slt(args, slt)
            if verify_data and verify_sql and verify_schema:
                verify_result(args, verify_sql, verify_schema, verify_data)
            if cmp_sqls and len(cmp_sqls) == 2:
                compare_sql(args, cmp_sqls)
            if verify_slt:
                execute_slt(args, verify_slt)
    except KeyboardInterrupt:
        log(f"test case {test_file} interrupted by user", level=LogLevel.ERROR)
        error = "Interrupted by user"
        success = False
        # Don't raise the exception, just return the failed result
        duration = datetime.now() - start_time
        return TestResult(test_file, success, duration, error)
    except Exception as e:
        log(f"test case {test_file} failed: {e}", level=LogLevel.ERROR)
        error = e
        success = False
        raise e
    finally:
        # Clean up even when the case failed: a leftover table keeps the shared
        # namespace non-empty and fails every later case's DROP SCHEMA.
        if drop_sqls:
            try:
                drop_table(args, drop_sqls)
            except Exception as e:
                # drop_table already logged the cause; don't mask an earlier failure.
                if success:
                    error = e
                    success = False
        duration = datetime.now() - start_time
        return TestResult(test_file, success, duration, error)


def get_parallel_job_info() -> Optional[tuple[int, int]]:
    """Get parallel job information from environment variables."""
    job = os.environ.get("BUILDKITE_PARALLEL_JOB")
    if job is None:
        return None
    job = int(job)
    total = int(os.environ.get("BUILDKITE_PARALLEL_JOB_COUNT"))
    return job, total


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test script for iceberg")
    parser.add_argument(
        "-t",
        dest="test_case",
        type=str,
        help="Optional: specific test case file (toml file) to run. If not provided, all test cases will be run.",
    )

    args = parser.parse_args()
    config = configparser.ConfigParser()
    config.read("config.ini")
    config_dict = {section: dict(config[section]) for section in config.sections()}

    prepare_test_env()

    test_results = []

    try:
        if args.test_case:
            # Run single test case if specified
            result = run_test_case(args.test_case, config_dict)
            test_results.append(result)
        else:
            # Run discovered test cases
            test_files = discover_test_cases()
            log(f"Discovered {len(test_files)} test cases", level=LogLevel.INFO)

            # Get parallel job information
            parallel_info = get_parallel_job_info()
            if parallel_info:
                job_index, total_jobs = parallel_info
                # Distribute test files among parallel jobs
                test_files.sort()  # Ensure consistent distribution
                test_files = test_files[job_index::total_jobs]
                log(
                    f"Running job {job_index + 1} of {total_jobs} with {len(test_files)} test cases",
                    level=LogLevel.INFO,
                )

            # Run all test cases assigned to this job
            for test_file in test_files:
                result = run_test_case(test_file, config_dict)
                test_results.append(result)
                if (
                    not result.success
                    and isinstance(result.error, str)
                    and "Interrupted" in result.error
                ):
                    # If the test was interrupted, stop running more tests
                    break
    finally:
        # Always print summary, even if some tests failed or were interrupted
        print_test_summary(test_results)
        # Exit with error code if any test failed
        if any(not result.success for result in test_results):
            sys.exit(1)
