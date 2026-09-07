#!/usr/bin/env python3
# Copyright 2025 RisingWave Labs
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

"""
Helper script for LanceDB e2e sink tests.

Usage:
  python3 lancedb_helper.py setup <db_uri>
      Create the LanceDB database and "e2e_test" table with the expected schema.

  python3 lancedb_helper.py verify <db_uri> <expected_count>
      Verify row count and print all rows in CSV format for diffing.
"""

import sys

import lancedb
import pyarrow as pa
import pyarrow.compute


TABLE_NAME = "e2e_test"

SCHEMA = pa.schema([
    pa.field("id", pa.int32(), nullable=False),
    pa.field("name", pa.utf8(), nullable=False),
    pa.field("score", pa.float64(), nullable=False),
    pa.field("flag", pa.bool_(), nullable=False),
])


def setup(db_uri: str):
    """Create a LanceDB database with an empty table."""
    db = lancedb.connect(db_uri)
    # Create an empty table with the defined schema
    db.create_table(TABLE_NAME, schema=SCHEMA)
    print(f"Created LanceDB table '{TABLE_NAME}' at {db_uri}")


def verify(db_uri: str, expected_count: int):
    """Verify row count and print rows as CSV."""
    db = lancedb.connect(db_uri)
    table = db.open_table(TABLE_NAME)
    arrow_table = table.to_arrow()

    actual_count = arrow_table.num_rows
    if actual_count != expected_count:
        print(
            f"ERROR: expected {expected_count} rows but found {actual_count}",
            file=sys.stderr,
        )
        sys.exit(1)

    # Sort by id for deterministic output
    indices = pa.compute.sort_indices(arrow_table, sort_keys=[("id", "ascending")])
    arrow_table = arrow_table.take(indices)

    # Print CSV without header for easy diffing
    for i in range(arrow_table.num_rows):
        row_id = arrow_table.column("id")[i].as_py()
        name = arrow_table.column("name")[i].as_py()
        score = arrow_table.column("score")[i].as_py()
        flag = arrow_table.column("flag")[i].as_py()
        flag_str = "true" if flag else "false"
        print(f"{row_id},{name},{score},{flag_str}")


def main():
    if len(sys.argv) < 3:
        print(__doc__, file=sys.stderr)
        sys.exit(1)

    command = sys.argv[1]
    db_uri = sys.argv[2]

    if command == "setup":
        setup(db_uri)
    elif command == "verify":
        if len(sys.argv) < 4:
            print("Usage: lancedb_helper.py verify <db_uri> <expected_count>", file=sys.stderr)
            sys.exit(1)
        expected_count = int(sys.argv[3])
        verify(db_uri, expected_count)
    else:
        print(f"Unknown command: {command}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
