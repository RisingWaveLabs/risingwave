# Iceberg E2E Tests

## Prerequisites

- Java 17 or 21 (required by Spark 4.0)
- [Poetry](https://python-poetry.org/) (Python package manager)

## Running Tests Locally

### 1. Start RisingWave with Minio

```bash
./risedev k; ./risedev clean-data && ./risedev d for-ctl && ./risedev mc mb -p hummock-minio/icebergdata
```

Always restart with this full sequence: `for-ctl` uses an in-memory meta store, so a restart against stale minio data panics the meta node with `Data directory is already used by another cluster`.

### 2. Start Spark Connect Server

```bash
cd e2e_test/iceberg
bash start_spark_connect_server.sh
```

This downloads Spark if needed, resolves Maven packages, and starts the server on port 15002.

If a long-running server fails writes with `Could not find any valid local directory for s3ablock`, macOS cleaned its /tmp buffer dir (and pid file, so the stop script no longer works). Kill it by port and restart:

```bash
lsof -ti :15002 | xargs kill
bash start_spark_connect_server.sh
```

### 3. Run Tests

In `e2e_test/iceberg` (`poetry install --quiet` on first run):

```bash
# single test case
poetry run python main.py -t ./test_case/no_partition_append_only.toml
# all test cases
poetry run python main.py
```

Pure SLT tests, from the repo root:

```bash
./risedev slt './e2e_test/iceberg/test_case/pure_slt/*.slt'
```

`iceberg_engine.slt` needs the Java connector libs (`ENABLE_BUILD_RW_CONNECTOR=true` in `risedev-components.user.env`); without them, `CREATE TABLE ... ENGINE = iceberg` fails with `failed to read connector libs`.

### 4. Cleanup

```bash
# from e2e_test/iceberg
./spark-*/sbin/stop-connect-server.sh
# from the repo root
cd ../.. && ./risedev k
```

## CI

CI runs `ci/scripts/e2e-iceberg-test.sh`. The main suite uses the `ci-iceberg-test` profile, which needs an external postgres for the meta store and prebuilt connector libs downloaded from CI artifacts. The steps above are the local equivalent.

### Multi-batch compaction rounds

CI runs `compaction_round.slt` first in its own profile, with one compactor worker and a
one-plan admission budget. It checks that scheduled compaction rewrites all three
partitions without further sink commits, excludes earlier batches' output, and drains.
It only needs MinIO and the connector libs. Start it with fresh RiseDev data, as
the profile uses an in-memory meta store:

```sh
./risedev d ci-iceberg-compaction-round
./risedev slt './e2e_test/iceberg/compaction_round.slt'
./risedev k
```

## Key Config Files

| File | Description |
|------|-------------|
| `e2e_test/commands/common.sh` | Spark/Iceberg versions and `SPARK_PACKAGES` |
| `e2e_test/iceberg/pyproject.toml` | Python (pyspark) dependencies |
| `e2e_test/iceberg/config.ini` | Spark connect URL and RisingWave connection |
| `e2e_test/iceberg/start_spark_connect_server.sh` | Spark server startup script |

## Test Case Format

- **`.toml` files** in `test_case/` — Orchestrated by `main.py`. Uses Spark to create Iceberg tables, RisingWave SLT for sink/source operations, and Spark for result verification.
- **`.slt` files** in `test_case/pure_slt/` — Pure SQL logic tests, run directly via `risedev slt`.
