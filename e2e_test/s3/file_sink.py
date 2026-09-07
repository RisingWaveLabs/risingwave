import os
import sys
import random
import psycopg2
import json
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from datetime import datetime, timezone
from time import sleep
from minio import Minio
from random import uniform
from time import sleep
import numpy as np
import time

def gen_data(file_num, item_num_per_file):
    assert item_num_per_file % 2 == 0, \
        f'item_num_per_file should be even to ensure sum(mark) == 0: {item_num_per_file}'

    struct_type = pa.struct([
        ('field1', pa.int32()),
        ('field2', pa.string())
    ])

    return [
        [{
            'id': file_id * item_num_per_file + item_id,
            'name': f'{file_id}_{item_id}_{file_id * item_num_per_file + item_id}',
            'sex': item_id % 2,
            'mark': (-1) ** (item_id % 2),
            'test_int': pa.scalar(1, type=pa.int32()),
            'test_int8': pa.scalar(1, type=pa.int8()),
            'test_uint8': pa.scalar(item_id % 256, type=pa.uint8()),  # UInt8
            'test_uint16': pa.scalar(item_id % 65536, type=pa.uint16()),  # UInt16
            'test_uint32': pa.scalar(item_id % (2**32), type=pa.uint32()),  # UInt32
            'test_uint64': pa.scalar(item_id % (2**64), type=pa.uint64()),  # UInt64
            'test_float_16': pa.scalar(np.float16(4.0), type=pa.float16()),
            'test_real': pa.scalar(4.0, type=pa.float32()),
            'test_double_precision': pa.scalar(5.0, type=pa.float64()),
            'test_varchar': pa.scalar('7', type=pa.string()),
            'test_bytea': pa.scalar(b'\xDe00BeEf', type=pa.binary()),
            'test_date': pa.scalar(datetime.now().date(), type=pa.date32()),
            'test_time': pa.scalar(datetime.now().time(), type=pa.time64('us')),
            'test_timestamp_s': pa.scalar(datetime.now().timestamp(), type=pa.timestamp('s')),
            'test_timestamp_ms': pa.scalar(datetime.now().timestamp() * 1000, type=pa.timestamp('ms')),
            'test_timestamp_us': pa.scalar(datetime.now().timestamp() * 1000000, type=pa.timestamp('us')),
            'test_timestamp_ns': pa.scalar(datetime.now().timestamp() * 1000000000, type=pa.timestamp('ns')),
            'test_timestamptz_s': pa.scalar(datetime.now().timestamp(), type=pa.timestamp('s', tz='+00:00')),
            'test_timestamptz_ms': pa.scalar(datetime.now().timestamp() * 1000, type=pa.timestamp('ms', tz='+00:00')),
            'test_timestamptz_us': pa.scalar(datetime.now().timestamp() * 1000000, type=pa.timestamp('us', tz='+00:00')),
            'test_timestamptz_ns': pa.scalar(datetime.now().timestamp() * 1000000000, type=pa.timestamp('ns', tz='+00:00')),
            'nested_struct': pa.scalar((item_id, f'struct_value_{item_id}'), type=struct_type),
        } for item_id in range(item_num_per_file)]
        for file_id in range(file_num)
    ]

def do_test(config, file_num, item_num_per_file, prefix):
    conn = psycopg2.connect(
        host="localhost",
        port="4566",
        user="root",
        database="dev"
    )

    # Open a cursor to execute SQL statements
    cur = conn.cursor()

    def _table():
        return 's3_test_parquet'

    print("test table function file scan")
    cur.execute(f'''
    SELECT
        count(*)
         FROM file_scan(
        'parquet',
        's3',
        'http://127.0.0.1:9301',
        'hummockadmin',
        'hummockadmin',
        's3://hummock001/test_file_scan/test_file_scan.parquet'
        );''')
    try:
        result = cur.fetchone()
        assert result[0] == 2000, f'file scan assertion failed: the first column is {result[0]}, expect 2000.'
    except ValueError as e:
        print(f"cur.fetchone() got ValueError: {e}")

    print("file scan test pass")
    # Execute a SELECT statement
    cur.execute(f'''CREATE TABLE {_table()}(
        id bigint primary key,
        name TEXT,
        sex bigint,
        mark bigint,
        test_int int,
        test_int8 smallint,
        test_uint8 smallint,
        test_uint16 int,
        test_uint32 bigint,
        test_uint64 decimal,
        test_float_16 real,
        test_real real,
        test_double_precision double precision,
        test_varchar varchar,
        test_bytea bytea,
        test_date date,
        test_time time,
        test_timestamp_s timestamp,
        test_timestamp_ms timestamp,
        test_timestamp_us timestamp,
        test_timestamp_ns timestamp,
        test_timestamptz_s timestamptz,
        test_timestamptz_ms timestamptz,
        test_timestamptz_us timestamptz,
        test_timestamptz_ns timestamptz,
        nested_struct STRUCT<"field1" int, "field2" varchar>
    ) WITH (
        connector = 's3',
        match_pattern = '*.parquet',
        s3.region_name = 'custom',
        s3.bucket_name = 'hummock001',
        s3.credentials.access = 'hummockadmin',
        s3.credentials.secret = 'hummockadmin',
        s3.endpoint_url = 'http://hummock001.127.0.0.1:9301',
        refresh.interval.sec = 1,
    ) FORMAT PLAIN ENCODE PARQUET;''')

    total_rows = file_num * item_num_per_file
    MAX_RETRIES = 40
    for retry_no in range(MAX_RETRIES):
        cur.execute(f'select count(*) from {_table()}')
        result = cur.fetchone()
        if result[0] == total_rows:
            break
        print(f"[retry {retry_no}] Now got {result[0]} rows in table, {total_rows} expected, wait 10s")
        sleep(10)

    stmt = f'select count(*), sum(id) from {_table()}'
    print(f'Execute {stmt}')
    cur.execute(stmt)
    result = cur.fetchone()

    print('Got:', result)

    def _assert_eq(field, got, expect):
        assert got == expect, f'{field} assertion failed: got {got}, expect {expect}.'

    _assert_eq('count(*)', result[0], total_rows)
    _assert_eq('sum(id)', result[1], (total_rows - 1) * total_rows / 2)

    print('File source test pass!')

    cur.close()
    conn.close()

def do_sink(config, file_num, item_num_per_file, prefix):
    conn = psycopg2.connect(
        host="localhost",
        port="4566",
        user="root",
        database="dev"
    )

    # Open a cursor to execute SQL statements
    cur = conn.cursor()

    def _table():
        return 's3_test_parquet'

    # Execute a SELECT statement
    cur.execute(f'''CREATE sink test_file_sink_parquet as select
        id,
        name,
        sex,
        mark,
        test_int,
        test_int8,
        test_uint8,
        test_uint16,
        test_uint32,
        test_uint64,
        test_float_16,
        test_real,
        test_double_precision,
        test_varchar,
        test_bytea,
        test_date,
        test_time,
        test_timestamp_s,
        test_timestamp_ms,
        test_timestamp_us,
        test_timestamp_ns,
        test_timestamptz_s,
        test_timestamptz_ms,
        test_timestamptz_us,
        test_timestamptz_ns,
        nested_struct
        from {_table()} WITH (
        connector = 's3',
        s3.region_name = 'custom',
        s3.bucket_name = 'hummock001',
        s3.credentials.access = 'hummockadmin',
        s3.credentials.secret = 'hummockadmin',
        s3.endpoint_url = 'http://hummock001.127.0.0.1:9301',
        s3.path = 'test_parquet_sink/',
        type = 'append-only',
        force_append_only='true'
    ) FORMAT PLAIN ENCODE PARQUET(force_append_only='true');''')

    print('Sink into s3 in parquet encode...')
    # Execute a SELECT statement
    cur.execute(f'''CREATE TABLE test_parquet_sink_table(
        id bigint primary key,
        name TEXT,
        sex bigint,
        mark bigint,
        test_int int,
        test_int8 smallint,
        test_uint8 smallint,
        test_uint16 int,
        test_uint32 bigint,
        test_uint64 decimal,
        test_float_16 real,
        test_real real,
        test_double_precision double precision,
        test_varchar varchar,
        test_bytea bytea,
        test_date date,
        test_time time,
        test_timestamp_s timestamp,
        test_timestamp_ms timestamp,
        test_timestamp_us timestamp,
        test_timestamp_ns timestamp,
        test_timestamptz_s timestamptz,
        test_timestamptz_ms timestamptz,
        test_timestamptz_us timestamptz,
        test_timestamptz_ns timestamptz,
        nested_struct STRUCT<"field1" int, "field2" varchar>,
    ) WITH (
        connector = 's3',
        match_pattern = 'test_parquet_sink/*.parquet',
        s3.region_name = 'custom',
        s3.bucket_name = 'hummock001',
        s3.credentials.access = 'hummockadmin',
        s3.credentials.secret = 'hummockadmin',
        s3.endpoint_url = 'http://hummock001.127.0.0.1:9301',
        refresh.interval.sec = 1,
    ) FORMAT PLAIN ENCODE PARQUET;''')
    total_rows = file_num * item_num_per_file
    MAX_RETRIES = 40
    for retry_no in range(MAX_RETRIES):
        cur.execute(f'select count(*) from test_parquet_sink_table')
        result = cur.fetchone()
        if result[0] == total_rows:
            break
        print(f"[retry {retry_no}] Now got {result[0]} rows in table, {total_rows} expected, wait 10s")
        sleep(10)

    stmt = f'select count(*), sum(id) from test_parquet_sink_table'
    print(f'Execute reading sink files: {stmt}')

    print(f'Create s3 sink json format')
    # Execute a SELECT statement
    cur.execute(f'''CREATE sink test_file_sink_json as select
        id,
        name,
        sex,
        mark,
        test_int,
        test_int8,
        test_uint8,
        test_uint16,
        test_uint32,
        test_uint64,
        test_float_16,
        test_real,
        test_double_precision,
        test_varchar,
        test_bytea,
        test_date,
        test_time,
        test_timestamp_s,
        test_timestamp_ms,
        test_timestamp_us,
        test_timestamp_ns,
        test_timestamptz_s,
        test_timestamptz_ms,
        test_timestamptz_us,
        test_timestamptz_ns,
        nested_struct
        from {_table()} WITH (
        connector = 'snowflake',
        snowflake.aws_region = 'custom',
        snowflake.s3_bucket = 'hummock001',
        snowflake.aws_access_key_id = 'hummockadmin',
        snowflake.aws_secret_access_key = 'hummockadmin',
        s3.endpoint_url = 'http://hummock001.127.0.0.1:9301',
        s3.path = 'test_json_sink/',
        type = 'append-only',
        force_append_only='true'
    ) FORMAT PLAIN ENCODE JSON(force_append_only='true');''')

    print('Sink into s3 in json encode...')
    # Execute a SELECT statement
    cur.execute(f'''CREATE TABLE test_json_sink_table(
        id bigint primary key,
        name TEXT,
        sex bigint,
        mark bigint,
        test_int int,
        test_int8 smallint,
        test_uint8 smallint,
        test_uint16 int,
        test_uint32 bigint,
        test_uint64 decimal,
        test_float_16 real,
        test_real real,
        test_double_precision double precision,
        test_varchar varchar,
        test_bytea bytea,
        test_date date,
        test_time time,
        test_timestamp_s timestamp,
        test_timestamp_ms timestamp,
        test_timestamp_us timestamp,
        test_timestamp_ns timestamp,
        test_timestamptz_s timestamptz,
        test_timestamptz_ms timestamptz,
        test_timestamptz_us timestamptz,
        test_timestamptz_ns timestamptz,
        nested_struct STRUCT<"field1" int, "field2" varchar>
    ) WITH (
        connector = 's3',
        match_pattern = 'test_json_sink/*.json',
        s3.region_name = 'custom',
        s3.bucket_name = 'hummock001',
        s3.credentials.access = 'hummockadmin',
        s3.credentials.secret = 'hummockadmin',
        s3.endpoint_url = 'http://hummock001.127.0.0.1:9301',
    ) FORMAT PLAIN ENCODE JSON;''')

    total_rows = file_num * item_num_per_file
    MAX_RETRIES = 40
    for retry_no in range(MAX_RETRIES):
        cur.execute(f'select count(*) from test_json_sink_table')
        result = cur.fetchone()
        if result[0] == total_rows:
            break
        print(f"[retry {retry_no}] Now got {result[0]} rows in table, {total_rows} expected, wait 10s")
        sleep(10)

    stmt = f'select count(*), sum(id) from test_json_sink_table'
    print(f'Execute reading sink files: {stmt}')
    cur.execute(stmt)
    result = cur.fetchone()

    print('Got:', result)

    def _assert_eq(field, got, expect):
        assert got == expect, f'{field} assertion failed: got {got}, expect {expect}.'

    _assert_eq('count(*)', result[0], total_rows)
    _assert_eq('sum(id)', result[1], (total_rows - 1) * total_rows / 2)

    print('File sink test pass!')
    cur.execute(f'drop sink test_file_sink_parquet')
    cur.execute(f'drop table test_parquet_sink_table')
    cur.execute(f'drop sink test_file_sink_json')
    cur.execute(f'drop table test_json_sink_table')
    cur.execute(f'drop table s3_test_parquet')
    cur.close()
    conn.close()

def test_file_sink_batching():
    conn = psycopg2.connect(
        host="localhost",
        port="4566",
        user="root",
        database="dev"
    )

    # Open a cursor to execute SQL statements
    cur = conn.cursor()


    # Execute a SELECT statement
    cur.execute(f'''CREATE TABLE t (v1 int, v2 int);''')

    print('test file sink batching...\n')
    cur.execute(f'''CREATE sink test_file_sink_batching as select
        v1, v2 from t WITH (
        connector = 's3',
        s3.region_name = 'custom',
        s3.bucket_name = 'hummock001',
        s3.credentials.access = 'hummockadmin',
        s3.credentials.secret = 'hummockadmin',
        s3.endpoint_url = 'http://hummock001.127.0.0.1:9301',
        s3.path = 'test_file_sink_batching/',
        type = 'append-only',
        rollover_seconds = 5,
        max_row_count = 5,
        force_append_only='true'
    ) FORMAT PLAIN ENCODE PARQUET(force_append_only='true');''')

    cur.execute(f'''CREATE TABLE test_file_sink_batching_table(
        v1 int,
        v2 int,
    ) WITH (
        connector = 's3',
        match_pattern = 'test_file_sink_batching/*.parquet',
        refresh.interval.sec = 1,
        s3.region_name = 'custom',
        s3.bucket_name = 'hummock001',
        s3.credentials.access = 'hummockadmin',
        s3.credentials.secret = 'hummockadmin',
        s3.endpoint_url = 'http://hummock001.127.0.0.1:9301',
    ) FORMAT PLAIN ENCODE PARQUET;''')

    cur.execute(f'''ALTER SINK test_file_sink_batching SET PARALLELISM = 2;''')

    cur.execute(f'''INSERT INTO t VALUES (10, 10);''')


    cur.execute(f'select count(*) from test_file_sink_batching_table')
    # no item will be selectedpsq
    result = cur.fetchone()

    def _assert_eq(field, got, expect):
        assert got == expect, f'{field} assertion failed: got {got}, expect {expect}.'
    def _assert_greater(field, got, expect):
        assert got > expect, f'{field} assertion failed: got {got}, expect {expect}.'

    _assert_eq('count(*)', result[0], 0)
    print('the rollover_seconds has not reached, count(*) = 0')


    time.sleep(11)

    cur.execute(f'select count(*) from test_file_sink_batching_table')
    result = cur.fetchone()
    _assert_eq('count(*)', result[0], 1)
    print('the rollover_seconds has reached, count(*) = ', result[0])

    cur.execute(f'''
    INSERT INTO t VALUES (20, 20);
    INSERT INTO t VALUES (30, 30);
    INSERT INTO t VALUES (40, 40);
    INSERT INTO t VALUES (50, 10);
    ''')

    cur.execute(f'select count(*) from test_file_sink_batching_table')
    # count(*) = 1
    result = cur.fetchone()
    _assert_eq('count(*)', result[0], 1)
    print('the max row count has not reached, count(*) = ', result[0])

    cur.execute(f'''
    INSERT INTO t VALUES (60, 20);
    INSERT INTO t VALUES (70, 30);
    INSERT INTO t VALUES (80, 10);
    INSERT INTO t VALUES (90, 20);
    INSERT INTO t VALUES (100, 30);
    INSERT INTO t VALUES (100, 10);
    ''')

    time.sleep(10)

    cur.execute(f'select count(*) from test_file_sink_batching_table')
    result = cur.fetchone()
    _assert_greater('count(*)', result[0], 1)
    print('the rollover_seconds has reached, count(*) = ', result[0])

    cur.execute(f'drop sink test_file_sink_batching;')
    cur.execute(f'drop table t;')
    cur.execute(f'drop table test_file_sink_batching_table;')
    cur.close()
    conn.close()
    # delete objects

    client = Minio(
        endpoint="127.0.0.1:9301",
        access_key="hummockadmin",
        secret_key="hummockadmin",
        secure=False,
    )
    objects = client.list_objects(bucket_name="hummock001", prefix="test_file_sink_batching/", recursive=True)

    for obj in objects:
        client.remove_object(bucket_name="hummock001", object_name=obj.object_name)
        print(f"Deleted: {obj.object_name}")



if __name__ == "__main__":
    FILE_NUM = 10
    ITEM_NUM_PER_FILE = 2000
    data = gen_data(FILE_NUM, ITEM_NUM_PER_FILE)

    config = json.loads(os.environ["S3_SOURCE_TEST_CONF"])
    client = Minio(
        endpoint="127.0.0.1:9301",
        access_key="hummockadmin",
        secret_key="hummockadmin",
        secure=False,
    )
    run_id = str(random.randint(1000, 9999))
    _local = lambda idx: f'data_{idx}.parquet'
    _s3 = lambda idx: f"{run_id}_data_{idx}.parquet"

    # put s3 files
    for idx, file_data in enumerate(data):
        table = pa.Table.from_pandas(pd.DataFrame(file_data))
        pq.write_table(table, _local(idx))

        client.fput_object(
            bucket_name="hummock001",
            object_name=_s3(idx),
            file_path=_local(idx),
        )
    # put parquet file to test table function file scan
    if data:
        first_file_data = data[0]
        first_table = pa.Table.from_pandas(pd.DataFrame(first_file_data))

        first_file_name = f"test_file_scan.parquet"
        first_file_path = f"test_file_scan/{first_file_name}"

        pq.write_table(first_table, "data_0.parquet")

        client.fput_object(
            bucket_name="hummock001",
            object_name=first_file_path,
            file_path="data_0.parquet",
        )

    # do test
    do_test(config, FILE_NUM, ITEM_NUM_PER_FILE, run_id)

    # clean up s3 files
    for idx, _ in enumerate(data):
       client.remove_object(bucket_name="hummock001", object_name=_s3(idx))

    do_sink(config, FILE_NUM, ITEM_NUM_PER_FILE, run_id)

    # clean up s3 files
    for idx, _ in enumerate(data):
       client.remove_object(bucket_name="hummock001", object_name=_s3(idx))

    # test file sink batching
    test_file_sink_batching()
