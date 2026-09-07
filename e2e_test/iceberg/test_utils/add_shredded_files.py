"""Register natively-written shredded variant parquet files into an iceberg table.

Spark's iceberg writer never shreds, so the test writes shredded files with the
native parquet writer instead. Those files carry no iceberg field ids; stamp the
ids the table schema expects (matching what iceberg's own writer embeds), then
`add_files` the directory. Invoked from iceberg_source_variant_shredded.slt.
"""

import configparser

import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import fs
from pyspark.sql import SparkSession

FILES_DIR = "icebergdata/variant_shredded_files"
FIELD_IDS = {"id": 1, "v": 2, "s": 3}

s3 = fs.S3FileSystem(
    endpoint_override="127.0.0.1:9301",
    scheme="http",
    access_key="hummockadmin",
    secret_key="hummockadmin",
)

stamped = 0
for info in s3.get_file_info(fs.FileSelector(FILES_DIR)):
    if not info.path.endswith(".parquet"):
        continue
    table = pq.read_table(info.path, filesystem=s3)
    fields = [
        f.with_metadata({b"PARQUET:field_id": str(FIELD_IDS[f.name]).encode()})
        for f in table.schema
    ]
    table = pa.table(table.columns, schema=pa.schema(fields))
    pq.write_table(table, info.path, filesystem=s3)
    stamped += 1
assert stamped > 0, f"no parquet files found under {FILES_DIR}"
print(f"stamped field ids into {stamped} files")

config = configparser.ConfigParser()
config.read("config.ini")
spark = SparkSession.builder.remote(config["spark"]["url"]).getOrCreate()
spark.sql(
    """CALL demo.system.add_files(
         table => 'demo_db.test_variant_shredded',
         source_table => '`parquet`.`s3a://icebergdata/variant_shredded_files`')"""
).show()
