#!/usr/bin/env python3

import argparse
import json
import os
import sys

import pulsar
from pulsar.schema import AvroSchema, Integer, JsonSchema, Record, String


class SchemaTestRecord(Record):
    id = Integer(required=True)
    name = String(required=True)


def main():
    parser = argparse.ArgumentParser(
        description="Produce records for the Pulsar Schema Registry source test"
    )
    parser.add_argument(
        "--broker",
        default=os.environ.get("PULSAR_BROKER_URL", "pulsar://localhost:6650"),
    )
    parser.add_argument("--topic", required=True)
    parser.add_argument("--schema-type", choices=("avro", "json"), default="avro")
    args = parser.parse_args()

    client = pulsar.Client(args.broker)
    schema = (
        AvroSchema(SchemaTestRecord)
        if args.schema_type == "avro"
        else JsonSchema(SchemaTestRecord)
    )
    producer = client.create_producer(args.topic, schema=schema)
    try:
        for line in sys.stdin:
            if not line.strip():
                continue
            value = json.loads(line)
            producer.send(SchemaTestRecord(id=value["id"], name=value["name"]))
    finally:
        producer.close()
        client.close()


if __name__ == "__main__":
    main()
