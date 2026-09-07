#!/usr/bin/env bash

# Exits as soon as any line fails.
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

sink_test_env_setup "$profile"


echo "--- create clickhouse table"
# v9 is left out of the sink on purpose: a Decimal256 column the sink does not write must not break it.
curl https://clickhouse.com/ | sh
sleep 2
./clickhouse client --host=clickhouse-server --port=9000 --password='default' --query="SET enable_json_type = 1;CREATE table demo_test(v1 Int32,v2 Int64,v3 String,v4 Enum16('A'=1,'B'=2), v5 decimal64(3), v6 Nullable(String), v7 DateTime64(6), v8 json, v9 Decimal(76, 4))ENGINE = ReplacingMergeTree PRIMARY KEY (v1);"

echo "--- testing sinks"
sqllogictest -p 4566 -d dev './e2e_test/sink/clickhouse_sink.slt'
sleep 5
./clickhouse client --host=clickhouse-server --port=9000 --password='default' --query="select v1,v2,v3,v4,v5,v6,v7,v8 from demo_test FORMAT CSV;" > ./query_result.csv

# check sink destination using shell
# Check each expected row exists in CSV output (order independent)
# Note: JSON field contains comma, so it gets split into $6 and $7 by awk
if awk -F "," '
BEGIN { c1=0; c2=0; c3=0; c4=0; c5=0; c6=0; c7=0; }
{
  if ($1 == 1 && $2 == 50 && $3 == "\"1-50\"" && $4 == "\"A\"" && $5 == 1.1 && $6 == "\\N" && $7 == "\"2013-01-01 00:01:01.000000\"" && $8 == "\"{\"\"sd\"\":123" && $9 == "\"\"sda\"\":23}\"") c1++;
  if ($1 == 13 && $2 == 2 && $3 == "\"13-2\"" && $4 == "\"B\"" && $5 == 0 && $6 == "\\N" && $7 == "\"2013-01-01 00:01:01.000000\"" && $8 == "\"{\"\"sd\"\":123" && $9 == "\"\"sda\"\":23}\"") c2++;
  if ($1 == 2 && $2 == 2 && $3 == "\"2-2\"" && $4 == "\"B\"" && $5 == 2.2 && $6 == "\\N" && $7 == "\"2013-01-01 00:01:01.000000\"" && $8 == "\"{\"\"sd\"\":123" && $9 == "\"\"sda\"\":23}\"") c3++;
  if ($1 == 21 && $2 == 2 && $3 == "\"21-2\"" && $4 == "\"A\"" && $5 == 0 && $6 == "\\N" && $7 == "\"2013-01-01 00:01:01.000000\"" && $8 == "\"{\"\"sd\"\":123" && $9 == "\"\"sda\"\":23}\"") c4++;
  if ($1 == 3 && $2 == 2 && $3 == "\"3-2\"" && $4 == "\"A\"" && $5 == 3.3 && $6 == "\\N" && $7 == "\"2013-01-01 00:01:01.000000\"" && $8 == "\"{\"\"sd\"\":123" && $9 == "\"\"sda\"\":23}\"") c5++;
  if ($1 == 5 && $2 == 2 && $3 == "\"5-2\"" && $4 == "\"B\"" && $5 == 4.4 && $6 == "\\N" && $7 == "\"2013-01-01 00:01:01.000000\"" && $8 == "\"{\"\"sd\"\":123" && $9 == "\"\"sda\"\":23}\"") c6++;
  if ($1 == 8 && $2 == 2 && $3 == "\"8-2\"" && $4 == "\"A\"" && $5 == 0 && $6 == "\\N" && $7 == "\"2013-01-01 00:01:01.000000\"" && $8 == "\"{\"\"sd\"\":123" && $9 == "\"\"sda\"\":23}\"") c7++;
}
END { exit !(c1 == 1 && c2 == 1 && c3 == 1 && c4 == 1 && c5 == 1 && c6 == 1 && c7 == 1); }
' ./query_result.csv; then
  echo "Clickhouse sink check passed"
else
  echo "The output is not as expected."
  cat ./query_result.csv
  exit 1
fi

echo "--- Kill cluster"
risedev ci-kill
