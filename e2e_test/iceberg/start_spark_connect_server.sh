#!/usr/bin/env bash

set -e

# stop if already running
if nc -z localhost 15002; then
    echo "Spark connect server already running at port 15002"
    exit 0
fi

SCRIPT_DIR="$(dirname "$0")"
cd "$SCRIPT_DIR"

source "${SCRIPT_DIR}/../commands/common.sh"

# check java version, only 17/21 are supported for Spark 4.0
if type -p java; then
    JAVA_VER=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}' | cut -d'.' -f1)
    if [[ "$JAVA_VER" != "17" && "$JAVA_VER" != "21" ]]; then
        echo -e "\e[31mOnly Java 17/21 are supported for Spark 4.0. Current version: $JAVA_VER\e[0m"
        exit 1
    fi
else
    echo -e "\e[31mJava not found. Please install Java 17 or 21.\e[0m"
    exit 1
fi


SPARK_FILE="spark-${SPARK_VERSION}-bin-hadoop3.tgz"
if [ ! -d "spark-${SPARK_VERSION}-bin-hadoop3" ];then
    echo "Downloading Spark ${SPARK_VERSION}..."
    START_TIME=$(date +%s)
    wget --no-verbose https://rw-ci-deps-dist.s3.amazonaws.com/$SPARK_FILE
    END_TIME=$(date +%s)
    ELAPSED_TIME=$((END_TIME - START_TIME))
    echo "Download ${SPARK_FILE} took ${ELAPSED_TIME} seconds"
    tar -xzf $SPARK_FILE --no-same-owner
fi

# start new server
./spark-${SPARK_VERSION}-bin-hadoop3/sbin/start-connect-server.sh --packages $SPARK_PACKAGES \
  --master local[3] \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.demo=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.demo.cache-enabled=false \
  --conf spark.sql.catalog.demo.type=hadoop \
  --conf spark.sql.catalog.demo.warehouse=s3a://icebergdata/demo \
  --conf spark.sql.catalog.demo.hadoop.fs.s3a.endpoint=http://127.0.0.1:9301 \
  --conf spark.sql.catalog.demo.hadoop.fs.s3a.path.style.access=true \
  --conf spark.sql.catalog.demo.hadoop.fs.s3a.access.key=hummockadmin \
  --conf spark.sql.catalog.demo.hadoop.fs.s3a.secret.key=hummockadmin \
  --conf spark.sql.defaultCatalog=demo

echo "Waiting spark connector server to launch on 15002..."

while ! nc -z localhost 15002; do
  sleep 1 # wait for 1/10 of the second before check again
done

echo "Spark connect server launched"
