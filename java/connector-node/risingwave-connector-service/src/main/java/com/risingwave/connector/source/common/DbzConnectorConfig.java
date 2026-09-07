/*
 * Copyright 2023 RisingWave Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.risingwave.connector.source.common;

import com.mongodb.ConnectionString;
import com.risingwave.connector.api.source.SourceTypeE;
import com.risingwave.connector.cdc.debezium.internal.ConfigurableOffsetBackingStore;
import com.risingwave.connector.cdc.debezium.internal.OpendalSchemaHistory;
import io.debezium.connector.mongodb.MongoDbConnectorConfig;
import io.debezium.heartbeat.DatabaseHeartbeatImpl;
import io.debezium.heartbeat.Heartbeat;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.commons.io.IOUtils;
import org.apache.commons.text.StringSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DbzConnectorConfig {
    private static final Logger LOG = LoggerFactory.getLogger(DbzConnectorConfig.class);

    private static final String WAIT_FOR_STREAMING_START_TIMEOUT_SECS =
            "cdc.source.wait.streaming.start.timeout";

    private static final String QUEUE_MAX_MEMORY_RATIO = "queue.memory.ratio";
    public static final double MIN_QUEUE_MAX_MEMORY_RATIO = 0.01;
    public static final double MAX_QUEUE_MAX_MEMORY_RATIO = 0.8;

    /* Common configs */
    public static final String HOST = "hostname";
    public static final String PORT = "port";
    public static final String USER = "username";
    public static final String PASSWORD = "password";

    public static final String DB_NAME = "database.name";
    public static final String TABLE_NAME = "table.name";

    public static final String DB_SERVERS = "database.servers";

    /* MySQL configs */
    public static final String MYSQL_SERVER_ID = "server.id";
    public static final String MYSQL_SSL_MODE = "ssl.mode";

    /* Postgres configs */
    public static final String PG_SLOT_NAME = "slot.name";
    public static final String PG_PUB_NAME = "publication.name";
    public static final String PG_PUB_CREATE = "publication.create.enable";
    public static final String PG_SCHEMA_NAME = "schema.name";
    public static final String PG_SSL_ROOT_CERT = "ssl.root.cert";
    public static final String PG_IS_AWS_RDS = "postgres.is.aws.rds";
    public static final String PG_TEST_ONLY_FORCE_RDS = "test.only.force.rds";

    /* Sql Server configs */
    public static final String SQL_SERVER_SCHEMA_NAME = "schema.name";
    public static final String SQL_SERVER_ENCRYPT = "database.encrypt";

    /* Oracle configs */
    public static final String ORACLE_PDB_NAME = "database.pdb.name";
    public static final String ORACLE_SCHEMA_NAME = "schema.name";
    public static final String ORACLE_HEARTBEAT_TABLE_NAME = "heartbeat.table.name";

    /* RisingWave configs */
    private static final String DBZ_CONFIG_FILE = "debezium.properties";
    private static final String MYSQL_CONFIG_FILE = "mysql.properties";
    private static final String POSTGRES_CONFIG_FILE = "postgres.properties";
    private static final String MONGODB_CONFIG_FILE = "mongodb.properties";
    private static final String SQL_SERVER_CONFIG_FILE = "sql_server.properties";
    private static final String ORACLE_CONFIG_FILE = "oracle.properties";

    private static final String DBZ_PROPERTY_PREFIX = "debezium.";

    public static final String HEARTBEAT_INTERVAL_KEY =
            DBZ_PROPERTY_PREFIX + Heartbeat.HEARTBEAT_INTERVAL_PROPERTY_NAME;
    public static final String HEARTBEAT_ACTION_QUERY_KEY =
            DBZ_PROPERTY_PREFIX + DatabaseHeartbeatImpl.HEARTBEAT_ACTION_QUERY_PROPERTY_NAME;

    private static final String SNAPSHOT_MODE_KEY = "debezium.snapshot.mode";
    private static final String SNAPSHOT_MODE_BACKFILL = "rw_cdc_backfill";

    public static class MongoDb {
        public static final String MONGO_URL = "mongodb.url";
        public static final String MONGO_COLLECTION_NAME = "collection.name";

        private static final String DATABASE_GROUP = "database";
        private static final Pattern LITERAL_COLLECTION_LIST_PATTERN =
                Pattern.compile(
                        "(?:\\A|[ ]*,[ ]*)"
                                + "(?<database>[A-Za-z_][A-Za-z0-9_-]*)\\."
                                + "(?<collection>[A-Za-z_][A-Za-z0-9_-]*)");
        private static final Set<String> RAW_FILTER_OPTIONS_DISABLING_INFERENCE =
                Set.of(
                        DBZ_PROPERTY_PREFIX + MongoDbConnectorConfig.DATABASE_INCLUDE_LIST.name(),
                        DBZ_PROPERTY_PREFIX + MongoDbConnectorConfig.DATABASE_EXCLUDE_LIST.name(),
                        DBZ_PROPERTY_PREFIX + MongoDbConnectorConfig.FILTERS_MATCH_MODE.name());
    }

    private static Map<String, String> extractDebeziumProperties(
            Map<String, String> userProperties) {
        // retain only debezium properties if any
        var dbzProps = new HashMap<String, String>();
        for (var entry : userProperties.entrySet()) {
            var key = entry.getKey();
            if (key.startsWith(DBZ_PROPERTY_PREFIX)) {
                // remove the prefix
                dbzProps.put(key.substring(DBZ_PROPERTY_PREFIX.length()), entry.getValue());
            }
        }
        return dbzProps;
    }

    private final long sourceId;
    private final SourceTypeE sourceType;
    private final Properties resolvedDbzProps;
    private final boolean isBackfillSource;
    private final int waitStreamingStartTimeout;

    public long getSourceId() {
        return sourceId;
    }

    public SourceTypeE getSourceType() {
        return sourceType;
    }

    public Properties getResolvedDebeziumProps() {
        return resolvedDbzProps;
    }

    public boolean isBackfillSource() {
        return isBackfillSource;
    }

    public int getWaitStreamingStartTimeout() {
        return waitStreamingStartTimeout;
    }

    public DbzConnectorConfig(
            SourceTypeE source,
            long sourceId,
            String startOffset,
            Map<String, String> userProps,
            boolean snapshotDone,
            boolean isCdcSourceJob) {

        var substitutionProps = new HashMap<>(userProps);
        substitutionProps.put("source.id", Long.toString(sourceId));
        StringSubstitutor substitutor = new StringSubstitutor(substitutionProps);
        var dbzProps = initiateDbConfig(DBZ_CONFIG_FILE, substitutor);
        var isCdcBackfill =
                null != userProps.get(SNAPSHOT_MODE_KEY)
                        && userProps.get(SNAPSHOT_MODE_KEY).equals(SNAPSHOT_MODE_BACKFILL);
        var waitStreamingStartTimeout =
                Integer.parseInt(
                        userProps.getOrDefault(WAIT_FOR_STREAMING_START_TIMEOUT_SECS, "60"));

        LOG.info(
                "DbzConnectorConfig: source={}, sourceId={}, startOffset={}, snapshotDone={},"
                        + " isCdcBackfill={}, isCdcSourceJob={}, waitStreamingStartTimeout={}",
                source,
                sourceId,
                startOffset,
                snapshotDone,
                isCdcBackfill,
                isCdcSourceJob,
                waitStreamingStartTimeout);

        if (source == SourceTypeE.MYSQL) {
            var mysqlProps = initiateDbConfig(MYSQL_CONFIG_FILE, substitutor);

            // Enable schema history for all MySQL CDC modes to handle schema changes properly
            mysqlProps.setProperty(OpendalSchemaHistory.SOURCE_ID, String.valueOf(sourceId));
            mysqlProps.setProperty(OpendalSchemaHistory.MAX_RECORDS_PER_FILE_CONFIG, "2048");

            if (isCdcBackfill) {
                // disable snapshot locking at all
                mysqlProps.setProperty("snapshot.locking.mode", "none");
                // If cdc backfill enabled, the source only emit incremental changes, so we must
                // rewind to the given offset and continue binlog reading from there
                if (null != startOffset && !startOffset.isBlank()) {
                    mysqlProps.setProperty("snapshot.mode", "custom");
                    mysqlProps.setProperty(
                            ConfigurableOffsetBackingStore.OFFSET_STATE_VALUE, startOffset);
                } else {
                    // read upstream table schemas and emit incremental changes only
                    mysqlProps.setProperty("snapshot.mode", "no_data");
                }
            } else {
                // if snapshot phase is finished and offset is specified, we will continue binlog
                // reading from the given offset
                if (snapshotDone && null != startOffset && !startOffset.isBlank()) {
                    // 'snapshot.mode=no_data' is used when binlog offset is specified.
                    // Since we use persistent schema history, we only need to snapshot schema when
                    // no offset is passed, restore directly from schema history when offset is
                    // available.
                    mysqlProps.setProperty("snapshot.mode", "custom");
                    mysqlProps.setProperty(
                            ConfigurableOffsetBackingStore.OFFSET_STATE_VALUE, startOffset);
                }
            }

            dbzProps.putAll(mysqlProps);

            if (isCdcSourceJob) {
                // remove table filtering for the shared MySQL source, since we
                // allow user to ingest tables in different database
                LOG.info("Disable table filtering for the shared MySQL source");
                dbzProps.remove("table.include.list");
            }

        } else if (source == SourceTypeE.POSTGRES) {
            var postgresProps = initiateDbConfig(POSTGRES_CONFIG_FILE, substitutor);

            // disable publication auto creation if needed
            var pubAutoCreate =
                    Boolean.parseBoolean(
                            userProps.getOrDefault(DbzConnectorConfig.PG_PUB_CREATE, "true"));
            if (!pubAutoCreate) {
                postgresProps.setProperty("publication.autocreate.mode", "disabled");
            }
            if (isCdcBackfill) {
                // skip the initial snapshot for cdc backfill
                postgresProps.setProperty("snapshot.mode", "no_data");

                // if startOffset is specified, we should continue
                // reading changes from the given offset
                if (null != startOffset && !startOffset.isBlank()) {
                    postgresProps.setProperty(
                            ConfigurableOffsetBackingStore.OFFSET_STATE_VALUE, startOffset);
                }

            } else {
                // if snapshot phase is finished and offset is specified, we will continue reading
                // changes from the given offset
                if (snapshotDone && null != startOffset && !startOffset.isBlank()) {
                    postgresProps.setProperty("snapshot.mode", "no_data");
                    postgresProps.setProperty(
                            ConfigurableOffsetBackingStore.OFFSET_STATE_VALUE, startOffset);
                }
            }

            // adapt value of sslmode to the expected value
            var sslMode = postgresProps.getProperty("database.sslmode");
            if (sslMode != null) {
                switch (sslMode) {
                    case "disabled":
                        sslMode = "disable";
                        break;
                    case "preferred":
                        sslMode = "prefer";
                        break;
                    case "required":
                        sslMode = "require";
                        break;
                }
                postgresProps.setProperty("database.sslmode", sslMode);
            }

            dbzProps.putAll(postgresProps);

            if (isCdcSourceJob) {
                // remove table filtering for the shared Postgres source, since we
                // allow user to ingest tables in different schemas
                LOG.info("Disable table filtering for the shared Postgres source");
                dbzProps.remove("table.include.list");
            }

            if (userProps.containsKey(PG_SSL_ROOT_CERT)) {
                dbzProps.setProperty("database.sslrootcert", userProps.get(PG_SSL_ROOT_CERT));
            }
        } else if (source == SourceTypeE.CITUS) {
            var postgresProps = initiateDbConfig(POSTGRES_CONFIG_FILE, substitutor);

            // citus needs all_tables publication to capture all shards
            postgresProps.setProperty("publication.autocreate.mode", "all_tables");

            // disable publication auto creation if needed
            var pubAutoCreate =
                    Boolean.parseBoolean(
                            userProps.getOrDefault(DbzConnectorConfig.PG_PUB_CREATE, "true"));

            if (!pubAutoCreate) {
                postgresProps.setProperty("publication.autocreate.mode", "disabled");
            }
            // if snapshot phase is finished and offset is specified, we will continue reading
            // changes from the given offset
            if (snapshotDone && null != startOffset && !startOffset.isBlank()) {
                postgresProps.setProperty("snapshot.mode", "no_data");
                postgresProps.setProperty(
                        ConfigurableOffsetBackingStore.OFFSET_STATE_VALUE, startOffset);
            }
            dbzProps.putAll(postgresProps);
        } else if (source == SourceTypeE.MONGODB) {
            var mongodbProps = initiateDbConfig(MONGODB_CONFIG_FILE, substitutor);

            // if snapshot phase is finished and offset is specified, we will continue reading
            // changes from the given offset
            if (snapshotDone && null != startOffset && !startOffset.isBlank()) {
                mongodbProps.setProperty("snapshot.mode", "no_data");
                mongodbProps.setProperty(
                        ConfigurableOffsetBackingStore.OFFSET_STATE_VALUE, startOffset);
            }

            var mongodbUrl = userProps.get(MongoDb.MONGO_URL);
            var collection = userProps.get(MongoDb.MONGO_COLLECTION_NAME);
            var hasRawFilterOverride =
                    MongoDb.RAW_FILTER_OPTIONS_DISABLING_INFERENCE.stream()
                            .anyMatch(userProps::containsKey);
            if (!hasRawFilterOverride) {
                inferMongoDatabaseList(collection)
                        .ifPresent(
                                databaseList -> {
                                    mongodbProps.setProperty(
                                            MongoDbConnectorConfig.DATABASE_INCLUDE_LIST.name(),
                                            databaseList);
                                    mongodbProps.setProperty(
                                            MongoDbConnectorConfig.FILTERS_MATCH_MODE.name(),
                                            MongoDbConnectorConfig.FiltersMatchMode.LITERAL
                                                    .getValue());
                                    LOG.info(
                                            "Inferred MongoDB database include list '{}' and"
                                                    + " literal filter match mode from collection list"
                                                    + " '{}'",
                                            databaseList,
                                            collection);
                                });
            }

            var connectionStr = new ConnectionString(mongodbUrl);
            var connectorName =
                    String.format(
                            "MongoDB_%d:%s:%s", sourceId, connectionStr.getHosts(), collection);
            mongodbProps.setProperty("name", connectorName);

            dbzProps.putAll(mongodbProps);
        } else if (source == SourceTypeE.SQL_SERVER) {
            var sqlServerProps = initiateDbConfig(SQL_SERVER_CONFIG_FILE, substitutor);
            // disable snapshot locking at all
            sqlServerProps.setProperty("snapshot.locking.mode", "none");

            if (isCdcBackfill) {
                // if startOffset is specified, we should continue
                // reading changes from the given offset
                if (null != startOffset && !startOffset.isBlank()) {
                    // skip the initial snapshot for cdc backfill
                    sqlServerProps.setProperty("snapshot.mode", "recovery");
                    sqlServerProps.setProperty(
                            ConfigurableOffsetBackingStore.OFFSET_STATE_VALUE, startOffset);
                } else {
                    sqlServerProps.setProperty("snapshot.mode", "no_data");
                }
            } else {
                // if snapshot phase is finished and offset is specified, we will continue reading
                // changes from the given offset
                if (snapshotDone && null != startOffset && !startOffset.isBlank()) {
                    sqlServerProps.setProperty("snapshot.mode", "recovery");
                    sqlServerProps.setProperty(
                            ConfigurableOffsetBackingStore.OFFSET_STATE_VALUE, startOffset);
                }
            }
            dbzProps.putAll(sqlServerProps);
            if (isCdcSourceJob) {
                // remove table filtering for the shared Sql Server source, since we
                // allow user to ingest tables in different schemas
                LOG.info("Disable table filtering for the shared Sql Server source");
                dbzProps.remove("table.include.list");
            }
        } else if (source == SourceTypeE.ORACLE) {
            var oracleProps = initiateDbConfig(ORACLE_CONFIG_FILE, substitutor);
            if (isHeartbeatEnabled(userProps)) {
                var heartbeatTable =
                        OracleHeartbeatTable.parse(userProps.get(ORACLE_HEARTBEAT_TABLE_NAME));
                oracleProps.setProperty(
                        DatabaseHeartbeatImpl.HEARTBEAT_ACTION_QUERY_PROPERTY_NAME,
                        heartbeatTable.actionQuery());
            }
            dbzProps.putAll(oracleProps);
            if (isCdcSourceJob) {
                // A shared Oracle source captures tables from multiple schemas in one PDB.
                LOG.info("Disable table filtering for the shared Oracle source");
                dbzProps.remove("table.include.list");
            }
        } else {
            throw new RuntimeException("unsupported source type: " + source);
        }
        var otherProps = extractDebeziumProperties(userProps);
        for (var entry : otherProps.entrySet()) {
            dbzProps.putIfAbsent(entry.getKey(), entry.getValue());
        }

        // Calculate max.queue.size.in.bytes based on JVM heap size and ratio
        calculateAndSetMaxQueueSizeInBytes(dbzProps);

        LOG.info("Final Debezium properties: {}", dbzProps);
        LOG.info(
                "Debezium max.queue.size.in.bytes: {} bytes ({} MB)",
                dbzProps.getProperty("max.queue.size.in.bytes", "not set"),
                dbzProps.getProperty("max.queue.size.in.bytes") != null
                        ? Long.parseLong(dbzProps.getProperty("max.queue.size.in.bytes"))
                                / 1024
                                / 1024
                        : 0);

        this.sourceId = sourceId;
        this.sourceType = source;
        this.resolvedDbzProps = dbzProps;
        this.isBackfillSource = isCdcBackfill;
        this.waitStreamingStartTimeout = waitStreamingStartTimeout;
    }

    public static boolean isHeartbeatEnabled(Map<String, String> userProps) {
        return userProps.containsKey(HEARTBEAT_INTERVAL_KEY);
    }

    private static Optional<String> inferMongoDatabaseList(String collectionList) {
        if (collectionList == null) {
            return Optional.empty();
        }

        var matcher = MongoDb.LITERAL_COLLECTION_LIST_PATTERN.matcher(collectionList);
        var databases = new LinkedHashSet<String>();
        var matchedUntil = 0;
        while (matcher.find()) {
            if (matcher.start() != matchedUntil) {
                return Optional.empty();
            }
            databases.add(matcher.group(MongoDb.DATABASE_GROUP));
            matchedUntil = matcher.end();
        }

        if (databases.isEmpty() || matchedUntil != collectionList.length()) {
            return Optional.empty();
        }
        return Optional.of(String.join(",", databases));
    }

    private Properties initiateDbConfig(String fileName, StringSubstitutor substitutor) {
        var dbProps = new Properties();
        try (var input = getClass().getClassLoader().getResourceAsStream(fileName)) {
            assert input != null;
            var inputStr = IOUtils.toString(input, StandardCharsets.UTF_8);
            // load before substitution, so that we do not need to escape the substituted text
            dbProps.load(new StringReader(inputStr));
            dbProps.replaceAll((k, v) -> substitutor.replace(v));
        } catch (IOException e) {
            throw new RuntimeException("failed to load config file " + fileName, e);
        }
        return dbProps;
    }

    /**
     * Calculate and set max.queue.size.in.bytes based on JVM heap size and memory ratio.
     *
     * <p>If user does not specify queue.memory.ratio, this method does nothing. Debezium will use
     * whatever is configured in debezium.properties (max.queue.size = 8192 by default).
     *
     * <p>If user specifies a valid ratio (0.01-0.8), calculate and set max.queue.size.in.bytes
     * accordingly.
     *
     * <p>JVM heap size is obtained from Runtime.getRuntime().maxMemory(), which reflects the -Xmx
     * value set by Rust code during JVM initialization. See: src/jni_core/src/jvm_runtime.rs:85-103
     *
     * <p>The Rust code sets -Xmx based on: 1. JVM_HEAP_SIZE env var if set 2. Otherwise:
     * system_memory_available_bytes() * 0.07
     *
     * @param dbzProps The Debezium properties to update
     */
    private void calculateAndSetMaxQueueSizeInBytes(Properties dbzProps) {
        // Check if user specified a custom ratio
        String ratioStr = dbzProps.getProperty(QUEUE_MAX_MEMORY_RATIO);
        if (ratioStr == null) {
            // User did not specify ratio, do nothing
            // Debezium will use its default max.queue.size from debezium.properties
            LOG.info(
                    "Debezium {} not specified, skipping calculating and setting"
                            + " max.queue.size.in.bytes calculation",
                    QUEUE_MAX_MEMORY_RATIO);
            return;
        }

        // Validation is already done in SourceValidateHandler during CREATE/ALTER SOURCE
        // At this point, ratioStr is guaranteed to be valid (0.01-0.8)
        double queueMemoryRatio = Double.parseDouble(ratioStr);

        // Get JVM max heap size from Runtime
        // This reflects the -Xmx value set by Rust during JVM initialization
        // No need to read JVM_HEAP_SIZE env var again - it's already baked into the JVM
        Runtime runtime = Runtime.getRuntime();
        long jvmHeapSize = runtime.maxMemory();

        LOG.info(
                "JVM max heap size (from Runtime): {} bytes ({} MB)",
                jvmHeapSize,
                jvmHeapSize / 1024 / 1024);
        LOG.info("Debezium {}: {}", QUEUE_MAX_MEMORY_RATIO, queueMemoryRatio);

        // Calculate max queue size in bytes
        long maxQueueSizeInBytes = (long) (jvmHeapSize * queueMemoryRatio);

        // Set the calculated value
        dbzProps.setProperty("max.queue.size.in.bytes", String.valueOf(maxQueueSizeInBytes));

        LOG.info(
                "Calculated debezium max.queue.size.in.bytes: {} bytes ({} MB)",
                maxQueueSizeInBytes,
                maxQueueSizeInBytes / 1024 / 1024);
    }
}
