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

package com.risingwave.connector.source;

import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.source.common.*;
import com.risingwave.proto.ConnectorServiceProto;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SourceValidateHandler {
    static final Logger LOG = LoggerFactory.getLogger(SourceValidateHandler.class);
    private final StreamObserver<ConnectorServiceProto.ValidateSourceResponse> responseObserver;

    public SourceValidateHandler(
            StreamObserver<ConnectorServiceProto.ValidateSourceResponse> responseObserver) {
        this.responseObserver = responseObserver;
    }

    public void handle(ConnectorServiceProto.ValidateSourceRequest request) {
        try {
            validateSource(request);
            // validate pass
            responseObserver.onNext(
                    ConnectorServiceProto.ValidateSourceResponse.newBuilder().build());
            responseObserver.onCompleted();

        } catch (StatusRuntimeException e) {
            LOG.warn("Source validation failed", e);
            responseObserver.onNext(validateResponse(e.getMessage()));
            responseObserver.onCompleted();

        } catch (Exception e) {
            LOG.error("Internal error on source validation", e);
            responseObserver.onNext(validateResponse("Internal error: " + e.getMessage()));
            responseObserver.onCompleted();
        }
    }

    public static ConnectorServiceProto.ValidateSourceResponse validateResponse(String message) {
        return ConnectorServiceProto.ValidateSourceResponse.newBuilder()
                .setError(
                        ConnectorServiceProto.ValidationError.newBuilder()
                                .setErrorMessage(message)
                                .build())
                .build();
    }

    private static void ensurePropNotBlank(Map<String, String> props, String name) {
        ensurePropsExists(props, name);
        if (StringUtils.isBlank(props.get(name))) {
            throw ValidatorUtils.invalidArgument(
                    String.format("'%s' cannot be empty. Please check the WITH properties", name));
        }
    }

    private static void ensurePropsExists(Map<String, String> props, String name) {
        if (!props.containsKey(name)) {
            throw ValidatorUtils.invalidArgument(
                    String.format("'%s' is not found. Please check the WITH properties", name));
        }
    }

    static void ensureRequiredProps(Map<String, String> props, boolean isCdcSourceJob) {
        ensurePropNotBlank(props, DbzConnectorConfig.HOST);
        ensurePropNotBlank(props, DbzConnectorConfig.PORT);
        ensurePropNotBlank(props, DbzConnectorConfig.DB_NAME);
        ensurePropNotBlank(props, DbzConnectorConfig.USER);
        ensurePropsExists(props, DbzConnectorConfig.PASSWORD);
        // ensure table name is passed by user in non-sharing mode
        if (!isCdcSourceJob) {
            ensurePropNotBlank(props, DbzConnectorConfig.TABLE_NAME);
        }
    }

    /**
     * Validate queue.memory.ratio if specified. The ratio must be a valid number between
     * MIN_QUEUE_MAX_MEMORY_RATIO and MAX_QUEUE_MAX_MEMORY_RATIO.
     */
    private static void validateQueueMemoryRatio(Map<String, String> props) {
        String ratioStr = props.get("debezium.queue.memory.ratio");
        if (ratioStr == null) {
            return; // Not specified, use default
        }

        double ratio;
        try {
            ratio = Double.parseDouble(ratioStr);
        } catch (NumberFormatException e) {
            throw ValidatorUtils.invalidArgument(
                    String.format(
                            "'debezium.queue.memory.ratio' must be a valid number, got: '%s'",
                            ratioStr));
        }

        // Validate ratio is in reasonable range (1% - 80%)
        if (ratio < DbzConnectorConfig.MIN_QUEUE_MAX_MEMORY_RATIO
                || ratio > DbzConnectorConfig.MAX_QUEUE_MAX_MEMORY_RATIO) {
            throw ValidatorUtils.invalidArgument(
                    String.format(
                            "'debezium.queue.memory.ratio' must be between %s and %s, got: %s",
                            DbzConnectorConfig.MIN_QUEUE_MAX_MEMORY_RATIO,
                            DbzConnectorConfig.MAX_QUEUE_MAX_MEMORY_RATIO,
                            ratio));
        }
    }

    /** Validate debezium.heartbeat.interval.ms if specified. If present, it must be positive. */
    private static void validateHeartbeatInterval(
            Map<String, String> props, boolean isCdcSourceJob) {
        String intervalStr = props.get(DbzConnectorConfig.HEARTBEAT_INTERVAL_KEY);
        if (intervalStr == null) {
            return; // Not specified, use default
        }

        long interval;
        try {
            interval = Long.parseLong(intervalStr);
        } catch (NumberFormatException e) {
            throw ValidatorUtils.invalidArgument(
                    String.format(
                            "'%s' must be a valid number, got: '%s'",
                            DbzConnectorConfig.HEARTBEAT_INTERVAL_KEY, intervalStr));
        }

        if (interval <= 0) {
            throw ValidatorUtils.invalidArgument(
                    String.format(
                            "'%s' must be greater than 0",
                            DbzConnectorConfig.HEARTBEAT_INTERVAL_KEY));
        }
    }

    public static void validateSource(ConnectorServiceProto.ValidateSourceRequest request)
            throws Exception {
        var props = request.getPropertiesMap();

        boolean isCdcSourceJob = request.getIsSourceJob();
        boolean isBackfillTable = request.getIsBackfillTable();
        LOG.info(
                "source_id: {}, is_cdc_source_job: {}, is_backfill_table: {}",
                request.getSourceId(),
                isCdcSourceJob,
                isBackfillTable);

        TableSchema tableSchema = TableSchema.fromProto(request.getTableSchema());
        switch (request.getSourceType()) {
            case POSTGRES:
                ensureRequiredProps(props, isCdcSourceJob);
                ensurePropNotBlank(props, DbzConnectorConfig.PG_SCHEMA_NAME);
                ensurePropNotBlank(props, DbzConnectorConfig.PG_SLOT_NAME);
                ensurePropNotBlank(props, DbzConnectorConfig.PG_PUB_NAME);
                ensurePropNotBlank(props, DbzConnectorConfig.PG_PUB_CREATE);
                validateQueueMemoryRatio(props);
                try (var validator = new PostgresValidator(props, tableSchema, isCdcSourceJob)) {
                    validator.validateAll();
                }
                break;

            case CITUS:
                ensureRequiredProps(props, isCdcSourceJob);
                ensurePropNotBlank(props, DbzConnectorConfig.TABLE_NAME);
                ensurePropNotBlank(props, DbzConnectorConfig.PG_SCHEMA_NAME);
                validateQueueMemoryRatio(props);
                try (var coordinatorValidator = new CitusValidator(props, tableSchema)) {
                    coordinatorValidator.validateDistributedTable();
                    coordinatorValidator.validateTable();
                }

                ensurePropNotBlank(props, DbzConnectorConfig.DB_SERVERS);
                var workerServers =
                        StringUtils.split(props.get(DbzConnectorConfig.DB_SERVERS), ',');
                // props extracted from grpc request, clone it to modify
                Map<String, String> mutableProps = new HashMap<>(props);
                for (String workerAddr : workerServers) {
                    String[] hostPort = StringUtils.split(workerAddr, ':');
                    if (hostPort.length != 2) {
                        throw ValidatorUtils.invalidArgument("invalid database.servers");
                    }
                    // set HOST for each worker server
                    mutableProps.put(DbzConnectorConfig.HOST, hostPort[0]);
                    mutableProps.put(DbzConnectorConfig.PORT, hostPort[1]);
                    try (var workerValidator = new CitusValidator(mutableProps, tableSchema)) {
                        workerValidator.validateDbConfig();
                        workerValidator.validateUserPrivilege();
                    }
                }

                break;
            case MYSQL:
                ensureRequiredProps(props, isCdcSourceJob);
                ensurePropNotBlank(props, DbzConnectorConfig.MYSQL_SERVER_ID);
                validateQueueMemoryRatio(props);
                validateHeartbeatInterval(props, isCdcSourceJob);
                try (var validator =
                        new MySqlValidator(props, tableSchema, isCdcSourceJob, isBackfillTable)) {
                    validator.validateAll();
                }
                break;
            case MONGODB:
                ensurePropNotBlank(props, DbzConnectorConfig.MongoDb.MONGO_URL);
                ensurePropNotBlank(props, DbzConnectorConfig.MongoDb.MONGO_COLLECTION_NAME);
                validateQueueMemoryRatio(props);
                try (var validator = new MongoDbValidator(props, request.getSourceId())) {
                    validator.validateAll();
                }
                break;
            case SQL_SERVER:
                ensureRequiredProps(props, isCdcSourceJob);
                ensurePropNotBlank(props, DbzConnectorConfig.SQL_SERVER_SCHEMA_NAME);
                validateQueueMemoryRatio(props);
                validateHeartbeatInterval(props, isCdcSourceJob);
                try (var sqlServerValidator =
                        new SqlServerValidator(props, tableSchema, isCdcSourceJob)) {
                    sqlServerValidator.validateAll();
                }
                break;
            case ORACLE:
                ensureRequiredProps(props, isCdcSourceJob);
                ensurePropNotBlank(props, DbzConnectorConfig.ORACLE_PDB_NAME);
                ensurePropNotBlank(props, DbzConnectorConfig.ORACLE_SCHEMA_NAME);
                validateHeartbeatInterval(props, isCdcSourceJob);
                if (DbzConnectorConfig.isHeartbeatEnabled(props)) {
                    ensurePropNotBlank(props, DbzConnectorConfig.ORACLE_HEARTBEAT_TABLE_NAME);
                }
                if (props.containsKey(DbzConnectorConfig.HEARTBEAT_ACTION_QUERY_KEY)) {
                    throw ValidatorUtils.invalidArgument(
                            String.format(
                                    "'%s' is generated internally; configure '%s' instead",
                                    DbzConnectorConfig.HEARTBEAT_ACTION_QUERY_KEY,
                                    DbzConnectorConfig.ORACLE_HEARTBEAT_TABLE_NAME));
                }
                validateQueueMemoryRatio(props);
                try (var oracleValidator = new OracleValidator(props, isCdcSourceJob)) {
                    oracleValidator.validateAll();
                }
                break;
            default:
                LOG.warn("Unknown source type");
                throw ValidatorUtils.invalidArgument("Unknown source type");
        }
    }
}
