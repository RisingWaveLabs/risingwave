/*
 * Copyright 2024 RisingWave Labs
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

package com.risingwave.connector.source.core;

import com.risingwave.connector.api.source.SourceTypeE;
import com.risingwave.connector.cdc.debezium.internal.DebeziumOffset;
import com.risingwave.connector.cdc.debezium.internal.DebeziumOffsetSerializer;
import com.risingwave.connector.source.common.CdcConnectorException;
import com.risingwave.proto.ConnectorServiceProto.CdcMessage;
import com.risingwave.proto.ConnectorServiceProto.GetEventStreamResponse;
import com.risingwave.proto.ConnectorServiceProto.SourceType;
import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.embedded.EmbeddedEngineChangeEventProxy;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

enum EventType {
    HEARTBEAT,
    TRANSACTION,
    DATA,
    SCHEMA_CHANGE,
}

public class DbzChangeEventConsumer
        implements DebeziumEngine.ChangeConsumer<ChangeEvent<SourceRecord, SourceRecord>> {
    static final Logger LOG = LoggerFactory.getLogger(DbzChangeEventConsumer.class);

    private final BlockingQueue<GetEventStreamResponse> outputChannel;

    private final SourceTypeE connector;
    private final long sourceId;
    private final JsonConverter payloadConverter;
    private final JsonConverter keyConverter;
    private final String heartbeatTopicPrefix;
    private final String transactionTopic;
    private final String schemaChangeTopic;

    private volatile DebeziumEngine.RecordCommitter<ChangeEvent<SourceRecord, SourceRecord>>
            currentRecordCommitter;

    DbzChangeEventConsumer(
            SourceTypeE connector,
            long sourceId,
            String heartbeatTopicPrefix,
            String transactionTopic,
            String schemaChangeTopic,
            BlockingQueue<GetEventStreamResponse> queue) {
        this.connector = connector;
        this.sourceId = sourceId;
        this.outputChannel = queue;
        this.heartbeatTopicPrefix = heartbeatTopicPrefix;
        this.transactionTopic = transactionTopic;
        this.schemaChangeTopic = schemaChangeTopic;
        LOG.info("heartbeat topic: {}, trnx topic: {}", heartbeatTopicPrefix, transactionTopic);

        // The default JSON converter will output the schema field in the JSON which is unnecessary
        // to source parser, we use a customized JSON converter to avoid outputting the `schema`
        // field.
        var payloadConverter = new DbzJsonConverter();
        final HashMap<String, Object> configs = new HashMap<>(2);
        // only serialize the value part
        configs.put(ConverterConfig.TYPE_CONFIG, ConverterType.VALUE.getName());
        // include record schema to output JSON in { "schema": { ... }, "payload": { ... } } format
        configs.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, true);
        payloadConverter.configure(configs);
        this.payloadConverter = payloadConverter;

        var keyConverter = new DbzJsonConverter();
        configs.put(ConverterConfig.TYPE_CONFIG, ConverterType.KEY.getName());
        keyConverter.configure(configs);
        this.keyConverter = keyConverter;
    }

    /**
     * Whether records can be acknowledged before the corresponding RisingWave checkpoint.
     *
     * <p>PostgreSQL needs checkpoint-delayed acknowledgement because the acknowledged LSN advances
     * the replication slot's low watermark, allowing PostgreSQL to recycle older WAL. Acknowledging
     * it before the output is durable could recycle WAL that RisingWave still needs for recovery.
     *
     * <p>Oracle needs checkpoint-delayed acknowledgement because Debezium's opaque offset contains
     * the LogMiner restart low watermark required to rebuild buffered transactions. Acknowledging
     * it before the output is durable could make recovery skip changes that RisingWave has not
     * checkpointed.
     */
    private boolean noNeedCommitOffset() {
        return connector != SourceTypeE.POSTGRES && connector != SourceTypeE.ORACLE;
    }

    private EventType getEventType(SourceRecord record) {
        if (isHeartbeatEvent(record)) {
            return EventType.HEARTBEAT;
        } else if (isTransactionMetaEvent(record)) {
            return EventType.TRANSACTION;
        } else if (isSchemaChangeEvent(record)) {
            return EventType.SCHEMA_CHANGE;
        } else {
            return EventType.DATA;
        }
    }

    private static SourceType toProtoSourceType(SourceTypeE connector) {
        if (connector == null) {
            return SourceType.UNSPECIFIED;
        }
        switch (connector) {
            case MYSQL:
                return SourceType.MYSQL;
            case POSTGRES:
                return SourceType.POSTGRES;
            case CITUS:
                return SourceType.CITUS;
            case MONGODB:
                return SourceType.MONGODB;
            case SQL_SERVER:
                return SourceType.SQL_SERVER;
            case ORACLE:
                return SourceType.ORACLE;
            default:
                return SourceType.UNSPECIFIED;
        }
    }

    private boolean isHeartbeatEvent(SourceRecord record) {
        String topic = record.topic();
        return topic != null
                && heartbeatTopicPrefix != null
                && topic.startsWith(heartbeatTopicPrefix);
    }

    private boolean isTransactionMetaEvent(SourceRecord record) {
        String topic = record.topic();
        return topic != null && topic.equals(transactionTopic);
    }

    private boolean isSchemaChangeEvent(SourceRecord record) {
        String topic = record.topic();
        return topic != null && topic.equals(schemaChangeTopic);
    }

    @Override
    public void handleBatch(
            List<ChangeEvent<SourceRecord, SourceRecord>> events,
            DebeziumEngine.RecordCommitter<ChangeEvent<SourceRecord, SourceRecord>> committer)
            throws InterruptedException {
        var respBuilder = GetEventStreamResponse.newBuilder();
        currentRecordCommitter = committer;
        for (ChangeEvent<SourceRecord, SourceRecord> event : events) {
            var record = event.value();
            EventType eventType = getEventType(record);
            DebeziumOffset offset =
                    new DebeziumOffset(
                            record.sourcePartition(),
                            record.sourceOffset(),
                            (eventType == EventType.HEARTBEAT));
            // serialize the offset to a JSON, so that kernel doesn't need to
            // aware its layout
            String offsetStr = "";
            try {
                byte[] serialized = DebeziumOffsetSerializer.INSTANCE.serialize(offset);
                offsetStr = new String(serialized, StandardCharsets.UTF_8);
            } catch (IOException e) {
                LOG.warn("failed to serialize debezium offset", e);
            }
            var msgBuilder =
                    CdcMessage.newBuilder()
                            .setOffset(offsetStr)
                            .setPartition(String.valueOf(sourceId))
                            .setSourceType(toProtoSourceType(connector));

            switch (eventType) {
                case HEARTBEAT:
                    {
                        var message =
                                msgBuilder.setMsgType(CdcMessage.CdcMessageType.HEARTBEAT).build();
                        LOG.debug("heartbeat => {}", message.getOffset());
                        respBuilder.addEvents(message);
                        break;
                    }
                case TRANSACTION:
                    {
                        long trxTs = ((Struct) record.value()).getInt64("ts_ms");
                        byte[] payload =
                                payloadConverter.fromConnectData(
                                        record.topic(), record.valueSchema(), record.value());
                        var message =
                                msgBuilder
                                        .setMsgType(CdcMessage.CdcMessageType.TRANSACTION_META)
                                        .setPayload(new String(payload, StandardCharsets.UTF_8))
                                        .setSourceTsMs(trxTs)
                                        .build();
                        LOG.debug("transaction => {}", message);
                        respBuilder.addEvents(message);
                        break;
                    }

                case SCHEMA_CHANGE:
                    {
                        var sourceStruct = ((Struct) record.value()).getStruct("source");
                        if (sourceStruct == null) {
                            throw new CdcConnectorException(
                                    "source field is missing in schema change event");
                        }

                        // upstream event time
                        long sourceTsMs = sourceStruct.getInt64("ts_ms");
                        byte[] payload =
                                payloadConverter.fromConnectData(
                                        record.topic(), record.valueSchema(), record.value());

                        // We intentionally don't set the fullTableName for schema change event,
                        // since it doesn't need to be routed to a specific cdc table
                        var message =
                                msgBuilder
                                        .setMsgType(CdcMessage.CdcMessageType.SCHEMA_CHANGE)
                                        .setPayload(new String(payload, StandardCharsets.UTF_8))
                                        .setSourceTsMs(sourceTsMs)
                                        .build();
                        LOG.debug(
                                "[schema] offset => {}, key => {}, payload => {}",
                                message.getOffset(),
                                message.getKey(),
                                message.getPayload());
                        respBuilder.addEvents(message);

                        // emit the schema change event as a single response
                        respBuilder.setSourceId(sourceId);
                        var response = respBuilder.build();
                        outputChannel.put(response);

                        // reset the response builder
                        respBuilder = GetEventStreamResponse.newBuilder();
                        break;
                    }

                case DATA:
                    {
                        // Topic naming conventions
                        // - PG: topicPrefix.schemaName.tableName
                        // - MySQL: topicPrefix.databaseName.tableName
                        // - Mongo: topicPrefix.databaseName.collectionName
                        // - SQL Server: topicPrefix.databaseName.schemaName.tableName
                        // - Oracle: topicPrefix.schemaName.tableName
                        // We can extract the full table name from the topic
                        var fullTableName =
                                record.topic().substring(record.topic().indexOf('.') + 1);

                        // ignore null record
                        if (record.value() == null) {
                            break;
                        }
                        // get upstream event time from the "source" field
                        var sourceStruct = ((Struct) record.value()).getStruct("source");
                        if (sourceStruct == null) {
                            throw new CdcConnectorException(
                                    "source field is missing in data change event");
                        }
                        long sourceTsMs = sourceStruct.getInt64("ts_ms");
                        byte[] payload =
                                payloadConverter.fromConnectData(
                                        record.topic(), record.valueSchema(), record.value());
                        byte[] key =
                                keyConverter.fromConnectData(
                                        record.topic(), record.keySchema(), record.key());
                        String msgPayload =
                                payload == null ? "" : new String(payload, StandardCharsets.UTF_8);
                        // key can be null if the table has no primary key
                        String msgKey = key == null ? "" : new String(key, StandardCharsets.UTF_8);
                        var message =
                                msgBuilder
                                        .setMsgType(CdcMessage.CdcMessageType.DATA)
                                        .setFullTableName(fullTableName)
                                        .setPayload(msgPayload)
                                        .setKey(msgKey)
                                        .setSourceTsMs(sourceTsMs)
                                        .build();
                        LOG.debug(
                                "[data] offset => {}, key => {}, payload => {}",
                                message.getOffset(),
                                message.getKey(),
                                message.getPayload());
                        respBuilder.addEvents(message);
                        break;
                    }
                default:
                    break;
            }
            if (noNeedCommitOffset()) {
                committer.markProcessed(event);
            }
        }

        LOG.debug("recv {} events", respBuilder.getEventsCount());
        // skip empty batch
        if (respBuilder.getEventsCount() > 0) {
            respBuilder.setSourceId(sourceId);
            var response = respBuilder.build();
            outputChannel.put(response);
        }
        if (noNeedCommitOffset()) {
            committer.markBatchFinished();
        }
    }

    public BlockingQueue<GetEventStreamResponse> getOutputChannel() {
        return this.outputChannel;
    }

    /**
     * Commit the offset to the Debezium engine. NOTES: The input offset is passed from the source
     * executor to here
     *
     * @param offset persisted offset in the Source state table
     */
    @SuppressWarnings("unchecked")
    public void commitOffset(DebeziumOffset offset) throws InterruptedException {
        // Although the committer is read/write by multi-thread, the committer will be not changed
        // frequently.
        if (currentRecordCommitter == null) {
            LOG.info(
                    "commitOffset() called on Debezium change consumer which doesn't receive records yet.");
            return;
        }

        // only the offset is used
        SourceRecord recordWrapper =
                new SourceRecord(
                        offset.sourcePartition,
                        adjustSourceOffset((Map<String, Object>) offset.sourceOffset),
                        "DUMMY",
                        Schema.BOOLEAN_SCHEMA,
                        true);
        ChangeEvent<SourceRecord, SourceRecord> changeEvent =
                EmbeddedEngineChangeEventProxy.create(null, recordWrapper, recordWrapper);
        currentRecordCommitter.markProcessed(changeEvent);
        currentRecordCommitter.markBatchFinished();
    }

    /**
     * We have to adjust type of LSN values to Long, because it might be Integer after
     * deserialization, however {@link
     * io.debezium.connector.postgresql.PostgresStreamingChangeEventSource#commitOffset(Map, Map)}
     * requires Long.
     */
    private Map<String, Object> adjustSourceOffset(Map<String, Object> sourceOffset) {
        if (sourceOffset.containsKey(PostgresOffsetContext.LAST_COMPLETELY_PROCESSED_LSN_KEY)) {
            String value =
                    sourceOffset
                            .get(PostgresOffsetContext.LAST_COMPLETELY_PROCESSED_LSN_KEY)
                            .toString();
            sourceOffset.put(
                    PostgresOffsetContext.LAST_COMPLETELY_PROCESSED_LSN_KEY, Long.parseLong(value));
        }
        if (sourceOffset.containsKey(PostgresOffsetContext.LAST_COMMIT_LSN_KEY)) {
            String value = sourceOffset.get(PostgresOffsetContext.LAST_COMMIT_LSN_KEY).toString();
            sourceOffset.put(PostgresOffsetContext.LAST_COMMIT_LSN_KEY, Long.parseLong(value));
        }
        return sourceOffset;
    }
}
