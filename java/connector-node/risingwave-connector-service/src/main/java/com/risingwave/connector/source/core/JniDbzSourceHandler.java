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

package com.risingwave.connector.source.core;

import static com.risingwave.proto.ConnectorServiceProto.SourceType.POSTGRES;

import com.risingwave.connector.api.source.SourceTypeE;
import com.risingwave.connector.cdc.debezium.internal.DebeziumOffset;
import com.risingwave.connector.cdc.debezium.internal.DebeziumOffsetSerializer;
import com.risingwave.connector.source.common.CdcConnectorException;
import com.risingwave.connector.source.common.DbzConnectorConfig;
import com.risingwave.connector.source.common.DbzSourceUtils;
import com.risingwave.java.binding.CdcSourceChannel;
import com.risingwave.metrics.ConnectorNodeMetrics;
import com.risingwave.proto.ConnectorServiceProto;
import com.risingwave.proto.ConnectorServiceProto.GetEventStreamResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** handler for starting a debezium source connectors for jni */
public class JniDbzSourceHandler {
    static final Logger LOG = LoggerFactory.getLogger(JniDbzSourceHandler.class);

    private final DbzConnectorConfig config;
    private final DbzCdcEngineRunner runner;
    private final CdcSourceChannel channel;

    public JniDbzSourceHandler(DbzConnectorConfig config, CdcSourceChannel channel) {
        this.config = config;
        this.runner = DbzCdcEngineRunner.create(config, channel);
        this.channel = channel;

        if (runner == null) {
            throw new CdcConnectorException("Failed to create engine runner");
        }
    }

    public long getSourceId() {
        return config.getSourceId();
    }

    public static void runJniDbzSourceThread(byte[] getEventStreamRequestBytes, long channelPtr)
            throws Exception {

        var channel = CdcSourceChannel.fromOwnedPointer(channelPtr);
        try {
            var request =
                    ConnectorServiceProto.GetEventStreamRequest.parseFrom(
                            getEventStreamRequestBytes);
            boolean isCdcSourceJob = request.getIsSourceJob();

            if (request.getSourceType() == POSTGRES) {
                DbzSourceUtils.createPostgresPublicationInSourceExecutor(
                        request.getPropertiesMap(), request.getSourceId());
            }

            var config =
                    new DbzConnectorConfig(
                            SourceTypeE.valueOf(request.getSourceType()),
                            request.getSourceId(),
                            request.getStartOffset(),
                            request.getPropertiesMap(),
                            request.getSnapshotDone(),
                            isCdcSourceJob);
            JniDbzSourceHandler handler = new JniDbzSourceHandler(config, channel);
            handler.start();
        } catch (Throwable t) {
            LOG.error("runJniDbzSourceThread throws an exception.", t);

            // The receiver in rust `CdcSplitReader` is relying on the Err to be sent for
            // correct error propagation.
            channel.sendError(t.getMessage());

            // re-throw the exception to provide the correct error message and stop the JNI
            // thread
            throw t;
        }
    }

    public void commitOffset(String encodedOffset) throws InterruptedException {
        try {
            if (encodedOffset.isEmpty()) {
                LOG.debug("Engine#{}: commit offset is empty", config.getSourceId());
                return;
            }
            DebeziumOffset offset =
                    DebeziumOffsetSerializer.INSTANCE.deserialize(
                            encodedOffset.getBytes(StandardCharsets.UTF_8));
            var changeEventConsumer = runner.getChangeEventConsumer();
            if (changeEventConsumer != null) {
                changeEventConsumer.commitOffset(offset);
                LOG.info("Engine#{}: committed offset {}", config.getSourceId(), offset);
            } else {
                LOG.warn("Engine#{}: changeEventConsumer is null", config.getSourceId());
            }
        } catch (IOException err) {
            LOG.error("Engine#{}: fail to commit offset.", config.getSourceId(), err);
            throw new CdcConnectorException(err.getMessage());
        }
    }

    public void start() {
        try {
            // register handler to the registry
            JniDbzSourceRegistry.register(this);

            // Start the engine
            var startOk = runner.start();
            if (!sendHandshakeMessage(runner, channel, startOk)) {
                LOG.error(
                        "Failed to send handshake message to channel. sourceId={}",
                        config.getSourceId());
                return;
            }

            LOG.info("Start consuming events of table {}", config.getSourceId());
            while (runner.isRunning()) {
                // check whether the send queue has room for new messages
                // Thread will block on the channel to get output from engine
                var resp = runner.getEngine().getOutputChannel().poll(500, TimeUnit.MILLISECONDS);
                boolean success;
                if (resp != null) {
                    ConnectorNodeMetrics.incSourceRowsReceived(
                            config.getSourceType().toString(),
                            String.valueOf(config.getSourceId()),
                            resp.getEventsCount());
                    LOG.debug(
                            "Engine#{}: emit one chunk {} events to network ",
                            config.getSourceId(),
                            resp.getEventsCount());
                    success = channel.send(resp.toByteArray());
                } else {
                    // If resp is null means just check whether channel is closed.
                    success = channel.send(null);
                }
                // When user drops the connector, the channel rx will be dropped and we fail to
                // send the message. We should stop the engine in this case.
                if (!success) {
                    LOG.info(
                            "Engine#{}: JNI receiver closed, stop the engine",
                            config.getSourceId());
                    runner.stop();
                    return;
                }
            }
        } catch (Throwable t) {
            LOG.error("Cdc engine failed.", t);

            // The receiver in rust `CdcSplitReader` is relying on the Err to be sent for
            // correct error propagation.
            channel.sendError(t.getMessage());

            try {
                runner.stop();
            } catch (Exception e) {
                LOG.warn("Failed to stop Engine#{}", config.getSourceId(), e);
            }
        } finally {
            // remove the handler from registry
            JniDbzSourceRegistry.unregister(this);
        }
    }

    private boolean sendHandshakeMessage(
            DbzCdcEngineRunner runner, CdcSourceChannel channel, boolean startOk) throws Exception {
        // send a handshake message to notify the Source executor
        // if the handshake is not ok, the split reader will return error to source
        // actor
        var controlInfo =
                GetEventStreamResponse.ControlInfo.newBuilder().setHandshakeOk(startOk).build();

        var handshakeMsg =
                GetEventStreamResponse.newBuilder()
                        .setSourceId(config.getSourceId())
                        .setControl(controlInfo)
                        .build();
        var success = channel.send(handshakeMsg.toByteArray());
        if (!success) {
            LOG.info(
                    "Engine#{}: JNI sender broken detected, stop the engine", config.getSourceId());
            runner.stop();
        }
        return success;
    }
}
