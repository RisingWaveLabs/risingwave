/*
 * Copyright 2026 RisingWave Labs
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

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mongodb.connection.client;

import com.mongodb.MongoClientSettings;
import io.debezium.config.Configuration;
import io.debezium.connector.mongodb.MongoDbConnectorConfig;
import io.debezium.connector.mongodb.connection.MongoDbAuthProvider;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
import org.bson.UuidRepresentation;

/**
 * RisingWave's patched MongoDB client factory.
 *
 * <p>Debezium supports Java key stores, while RisingWave's MongoDB sink supports the standard
 * {@code tlsCAFile} and {@code tlsCertificateKeyFile} URI options with PEM files. This patch gives
 * MongoDB CDC sources the same URI behavior without changing Debezium's other TLS configuration
 * paths.
 */
public class DefaultMongoDbClientFactory implements MongoDbClientFactory {

    private final MongoDbConnectorConfig connectorConfig;
    private final MongoClientSettings clientSettings;
    private final MongoDbAuthProvider authProvider;
    private final MongoDbTlsUtils.TlsFiles tlsFiles;

    public DefaultMongoDbClientFactory(Configuration config) {
        String rawConnectionString = config.getString(MongoDbConnectorConfig.CONNECTION_STRING);
        this.tlsFiles = MongoDbTlsUtils.tlsFiles(rawConnectionString);
        Configuration effectiveConfig =
                config.edit()
                        .with(
                                MongoDbConnectorConfig.CONNECTION_STRING,
                                MongoDbTlsUtils.withoutTlsFileOptions(rawConnectionString))
                        .build();
        this.connectorConfig = new MongoDbConnectorConfig(effectiveConfig);
        this.authProvider = connectorConfig.getAuthProvider();
        this.authProvider.init(effectiveConfig);
        this.clientSettings = createMongoClientSettings();
    }

    @Override
    public MongoClientSettings getMongoClientSettings() {
        return clientSettings;
    }

    protected MongoClientSettings createMongoClientSettings() {
        String connectionString = connectorConfig.getConnectionString().getConnectionString();
        Optional<SSLContext> tlsSslContext =
                tlsFiles.isEmpty()
                        ? Optional.empty()
                        : Optional.of(
                                MongoDbTlsUtils.createTlsSslContext(
                                        connectionString, tlsFiles, connectorConfig));
        SSLContext sslContext =
                tlsSslContext.orElseGet(
                        () -> MongoDbClientFactory.createSSLContext(connectorConfig));

        // 1. apply property configuration
        var settings =
                MongoClientSettings.builder()
                        .uuidRepresentation(UuidRepresentation.STANDARD)
                        .applyToSocketSettings(
                                builder ->
                                        builder.connectTimeout(
                                                        connectorConfig.getConnectTimeoutMs(),
                                                        TimeUnit.MILLISECONDS)
                                                .readTimeout(
                                                        connectorConfig.getSocketTimeoutMs(),
                                                        TimeUnit.MILLISECONDS))
                        .applyToClusterSettings(
                                builder ->
                                        builder.serverSelectionTimeout(
                                                connectorConfig.getServerSelectionTimeoutMs(),
                                                TimeUnit.MILLISECONDS))
                        .applyToServerSettings(
                                builder ->
                                        builder.heartbeatFrequency(
                                                connectorConfig.getHeartbeatFrequencyMs(),
                                                TimeUnit.MILLISECONDS))
                        .applyToSocketSettings(
                                builder ->
                                        builder.connectTimeout(
                                                        connectorConfig.getConnectTimeoutMs(),
                                                        TimeUnit.MILLISECONDS)
                                                .readTimeout(
                                                        connectorConfig.getSocketTimeoutMs(),
                                                        TimeUnit.MILLISECONDS))
                        .applyToClusterSettings(
                                builder ->
                                        builder.serverSelectionTimeout(
                                                connectorConfig.getServerSelectionTimeoutMs(),
                                                TimeUnit.MILLISECONDS))
                        .applyToSslSettings(
                                builder ->
                                        builder.enabled(
                                                        !tlsFiles.isEmpty()
                                                                || connectorConfig.isSslEnabled())
                                                .invalidHostNameAllowed(
                                                        connectorConfig
                                                                .isSslAllowInvalidHostnames())
                                                .context(sslContext));

        // 2. apply auth provider configuration
        authProvider.addAuthConfig(settings);

        // 3. apply connection string configuration
        settings.applyConnectionString(connectorConfig.getConnectionString());

        // The Java driver does not apply the PEM file options from a connection string. Apply the
        // custom context last so the URI's other TLS options cannot replace it. Either PEM option
        // also implies TLS, matching the Rust MongoDB driver's behavior used by the sink.
        tlsSslContext.ifPresent(
                context ->
                        settings.applyToSslSettings(
                                builder -> builder.enabled(true).context(context)));

        return settings.build();
    }
}
