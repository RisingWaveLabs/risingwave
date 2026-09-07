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

package com.risingwave.connector.source.common;

import com.mongodb.ConnectionString;
import com.risingwave.connector.api.source.SourceTypeE;
import io.debezium.config.Configuration;
import io.debezium.connector.mongodb.MongoDbConnectorConfig;
import io.debezium.connector.mongodb.connection.MongoDbConnection;
import io.debezium.connector.mongodb.connection.MongoDbConnections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoDbValidator extends DatabaseValidator implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(MongoDbValidator.class);
    private static final int VALIDATION_TIMEOUT_SECONDS = 5;
    private static final String VALIDATION_TIMEOUT_MILLIS = "5000";

    String mongodbUrl;
    boolean isShardedCluster;

    ConnectionString connStr;
    MongoDbConnection connection;

    @Override
    public void close() {
        if (connection != null) {
            connection.close();
        }
    }

    static final String USERS = "users";
    static final String ROLES = "roles";
    static final String INHERITED_ROLES = "inheritedRoles";
    static final String INHERITED_PRIVILEGES = "inheritedPrivileges";

    public MongoDbValidator(Map<String, String> userProps, long sourceId) {
        this.mongodbUrl = userProps.get("mongodb.url");
        this.connStr = new ConnectionString(mongodbUrl);
        this.isShardedCluster = false;

        var connectorConfig =
                new DbzConnectorConfig(
                        SourceTypeE.MONGODB, sourceId, null, userProps, false, false);
        var validationProperties = new Properties();
        validationProperties.putAll(connectorConfig.getResolvedDebeziumProps());
        validationProperties.setProperty(
                MongoDbConnectorConfig.CONNECT_TIMEOUT_MS.name(), VALIDATION_TIMEOUT_MILLIS);
        validationProperties.setProperty(
                MongoDbConnectorConfig.SERVER_SELECTION_TIMEOUT_MS.name(),
                VALIDATION_TIMEOUT_MILLIS);
        validationProperties.setProperty(
                MongoDbConnectorConfig.SOCKET_TIMEOUT_MS.name(), VALIDATION_TIMEOUT_MILLIS);
        validationProperties.setProperty(
                MongoDbConnectorConfig.HEARTBEAT_FREQUENCY_MS.name(), VALIDATION_TIMEOUT_MILLIS);
        this.connection = MongoDbConnections.create(Configuration.from(validationProperties));
    }

    @Override
    public void validateDbConfig() {
        try {
            connection.execute(
                    "ping MongoDB",
                    client -> {
                        client.getDatabase("admin")
                                .runCommand(org.bson.BsonDocument.parse("{ping: 1}"));
                    });
            LOG.info("MongoDB connection validated successfully");

            validateMatchingCollections();
        } catch (InterruptedException e) {
            throw interrupted("validating the MongoDB connection", e);
        } catch (CdcConnectorException e) {
            // Re-throw our custom exceptions
            LOG.error("MongoDB validation failed: {}", e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            // Wrap other exceptions with clear error message
            LOG.error(
                    "Failed to connect to MongoDB at {} within {} seconds",
                    mongodbUrl,
                    VALIDATION_TIMEOUT_SECONDS,
                    e);
            throw new CdcConnectorException(
                    String.format(
                            "Failed to connect to MongoDB at %s within %d seconds: %s",
                            mongodbUrl, VALIDATION_TIMEOUT_SECONDS, causeMessage(e)),
                    e);
        }
    }

    private void validateMatchingCollections() throws InterruptedException {
        try {
            var matchingCollections = connection.collections();
            LOG.info(
                    "MongoDB collection discovery validated successfully; {} collection(s) match",
                    matchingCollections.size());
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            throw new CdcConnectorException(
                    String.format(
                            "Failed to discover MongoDB collections with the configured user: %s",
                            rootCauseMessage(e)),
                    e);
        }
    }

    private static String rootCauseMessage(Throwable exception) {
        var cause = exception;
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        return cause.getMessage();
    }

    static String causeMessage(Throwable exception) {
        return exception.getCause() == null
                ? exception.getMessage()
                : exception.getCause().getMessage();
    }

    private static CdcConnectorException interrupted(
            String operation, InterruptedException exception) {
        Thread.currentThread().interrupt();
        return new CdcConnectorException("Interrupted while " + operation, exception);
    }

    boolean checkReadRoleForAdminDb(List<Document> roles) {
        for (Document roleDoc : roles) {
            var db = roleDoc.getString("db");
            var role = roleDoc.getString("role");
            if (db.equals("admin")
                    && (role.equals("readWrite")
                            || role.equals("read")
                            || role.equals("readWriteAnyDatabase")
                            || role.equals("readAnyDatabase"))) {
                LOG.info("user has the appropriate roles to read the admin database");
                return true;
            }
        }
        return false;
    }

    @Override
    void validateUserPrivilege() {
        // https://debezium.io/documentation/reference/stable/connectors/mongodb.html#setting-up-mongodb
        // You must also have a MongoDB user that has the appropriate roles to read the admin
        // database where the oplog can be read. Additionally, the user must also be able to read
        // the config database in the configuration server of a sharded cluster and must have
        // listDatabases privilege action. When change streams are used (the default) the user also
        // must have cluster-wide privilege actions find and changeStream.

        if (null != connStr.getCredential()) {
            var secret = connStr.getCredential();

            Bson command =
                    BsonDocument.parse(
                            String.format(
                                    "{usersInfo: \"%s\", showPrivileges: true}",
                                    secret.getUserName()));

            Document ret;
            try {
                ret =
                        connection.execute(
                                "read MongoDB user privileges",
                                client -> {
                                    return client.getDatabase(secret.getSource())
                                            .runCommand(command);
                                });
            } catch (InterruptedException e) {
                throw interrupted("validating MongoDB user privileges", e);
            }
            LOG.info("mongodb userInfo: {}", ret.toJson());

            List<Document> users = ret.getEmbedded(List.of(USERS), List.class);
            LOG.info("mongodb users => {}", users);
            if (users.isEmpty()) {
                throw new CdcConnectorException("user not found in the database");
            }

            // https://debezium.io/documentation/reference/stable/connectors/mongodb.html#setting-up-mongodb
            // You must also have a MongoDB user that has the appropriate roles to read the admin
            // database where the oplog can be read.   boolean hasReadForAdmin = false;
            Document user = users.get(0);
            List<Document> roles = user.getEmbedded(List.of(ROLES), List.class);
            boolean hasReadForAdmin = false;
            if (!roles.isEmpty()) {
                // check direct roles
                hasReadForAdmin = checkReadRoleForAdminDb(roles);
                if (!hasReadForAdmin) {
                    // check inherited roles
                    List<Document> inheriRoles =
                            user.getEmbedded(List.of(INHERITED_ROLES), List.class);
                    if (!inheriRoles.isEmpty()) {
                        hasReadForAdmin = checkReadRoleForAdminDb(inheriRoles);
                    }
                }
            }
            if (!hasReadForAdmin) {
                throw new CdcConnectorException(
                        "user does not have the appropriate roles to read the admin database");
            }

            // When change streams are used (the default) the user also
            // must have cluster-wide privilege actions find and changeStream.
            // TODO: may check the privilege actions find and changeStream
        }

        // TODO: may check privilege for sharded cluster
    }

    @Override
    void validateTable() {
        // do nothing since MongoDB is schemaless
    }

    @Override
    boolean isCdcSourceJob() {
        return false;
    }
}
