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

package com.risingwave.connector.source.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.risingwave.connector.api.source.SourceTypeE;
import com.risingwave.connector.source.SourceValidateHandler;
import com.risingwave.proto.ConnectorServiceProto;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.junit.Test;

public class OracleConnectorConfigTest {
    @Test
    public void resolvesOracleCdbPdbAndLogMinerProperties() {
        var userProps = oracleProperties();
        userProps.put("debezium.database.connection.adapter", "xstream");

        var config = new DbzConnectorConfig(SourceTypeE.ORACLE, 42, null, userProps, false, false);
        var properties = config.getResolvedDebeziumProps();

        assertEquals(
                "io.debezium.connector.oracle.OracleConnector",
                properties.getProperty("connector.class"));
        assertEquals("db.example.com", properties.getProperty("database.hostname"));
        assertEquals("1521", properties.getProperty("database.port"));
        assertEquals("FREE", properties.getProperty("database.dbname"));
        assertEquals("FREEPDB1", properties.getProperty("database.pdb.name"));
        assertEquals("APP.CUSTOMERS", properties.getProperty("table.include.list"));
        assertEquals("logminer", properties.getProperty("database.connection.adapter"));
        assertEquals("online_catalog", properties.getProperty("log.mining.strategy"));
        assertEquals("initial", properties.getProperty("snapshot.mode"));
        assertEquals("RW_CDC_42", properties.getProperty("topic.prefix"));
        assertEquals("300000", properties.getProperty("heartbeat.interval.ms"));
        assertEquals(
                "UPDATE APP.RW_HEARTBEAT SET HEARTBEAT = CASE HEARTBEAT WHEN 0 THEN 1 ELSE 0 END "
                        + "WHERE ID = 1",
                properties.getProperty("heartbeat.action.query"));
    }

    @Test
    public void disablesOracleHeartbeatWhenIntervalIsAbsent() {
        var userProps = oracleProperties();
        userProps.remove(DbzConnectorConfig.HEARTBEAT_INTERVAL_KEY);
        userProps.remove(DbzConnectorConfig.ORACLE_HEARTBEAT_TABLE_NAME);

        var config = new DbzConnectorConfig(SourceTypeE.ORACLE, 42, null, userProps, false, false);

        assertFalse(config.getResolvedDebeziumProps().containsKey("heartbeat.interval.ms"));
        assertFalse(config.getResolvedDebeziumProps().containsKey("heartbeat.action.query"));
    }

    @Test
    public void removesTableFilterForSharedOracleSource() {
        var config =
                new DbzConnectorConfig(
                        SourceTypeE.ORACLE, 42, null, oracleProperties(), false, true);

        assertFalse(config.getResolvedDebeziumProps().containsKey("table.include.list"));
    }

    @Test
    public void buildsOracleThinJdbcUrl() {
        assertEquals(
                "jdbc:oracle:thin:@//db.example.com:1521/FREE",
                ValidatorUtils.getJdbcUrl(SourceTypeE.ORACLE, "db.example.com", "1521", "FREE"));
    }

    @Test
    public void validatesOracleIdentifierAndRequiredGrants() {
        assertEquals("FREEPDB1", OracleValidator.normalizePdbName("freepdb1"));
        assertThrows(
                RuntimeException.class,
                () -> OracleValidator.normalizePdbName("FREEPDB1; DROP TABLE APP.CUSTOMERS"));

        OracleValidator.validateRequiredGrants(
                "privileges", Set.of("CREATE SESSION"), Set.of("CREATE SESSION"));
        assertThrows(
                RuntimeException.class,
                () ->
                        OracleValidator.validateRequiredGrants(
                                "privileges", Set.of("CREATE SESSION"), Set.of()));
    }

    @Test
    public void validatesOracleHeartbeatTableAndBuildsActionQuery() {
        var heartbeatTable = OracleHeartbeatTable.parse("app.rw_heartbeat");
        assertEquals("APP", heartbeatTable.owner());
        assertEquals("RW_HEARTBEAT", heartbeatTable.table());
        assertEquals("APP.RW_HEARTBEAT", heartbeatTable.qualifiedName());
        assertEquals(
                "UPDATE APP.RW_HEARTBEAT SET HEARTBEAT = CASE HEARTBEAT WHEN 0 THEN 1 ELSE 0 END "
                        + "WHERE ID = 1",
                heartbeatTable.actionQuery());

        assertThrows(RuntimeException.class, () -> OracleHeartbeatTable.parse("RW_HEARTBEAT"));
        assertThrows(
                RuntimeException.class,
                () -> OracleHeartbeatTable.parse("APP.RW_HEARTBEAT; DROP TABLE APP.CUSTOMERS"));
    }

    @Test
    public void rejectsUserProvidedOracleHeartbeatActionQuery() {
        var userProps = oracleProperties();
        userProps.put(
                DbzConnectorConfig.HEARTBEAT_ACTION_QUERY_KEY,
                "UPDATE APP.RW_HEARTBEAT SET HEARTBEAT = 1");
        var request = oracleValidateRequest(userProps);

        var exception =
                assertThrows(
                        StatusRuntimeException.class,
                        () -> SourceValidateHandler.validateSource(request));
        assertEquals(Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());
        assertEquals(
                "'debezium.heartbeat.action.query' is generated internally; configure "
                        + "'heartbeat.table.name' instead",
                exception.getStatus().getDescription());
    }

    @Test
    public void rejectsNonPositiveOracleHeartbeatInterval() {
        for (var interval : new String[] {"0", "-1"}) {
            var userProps = oracleProperties();
            userProps.put(DbzConnectorConfig.HEARTBEAT_INTERVAL_KEY, interval);

            var exception =
                    assertThrows(
                            StatusRuntimeException.class,
                            () ->
                                    SourceValidateHandler.validateSource(
                                            oracleValidateRequest(userProps)));
            assertEquals(Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());
            assertEquals(
                    "'debezium.heartbeat.interval.ms' must be greater than 0",
                    exception.getStatus().getDescription());
        }
    }

    @Test
    public void validatesOracleSourceTableExistence() {
        OracleValidator.validateTableExists("Oracle table", "APP", "CUSTOMERS", "FREEPDB1", 1);

        var exception =
                assertThrows(
                        StatusRuntimeException.class,
                        () ->
                                OracleValidator.validateTableExists(
                                        "Oracle table", "APP", "CUSTOMERS", "FREEPDB1", 0));
        assertEquals(Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());
        assertEquals(
                "Oracle table 'APP.CUSTOMERS' does not exist in PDB 'FREEPDB1'",
                exception.getStatus().getDescription());
    }

    @Test
    public void validatesOracleHeartbeatUpdatePrivilege() {
        assertFalse(
                OracleValidator.hasHeartbeatUpdatePrivilege(
                        "C##DBZUSER", "APP", Set.of(), Set.of(), Set.of()));
        assertTrue(
                OracleValidator.hasHeartbeatUpdatePrivilege(
                        "APP", "APP", Set.of(), Set.of(), Set.of()));
        assertTrue(
                OracleValidator.hasHeartbeatUpdatePrivilege(
                        "C##DBZUSER", "APP", Set.of("UPDATE ANY TABLE"), Set.of(), Set.of()));
        assertTrue(
                OracleValidator.hasHeartbeatUpdatePrivilege(
                        "C##DBZUSER", "APP", Set.of(), Set.of(), Set.of("C##DBZUSER")));
        assertTrue(
                OracleValidator.hasHeartbeatUpdatePrivilege(
                        "C##DBZUSER", "APP", Set.of(), Set.of("CDC_ROLE"), Set.of("CDC_ROLE")));
    }

    @Test
    public void validatesOracleLoggingConfiguration() {
        OracleValidator.validateLoggingConfiguration("ARCHIVELOG", "YES", "YES");

        assertThrows(
                RuntimeException.class,
                () -> OracleValidator.validateLoggingConfiguration("NOARCHIVELOG", "YES", "YES"));
        assertThrows(
                RuntimeException.class,
                () -> OracleValidator.validateLoggingConfiguration("ARCHIVELOG", "NO", "YES"));
        assertThrows(
                RuntimeException.class,
                () -> OracleValidator.validateLoggingConfiguration("ARCHIVELOG", "YES", "NO"));
    }

    private static HashMap<String, String> oracleProperties() {
        var properties = new HashMap<String, String>();
        properties.put(DbzConnectorConfig.HOST, "db.example.com");
        properties.put(DbzConnectorConfig.PORT, "1521");
        properties.put(DbzConnectorConfig.USER, "C##DBZUSER");
        properties.put(DbzConnectorConfig.PASSWORD, "secret");
        properties.put(DbzConnectorConfig.DB_NAME, "FREE");
        properties.put(DbzConnectorConfig.ORACLE_PDB_NAME, "FREEPDB1");
        properties.put(DbzConnectorConfig.ORACLE_SCHEMA_NAME, "APP");
        properties.put(DbzConnectorConfig.TABLE_NAME, "CUSTOMERS");
        properties.put(DbzConnectorConfig.HEARTBEAT_INTERVAL_KEY, "300000");
        properties.put(DbzConnectorConfig.ORACLE_HEARTBEAT_TABLE_NAME, "APP.RW_HEARTBEAT");
        return properties;
    }

    private static ConnectorServiceProto.ValidateSourceRequest oracleValidateRequest(
            Map<String, String> properties) {
        return ConnectorServiceProto.ValidateSourceRequest.newBuilder()
                .setSourceType(ConnectorServiceProto.SourceType.ORACLE)
                .putAllProperties(properties)
                .build();
    }
}
