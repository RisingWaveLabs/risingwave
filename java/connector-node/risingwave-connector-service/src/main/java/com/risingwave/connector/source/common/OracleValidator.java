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

import com.risingwave.connector.api.source.SourceTypeE;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

public class OracleValidator extends DatabaseValidator implements AutoCloseable {
    private static final Pattern ORACLE_UNQUOTED_IDENTIFIER =
            Pattern.compile("[A-Za-z][A-Za-z0-9_$#]*");

    private static final Set<String> REQUIRED_PRIVILEGES =
            Set.of(
                    "CREATE SESSION",
                    "SET CONTAINER",
                    "FLASHBACK ANY TABLE",
                    "SELECT ANY TABLE",
                    "SELECT ANY TRANSACTION",
                    "LOGMINING",
                    "LOCK ANY TABLE",
                    "CREATE TABLE",
                    "CREATE SEQUENCE");

    private static final Set<String> REQUIRED_ROLES =
            Set.of("SELECT_CATALOG_ROLE", "EXECUTE_CATALOG_ROLE");

    private final Connection jdbcConnection;
    private final String pdbName;
    private final String schemaName;
    private final String tableName;
    private final Optional<OracleHeartbeatTable> heartbeatTable;
    private final boolean isCdcSourceJob;

    public OracleValidator(Map<String, String> userProps, boolean isCdcSourceJob)
            throws SQLException {
        this.pdbName = normalizePdbName(userProps.get(DbzConnectorConfig.ORACLE_PDB_NAME));
        this.heartbeatTable =
                DbzConnectorConfig.isHeartbeatEnabled(userProps)
                        ? Optional.of(
                                OracleHeartbeatTable.parse(
                                        userProps.get(
                                                DbzConnectorConfig.ORACLE_HEARTBEAT_TABLE_NAME)))
                        : Optional.empty();
        var jdbcUrl =
                ValidatorUtils.getJdbcUrl(
                        SourceTypeE.ORACLE,
                        userProps.get(DbzConnectorConfig.HOST),
                        userProps.get(DbzConnectorConfig.PORT),
                        userProps.get(DbzConnectorConfig.DB_NAME));
        this.jdbcConnection =
                DriverManager.getConnection(
                        jdbcUrl,
                        userProps.get(DbzConnectorConfig.USER),
                        userProps.get(DbzConnectorConfig.PASSWORD));
        this.schemaName = userProps.get(DbzConnectorConfig.ORACLE_SCHEMA_NAME);
        this.tableName = userProps.get(DbzConnectorConfig.TABLE_NAME);
        this.isCdcSourceJob = isCdcSourceJob;
    }

    @Override
    public void validateAll() {
        super.validateAll();
        heartbeatTable.ifPresent(this::validateHeartbeatTable);
    }

    @Override
    void validateDbConfig() {
        try {
            validateLoggingConfiguration();
            validatePdb();
        } catch (SQLException e) {
            throw ValidatorUtils.internalError(e.getMessage());
        }
    }

    private void validateLoggingConfiguration() throws SQLException {
        try (var stmt = jdbcConnection.createStatement();
                var result =
                        stmt.executeQuery(
                                "SELECT LOG_MODE, FORCE_LOGGING, SUPPLEMENTAL_LOG_DATA_MIN "
                                        + "FROM V$DATABASE")) {
            if (!result.next()) {
                throw ValidatorUtils.invalidArgument(
                        "Unable to read Oracle logging configuration from V$DATABASE");
            }
            validateLoggingConfiguration(
                    result.getString("LOG_MODE"),
                    result.getString("FORCE_LOGGING"),
                    result.getString("SUPPLEMENTAL_LOG_DATA_MIN"));
        }
    }

    static void validateLoggingConfiguration(
            String logMode, String forceLogging, String supplementalLogDataMin) {
        if (!"ARCHIVELOG".equalsIgnoreCase(logMode)) {
            throw ValidatorUtils.failedPrecondition(
                    "Oracle must run in ARCHIVELOG mode for LogMiner CDC");
        }
        if (!"YES".equalsIgnoreCase(forceLogging)) {
            throw ValidatorUtils.failedPrecondition(
                    "Oracle FORCE LOGGING must be enabled for LogMiner CDC");
        }
        if (supplementalLogDataMin == null || "NO".equalsIgnoreCase(supplementalLogDataMin)) {
            throw ValidatorUtils.failedPrecondition(
                    "Oracle minimal supplemental logging must be enabled for LogMiner CDC");
        }
    }

    private void validatePdb() throws SQLException {
        try (var stmt =
                jdbcConnection.prepareStatement(
                        "SELECT OPEN_MODE FROM V$PDBS WHERE UPPER(NAME) = ?")) {
            stmt.setString(1, pdbName);
            try (var result = stmt.executeQuery()) {
                if (!result.next()) {
                    throw ValidatorUtils.invalidArgument(
                            String.format("Oracle PDB '%s' does not exist", pdbName));
                }
                var openMode = result.getString("OPEN_MODE");
                if (!"READ WRITE".equalsIgnoreCase(openMode)) {
                    throw ValidatorUtils.failedPrecondition(
                            String.format(
                                    "Oracle PDB '%s' must be open READ WRITE, but is '%s'",
                                    pdbName, openMode));
                }
            }
        }
    }

    @Override
    void validateUserPrivilege() {
        try {
            var privileges = querySingleColumn("SELECT PRIVILEGE FROM SESSION_PRIVS");
            validateRequiredGrants("privileges", REQUIRED_PRIVILEGES, privileges);

            var roles = querySingleColumn("SELECT ROLE FROM SESSION_ROLES");
            validateRequiredGrants("roles", REQUIRED_ROLES, roles);
        } catch (SQLException e) {
            throw ValidatorUtils.internalError(e.getMessage());
        }
    }

    private Set<String> querySingleColumn(String sql, String... parameters) throws SQLException {
        var values = new HashSet<String>();
        try (var stmt = jdbcConnection.prepareStatement(sql)) {
            for (int i = 0; i < parameters.length; i++) {
                stmt.setString(i + 1, parameters[i]);
            }
            try (var result = stmt.executeQuery()) {
                while (result.next()) {
                    values.add(result.getString(1).toUpperCase(Locale.ROOT));
                }
            }
        }
        return values;
    }

    static void validateRequiredGrants(
            String grantType, Set<String> required, Set<String> granted) {
        var missing = new HashSet<>(required);
        missing.removeAll(granted);
        if (!missing.isEmpty()) {
            throw ValidatorUtils.invalidArgument(
                    String.format("Oracle user is missing required %s: %s", grantType, missing));
        }
    }

    @Override
    void validateTable() {
        try {
            switchToPdb();
            validateTableExists();
            validateTableSupplementalLogging();
        } catch (SQLException e) {
            throw ValidatorUtils.internalError(e.getMessage());
        }
    }

    private void switchToPdb() throws SQLException {
        try (var stmt = jdbcConnection.createStatement()) {
            stmt.execute("ALTER SESSION SET CONTAINER = " + pdbName);
        }
    }

    private void validateTableExists() throws SQLException {
        validateTableExists(schemaName, tableName, "Oracle table");
    }

    private void validateTableExists(String owner, String table, String description)
            throws SQLException {
        try (var stmt =
                jdbcConnection.prepareStatement(
                        "SELECT COUNT(*) FROM ALL_TABLES WHERE OWNER = ? AND TABLE_NAME = ?")) {
            stmt.setString(1, owner);
            stmt.setString(2, table);
            try (var result = stmt.executeQuery()) {
                result.next();
                validateTableExists(description, owner, table, pdbName, result.getInt(1));
            }
        }
    }

    static void validateTableExists(
            String description, String owner, String table, String pdbName, int tableCount) {
        if (tableCount == 0) {
            throw ValidatorUtils.invalidArgument(
                    String.format(
                            "%s '%s.%s' does not exist in PDB '%s'",
                            description, owner, table, pdbName));
        }
    }

    private void validateHeartbeatTable(OracleHeartbeatTable heartbeatTable) {
        try {
            switchToPdb();
            validateTableExists(
                    heartbeatTable.owner(), heartbeatTable.table(), "Oracle heartbeat table");
            validateHeartbeatColumns(heartbeatTable);
            validateHeartbeatPrimaryKey(heartbeatTable);
            validateHeartbeatTableHasRow(heartbeatTable);
            validateHeartbeatUpdatePrivilege(heartbeatTable);
        } catch (SQLException e) {
            throw ValidatorUtils.internalError(e.getMessage());
        }
    }

    private void validateHeartbeatColumns(OracleHeartbeatTable heartbeatTable) throws SQLException {
        try (var stmt =
                jdbcConnection.prepareStatement(
                        "SELECT COLUMN_NAME, DATA_TYPE FROM ALL_TAB_COLUMNS "
                                + "WHERE OWNER = ? AND TABLE_NAME = ? AND COLUMN_NAME IN (?, ?)")) {
            stmt.setString(1, heartbeatTable.owner());
            stmt.setString(2, heartbeatTable.table());
            stmt.setString(3, OracleHeartbeatTable.ID_COLUMN);
            stmt.setString(4, OracleHeartbeatTable.HEARTBEAT_COLUMN);
            try (var result = stmt.executeQuery()) {
                var columns = new HashSet<String>();
                while (result.next()) {
                    if ("NUMBER".equalsIgnoreCase(result.getString("DATA_TYPE"))) {
                        columns.add(result.getString("COLUMN_NAME").toUpperCase(Locale.ROOT));
                    }
                }
                if (!columns.containsAll(
                        Set.of(
                                OracleHeartbeatTable.ID_COLUMN,
                                OracleHeartbeatTable.HEARTBEAT_COLUMN))) {
                    throw ValidatorUtils.invalidArgument(
                            String.format(
                                    "Oracle heartbeat table '%s' must contain NUMBER columns "
                                            + "named '%s' and '%s'",
                                    heartbeatTable.qualifiedName(),
                                    OracleHeartbeatTable.ID_COLUMN,
                                    OracleHeartbeatTable.HEARTBEAT_COLUMN));
                }
            }
        }
    }

    private void validateHeartbeatPrimaryKey(OracleHeartbeatTable heartbeatTable)
            throws SQLException {
        try (var stmt =
                jdbcConnection.prepareStatement(
                        "SELECT COUNT(*) FROM ("
                                + "SELECT C.CONSTRAINT_NAME FROM ALL_CONSTRAINTS C "
                                + "JOIN ALL_CONS_COLUMNS CC "
                                + "ON C.OWNER = CC.OWNER AND C.CONSTRAINT_NAME = CC.CONSTRAINT_NAME "
                                + "WHERE C.OWNER = ? AND C.TABLE_NAME = ? AND C.CONSTRAINT_TYPE = 'P' "
                                + "GROUP BY C.CONSTRAINT_NAME "
                                + "HAVING COUNT(*) = 1 AND MAX(CC.COLUMN_NAME) = ?)")) {
            stmt.setString(1, heartbeatTable.owner());
            stmt.setString(2, heartbeatTable.table());
            stmt.setString(3, OracleHeartbeatTable.ID_COLUMN);
            try (var result = stmt.executeQuery()) {
                result.next();
                if (result.getInt(1) == 0) {
                    throw ValidatorUtils.invalidArgument(
                            String.format(
                                    "Oracle heartbeat table '%s' must use '%s' as its "
                                            + "single-column primary key",
                                    heartbeatTable.qualifiedName(),
                                    OracleHeartbeatTable.ID_COLUMN));
                }
            }
        }
    }

    private void validateHeartbeatTableHasRow(OracleHeartbeatTable heartbeatTable)
            throws SQLException {
        var sql =
                String.format(
                        "SELECT 1 FROM %s WHERE %s = 1",
                        heartbeatTable.qualifiedName(), OracleHeartbeatTable.ID_COLUMN);
        try (var stmt = jdbcConnection.createStatement();
                var result = stmt.executeQuery(sql)) {
            if (!result.next()) {
                throw ValidatorUtils.invalidArgument(
                        String.format(
                                "Oracle heartbeat table '%s' must contain a row with %s = 1",
                                heartbeatTable.qualifiedName(), OracleHeartbeatTable.ID_COLUMN));
            }
        }
    }

    private void validateHeartbeatUpdatePrivilege(OracleHeartbeatTable heartbeatTable)
            throws SQLException {
        var sessionUser =
                querySingleColumn("SELECT SYS_CONTEXT('USERENV', 'SESSION_USER') FROM DUAL")
                        .iterator()
                        .next();
        var sessionPrivileges = querySingleColumn("SELECT PRIVILEGE FROM SESSION_PRIVS");
        var sessionRoles = querySingleColumn("SELECT ROLE FROM SESSION_ROLES");
        var updateGrantees =
                querySingleColumn(
                        "SELECT GRANTEE FROM ALL_TAB_PRIVS "
                                + "WHERE OWNER = ? AND TABLE_NAME = ? AND PRIVILEGE = 'UPDATE' "
                                + "UNION SELECT GRANTEE FROM ALL_COL_PRIVS "
                                + "WHERE OWNER = ? AND TABLE_NAME = ? AND COLUMN_NAME = ? "
                                + "AND PRIVILEGE = 'UPDATE'",
                        heartbeatTable.owner(),
                        heartbeatTable.table(),
                        heartbeatTable.owner(),
                        heartbeatTable.table(),
                        OracleHeartbeatTable.HEARTBEAT_COLUMN);

        if (!hasHeartbeatUpdatePrivilege(
                sessionUser,
                heartbeatTable.owner(),
                sessionPrivileges,
                sessionRoles,
                updateGrantees)) {
            throw ValidatorUtils.invalidArgument(
                    String.format(
                            "Oracle user '%s' needs UPDATE permission on heartbeat table '%s'",
                            sessionUser, heartbeatTable.qualifiedName()));
        }
    }

    static boolean hasHeartbeatUpdatePrivilege(
            String sessionUser,
            String owner,
            Set<String> sessionPrivileges,
            Set<String> sessionRoles,
            Set<String> updateGrantees) {
        if (owner.equalsIgnoreCase(sessionUser)
                || sessionPrivileges.contains("UPDATE ANY TABLE")
                || updateGrantees.contains(sessionUser.toUpperCase(Locale.ROOT))
                || updateGrantees.contains("PUBLIC")) {
            return true;
        }
        return sessionRoles.stream().anyMatch(updateGrantees::contains);
    }

    private void validateTableSupplementalLogging() throws SQLException {
        try (var stmt =
                jdbcConnection.prepareStatement(
                        "SELECT COUNT(*) FROM ALL_LOG_GROUPS "
                                + "WHERE OWNER = ? AND TABLE_NAME = ?")) {
            stmt.setString(1, schemaName);
            stmt.setString(2, tableName);
            try (var result = stmt.executeQuery()) {
                result.next();
                if (result.getInt(1) == 0) {
                    throw ValidatorUtils.failedPrecondition(
                            String.format(
                                    "Oracle supplemental logging must be enabled on table '%s.%s'",
                                    schemaName, tableName));
                }
            }
        }
    }

    static String normalizePdbName(String pdbName) {
        if (pdbName == null || !ORACLE_UNQUOTED_IDENTIFIER.matcher(pdbName).matches()) {
            throw ValidatorUtils.invalidArgument(
                    "'database.pdb.name' must be an unquoted Oracle identifier");
        }
        return pdbName.toUpperCase(Locale.ROOT);
    }

    @Override
    boolean isCdcSourceJob() {
        return isCdcSourceJob;
    }

    @Override
    public void close() throws SQLException {
        jdbcConnection.close();
    }
}
