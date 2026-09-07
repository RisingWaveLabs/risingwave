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

import com.risingwave.connector.api.TableSchema;
import com.risingwave.java.binding.Binding;
import com.risingwave.proto.Catalog;
import com.risingwave.proto.Data;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;

public class MySqlValidator extends DatabaseValidator implements AutoCloseable {
    private static final String REPLICATION_CLIENT = "REPLICATION CLIENT";
    private static final String BINLOG_MONITOR = "BINLOG MONITOR";
    private static final int CDC_TABLE_TYPE =
            Catalog.Table.CdcTableType.CDC_TABLE_TYPE_MYSQL.getNumber();

    private final Map<String, String> userProps;

    private final TableSchema tableSchema;

    private final Connection jdbcConnection;

    // validation is for cdc source job
    private final boolean isCdcSourceJob;
    // validation is for backfill table
    private final boolean isBackfillTable;

    public MySqlValidator(
            Map<String, String> userProps,
            TableSchema tableSchema,
            boolean isCdcSourceJob,
            boolean isBackfillTable)
            throws SQLException {
        this.userProps = userProps;
        this.tableSchema = tableSchema;

        var dbHost = userProps.get(DbzConnectorConfig.HOST);
        var dbPort = userProps.get(DbzConnectorConfig.PORT);
        var jdbcUrl = String.format("jdbc:mysql://%s:%s", dbHost, dbPort);
        var properties = new Properties();
        properties.setProperty("user", userProps.get(DbzConnectorConfig.USER));
        properties.setProperty("password", userProps.get(DbzConnectorConfig.PASSWORD));
        properties.setProperty(
                "sslMode", userProps.getOrDefault(DbzConnectorConfig.MYSQL_SSL_MODE, "DISABLED"));
        properties.setProperty("allowPublicKeyRetrieval", "true");

        this.jdbcConnection = DriverManager.getConnection(jdbcUrl, properties);
        this.isCdcSourceJob = isCdcSourceJob;
        this.isBackfillTable = isBackfillTable;
    }

    @Override
    public void validateDbConfig() {
        try {
            // Check whether MySQL version is less than 8.4,
            // since MySQL 8.4 introduces some breaking changes:
            // https://dev.mysql.com/doc/relnotes/mysql/8.4/en/news-8-4-0.html#mysqld-8-4-0-deprecation-removal
            var major = jdbcConnection.getMetaData().getDatabaseMajorVersion();
            var minor = jdbcConnection.getMetaData().getDatabaseMinorVersion();

            // if ((major > 8) || (major == 8 && minor >= 4)) {
            //     throw ValidatorUtils.failedPrecondition("MySQL version should be less than 8.4");
            // }

            // "database.name" is a comma-separated list of database names
            var dbNames = userProps.get(DbzConnectorConfig.DB_NAME);
            for (var dbName : dbNames.split(",")) {
                // check the existence of the database
                try (var stmt =
                        jdbcConnection.prepareStatement(
                                ValidatorUtils.getSql("mysql.check_db_exist"))) {
                    stmt.setString(1, dbName.trim());
                    var res = stmt.executeQuery();
                    while (res.next()) {
                        var ret = res.getInt(1);
                        if (ret == 0) {
                            throw ValidatorUtils.invalidArgument(
                                    String.format(
                                            "MySQL database '%s' doesn't exist", dbName.trim()));
                        }
                    }
                }
            }

            validateBinlogConfig();
        } catch (SQLException e) {
            throw ValidatorUtils.internalError(e.getMessage());
        }
    }

    private void validateBinlogConfig() throws SQLException {
        // check whether source db has enabled binlog
        try (var stmt = jdbcConnection.createStatement()) {
            var res = stmt.executeQuery(ValidatorUtils.getSql("mysql.bin_log"));
            while (res.next()) {
                if (!res.getString(2).equalsIgnoreCase("ON")) {
                    throw ValidatorUtils.internalError(
                            "MySQL doesn't enable binlog.\nPlease set the value of log_bin to 'ON' and restart your MySQL server.");
                }
            }
        }

        // check binlog format
        try (var stmt = jdbcConnection.createStatement()) {
            var res = stmt.executeQuery(ValidatorUtils.getSql("mysql.bin_format"));
            while (res.next()) {
                if (!res.getString(2).equalsIgnoreCase("ROW")) {
                    throw ValidatorUtils.internalError(
                            "MySQL binlog_format should be 'ROW'.\nPlease modify the config and restart your MySQL server.");
                }
            }
        }

        // check binlog image
        try (var stmt = jdbcConnection.createStatement()) {
            var res = stmt.executeQuery(ValidatorUtils.getSql("mysql.bin_row_image"));
            while (res.next()) {
                if (!res.getString(2).equalsIgnoreCase("FULL")) {
                    throw ValidatorUtils.internalError(
                            "MySQL binlog_row_image should be 'FULL'.\\nPlease modify the config and restart your MySQL server.");
                }
            }
        }
    }

    @Override
    public void validateUserPrivilege() {
        try {
            String[] privilegesRequired = getRequiredPrivileges();
            var hashSet = new HashSet<>(List.of(privilegesRequired));
            try (var stmt = jdbcConnection.createStatement()) {
                var res = stmt.executeQuery(ValidatorUtils.getSql("mysql.grants"));
                while (res.next()) {
                    String granted = res.getString(1).toUpperCase();
                    // mysql 5.7 root user has all privileges
                    if (granted.contains("ALL")) {
                        // all privileges granted, check passed
                        return;
                    }

                    // remove granted privilege from the set
                    hashSet.removeIf(required -> isPrivilegeGranted(required, granted));
                    if (hashSet.isEmpty()) {
                        break;
                    }
                }
                if (!hashSet.isEmpty()) {
                    throw ValidatorUtils.invalidArgument(
                            "MySQL user doesn't have enough privileges: " + hashSet);
                }
            }
        } catch (SQLException e) {
            throw ValidatorUtils.internalError(e.getMessage());
        }
    }

    static boolean isPrivilegeGranted(String required, String granted) {
        if (granted.contains(required)) {
            return true;
        }

        // MariaDB 10.5 renamed REPLICATION CLIENT to BINLOG MONITOR. The old GRANT syntax is
        // accepted for compatibility, but SHOW GRANTS reports the new privilege name.
        return required.equals(REPLICATION_CLIENT) && granted.contains(BINLOG_MONITOR);
    }

    private String[] getRequiredPrivileges() {
        if (isCdcSourceJob) {
            return new String[] {"SELECT", "REPLICATION SLAVE", REPLICATION_CLIENT};
        } else if (isBackfillTable) {
            // check privilege again to ensure the user has the privilege to backfill
            return new String[] {"SELECT", "REPLICATION SLAVE", REPLICATION_CLIENT};
        } else {
            // dedicated source needs more privileges to acquire global lock
            return new String[] {
                "SELECT", "RELOAD", "SHOW DATABASES", "REPLICATION SLAVE", REPLICATION_CLIENT
            };
        }
    }

    @Override
    public void validateTable() {
        try {
            validateTableSchema();
        } catch (SQLException e) {
            throw ValidatorUtils.internalError(e.getMessage());
        }
    }

    @Override
    boolean isCdcSourceJob() {
        return isCdcSourceJob;
    }

    private void validateTableSchema() throws SQLException {
        // check whether table exist
        var dbName = userProps.get(DbzConnectorConfig.DB_NAME);
        var tableName = userProps.get(DbzConnectorConfig.TABLE_NAME);
        try (var stmt = jdbcConnection.prepareStatement(ValidatorUtils.getSql("mysql.table"))) {
            stmt.setString(1, dbName);
            stmt.setString(2, tableName);
            var res = stmt.executeQuery();
            while (res.next()) {
                var ret = res.getInt(1);
                if (ret == 0) {
                    throw ValidatorUtils.invalidArgument(
                            String.format("MySQL table '%s' doesn't exist", tableName));
                }
            }
        }

        // check whether PK constraint match source table definition
        try (var stmt =
                jdbcConnection.prepareStatement(ValidatorUtils.getSql("mysql.table_schema"))) {
            stmt.setString(1, dbName);
            stmt.setString(2, tableName);

            // Record charMaxLength for each column
            class ColumnInfo {
                String dataType;
                long charMaxLength; // Use long to avoid overflow for 4294967295
                String columnType; // Full column type including UNSIGNED modifier

                ColumnInfo(String dataType, long charMaxLength, String columnType) {
                    this.dataType = dataType;
                    this.charMaxLength = charMaxLength;
                    this.columnType = columnType;
                }

                boolean isUnsigned() {
                    return columnType != null && columnType.toLowerCase().contains("unsigned");
                }
            }

            // field name (lowercase) -> ColumnInfo
            var upstreamSchema = new HashMap<String, ColumnInfo>();
            var pkFields = new HashSet<String>();
            var res = stmt.executeQuery();
            while (res.next()) {
                var field = res.getString(1);
                var dataType = res.getString(2);
                var key = res.getString(3);
                long charMaxLength = res.getLong(4);
                var columnType = res.getString(5); // Get full column type (e.g., "bigint unsigned")
                // In MySQL, some types (such as text/blob) will return 4294967295 for
                // charMaxLength, need special handling
                if (res.wasNull()) {
                    charMaxLength = -1;
                }
                upstreamSchema.put(
                        field.toLowerCase(), new ColumnInfo(dataType, charMaxLength, columnType));
                if (key.equalsIgnoreCase("PRI")) {
                    pkFields.add(field.toLowerCase());
                }
            }

            // All columns defined must exist in upstream database
            for (var e : tableSchema.getColumnTypes().entrySet()) {
                // skip validate internal columns
                if (e.getKey().startsWith(ValidatorUtils.INTERNAL_COLUMN_PREFIX)) {
                    continue;
                }
                var columnInfo = upstreamSchema.get(e.getKey().toLowerCase());
                if (columnInfo == null) {
                    throw ValidatorUtils.invalidArgument(
                            "Column '" + e.getKey() + "' not found in the upstream database");
                }
                if (!isDataTypeCompatible(
                        columnInfo.dataType,
                        e.getValue(),
                        columnInfo.charMaxLength,
                        columnInfo.isUnsigned())) {
                    throw ValidatorUtils.invalidArgument(
                            "Incompatible data type of column "
                                    + e.getKey()
                                    + ". MySQL type: "
                                    + columnInfo.columnType
                                    + ", RisingWave type: "
                                    + e.getValue());
                }
            }

            primaryKeyCheck(tableSchema, pkFields);
        }
    }

    @Override
    public void close() throws Exception {
        if (null != jdbcConnection) {
            jdbcConnection.close();
        }
    }

    private static void primaryKeyCheck(TableSchema sourceSchema, Set<String> pkFields)
            throws RuntimeException {
        if (sourceSchema.getPrimaryKeys().size() != pkFields.size()) {
            throw ValidatorUtils.invalidArgument(
                    "Primary key mismatch: the SQL schema defines "
                            + sourceSchema.getPrimaryKeys().size()
                            + " primary key columns, but the source table in MySQL has "
                            + pkFields.size()
                            + " columns.");
        }
        for (var colName : sourceSchema.getPrimaryKeys()) {
            if (!pkFields.contains(colName.toLowerCase())) {
                throw ValidatorUtils.invalidArgument(
                        "Primary key mismatch: The primary key list of the source table in MySQL does not contain '"
                                + colName
                                + "'.");
            }
        }
    }

    private boolean isDataTypeCompatible(
            String mysqlDataType,
            Data.DataType.TypeName typeName,
            long charMaxLength,
            boolean isUnsigned) {
        return Binding.validateCdcSourceColumnType(
                CDC_TABLE_TYPE,
                mysqlDataType,
                typeName.getNumber(),
                charMaxLength,
                isUnsigned,
                null);
    }
}
