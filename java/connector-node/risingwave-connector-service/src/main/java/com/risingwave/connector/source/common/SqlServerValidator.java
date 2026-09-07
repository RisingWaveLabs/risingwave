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

import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.source.SourceTypeE;
import com.risingwave.java.binding.Binding;
import com.risingwave.proto.Catalog;
import com.risingwave.proto.Data;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqlServerValidator extends DatabaseValidator implements AutoCloseable {
    static final Logger LOG = LoggerFactory.getLogger(SqlServerValidator.class);
    private static final int CDC_TABLE_TYPE =
            Catalog.Table.CdcTableType.CDC_TABLE_TYPE_SQLSERVER.getNumber();

    private final TableSchema tableSchema;

    private final Connection jdbcConnection;

    private final String user;
    private final String dbName;
    private final String schemaName;
    private final String tableName;

    // Whether the properties to validate is shared by multiple tables.
    // If true, we will skip validation check for table
    private final boolean isCdcSourceJob;

    public SqlServerValidator(
            Map<String, String> userProps, TableSchema tableSchema, boolean isCdcSourceJob)
            throws SQLException {
        this.tableSchema = tableSchema;

        var dbHost = userProps.get(DbzConnectorConfig.HOST);
        var dbPort = userProps.get(DbzConnectorConfig.PORT);
        var dbName = userProps.get(DbzConnectorConfig.DB_NAME);
        var user = userProps.get(DbzConnectorConfig.USER);
        var password = userProps.get(DbzConnectorConfig.PASSWORD);
        var encrypt =
                Boolean.parseBoolean(
                        userProps.getOrDefault(DbzConnectorConfig.SQL_SERVER_ENCRYPT, "false"));

        var jdbcUrl = ValidatorUtils.getJdbcUrl(SourceTypeE.SQL_SERVER, dbHost, dbPort, dbName);
        jdbcUrl = jdbcUrl + ";encrypt=" + encrypt + ";trustServerCertificate=true";
        this.jdbcConnection = DriverManager.getConnection(jdbcUrl, user, password);

        this.dbName = dbName;
        this.user = user;
        this.schemaName = userProps.get(DbzConnectorConfig.SQL_SERVER_SCHEMA_NAME);
        this.tableName = userProps.get(DbzConnectorConfig.TABLE_NAME);
        this.isCdcSourceJob = isCdcSourceJob;
    }

    @Override
    public void validateDbConfig() {
        try (var stmt =
                jdbcConnection.prepareStatement(
                        ValidatorUtils.getSql("sqlserver.db.cdc.enabled"))) {
            // check whether cdc has been enabled
            var res = stmt.executeQuery();
            while (res.next()) {
                if (!res.getString(1).equals(dbName)) {
                    throw ValidatorUtils.invalidArgument(
                            "Sql Server's DB_NAME() '"
                                    + res.getString(1)
                                    + "' does not match db_name '"
                                    + dbName
                                    + "'.");
                }
                if (res.getInt(2) != 1) {
                    throw ValidatorUtils.invalidArgument(
                            "Sql Server's '"
                                    + dbName
                                    + "' has not enabled CDC.\nPlease modify the config your Sql Server with 'EXEC sys.sp_cdc_enable_db'.");
                }
            }
        } catch (SQLException e) {
            throw ValidatorUtils.internalError(e.getMessage());
        }
        if (isCdcSourceJob) {
            try (var stmt =
                    jdbcConnection.prepareStatement(
                            ValidatorUtils.getSql("sqlserver.sql.agent.enabled"))) {
                // check whether sql server agent is enabled. It's required to run
                // fn_cdc_get_max_lsn
                var res = stmt.executeQuery();
                while (res.next()) {
                    if (res.wasNull()) {
                        throw ValidatorUtils.invalidArgument(
                                "Sql Server's sql server agent is not activated.\nYou can check it by running `SELECT servicename, startup_type_desc, status_desc FROM sys.dm_server_services WHERE servicename LIKE 'SQL Server Agent%'` in Sql Server.");
                    }
                }
            } catch (SQLException e) {
                throw ValidatorUtils.internalError(e.getMessage());
            }
        }
    }

    @Override
    public void validateUserPrivilege() {
        try {
            validatePrivileges();
        } catch (SQLException e) {
            throw ValidatorUtils.internalError(e.getMessage());
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
        if (isCdcSourceJob) {
            return;
        }
        // check whether table exist
        try (var stmt = jdbcConnection.prepareStatement(ValidatorUtils.getSql("sqlserver.table"))) {
            stmt.setString(1, schemaName);
            stmt.setString(2, tableName);
            var res = stmt.executeQuery();
            while (res.next()) {
                if (res.getInt(1) == 0) {
                    throw ValidatorUtils.invalidArgument(
                            String.format(
                                    "Sql Server table '%s'.'%s' doesn't exist in '%s'",
                                    schemaName, tableName, dbName));
                }
            }
        }

        // check cdc enabled
        try (var stmt =
                jdbcConnection.prepareStatement(
                        ValidatorUtils.getSql("sqlserver.table.cdc.enabled"))) {
            stmt.setString(1, schemaName);
            stmt.setString(2, tableName);
            var res = stmt.executeQuery();
            while (res.next()) {
                if (res.getInt(1) < 1) {
                    throw ValidatorUtils.invalidArgument(
                            "Table '"
                                    + schemaName
                                    + "."
                                    + tableName
                                    + "' has not enabled CDC.\nPlease ensure CDC is enabled.");
                }
            }
        }

        // check primary key
        try (var stmt = jdbcConnection.prepareStatement(ValidatorUtils.getSql("sqlserver.pk"))) {
            stmt.setString(1, this.schemaName);
            stmt.setString(2, this.tableName);
            var res = stmt.executeQuery();
            var pkFields = new HashSet<String>();
            while (res.next()) {
                var name = res.getString(1);
                pkFields.add(name);
            }
            primaryKeyCheck(tableSchema, pkFields);
        }

        // Check whether the db is case-sensitive
        boolean isCaseSensitive = false;
        try (var stmt =
                jdbcConnection.prepareStatement(
                        ValidatorUtils.getSql("sqlserver.case.sensitive"))) {
            stmt.setString(1, this.dbName);
            var res = stmt.executeQuery();
            while (res.next()) {
                var caseSensitive = res.getInt(2);
                if (caseSensitive == 1) {
                    isCaseSensitive = true;
                }
            }
        }
        // Check whether source schema match table schema on upstream
        // All columns defined must exist in upstream database
        try (var stmt =
                jdbcConnection.prepareStatement(ValidatorUtils.getSql("sqlserver.table_schema"))) {
            stmt.setString(1, this.schemaName);
            stmt.setString(2, this.tableName);
            var res = stmt.executeQuery();

            // Field names in lower case -> data type
            Map<String, String> schema = new HashMap<>();
            while (res.next()) {
                var field = res.getString(1);
                var dataType = res.getString(2);
                if (isCaseSensitive) {
                    schema.put(field, dataType);
                } else {
                    schema.put(field.toLowerCase(), dataType);
                }
            }

            for (var e : tableSchema.getColumnTypes().entrySet()) {
                // skip validate internal columns
                if (e.getKey().startsWith(ValidatorUtils.INTERNAL_COLUMN_PREFIX)) {
                    continue;
                }
                var dataType = schema.get(isCaseSensitive ? e.getKey() : e.getKey().toLowerCase());
                if (dataType == null) {
                    throw ValidatorUtils.invalidArgument(
                            "Column '" + e.getKey() + "' not found in the upstream database");
                }
                if (!isDataTypeCompatible(dataType, e.getValue())) {
                    throw ValidatorUtils.invalidArgument(
                            "Incompatible data type of column " + e.getKey());
                }
            }
        }
    }

    private void validatePrivileges() throws SQLException {
        if (isCdcSourceJob) {
            return;
        }

        try (var stmt =
                jdbcConnection.prepareStatement(ValidatorUtils.getSql("sqlserver.has.perms"))) {
            stmt.setString(1, this.schemaName);
            stmt.setString(2, this.tableName);
            var res = stmt.executeQuery();
            while (res.next()) {
                if (res.getInt(1) != 1) {
                    throw ValidatorUtils.invalidArgument(
                            "Sql Server user '"
                                    + user
                                    + "' must have select privilege on table '"
                                    + schemaName
                                    + "."
                                    + tableName
                                    + "''s CDC table.");
                }
            }
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
                            + " primary key columns, but the source table in SQL Server has "
                            + pkFields.size()
                            + " columns.");
        }
        for (var colName : sourceSchema.getPrimaryKeys()) {
            if (!pkFields.contains(colName)) {
                throw ValidatorUtils.invalidArgument(
                        "Primary key mismatch: The primary key list of the source table in SQL Server does not contain '"
                                + colName
                                + "'.\nHint: If your primary key contains uppercase letters, please ensure that the primary key in the DML of RisingWave uses the same uppercase format and is wrapped with double quotes (\"\").");
            }
        }
    }

    private boolean isDataTypeCompatible(String ssDataType, Data.DataType.TypeName typeName) {
        return Binding.validateCdcSourceColumnType(
                CDC_TABLE_TYPE, ssDataType, typeName.getNumber(), -1, false, null);
    }
}
