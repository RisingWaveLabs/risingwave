/*
 * Copyright 2025 RisingWave Labs
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

package com.risingwave.connector;

import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkRow;
import com.risingwave.connector.api.sink.SinkWriter;
import com.risingwave.connector.jdbc.JdbcDialect;
import com.risingwave.proto.ConnectorServiceProto;
import com.risingwave.proto.Data;
import com.risingwave.proto.Data.DataType.TypeName;
import io.grpc.Status;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchAppendOnlyJDBCSink implements SinkWriter {
    private static final String ERROR_REPORT_TEMPLATE = "Error when exec %s, message %s";

    private final JdbcDialect jdbcDialect;
    private final JDBCSinkConfig config;
    private Connection conn;
    private JdbcStatements jdbcStatements;

    private final TableSchema tableSchema;

    public static final String JDBC_COLUMN_NAME_KEY = "COLUMN_NAME";
    public static final String JDBC_DATA_TYPE_KEY = "DATA_TYPE";
    public static final String JDBC_TYPE_NAME_KEY = "TYPE_NAME";

    private static final Logger LOG = LoggerFactory.getLogger(BatchAppendOnlyJDBCSink.class);

    public BatchAppendOnlyJDBCSink(JDBCSinkConfig config, TableSchema tableSchema) {
        this.tableSchema = tableSchema;

        var jdbcUrl = config.getJdbcUrl().toLowerCase();
        var factory = JdbcUtils.getDialectFactory(jdbcUrl);
        this.config = config;
        try {
            conn = config.getConnection();
            // column name -> java.sql.Types
            Map<String, Integer> columnTypeMapping =
                    getColumnTypeMapping(conn, config.getTableName(), config.getSchemaName());

            // A vector of upstream column types
            List<Integer> columnSqlTypes =
                    Arrays.stream(tableSchema.getColumnNames())
                            .map(
                                    columnName -> {
                                        Integer sqlType = columnTypeMapping.get(columnName);
                                        return sqlType != null
                                                ? sqlType
                                                : getSqlTypeFromTableSchema(
                                                        tableSchema, columnName);
                                    })
                            .collect(Collectors.toList());

            List<Integer> pkIndices =
                    tableSchema.getPrimaryKeys().stream()
                            .map(tableSchema::getColumnIndex)
                            .collect(Collectors.toList());

            LOG.info(
                    "schema = {}, table = {}, tableSchema = {}, columnSqlTypes = {}, pkIndices = {}, queryTimeout = {}, batchInsertRows = {}",
                    config.getSchemaName(),
                    config.getTableName(),
                    tableSchema,
                    columnSqlTypes,
                    pkIndices,
                    config.getQueryTimeout(),
                    config.getBatchInsertRows());
            if (factory.isPresent()) {
                this.jdbcDialect = factory.get().create(columnSqlTypes, pkIndices);
            } else {
                throw Status.INVALID_ARGUMENT
                        .withDescription("Unsupported jdbc url: " + jdbcUrl)
                        .asRuntimeException();
            }
            LOG.info(
                    "JDBC connection: autoCommit = {}, trxn = {}",
                    conn.getAutoCommit(),
                    conn.getTransactionIsolation());
            // Commit the `getTransactionIsolation`
            if (!conn.getAutoCommit()) {
                conn.commit();
            }

            jdbcStatements =
                    new JdbcStatements(conn, config.getQueryTimeout(), config.getBatchInsertRows());
        } catch (SQLException e) {
            throw Status.INTERNAL
                    .withDescription(
                            String.format(ERROR_REPORT_TEMPLATE, e.getSQLState(), e.getMessage()))
                    .asRuntimeException();
        }
    }

    private static Map<String, Integer> getColumnTypeMapping(
            Connection conn, String tableName, String schemaName) {
        Map<String, Integer> columnTypeMap = new HashMap<>();
        try {
            ResultSet columnResultSet =
                    conn.getMetaData().getColumns(null, schemaName, tableName, null);

            while (columnResultSet.next()) {
                var typeName = columnResultSet.getString(JDBC_TYPE_NAME_KEY);
                int dt = columnResultSet.getInt(JDBC_DATA_TYPE_KEY);
                // NOTE: Workaround a known issue of pgjdbc
                // See also https://github.com/pgjdbc/pgjdbc/issues/1766
                if (dt == Types.TIMESTAMP
                        && (typeName.equalsIgnoreCase("timestamptz")
                                || typeName.equalsIgnoreCase("timestamp with time zone"))) {
                    dt = Types.TIMESTAMP_WITH_TIMEZONE;
                }
                columnTypeMap.put(columnResultSet.getString(JDBC_COLUMN_NAME_KEY), dt);
            }
        } catch (SQLException e) {
            throw Status.INTERNAL
                    .withDescription(
                            String.format(ERROR_REPORT_TEMPLATE, e.getSQLState(), e.getMessage()))
                    .asRuntimeException();
        }
        LOG.info(
                "schema = {}, table = {}: detected column type mapping = {}",
                schemaName,
                tableName,
                columnTypeMap);
        return columnTypeMap;
    }

    private static int getSqlTypeFromTableSchema(TableSchema tableSchema, String columnName) {
        TypeName typeName = tableSchema.getColumnType(columnName);
        return switch (typeName) {
            case INT16 -> Types.SMALLINT;
            case INT32 -> Types.INTEGER;
            case INT64 -> Types.BIGINT;
            case FLOAT -> Types.REAL;
            case DOUBLE -> Types.DOUBLE;
            case BOOLEAN -> Types.BOOLEAN;
            case VARCHAR, JSONB, INTERVAL -> Types.VARCHAR;
            case DECIMAL -> Types.DECIMAL;
            case TIME -> Types.TIME;
            case TIMESTAMP -> Types.TIMESTAMP;
            case TIMESTAMPTZ -> Types.TIMESTAMP_WITH_TIMEZONE;
            case DATE -> Types.DATE;
            default ->
                    throw Status.INVALID_ARGUMENT
                            .withDescription(
                                    "Unsupported table schema type for JDBC sink: " + typeName)
                            .asRuntimeException();
        };
    }

    @Override
    public boolean write(Iterable<SinkRow> rows) {
        for (SinkRow row : rows) {
            if (row.getOp() != Data.Op.INSERT) {
                throw Status.FAILED_PRECONDITION
                        .withDescription("unexpected op type: " + row.getOp())
                        .asRuntimeException();
            }
            jdbcStatements.prepareInsert(row);
        }
        try {
            jdbcStatements.tryExecute();
        } catch (SQLException e) {
            LOG.error("Error executing JDBC statements in write: {}", e.getMessage());
            throw Status.INTERNAL
                    .withDescription(
                            String.format(ERROR_REPORT_TEMPLATE, e.getSQLState(), e.getMessage()))
                    .withCause(e)
                    .asRuntimeException();
        }
        return true;
    }

    /**
     * A wrapper class for JDBC prepared statements We create this object one time and reuse it
     * across multiple batches if only the JDBC connection is valid.
     */
    class JdbcStatements implements AutoCloseable {
        private final int queryTimeoutSecs;
        private final int batchInsertRows;
        private PreparedStatement insertStatement;
        private int cnt;

        private final Connection conn;

        public JdbcStatements(Connection conn, int queryTimeoutSecs, int batchInsertRows)
                throws SQLException {
            this.queryTimeoutSecs = queryTimeoutSecs;
            this.batchInsertRows = batchInsertRows;
            this.conn = conn;
            this.cnt = 0;
            // Set database and schema for Snowflake connections
            if (config.getJdbcUrl().toLowerCase().startsWith("jdbc:snowflake")) {
                setSnowflakeDatabaseAndSchema();
            }
            var schemaTableName =
                    jdbcDialect.createSchemaTableName(
                            config.getSchemaName(), config.getTableName());

            var insertSql =
                    jdbcDialect.getInsertIntoStatement(
                            schemaTableName, List.of(tableSchema.getColumnNames()));
            this.insertStatement = conn.prepareStatement(insertSql, Statement.NO_GENERATED_KEYS);
        }

        public void prepareInsert(SinkRow row) {
            if (row.getOp() != Data.Op.INSERT) {
                throw Status.FAILED_PRECONDITION
                        .withDescription("unexpected op type: " + row.getOp())
                        .asRuntimeException();
            }
            try {
                jdbcDialect.bindInsertIntoStatement(insertStatement, conn, tableSchema, row);
                insertStatement.addBatch();
                this.cnt++;
            } catch (SQLException e) {
                throw io.grpc.Status.INTERNAL
                        .withDescription(
                                String.format(
                                        ERROR_REPORT_TEMPLATE, e.getSQLState(), e.getMessage()))
                        .withCause(e)
                        .asRuntimeException();
            }
        }

        public void tryExecute() throws SQLException {
            if (this.batchInsertRows > 0 && this.cnt >= this.batchInsertRows) {
                this.execute();
            }
        }

        public void execute() throws SQLException {
            executeStatement(this.insertStatement);

            if (!conn.getAutoCommit()) {
                this.conn.commit();
            }

            this.cnt = 0;
        }

        @Override
        public void close() {
            closeStatement(this.insertStatement);
            // we don't close the connection here, because we don't own it
        }

        private void executeStatement(PreparedStatement stmt) throws SQLException {
            if (stmt == null) {
                return;
            }
            // if timeout occurs, a SQLTimeoutException will be thrown
            // and we will retry to write the stream chunk in `JDBCSink.write`
            stmt.setQueryTimeout(queryTimeoutSecs);
            LOG.debug("Executing statement: {}", stmt);
            stmt.executeBatch();
            stmt.clearParameters();
        }

        private void closeStatement(PreparedStatement stmt) {
            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException e) {
                LOG.error("unable to close the prepared stmt", e);
            }
        }

        private void setSnowflakeDatabaseAndSchema() throws SQLException {
            if (config.getDatabaseName() == null
                    || config.getDatabaseName().isEmpty()
                    || config.getSchemaName() == null
                    || config.getSchemaName().isEmpty()) {
                LOG.warn(
                        "Snowflake database or schema is not set, will not run USE DATABASE and USE SCHEMA");
                return;
            }

            String database =
                    config.getDatabaseName() != null && !config.getDatabaseName().isEmpty()
                            ? config.getDatabaseName()
                            : "DEMO";
            String schema =
                    config.getSchemaName() != null && !config.getSchemaName().isEmpty()
                            ? config.getSchemaName()
                            : "PUBLIC";
            // Set database if available
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("USE DATABASE " + quoteIdentifier(database));
                LOG.info("Set Snowflake database to: {}", database);
            }
            // Set schema if available
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("USE SCHEMA " + quoteIdentifier(schema));
                LOG.info("Set Snowflake schema to: {}", schema);
            }
        }

        private String quoteIdentifier(String identifier) {
            return "\"" + identifier + "\"";
        }
    }

    @Override
    public void beginEpoch(long epoch) {}

    @Override
    public Optional<ConnectorServiceProto.SinkMetadata> barrier(boolean isCheckpoint) {
        try {
            if (jdbcStatements != null) {
                jdbcStatements.execute();
            }
        } catch (SQLException e) {
            LOG.error("Error executing JDBC statements: {}", e.getMessage());
            throw Status.INTERNAL
                    .withDescription(
                            String.format(ERROR_REPORT_TEMPLATE, e.getSQLState(), e.getMessage()))
                    .withCause(e)
                    .asRuntimeException();
        }
        // Return a mock SinkMetadata with one empty byte field
        ConnectorServiceProto.SinkMetadata metadata =
                ConnectorServiceProto.SinkMetadata.newBuilder()
                        .setSerialized(
                                ConnectorServiceProto.SinkMetadata.SerializedMetadata.newBuilder()
                                        .setMetadata(com.google.protobuf.ByteString.EMPTY)
                                        .build())
                        .build();
        return Optional.of(metadata);
    }

    @Override
    public void drop() {
        if (jdbcStatements != null) {
            jdbcStatements.close();
        }

        try {
            if (conn != null) {
                if (!conn.getAutoCommit()) {
                    LOG.info("rollback the transaction");
                    conn.rollback();
                }
                conn.close();
            }
        } catch (SQLException e) {
            LOG.error("unable to close conn: %s", e);
        }
    }

    public String getTableName() {
        return config.getTableName();
    }

    public Connection getConn() {
        return conn;
    }
}
