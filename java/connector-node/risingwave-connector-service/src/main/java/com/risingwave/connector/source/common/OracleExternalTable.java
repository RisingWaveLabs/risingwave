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

import com.google.protobuf.ByteString;
import com.risingwave.connector.api.source.SourceTypeE;
import com.risingwave.proto.ConnectorServiceProto;
import com.risingwave.proto.Data;
import com.risingwave.proto.Data.DataType.TypeName;
import com.risingwave.proto.PlanCommon;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

/** Pull-based Oracle catalog and flashback snapshot operations used by the Rust CDC backfill. */
final class OracleExternalTable {
    private static final Pattern ORACLE_UNQUOTED_IDENTIFIER =
            Pattern.compile("[A-Za-z][A-Za-z0-9_$#]*");

    private OracleExternalTable() {}

    static ConnectorServiceProto.OracleExternalTableResponse discover(
            ConnectorServiceProto.OracleExternalTableRequest request) throws SQLException {
        try (var connection = connect(request.getPropertiesMap())) {
            var schemaName = propertyIdentifier(request, DbzConnectorConfig.ORACLE_SCHEMA_NAME);
            var tableName = propertyIdentifier(request, DbzConnectorConfig.TABLE_NAME);
            var columns = discoverColumns(connection, schemaName, tableName);
            if (columns.isEmpty()) {
                throw new SQLException(
                        String.format(
                                "Oracle table '%s.%s' has no visible columns",
                                schemaName, tableName));
            }

            var columnIndices = new HashMap<String, Integer>();
            var tableSchema = ConnectorServiceProto.TableSchema.newBuilder();
            for (int i = 0; i < columns.size(); i++) {
                var column = columns.get(i);
                columnIndices.put(column.name(), i);
                tableSchema.addColumns(
                        PlanCommon.ColumnDesc.newBuilder()
                                .setName(column.name())
                                .setColumnType(column.dataType())
                                .setNullable(column.nullable())
                                .build());
            }

            for (var primaryKey : discoverPrimaryKeys(connection, schemaName, tableName)) {
                var index = columnIndices.get(primaryKey);
                if (index == null) {
                    throw new SQLException(
                            String.format(
                                    "Oracle primary-key column '%s' is not a visible table column",
                                    primaryKey));
                }
                tableSchema.addPkIndices(index);
            }
            if (tableSchema.getPkIndicesCount() == 0) {
                throw new SQLException(
                        String.format(
                                "Oracle table '%s.%s' doesn't define a primary key",
                                schemaName, tableName));
            }

            return ConnectorServiceProto.OracleExternalTableResponse.newBuilder()
                    .setTableSchema(tableSchema)
                    .build();
        }
    }

    static ConnectorServiceProto.OracleExternalTableResponse currentScn(
            ConnectorServiceProto.OracleExternalTableRequest request) throws SQLException {
        try (var connection = connect(request.getPropertiesMap());
                var statement = connection.createStatement();
                var result = statement.executeQuery("SELECT CURRENT_SCN FROM V$DATABASE")) {
            if (!result.next()) {
                throw new SQLException(
                        "Oracle returned no row for SELECT CURRENT_SCN FROM V$DATABASE");
            }
            var scn = result.getLong(1);
            if (scn <= 0) {
                throw new SQLException("Oracle returned an invalid current SCN: " + scn);
            }
            return ConnectorServiceProto.OracleExternalTableResponse.newBuilder()
                    .setSnapshotScn(scn)
                    .build();
        }
    }

    static ConnectorServiceProto.OracleExternalTableResponse snapshotRead(
            ConnectorServiceProto.OracleExternalTableRequest request) throws SQLException {
        if (!request.hasTableSchema()) {
            throw new SQLException(
                    "Oracle snapshot request is missing its RisingWave table schema");
        }
        if (request.getSnapshotScn() <= 0) {
            throw new SQLException("Oracle snapshot request has an invalid snapshot SCN");
        }
        if (request.getLimit() <= 0) {
            throw new SQLException("Oracle snapshot request has an invalid limit");
        }

        var schemaName = propertyIdentifier(request, DbzConnectorConfig.ORACLE_SCHEMA_NAME);
        var tableName = propertyIdentifier(request, DbzConnectorConfig.TABLE_NAME);
        var tableSchema = request.getTableSchema();
        var columnNames =
                tableSchema.getColumnsList().stream().map(PlanCommon.ColumnDesc::getName).toList();
        validatePrimaryKeys(request, tableSchema);
        var sql =
                buildSnapshotSql(
                        columnNames,
                        schemaName,
                        tableName,
                        request.getSnapshotScn(),
                        request.getPrimaryKeysList(),
                        request.getStartPkCount() > 0,
                        request.getLimit());

        var response = ConnectorServiceProto.OracleExternalTableResponse.newBuilder();
        try (var connection = connect(request.getPropertiesMap());
                var statement = connection.prepareStatement(sql)) {
            if (request.getStartPkCount() > 0) {
                bindStartPrimaryKey(statement, request, tableSchema);
            }
            try (var result = statement.executeQuery()) {
                while (result.next()) {
                    response.addRows(readRow(result, tableSchema));
                }
            }
        }
        return response.build();
    }

    private static Connection connect(Map<String, String> properties) throws SQLException {
        var pdbName =
                OracleValidator.normalizePdbName(
                        properties.get(DbzConnectorConfig.ORACLE_PDB_NAME));
        var jdbcUrl =
                ValidatorUtils.getJdbcUrl(
                        SourceTypeE.ORACLE,
                        properties.get(DbzConnectorConfig.HOST),
                        properties.get(DbzConnectorConfig.PORT),
                        properties.get(DbzConnectorConfig.DB_NAME));
        var connection =
                DriverManager.getConnection(
                        jdbcUrl,
                        properties.get(DbzConnectorConfig.USER),
                        properties.get(DbzConnectorConfig.PASSWORD));
        try (var statement = connection.createStatement()) {
            statement.execute("ALTER SESSION SET CONTAINER = " + pdbName);
            statement.execute("ALTER SESSION SET TIME_ZONE = 'UTC'");
        } catch (SQLException error) {
            connection.close();
            throw error;
        }
        return connection;
    }

    private static List<OracleColumn> discoverColumns(
            Connection connection, String schemaName, String tableName) throws SQLException {
        var columns = new ArrayList<OracleColumn>();
        try (var statement =
                connection.prepareStatement(
                        "SELECT COLUMN_NAME, DATA_TYPE, DATA_PRECISION, DATA_SCALE, NULLABLE "
                                + "FROM ALL_TAB_COLUMNS "
                                + "WHERE OWNER = ? AND TABLE_NAME = ? AND HIDDEN_COLUMN = 'NO' "
                                + "ORDER BY COLUMN_ID")) {
            statement.setString(1, schemaName);
            statement.setString(2, tableName);
            try (var result = statement.executeQuery()) {
                while (result.next()) {
                    var precision = nullableInteger(result, "DATA_PRECISION");
                    var scale = nullableInteger(result, "DATA_SCALE");
                    columns.add(
                            new OracleColumn(
                                    result.getString("COLUMN_NAME"),
                                    oracleTypeToRisingWaveType(
                                            result.getString("DATA_TYPE"), precision, scale),
                                    "Y".equals(result.getString("NULLABLE"))));
                }
            }
        }
        return columns;
    }

    private static List<String> discoverPrimaryKeys(
            Connection connection, String schemaName, String tableName) throws SQLException {
        var primaryKeys = new ArrayList<String>();
        try (var statement =
                connection.prepareStatement(
                        "SELECT cols.COLUMN_NAME "
                                + "FROM ALL_CONSTRAINTS cons "
                                + "JOIN ALL_CONS_COLUMNS cols "
                                + "ON cons.OWNER = cols.OWNER "
                                + "AND cons.CONSTRAINT_NAME = cols.CONSTRAINT_NAME "
                                + "WHERE cons.OWNER = ? AND cons.TABLE_NAME = ? "
                                + "AND cons.CONSTRAINT_TYPE = 'P' AND cons.STATUS = 'ENABLED' "
                                + "ORDER BY cols.POSITION")) {
            statement.setString(1, schemaName);
            statement.setString(2, tableName);
            try (var result = statement.executeQuery()) {
                while (result.next()) {
                    primaryKeys.add(result.getString(1));
                }
            }
        }
        return primaryKeys;
    }

    static Data.DataType oracleTypeToRisingWaveType(
            String rawType, Integer precision, Integer scale) throws SQLException {
        var oracleType = rawType.toUpperCase(Locale.ROOT);
        var typeName =
                switch (oracleType) {
                    case "BINARY_FLOAT" -> TypeName.FLOAT;
                    case "BINARY_DOUBLE" -> TypeName.DOUBLE;
                        // Oracle FLOAT and its aliases are NUMBER subtypes with binary precision,
                        // not
                        // IEEE-754 values. Preserve their decimal semantics.
                    case "FLOAT", "REAL", "DOUBLE PRECISION" -> TypeName.DECIMAL;
                    case "NUMBER", "DECIMAL", "NUMERIC", "INTEGER", "INT", "SMALLINT" ->
                            oracleNumberType(precision, scale);
                    case "CHAR",
                                    "NCHAR",
                                    "VARCHAR",
                                    "VARCHAR2",
                                    "NVARCHAR2",
                                    "CLOB",
                                    "NCLOB",
                                    "LONG",
                                    "ROWID",
                                    "UROWID",
                                    "XMLTYPE" ->
                            TypeName.VARCHAR;
                    case "RAW", "LONG RAW", "BLOB" -> TypeName.BYTEA;
                    case "JSON" -> TypeName.JSONB;
                    case "DATE" -> TypeName.TIMESTAMP;
                    default -> {
                        if (oracleType.startsWith("TIMESTAMP")) {
                            yield oracleType.contains("TIME ZONE")
                                    ? TypeName.TIMESTAMPTZ
                                    : TypeName.TIMESTAMP;
                        }
                        throw new SQLException("Unsupported Oracle data type: " + rawType);
                    }
                };
        return Data.DataType.newBuilder().setTypeName(typeName).build();
    }

    private static TypeName oracleNumberType(Integer precision, Integer scale) {
        if (precision == null || precision <= 0 || scale == null || scale > 0) {
            return TypeName.DECIMAL;
        }
        var integerDigits = precision - scale;
        if (integerDigits < 5) {
            return TypeName.INT16;
        }
        if (integerDigits < 10) {
            return TypeName.INT32;
        }
        if (integerDigits < 19) {
            return TypeName.INT64;
        }
        return TypeName.DECIMAL;
    }

    static String buildSnapshotSql(
            List<String> columns,
            String schemaName,
            String tableName,
            long scn,
            List<String> primaryKeys,
            boolean hasStartPrimaryKey,
            int limit)
            throws SQLException {
        if (columns.isEmpty() || primaryKeys.isEmpty()) {
            throw new SQLException("Oracle snapshot query requires columns and primary keys");
        }
        if (scn <= 0) {
            throw new SQLException("Oracle snapshot query has an invalid SCN");
        }
        if (limit <= 0) {
            throw new SQLException("Oracle snapshot query has an invalid limit");
        }
        var fields = columns.stream().map(OracleExternalTable::quoteIdentifier).toList();
        var keys = primaryKeys.stream().map(OracleExternalTable::quoteIdentifier).toList();
        var sql =
                new StringBuilder("SELECT ")
                        .append(String.join(",", fields))
                        .append(" FROM ")
                        .append(quoteIdentifier(schemaName))
                        .append(".")
                        .append(quoteIdentifier(tableName))
                        .append(" AS OF SCN ")
                        .append(scn);
        if (hasStartPrimaryKey) {
            sql.append(" WHERE ").append(buildPrimaryKeyFilter(keys));
        }
        return sql.append(" ORDER BY ")
                .append(String.join(",", keys))
                .append(" FETCH FIRST ")
                .append(limit)
                .append(" ROWS ONLY")
                .toString();
    }

    private static String buildPrimaryKeyFilter(List<String> quotedPrimaryKeys) {
        var disjunction = new ArrayList<String>();
        for (int greaterIndex = 0; greaterIndex < quotedPrimaryKeys.size(); greaterIndex++) {
            var conjunction = new ArrayList<String>();
            for (int equalIndex = 0; equalIndex < greaterIndex; equalIndex++) {
                conjunction.add(quotedPrimaryKeys.get(equalIndex) + " = ?");
            }
            conjunction.add(quotedPrimaryKeys.get(greaterIndex) + " > ?");
            disjunction.add("(" + String.join(" AND ", conjunction) + ")");
        }
        return String.join(" OR ", disjunction);
    }

    private static void validatePrimaryKeys(
            ConnectorServiceProto.OracleExternalTableRequest request,
            ConnectorServiceProto.TableSchema tableSchema)
            throws SQLException {
        if (request.getPrimaryKeysCount() != tableSchema.getPkIndicesCount()) {
            throw new SQLException(
                    "Oracle snapshot primary-key names and indices have different lengths");
        }
        if (request.getStartPkCount() != 0
                && request.getStartPkCount() != request.getPrimaryKeysCount()) {
            throw new SQLException("Oracle snapshot start key has the wrong number of values");
        }
        for (int i = 0; i < request.getPrimaryKeysCount(); i++) {
            var schemaIndex = tableSchema.getPkIndices(i);
            if (schemaIndex >= tableSchema.getColumnsCount()) {
                throw new SQLException("Oracle snapshot primary-key index is outside the schema");
            }
            var schemaName = tableSchema.getColumns(schemaIndex).getName();
            if (!normalizeIdentifier(request.getPrimaryKeys(i), "primary key")
                    .equals(normalizeIdentifier(schemaName, "primary key"))) {
                throw new SQLException(
                        String.format(
                                "Oracle snapshot primary key '%s' does not match schema column '%s'",
                                request.getPrimaryKeys(i), schemaName));
            }
        }
    }

    private static void bindStartPrimaryKey(
            PreparedStatement statement,
            ConnectorServiceProto.OracleExternalTableRequest request,
            ConnectorServiceProto.TableSchema tableSchema)
            throws SQLException {
        var parameterIndex = 1;
        for (int greaterIndex = 0; greaterIndex < request.getPrimaryKeysCount(); greaterIndex++) {
            for (int keyIndex = 0; keyIndex <= greaterIndex; keyIndex++) {
                var columnIndex = tableSchema.getPkIndices(keyIndex);
                bindDatum(
                        statement,
                        parameterIndex++,
                        request.getStartPk(keyIndex),
                        tableSchema.getColumns(columnIndex).getColumnType().getTypeName());
            }
        }
    }

    private static void bindDatum(
            PreparedStatement statement,
            int parameterIndex,
            ConnectorServiceProto.OracleDatum datum,
            TypeName typeName)
            throws SQLException {
        if (datum.getIsNull()) {
            throw new SQLException("Oracle snapshot primary-key value cannot be NULL");
        }
        if (typeName == TypeName.BYTEA) {
            statement.setBytes(parameterIndex, datum.getValue().toByteArray());
            return;
        }

        var text = datum.getValue().toString(StandardCharsets.UTF_8);
        switch (typeName) {
            case INT16, INT32, INT64 -> statement.setLong(parameterIndex, Long.parseLong(text));
            case FLOAT -> statement.setFloat(parameterIndex, Float.parseFloat(text));
            case DOUBLE -> statement.setDouble(parameterIndex, Double.parseDouble(text));
            case DECIMAL -> statement.setBigDecimal(parameterIndex, new BigDecimal(text));
            case BOOLEAN -> statement.setBoolean(parameterIndex, parseBoolean(text));
            case DATE -> statement.setObject(parameterIndex, java.time.LocalDate.parse(text));
            case TIMESTAMP ->
                    statement.setObject(
                            parameterIndex, java.time.LocalDateTime.parse(text.replace(' ', 'T')));
            case TIMESTAMPTZ ->
                    statement.setObject(
                            parameterIndex, OffsetDateTime.parse(text.replace(' ', 'T')));
            case VARCHAR, JSONB -> statement.setString(parameterIndex, text);
            default ->
                    throw new SQLException(
                            "Unsupported RisingWave primary-key type for Oracle snapshot: "
                                    + typeName);
        }
    }

    private static ConnectorServiceProto.OracleRow readRow(
            ResultSet result, ConnectorServiceProto.TableSchema tableSchema) throws SQLException {
        var row = ConnectorServiceProto.OracleRow.newBuilder();
        for (int index = 0; index < tableSchema.getColumnsCount(); index++) {
            var typeName = tableSchema.getColumns(index).getColumnType().getTypeName();
            row.addValues(readDatum(result, index + 1, typeName));
        }
        return row.build();
    }

    private static ConnectorServiceProto.OracleDatum readDatum(
            ResultSet result, int jdbcIndex, TypeName typeName) throws SQLException {
        byte[] value;
        if (typeName == TypeName.BYTEA) {
            value = result.getBytes(jdbcIndex);
        } else {
            String text =
                    switch (typeName) {
                        case BOOLEAN -> Boolean.toString(result.getBoolean(jdbcIndex));
                        case DECIMAL -> {
                            var decimal = result.getBigDecimal(jdbcIndex);
                            yield decimal == null ? null : decimal.toPlainString();
                        }
                        case DATE -> {
                            var date = result.getDate(jdbcIndex);
                            yield date == null ? null : date.toLocalDate().toString();
                        }
                        case TIMESTAMP -> {
                            var timestamp = result.getTimestamp(jdbcIndex);
                            yield timestamp == null ? null : timestamp.toLocalDateTime().toString();
                        }
                        case TIMESTAMPTZ -> readTimestampWithTimeZone(result, jdbcIndex);
                        default -> result.getString(jdbcIndex);
                    };
            value = text == null ? null : text.getBytes(StandardCharsets.UTF_8);
        }
        if (value == null || result.wasNull()) {
            return ConnectorServiceProto.OracleDatum.newBuilder().setIsNull(true).build();
        }
        return ConnectorServiceProto.OracleDatum.newBuilder()
                .setValue(ByteString.copyFrom(value))
                .build();
    }

    private static String readTimestampWithTimeZone(ResultSet result, int jdbcIndex)
            throws SQLException {
        try {
            var value = result.getObject(jdbcIndex, OffsetDateTime.class);
            return value == null ? null : value.toString();
        } catch (SQLException unsupportedConversion) {
            var value = result.getTimestamp(jdbcIndex);
            return value == null
                    ? null
                    : value.toLocalDateTime().atOffset(ZoneOffset.UTC).toString();
        }
    }

    private static boolean parseBoolean(String value) throws SQLException {
        return switch (value) {
            case "t", "true", "TRUE", "1" -> true;
            case "f", "false", "FALSE", "0" -> false;
            default -> throw new SQLException("Invalid RisingWave boolean value: " + value);
        };
    }

    private static Integer nullableInteger(ResultSet result, String column) throws SQLException {
        var value = result.getInt(column);
        return result.wasNull() ? null : value;
    }

    private static String propertyIdentifier(
            ConnectorServiceProto.OracleExternalTableRequest request, String property) {
        return normalizeIdentifier(request.getPropertiesMap().get(property), property);
    }

    private static String quoteIdentifier(String identifier) {
        return '"' + normalizeIdentifier(identifier, "Oracle identifier") + '"';
    }

    private static String normalizeIdentifier(String identifier, String description) {
        if (identifier == null || !ORACLE_UNQUOTED_IDENTIFIER.matcher(identifier).matches()) {
            throw new IllegalArgumentException(
                    description + " must be an unquoted Oracle identifier");
        }
        return identifier.toUpperCase(Locale.ROOT);
    }

    private record OracleColumn(String name, Data.DataType dataType, boolean nullable) {}
}
