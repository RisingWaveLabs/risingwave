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
import static org.junit.Assert.assertThrows;

import com.risingwave.proto.ConnectorServiceProto;
import com.risingwave.proto.Data.DataType.TypeName;
import java.sql.SQLException;
import java.util.List;
import org.junit.Test;

public class OracleExternalTableTest {
    @Test
    public void mapsOracleTypesUsingDebeziumCompatibleWidths() throws Exception {
        assertType(TypeName.INT16, "NUMBER", 4, 0);
        assertType(TypeName.INT32, "NUMBER", 9, 0);
        assertType(TypeName.INT64, "NUMBER", 18, 0);
        assertType(TypeName.DECIMAL, "NUMBER", 19, 0);
        assertType(TypeName.INT32, "NUMBER", 7, -2);
        assertType(TypeName.DECIMAL, "NUMBER", null, null);
        assertType(TypeName.DECIMAL, "NUMBER", 0, 0);
        assertType(TypeName.DECIMAL, "NUMBER", 10, 2);
        assertType(TypeName.FLOAT, "BINARY_FLOAT", null, null);
        assertType(TypeName.DOUBLE, "BINARY_DOUBLE", null, null);
        assertType(TypeName.DECIMAL, "FLOAT", 126, null);
        assertType(TypeName.DECIMAL, "DOUBLE PRECISION", 126, null);
        assertType(TypeName.VARCHAR, "VARCHAR2", null, null);
        assertType(TypeName.VARCHAR, "LONG", null, null);
        assertType(TypeName.BYTEA, "RAW", null, null);
        assertType(TypeName.BYTEA, "LONG RAW", null, null);
        assertType(TypeName.TIMESTAMP, "DATE", null, null);
        assertType(TypeName.TIMESTAMP, "TIMESTAMP(6)", null, 6);
        assertType(TypeName.TIMESTAMPTZ, "TIMESTAMP(6) WITH TIME ZONE", null, 6);
        assertType(TypeName.JSONB, "JSON", null, null);

        assertThrows(
                SQLException.class,
                () -> OracleExternalTable.oracleTypeToRisingWaveType("BOOLEAN", null, null));
        assertThrows(
                SQLException.class,
                () ->
                        OracleExternalTable.oracleTypeToRisingWaveType(
                                "INTERVAL DAY TO SECOND", null, null));
    }

    @Test
    public void buildsPrimaryKeyOrderedFlashbackQuery() throws Exception {
        assertEquals(
                "SELECT \"ID\",\"REGION\",\"VALUE\" FROM \"APP\".\"ORDERS\" AS OF SCN 42 "
                        + "ORDER BY \"ID\",\"REGION\" FETCH FIRST 128 ROWS ONLY",
                OracleExternalTable.buildSnapshotSql(
                        List.of("id", "region", "value"),
                        "app",
                        "orders",
                        42,
                        List.of("id", "region"),
                        false,
                        128));
        assertEquals(
                "SELECT \"ID\",\"REGION\",\"VALUE\" FROM \"APP\".\"ORDERS\" AS OF SCN 42 "
                        + "WHERE (\"ID\" > ?) OR (\"ID\" = ? AND \"REGION\" > ?) "
                        + "ORDER BY \"ID\",\"REGION\" FETCH FIRST 128 ROWS ONLY",
                OracleExternalTable.buildSnapshotSql(
                        List.of("id", "region", "value"),
                        "app",
                        "orders",
                        42,
                        List.of("id", "region"),
                        true,
                        128));
    }

    @Test
    public void rejectsNegativeSnapshotScnAndLimit() {
        var validBaseRequest =
                ConnectorServiceProto.OracleExternalTableRequest.newBuilder()
                        .setTableSchema(ConnectorServiceProto.TableSchema.getDefaultInstance())
                        .setSnapshotScn(1)
                        .setLimit(1);

        assertThrows(
                SQLException.class,
                () ->
                        OracleExternalTable.snapshotRead(
                                validBaseRequest.clone().setSnapshotScn(-1).build()));
        assertThrows(
                SQLException.class,
                () ->
                        OracleExternalTable.snapshotRead(
                                validBaseRequest.clone().setLimit(-1).build()));
    }

    private static void assertType(
            TypeName expected, String oracleType, Integer precision, Integer scale)
            throws SQLException {
        assertEquals(
                expected,
                OracleExternalTable.oracleTypeToRisingWaveType(oracleType, precision, scale)
                        .getTypeName());
    }
}
