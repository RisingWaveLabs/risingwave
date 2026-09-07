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

package com.risingwave.connector.cdc.debezium.converters;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.connect.data.SchemaBuilder;

/** Converts PostgreSQL composite values (and arrays of composites) into plain strings. */
public class PgCompositeToStringConverter
        implements CustomConverter<SchemaBuilder, RelationalColumn> {

    // PG FirstNormalObjectId: OIDs >= this are user-defined types.
    // Built-in array types (e.g. _int4, _text) sit below this threshold and go
    // through Debezium's standard handlers untouched.
    private static final int PG_FIRST_USER_OID = 16384;

    // Must match `DEBEZIUM_UNAVAILABLE_VALUE` on the Rust side
    // (src/common/src/types/mod.rs).
    private static final String UNAVAILABLE_VALUE_PLACEHOLDER = "__debezium_unavailable_value";

    private boolean debug;

    @Override
    public void configure(Properties props) {
        debug = Boolean.parseBoolean(props.getProperty("debug", "false"));
    }

    @Override
    public void converterFor(
            RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        if (debug) {
            System.err.printf(
                    "PgCompositeToStringConverter field=%s.%s jdbcType=%d nativeType=%d typeName=%s typeExpression=%s optional=%s%n",
                    column.dataCollection(),
                    column.name(),
                    column.jdbcType(),
                    column.nativeType(),
                    column.typeName(),
                    column.typeExpression(),
                    column.isOptional());
        }

        if (column.jdbcType() == Types.STRUCT) {
            // Always optional: PG DELETE before-image fills non-PK columns
            // with null under REPLICA IDENTITY DEFAULT, regardless of
            // upstream NOT NULL.
            registration.register(SchemaBuilder.string().optional(), this::convertScalar);
            return;
        }
        if (isUserDefinedArray(column)) {
            var elementSchema = SchemaBuilder.string().optional().build();
            registration.register(
                    SchemaBuilder.array(elementSchema).optional(), this::convertArray);
        }
    }

    private static boolean isUserDefinedArray(RelationalColumn column) {
        // ARRAY columns whose element OID is user-defined are treated as
        // arrays of composites. Built-in arrays (int[], text[], ...) have OIDs
        // below PG_FIRST_USER_OID and are skipped. User-defined enum/domain
        // arrays will also match and get rendered as text — acceptable
        // since the downstream column is varchar[] anyway.
        //
        // Do not intercept extension arrays that Debezium already knows how to
        // encode with a native schema. For example, PostGIS geometry[] arrives
        // as an array of geometry structs and RisingWave decodes each element
        // as EWKB bytea. Re-registering it as array(string) would make the
        // downstream bytea parser treat hex EWKB text as base64.
        return column.jdbcType() == Types.ARRAY
                && column.nativeType() >= PG_FIRST_USER_OID
                && !isDebeziumNativeExtensionArray(column);
    }

    private static boolean isDebeziumNativeExtensionArray(RelationalColumn column) {
        return isPostgresArrayOf(column, "geometry") || isPostgresArrayOf(column, "geography");
    }

    private static boolean isPostgresArrayOf(RelationalColumn column, String elementTypeName) {
        return isPostgresArrayTypeName(column.typeName(), elementTypeName)
                || isPostgresArrayTypeExpression(column.typeExpression(), elementTypeName);
    }

    private static boolean isPostgresArrayTypeName(String typeName, String elementTypeName) {
        if (typeName == null) {
            return false;
        }
        var normalized = unquoteAndUnqualify(typeName.toLowerCase());
        return normalized.equals("_" + elementTypeName)
                || normalized.equals(elementTypeName + "[]");
    }

    private static boolean isPostgresArrayTypeExpression(
            String typeExpression, String elementTypeName) {
        if (typeExpression == null) {
            return false;
        }
        var normalized = typeExpression.toLowerCase().replace("\"", "").trim();
        var unqualified = unquoteAndUnqualify(normalized);
        return unqualified.equals("_" + elementTypeName)
                || unqualified.equals(elementTypeName + "[]")
                || (unqualified.startsWith(elementTypeName + "(") && unqualified.endsWith("[]"));
    }

    private static String unquoteAndUnqualify(String typeName) {
        var unquoted = typeName.replace("\"", "").trim();
        var dot = unquoted.lastIndexOf('.');
        return dot >= 0 ? unquoted.substring(dot + 1) : unquoted;
    }

    private String convertScalar(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        if (value instanceof ByteBuffer buffer) {
            return bufferToString(buffer);
        }
        return value.toString();
    }

    private List<String> convertArray(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof List<?> list) {
            // Pre-decoded form (rare for unknown composite arrays, but handle it).
            var out = new ArrayList<String>(list.size());
            for (Object el : list) {
                out.add(elementToString(el));
            }
            return out;
        }
        if (isDebeziumUnavailableValueObject(value)) {
            return List.of(UNAVAILABLE_VALUE_PLACEHOLDER);
        }
        // Under include.unknown.datatypes=true, Debezium hands us the raw
        // PG textual array form `{"(a,b)","(c,d)"}` as bytes / string.
        // Split it into individual element strings.
        return parsePgTextArray(elementToString(value));
    }

    /**
     * Minimal PG textual-array parser, sufficient for composite arrays. Recognizes the standard
     * form {@code "{elem,elem,...}"} where each element is either NULL, a bare token, or a
     * double-quoted string with {@code \\} and {@code \"} escapes.
     */
    private static List<String> parsePgTextArray(String s) {
        if (s == null || s.length() < 2 || s.charAt(0) != '{' || s.charAt(s.length() - 1) != '}') {
            return List.of();
        }
        var out = new ArrayList<String>();
        int i = 1;
        int end = s.length() - 1;
        while (i < end) {
            if (s.charAt(i) == '"') {
                i++;
                var sb = new StringBuilder();
                while (i < end && s.charAt(i) != '"') {
                    if (s.charAt(i) == '\\' && i + 1 < end) {
                        sb.append(s.charAt(i + 1));
                        i += 2;
                    } else {
                        sb.append(s.charAt(i));
                        i++;
                    }
                }
                i++; // consume closing "
                out.add(sb.toString());
            } else {
                int start = i;
                while (i < end && s.charAt(i) != ',') {
                    i++;
                }
                var raw = s.substring(start, i);
                out.add("NULL".equals(raw) ? null : raw);
            }
            if (i < end && s.charAt(i) == ',') {
                i++;
            }
        }
        return out;
    }

    private static boolean isDebeziumUnavailableValueObject(Object value) {
        // Debezium's unchanged-TOAST sentinel for non-STRING schema columns is
        // a bare java.lang.Object singleton. Normal composite array values
        // arrive as concrete subclasses (byte[] / ByteBuffer / String / ...).
        return value != null && value.getClass() == Object.class;
    }

    private static String elementToString(Object el) {
        if (el == null) {
            return null;
        }
        if (isDebeziumUnavailableValueObject(el)) {
            return UNAVAILABLE_VALUE_PLACEHOLDER;
        }
        if (el instanceof byte[] eb) {
            return new String(eb, StandardCharsets.UTF_8);
        }
        if (el instanceof ByteBuffer ebb) {
            return bufferToString(ebb);
        }
        return el.toString();
    }

    private static String bufferToString(ByteBuffer buffer) {
        var readOnly = buffer.asReadOnlyBuffer();
        var bytes = new byte[readOnly.remaining()];
        readOnly.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
