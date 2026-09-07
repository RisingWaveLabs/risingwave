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

import java.util.Locale;
import java.util.regex.Pattern;

final class OracleHeartbeatTable {
    static final String ID_COLUMN = "ID";
    static final String HEARTBEAT_COLUMN = "HEARTBEAT";

    private static final Pattern ORACLE_UNQUOTED_IDENTIFIER =
            Pattern.compile("[A-Za-z][A-Za-z0-9_$#]*");

    private final String owner;
    private final String table;

    private OracleHeartbeatTable(String owner, String table) {
        this.owner = owner;
        this.table = table;
    }

    static OracleHeartbeatTable parse(String value) {
        if (value == null) {
            throw invalidName();
        }

        var parts = value.split("\\.", -1);
        if (parts.length != 2
                || !ORACLE_UNQUOTED_IDENTIFIER.matcher(parts[0]).matches()
                || !ORACLE_UNQUOTED_IDENTIFIER.matcher(parts[1]).matches()) {
            throw invalidName();
        }

        return new OracleHeartbeatTable(
                parts[0].toUpperCase(Locale.ROOT), parts[1].toUpperCase(Locale.ROOT));
    }

    private static RuntimeException invalidName() {
        return ValidatorUtils.invalidArgument(
                "'heartbeat.table.name' must use the unquoted Oracle identifier format "
                        + "'<schema>.<table>'");
    }

    String owner() {
        return owner;
    }

    String table() {
        return table;
    }

    String qualifiedName() {
        return owner + "." + table;
    }

    String actionQuery() {
        return String.format(
                "UPDATE %s SET %s = CASE %s WHEN 0 THEN 1 ELSE 0 END WHERE %s = 1",
                qualifiedName(), HEARTBEAT_COLUMN, HEARTBEAT_COLUMN, ID_COLUMN);
    }
}
