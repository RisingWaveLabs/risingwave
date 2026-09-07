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

package com.risingwave.connector.api.source;

import com.risingwave.proto.ConnectorServiceProto;

public enum SourceTypeE {
    MYSQL,
    POSTGRES,
    CITUS,
    MONGODB,
    SQL_SERVER,
    ORACLE,
    INVALID;

    public static SourceTypeE valueOf(ConnectorServiceProto.SourceType type) {
        switch (type) {
            case MYSQL:
                return SourceTypeE.MYSQL;
            case POSTGRES:
                return SourceTypeE.POSTGRES;
            case CITUS:
                return SourceTypeE.CITUS;
            case MONGODB:
                return SourceTypeE.MONGODB;
            case SQL_SERVER:
                return SourceTypeE.SQL_SERVER;
            case ORACLE:
                return SourceTypeE.ORACLE;
            default:
                return SourceTypeE.INVALID;
        }
    }
}
