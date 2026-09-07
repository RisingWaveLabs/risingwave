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

import com.risingwave.proto.ConnectorServiceProto;

/** Byte-array JNI facade for {@link OracleExternalTable}. */
public final class JniOracleExternalTable {
    private JniOracleExternalTable() {}

    public static byte[] discover(byte[] requestBytes) {
        return invoke(requestBytes, Operation.DISCOVER);
    }

    public static byte[] currentScn(byte[] requestBytes) {
        return invoke(requestBytes, Operation.CURRENT_SCN);
    }

    public static byte[] snapshotRead(byte[] requestBytes) {
        return invoke(requestBytes, Operation.SNAPSHOT_READ);
    }

    private static byte[] invoke(byte[] requestBytes, Operation operation) {
        try {
            var request = ConnectorServiceProto.OracleExternalTableRequest.parseFrom(requestBytes);
            var response =
                    switch (operation) {
                        case DISCOVER -> OracleExternalTable.discover(request);
                        case CURRENT_SCN -> OracleExternalTable.currentScn(request);
                        case SNAPSHOT_READ -> OracleExternalTable.snapshotRead(request);
                    };
            return response.toByteArray();
        } catch (Exception error) {
            var message = error.getMessage();
            if (message == null || message.isBlank()) {
                message = error.getClass().getSimpleName();
            }
            return ConnectorServiceProto.OracleExternalTableResponse.newBuilder()
                    .setError(
                            ConnectorServiceProto.ValidationError.newBuilder()
                                    .setErrorMessage(message))
                    .build()
                    .toByteArray();
        }
    }

    private enum Operation {
        DISCOVER,
        CURRENT_SCN,
        SNAPSHOT_READ
    }
}
