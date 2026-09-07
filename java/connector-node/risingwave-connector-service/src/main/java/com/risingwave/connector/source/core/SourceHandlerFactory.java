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

package com.risingwave.connector.source.core;

import com.risingwave.connector.api.source.SourceHandler;
import com.risingwave.connector.api.source.SourceTypeE;
import com.risingwave.connector.source.common.DbzConnectorConfig;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SourceHandlerFactory {
    static final Logger LOG = LoggerFactory.getLogger(SourceHandlerFactory.class);

    public static SourceHandler createSourceHandler(
            SourceTypeE source,
            long sourceId,
            String startOffset,
            Map<String, String> userProps,
            boolean snapshotDone,
            boolean isCdcSourceJob) {
        var config =
                new DbzConnectorConfig(
                        source, sourceId, startOffset, userProps, snapshotDone, isCdcSourceJob);
        return new DbzSourceHandler(config);
    }
}
