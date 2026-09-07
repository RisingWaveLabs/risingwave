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

package com.risingwave.connector.source;

import static org.assertj.core.api.Assertions.assertThat;

import com.risingwave.connector.api.source.SourceTypeE;
import com.risingwave.connector.source.common.DbzConnectorConfig;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.Test;

public class MongoDbConnectorConfigTest {

    @Test
    public void testLiteralCollectionListInfersDatabaseListAndMatchMode() {
        var properties =
                resolvedProperties(
                        "Sales_db.Orders-2026, archive-db.events_2, Sales_db.Other", Map.of());

        assertThat(properties)
                .containsEntry("database.include.list", "Sales_db,archive-db")
                .containsEntry(
                        "collection.include.list",
                        "Sales_db.Orders-2026, archive-db.events_2, Sales_db.Other")
                .containsEntry("filters.match.mode", "literal");
    }

    @Test
    public void testLiteralCollectionListSupportsLeadingUnderscores() {
        var properties = resolvedProperties("_internal._events", Map.of());

        assertThat(properties)
                .containsEntry("database.include.list", "_internal")
                .containsEntry("collection.include.list", "_internal._events")
                .containsEntry("filters.match.mode", "literal");
    }

    @Test
    public void testNonLiteralCollectionListsAreNotInferred() {
        for (var collectionList :
                new String[] {
                    "db[12][.]events_.*",
                    "db.collection.more",
                    "db.collection,",
                    "db.collection,,other.events",
                    " db.collection",
                    "db.collection ",
                    "db.collection\t,other.events",
                    "1db.collection",
                    "db.$collection"
                }) {
            var properties = resolvedProperties(collectionList, Map.of());

            assertThat(properties)
                    .describedAs("resolved properties for collection list %s", collectionList)
                    .doesNotContainKey("database.include.list")
                    .doesNotContainKey("filters.match.mode")
                    .containsEntry("collection.include.list", collectionList);
        }
    }

    @Test
    public void testDebeziumDatabaseIncludeListDisablesInference() {
        var properties =
                resolvedProperties(
                        "db1.events", Map.of("debezium.database.include.list", "explicit_db"));

        assertThat(properties)
                .containsEntry("database.include.list", "explicit_db")
                .doesNotContainKey("filters.match.mode");
    }

    @Test
    public void testDebeziumDatabaseExcludeListDisablesInference() {
        var properties =
                resolvedProperties(
                        "db1.events", Map.of("debezium.database.exclude.list", "legacy_.*"));

        assertThat(properties)
                .doesNotContainKey("database.include.list")
                .containsEntry("database.exclude.list", "legacy_.*")
                .doesNotContainKey("filters.match.mode");
    }

    @Test
    public void testDebeziumMatchModeDisablesInference() {
        for (var matchMode : new String[] {"regex", "literal", "invalid"}) {
            var properties =
                    resolvedProperties(
                            "db1.events", Map.of("debezium.filters.match.mode", matchMode));

            assertThat(properties)
                    .doesNotContainKey("database.include.list")
                    .containsEntry("filters.match.mode", matchMode);
        }
    }

    @Test
    public void testCollectionNameKeepsPrecedenceForHistoricalProperties() {
        var properties =
                resolvedProperties(
                        "db1.events",
                        Map.of("debezium.collection.include.list", "legacy_db.events"));

        assertThat(properties)
                .containsEntry("database.include.list", "db1")
                .containsEntry("collection.include.list", "db1.events")
                .containsEntry("filters.match.mode", "literal");
    }

    private static Map<String, String> resolvedProperties(
            String collectionName, Map<String, String> additionalProperties) {
        var userProperties = new HashMap<String, String>();
        userProperties.put("mongodb.url", "mongodb://localhost:27017/?replicaSet=rs0");
        userProperties.put("collection.name", collectionName);
        userProperties.putAll(additionalProperties);

        var config =
                new DbzConnectorConfig(SourceTypeE.MONGODB, 1, null, userProperties, false, false);
        return config.getResolvedDebeziumProps().entrySet().stream()
                .collect(
                        Collectors.toMap(
                                entry -> entry.getKey().toString(),
                                entry -> entry.getValue().toString()));
    }
}
