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

package com.risingwave.connector.source;

import com.risingwave.connector.source.common.MongoDbValidator;
import java.util.HashMap;
import org.junit.Ignore;
import org.junit.Test;

public class MongoDbValidatorTest {

    @Ignore // manual test
    @Test
    public void testValidate() {
        var userProps = new HashMap<String, String>();
        userProps.put("mongodb.url", "mongodb://rwcdc:123456@localhost:27017/?authSource=admin");
        userProps.put("collection.name", "test.users");
        MongoDbValidator validator = new MongoDbValidator(userProps, 1);
        validator.validateAll();
    }
}
