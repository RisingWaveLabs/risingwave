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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class MySqlValidatorTest {
    @Test
    public void testBinlogMonitorSatisfiesReplicationClient() {
        assertTrue(
                MySqlValidator.isPrivilegeGranted(
                        "REPLICATION CLIENT",
                        "GRANT SELECT, REPLICATION SLAVE, BINLOG MONITOR ON *.* TO `rw`@`%`"));
    }

    @Test
    public void testOtherPrivilegesDoNotSatisfyReplicationClient() {
        assertFalse(
                MySqlValidator.isPrivilegeGranted(
                        "REPLICATION CLIENT",
                        "GRANT SELECT, REPLICATION SLAVE ON *.* TO `rw`@`%`"));
    }
}
