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

package io.debezium.connector.postgresql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

public class PostgresStreamingChangeEventSourceTest {

    @Test
    public void keepAliveMonitorReportsUnexpectedTaskCompletionWhenFailureIsSwallowed() {
        AtomicBoolean stopping = new AtomicBoolean(false);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        ExecutorService executor =
                new PostgresStreamingChangeEventSource.MonitoredKeepAliveExecutor(
                        new SameThreadExecutor(), stopping::get, failure::set);

        Future<?> future =
                executor.submit(
                        () -> {
                            try {
                                throw new RuntimeException("status update failed");
                            } catch (Exception ignored) {
                                return;
                            }
                        });

        assertTrue(future.isDone());
        assertTrue(failure.get() instanceof IllegalStateException);
        assertEquals("Keep-alive thread stopped unexpectedly", failure.get().getMessage());
    }

    @Test
    public void keepAliveMonitorIgnoresTaskCompletionDuringStop() {
        AtomicBoolean stopping = new AtomicBoolean(true);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        ExecutorService executor =
                new PostgresStreamingChangeEventSource.MonitoredKeepAliveExecutor(
                        new SameThreadExecutor(), stopping::get, failure::set);

        Future<?> future = executor.submit(() -> {});

        assertTrue(future.isDone());
        assertNull(failure.get());
    }

    @Test
    public void keepAliveMonitorUnwrapsFutureTaskFailure() {
        AtomicBoolean stopping = new AtomicBoolean(false);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        ExecutorService executor =
                new PostgresStreamingChangeEventSource.MonitoredKeepAliveExecutor(
                        new SameThreadExecutor(), stopping::get, failure::set);
        RuntimeException expected = new RuntimeException("status update failed");

        Future<?> future =
                executor.submit(
                        () -> {
                            throw expected;
                        });

        assertTrue(future.isDone());
        assertSame(expected, failure.get());
    }

    private static class SameThreadExecutor extends AbstractExecutorService {
        private boolean shutdown;

        @Override
        public void shutdown() {
            shutdown = true;
        }

        @Override
        public List<Runnable> shutdownNow() {
            shutdown = true;
            return Collections.emptyList();
        }

        @Override
        public boolean isShutdown() {
            return shutdown;
        }

        @Override
        public boolean isTerminated() {
            return shutdown;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return shutdown;
        }

        @Override
        public void execute(Runnable command) {
            command.run();
        }
    }
}
