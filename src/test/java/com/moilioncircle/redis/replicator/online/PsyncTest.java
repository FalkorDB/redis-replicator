/*
 * Copyright 2016-2018 Leon Chen
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

package com.moilioncircle.redis.replicator.online;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.slf4j.Logger;

import com.moilioncircle.redis.replicator.Configuration;
import com.moilioncircle.redis.replicator.RedisSocketReplicator;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.cmd.CommandName;
import com.moilioncircle.redis.replicator.cmd.impl.SetCommand;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.event.EventListener;
import com.moilioncircle.redis.replicator.event.PostRdbSyncEvent;
import com.moilioncircle.redis.replicator.event.PreRdbSyncEvent;
import com.moilioncircle.redis.replicator.rdb.datatype.KeyValuePair;
import com.moilioncircle.redis.replicator.util.Strings;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

/**
 * Tests the PSYNC reconnect and resume behavior of {@link RedisSocketReplicator}.
 *
 * <p>The test pre-populates the server with 100 keys, then starts a replicator and exercises
 * three disconnect/reconnect scenarios:
 * <ol>
 *   <li>Disconnects mid-RDB (during the initial full sync) by closing the socket as soon as the
 *       first {@link com.moilioncircle.redis.replicator.rdb.datatype.KeyValuePair} arrives.
 *       The replicator must initiate a second full sync, so {@code PreRdbSyncEvent} is expected
 *       to fire exactly twice.</li>
 *   <li>After the full sync completes ({@code PostRdbSyncEvent}), a background thread writes
 *       1500 SET commands ("psync 0".."psync 1499") to the server. The socket is closed after
 *       the 500th command is received, forcing a partial resync (PSYNC with the saved
 *       replication ID and offset).</li>
 *   <li>The socket is closed again after the 1010th command, triggering a second partial
 *       resync.</li>
 * </ol>
 * At the end the replicator cleanly closes after the 1500th "psync *" SET command, and the
 * test asserts that exactly 2 full syncs and exactly 1500 SET events were observed.
 *
 * @author Leon Chen
 * @since 2.1.0
 */
public class PsyncTest extends OnlineTestBase {

    @Test
    public void psync() throws IOException {
    
        try (Jedis jedis = new Jedis("127.0.0.1", 6380)) {
            jedis.auth("test");
            Pipeline pipeline = jedis.pipelined();
            for (int i = 0; i < 100; i++) {
                pipeline.set("pre-psync " + i, "pre-psync" + i);
            }
            pipeline.sync();
        }

        final Configuration configuration = config().
                setAuthPassword("test").
                setConnectionTimeout(3000).
                setReadTimeout(3000).
                setBufferSize(64).
                setAsyncCachedBytes(0).
                setHeartbeatPeriod(200).
                setReceiveBufferSize(0).
                setSendBufferSize(0).
                setRetryTimeInterval(1000).
                setUseDefaultExceptionListener(false);
        @SuppressWarnings("resource")
        TestRedisSocketReplicator r = new TestRedisSocketReplicator("127.0.0.1", 6380, configuration);
        // Fires once after PostRdbSyncEvent to disconnect and kick off the background writer.
        final AtomicBoolean postRdbDisconnected = new AtomicBoolean(false);
        // Count of received SET commands whose key starts with "psync".
        final AtomicInteger psyncSetCount = new AtomicInteger();
        // Fires when the first KV pair arrives during the RDB dump, closing the socket mid-transfer
        // so the replicator must restart with a second full sync.
        final AtomicBoolean midRdbDisconnected = new AtomicBoolean(false);
        // Number of PreRdbSyncEvent firings; expected to be 2 (initial + mid-RDB restart).
        final AtomicInteger fullSyncCount = new AtomicInteger();
        r.addEventListener(new EventListener() {
            @Override
            public void onEvent(Replicator replicator, Event event) {
                if (event instanceof PreRdbSyncEvent) {
                    r.getLogger().info("id:{}, offset:{}", configuration.getReplId(), configuration.getReplOffset());
                    fullSyncCount.incrementAndGet();
                }

                if (event instanceof KeyValuePair) {
                    if (midRdbDisconnected.compareAndSet(false, true)) {
                        // will trigger full sync at this time
                        r.getLogger().info("psync close 1");
                        close(replicator);
                    }
                }

                if (event instanceof PostRdbSyncEvent) {
                    if (postRdbDisconnected.compareAndSet(false, true)) {
                        close(replicator);
                        Thread thread = new Thread(new JRun());
                        thread.setDaemon(true);
                        thread.start();
                        replicator.removeCommandParser(CommandName.name("PING"));
                    }
                }
                if (event instanceof SetCommand && Strings.toString(((SetCommand) event).getKey()).startsWith("psync")) {
                    psyncSetCount.incrementAndGet();
                    if (psyncSetCount.get() == 500) {
                        //close current process port;
                        //that will auto trigger psync command
                        r.getLogger().info("psync close 2");
                        r.getLogger().info("id:{}, offset:{}", configuration.getReplId(), configuration.getReplOffset());
                        close(replicator);
                    }

                    if (psyncSetCount.get() == 1010) {
                        //close current process port;
                        //that will auto trigger psync command
                        r.getLogger().info("psync close 3");
                        r.getLogger().info("id:{}, offset:{}", configuration.getReplId(), configuration.getReplOffset());
                        close(replicator);
                    }
                    if (psyncSetCount.get() == 1480) {
                        configuration.setVerbose(true);
                    }
                    if (psyncSetCount.get() == 1500) {
                        try {
                            replicator.close();
                        } catch (IOException e) {
                        }
                    }
                }
            }
        });
        r.open();
        assertEquals(2, fullSyncCount.get());
        assertEquals(1500, psyncSetCount.get());
    }

    private static void close(Replicator replicator) {
        try {
            ((TestRedisSocketReplicator) replicator).getOutputStream().close();
        } catch (IOException e) {
        }
        try {
            ((TestRedisSocketReplicator) replicator).getInputStream().close();
        } catch (IOException e) {
        }
        try {
            ((TestRedisSocketReplicator) replicator).getSocket().close();
        } catch (IOException e) {
        }
    }

    private static class JRun implements Runnable {

        @Override
        public void run() {
            try (Jedis jedis = new Jedis("127.0.0.1", 6380)) {
                jedis.auth("test");
                Pipeline pipeline = jedis.pipelined();
                for (int i = 0; i < 1500; i++) {
                    pipeline.set("psync " + i, "psync" + i);
                }
                pipeline.sync();
            }
        }
    }

    private static class TestRedisSocketReplicator extends RedisSocketReplicator {

        public TestRedisSocketReplicator(String host, int port, Configuration configuration) {
            super(host, port, configuration);
        }
        
        public Logger getLogger() {
            return TestRedisSocketReplicator.logger;
        }

        public Socket getSocket() {
            return super.socket;
        }

        public InputStream getInputStream() {
            return super.inputStream;
        }

        public OutputStream getOutputStream() {
            return super.outputStream;
        }
    }

}
