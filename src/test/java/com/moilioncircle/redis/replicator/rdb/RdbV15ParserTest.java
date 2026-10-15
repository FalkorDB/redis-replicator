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

package com.moilioncircle.redis.replicator.rdb;

import static com.moilioncircle.redis.replicator.Constants.*;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.moilioncircle.redis.replicator.Configuration;
import com.moilioncircle.redis.replicator.FileType;
import com.moilioncircle.redis.replicator.Flavor;
import com.moilioncircle.redis.replicator.RedisReplicator;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.event.PostRdbSyncEvent;
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.rdb.datatype.ContextKeyValuePair;
import com.moilioncircle.redis.replicator.rdb.datatype.ExpiredType;
import com.moilioncircle.redis.replicator.rdb.datatype.KeyStringValueArray;
import com.moilioncircle.redis.replicator.rdb.datatype.KeyStringValueString;
import com.moilioncircle.redis.replicator.rdb.datatype.KeyValuePair;
import com.moilioncircle.redis.replicator.rdb.datatype.Stream;
import com.moilioncircle.redis.replicator.rdb.dump.DumpRdbVisitor;
import com.moilioncircle.redis.replicator.rdb.dump.datatype.DumpKeyValuePair;
import com.moilioncircle.redis.replicator.rdb.dump.parser.DefaultDumpValueParser;
import com.moilioncircle.redis.replicator.rdb.iterable.ValueIterableRdbVisitor;
import com.moilioncircle.redis.replicator.rdb.skip.SkipRdbValueVisitor;
import com.moilioncircle.redis.replicator.rdb.skip.SkipRdbVisitor;
import com.moilioncircle.redis.replicator.util.Strings;

/**
 * Parses dump-v15.rdb, written by redis 8.10 with gen-dump-v15.sh.
 */
public class RdbV15ParserTest {

    private static Map<String, KeyValuePair<?, ?>> parse(Replicator r) throws IOException {
        Map<String, KeyValuePair<?, ?>> kvs = new HashMap<>();
        AtomicBoolean done = new AtomicBoolean();
        r.addEventListener((rep, event) -> {
            if (event instanceof KeyValuePair) {
                KeyValuePair<?, ?> kv = (KeyValuePair<?, ?>) event;
                kvs.put(Strings.toString(kv.getKey()), kv);
            }
            if (event instanceof PostRdbSyncEvent) done.set(true);
        });
        r.open();
        assertTrue(done.get());
        return kvs;
    }

    private static Replicator replicator() {
        return new RedisReplicator(RdbV15ParserTest.class.getClassLoader().getResourceAsStream("dump-v15.rdb"), FileType.RDB, Configuration.defaultSetting());
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> hash(KeyValuePair<?, ?> kv) {
        Map<String, String> map = new HashMap<>();
        for (Map.Entry<byte[], byte[]> e : ((Map<byte[], byte[]>) kv.getValue()).entrySet()) {
            map.put(Strings.toString(e.getKey()), Strings.toString(e.getValue()));
        }
        return map;
    }

    @SuppressWarnings("unchecked")
    private static Map<Long, String> array(KeyValuePair<?, ?> kv) {
        Map<Long, String> array = new java.util.LinkedHashMap<>();
        for (Map.Entry<Long, byte[]> e : ((NavigableMap<Long, byte[]>) kv.getValue()).entrySet()) {
            array.put(e.getKey(), Strings.toString(e.getValue()));
        }
        return array;
    }

    private static Map<String, Map<String, String>> entries(KeyValuePair<?, ?> kv) {
        Map<String, Map<String, String>> entries = new java.util.LinkedHashMap<>();
        for (Stream.Entry entry : ((Stream) kv.getValue()).getEntries().values()) {
            Map<String, String> fields = new HashMap<>();
            for (Map.Entry<byte[], byte[]> f : entry.getFields().entrySet()) {
                fields.put(Strings.toString(f.getKey()), Strings.toString(f.getValue()));
            }
            entries.put(entry.getId().toString(), fields);
        }
        return entries;
    }

    @Test
    public void testDefaultVisitor() throws IOException {
        Map<String, KeyValuePair<?, ?>> kvs = parse(replicator());
        assertEquals(13, kvs.size());

        // template hashes
        KeyValuePair<?, ?> tl1 = kvs.get("tl1");
        assertEquals(RDB_TYPE_HASH_TMPL_LP_REF, tl1.getValueRdbType());
        Map<String, String> h = hash(tl1);
        assertEquals(3, h.size());
        assertEquals("n1", h.get("name"));
        assertEquals("1", h.get("age"));
        assertEquals("c1", h.get("city"));
        assertEquals("c2", hash(kvs.get("tl2")).get("city"));

        KeyValuePair<?, ?> ta2 = kvs.get("ta2");
        assertEquals(RDB_TYPE_HASH_TMPL_ARRAY_REF, ta2.getValueRdbType());
        h = hash(ta2);
        assertEquals(300, h.size());
        String pad = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";
        assertEquals("v2-7-" + pad, h.get("f7"));
        assertEquals("v2-300-" + pad, h.get("f300"));
        assertEquals("one", hash(kvs.get("hplain")).get("only"));

        // array
        KeyValuePair<?, ?> arr = kvs.get("arr");
        assertTrue(arr instanceof KeyStringValueArray);
        assertEquals(RDB_TYPE_ARRAY, arr.getValueRdbType());
        @SuppressWarnings("unchecked")
        NavigableMap<Long, byte[]> array = (NavigableMap<Long, byte[]>) arr.getValue();
        assertEquals(5, array.size());
        assertEquals("hello", Strings.toString(array.get(0L)));
        assertEquals("1", Strings.toString(array.get(1L)));
        assertEquals("3.5", Strings.toString(array.get(2L)));
        assertEquals("a much longer string value that is not a small string", Strings.toString(array.get(3L)));
        assertEquals("tail", Strings.toString(array.get(1000L)));

        // streams
        KeyValuePair<?, ?> s1 = kvs.get("s1");
        assertEquals(RDB_TYPE_STREAM_LISTPACKS_5, s1.getValueRdbType());
        Stream stream = (Stream) s1.getValue();
        assertEquals(3, stream.getLength());
        Stream.Group group = stream.getGroups().get(0);
        assertEquals("g1", Strings.toString(group.getName()));
        assertEquals(3, group.getPendingEntries().size());
        assertEquals(2, group.getConsumers().get(0).getPendingEntries().size());
        // the NACKed entry has no consumer
        assertNull(group.getPendingEntries().get(new Stream.ID(1, 1)).getConsumer());
        assertEquals(2, ((Stream) kvs.get("s2").getValue()).getLength());

        // plain types after the new ones
        assertEquals("v", Strings.toString((byte[]) kvs.get("str").getValue()));
        assertEquals(ExpiredType.MS, kvs.get("ttl").getExpiredType());
        assertEquals(3, ((java.util.List<?>) kvs.get("list").getValue()).size());
        assertEquals(2, ((java.util.Set<?>) kvs.get("zset").getValue()).size());
    }

    @Test
    public void testSkipVisitor() throws IOException {
        Replicator r = replicator();
        r.setRdbVisitor(new SkipRdbVisitor(r, new SkipRdbValueVisitor(r)));
        assertEquals(0, parse(r).size());
    }

    @Test
    public void testValueIterableVisitor() throws IOException {
        Replicator r = replicator();
        r.setRdbVisitor(new ValueIterableRdbVisitor(r));
        Map<String, Integer> sizes = new HashMap<>();
        AtomicBoolean done = new AtomicBoolean();
        r.addEventListener((rep, event) -> {
            if (event instanceof KeyValuePair) {
                // iterators read from the rdb stream, so they must be drained here
                KeyValuePair<?, ?> kv = (KeyValuePair<?, ?>) event;
                int size = -1;
                if (kv.getValue() instanceof Iterator) {
                    size = 0;
                    for (Iterator<?> it = (Iterator<?>) kv.getValue(); it.hasNext(); it.next()) size++;
                }
                sizes.put(Strings.toString(kv.getKey()), size);
            }
            if (event instanceof PostRdbSyncEvent) done.set(true);
        });
        r.open();
        assertTrue(done.get());
        assertEquals(13, sizes.size());
        assertEquals(Integer.valueOf(300), sizes.get("ta1"));
        assertEquals(Integer.valueOf(3), sizes.get("tl1"));
    }

    @Test
    public void testDumpVisitor() throws IOException {
        Map<String, KeyValuePair<?, ?>> expected = parse(replicator());
        for (int version : new int[] {-1, 15, 14, 13, 11, 9}) {
            Replicator r = replicator();
            boolean arrays = version == -1 || version >= 14;
            if (arrays) {
                r.setRdbVisitor(new DumpRdbVisitor(r, version));
            } else {
                // arrays can't be dumped for these targets, see testDumpArrayForOldTarget
                r.setRdbVisitor(new DumpRdbVisitor(r, version) {
                    @Override
                    public Event applyArray(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
                        return new SkipRdbVisitor(r, new SkipRdbValueVisitor(r)).applyArray(in, version, context);
                    }
                });
            }
            Map<String, KeyValuePair<?, ?>> dumps = parse(r);
            assertEquals(arrays ? 13 : 12, dumps.size());
            DefaultDumpValueParser parser = new DefaultDumpValueParser(r);
            for (Map.Entry<String, KeyValuePair<?, ?>> e : dumps.entrySet()) {
                DumpKeyValuePair dump = (DumpKeyValuePair) e.getValue();
                byte[] payload = dump.getValue();
                // the payload footer carries the dump version
                assertEquals(version == -1 ? 15 : version, payload[payload.length - 10]);
                assertEquals(dump.getValueRdbType(), payload[0]);
                KeyValuePair<?, ?> kv = parser.parse(dump);
                KeyValuePair<?, ?> want = expected.get(e.getKey());
                if (want instanceof KeyStringValueArray) {
                    assertEquals(array(want), array(kv));
                } else if (want.getValue() instanceof Map) {
                    assertEquals(hash(want), hash(kv));
                } else if (want.getValue() instanceof Stream) {
                    assertEquals(((Stream) want.getValue()).getLength(), ((Stream) kv.getValue()).getLength());
                    assertEquals(entries(want), entries(kv));
                }
            }
            // RESTORE rejects the RDB-file-only *_REF forms
            assertEquals(RDB_TYPE_HASH, dumps.get("tl1").getValueRdbType());
            assertEquals(RDB_TYPE_HASH, dumps.get("ta1").getValueRdbType());
            int streamType = dumps.get("s1").getValueRdbType();
            if (version == -1 || version >= 14) assertEquals(RDB_TYPE_STREAM_LISTPACKS_5, streamType);
            else if (version == 13) assertEquals(RDB_TYPE_STREAM_LISTPACKS_4, streamType);
            else if (version >= 11) assertEquals(RDB_TYPE_STREAM_LISTPACKS_3, streamType);
            else assertEquals(RDB_TYPE_STREAM_LISTPACKS, streamType);

            Stream.Group group = ((Stream) parser.parse((DumpKeyValuePair) dumps.get("s1")).getValue()).getGroups().get(0);
            if (version == -1 || version >= 14) {
                assertEquals(3, group.getPendingEntries().size());
            } else {
                // redis before 8.8 expects every pending entry to have a consumer, so the NACKed one is dropped
                assertEquals(2, group.getPendingEntries().size());
                for (Stream.Nack nack : group.getPendingEntries().values()) assertNotNull(nack.getConsumer());
                assertEquals(2, group.getConsumers().get(0).getPendingEntries().size());
            }
        }
    }

    @Test
    public void testDumpArrayForOldTarget() throws IOException {
        Replicator r = replicator();
        r.setRdbVisitor(new DumpRdbVisitor(r, 13));
        try {
            r.open();
            fail();
        } catch (UnsupportedOperationException e) {
            assertTrue(e.getMessage().contains("RDB_TYPE_ARRAY"));
        }
    }

    @Test
    public void testArraySerializable() throws Exception {
        KeyValuePair<?, ?> arr = parse(replicator()).get("arr");
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(arr);
        }
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            assertEquals(array(arr), array((KeyValuePair<?, ?>) in.readObject()));
        }
    }

    @Test
    public void testKeyMeta() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write("REDIS0015".getBytes());
        out.write(new byte[] {(byte) RDB_OPCODE_SELECTDB, 0});
        // key with metadata: 1 class, 4 byte class spec, UINT 5, EOF
        out.write(new byte[] {(byte) RDB_OPCODE_KEY_META, 1, 'k', 'm', 'e', 't', RDB_MODULE_OPCODE_UINT, 5, RDB_MODULE_OPCODE_EOF});
        out.write(new byte[] {RDB_TYPE_STRING, 2, 'k', '1', 1, 'a'});
        // key with an expire, then metadata: 1 class, STRING "xy", EOF
        out.write(new byte[] {(byte) RDB_OPCODE_EXPIRETIME_MS, 0, 0, 0, 0, 0, 0, 0, 1});
        out.write(new byte[] {(byte) RDB_OPCODE_KEY_META, 1, 'k', 'm', 'e', 't', RDB_MODULE_OPCODE_STRING, 2, 'x', 'y', RDB_MODULE_OPCODE_EOF});
        out.write(new byte[] {RDB_TYPE_STRING, 2, 'k', '2', 1, 'b'});
        out.write(new byte[] {RDB_TYPE_STRING, 2, 'k', '3', 1, 'c'});
        out.write(RDB_OPCODE_EOF);
        out.write(new byte[8]);

        Replicator r = new RedisReplicator(new ByteArrayInputStream(out.toByteArray()), FileType.RDB, Configuration.defaultSetting());
        Map<String, KeyValuePair<?, ?>> kvs = parse(r);
        assertEquals(3, kvs.size());
        assertTrue(kvs.get("k1") instanceof KeyStringValueString);
        assertArrayEquals("a".getBytes(), (byte[]) kvs.get("k1").getValue());
        assertArrayEquals("b".getBytes(), (byte[]) kvs.get("k2").getValue());
        assertEquals(ExpiredType.MS, kvs.get("k2").getExpiredType());
        assertArrayEquals("c".getBytes(), (byte[]) kvs.get("k3").getValue());

        r = new RedisReplicator(new ByteArrayInputStream(out.toByteArray()), FileType.RDB, Configuration.defaultSetting());
        r.setRdbVisitor(new SkipRdbVisitor(r, new SkipRdbValueVisitor(r)));
        AtomicInteger keys = new AtomicInteger();
        r.addEventListener((rep, event) -> {
            if (event instanceof KeyValuePair) keys.incrementAndGet();
        });
        r.open();
        assertEquals(0, keys.get());
    }

    @Test
    public void testListPackWithUnknownCount() throws IOException {
        // 65535 means the count doesn't fit in 16 bits, so the entries are read up to lp-end
        byte[] lp = {11, 0, 0, 0, (byte) 0xFF, (byte) 0xFF, 5, 1, 7, 1, (byte) 0xFF};
        java.util.List<byte[]> entries = DefaultRdbValueVisitor.listPackEntries(lp);
        assertEquals(2, entries.size());
        assertEquals("5", Strings.toString(entries.get(0)));
        assertEquals("7", Strings.toString(entries.get(1)));

        // counts of 32768 and more are unsigned
        ByteArrayOutputStream big = new ByteArrayOutputStream();
        int count = 40000;
        int total = 4 + 2 + count * 2 + 1;
        big.write(new byte[] {(byte) total, (byte) (total >> 8), (byte) (total >> 16), 0, (byte) count, (byte) (count >> 8)});
        for (int i = 0; i < count; i++) big.write(new byte[] {1, 1});
        big.write(0xFF);
        assertEquals(count, DefaultRdbValueVisitor.listPackEntries(big.toByteArray()).size());
    }

    @Test
    public void testResolveRdbVersion() {
        assertEquals(12, Flavor.REDIS.resolveRdbVersion("8.4.0"));
        assertEquals(13, Flavor.REDIS.resolveRdbVersion("8.6.2"));
        assertEquals(14, Flavor.REDIS.resolveRdbVersion("8.8.3"));
        assertEquals(15, Flavor.REDIS.resolveRdbVersion("8.10.2"));
        // newer releases than the table knows
        assertEquals(15, Flavor.REDIS.resolveRdbVersion("8.12.0"));
        assertTrue(Flavor.REDIS.isValidRdbVersion(15));
        assertNotNull(Flavor.REDIS);
    }
}
