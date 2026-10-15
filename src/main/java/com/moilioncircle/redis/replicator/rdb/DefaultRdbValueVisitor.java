
package com.moilioncircle.redis.replicator.rdb;

import static com.moilioncircle.redis.replicator.Constants.*;
import static com.moilioncircle.redis.replicator.rdb.BaseRdbParser.StringHelper.listPackEntry;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moilioncircle.redis.replicator.Constants;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.io.RedisInputStream;
import com.moilioncircle.redis.replicator.rdb.datatype.Function;
import com.moilioncircle.redis.replicator.rdb.datatype.Module;
import com.moilioncircle.redis.replicator.rdb.datatype.Stream;
import com.moilioncircle.redis.replicator.rdb.datatype.TTLValue;
import com.moilioncircle.redis.replicator.rdb.datatype.ZSetEntry;
import com.moilioncircle.redis.replicator.rdb.module.ModuleParser;
import com.moilioncircle.redis.replicator.rdb.skip.SkipRdbParser;
import com.moilioncircle.redis.replicator.util.ByteArray;
import com.moilioncircle.redis.replicator.util.ByteArrayList;
import com.moilioncircle.redis.replicator.util.ByteArrayMap;
import com.moilioncircle.redis.replicator.util.ByteArraySet;
import com.moilioncircle.redis.replicator.util.Strings;
import com.moilioncircle.redis.replicator.util.TTLByteArrayMap;

/**
 * @author Leon Chen
 * @since 3.1.0
 */
@SuppressWarnings({"unchecked", "deprecation"})
public class DefaultRdbValueVisitor extends RdbValueVisitor {

    protected static final Logger logger = LoggerFactory.getLogger(DefaultRdbValueVisitor.class);

    protected final Replicator replicator;

    /*
     * hash templates (since redis 8.10). an rdb file writes each template once, before the keys,
     * and RDB_TYPE_HASH_TMPL_*_REF hashes refer to it by id.
     */
    protected Map<Long, List<byte[]>> hashTemplates = new HashMap<>();

    public DefaultRdbValueVisitor(final Replicator replicator) {
        this.replicator = replicator;
    }
    
    @Override
    public <T> T applyFunction(RedisInputStream in, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        Function function = new Function();
        function.setName(parser.rdbLoadPlainStringObject().first());
        function.setEngineName(parser.rdbLoadPlainStringObject().first());
        long hasDesc = parser.rdbLoadLen().len;
        if (hasDesc == 1) {
            function.setDescription(parser.rdbLoadPlainStringObject().first());
        }
        function.setCode(parser.rdbLoadPlainStringObject().first());
        return (T) function;
    }
    
    @Override
    public <T> T applyFunction2(RedisInputStream in, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        Function function = new Function();
        function.setCode(parser.rdbLoadPlainStringObject().first());
        return (T) function;
    }

    @Override
    public <T> T applyString(RedisInputStream in, int version) throws IOException {
        /*
         * |       <content>       |
         * |    string contents    |
         */
        BaseRdbParser parser = new BaseRdbParser(in);

        byte[] val = parser.rdbLoadEncodedStringObject().first();
        return (T) val;
    }

    @Override
    public <T> T applyList(RedisInputStream in, int version) throws IOException {
        /*
         * |    <len>     |       <content>       |
         * | 1 or 5 bytes |    string contents    |
         */
        BaseRdbParser parser = new BaseRdbParser(in);

        long len = parser.rdbLoadLen().len;
        List<byte[]> list = new ByteArrayList();
        while (len > 0) {
            byte[] element = parser.rdbLoadEncodedStringObject().first();
            list.add(element);
            len--;
        }
        return (T) list;
    }

    @Override
    public <T> T applySet(RedisInputStream in, int version) throws IOException {
        /*
         * |    <len>     |       <content>       |
         * | 1 or 5 bytes |    string contents    |
         */
        BaseRdbParser parser = new BaseRdbParser(in);

        long len = parser.rdbLoadLen().len;
        Set<byte[]> set = new ByteArraySet();
        while (len > 0) {
            byte[] element = parser.rdbLoadEncodedStringObject().first();
            set.add(element);
            len--;
        }
        return (T) set;
    }
    
    @Override
    public <T> T applySetListPack(RedisInputStream in, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
    
        RedisInputStream listPack = new RedisInputStream(parser.rdbLoadPlainStringObject());
        Set<byte[]> set = new ByteArraySet();
        listPack.skip(4); // total-bytes
        int len = listPack.readInt(2);
        while (len > 0) {
            byte[] element = listPackEntry(listPack);
            set.add(element);
            len--;
        }
        int lpend = listPack.read(); // lp-end
        if (lpend != 255) {
            throw new AssertionError("listpack expect 255 but " + lpend);
        }
        return (T) set;
    }

    @Override
    public <T> T applyZSet(RedisInputStream in, int version) throws IOException {
        /*
         * |    <len>     |       <content>       |        <score>       |
         * | 1 or 5 bytes |    string contents    |    double content    |
         */
        BaseRdbParser parser = new BaseRdbParser(in);

        long len = parser.rdbLoadLen().len;
        Set<ZSetEntry> zset = new LinkedHashSet<>();
        while (len > 0) {
            byte[] element = parser.rdbLoadEncodedStringObject().first();
            double score = parser.rdbLoadDoubleValue();
            zset.add(new ZSetEntry(element, score));
            len--;
        }
        return (T) zset;
    }

    @Override
    public <T> T applyZSet2(RedisInputStream in, int version) throws IOException {
        /*
         * |    <len>     |       <content>       |        <score>       |
         * | 1 or 5 bytes |    string contents    |    binary double     |
         */
        BaseRdbParser parser = new BaseRdbParser(in);

        /* rdb version 8*/
        long len = parser.rdbLoadLen().len;
        Set<ZSetEntry> zset = new LinkedHashSet<>();
        while (len > 0) {
            byte[] element = parser.rdbLoadEncodedStringObject().first();
            double score = parser.rdbLoadBinaryDoubleValue();
            zset.add(new ZSetEntry(element, score));
            len--;
        }
        return (T) zset;
    }

    @Override
    public <T> T applyHash(RedisInputStream in, int version) throws IOException {
        /*
         * |    <len>     |       <content>       |
         * | 1 or 5 bytes |    string contents    |
         */
        BaseRdbParser parser = new BaseRdbParser(in);

        long len = parser.rdbLoadLen().len;
        ByteArrayMap map = new ByteArrayMap();
        while (len > 0) {
            byte[] field = parser.rdbLoadEncodedStringObject().first();
            byte[] value = parser.rdbLoadEncodedStringObject().first();
            map.put(field, value);
            len--;
        }
        return (T) map;
    }

    @Override
    public <T> T applyHashZipMap(RedisInputStream in, int version) throws IOException {
        /*
         * |<zmlen> |   <len>     |"foo"    |    <len>   | <free> |   "bar" |<zmend> |
         * | 1 byte | 1 or 5 byte | content |1 or 5 byte | 1 byte | content | 1 byte |
         */
        BaseRdbParser parser = new BaseRdbParser(in);

        RedisInputStream stream = new RedisInputStream(parser.rdbLoadPlainStringObject());
        ByteArrayMap map = new ByteArrayMap();
        BaseRdbParser.LenHelper.zmlen(stream); // zmlen
        while (true) {
            int zmEleLen = BaseRdbParser.LenHelper.zmElementLen(stream);
            if (zmEleLen == 255) {
                return (T) map;
            }
            byte[] field = BaseRdbParser.StringHelper.bytes(stream, zmEleLen);
            zmEleLen = BaseRdbParser.LenHelper.zmElementLen(stream);
            if (zmEleLen == 255) {
                //value is null
                map.put(field, null);
                return (T) map;
            }
            int free = BaseRdbParser.LenHelper.free(stream);
            byte[] value = BaseRdbParser.StringHelper.bytes(stream, zmEleLen);
            BaseRdbParser.StringHelper.skip(stream, free);
            map.put(field, value);
        }
    }

    @Override
    public <T> T applyListZipList(RedisInputStream in, int version) throws IOException {
        /*
         * |<zlbytes>| <zltail>| <zllen>| <entry> ...<entry> | <zlend>|
         * | 4 bytes | 4 bytes | 2bytes | zipListEntry ...   | 1byte  |
         */
        BaseRdbParser parser = new BaseRdbParser(in);

        RedisInputStream stream = new RedisInputStream(parser.rdbLoadPlainStringObject());
        List<byte[]> list = new ByteArrayList();
        BaseRdbParser.LenHelper.zlbytes(stream); // zlbytes
        BaseRdbParser.LenHelper.zltail(stream); // zltail
        int zllen = BaseRdbParser.LenHelper.zllen(stream);
        for (int i = 0; i < zllen; i++) {
            byte[] e = BaseRdbParser.StringHelper.zipListEntry(stream);
            list.add(e);
        }
        int zlend = BaseRdbParser.LenHelper.zlend(stream);
        if (zlend != 255) {
            throw new AssertionError("zlend expect 255 but " + zlend);
        }
        return (T) list;
    }

    @Override
    public <T> T applySetIntSet(RedisInputStream in, int version) throws IOException {
        /*
         * |<encoding>| <length-of-contents>|              <contents>                            |
         * | 4 bytes  |            4 bytes  | 2 bytes element| 4 bytes element | 8 bytes element |
         */
        BaseRdbParser parser = new BaseRdbParser(in);

        RedisInputStream stream = new RedisInputStream(parser.rdbLoadPlainStringObject());
        Set<byte[]> set = new ByteArraySet();
        int encoding = BaseRdbParser.LenHelper.encoding(stream);
        long lenOfContent = BaseRdbParser.LenHelper.lenOfContent(stream);
        for (long i = 0; i < lenOfContent; i++) {
            switch (encoding) {
                case 2:
                    String element = String.valueOf(stream.readInt(2));
                    set.add(element.getBytes());
                    break;
                case 4:
                    element = String.valueOf(stream.readInt(4));
                    set.add(element.getBytes());
                    break;
                case 8:
                    element = String.valueOf(stream.readLong(8));
                    set.add(element.getBytes());
                    break;
                default:
                    throw new AssertionError("expect encoding [2,4,8] but:" + encoding);
            }
        }
        return (T) set;
    }

    @Override
    public <T> T applyZSetZipList(RedisInputStream in, int version) throws IOException {
        /*
         * |<zlbytes>| <zltail>| <zllen>| <entry> ...<entry> | <zlend>|
         * | 4 bytes | 4 bytes | 2bytes | zipListEntry ...   | 1byte  |
         */
        BaseRdbParser parser = new BaseRdbParser(in);

        RedisInputStream stream = new RedisInputStream(parser.rdbLoadPlainStringObject());
        Set<ZSetEntry> zset = new LinkedHashSet<>();
        BaseRdbParser.LenHelper.zlbytes(stream); // zlbytes
        BaseRdbParser.LenHelper.zltail(stream); // zltail
        int zllen = BaseRdbParser.LenHelper.zllen(stream);
        while (zllen > 0) {
            byte[] element = BaseRdbParser.StringHelper.zipListEntry(stream);
            zllen--;
            double score = Double.valueOf(Strings.toString(BaseRdbParser.StringHelper.zipListEntry(stream)));
            zllen--;
            zset.add(new ZSetEntry(element, score));
        }
        int zlend = BaseRdbParser.LenHelper.zlend(stream);
        if (zlend != 255) {
            throw new AssertionError("zlend expect 255 but " + zlend);
        }
        return (T) zset;
    }
    
    @Override
    public <T> T applyZSetListPack(RedisInputStream in, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);

        RedisInputStream listPack = new RedisInputStream(parser.rdbLoadPlainStringObject());
        Set<ZSetEntry> zset = new LinkedHashSet<>();
        listPack.skip(4); // total-bytes
        int len = listPack.readInt(2);
        while (len > 0) {
            byte[] element = listPackEntry(listPack);
            len--;
            double score = Double.valueOf(Strings.toString(listPackEntry(listPack)));
            len--;
            zset.add(new ZSetEntry(element, score));
        }
        int lpend = listPack.read(); // lp-end
        if (lpend != 255) {
            throw new AssertionError("listpack expect 255 but " + lpend);
        }
        return (T) zset;
    }

    @Override
    public <T> T applyHashZipList(RedisInputStream in, int version) throws IOException {
        /*
         * |<zlbytes>| <zltail>| <zllen>| <entry> ...<entry> | <zlend>|
         * | 4 bytes | 4 bytes | 2bytes | zipListEntry ...   | 1byte  |
         */
        BaseRdbParser parser = new BaseRdbParser(in);

        RedisInputStream stream = new RedisInputStream(parser.rdbLoadPlainStringObject());
        ByteArrayMap map = new ByteArrayMap();
        BaseRdbParser.LenHelper.zlbytes(stream); // zlbytes
        BaseRdbParser.LenHelper.zltail(stream); // zltail
        int zllen = BaseRdbParser.LenHelper.zllen(stream);
        while (zllen > 0) {
            byte[] field = BaseRdbParser.StringHelper.zipListEntry(stream);
            zllen--;
            byte[] value = BaseRdbParser.StringHelper.zipListEntry(stream);
            zllen--;
            map.put(field, value);
        }
        int zlend = BaseRdbParser.LenHelper.zlend(stream);
        if (zlend != 255) {
            throw new AssertionError("zlend expect 255 but " + zlend);
        }
        return (T) map;
    }
    
    @Override
    public <T> T applyHashListPack(RedisInputStream in, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);

        RedisInputStream listPack = new RedisInputStream(parser.rdbLoadPlainStringObject());
        ByteArrayMap map = new ByteArrayMap();
        listPack.skip(4); // total-bytes
        int len = listPack.readInt(2);
        while (len > 0) {
            byte[] field = listPackEntry(listPack);
            len--;
            byte[] value = listPackEntry(listPack);
            len--;
            map.put(field, value);
        }
        int lpend = listPack.read(); // lp-end
        if (lpend != 255) {
            throw new AssertionError("listpack expect 255 but " + lpend);
        }
        return (T) map;
    }

    @Override
    public <T> T applyListQuickList(RedisInputStream in, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);

        long len = parser.rdbLoadLen().len;
        List<byte[]> list = new ByteArrayList();
        for (long i = 0; i < len; i++) {
            RedisInputStream stream = new RedisInputStream(parser.rdbGenericLoadStringObject(RDB_LOAD_NONE));

            BaseRdbParser.LenHelper.zlbytes(stream); // zlbytes
            BaseRdbParser.LenHelper.zltail(stream); // zltail
            int zllen = BaseRdbParser.LenHelper.zllen(stream);
            for (int j = 0; j < zllen; j++) {
                byte[] e = BaseRdbParser.StringHelper.zipListEntry(stream);
                list.add(e);
            }
            int zlend = BaseRdbParser.LenHelper.zlend(stream);
            if (zlend != 255) {
                throw new AssertionError("zlend expect 255 but " + zlend);
            }
        }
        return (T) list;
    }
    
    @Override
    public <T> T applyListQuickList2(RedisInputStream in, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);

        long len = parser.rdbLoadLen().len;
        List<byte[]> list = new ByteArrayList();
        for (long i = 0; i < len; i++) {
            long container = parser.rdbLoadLen().len;
            ByteArray bytes = parser.rdbLoadPlainStringObject();
            if (container == QUICKLIST_NODE_CONTAINER_PLAIN) {
                list.add(bytes.first());
            } else if (container == QUICKLIST_NODE_CONTAINER_PACKED) {
                RedisInputStream listPack = new RedisInputStream(bytes);
                listPack.skip(4); // total-bytes
                int innerLen = listPack.readInt(2);
                for (int j = 0; j < innerLen; j++) {
                    byte[] e = listPackEntry(listPack);
                    list.add(e);
                }
                int lpend = listPack.read(); // lp-end
                if (lpend != 255) {
                    throw new AssertionError("listpack expect 255 but " + lpend);
                }
            } else {
                throw new UnsupportedOperationException(String.valueOf(container));
            }
        }
        return (T) list;
    }
    
    @Override
    public <T> T applyHash2(RedisInputStream in, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        long len = parser.rdbLoadLen().len;
        TTLByteArrayMap map = new TTLByteArrayMap();
        while (len > 0) {
            byte[] field = parser.rdbLoadEncodedStringObject().first();
            byte[] value = parser.rdbLoadEncodedStringObject().first();
            long expiry = parser.rdbLoadMillisecondTime();
            map.put(field, new TTLValue(expiry == -1L ? null : expiry, value));
            len--;
        }
        return (T) map;
    }

    @Override
    public <T> T applyHashMetadata(RedisInputStream in, int version) throws IOException{
        BaseRdbParser parser = new BaseRdbParser(in);
        long minExpire = parser.rdbLoadMillisecondTime();
        long len = parser.rdbLoadLen().len;
        TTLByteArrayMap map = new TTLByteArrayMap();
        while (len > 0) {
            long ttl = parser.rdbLoadLen().len;
            byte[] field = parser.rdbLoadEncodedStringObject().first();
            byte[] value = parser.rdbLoadEncodedStringObject().first();
            map.put(field, new TTLValue(ttl != 0 ? ttl + minExpire - 1 : null, value));
            len--;
        }
        return (T) map;
    }
    
    @Override
    public <T> T applyHashListPackEx(RedisInputStream in, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        long minExpire = parser.rdbLoadMillisecondTime();
        RedisInputStream listPack = new RedisInputStream(parser.rdbLoadPlainStringObject());
        TTLByteArrayMap map = new TTLByteArrayMap();
        listPack.skip(4); // total-bytes
        int len = listPack.readInt(2);
        while (len > 0) {
            byte[] field = listPackEntry(listPack);
            len--;
            byte[] value = listPackEntry(listPack);
            len--;
            long ttl = Long.parseLong(Strings.toString(listPackEntry(listPack)));
            len--;
            map.put(field, new TTLValue(ttl != 0 ? ttl : null, value));
        }
        int lpend = listPack.read(); // lp-end
        if (lpend != 255) {
            throw new AssertionError("listpack expect 255 but " + lpend);
        }
        return (T) map;
    }

    @Override
    public <T> T applyModule(RedisInputStream in, int version) throws IOException {
        //|6|6|6|6|6|6|6|6|6|10|
        BaseRdbParser parser = new BaseRdbParser(in);

        char[] c = new char[9];
        long moduleid = parser.rdbLoadLen().len;
        for (int i = 0; i < c.length; i++) {
            c[i] = MODULE_SET[(int) (moduleid >>> (10 + (c.length - 1 - i) * 6) & 63)];
        }
        String moduleName = new String(c);
        int moduleVersion = (int) (moduleid & 1023);
        ModuleParser<? extends Module> moduleParser = lookupModuleParser(moduleName, moduleVersion);
        if (moduleParser == null) {
            throw new NoSuchElementException("module parser[" + moduleName + ", " + moduleVersion + "] not register. rdb type: [RDB_TYPE_MODULE]");
        }
        Module module = moduleParser.parse(in, 1);
        return (T) module;
    }

    @Override
    public <T> T applyModule2(RedisInputStream in, int version) throws IOException {
        //|6|6|6|6|6|6|6|6|6|10|
        BaseRdbParser parser = new BaseRdbParser(in);

        char[] c = new char[9];
        long moduleid = parser.rdbLoadLen().len;
        for (int i = 0; i < c.length; i++) {
            c[i] = MODULE_SET[(int) (moduleid >>> (10 + (c.length - 1 - i) * 6) & 63)];
        }
        String moduleName = new String(c);
        int moduleVersion = (int) (moduleid & 1023);
        ModuleParser<? extends Module> moduleParser = lookupModuleParser(moduleName, moduleVersion);
        Module module = null;
        if (moduleParser == null) {
            logger.warn("module parser[{}, {}] not register. rdb type: [RDB_TYPE_MODULE_2]. module parse skipped.", moduleName, moduleVersion);
            SkipRdbParser skipRdbParser = new SkipRdbParser(in);
            skipRdbParser.rdbLoadCheckModuleValue();
        } else {
            module = moduleParser.parse(in, 2);
            long eof = parser.rdbLoadLen().len;
            if (eof != RDB_MODULE_OPCODE_EOF) {
                throw new UnsupportedOperationException("The RDB file contains module data for the module '" + moduleName + "' that is not terminated by the proper module value EOF marker");
            }
        }
        return (T) module;
    }

    protected ModuleParser<? extends Module> lookupModuleParser(String moduleName, int moduleVersion) {
        return replicator.getModuleParser(moduleName, moduleVersion);
    }

    @Override
    public <T> T applyStreamListPacks(RedisInputStream in, int version) throws IOException {
        return applyStreamListPacks(in, version, Constants.RDB_TYPE_STREAM_LISTPACKS);
    }
    
    @Override
    public <T> T applyStreamListPacks2(RedisInputStream in, int version) throws IOException {
        return applyStreamListPacks(in, version, Constants.RDB_TYPE_STREAM_LISTPACKS_2);
    }
    
    @Override
    public <T> T applyStreamListPacks3(RedisInputStream in, int version) throws IOException {
        return applyStreamListPacks(in, version, Constants.RDB_TYPE_STREAM_LISTPACKS_3);
    }

    @Override
    public <T> T applyStreamListPacks4(RedisInputStream in, int version) throws IOException {
        return applyStreamListPacks(in, version, Constants.RDB_TYPE_STREAM_LISTPACKS_4);
    }

    @Override
    public <T> T applyStreamListPacks5(RedisInputStream in, int version) throws IOException {
        return applyStreamListPacks(in, version, Constants.RDB_TYPE_STREAM_LISTPACKS_5);
    }
    
    @SuppressWarnings("resource")
    protected <T> T applyStreamListPacks(RedisInputStream in, int version, int type) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
    
        Stream stream = new Stream();
    
        // Entries
        NavigableMap<Stream.ID, Stream.Entry> entries = new TreeMap<>(Stream.ID.COMPARATOR);
        long listPacks = parser.rdbLoadLen().len;
        while (listPacks-- > 0) {
            RedisInputStream rawId = new RedisInputStream(parser.rdbLoadPlainStringObject());
            Stream.ID baseId = new Stream.ID(rawId.readLong(8, false), rawId.readLong(8, false));
            RedisInputStream listPack = new RedisInputStream(parser.rdbLoadPlainStringObject());
            listPack.skip(4); // total-bytes
            listPack.skip(2); // num-elements
            /*
             * Master entry
             * +-------+---------+------------+---------+--/--+---------+---------+-+
             * | count | deleted | num-fields | field_1 | field_2 | ... | field_N |0|
             * +-------+---------+------------+---------+--/--+---------+---------+-+
             */
            long count = Long.parseLong(Strings.toString(listPackEntry(listPack))); // count
            long deleted = Long.parseLong(Strings.toString(listPackEntry(listPack))); // deleted
            int numFields = Integer.parseInt(Strings.toString(listPackEntry(listPack))); // num-fields
            byte[][] tempFields = new byte[numFields][];
            for (int i = 0; i < numFields; i++) {
                tempFields[i] = listPackEntry(listPack);
            }
            listPackEntry(listPack); // 0
        
            long total = count + deleted;
            while (total-- > 0) {
                Map<byte[], byte[]> fields = new ByteArrayMap();
                /*
                 * FLAG
                 * +-----+--------+
                 * |flags|entry-id|
                 * +-----+--------+
                 */
                int flag = Integer.parseInt(Strings.toString(listPackEntry(listPack)));
                long ms = Long.parseLong(Strings.toString(listPackEntry(listPack)));
                long seq = Long.parseLong(Strings.toString(listPackEntry(listPack)));
                Stream.ID id = baseId.delta(ms, seq);
                boolean delete = (flag & STREAM_ITEM_FLAG_DELETED) != 0;
                if ((flag & STREAM_ITEM_FLAG_SAMEFIELDS) != 0) {
                    /*
                     * SAMEFIELD
                     * +-------+-/-+-------+--------+
                     * |value-1|...|value-N|lp-count|
                     * +-------+-/-+-------+--------+
                     */
                    for (int i = 0; i < numFields; i++) {
                        byte[] value = listPackEntry(listPack);
                        byte[] field = tempFields[i];
                        fields.put(field, value);
                    }
                    entries.put(id, new Stream.Entry(id, delete, fields));
                } else {
                    /*
                     * NONEFIELD
                     * +----------+-------+-------+-/-+-------+-------+--------+
                     * |num-fields|field-1|value-1|...|field-N|value-N|lp-count|
                     * +----------+-------+-------+-/-+-------+-------+--------+
                     */
                    numFields = Integer.parseInt(Strings.toString(listPackEntry(listPack)));
                    for (int i = 0; i < numFields; i++) {
                        byte[] field = listPackEntry(listPack);
                        byte[] value = listPackEntry(listPack);
                        fields.put(field, value);
                    }
                    entries.put(id, new Stream.Entry(id, delete, fields));
                }
                listPackEntry(listPack); // lp-count
            }
            int lpend = listPack.read(); // lp-end
            if (lpend != 255) {
                throw new AssertionError("listpack expect 255 but " + lpend);
            }
        }
    
        long length = parser.rdbLoadLen().len;
        Stream.ID lastId = new Stream.ID(parser.rdbLoadLen().len, parser.rdbLoadLen().len);
    
        Stream.ID firstId = null;
        Stream.ID maxDeletedEntryId = null;
        Long entriesAdded = null;
        if (type >= Constants.RDB_TYPE_STREAM_LISTPACKS_2) {
            firstId = new Stream.ID(parser.rdbLoadLen().len, parser.rdbLoadLen().len);
            maxDeletedEntryId = new Stream.ID(parser.rdbLoadLen().len, parser.rdbLoadLen().len);
            entriesAdded = parser.rdbLoadLen().len;
        }
    
        // Group
        List<Stream.Group> groups = new ArrayList<>();
        long groupCount = parser.rdbLoadLen().len;
        while (groupCount-- > 0) {
            Stream.Group group = new Stream.Group();
            byte[] groupName = parser.rdbLoadPlainStringObject().first();
            Stream.ID groupLastId = new Stream.ID(parser.rdbLoadLen().len, parser.rdbLoadLen().len);
            Long entriesRead = null;
            if (type >= Constants.RDB_TYPE_STREAM_LISTPACKS_2) {
                entriesRead = parser.rdbLoadLen().len;
            }
            
            // Group PEL
            NavigableMap<Stream.ID, Stream.Nack> groupPendingEntries = new TreeMap<>(Stream.ID.COMPARATOR);
            long globalPel = parser.rdbLoadLen().len;
            while (globalPel-- > 0) {
                Stream.ID rawId = new Stream.ID(in.readLong(8, false), in.readLong(8, false));
                long deliveryTime = parser.rdbLoadMillisecondTime();
                long deliveryCount = parser.rdbLoadLen().len;
                groupPendingEntries.put(rawId, new Stream.Nack(rawId, null, deliveryTime, deliveryCount));
            }
        
            // Consumer
            List<Stream.Consumer> consumers = new ArrayList<>();
            long consumerCount = parser.rdbLoadLen().len;
            while (consumerCount-- > 0) {
                Stream.Consumer consumer = new Stream.Consumer();
                byte[] consumerName = parser.rdbLoadPlainStringObject().first();
                long seenTime = parser.rdbLoadMillisecondTime();
                long activeTime = -1;
                if (type >= Constants.RDB_TYPE_STREAM_LISTPACKS_3) {
                    activeTime = parser.rdbLoadMillisecondTime();
                }
            
                // Consumer PEL
                NavigableMap<Stream.ID, Stream.Nack> consumerPendingEntries = new TreeMap<>(Stream.ID.COMPARATOR);
                long pel = parser.rdbLoadLen().len;
                while (pel-- > 0) {
                    Stream.ID rawId = new Stream.ID(in.readLong(8, false), in.readLong(8, false));
                    Stream.Nack nack = groupPendingEntries.get(rawId);
                    nack.setConsumer(consumer);
                    consumerPendingEntries.put(rawId, nack);
                }
            
                consumer.setName(consumerName);
                consumer.setSeenTime(seenTime);
                if (type >= Constants.RDB_TYPE_STREAM_LISTPACKS_3) {
                    consumer.setActiveTime(activeTime);
                }
                consumer.setPendingEntries(consumerPendingEntries);
                consumers.add(consumer);
            }
        
            if (type >= Constants.RDB_TYPE_STREAM_LISTPACKS_5) {
                // XNACK zone: ids of group PEL entries that were released without a consumer
                long nacked = parser.rdbLoadLen().len;
                while (nacked-- > 0) {
                    in.skip(16);
                }
            }
        
            group.setName(groupName);
            group.setLastId(groupLastId);
            if (type >= Constants.RDB_TYPE_STREAM_LISTPACKS_2) {
                group.setEntriesRead(entriesRead);
            }
            group.setPendingEntries(groupPendingEntries);
            group.setConsumers(consumers);
            groups.add(group);
        }
    
        if (type >= Constants.RDB_TYPE_STREAM_LISTPACKS_4) {
            parser.rdbLoadLen(); // idmp_duration
            parser.rdbLoadLen(); // idmp_max_entries
            long producers = parser.rdbLoadLen().len;
            while (producers-- > 0) {
                parser.rdbLoadPlainStringObject(); // producer id
                long idmpEntries = parser.rdbLoadLen().len;
                while (idmpEntries-- > 0) {
                    parser.rdbLoadPlainStringObject(); // iid
                    parser.rdbLoadLen(); // stream id ms
                    parser.rdbLoadLen(); // stream id seq
                }
            }
            parser.rdbLoadLen(); // iids_added
            parser.rdbLoadLen(); // iids_duplicates
        }
    
        stream.setLastId(lastId);
        if (type >= Constants.RDB_TYPE_STREAM_LISTPACKS_2) {
            stream.setFirstId(firstId);
            stream.setMaxDeletedEntryId(maxDeletedEntryId);
            stream.setEntriesAdded(entriesAdded);
        }
        stream.setEntries(entries);
        stream.setLength(length);
        stream.setGroups(groups);
    
        return (T) stream;
    }

    @Override
    public <T> T applyKeyMeta(RedisInputStream in, int version) throws IOException {
        /*
         * $num-classes, then for each class a 4 byte class spec (name, version and flags) followed by
         * the module-encoded metadata value terminated by RDB_MODULE_OPCODE_EOF.
         */
        SkipRdbParser parser = new SkipRdbParser(in);
        long classes = parser.rdbLoadLen().len;
        while (classes-- > 0) {
            in.skip(4); // class spec
            parser.rdbLoadCheckModuleValue();
        }
        return null;
    }

    @Override
    public <T> T applyHashTemplate(RedisInputStream in, int version) throws IOException {
        /*
         * $template-id $field-count $field1 ... $fieldN
         */
        BaseRdbParser parser = new BaseRdbParser(in);
        long id = parser.rdbLoadLen().len;
        long count = parser.rdbLoadLen().len;
        List<byte[]> fields = new ByteArrayList();
        while (count-- > 0) {
            fields.add(parser.rdbLoadEncodedStringObject().first());
        }
        hashTemplates.put(id, fields);
        return (T) fields;
    }

    @Override
    public <T> T applyArray(RedisInputStream in, int version) throws IOException {
        /*
         * $count $has-insert-idx [$insert-idx] ($index $type-tag $value)*
         */
        BaseRdbParser parser = new BaseRdbParser(in);
        long count = parser.rdbLoadLen().len;
        if (parser.rdbLoadLen().len == 1) {
            parser.rdbLoadLen(); // insert_idx
        }
        NavigableMap<Long, byte[]> array = new TreeMap<>(Long::compareUnsigned);
        while (count-- > 0) {
            long index = parser.rdbLoadLen().len;
            int tag = (int) parser.rdbLoadLen().len;
            byte[] value;
            switch (tag) {
                case Constants.ARRAY_TAG_SDS:
                case Constants.ARRAY_TAG_SMALLSTR:
                    value = parser.rdbLoadEncodedStringObject().first();
                    break;
                case Constants.ARRAY_TAG_INT:
                    value = String.valueOf(parser.rdbLoadMillisecondTime()).getBytes();
                    break;
                case Constants.ARRAY_TAG_FLOAT:
                    value = formatDouble(parser.rdbLoadBinaryDoubleValue()).getBytes();
                    break;
                default:
                    throw new AssertionError("unexpected array element type tag:" + tag);
            }
            array.put(index, value);
        }
        return (T) array;
    }

    @Override
    public <T> T applyHashTemplateListPack(RedisInputStream in, int version) throws IOException {
        /*
         * $fields-format $fields $listpack (first entry is the template id)
         */
        BaseRdbParser parser = new BaseRdbParser(in);
        List<byte[]> fields = loadHashTemplateFields(parser);
        List<byte[]> values = listPackEntries(parser.rdbLoadPlainStringObject().first());
        return (T) hashTemplateMap(fields, values.subList(1, values.size()));
    }

    @Override
    public <T> T applyHashTemplateListPackRef(RedisInputStream in, int version) throws IOException {
        /*
         * $listpack (first entry is the template id)
         */
        BaseRdbParser parser = new BaseRdbParser(in);
        List<byte[]> values = listPackEntries(parser.rdbLoadPlainStringObject().first());
        long id = Long.parseLong(Strings.toString(values.get(0)));
        return (T) hashTemplateMap(getHashTemplate(id), values.subList(1, values.size()));
    }

    @Override
    public <T> T applyHashTemplateArray(RedisInputStream in, int version) throws IOException {
        /*
         * $fields-format $fields $value1 ... $valueN
         */
        BaseRdbParser parser = new BaseRdbParser(in);
        List<byte[]> fields = loadHashTemplateFields(parser);
        return (T) hashTemplateMap(fields, loadHashTemplateValues(parser, fields.size()));
    }

    @Override
    public <T> T applyHashTemplateArrayRef(RedisInputStream in, int version) throws IOException {
        /*
         * $template-id $value1 ... $valueN
         */
        BaseRdbParser parser = new BaseRdbParser(in);
        List<byte[]> fields = getHashTemplate(parser.rdbLoadLen().len);
        return (T) hashTemplateMap(fields, loadHashTemplateValues(parser, fields.size()));
    }

    /**
     * @return the hash templates read so far, by template id
     * @since 3.12.0
     */
    public Map<Long, List<byte[]>> getHashTemplates() {
        return hashTemplates;
    }

    /**
     * Share the hash templates with another value visitor reading the same rdb, when a rdb visitor
     * uses one value visitor for some keys and another for the rest.
     *
     * @param hashTemplates the hash templates, by template id
     * @since 3.12.0
     */
    public void setHashTemplates(Map<Long, List<byte[]>> hashTemplates) {
        this.hashTemplates = hashTemplates;
    }

    protected List<byte[]> getHashTemplate(long id) {
        List<byte[]> fields = hashTemplates.get(id);
        if (fields == null) {
            throw new AssertionError("unknown hash template id:" + id);
        }
        return fields;
    }

    protected List<byte[]> loadHashTemplateFields(BaseRdbParser parser) throws IOException {
        long format = parser.rdbLoadLen().len;
        if (format == Constants.HASH_TMPL_FIELDS_LP) {
            return listPackEntries(parser.rdbLoadPlainStringObject().first());
        } else if (format == Constants.HASH_TMPL_FIELDS_RAW) {
            long count = parser.rdbLoadLen().len;
            return loadHashTemplateValues(parser, count);
        }
        throw new AssertionError("unexpected hash template fields format:" + format);
    }

    protected List<byte[]> loadHashTemplateValues(BaseRdbParser parser, long count) throws IOException {
        List<byte[]> values = new ByteArrayList();
        while (count-- > 0) {
            values.add(parser.rdbLoadEncodedStringObject().first());
        }
        return values;
    }

    protected static ByteArrayMap hashTemplateMap(List<byte[]> fields, List<byte[]> values) {
        if (fields.size() != values.size()) {
            throw new AssertionError("hash template has " + fields.size() + " fields but " + values.size() + " values");
        }
        ByteArrayMap map = new ByteArrayMap();
        for (int i = 0; i < fields.size(); i++) {
            map.put(fields.get(i), values.get(i));
        }
        return map;
    }

    protected static List<byte[]> listPackEntries(byte[] bytes) throws IOException {
        RedisInputStream listPack = new RedisInputStream(new ByteArray(bytes));
        listPack.skip(4); // total-bytes
        int len = listPack.readInt(2);
        List<byte[]> entries = new ByteArrayList();
        while (len-- > 0) {
            entries.add(listPackEntry(listPack));
        }
        int lpend = listPack.read(); // lp-end
        if (lpend != 255) {
            throw new AssertionError("listpack expect 255 but " + lpend);
        }
        return entries;
    }

    protected static String formatDouble(double value) {
        if (Double.isNaN(value)) return "nan";
        if (Double.isInfinite(value)) return value > 0 ? "inf" : "-inf";
        return BigDecimal.valueOf(value).stripTrailingZeros().toPlainString();
    }
}
