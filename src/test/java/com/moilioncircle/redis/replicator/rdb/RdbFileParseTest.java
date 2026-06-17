package com.moilioncircle.redis.replicator.rdb;

import com.moilioncircle.redis.replicator.Configuration;
import com.moilioncircle.redis.replicator.FileType;
import com.moilioncircle.redis.replicator.RedisReplicator;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.rdb.datatype.KeyValuePair;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;

public class RdbFileParseTest {

    @Test
    public void testParseExternalRdb() throws Exception {
        String path = System.getProperty("rdb.file");
        if (path == null || path.isEmpty() || !new File(path).exists()) {
            System.out.println("Skipping: rdb.file not set or not found. Pass -Drdb.file=<path>");
            return;
        }
        AtomicLong count = new AtomicLong();
        AtomicLong streamCount = new AtomicLong();
        try (Replicator r = new RedisReplicator(new File(path), FileType.RDB, Configuration.defaultSetting())) {
            r.addEventListener((replicator, event) -> {
                if (event instanceof KeyValuePair) {
                    long n = count.incrementAndGet();
                    int type = ((KeyValuePair<?, ?>) event).getValueRdbType();
                    if (type == 26) streamCount.incrementAndGet();
                    if (n % 100000 == 0) System.out.println("Parsed " + n + " keys...");
                }
            });
            r.open();
        }
        System.out.println("Done. Total keys: " + count.get() + ", stream-listpacks-4 keys: " + streamCount.get());
    }
}
