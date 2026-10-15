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

package com.moilioncircle.redis.replicator.rdb.datatype;

import java.io.Serializable;
import java.util.Comparator;
import java.util.NavigableMap;

/**
 * A redis array (since redis 8.8). Maps each set index to its value. Integers and floats are
 * converted to their string form, the way ARGET returns them.
 *
 * @since 3.12.0
 * @see com.moilioncircle.redis.replicator.rdb.DefaultRdbVisitor
 */
public class KeyStringValueArray extends KeyValuePair<byte[], NavigableMap<Long, byte[]>> {
    private static final long serialVersionUID = 1L;

    /**
     * Array indexes are unsigned 64 bit integers. Serializable, like the map that uses it.
     */
    public static final Comparator<Long> INDEX_ORDER = new IndexOrder();

    private static final class IndexOrder implements Comparator<Long>, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public int compare(Long a, Long b) {
            return Long.compareUnsigned(a, b);
        }

        private Object readResolve() {
            return INDEX_ORDER;
        }
    }
}
