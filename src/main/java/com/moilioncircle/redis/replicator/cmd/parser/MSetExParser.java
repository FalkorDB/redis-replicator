/*
 * Copyright 2016-2024 Leon Chen
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

package com.moilioncircle.redis.replicator.cmd.parser;

import static com.moilioncircle.redis.replicator.cmd.CommandParsers.toBytes;
import static com.moilioncircle.redis.replicator.cmd.CommandParsers.toRune;
import static com.moilioncircle.redis.replicator.util.Strings.isEquals;

import com.moilioncircle.redis.replicator.cmd.CommandParser;
import com.moilioncircle.redis.replicator.cmd.impl.ExistType;
import com.moilioncircle.redis.replicator.cmd.impl.MSetExCommand;
import com.moilioncircle.redis.replicator.cmd.impl.XATType;
import com.moilioncircle.redis.replicator.rdb.datatype.ExpiredType;
import com.moilioncircle.redis.replicator.util.ByteArrayMap;

/**
 * @author Leon Chen
 * @since 3.11.0
 */
public class MSetExParser implements CommandParser<MSetExCommand> {

    @Override
    public MSetExCommand parse(Object[] command) {
        int numKeys = Integer.parseInt(toRune(command[1]));
        int idx = 2;
        ByteArrayMap kv = new ByteArrayMap();
        for (int i = 0; i < numKeys; i++) {
            byte[] key = toBytes(command[idx++]);
            byte[] value = toBytes(command[idx++]);
            kv.put(key, value);
        }

        ExistType existType = ExistType.NONE;
        Long expiredValue = null;
        XATType xatType = XATType.NONE;
        Long xatValue = null;
        boolean et = false, st = false;
        boolean keepTtl = false;
        ExpiredType expiredType = ExpiredType.NONE;

        while (idx < command.length) {
            String param = toRune(command[idx++]);
            if (!et && isEquals(param, "NX")) {
                existType = ExistType.NX;
                et = true;
            } else if (!et && isEquals(param, "XX")) {
                existType = ExistType.XX;
                et = true;
            }

            if (!st && isEquals(param, "EX")) {
                expiredType = ExpiredType.SECOND;
                expiredValue = Long.valueOf(toRune(command[idx++]));
                st = true;
            } else if (!st && isEquals(param, "PX")) {
                expiredType = ExpiredType.MS;
                expiredValue = Long.valueOf(toRune(command[idx++]));
                st = true;
            } else if (!st && isEquals(param, "EXAT")) {
                xatType = XATType.EXAT;
                xatValue = Long.valueOf(toRune(command[idx++]));
                st = true;
            } else if (!st && isEquals(param, "PXAT")) {
                xatType = XATType.PXAT;
                xatValue = Long.valueOf(toRune(command[idx++]));
                st = true;
            } else if (!st && isEquals(param, "KEEPTTL")) {
                keepTtl = true;
                st = true;
            }
        }

        return new MSetExCommand(kv, keepTtl, expiredType, expiredValue, xatType, xatValue, existType);
    }
}
