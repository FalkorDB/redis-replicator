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

package com.moilioncircle.redis.replicator.cmd.parser;

import static junit.framework.TestCase.assertTrue;

import org.junit.Test;

import com.moilioncircle.redis.replicator.cmd.impl.ExistType;
import com.moilioncircle.redis.replicator.cmd.impl.MSetExCommand;
import com.moilioncircle.redis.replicator.rdb.datatype.ExpiredType;

import junit.framework.TestCase;

/**
 * @author Leon Chen
 * @since 3.11.0
 */
public class MSetExParserTest extends AbstractParserTest {

    @Test
    public void testParse() {
        {
            MSetExParser parser = new MSetExParser();
            MSetExCommand command = parser.parse(toObjectArray("msetex 2 k1 v1 k2 v2 nx ex 100".split(" ")));
            assertEquals(2, command.getKv().size());
            assertTrue(command.getKv().containsKey("k1".getBytes()));
            assertTrue(command.getKv().containsKey("k2".getBytes()));
            assertEquals("v1", command.getKv().get("k1".getBytes()));
            assertEquals("v2", command.getKv().get("k2".getBytes()));
            TestCase.assertEquals(ExpiredType.SECOND, command.getExpiredType());
            assertEquals(100L, command.getExpiredValue());
            TestCase.assertEquals(ExistType.NX, command.getExistType());
        }
        
        {
            MSetExParser parser = new MSetExParser();
            MSetExCommand command = parser.parse(toObjectArray("msetex 1 k1 v1 nx keepttl".split(" ")));
            assertEquals(1, command.getKv().size());
            assertTrue(command.getKv().containsKey("k1".getBytes()));
            assertEquals("v1", command.getKv().get("k1".getBytes()));
            TestCase.assertTrue(command.isKeepTtl());
        }
    }
}