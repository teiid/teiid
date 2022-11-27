/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.teiid.infinispan.api;

import static org.junit.Assert.*;

import org.infinispan.commons.api.BasicCache;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestHotrodServer {
    HotRodTestServer server;

    @Before
    public void setup() {
        this.server = new HotRodTestServer(31323);
    }

    @After
    public void tearDown() {
        if (this.server != null) {
            this.server.stop();
        }
    }

    @Test
    public void testServer() throws Exception {
        InfinispanConnection connection = this.server.getConnection();
        BasicCache<Object, Object> cache = connection.getCache();

        cache.put(100, "hello");
        cache.put(101, "Infinispan");

        assertEquals("hello", cache.get(100));
        assertEquals("Infinispan", cache.get(101));
    }
}
