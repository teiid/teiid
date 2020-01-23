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

package org.teiid.cache.caffeine;

import static org.junit.Assert.*;

import org.junit.Test;
import org.teiid.cache.Cache;

public class TestCaffeineCacheFactory {

    @Test public void testCache() throws InterruptedException {
        CaffeineCacheFactory ccf = new CaffeineCacheFactory();
        Cache<String, String> cache = ccf.get("default");

        assertNull(cache.put("key", "value", null));

        assertNotNull(cache.put("key", "value", 10l));

        Thread.sleep(20);

        assertNull(cache.get("key"));
    }

}
