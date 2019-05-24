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

package org.teiid.runtime.jmx;

import org.teiid.dqp.internal.process.SessionAwareCache;

class Cache implements CacheBean {

    private SessionAwareCache<?> cache;

    public Cache(SessionAwareCache<?> cache) {
        this.cache = cache;
    }

    @Override
    public double getHitRatio() {
        return cache.getCacheHitRatio();
    }

    @Override
    public int getTotalEntries() {
        return cache.getTotalCacheEntries();
    }

    @Override
    public long getRequestCount() {
        return cache.getRequestCount();
    }

    @Override
    public void clear() {
        cache.clearAll();
    }

}