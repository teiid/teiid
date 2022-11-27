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
package org.teiid.cache;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import org.teiid.cache.CacheConfiguration.Policy;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.LRUCache;
import org.teiid.query.QueryPlugin;

/**
 * Provides a non-thread safe simple map backed cache suitable for testing
 */
public class DefaultCacheFactory implements CacheFactory, Serializable {
    private static final long serialVersionUID = -5541424157695857527L;
    private static CacheConfiguration DEFAULT = new CacheConfiguration(Policy.LRU, 60*60, 100, "default"); // 1 hours with 100 nodes. //$NON-NLS-1$

    public static DefaultCacheFactory INSTANCE = new DefaultCacheFactory(DEFAULT);

    private volatile boolean destroyed = false;
    private CacheConfiguration config;

    public DefaultCacheFactory(CacheConfiguration config) {
        this.config = config;
    }

    @Override
    public void destroy() {
        this.destroyed = true;
    }

    @Override
    public <K, V> Cache<K, V> get(String cacheName) {
        if (!destroyed) {
            return new MockCache<K, V>(cacheName, config.getMaxEntries());
        }
         throw new TeiidRuntimeException(QueryPlugin.Event.TEIID30562, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30562));
    }

    @SuppressWarnings("serial")
    private static class MockCache<K, V> extends LRUCache<K, V> implements Cache<K, V> {

        private String name;

        public MockCache(String cacheName, int maxSize) {
            super(maxSize<0?Integer.MAX_VALUE:maxSize);
            this.name = cacheName;
        }

        @Override
        public V put(K key, V value, Long ttl) {
            return put(key, value);
        }

        @Override
        public Set<K> keySet() {
            return new HashSet<K>(super.keySet());
        }

        @Override
        public String getName() {
            return this.name;
        }

        @Override
        public boolean isTransactional() {
            return false;
        }
    }
}
