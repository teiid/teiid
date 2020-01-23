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

package org.teiid.cache.infinispan;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.infinispan.transaction.TransactionMode;
import org.teiid.cache.Cache;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;


/**
 * Implementation of Cache using Infinispan
 */
public class InfinispanCache<K, V> implements Cache<K, V> {

    protected org.infinispan.AdvancedCache<K, V> cacheStore;
    private final String name;
    private ClassLoader classloader;
    private boolean transactional;

    public InfinispanCache(org.infinispan.Cache<K, V> cacheStore, String cacheName, ClassLoader classloader) {
        assert(cacheStore != null);
        this.cacheStore = cacheStore.getAdvancedCache();
        TransactionMode transactionMode = this.cacheStore.getCacheConfiguration().transaction().transactionMode();
        this.transactional = transactionMode == TransactionMode.TRANSACTIONAL;
        LogManager.logDetail(LogConstants.CTX_RUNTIME, "Added", transactionMode, "infinispan cache", cacheName); //$NON-NLS-1$ //$NON-NLS-2$
        this.name = cacheName;
        this.classloader = classloader;
    }

    @Override
    public boolean isTransactional() {
        return transactional;
    }

    @Override
    public V get(K key) {
        return this.cacheStore.with(this.classloader).get(key);
    }

    public V put(K key, V value) {
        return this.cacheStore.with(this.classloader).put(key, value);
    }

    @Override
    public V put(K key, V value, Long ttl) {
        if (ttl != null) {
            return this.cacheStore.with(this.classloader).put(key, value, ttl, TimeUnit.MILLISECONDS);
        }
        return this.cacheStore.with(this.classloader).put(key, value);
    }

    @Override
    public V remove(K key) {
        return this.cacheStore.with(this.classloader).remove(key);
    }

    @Override
    public int size() {
        return this.cacheStore.with(this.classloader).size();
    }

    @Override
    public void clear() {
        this.cacheStore.with(this.classloader).clear();
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public Set<K> keySet() {
        return this.cacheStore.with(this.classloader).keySet();
    }
}
