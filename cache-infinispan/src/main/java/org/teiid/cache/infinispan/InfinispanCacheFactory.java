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

import java.io.Serializable;

import org.infinispan.manager.EmbeddedCacheManager;
import org.teiid.cache.Cache;
import org.teiid.cache.CacheFactory;
import org.teiid.core.TeiidRuntimeException;


public class InfinispanCacheFactory implements CacheFactory, Serializable{
    private static final long serialVersionUID = -2767452034178675653L;
    private transient EmbeddedCacheManager cacheStore;
    private volatile boolean destroyed = false;
    private ClassLoader classLoader;

    public InfinispanCacheFactory(EmbeddedCacheManager cm, ClassLoader classLoader) {
        this.cacheStore = cm;
        this.classLoader = classLoader;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> Cache<K, V> get(String cacheName) {
        if (!destroyed) {
            org.infinispan.Cache cache = this.cacheStore.getCache(cacheName, false);
            if (cache != null) {
                return new InfinispanCache<K, V>(cache, cacheName, this.classLoader);
            }
            return null;
        }
        throw new TeiidRuntimeException("Cache system has been shutdown");
    }

    public void destroy() {
        this.destroyed = true;
        this.cacheStore.stop();
    }

    public void stop() {
        destroy();
    }

}
