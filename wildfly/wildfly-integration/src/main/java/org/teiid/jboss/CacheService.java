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
package org.teiid.jboss;

import org.jboss.msc.service.Service;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StartException;
import org.jboss.msc.service.StopContext;
import org.jboss.msc.value.InjectedValue;
import org.teiid.cache.CacheFactory;
import org.teiid.common.buffer.TupleBufferCache;
import org.teiid.dqp.internal.process.SessionAwareCache;
import org.teiid.dqp.internal.process.SessionAwareCache.Type;

class CacheService<T> implements Service<SessionAwareCache<T>> {

    private SessionAwareCache<T> cache;
    protected InjectedValue<TupleBufferCache> tupleBufferCacheInjector = new InjectedValue<TupleBufferCache>();
    protected InjectedValue<CacheFactory> cacheFactoryInjector = new InjectedValue<CacheFactory>();

    private SessionAwareCache.Type type;
    private String cacheName;
    private int maxStaleness;

    public CacheService(String cacheName, SessionAwareCache.Type type, int maxStaleness){
        this.cacheName = cacheName;
        this.type = type;
        this.maxStaleness = maxStaleness;
    }

    @Override
    public void start(StartContext context) throws StartException {
        this.cache = new SessionAwareCache<T>(this.cacheName, cacheFactoryInjector.getValue(), this.type, this.maxStaleness);
        if (type == Type.RESULTSET) {
            this.cache.setTupleBufferCache(this.tupleBufferCacheInjector.getValue());
        }
    }

    @Override
    public void stop(StopContext context) {
        this.cache = null;
    }

    @Override
    public SessionAwareCache<T> getValue() throws IllegalStateException, IllegalArgumentException {
        return this.cache;
    }
}
