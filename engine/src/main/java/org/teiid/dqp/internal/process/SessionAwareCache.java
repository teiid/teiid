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

package org.teiid.dqp.internal.process;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.teiid.adminapi.Admin;
import org.teiid.adminapi.impl.CacheStatisticsMetadata;
import org.teiid.cache.Cachable;
import org.teiid.cache.Cache;
import org.teiid.cache.CacheFactory;
import org.teiid.common.buffer.TupleBufferCache;
import org.teiid.core.util.Assertion;
import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.AbstractMetadataRecord.DataModifiable;
import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.query.parser.ParseInfo;
import org.teiid.vdb.runtime.VDBKey;


/**
 * This class is used to cache session aware objects
 */
public class SessionAwareCache<T> {
    public static final String REPL = "-repl"; //$NON-NLS-1$
    public static final int DEFAULT_MAX_SIZE_TOTAL = 512;
    public enum Type {
        RESULTSET,
        PREPAREDPLAN;
    }

    private Cache<CacheID, T> localCache;
    private Cache<CacheID, T> distributedCache;

    private long modTime;
    private Type type;

    private AtomicInteger cacheHit = new AtomicInteger();
    private AtomicInteger totalRequests = new AtomicInteger();
    private AtomicInteger cachePuts = new AtomicInteger();

    private TupleBufferCache bufferManager;

    public SessionAwareCache (String cacheName, final CacheFactory cacheFactory, final Type type, int maxStaleness) {
        assert (cacheFactory != null);

        this.localCache = cacheFactory.get(cacheName);

        if (type == Type.PREPAREDPLAN) {
            this.distributedCache = localCache;
        }
        else {
            this.distributedCache = cacheFactory.get(cacheName+REPL);
            if (this.distributedCache == null && this.localCache != null) {
                this.distributedCache = this.localCache;
            }
        }
        this.modTime = maxStaleness * 1000;
        this.type = type;

        assert (this.localCache != null);
        assert (this.distributedCache != null);
    }

    public T get(CacheID id){

        this.totalRequests.getAndIncrement();

        id.setSessionId(id.originalSessionId);
        T result = localCache.get(id);

        if (result == null) {
            id.setSessionId(null);

            id.setUserName(id.originalUserName);
            result = distributedCache.get(id);

            if (result == null) {
                id.setUserName(null);
                result = distributedCache.get(id);
            }

            if (result instanceof Cachable) {
                Cachable c = (Cachable)result;
                if (!c.restore(this.bufferManager)) {
                    result = null;
                }
            }
        }

        if (result != null) {
            if (result instanceof Cachable) {
                Cachable c = (Cachable)result;
                AccessInfo info = c.getAccessInfo();
                if (info != null && !info.validate(type == Type.RESULTSET, modTime)) {
                    LogManager.logTrace(LogConstants.CTX_DQP, "Invalidating cache entry", id); //$NON-NLS-1$
                    if (id.getSessionId() == null) {
                        this.distributedCache.remove(id);
                    } else {
                        this.localCache.remove(id);
                    }
                    return null;
                }
            }
            LogManager.logTrace(LogConstants.CTX_DQP, "Cache hit for", id); //$NON-NLS-1$
            cacheHit.getAndIncrement();
        } else {
            LogManager.logTrace(LogConstants.CTX_DQP, "Cache miss for", id); //$NON-NLS-1$
        }
        return result;
    }

    public int getCacheHitCount() {
        return cacheHit.get();
    }

    public int getRequestCount() {
        return this.totalRequests.get();
    }

    public int getCachePutCount() {
        return cachePuts.get();
    }

    public int getTotalCacheEntries() {
        if (this.localCache == this.distributedCache) {
            return this.localCache.size();
        }
        return localCache.size() + distributedCache.size();
    }

    public T remove(CacheID id, Determinism determinismLevel){
        if (determinismLevel.compareTo(Determinism.SESSION_DETERMINISTIC) <= 0) {
            id.setSessionId(id.originalSessionId);
            LogManager.logTrace(LogConstants.CTX_DQP, "Removing from session/local cache", id); //$NON-NLS-1$
            return this.localCache.remove(id);
        }
        id.setSessionId(null);

        if (determinismLevel == Determinism.USER_DETERMINISTIC) {
            id.setUserName(id.originalUserName);
        }
        else {
            id.setUserName(null);
        }

        LogManager.logTrace(LogConstants.CTX_DQP, "Removing from global/distributed cache", id); //$NON-NLS-1$
        return this.distributedCache.remove(id);
    }

    public void put(CacheID id, Determinism determinismLevel, T t, Long ttl){
        cachePuts.incrementAndGet();
        if (determinismLevel.compareTo(Determinism.SESSION_DETERMINISTIC) <= 0) {
            id.setSessionId(id.originalSessionId);
            LogManager.logTrace(LogConstants.CTX_DQP, "Adding to session/local cache", id); //$NON-NLS-1$
            ttl = computeTtl(id, t, ttl);
            if (ttl != null && ttl == 0) {
                return;
            }
            this.localCache.put(id, t, ttl);
        }
        else {

            boolean insert = true;

            id.setSessionId(null);

            if (determinismLevel == Determinism.USER_DETERMINISTIC) {
                id.setUserName(id.originalUserName);
            }
            else {
                id.setUserName(null);
            }

            if (t instanceof Cachable) {
                Cachable c = (Cachable)t;
                ttl = computeTtl(id, t, ttl);
                if (ttl != null && ttl == 0) {
                    return;
                }
                insert = c.prepare(this.bufferManager);
            }

            if (insert) {
                LogManager.logTrace(LogConstants.CTX_DQP, "Adding to global/distributed cache", id); //$NON-NLS-1$
                this.distributedCache.put(id, t, ttl);
            }
        }
    }

    Long computeTtl(CacheID id, T t, Long ttl) {
        if (!(t instanceof Cachable) || type != Type.RESULTSET) {
            return ttl;
        }
        Cachable c = (Cachable)t;
        AccessInfo info = c.getAccessInfo();
        if (info == null) {
            return ttl;
        }
        Set<Object> objects = info.getObjectsAccessed();
        if (objects == null) {
            return ttl;
        }
        long computedTtl = Long.MAX_VALUE;
        for (Object o : objects) {
            if (!(o instanceof AbstractMetadataRecord)) {
                continue;
            }
            AbstractMetadataRecord amr = (AbstractMetadataRecord)o;
            Long l = getDataTtl(amr);
            if (l == null || l < 0) {
                continue;
            }
            if (l == 0) {
                if (LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.DETAIL)) {
                    LogManager.logDetail(LogConstants.CTX_DQP, "Not adding cache entry", id, "since", amr.getFullName(), "has caching disabled"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                }
                return Long.valueOf(0);
            }
            computedTtl = Math.min(l, computedTtl);
        }
        if (ttl != null) {
            ttl = Math.min(ttl, computedTtl);
        } else if (computedTtl != Long.MAX_VALUE) {
            ttl = computedTtl;
        }
        return ttl;
    }

    static Long getDataTtl(AbstractMetadataRecord record) {
        String value = record.getProperty(DataModifiable.DATA_TTL, false);
        if (value != null) {
            try {
                return Long.valueOf(value);
            } catch (NumberFormatException e) {
                if (LogManager.isMessageToBeRecorded(LogConstants.CTX_RUNTIME, MessageLevel.DETAIL)) {
                    LogManager.logDetail(LogConstants.CTX_RUNTIME, "Invalid data ttl specified for ", record.getFullName()); //$NON-NLS-1$
                }
            }
        }
        if (record.getParent() != null) {
            return getDataTtl(record.getParent());
        }
        return null;
    }

    /**
     * Clear all the cached plans for all the clientConns
     */
    public void clearAll(){
        this.localCache.clear();
        this.distributedCache.clear();
        this.totalRequests.set(0);
        this.cacheHit.set(0);
        this.cachePuts.set(0);
    }

    public void clearForVDB(String vdbName, String version) {
        VDBKey vdbKey = new VDBKey(vdbName, version);
        clearForVDB(vdbKey);
    }

    public void clearForVDB(VDBKey vdbKey) {
        clearCache(this.localCache, vdbKey);
        clearCache(this.distributedCache, vdbKey);
    }

    private void clearCache(Cache<CacheID, T> cache, VDBKey vdbKey) {
        Set<CacheID> keys = cache.keySet();
        for (CacheID key:keys) {
            if (key.vdbInfo.equals(vdbKey)) {
                cache.remove(key);
            }
        }
    }

    public static class CacheID implements Serializable {
        private static final long serialVersionUID = 8261905111156764744L;
        private String sql;
        private VDBKey vdbInfo;
        private boolean ansiIdentifiers;
        private String sessionId;
        private String originalSessionId;
        private List<Serializable> parameters;
        private String userName;
        private String originalUserName;

        public CacheID(DQPWorkContext context, ParseInfo pi, String sql){
            this(pi, sql, context.getVdbName(), context.getVdbVersion(), context.getSessionId(), context.getUserName());
        }

        public CacheID(ParseInfo pi, String sql, String vdbName, String vdbVersion, String sessionId, String userName){
            Assertion.isNotNull(sql);
            this.sql = sql;
            this.vdbInfo = new VDBKey(vdbName, vdbVersion);
            this.ansiIdentifiers = pi.ansiQuotedIdentifiers;
            this.originalSessionId = sessionId;
            this.originalUserName = userName;
        }


        public String getSessionId() {
            return sessionId;
        }

        public String getUserName() {
            return userName;
        }

        private void setSessionId(String sessionId) {
            this.sessionId = sessionId;
        }

        /**
         * Set the raw (non-Constant) parameter values.
         * @param parameters
         * @return
         */
        public boolean setParameters(List<?> parameters) {
            if (parameters !=  null && !parameters.isEmpty()) {
                this.parameters = new ArrayList<Serializable>(parameters.size());
                for (Object obj:parameters) {
                    if (obj == null) {
                        this.parameters.add(null);
                        continue;
                    }
                    if (!(obj instanceof Serializable)) {
                        return false;
                    }
                    this.parameters.add((Serializable)obj);
                }
            }
            return true;
        }

        void setUserName(String name) {
            this.userName = name;
        }

        public VDBKey getVDBKey() {
            return vdbInfo;
        }

        public boolean equals(Object obj){
            if(obj == this) {
                return true;
            }
            if(! (obj instanceof CacheID)) {
                return false;
            }
            CacheID that = (CacheID)obj;
            return ansiIdentifiers == that.ansiIdentifiers && this.vdbInfo.equals(that.vdbInfo) && this.sql.equals(that.sql)
                && EquivalenceUtil.areEqual(this.userName, that.userName)
                && EquivalenceUtil.areEqual(this.sessionId, that.sessionId)
                && EquivalenceUtil.areEqual(this.parameters, that.parameters);
        }

        public int hashCode() {
            return HashCodeUtil.hashCode(0, vdbInfo, sql, this.userName, sessionId, parameters);
        }

        @Override
        public String toString() {
            return "Cache Entry<" + originalSessionId + "="+ originalUserName + "> params:" + parameters + " sql:" + sql; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        }

    }

    public void setTupleBufferCache(TupleBufferCache bufferManager) {
        this.bufferManager = bufferManager;
    }

    public void setModTime(long modTime) {
        this.modTime = modTime;
    }

    public static Collection<String> getCacheTypes(){
        ArrayList<String> caches = new ArrayList<String>();
        caches.add(Admin.Cache.PREPARED_PLAN_CACHE.toString());
        caches.add(Admin.Cache.QUERY_SERVICE_RESULT_SET_CACHE.toString());
        return caches;
    }

    public boolean isTransactional() {
        return this.localCache.isTransactional() || this.distributedCache.isTransactional();
    }

    public double getCacheHitRatio() {
        return this.getRequestCount() == 0?0:((double)this.getCacheHitCount()/this.getRequestCount())*100;
    }

    public CacheStatisticsMetadata buildCacheStats(String name) {
        CacheStatisticsMetadata stats = new CacheStatisticsMetadata();
        stats.setName(name);
        stats.setHitRatio(getCacheHitRatio());
        stats.setTotalEntries(this.getTotalCacheEntries());
        stats.setRequestCount(this.getRequestCount());
        return stats;
    }
}
