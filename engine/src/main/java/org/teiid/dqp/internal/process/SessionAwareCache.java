/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

package org.teiid.dqp.internal.process;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.teiid.cache.Cachable;
import org.teiid.cache.Cache;
import org.teiid.cache.CacheConfiguration;
import org.teiid.cache.CacheFactory;
import org.teiid.cache.DefaultCache;
import org.teiid.cache.DefaultCacheFactory;
import org.teiid.cache.CacheConfiguration.Policy;
import org.teiid.common.buffer.BufferManager;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.Assertion;
import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.query.parser.ParseInfo;
import org.teiid.vdb.runtime.VDBKey;


/**
 * This class is used to cache session aware objects
 */
public class SessionAwareCache<T> {
	public static final int DEFAULT_MAX_SIZE_TOTAL = 512;
	public enum Type {
		RESULTSET,
		PREPAREDPLAN;
	}

	private Cache<CacheID, T> localCache;
	private Cache<CacheID, T> distributedCache;
	private Cache tupleBatchCache;
	
	private int maxSize = DEFAULT_MAX_SIZE_TOTAL;
	private long modTime;
	private Type type;
	
	private AtomicInteger cacheHit = new AtomicInteger();
	private AtomicInteger totalRequests = new AtomicInteger();
	
	private BufferManager bufferManager;
	
	public SessionAwareCache(){
		this(DEFAULT_MAX_SIZE_TOTAL);
	}
	
	SessionAwareCache(int maxSize){
		this(new DefaultCacheFactory(), Type.RESULTSET, new CacheConfiguration(Policy.LRU, 60, maxSize, "default")); //$NON-NLS-1$
	}
	
	SessionAwareCache (final CacheFactory cacheFactory, final Type type, final CacheConfiguration config){
		this.maxSize = config.getMaxEntries();
		if(this.maxSize < 0){
			this.maxSize = Integer.MAX_VALUE;
		}		
		this.localCache = new DefaultCache<CacheID, T>("local", maxSize, config.getMaxAgeInSeconds()*1000); //$NON-NLS-1$
		
		if (type == Type.PREPAREDPLAN) {
			this.distributedCache = localCache;
		}
		else {
			String location = config.getLocation()+"/"+type.name(); //$NON-NLS-1$
			this.distributedCache = cacheFactory.get(location, config);
			if (type == Type.RESULTSET) {
				this.tupleBatchCache = cacheFactory.get(location+"/batches", config); //$NON-NLS-1$
			}
			else {
				this.tupleBatchCache = this.distributedCache;
			}
		}
		this.modTime = config.getMaxStaleness()*1000;
		this.type = type;
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
				if (!c.restore(this.tupleBatchCache, this.bufferManager)) {
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
	
	public int getTotalCacheEntries() {
		if (this.localCache == this.distributedCache) {
			return this.localCache.size();
		}
		return localCache.size() + distributedCache.size();
	}
	
	public void put(CacheID id, Determinism determinismLevel, T t, Long ttl){
		if (determinismLevel.compareTo(Determinism.SESSION_DETERMINISTIC) <= 0) {
			id.setSessionId(id.originalSessionId);
			LogManager.logTrace(LogConstants.CTX_DQP, "Adding to session/local cache", id); //$NON-NLS-1$
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
				insert = c.prepare(this.tupleBatchCache, this.bufferManager);
			}
			
			if (insert) {
				LogManager.logTrace(LogConstants.CTX_DQP, "Adding to global/distributed cache", id); //$NON-NLS-1$
				this.distributedCache.put(id, t, ttl);
			}
		}
	}
	
	/**
	 * Clear all the cached plans for all the clientConns
	 * @param clientConn ClientConnection
	 */
	public void clearAll(){
		this.localCache.clear();
		this.distributedCache.clear();
		this.totalRequests = new AtomicInteger();
		this.cacheHit = new AtomicInteger();
	}	
	
	public void clearForVDB(String vdbName, int version) {
		clearCache(this.localCache, vdbName, version);
		clearCache(this.distributedCache, vdbName, version);
	}
	
	private void clearCache(Cache<CacheID, T> cache, String vdbName, int version) {
		Set<CacheID> keys = cache.keys();
		VDBKey vdbKey = new VDBKey(vdbName, version);
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
		private ParseInfo pi;
		private String sessionId;
		private String originalSessionId;
		private List<Serializable> parameters;
		private String userName;
		private String originalUserName;
		
		public CacheID(DQPWorkContext context, ParseInfo pi, String sql){
			this(pi, sql, context.getVdbName(), context.getVdbVersion(), context.getSessionId(), context.getUserName());
		}
		
		public CacheID(ParseInfo pi, String sql, String vdbName, int vdbVersion, String sessionId, String userName){
			Assertion.isNotNull(sql);
			this.sql = sql;
			this.vdbInfo = new VDBKey(vdbName, vdbVersion);
			this.pi = pi;
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
				this.parameters = new ArrayList<Serializable>();
				for (Object obj:parameters) {
					if (obj == null) {
						this.parameters.add(null);
						continue;
					}
					if (!(obj instanceof Serializable)) {
						return false;
					}
					
					Class<?> type = DataTypeManager.determineDataTypeClass(obj);
					if (type == DataTypeManager.DefaultDataClasses.OBJECT) {
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
            return EquivalenceUtil.areEqual(this.pi, that.pi) && this.vdbInfo.equals(that.vdbInfo) && this.sql.equals(that.sql) 
            	&& EquivalenceUtil.areEqual(this.userName, that.userName)            	
            	&& EquivalenceUtil.areEqual(this.sessionId, that.sessionId)
            	&& EquivalenceUtil.areEqual(this.parameters, that.parameters);
		}
		
	    public int hashCode() {
	        return HashCodeUtil.hashCode(0, vdbInfo, sql, pi, this.userName, sessionId, parameters);
	    }
	    
	    @Override
	    public String toString() {
	    	return "Cache Entry<" + originalSessionId + "="+ originalUserName + "> params:" + parameters + " sql:" + sql; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
	    }
	    
	}
	
    int getSpaceAllowed() {
        return maxSize;
    }
    
    public void setBufferManager(BufferManager bufferManager) {
    	this.bufferManager = bufferManager;
    }
    
    public void setModTime(long modTime) {
		this.modTime = modTime;
	}
    
    public Set<CacheID> replicatableKeys() {
    	if (this.distributedCache == this.localCache) {
    		return Collections.EMPTY_SET;
    	}
    	return this.distributedCache.keys();
    }
}
