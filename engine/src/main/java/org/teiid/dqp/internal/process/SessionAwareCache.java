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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.teiid.cache.Cache;
import org.teiid.cache.CacheFactory;
import org.teiid.cache.DefaultCache;
import org.teiid.cache.DefaultCacheFactory;
import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.query.parser.ParseInfo;
import org.teiid.vdb.runtime.VDBKey;


/**
 * This class is used to cache session aware objects
 */
public class SessionAwareCache<T> {
	public static final int DEFAULT_MAX_SIZE_TOTAL = 250;

	private Cache<CacheID, T> localCache;
	private Cache<CacheID, T> distributedCache;
	
	private int maxSize;
	
	private AtomicInteger cacheHit = new AtomicInteger();
	
	SessionAwareCache(){
		this(DEFAULT_MAX_SIZE_TOTAL, new DefaultCacheFactory(), Cache.Type.RESULTSET);
	}
	
	SessionAwareCache(int maxSize){
		this(maxSize, new DefaultCacheFactory(), Cache.Type.RESULTSET);
	}
	
	SessionAwareCache (int maxSize, final CacheFactory cacheFactory, final Cache.Type type){
		if(maxSize < 0){
			maxSize = DEFAULT_MAX_SIZE_TOTAL;
		}
		this.maxSize = maxSize;
		this.localCache = new DefaultCache<CacheID, T>("local", maxSize); //$NON-NLS-1$
		
		this.distributedCache = localCache;
		
//		if (type == Cache.Type.PREPAREDPLAN) {
//			this.distributedCache = localCache;
//		}
//		else {
//			this.distributedCache = cacheFactory.get(type, new CacheConfiguration(CacheConfiguration.Policy.LRU, -1, SessionAwareCache.this.maxSize));
//		}
	}	
	
	public T get(CacheID id){
		id.setSessionId(id.originalSessionId);
		T result = localCache.get(id);
		if (result == null) {
			id.setSessionId(null);
			result = distributedCache.get(id);
			if (result == null) {
				id.setUserName(null);
				result = distributedCache.get(id);
			}
		}
		if (result != null) {
			cacheHit.getAndIncrement();
		}
		return result;
	}
	
	public int getCacheHitCount() {
		return cacheHit.get();
	}
	
	/**
	 * Create PreparedPlan for the given clientConn and SQl query
	 */
	public void put(CacheID id, boolean sessionSpecific, boolean userSpecific, T t){
		if (!id.cachable) {
			return;
		}
		if (sessionSpecific) {
			id.setSessionId(id.originalSessionId);
			this.localCache.put(id, t);
		} else {
			id.setSessionId(null);
			if (!userSpecific) {
				id.setUserName(null);
			}
			this.distributedCache.put(id, t);
		}
	}
	
	/**
	 * Clear all the cached plans for all the clientConns
	 * @param clientConn ClientConnection
	 */
	public void clearAll(){
		this.localCache.clear();
		this.distributedCache.clear();
	}	
	
	static class CacheID implements Serializable {
		private static final long serialVersionUID = 8261905111156764744L;
		private String sql;
		private VDBKey vdbInfo;
		private ParseInfo pi;
		private String sessionId;
		private String originalSessionId;
		private List<Serializable> parameters;
		//private String userName;
		boolean cachable = true;
				
		CacheID(DQPWorkContext context, ParseInfo pi, String sql){
			this.sql = sql;
			this.vdbInfo = new VDBKey(context.getVdbName(), context.getVdbVersion());
			this.pi = pi;
			this.originalSessionId = context.getSessionId();
			//this.userName = context.getUserName();
		}
		
		private void setSessionId(String sessionId) {
			this.sessionId = sessionId;
		}
		
		public void setParameters(List<?> parameters) {
			if (parameters !=  null && !parameters.isEmpty()) {
				this.parameters = new ArrayList<Serializable>();
				for (Object obj:parameters) {
					if (obj instanceof Serializable) {
						this.parameters.add((Serializable)obj);
					}
					else{
						this.cachable = false;
					}
				}
			}
		}
		
		public void setUserName(String name) {
			//this.userName = name;
		}
						
		public boolean equals(Object obj){
	        if(obj == this) {
	            return true;
	        } 
	        if(! (obj instanceof CacheID)) {
	            return false;
	        } 
        	CacheID that = (CacheID)obj;
            return this.pi.equals(that.pi) && this.vdbInfo.equals(that.vdbInfo) && this.sql.equals(that.sql) 
            	//&& EquivalenceUtil.areEqual(this.userName, that.userName)            	
            	&& EquivalenceUtil.areEqual(this.sessionId, that.sessionId)
            	&& EquivalenceUtil.areEqual(this.parameters, that.parameters);
		}
		
	    public int hashCode() {
	        return HashCodeUtil.hashCode(0, vdbInfo, sql, pi,  sessionId, parameters);
	    }
	    
	    @Override
	    public String toString() {
	    	return "Cache Entry<" + originalSessionId + "> params:" + parameters + " sql:" + sql; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	    			
	    }
	}
	
	//for testing purpose 
	int getSpaceUsed() {
		return localCache.size();
	}
    int getSpaceAllowed() {
        return maxSize;
    }
    
}
