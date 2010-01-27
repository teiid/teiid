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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.metamatrix.core.util.EquivalenceUtil;
import com.metamatrix.core.util.HashCodeUtil;
import com.metamatrix.core.util.LRUCache;
import com.metamatrix.query.parser.ParseInfo;
import com.metamatrix.vdb.runtime.VDBKey;

/**
 * This class is used to cache session aware objects
 */
public class SessionAwareCache<T> {
	public static final int DEFAULT_MAX_SIZE_TOTAL = 250;

	private Map<CacheID, T> cache;
	private int maxSize;
	
	private AtomicInteger cacheHit = new AtomicInteger();
	
	SessionAwareCache(){
		this(DEFAULT_MAX_SIZE_TOTAL);
	}
	
	SessionAwareCache (int maxSize ){
		if(maxSize < 0){
			maxSize = DEFAULT_MAX_SIZE_TOTAL;
		}
		this.maxSize = maxSize;
		cache = Collections.synchronizedMap(new LRUCache<CacheID, T>(maxSize));
	}	
	
	public T get(CacheID id){
		id.setSessionId(id.originalSessionId);
		T result = cache.get(id);
		if (result == null) {
			id.setSessionId(null);
			result = cache.get(id);
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
	public void put(CacheID id, boolean sessionSpecific, T t){
		if (sessionSpecific) {
			id.setSessionId(id.originalSessionId);
		} else {
			id.setSessionId(null);
		}
		this.cache.put(id, t);
	}
	
	/**
	 * Clear all the cached plans for all the clientConns
	 * @param clientConn ClientConnection
	 */
	public void clearAll(){
		cache.clear();
	}	
	
	static class CacheID{
		private String sql;
		private VDBKey vdbInfo;
		private ParseInfo pi;
		private String sessionId;
		private String originalSessionId;
		private List<?> parameters;
				
		CacheID(DQPWorkContext context, ParseInfo pi, String sql){
			this.sql = sql;
			this.vdbInfo = new VDBKey(context.getVdbName(), context.getVdbVersion());
			this.pi = pi;
			this.originalSessionId = context.getConnectionID();
		}
		
		private void setSessionId(String sessionId) {
			this.sessionId = sessionId;
		}
		
		public void setParameters(List<?> parameters) {
			this.parameters = parameters;
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
            	&& EquivalenceUtil.areEqual(this.sessionId, that.sessionId)
            	&& EquivalenceUtil.areEqual(this.parameters, that.parameters);
		}
		
	    public int hashCode() {
	        return HashCodeUtil.hashCode(0, vdbInfo, sql, pi, sessionId, parameters);
	    }
	    
	    @Override
	    public String toString() {
	    	return "Cache Entry<" + originalSessionId + "> params:" + parameters + " sql:" + sql; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	    			
	    }
	}
	
	//for testing purpose 
	int getSpaceUsed() {
		return cache.size();
	}
    int getSpaceAllowed() {
        return maxSize;
    }
    
}
