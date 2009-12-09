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

package org.teiid.dqp.internal.cache;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.teiid.dqp.internal.process.DQPWorkContext;

import com.metamatrix.cache.Cache;
import com.metamatrix.cache.CacheConfiguration;
import com.metamatrix.cache.CacheFactory;
import com.metamatrix.cache.Cache.Type;
import com.metamatrix.cache.CacheConfiguration.Policy;
import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.common.buffer.impl.BufferManagerImpl;
import com.metamatrix.common.buffer.impl.SizeUtility;
import com.metamatrix.core.util.HashCodeUtil;

/**
 * Used to cache ResultSet based on the exact match of sql string.
 */
public class ResultSetCache {
	
	static class TempKey {
		CacheID cacheID;
		Object requestID;

		public TempKey(CacheID cacheID, Object requestID) {
			this.cacheID = cacheID;
			this.requestID = requestID;
		}
		@Override
		public int hashCode() {
			return HashCodeUtil.hashCode(cacheID.hashCode(), requestID.hashCode());
		}
		
		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (!(obj instanceof TempKey)) {
				return false;
			}
			TempKey other = (TempKey) obj;
			return (cacheID.equals(other.cacheID) && requestID.equals(other.requestID));
		}
	}
	
	//configurable parameters
	public static final String RS_CACHE_MAX_ENTRIES = "maxEntries"; //$NON-NLS-1$
	public static final String RS_CACHE_MAX_SIZE = "maxEntrySize"; //$NON-NLS-1$
	public static final String RS_CACHE_MAX_AGE = "maxAge"; //$NON-NLS-1$
	public static final String RS_CACHE_SCOPE = "scope"; //$NON-NLS-1$
		
	//constants
	public static final String RS_CACHE_SCOPE_VDB = "vdb"; //$NON-NLS-1$
	public static final String RS_CACHE_SCOPE_CONN = "connection"; //$NON-NLS-1$
	private static final char DELIMITOR = '.';
	
	private Cache<CacheID, CacheResults> cache; 
	private String scope = RS_CACHE_SCOPE_VDB;
	private Map<TempKey, CacheResults> tempBatchResults = new HashMap<TempKey, CacheResults>();
	private int maxEntrySize = 100 * 1024; //bytes
	private int maxAge = 60 * 60; // seconds
	private int maxEntries = 1024;
	
	public void start(CacheFactory cacheFactory) {
		this.cache = cacheFactory.get(Type.RESULTSET, new CacheConfiguration(Policy.MRU, maxAge, maxEntries));
	}
	
	public void setMaxEntries(int maxEntries) {
		this.maxEntries = maxEntries;
	}
	
	public void setMaxEntrySize(int maxEntrySize) {
		this.maxEntrySize = maxEntrySize;
	}
	
	public void setMaxAge(int maxAge) {
		if (maxAge <= 0) {
			this.maxAge = Integer.MAX_VALUE;
		}
		this.maxAge = Math.max(1, maxAge / 1000);
	}
	
	public void setScope(String scope) {
		this.scope = scope;
	}
		
	//interval is 1 based. 
	public final CacheResults getResults(CacheID cacheID, int[] interval){
		CacheResults cacheResults = null;
		cacheResults = cache.get(cacheID);
		if(cacheResults == null){
			return null;
		}
		if (interval == null) {
			return cacheResults;
		}
		int firstRow = interval[0] - 1;
		int resultSize = cacheResults.getResults().length;
		int finalRow = resultSize - 1;
		int lastRow = Math.min(finalRow, interval[1] - 1);
		if(resultSize == 0 || (firstRow ==0 && lastRow == finalRow)){
			//the whole results
			return cacheResults;
		}
		int batchSize = lastRow - firstRow + 1;
		List<?>[] resultsPart = new List[batchSize];
		System.arraycopy(cacheResults.getResults(), firstRow, resultsPart, 0, batchSize);
		CacheResults newCacheResults = new CacheResults(resultsPart, firstRow + 1, lastRow == finalRow);
		newCacheResults.setCommand(cacheResults.getCommand());
		newCacheResults.setAnalysisRecord(cacheResults.getAnalysisRecord());
		return newCacheResults;
	}
	
	public boolean hasResults(CacheID cacheID){
		boolean hasResults = cache.get(cacheID) != null;
		return hasResults;	
	}
	
    /**
     * if valueID is not null, it is lob (XML document) chunk
     * @return true if the result was cachable 
     */
	public boolean setResults(CacheID cacheID, CacheResults cacheResults, Object requestID){	
		List<?>[] results = cacheResults.getResults();
		if(cacheResults.getSize() == TupleBatch.UNKNOWN_SIZE){
			cacheResults.setSize(SizeUtility.getBatchSize(BufferManagerImpl.getTypeNames(cacheResults.getElements()), results));
		}
		
		TempKey key = new TempKey(cacheID, requestID);
		//do not cache if it is over cache limit
		if(isOverCacheLimit(cacheResults.getSize())){
			removeTempResults(key);
			return false;
		}
	
		synchronized(tempBatchResults){
			CacheResults savedResults = tempBatchResults.get(key);
			if(savedResults == null){
				if (tempBatchResults.size() >= maxEntries) {
					return false;
				}
				savedResults = cacheResults; 
				tempBatchResults.put(key, cacheResults);
			} else if(!savedResults.addResults(cacheResults)){
				removeTempResults(key);
				return false;
			}
			
			//do not cache if it is over cache limit
			if(isOverCacheLimit(savedResults.getSize())){
				removeTempResults(key);
				return false;
			}
		
			if(savedResults.isFinal()){
				tempBatchResults.remove(cacheID);
				cache.put(cacheID, savedResults);
			}
		}
		
		return true;
	}

	private boolean isOverCacheLimit(long sizeToAdd) {
		if(maxEntrySize == 0 || sizeToAdd == 0){
			return false;
		}
		return sizeToAdd > maxEntrySize;
	}
	
	public void removeTempResults(CacheID cacheID, Object requestID){
		removeTempResults(new TempKey(cacheID, requestID));
	}

	public void removeTempResults(TempKey cacheID){
		synchronized(tempBatchResults){
			tempBatchResults.remove(cacheID);
		}
	}
	
	public void clear(){
		cache.clear();
		synchronized(tempBatchResults){
			tempBatchResults.clear();
		}
	}

	public void shutDown(){
		clear();
	}
	
	public String getCacheScope(){
		return scope;
	}

	public CacheID createCacheID(DQPWorkContext workContext, String command, List<?> parameterValues){
		String scopeID = null;
		if(RS_CACHE_SCOPE_VDB.equalsIgnoreCase(getCacheScope())){
			scopeID = workContext.getVdbName() + DELIMITOR + workContext.getVdbVersion();
		}else{
			scopeID = workContext.getConnectionID();
		}
		return new CacheID(scopeID, command, parameterValues);
	}

}
