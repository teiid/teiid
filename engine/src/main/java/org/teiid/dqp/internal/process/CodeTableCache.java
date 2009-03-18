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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.core.util.HashCodeUtil;
import com.metamatrix.dqp.DQPPlugin;
import com.metamatrix.dqp.util.LogConstants;
import com.metamatrix.query.util.CommandContext;

/**
 * Code table cache.
 */
class CodeTableCache {
	
	// Max number of code tables that can be loaded
	private int maxCodeTables;
	
    // Caches being loaded - key is CacheKey, value is WaitingRequests
    private Map loadingCaches = new HashMap();

    // Map of RequestID/nodeID -> CacheKey
    private Map requestToCacheKeyMap = Collections.synchronizedMap(new HashMap());

    // Cache itself - key is CacheKey, value is Map (which is the key value -> return value for the code table)
    private Map codeTableCache = new HashMap();

    // Cache keys for stuff already in the cache  
    private Set cacheKeyDone = new HashSet();

    public enum CacheState {
		CACHE_EXISTS,
		CACHE_LOADING,
		CACHE_NOT_EXIST,
		CACHE_OVERLOAD
    }
    
	private AtomicInteger requestSequence = new AtomicInteger();

    /**
     * Construct a code table cache 
     */
    public CodeTableCache(int maxCodeTables) {
    	this.maxCodeTables = maxCodeTables;    	    
    }
     
    /**
     * Return the state of cache.
     * @param codeTable code table name
     * @param returnElement return element name
     * @param keyElement key element name
     * @param context context in processing
     * @param keyValye key value cached in data map
     * @return int of cache states
     */  
    public synchronized CacheState cacheExists(String codeTable, String returnElement, String keyElement, CommandContext context) {
        // Check whether CacheKey exist in cacheKeyDone:
        // If yes, return CACHE_EXISTS         
        // If no, does it exist in loadingCaches?
        //   If yes, add to additional contexts and return CACHE_LOADING
        //   If no, can we add another cache?
        //     If yes, add to loadingCaches as primary context, return CACHE_NOT_EXIST
        //     If no, return CACHE_OVERLOAD
        
        // Create a CacheKey
        CacheKey cacheKey = new CacheKey(codeTable, returnElement, keyElement, context.getVdbName(), context.getVdbVersion());
        
		if (cacheKeyDone.contains(cacheKey)) { // CacheKey exists in codeTableCache
			return CacheState.CACHE_EXISTS;
			
		}
		if (loadingCaches.containsKey(cacheKey)) { // CacheKey exists in loadingCache
			// Add context to additional contexts
			WaitingRequests wqr = (WaitingRequests) loadingCaches.get(cacheKey);
			wqr.addRequestID(context.getProcessorID());	
			loadingCaches.put(cacheKey, wqr);
			return CacheState.CACHE_LOADING;
			
		} else if(codeTableCache.size() + loadingCaches.size() >= maxCodeTables) { 
			// In this case we already have some number of existing + loading caches
			// that are >= the max number we are allowed to have.  Thus, we cannot load
			// another cache.
			return CacheState.CACHE_OVERLOAD;
				
		} else { // CacheKey not exists in loadingCache
			// Add to loadingCaches as primary context
			WaitingRequests wqr = new WaitingRequests(context.getProcessorID());
			loadingCaches.put(cacheKey, wqr);
			return CacheState.CACHE_NOT_EXIST;
		} 
    }

    /**
     * Set request ID for request key to cache key mapping. 
     * <Map: requestKey(requestID, nodeID) --> cacheKey(codeTable, returnElement, keyElement) >
     * @param codeTable Code table name
     * @param returnElement Return element name
     * @param keyElement Key element name
     * @param requestID Request ID
     * @param nodeID Plan Node ID
     */
    public Integer createCacheRequest(String codeTable, String returnElement, String keyElement, CommandContext context) {
        // Create a cache key
		CacheKey cacheKey = new CacheKey(codeTable, returnElement, keyElement, context.getVdbName(), context.getVdbVersion());
		Integer result = this.requestSequence.getAndIncrement();
		
		// Add requestID/nodeID pair to map for later lookup
		requestToCacheKeyMap.put(result, cacheKey);
		return result;
    }
    
    /**
     * Load all rows from the tuple source. Each row contains: keyElement and returnElement.
     * @param requestID Part of RequestKey
     * @param nodeID Part of RequestKey
     * @param results QueryResults of <List<List<keyValue, returnValue>>
     * @throws MetaMatrixProcessingException 
     */
    public synchronized void loadTable(Integer requestKey, List[] records) throws MetaMatrixProcessingException {
        // Look up cache key by requestID/nodeID pair
  		CacheKey cacheKey = (CacheKey) requestToCacheKeyMap.get(requestKey);

		// Lookup the existing data  
		// Map of data: keyValue --> returnValue;
		Map existingMap = (Map) codeTableCache.get(cacheKey);
		if(existingMap == null) {
			existingMap = new HashMap();
			codeTableCache.put(cacheKey, existingMap);
		}

        // Add data: <List<List<keyValue, returnValue>> from results to the code table cache
      	for ( int i = 0; i < records.length; i++ ) {
      		// each record or row
      		List record = records[i];
      		Object keyValue = record.get(0);
      		Object returnValue = record.get(1);
      		Object existing = existingMap.put(keyValue, returnValue);
      		if (existing != null) {
      			throw new MetaMatrixProcessingException(DQPPlugin.Util.getString("CodeTableCache.duplicate_key", cacheKey.getCodeTable(), cacheKey.getKeyElement(), keyValue)); //$NON-NLS-1$
      		}
      	}      	 
    }
    
    /**
     * Look up return value in code table cache given the key value.
     * @param codeTable Code Table name
     * @param returnElement Return element name
     * @param keyElement Key element name
     * @param keyValue Input key value
     * @return Object of return value in code table cache
     */ 
    public synchronized Object lookupValue(String codeTable, String returnElement, String keyElement, Object keyValue, CommandContext context) throws MetaMatrixComponentException {
		Object returnValue = null;
		        
        // Create CacheKey
        CacheKey cacheKey = new CacheKey(codeTable, returnElement, keyElement, context.getVdbName(), context.getVdbVersion());

        // Find the corresponding data map in cache for the cache key
        Map dataMap = (Map) codeTableCache.get(cacheKey);
        if(dataMap == null) {
            Object[] params = new Object[] {codeTable,keyElement,returnElement};
            throw new MetaMatrixComponentException(DQPPlugin.Util.getString("CodeTableCache.No_code_table", params)); //$NON-NLS-1$
        }
		returnValue = dataMap.get(keyValue);
        return returnValue;
    }
    
   /**
    * Places the lookup results in the cache and marks the cache loaded
    * @param requestID
    * @param nodeID
    * @return the set of waiting requests
    * @since 4.2
    */
    public Set markCacheLoaded(Integer requestKey) {
        return markCacheDone(requestKey, false);
    }
       
   /**
    * Notifies the CodeTableCache that this code table had an error. Removes any existing cached results and clears any state
    * for this CacheKey.
    * @param requestID
    * @param nodeID
    * @return the set of waiting requests
    * @since 4.2
    */
    public Set errorLoadingCache(Integer requestKey) {
        return markCacheDone(requestKey, true);
    }
   
    private synchronized Set markCacheDone(Integer requestKey, boolean errorOccurred) {
        // Remove request from requestToCacheKeyMap
        CacheKey cacheKey = (CacheKey) requestToCacheKeyMap.remove(requestKey);
        if (errorOccurred) {
            // Remove any results already cached
            codeTableCache.remove(cacheKey);
        } else {
            cacheKeyDone.add(cacheKey);
        }
          
        // Remove cache key from loadingCaches
        WaitingRequests waitingRequests = (WaitingRequests)loadingCaches.remove(cacheKey);
        if (waitingRequests != null) {
            return waitingRequests.getWaitingRequestIDs();
        } 
        return null;
    }
   
    public synchronized void clearAll() {
        // Look through the loaded caches and clear them - this is safe because
        // these are done.  There is a window where cacheExists() can be called and
        // return CACHE_EXISTS but then clearAll() could be called before lookValue()
        // is called.  We accept this as a possibility such that this is a risk when
        // clearing the cache - we need to throw an exception in this case to ensure the 
        // query fails (rather than return the wrong result).
        
        // Walk through every key in the done cache and remove it 
        int removedTables = 0;
        int removedRecords = 0;
        Iterator keyIter = cacheKeyDone.iterator();
        while(keyIter.hasNext()) {          
            CacheKey cacheKey = (CacheKey) keyIter.next();          
            Map codeTable = (Map) codeTableCache.remove(cacheKey);
            removedTables++;
            removedRecords += codeTable.size();
        }
        
        // Clear the cacheKeyDone
        cacheKeyDone.clear();
        
        // Log status
        LogManager.logInfo(LogConstants.CTX_DQP, DQPPlugin.Util.getString("CodeTableCache.Cleared_code_tables", new Object[]{new Integer(removedTables), new Integer(removedRecords)})); //$NON-NLS-1$
    }
        
	/**
	 * Cache Key consists: codeTable, returnElement and keyElement. 
	 */
    private static class CacheKey {
        private String codeTable;
        private String returnElement;
        private String keyElement;
        private String vdbName;
        private String vdbVersion;
        
        private int hashCode;
        
        public CacheKey(String codeTable, String returnElement, String keyElement, String vdbName, String vdbVersion) {
            this.codeTable = codeTable;
            this.returnElement = returnElement;
            this.keyElement = keyElement;
            this.vdbName = vdbName;
            this.vdbVersion = vdbVersion;
            
            // Compute hash code and cache it
            hashCode = HashCodeUtil.hashCode(0, codeTable);
            hashCode = HashCodeUtil.hashCode(hashCode, returnElement);
            hashCode = HashCodeUtil.hashCode(hashCode, keyElement);   
            hashCode = HashCodeUtil.hashCode(hashCode, vdbName);
            hashCode = HashCodeUtil.hashCode(hashCode, vdbVersion);
        }
        
        public String getCodeTable() {
        	return this.codeTable;
        }
        
        public String getReturnElement() {
        	return this.returnElement;
        }
        
        public String getKeyElement() {
        	return this.keyElement;	
        }
        
        public int hashCode() {
            return hashCode;
        }
        
        public boolean equals(Object obj) {
            if(this == obj) {
                return true;                
            }
            if(obj instanceof CacheKey) {
                CacheKey other = (CacheKey) obj;
                
                return (other.hashCode() == hashCode() &&
                    this.codeTable.equals(other.codeTable) &&
                    this.returnElement.equals(other.returnElement) && 
                    this.keyElement.equals(other.keyElement) &&
                    this.vdbName.equals(other.vdbName) &&
                    this.vdbVersion.equals(other.vdbVersion));
            }
            return false;
        }
    }
   
	/**
	 * Waiting Requests consist: primary requestID and list of additional waiting requestIDs. 
	 */ 
    private static class WaitingRequests {
        Object primaryRequestID;
        Set additionalRequestIDs;
        
        public WaitingRequests(Object requestID) {
            this.primaryRequestID = requestID; 
        }
        
        public void addRequestID(Object requestID) {
            if(additionalRequestIDs == null) {
                additionalRequestIDs = new HashSet(8, 0.9f);
            }
            additionalRequestIDs.add(requestID);
        }
    
        /**
        * Return the set of requestIDs for waiting requests.
        * @return Set of waiting requests' IDs
        */
       private Set getWaitingRequestIDs() {
           Set requestIDs = null;
            
           // Waiting Requests can contain both primary and additional context
           if (additionalRequestIDs != null) {
               requestIDs = new HashSet(additionalRequestIDs.size() + 1, 1.0f);
               requestIDs.addAll(additionalRequestIDs);
           } else {
               requestIDs = new HashSet(2, 1.0f);
           }
           if (primaryRequestID != null) {
               requestIDs.add(primaryRequestID);
           }
           
           return requestIDs;
       }
       
    }
    
}
