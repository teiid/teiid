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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.dqp.DQPPlugin;
import org.teiid.logging.LogManager;
import org.teiid.query.util.CommandContext;
import org.teiid.vdb.runtime.VDBKey;


/**
 * Code table cache.  Heavily synchronized in-memory cache of code tables.  There is no purging policy for this cache.  Once the limits have been reached exceptions will occur.
 */
class CodeTableCache {
	
	private static class CodeTable {
		Map<Object, Object> codeMap;
		Set<Object> waitingRequests = new HashSet<Object>();
	}
	
    // Max number of code tables that can be loaded
	private int maxCodeTables;
	
	// Max number of code records that can be loaded
	private int maxCodeRecords;
	
	private int maxCodeTableRecords;
	
	private int rowCount;
	
    // Cache itself - key is CacheKey, value is Map (which is the key value -> return value for the code table)
    private Map<CacheKey, CodeTable> codeTableCache = new HashMap<CacheKey, CodeTable>();

    public enum CacheState {
		CACHE_EXISTS,
		CACHE_LOADING,
		CACHE_NOT_EXIST,
		CACHE_OVERLOAD
    }
    
    /**
     * Construct a code table cache 
     */
    public CodeTableCache(int maxCodeTables, int maxCodeRecords, int maxCodeTableRecords) {
    	this.maxCodeRecords = maxCodeRecords;   
    	this.maxCodeTables = maxCodeTables; 	    
    	this.maxCodeTableRecords = maxCodeTableRecords;
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
        CodeTable table = this.codeTableCache.get(cacheKey);
        if (table == null) {
        	if(codeTableCache.size() >= maxCodeTables) { 
    			// In this case we already have some number of existing + loading caches
    			// that are >= the max number we are allowed to have.  Thus, we cannot load
    			// another cache.
    			return CacheState.CACHE_OVERLOAD;
    		}
        	table = new CodeTable();
        	table.waitingRequests.add(context.getProcessorID());
        	this.codeTableCache.put(cacheKey, table);
        	return CacheState.CACHE_NOT_EXIST;
        }
		if (table.waitingRequests == null) { // CacheKey exists in codeTableCache
			return CacheState.CACHE_EXISTS;
		}
		// Add context to additional contexts
		table.waitingRequests.add(context.getProcessorID());
		return CacheState.CACHE_LOADING;
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
    public CacheKey createCacheRequest(String codeTable, String returnElement, String keyElement, CommandContext context) {
		return new CacheKey(codeTable, returnElement, keyElement, context.getVdbName(), context.getVdbVersion());
    }
    
    /**
     * Load all rows from the tuple source. Each row contains: keyElement and returnElement.
     * @param requestID Part of RequestKey
     * @param nodeID Part of RequestKey
     * @param results QueryResults of <List<List<keyValue, returnValue>>
     * @throws TeiidProcessingException 
     */
    public synchronized void loadTable(CacheKey cacheKey, List<List> records) throws TeiidProcessingException {
		// Lookup the existing data  
		// Map of data: keyValue --> returnValue;
		CodeTable table = codeTableCache.get(cacheKey);
		if(table.codeMap == null) {
			table.codeMap = new HashMap<Object, Object>();
		}
		
		// Determine whether the results should be added to code table cache
    	// Depends on size of results and available memory and system parameters
		int potentialSize = table.codeMap.size() + records.size();
    	if (potentialSize > maxCodeTableRecords) {
    		throw new TeiidProcessingException("ERR.018.005.0100", DQPPlugin.Util.getString("ERR.018.005.0100", "maxCodeTables")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$                  
    	}
    	
    	if (potentialSize + rowCount > maxCodeRecords) {
    		throw new TeiidProcessingException("ERR.018.005.0100", DQPPlugin.Util.getString("ERR.018.005.0100", "maxCodeTableRecords")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    	}
		
        // Add data: <List<List<keyValue, returnValue>> from results to the code table cache
    	for (List<Object> record : records) {
      		// each record or row
      		Object keyValue = record.get(0);
      		Object returnValue = record.get(1);
      		Object existing = table.codeMap.put(keyValue, returnValue);
      		if (existing != null) {
      			throw new TeiidProcessingException(DQPPlugin.Util.getString("CodeTableCache.duplicate_key", cacheKey.getCodeTable(), cacheKey.getKeyElement(), keyValue)); //$NON-NLS-1$
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
    public synchronized Object lookupValue(String codeTable, String returnElement, String keyElement, Object keyValue, CommandContext context) throws TeiidComponentException {
        // Create CacheKey
        CacheKey cacheKey = new CacheKey(codeTable, returnElement, keyElement, context.getVdbName(), context.getVdbVersion());

        // Find the corresponding data map in cache for the cache key
        CodeTable table = codeTableCache.get(cacheKey);
        if(table == null || table.codeMap == null) {
            throw new TeiidComponentException(DQPPlugin.Util.getString("CodeTableCache.No_code_table", cacheKey.codeTable,cacheKey.keyElement,cacheKey.returnElement)); //$NON-NLS-1$
        }
		return table.codeMap.get(keyValue);
    }
   
    /**
     * Notifies the CodeTableCache that this code is done.  If the table had an error, it removes any temporary results.
     * @return the set of waiting requests
     */
    public synchronized Set<Object> markCacheDone(CacheKey cacheKey, boolean success) {
        if (!success) {
            // Remove any results already cached
            CodeTable table = codeTableCache.remove(cacheKey);
            if (table != null) {
            	return table.waitingRequests;
            }
            return null;
        }
    	CodeTable table = codeTableCache.get(cacheKey);
    	if (table == null || table.codeMap == null) {
    		return null; //can only happen if cache was cleared between load and now
    	}
        rowCount += table.codeMap.size();
        Set<Object> waiting = table.waitingRequests;
        table.waitingRequests = null;
        return waiting;
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
        int removedRecords = this.rowCount;
        for (Iterator<CodeTable> iter = codeTableCache.values().iterator(); iter.hasNext();) {
        	CodeTable table = iter.next();
        	if (table.waitingRequests == null) {
                removedTables++;
                iter.remove();
        	}
        }
        
        // Clear the cacheKeyDone
        this.rowCount = 0;
        // Log status
        LogManager.logInfo(org.teiid.logging.LogConstants.CTX_DQP, DQPPlugin.Util.getString("CodeTableCache.Cleared_code_tables", removedTables, removedRecords)); //$NON-NLS-1$
    }
        
	/**
	 * Cache Key consists: codeTable, returnElement and keyElement. 
	 */
    static class CacheKey {
        private String codeTable;
        private String returnElement;
        private String keyElement;
        private VDBKey vdbKey;
        
        private int hashCode;
        
        public CacheKey(String codeTable, String returnElement, String keyElement, String vdbName, int vdbVersion) {
            this.codeTable = codeTable;
            this.returnElement = returnElement;
            this.keyElement = keyElement;
            this.vdbKey = new VDBKey(vdbName, vdbVersion);
            
            // Compute hash code and cache it
            hashCode = HashCodeUtil.hashCode(codeTable.toUpperCase().hashCode(), returnElement.toUpperCase(), 
            		keyElement.toUpperCase(), vdbKey);
        }
        
        public String getCodeTable() {
        	return this.codeTable;
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
                    this.codeTable.equalsIgnoreCase(other.codeTable) &&
                    this.returnElement.equalsIgnoreCase(other.returnElement) && 
                    this.keyElement.equalsIgnoreCase(other.keyElement) &&
                    this.vdbKey.equals(other.vdbKey));
            }
            return false;
        }
    }
   
}
