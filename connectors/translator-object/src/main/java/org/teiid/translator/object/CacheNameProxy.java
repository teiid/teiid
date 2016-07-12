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
package org.teiid.translator.object;

import java.util.HashMap;
import java.util.Map;


/**
 * The CacheNameProxy is used to map the aliasing of the primary and staging cache names, so that the caches can be swapped by reference.  This is different
 * than RDBMS materialilzation, where the data source table names are "renamed" to match what the model nameInSource names are.  Where, in this case, the cache doesn't
 * have the ability to perform renames, so the translator takes on the responsibility of aliasing the cache names and swapping the alias names that the nameInSource
 * names reference.
 *    
 * @author vanhalbert
 *
 */
public class CacheNameProxy {
	
	private Map<Object, Object> aliasMap = null;
	
	private String primaryCacheNameKey = null;
	
	private String stageCacheNameKey = null;
	
	private String aliasCacheName = null;
	
	private ObjectMaterializeLifeCycle materialize = null;
	
	public CacheNameProxy(String primaryCacheNameKey, String stageCacheNameKey, String aliasCacheName) {
		this(primaryCacheNameKey);

		if (stageCacheNameKey == null) {
			throw new IllegalArgumentException("Program error: stageCacheNameKey must not be null");
		}
		if (aliasCacheName == null) {
			throw new IllegalArgumentException("Program error: aliasCacheName must not be null");
		}
		
		this.stageCacheNameKey = stageCacheNameKey;
		this.aliasCacheName = aliasCacheName;
		
		initializeKey(this.stageCacheNameKey, this.stageCacheNameKey);
		
	}
	
	public CacheNameProxy(String primaryCacheNameKey, String stageCacheNameKey, String aliasCacheName, Map<Object, Object> aliasMap) {
		this(primaryCacheNameKey, aliasMap);
		
		if (stageCacheNameKey == null) {
			throw new IllegalArgumentException("Program error: stageCacheNameKey must not be null");
		}
		if (aliasCacheName == null) {
			throw new IllegalArgumentException("Program error: aliasCacheName must not be null");
		}
		
		this.stageCacheNameKey = stageCacheNameKey;
		this.aliasCacheName = aliasCacheName;
		
		initializeKey(this.stageCacheNameKey, this.stageCacheNameKey);
		
	}
	
	/** instantiated when materialization isnt being performed 
	 * @param primaryCacheName 
   **/
	public CacheNameProxy(String primaryCacheName) {
		if (primaryCacheName == null) {
			throw new IllegalArgumentException("Program error: primaryCacheNameKey must not be null");
		}

		this.primaryCacheNameKey = primaryCacheName;
		
		setAliasCache( new HashMap<Object,Object>(2) );
		
		initializeKey(primaryCacheNameKey, primaryCacheNameKey);
		
		materialize = new ObjectMaterializeLifeCycle(this);
	}
	
	/** instantiated when materialization isnt being performed 
	 * @param primaryCacheName 
	 * @param aliasMap 
   **/
	public CacheNameProxy(String primaryCacheName, Map<Object, Object> aliasMap) {
		if (primaryCacheName == null) {
			throw new IllegalArgumentException("Program error: primaryCacheNameKey must not be null");
		}
		
		if (aliasMap == null) {
			throw new IllegalArgumentException("Program error: aliasMap must not be null");
		}

		this.primaryCacheNameKey = primaryCacheName;
		
		setAliasCache( aliasMap );
		
		initializeKey(primaryCacheNameKey, primaryCacheNameKey);
		
		materialize = new ObjectMaterializeLifeCycle(this);
	}
	
	/**
	 * Called to initialize.  This can be used to reset the state of the proxy.
	 * @param cache
	 */
	public synchronized void initializeAliasCache(Map<Object, Object> cache) {
		this.aliasMap = cache;
		initializeKey(primaryCacheNameKey, primaryCacheNameKey);
		initializeKey(stageCacheNameKey, stageCacheNameKey);
	}
	
	public boolean isAliasCacheValid() {
		if (this.aliasMap == null) return false;
		if (this.aliasMap.isEmpty()) return false;
		if (this.aliasMap.get(this.primaryCacheNameKey) == null) return false;
		
		return true;
	}
	
	public boolean useMaterialization() {
		return (this.stageCacheNameKey != null);
	}
	
	public ObjectMaterializeLifeCycle getObjectMaterializeLifeCycle() {
		return this.materialize;
	}
		
	public String getPrimaryCacheKey() {
		return this.primaryCacheNameKey;
	}
	
	public String getStageCacheKey() {
		return this.stageCacheNameKey;
	}
	
	public synchronized String getStageCacheAliasName() {
		return (String) aliasMap.get(getStageCacheKey());
	}
	
	public synchronized String getPrimaryCacheAliasName() {
			return (String) aliasMap.get(getPrimaryCacheKey());	
	}

	public synchronized String getCacheName(String cacheNameKey) {
		return (String) aliasMap.get(cacheNameKey);
	}
	
	public String getAliasCacheName() {
		return this.aliasCacheName;
	}
	
	public synchronized void swapCacheNames(){
		
			Object scn = aliasMap.get(stageCacheNameKey);
			Object pcn = aliasMap.get(primaryCacheNameKey);
		
			aliasMap.put(this.primaryCacheNameKey, scn);
			aliasMap.put(this.stageCacheNameKey, pcn);

	}
	
	public synchronized void ensureCacheNames(){
		
		Object scn = aliasMap.get(stageCacheNameKey);
		Object pcn = aliasMap.get(primaryCacheNameKey);
		
		if (pcn.equals(scn)) {
			if (scn.equals(stageCacheNameKey)) {
				aliasMap.put(stageCacheNameKey, primaryCacheNameKey);
			} else {
				aliasMap.put(stageCacheNameKey, stageCacheNameKey);
			}
		}

	}
	
	// only set the key if its not already set, so that there is an initial state
	private void initializeKey(String key, String alias) {
		if (aliasMap.get(key) != null) return;
		
		aliasMap.put(key, alias);
	}
	
	private void setAliasCache(Map<Object, Object> aliasCache) {
		this.aliasMap = aliasCache;
	}
	
	public Map<Object, Object> getAliasCache() {
		return this.aliasMap;
	}

}
