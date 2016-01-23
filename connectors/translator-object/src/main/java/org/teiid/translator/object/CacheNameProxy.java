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
	
	private Map<Object, Object> aliasMap = new HashMap<Object,Object>(2);
	
	private String primaryCacheNameKey = null;
	
	private String stageCacheNameKey = null;
	
	private String aliasCacheName = null;
	
	private Object matLock = new Object();
	
	public CacheNameProxy(String primaryCacheNameKey, String stageCacheNameKey, String aliasCacheName) {
		if (primaryCacheNameKey == null) {
			throw new IllegalArgumentException("Program error: primaryCacheNameKey must not be null");
		}
		if (stageCacheNameKey == null) {
			throw new IllegalArgumentException("Program error: stageCacheNameKey must not be null");
		}
		if (aliasCacheName == null) {
			throw new IllegalArgumentException("Program error: aliasCacheName must not be null");
		}
		
		setPrimaryCacheName(primaryCacheNameKey, primaryCacheNameKey);
		
		setStageCacheName(stageCacheNameKey, stageCacheNameKey);
		
		setAliasCacheName(aliasCacheName);
	}
	
	/** instantiated when materialization isnt being performed 
	 * @param primaryCacheName 
   **/
	public CacheNameProxy(String primaryCacheName) {
		setPrimaryCacheName(primaryCacheName, primaryCacheName);
	}
	
	/**
	 * Called to initialize.  If the cache already has the aliases, then set those values
	 * on the cacheNameProxy.   If they do not exist, generally the first time this runs, then update
	 * the cache with the defaults.
	 * @param cache
	 */
	public void initializeAliasCache(Map<Object, Object> cache) {
		String pcn = (String) cache.get(primaryCacheNameKey);
		if (pcn != null) {
			setPrimaryCacheName(primaryCacheNameKey, pcn);
		} else {
			cache.put(primaryCacheNameKey, primaryCacheNameKey);
			
		}

		String scn = (String) cache.get(stageCacheNameKey);
		if (scn != null) {
			setPrimaryCacheName(stageCacheNameKey, scn);
		} else {
			cache.put(stageCacheNameKey, stageCacheNameKey);
			
		}
	}
	
	public boolean useMaterialization() {
		return (this.stageCacheNameKey != null);
	}
	
	public String getPrimaryCacheKey() {
		return this.primaryCacheNameKey;
	}
	
	public String getStageCacheKey() {
		return this.stageCacheNameKey;
	}	
	
	public String getStageCacheAliasName() {
		return (String) aliasMap.get(getStageCacheKey());
	}
	
	public String getPrimaryCacheAliasName() {
		synchronized(matLock) {
			return (String) aliasMap.get(getPrimaryCacheKey());
		}		
	}

	public String getCacheName(String cacheNameKey) {
		return (String) aliasMap.get(cacheNameKey);
	}
	
	public String getAliasCacheName() {
		return this.aliasCacheName;
	}
	
	public void swapCacheNames( Map<Object, Object> cache){
		
		synchronized(matLock) {
			Object scn = cache.get(stageCacheNameKey);
			Object pcn = cache.get(primaryCacheNameKey);
		
			aliasMap.put(this.primaryCacheNameKey, scn);
			aliasMap.put(this.stageCacheNameKey, pcn);

			cache.put(primaryCacheNameKey, scn);
			cache.put(stageCacheNameKey, pcn);
		}	
	}
	
	private void setPrimaryCacheName(String key, String alias) {
		this.primaryCacheNameKey = key;
		aliasMap.put(key, alias);
	}

	private void setStageCacheName(String key, String alias) {
		// once set, cannot be changed, can only use swapCacheNames
		this.stageCacheNameKey = key;
		aliasMap.put(key, alias);
	}
	
	private void setAliasCacheName(String aliasCacheName) {		
		this.aliasCacheName = aliasCacheName;
	}	
}
