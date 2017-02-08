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

import java.util.Map;

import org.teiid.translator.TranslatorException;


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
	
	private String primaryCacheNameKey = null;
	
	private String stageCacheNameKey = null;
	
	private String aliasCacheName = null;
	
	private DDLHandler ddlHandler = null;
	
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
				
	}
	
	/** instantiated when materialization isnt being performed 
	 * @param primaryCacheName 
   **/
	public CacheNameProxy(String primaryCacheName) {
		if (primaryCacheName == null) {
			throw new IllegalArgumentException("Program error: primaryCacheNameKey must not be null");
		}

		this.primaryCacheNameKey = primaryCacheName;
		
		ddlHandler = new DDLHandler(this);
	}	
	
	public boolean isMaterialized() {
		if (this.stageCacheNameKey != null && this.aliasCacheName != null) {
			return true;
		}
		return false;
	}
	
	public DDLHandler getDDLHandler() {
		return this.ddlHandler;
	}
		
	public String getPrimaryCacheKey() {
		return this.primaryCacheNameKey;
	}
	
	public String getStageCacheKey() {
		return this.stageCacheNameKey;
	}
	
	public synchronized String getStageCacheAliasName(ObjectConnection conn) throws TranslatorException{
		Map<Object, Object> m = getAliasCache(conn);
		return (String) m.get(getStageCacheKey());
	}
	
	public synchronized String getPrimaryCacheAliasName(ObjectConnection conn) throws TranslatorException{
		if (this.isMaterialized()) {
			Map<Object, Object> m = getAliasCache(conn);
			return (String) m.get(getPrimaryCacheKey());	
		}
		return this.primaryCacheNameKey;
	}

	public synchronized String getCacheName(String cacheNameKey, ObjectConnection conn) throws TranslatorException{
		if (this.isMaterialized()) {
			Map<Object, Object> m = getAliasCache(conn);
			return (String) m.get(cacheNameKey);
		}
		return primaryCacheNameKey;
	}
	
	public String getAliasCacheName() {
		return this.aliasCacheName;
	}
	
	public synchronized void swapCacheNames(ObjectConnection conn) throws TranslatorException{
		Map<Object, Object> m = getAliasCache(conn);
		
		Object scn = m.get(stageCacheNameKey);
		Object pcn = m.get(primaryCacheNameKey);
		
		m.put(this.primaryCacheNameKey, scn);
		m.put(this.stageCacheNameKey, pcn);

	}
	
	public synchronized void ensureCacheNames(ObjectConnection conn) throws TranslatorException{
		
		Map<Object, Object> m = getAliasCache(conn);
		

		Object pcn = m.get(primaryCacheNameKey);
		if (pcn == null) {
			m.put(primaryCacheNameKey, primaryCacheNameKey);
			m.put(stageCacheNameKey, stageCacheNameKey);
			return;
		}
		Object scn = m.get(stageCacheNameKey);
		
		
		if (pcn.equals(scn)) {
			if (scn.equals(stageCacheNameKey)) {
				m.put(stageCacheNameKey, primaryCacheNameKey);
			} else {
				m.put(stageCacheNameKey, stageCacheNameKey);
			}
		}

	}
	
	private Map<Object, Object> getAliasCache(ObjectConnection conn) throws TranslatorException {
		return (Map<Object, Object>) conn.getCache(this.getAliasCacheName());
	}

}
