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
package org.teiid.resource.adapter.infinispan.local;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.resource.ResourceException;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.configuration.cache.Configuration;
//import org.infinispan.;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.QueryFactory;
import org.teiid.core.util.Assertion;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.resource.adapter.infinispan.DSLSearch;
import org.teiid.resource.adapter.infinispan.InfinispanCacheWrapper;
import org.teiid.resource.adapter.infinispan.InfinispanManagedConnectionFactory;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.object.SearchType;
import org.teiid.translator.object.Version;

/**
 * This wrapper will contain a local Infinispan cache, and will the necessary logic
 * for looking up the cache by {@link #performJNDICacheLookup(String) jndiName}
 * 
 * @author vanhalbert
 * @param <K>
 * @param <V> 
 */
public class LocalCacheConnection<K,V>  extends InfinispanCacheWrapper<K,V> {

	private EmbeddedCacheManager ecm = null;
	private InfinispanManagedConnectionFactory config;

	@Override
	public InfinispanManagedConnectionFactory getConfig() {
		return config;
	}

	@Override
	public void init(InfinispanManagedConnectionFactory config, Object cacheManager) {

		this.config = config;
		ecm = (EmbeddedCacheManager) cacheManager;
	}
	
	/**
	 */
	@Override
	public void init(InfinispanManagedConnectionFactory config) throws ResourceException {
		this.config = config;

		if (config.getConfigurationFileNameForLocalCache() != null) {
			try {
				 DefaultCacheManager cc = new DefaultCacheManager(config.getConfigurationFileNameForLocalCache());
				
				LogManager
				.logInfo(LogConstants.CTX_CONNECTOR,
						"=== Using DefaultCacheManager (loaded by configuration) ==="); //$NON-NLS-1$

				ecm = cc;
				
				Configuration conf = cc.getCacheConfiguration(config.getCacheNameProxy().getPrimaryCacheAliasName(this));
				if (conf == null) {
					throw new ResourceException("Program Error: cache " +  config.getCacheNameProxy().getPrimaryCacheAliasName(this) + " was not configured");
				}
				conf.module(config.getCacheClassType());
				
				if (config.getCacheNameProxy().isMaterialized()) {
					conf = cc.getCacheConfiguration(config.getCacheNameProxy().getStageCacheAliasName(this));
					if (conf == null) {
						throw new ResourceException("Program Error: cache " +  config.getCacheNameProxy().getStageCacheAliasName(this) + " was not configured");
					}
					
					conf.module(config.getCacheClassType());
				}
			} catch (TranslatorException te) {
				throw new RuntimeException(te);
			} catch (IOException e) {
				throw new ResourceException(e);
			}
		} else {
			if (ecm == null) {
				throw new ResourceException("Program Error: DefaultCacheManager was not configured");
			}
		}

	}
	
	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.resource.adapter.infinispan.InfinispanCacheWrapper#isAlive()
	 */
	@Override
	public boolean isAlive() {		
		if (ecm == null) return false;
		return ecm.isRunning(getTargetCacheName());
	}	
	

	/**getTargetCache()
	 * {@inheritDoc}
	 *
	 * @see org.teiid.resource.adapter.infinispan.InfinispanCacheWrapper#getCache()
	 */
	@Override
	public Cache getCache() {	
		String name = getTargetCacheName();
		if (name == null) {
			return ecm.getCache();
		}
		
		return getCache(name);
	}
	
	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#getCache(java.lang.String)
	 */
	@Override
	public Cache<Object, Object> getCache(String cacheName) {
		return ecm.getCache(cacheName);
	}	

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.resource.adapter.infinispan.InfinispanCacheWrapper#getAll()
	 */
	@Override
	public Collection<Object> getAll() {
		Collection<Object> objs = new ArrayList<Object>();
		Cache c = getCache();
		for (Object k : c.keySet()) {
			objs.add(c.get(k));
		}
		return objs;
	}

	/* split out for testing purposes */
	protected Object performJNDICacheLookup(String jndiName) throws Exception {
		Context context = null;

		context = new InitialContext();
		final Object cache = context.lookup(jndiName);

		if (cache == null) {
			throw new ResourceException(
					InfinispanManagedConnectionFactory.UTIL
							.getString(
									"InfinispanManagedConnectionFactory.unableToFindCacheUsingJNDI", jndiName)); //$NON-NLS-1$
		}

		return cache;
	}
	
	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.resource.adapter.infinispan.InfinispanCacheWrapper#cleanUp()
	 */
	@Override
	public void cleanUp() {
		ecm = null;
		config = null;

	}

	@Override
	public void forceCleanUp() {
		cleanUp();
		
	}
	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.resource.adapter.infinispan.InfinispanCacheWrapper#getQueryFactory()
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public QueryFactory getQueryFactory() {

		return Search.getQueryFactory(getCache());

	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.resource.adapter.infinispan.InfinispanCacheWrapper#get(java.lang.Object)
	 */
	@Override
	public Object get(Object key)  {
		return getCache().get(key);
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.resource.adapter.infinispan.InfinispanCacheWrapper#add(java.lang.Object, java.lang.Object)
	 */
	@Override
	public void add(Object key, Object value)  {
		getCache(getTargetCacheName()).put(key,value);
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.resource.adapter.infinispan.InfinispanCacheWrapper#remove(java.lang.Object)
	 */
	@Override
	public Object remove(Object key)  {
		return getCache(getTargetCacheName()).remove(key);
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.resource.adapter.infinispan.InfinispanCacheWrapper#update(java.lang.Object, java.lang.Object)
	 */
	@Override
	public void update(Object key, Object value) {
		getCache(getTargetCacheName()).replace(key, value);

	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#clearCache(java.lang.String)
	 */
	@Override
	public void clearCache(String cacheName) throws TranslatorException {	
		getCache(cacheName).clear();

	}
	
	/**
	 * Note:  This is used in testing only to enable shutting down the cache so that the next test can recreate it
	 * {@inheritDoc}
	 *
	 * @see org.teiid.resource.adapter.infinispan.InfinispanCacheWrapper#shutDownCacheManager()
	 */
	@Override
	protected void shutDownCacheManager() {
		ecm.stop();
	}
	
	/** 
	 * Returns the <code>SearchType</code> that will be used to perform
	 * dynamic searching of the cache.
	 * @return SearchType
	 */
	@Override
	public SearchType getSearchType() {
		return new DSLSearch(this);
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#getVersion()
	 */
	@Override
	public Version getVersion() throws TranslatorException {
		return null;
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#configuredUsingAnnotations()
	 */
	@Override
	public boolean configuredUsingAnnotations() {
		return false;
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#configuredForMaterialization()
	 */
	@Override
	public boolean configuredForMaterialization() {
		return false;
	}

}
