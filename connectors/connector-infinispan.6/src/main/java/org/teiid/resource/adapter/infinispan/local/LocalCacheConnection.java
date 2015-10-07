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
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.QueryFactory;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.resource.adapter.infinispan.InfinispanCacheWrapper;
import org.teiid.resource.adapter.infinispan.InfinispanManagedConnectionFactory;

/**
 * This wrapper will contain a local Infinispan cache, and will the necessary logic
 * for looking up the cache by {@link #performJNDICacheLookup(String) jndiName}
 * 
 * @author vanhalbert
 * @param <K>
 * @param <V> 
 */
public class LocalCacheConnection<K,V>  implements InfinispanCacheWrapper<K,V> {

	private EmbeddedCacheManager ecm = null;
	private InfinispanManagedConnectionFactory config;

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
		return ecm.isRunning(config.getCacheName());
	}	

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.resource.adapter.infinispan.InfinispanCacheWrapper#getCache()
	 */
	@Override
	public Cache getCache() {
		if (config.getCacheName() == null) {
			return ecm.getCache();
		}
		return ecm.getCache(config.getCacheName());
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
		getCache().put(key, value);
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.resource.adapter.infinispan.InfinispanCacheWrapper#remove(java.lang.Object)
	 */
	@Override
	public Object remove(Object key)  {
		return getCache().removeAsync(key);
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.resource.adapter.infinispan.InfinispanCacheWrapper#update(java.lang.Object, java.lang.Object)
	 */
	@Override
	public void update(Object key, Object value) {
		getCache().replace(key, value);
	}

}