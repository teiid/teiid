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
package org.teiid.resource.adapter.infinispan;

import java.util.Collection;
import java.util.Map;

import org.infinispan.query.dsl.QueryFactory;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.resource.spi.BasicConnection;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.infinispan.cache.InfinispanCacheConnection;
import org.teiid.translator.object.ClassRegistry;

/**
 * @author vanhalbert
 *
 */
public class InfinispanCacheRAConnection extends BasicConnection
		implements InfinispanCacheConnection {

	private InfinispanCacheWrapper<?, ?> cacheWrapper;
	private InfinispanManagedConnectionFactory config;	
	
	public InfinispanCacheRAConnection(InfinispanManagedConnectionFactory config) {
		this.config = config;	
		this.cacheWrapper = config.getCacheWrapper();
		LogManager.logDetail(LogConstants.CTX_CONNECTOR, "Infinispan-Cache Connection has been created."); //$NON-NLS-1$				
	}
	
	protected InfinispanManagedConnectionFactory getConfig() {
		return config;
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#getCache()
	 */
	@Override
	public Map<Object, Object> getCache() {
		return cacheWrapper.getCache();
	}
	
	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#getAll()
	 */
	@Override
	public Collection<Object> getAll()  {
		return cacheWrapper.getAll();
	}
	
	
	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.infinispanCache.InfinispanCacheConnection#getQueryFactory()
	 */
	@Override
	public QueryFactory getQueryFactory() throws TranslatorException {
		return cacheWrapper.getQueryFactory();
	}


	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#getPkField()
	 */
	@Override
	public String getPkField() {
		return config.getPKey();
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#getCacheKeyClassType()
	 */
	@Override
	public Class<?> getCacheKeyClassType() {
		return config.getCacheKeyClassType();
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#getCacheName()
	 */
	@Override
	public String getCacheName()  {
		return config.getCacheName();
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#getCacheClassType()
	 */
	@Override
	public Class<?> getCacheClassType() {
		return config.getCacheClassType();
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#add(java.lang.Object, java.lang.Object)
	 */
	@Override
	public void add(Object key, Object value) throws TranslatorException {
		 this.cacheWrapper.add(key, value);
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#remove(java.lang.Object)
	 */
	@Override
	public Object remove(Object key) throws TranslatorException {
		return this.cacheWrapper.remove(key);
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#update(java.lang.Object, java.lang.Object)
	 */
	@Override
	public void update(Object key, Object value) throws TranslatorException {
		this.cacheWrapper.update(key, value);
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#getClassRegistry()
	 */
	@Override
	public ClassRegistry getClassRegistry() {
		return config.getClassRegistry();
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#get(java.lang.Object)
	 */
	@Override
	public Object get(Object key) throws TranslatorException {
		return this.cacheWrapper.get(key);
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see javax.resource.cci.Connection#close()
	 */
	@Override
	public void close() {
		this.cacheWrapper = null;
		this.config = null;
	}


}
