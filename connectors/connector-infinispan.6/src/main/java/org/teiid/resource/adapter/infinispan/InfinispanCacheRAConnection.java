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

import org.infinispan.query.dsl.QueryFactory;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.resource.spi.BasicConnection;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.infinispan.cache.InfinispanCacheConnection;
import org.teiid.translator.object.ClassRegistry;
import org.teiid.translator.object.DDLHandler;
import org.teiid.translator.object.SearchType;
import org.teiid.translator.object.Version;

/**
 * @author vanhalbert
 *
 */
public class InfinispanCacheRAConnection extends BasicConnection
		implements InfinispanCacheConnection {

	private InfinispanCacheWrapper<?, ?> cacheWrapper;
	
	public InfinispanCacheRAConnection(InfinispanManagedConnectionFactory config) {
		this.cacheWrapper = config.getCacheWrapper();
		LogManager.logDetail(LogConstants.CTX_CONNECTOR, "Infinispan-Cache Connection has been created."); //$NON-NLS-1$				
	}

	@Override
	public Object getCache() throws TranslatorException  {
		return cacheWrapper.getCache();
	}
	
	@Override
	public Collection<Object> getAll() throws TranslatorException {
		return cacheWrapper.getAll();
	}
	
	@Override
	public QueryFactory getQueryFactory() throws TranslatorException {
		return cacheWrapper.getQueryFactory();
	}

	@Override
	public String getPkField() {
		return cacheWrapper.getPkField();
	}

	@Override
	public Class<?> getCacheKeyClassType() {
		return cacheWrapper.getCacheKeyClassType();
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#getCacheName()
	 */
	@Override
	public String getCacheName()  {
		try {			
			return cacheWrapper.getConfig().getCacheNameProxy().getPrimaryCacheAliasName(this);
		} catch (TranslatorException te) {
			throw new RuntimeException(te);
		}
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#getCacheClassType()
	 */
	@Override
	public Class<?> getCacheClassType() {
		return cacheWrapper.getCacheClassType();
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
		return cacheWrapper.getClassRegistry();
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
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#getCache(java.lang.String)
	 */
	@Override
	public Object getCache(String cacheName) throws TranslatorException {
		return cacheWrapper.getCache(cacheName);
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#clearCache(java.lang.String)
	 */
	@Override
	public void clearCache(String cacheName) throws TranslatorException {
		cacheWrapper.clearCache(cacheName);
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#getDDLHandler()
	 */
	@Override
	public DDLHandler getDDLHandler() {
		return cacheWrapper.getDDLHandler();
	}

	/** 
	 * Returns the <code>SearchType</code> that will be used to perform
	 * dynamic searching of the cache.
	 * @return SearchType
	 */
	@Override
	public SearchType getSearchType() {
		return cacheWrapper.getSearchType();
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
