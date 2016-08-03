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

package org.teiid.resource.adapter.infinispan.hotrod;


import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.resource.ResourceException;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.Search;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.query.dsl.QueryFactory;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.resource.spi.BasicConnection;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.infinispan.hotrod.InfinispanHotRodConnection;
import org.teiid.translator.infinispan.hotrod.InfinispanPlugin;
import org.teiid.translator.object.DDLHandler;
import org.teiid.translator.object.SearchType;


/** 
 * Represents a connection to an Infinispan cache container. The <code>cacheName</code> that is specified will dictate the
 * cache to be accessed in the container.
 * 
 */
public class InfinispanConnectionImpl extends BasicConnection implements InfinispanHotRodConnection { 
	
	AbstractInfinispanManagedConnectionFactory config = null;

	public InfinispanConnectionImpl(AbstractInfinispanManagedConnectionFactory config)  throws ResourceException {
		this.config = config;

		this.config.createCacheContainer();
		
		LogManager.logDetail(LogConstants.CTX_CONNECTOR, "Infinispan Connection has been newly created "); //$NON-NLS-1$
	}
	
	@Override
	public String getVersion() throws TranslatorException {
		return this.config.getVersion();
	}
	
	/** 
	 * Close the connection, if a connection requires closing.
	 * (non-Javadoc)
	 */
	@Override
    public void close() {
		config = null;	
	}

	/** 
	 * Will return <code>true</true> if the CacheContainer has been started.
	 * @return boolean true if CacheContainer has been started
	 */
	@Override
	public boolean isAlive() {
		boolean alive = (config == null ? false : config.isAlive());
		LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Infinispan Cache Connection is alive:", alive); //$NON-NLS-1$
		return (alive);
	}	

	@Override
	public Class<?> getCacheClassType() throws TranslatorException {		
		LogManager.logTrace(LogConstants.CTX_CONNECTOR, "=== GetType for cache :", getCacheName(),  "==="); //$NON-NLS-1$ //$NON-NLS-2$

		Class<?> type = config.getCacheClassType();
		if (type != null) {
			return type;
		}
		throw new TranslatorException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25040, getCacheName()));

	}
	
	@Override
	public Class<?> getCacheKeyClassType()  {
		return config.getCacheKeyClassType();
	}
	

	@SuppressWarnings({ "rawtypes"})
	@Override
	public RemoteCache getCache() throws TranslatorException {

		return config.getCache(getTargetCache());

	}

	/**
	 * {@inheritDoc}
	 *
	 */
	@Override
	public Descriptor getDescriptor()
			throws TranslatorException {
		Descriptor d = config.getContext().getMessageDescriptor(config.getMessageDescriptor());
		if (d == null) {
			throw new TranslatorException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25028,  config.getMessageDescriptor(), getCacheName()));			
		}
		
		return d;
	}
	
	@SuppressWarnings({ "rawtypes" })
	@Override
	public QueryFactory getQueryFactory() throws TranslatorException {
		
		return Search.getQueryFactory(getCache(getTargetCache()));
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#add(java.lang.Object, java.lang.Object)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void add(Object key, Object value) throws TranslatorException {
		getCache(getTargetCache()).put(key, value);
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#remove(java.lang.Object)
	 */
	@Override
	public Object remove(Object key) throws TranslatorException {
		return getCache(getTargetCache()).removeAsync(key);
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#update(java.lang.Object, java.lang.Object)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void update(Object key, Object value) throws TranslatorException {
		getCache(getTargetCache()).replace(key, value);
	}

	private String getTargetCache() {
		if (getDDLHandler().isStagingTarget()) {
			return config.getCacheNameProxy().getStageCacheAliasName();
		}
		return config.getCacheNameProxy().getPrimaryCacheAliasName();
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#getClassRegistry()
	 */
	@Override
	public org.teiid.translator.object.ClassRegistry getClassRegistry() {
		return config.getClassRegistry();
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#get(java.lang.Object)
	 */
	@Override
	public Object get(Object key) throws TranslatorException {
		return getCache(getTargetCache()).get(key);
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#getAll()
	 */
	@Override
	public Collection<Object> getAll() throws TranslatorException {
		@SuppressWarnings("rawtypes")
		RemoteCache cache = getCache(getTargetCache());

		Map<Object, Object> c = cache.getBulk();
		List<Object> results = new ArrayList<Object>();
		for (Object k:c.keySet()) {
			Object v = cache.get(k);
			results.add(v);
			
		}

		return results;
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#getDDLHandler()
	 */
	@Override
	public DDLHandler getDDLHandler() {
		return config.getCacheNameProxy().getDDLHandler();
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#getPkField()
	 */
	@Override
	public String getPkField() {
		return config.getPk();
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#getCacheName()
	 */
	@Override
	public String getCacheName() {
		return getTargetCache();
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#getCache(java.lang.String)
	 */
	@Override
	public RemoteCache getCache(String cacheName) {
		return config.getCache(cacheName);
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#clearCache(java.lang.String)
	 */
	@Override
	public void clearCache(String cacheName) throws TranslatorException {
		config.getCache(cacheName).clear();
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#getSearchType()
	 */
	@Override
	public SearchType getSearchType() {
		return new DSLSearch(this);
	}

}