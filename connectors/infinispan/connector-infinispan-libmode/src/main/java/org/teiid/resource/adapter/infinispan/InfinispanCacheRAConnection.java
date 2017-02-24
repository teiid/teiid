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

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.resource.ResourceException;

import org.infinispan.Cache;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.QueryFactory;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.resource.spi.BasicConnection;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.infinispan.libmode.InfinispanCacheConnection;
import org.teiid.translator.object.ClassRegistry;
import org.teiid.translator.object.DDLHandler;
import org.teiid.translator.object.SearchType;
import org.teiid.util.Version;

/**
 * @author vanhalbert
 *
 */
public class InfinispanCacheRAConnection extends BasicConnection
		implements InfinispanCacheConnection {
	
	private InfinispanManagedConnectionFactory config;

	
	public InfinispanCacheRAConnection(InfinispanManagedConnectionFactory config) {
		this.config = config;
		LogManager.logDetail(LogConstants.CTX_CONNECTOR, "Infinispan-Library Mode Cache Connection has been created."); //$NON-NLS-1$				
	}
	
	public InfinispanManagedConnectionFactory getConfig() {
		return config;
	}
	
	@Override
	public Version getVersion() throws TranslatorException {
		return this.getConfig().getVersion();
	}
	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#getPkField()
	 */
	@Override
	public String getPkField() {
		return getConfig().getPKey();
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#getCacheKeyClassType()
	 */
	@Override
	public Class<?> getCacheKeyClassType()  {
		return getConfig().getCacheKeyClassType();
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#getCacheName()
	 */
	@Override
	public String getCacheName() throws TranslatorException {
		return getTargetCacheName();
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#getCacheClassType()
	 */
	@Override
	public Class<?> getCacheClassType()  {
		return getConfig().getCacheClassType();
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#getClassRegistry()
	 */
	@Override
	public ClassRegistry getClassRegistry() {
		return getConfig().getClassRegistry();
	}
	
	@Override
	public DDLHandler getDDLHandler() {
		 return getConfig().getCacheNameProxy().getDDLHandler();
	}	
	
	@Override
	public boolean isAlive() {		
		boolean alive = (config == null ? false :  this.config.isAlive());
		LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Infinispan Library Mode Connection is alive:", alive); //$NON-NLS-1$
		return (alive);

	}	
	
	private String getTargetCacheName() throws TranslatorException {
		if (getDDLHandler().isStagingTarget()) {
			return config.getCacheNameProxy().getStageCacheAliasName(this);
		}
		return config.getCacheNameProxy().getPrimaryCacheAliasName(this);//	public String getCacheName() {

	}

	public Cache<Object, Object> getCache() throws TranslatorException {
		return getCache(getTargetCacheName());
	}
	
	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.ObjectConnection#getCache(java.lang.String)
	 */
	@Override
	public Cache<Object, Object> getCache(String cacheName) throws TranslatorException {
		return config.getCache(cacheName);
	}	

	@Override
	public Collection<Object> getAll() throws TranslatorException {
		
		DSLSearch s = (DSLSearch) getSearchType();
		
		return s.getAll();

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
	
	@Override
	public void cleanUp() {
		config = null;

	}

	@SuppressWarnings("rawtypes")
	@Override
	public QueryFactory getQueryFactory() throws TranslatorException {

		return Search.getQueryFactory(getCache(getTargetCacheName()));

	}

	@Override
	public Object get(Object key) throws TranslatorException {
		return getCache(getTargetCacheName()).get(key);
	}

	@Override
	public void add(Object key, Object value) throws TranslatorException {
		getCache(getTargetCacheName()).put(key, value);
	}

	@Override
	public Object remove(Object key) throws TranslatorException {
		return getCache(getTargetCacheName()).removeAsync(key);
	}

	@Override
	public void update(Object key, Object value) throws TranslatorException {
		getCache(getTargetCacheName()).replace(key, value);
	}

	@Override
	public void clearCache(String cacheName) throws TranslatorException {	
		getCache(cacheName).clear();
	}
	
	protected void shutDownCacheManager() throws TranslatorException {
		getCache().stop();
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
	 * @see javax.resource.cci.Connection#close()
	 */
	@Override
	public void close() throws ResourceException {
	}
	

	/**
	* Call to determine if the JDG cache is configured using annotation (or using protobuf and marsharllers).
	* @return true if annotations are used
	*/

	@Override
	 public boolean configuredUsingAnnotations() {
	        return false;
	}

	@Override
	public boolean configuredForMaterialization() {
		return (config.getStagingCacheName() != null);
	}

}
