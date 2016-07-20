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

import javax.resource.ResourceException;

import org.teiid.translator.infinispan.libmode.InfinispanCacheConnection;
import org.teiid.translator.object.ClassRegistry;
import org.teiid.translator.object.DDLHandler;

/**
 * @author vanhalbert
 * @param <K> 
 * @param <V>
 *
 */
public abstract class InfinispanCacheWrapper<K,V> implements InfinispanCacheConnection {

	public abstract InfinispanManagedConnectionFactory getConfig();
	
	/**
	 * Called so the wrapper, with the configuration, can create the cache manager it will use
	 * @param config
	 * @throws ResourceException
	 */
	public abstract void init(InfinispanManagedConnectionFactory config) throws ResourceException;

	/** 
	 * Called to pass in the cacheManager it will use.  This will be called when
	 * the CacheManager was registered via JNDI.  The factory makes the determination
	 * if its a local or remote cache and initializes the wrapper accordingly.
	 * @param config 
	 * @param cacheManager object
	 */
	public abstract void init(InfinispanManagedConnectionFactory config, Object cacheManager);

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
	public String getCacheName() {
		return getConfig().getCacheName();
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
	
	/**
	 * Provided for testing purposes
	 */
	protected abstract void shutDownCacheManager();

}