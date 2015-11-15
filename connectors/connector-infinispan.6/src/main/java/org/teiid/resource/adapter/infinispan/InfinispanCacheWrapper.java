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

import javax.resource.ResourceException;

import org.infinispan.query.dsl.QueryFactory;
import org.teiid.translator.TranslatorException;

/**
 * @author vanhalbert
 * @param <K> 
 * @param <V>
 *
 */
public interface InfinispanCacheWrapper<K,V> {

	/**
	 * Called so the wrapper, with the configuration, can create the cache manager it will use
	 * @param config
	 * @throws ResourceException
	 */
	public void init(InfinispanManagedConnectionFactory config) throws ResourceException;

	/** 
	 * Called to pass in the cacheManager it will use.  This will be called when
	 * the CacheManager was registered via JNDI.  The factory makes the determination
	 * if its a local or remote cache and initializes the wrapper accordingly.
	 * @param config 
	 * @param cacheManager object
	 */
	public void init(InfinispanManagedConnectionFactory config, Object cacheManager);

	/** 
	 * Will return <code>true</true> if the CacheContainer has been started.
	 * @return boolean true if CacheContainer has been started
	 */
	public boolean isAlive();

	/** 
	 * Call to obtain Cache
	 * @return Map of cache
	 */
	public Map<Object, Object> getCache();

	/**
	 * Call to obtain all the objects from the cache
	 * @return List of all the objects in the cache
	 */
	public Collection<Object> getAll();


	public void cleanUp();

	/**
	 * @return QueryFactory
	 */
	public QueryFactory getQueryFactory();
	
	/**
	 *
	 * @param key 
	 * @return Object from cache
	 * @throws TranslatorException 
	 * @see org.teiid.translator.object.ObjectConnection#get(java.lang.Object)
	 */
	public Object get(Object key) throws TranslatorException;
	
	/**
	 * @param key 
	 * @param value 
	 * @throws TranslatorException 
	 *
	 * @see org.teiid.translator.object.ObjectConnection#add(java.lang.Object, java.lang.Object)
	 */
	public void add(Object key, Object value) throws TranslatorException;

	/**
	 *
	 * @param key 
	 * @return Object removed from cache
	 * @throws TranslatorException 
	 * @see org.teiid.translator.object.ObjectConnection#remove(java.lang.Object)
	 */
	public Object remove(Object key) throws TranslatorException;
	
	/**
	 *
	 * @param key 
	 * @param value 
	 * @throws TranslatorException 
	 * @see org.teiid.translator.object.ObjectConnection#update(java.lang.Object, java.lang.Object)
	 */
	public void update(Object key, Object value) throws TranslatorException;

}