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

import java.util.Collection;
import java.util.Map;

import org.teiid.translator.TranslatorException;

/**
 * Each ObjectConnection implementation represents a connection to maps or caches to be searched
 * 
 * @author vhalbert
 *
 */
public interface ObjectConnection {

	/**
	 * Call to check the status of the connection
	 * @return boolean true if the connection is alive.
	 */
	public boolean isAlive();
	
	/**
	 * Call to obtain the cache object
	 * @return Map object cache
	 */
	public Map<Object, Object> getCache();  
		
	/**
	 * Returns the name of the primary key to the cache
	 * @return String key name
	 */
	public String getPkField();	
	
	/**
	 * Returns the class type of the key to the cache.
	 * If the primary key class type is different from the 
	 * cache key type, then the value will be converted
	 * to the cache key class type before performing a get/put on the cache.
	 * @return Class<?>
	 * @throws TranslatorException
	 */
	public Class<?> getCacheKeyClassType() throws TranslatorException;
	
	
	/**
	 * Returns the name of the cache
	 * @return String cacheName
	 * @throws TranslatorException 
	 */
	public String getCacheName() throws TranslatorException;
	
		
	/**
	 * Returns root object class type
	 * that is defined for the cache.
	 * @return Cache ClassType
	 * @throws TranslatorException 
	 */
	public Class<?> getCacheClassType() throws TranslatorException;
		
	
	/**
	 * Call to add an object to the cache
	 * @param key
	 * @param value
	 * @throws TranslatorException
	 */
	public void add(Object key, Object value) throws TranslatorException;
	
	
	/**
	 * Call to remove an object from the cache
	 * @param key
	 * @return Object that was removed
	 * @throws TranslatorException
	 */
	public Object remove(Object key) throws TranslatorException;
	
	/**
	 * Call to update an object in the cache
	 * @param key
	 * @param value
	 * @throws TranslatorException
	 */
	public void update(Object key, Object value)  throws TranslatorException;

	
	/** 
	 * Called to enable the connection to cleanup after use
	 */
	public void cleanUp();
	
	
	/**
	 * Return the ClassRegistry that contains which classes and their methods.
	 * @return ClassRegistry
	 */
	public ClassRegistry getClassRegistry();
	
	/**
	 * Call to obtain an object from the cache based on the specified key
	 * @param key to use to get the object from the cache
	 * @return Object
	 * @throws TranslatorException 
	 */
	public Object get(Object key) throws TranslatorException;
	
	/**
	 * Call to obtain all the objects from the cache
	 * @return List of all the objects in the cache
	 * @throws TranslatorException 
	 */
	public Collection<Object> getAll() throws TranslatorException;

	
}
