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

package org.teiid.cache;

import java.util.Set;

/**
 * Abstraction over cache providers
 */
public interface Cache<K, V>  {
		
   /**
    * Retrieves the value for the given Key 
    *
    * @param key key under which value is to be retrieved.
    * @return returns data held under specified key in cache
    */
	V get(K key);
	
   /**
    * Associates the specified value with the specified key this cache.
    * If the cache previously contained a mapping for this key, the old value is replaced by the specified value.
    *
    * @param key   key with which the specified value is to be associated.
    * @param value value to be associated with the specified key.
    * @param ttl the time for this entry to live
    * @return previous value associated with specified key, or <code>null</code> if there was no mapping for key.
    *    	A <code>null</code> return can also indicate that the key previously associated <code>null</code> with the specified key, 
    *    	if the implementation supports null values.
    */
	V put(K key, V value, Long ttl);
	
   /**
    * Removes the value for this key from a Cache.
    * Returns the value to which the Key previously associated , or
    * <code>null</code> if the Key contained no mapping.
    *
    * @param key key whose mapping is to be removed
    * @return previous value associated with specified Node's key
    */	
	V remove(K key);
	
	/**
	 * Size of the cache 
	 * @return number of items in this cache
	 */
	int size();
	
	/**
	 * Removes all the keys and their values from the Cache
	 */
	void clear();
	   
	/**
	 * Name of the cache node
	 * @return
	 */
	String getName();
	
	/**
	 * Return all the keys
	 * @return
	 */
	Set<K> keys();
}
