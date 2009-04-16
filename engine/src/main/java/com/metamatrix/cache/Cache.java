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

package com.metamatrix.cache;

import java.util.Collection;
import java.util.List;
import java.util.Set;


/**
 * Abstraction over cache providers
 */
public interface Cache<K, V>  {
	
	public enum Type {  REGISTRY("Registry"), //$NON-NLS-1$ 
						SESSION("Session"), //$NON-NLS-1$ 
						SESSION_MONITOR("Session-Monitor"), //$NON-NLS-1$ 
						AUTHORIZATION_POLICY("Authorization-Policy"), //$NON-NLS-1$ 
						AUTHORIZATION_PRINCIPAL("Auhtorization-Principal"), //$NON-NLS-1$ 
						RESULTSET("ResultSet"), //$NON-NLS-1$
						VDBMETADATA("VdbMetadata"), //$NON-NLS-1$
						VDBMODELS("VdbModels"), //$NON-NLS-1$
						SCOPED_CACHE("Scoped-Cache"); //$NON-NLS-1$
		
		private String location;
		
		Type(String location){
			this.location = location;
		}
		
		public String location() {
			return this.location;
		}
	}
	
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
    * @return previous value associated with specified key, or <code>null</code> if there was no mapping for key.
    *    	A <code>null</code> return can also indicate that the key previously associated <code>null</code> with the specified key, 
    *    	if the implementation supports null values.
    */
	V put(K key, V value);
	
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
    * Returns a {@link Set} containing the data in this Cache
    *
    * @return a {@link Set} containing the data in this Cache.  If there is no data, 
    * an empty {@link Set} is returned.  The {@link Set} returned is always immutable.
    */
	Set<K> keySet();
	
	/**
	 * Removes all the keys and their values from the Cache
	 */
	void clear();
	   
	/**
	 * Listener to get the updates on this Cache
	 * @param listener
	 */
	void addListener(CacheListener listener);
	
	/**
	 * Remove Listener to stop the updates on this Cache
	 * @param listener
	 */
	void removeListener();
	
	/**
     * Returns a {@link Collection} containing the data in this Cache
     *
     * @return a {@link Collection} containing the data in this Cache.  If there is no data, 
     * an empty {@link Collection} is returned.
     */
	Collection<V> values();
	
	
	/** 
	 * Add a child node to the current cache node
	 * @param name - name of the child
	 * @return Cache instance.
	 */
	Cache addChild(String name);
	
	/**
	 * Get the child cache node from the current node
	 * @param name
	 * @return null if not found
	 */
	Cache getChild(String name);
	
	/**
	 * Destroys the child from the current node; no-op if node not found
	 * @param name
	 * @return true if removed; false otherwise
	 */
	boolean removeChild(String name);
	
	
	/**
	 * Get child nodes under this cache node. If none found empty set is returned
	 * @return
	 */
	List<Cache> getChildren();
	
	/**
	 * Name of the cache node
	 * @return
	 */
	String getName();
}
