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

package org.teiid.common.buffer;

import java.lang.ref.WeakReference;
import java.util.Collection;

import org.teiid.core.TeiidComponentException;

/**
 * Represents the storage strategy for the {@link BufferManager}
 */
public interface Cache<T> extends StorageManager {
	/**
	 * Must be called prior to adding any group entries
	 * @param gid
	 */
	void createCacheGroup(Long gid); 
	
	/**
	 * Remove an entire cache group
	 * 
	 * TODO: this should use a callback on the buffermangaer to remove memory entries
	 * without materializing all group keys
	 * @param gid
	 * @return
	 */
	Collection<Long> removeCacheGroup(Long gid);

	/**
	 * Must be called prior to adding an entry
	 * @param gid
	 * @param oid
	 * @return if the add was successful
	 */
	boolean addToCacheGroup(Long gid, Long oid); 

	/**
	 * Lock the object for load and return an identifier/lock
	 * that can be used to retrieve the object.
	 * @param oid
	 * @param serializer
	 * @return the identifier, may be null
	 */
	T lockForLoad(Long oid, Serializer<?> serializer);
	
	/**
	 * Must be called after lockForLoad
	 * @param o
	 */
	void unlockForLoad(T lock);

	/**
	 * Get method, must be called using the object obtained in the
	 * lockForLoad method
	 * @return
	 * @throws TeiidComponentException
	 */
	CacheEntry get(T lock, Long oid, WeakReference<? extends Serializer<?>> ref) throws TeiidComponentException;
	
	/**
	 * Adds an entry to the cache.
	 * @param entry
	 * @param s
	 * @throws Exception
	 */
	boolean add(CacheEntry entry, Serializer<?> s) throws Exception;
	
	/**
	 * Remove an entry from the cache
	 * @param gid
	 * @param id
	 */
	boolean remove(Long gid, Long id);
	
}