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


package com.metamatrix.connector.xml.cache;

import com.metamatrix.connector.exception.ConnectorException;

public interface IDocumentCache {

	public abstract void addToCache(String cacheKey, Object obj, int size,
			String id) throws ConnectorException;

	/**
	 * Called by the CachedObjectRecord contained in the connector to remove 
	 * a reference on a cache item.
	 * @param cacheKey
	 */
	public abstract void release(String cacheKey, String id);

	/**
	 * @param id The CacheKey of the cached object.
	 * @param id The ID of the request passed to the connector. 
	 */
	public abstract Object fetchObject(String cacheKey, String id);

	public abstract void clearCache();

	public abstract void shutdownCleaner();

}