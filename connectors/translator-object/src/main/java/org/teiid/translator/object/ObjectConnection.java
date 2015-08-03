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

import java.util.Map;

import org.teiid.translator.TranslatorException;

/**
 * Each ObjectConnection implementation represents a connection to maps or caches to be searched
 * 
 * @author vhalbert
 *
 */
public interface ObjectConnection  {
	
	CacheContainerWrapper getCacheContainer() throws TranslatorException;
		
	/**
	 * Return the root class type stored in the specified cache
	 * @param cacheName 
	 * @return Class
	 * @throws TranslatorException
	 */
	Class<?> getType(String cacheName) throws TranslatorException;
	
	/**
	 * Returns the name of the primary key to the cache
	 * @param cacheName
	 * @return String key name
	 */
	String getPkField(String cacheName);
		
	/**
	 * Returns a map of all defined caches, and their respective root object class type,
	 * that are accessible using this connection.
	 * @return Map<String, Class>  --> CacheName, ClassType
	 */
	Map<String, Class<?>> getCacheNameClassTypeMapping();
	
	/**
	 * Return the ClassRegistry that contains which classes and their methods.
	 * @return ClassRegistry
	 */
	ClassRegistry getClassRegistry();	

	
	/** 
	 * Called to enable the connection to cleanup after use
	 */
	public void cleanUp();
	
}
