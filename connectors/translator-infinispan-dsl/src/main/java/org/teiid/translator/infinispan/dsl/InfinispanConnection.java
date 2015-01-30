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
package org.teiid.translator.infinispan.dsl;

import java.util.Map;

import org.infinispan.query.dsl.QueryFactory;
import org.teiid.translator.TranslatorException;

import org.infinispan.protostream.descriptors.Descriptor;


/**
 * Each InfinispanConnection implementation represents a connection to one or more
 * remote caches to be searched
 * 
 * @author vhalbert
 */
public interface InfinispanConnection {
	
		
	/**
	 * Return the root class type stored in the specified cache
	 * @param cacheName 
	 * @return Class
	 * @throws TranslatorException
	 */
	public Class<?> getType(String cacheName) throws TranslatorException;
	
	/**
	 * Returns the name of the primary key to the cache
	 * @param cacheName
	 * @return String key name
	 * @throws TranslatorException 
	 */
	public String getPkField(String cacheName) throws TranslatorException;
		
	/**
	 * Returns a map of all defined caches, and their respective root object class type,
	 * that are accessible using this connection.
	 * @return Map<String, Class>  --> CacheName, ClassType
	 * @throws TranslatorException 
	 */
	public Map<String, Class<?>> getCacheNameClassTypeMapping() throws TranslatorException;
	

	/**
	 * Returns the descriptor that desribes the messages being serialized
	 * @param cacheName
	 * @return Descriptor
	 * @throws TranslatorException
	 */
	public Descriptor getDescriptor(String cacheName) throws TranslatorException;
	
	/**
	 * Call to obtain the cache object
	 * @param cacheName
	 * @return Map cache
	 * @throws TranslatorException 
	 */
	public Map<Object, Object> getCache(String cacheName) throws TranslatorException;
	
	/**
	 * Return the QueryFactory used by the cache.
	 * @param cacheName
	 * @return QueryFactory
	 * @throws TranslatorException
	 */
	@SuppressWarnings("rawtypes")
	public QueryFactory getQueryFactory(String cacheName) throws TranslatorException;
	
	/**
	 * Return the ClassRegistry that contains which classes and their methods.
	 * @return ClassRegistry
	 */
	public ClassRegistry getClassRegistry();
	
	
}
