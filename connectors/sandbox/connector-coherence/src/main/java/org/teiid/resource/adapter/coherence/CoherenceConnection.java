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
package org.teiid.resource.adapter.coherence;

import java.util.List;

import com.tangosol.util.Filter;

import javax.resource.ResourceException;

/**
 * CoherenceConnection interface used by the Coherence Translator to obtain cached objects.
 * @author vhalbert
 *
 * TODO:  Add the ability to add/update objects in the cache
 */

public interface CoherenceConnection {
	
		
	/**
	 * Returns the objects from the Coherence Cache based on the <code>criteria</code> filter specified.
	 * @param criteria
	 * @return List of objects found in the cache.
	 * @throws ResourceException
	 */
	public List<Object> get(Filter criteria) throws ResourceException;
	
	/**
	 * Returns the name of the cache translator class name to use.
	 * @return String name of the cache translator class
	 */
	public String getCacheTranslatorClassName();
	
	/**
	 * Call to add a new top level object to the cache.
	 * @param key to the object in the cache
	 * @param object to be added to the cache
	 * @throws ResourceException
	 */
	public void add(Object key, Object object) throws ResourceException;
	
	
	/**
	 * Call to remove the object based on its <code>key</code> that was specified
	 * @param key of object to be removed
	 * @throws ResourceException
	 */
	public void remove(Object key) throws ResourceException;
	
	
	/**
	 * Call to update the root object in the cache.
	 * @param key is the key to the object in the cache
	 * @param object is the root object to be updated
	 * @throws ResourceException
	 */
	public void update(Object key, Object object) throws ResourceException;
	
}
