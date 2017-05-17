/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
