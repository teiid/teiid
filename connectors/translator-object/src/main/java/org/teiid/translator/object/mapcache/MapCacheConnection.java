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
package org.teiid.translator.object.mapcache;

import java.util.List;

import javax.resource.ResourceException;

import org.teiid.language.Select;
import org.teiid.resource.spi.BasicConnection;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.object.ObjectConnection;
import org.teiid.translator.object.infinispan.search.SearchByKey;
import org.teiid.translator.object.search.BasicKeySearchCriteria;

/**
 * The MapCacheConnection provides simple key searches of the cache.
 * 
 * @author vhalbert
 * 
 */
public class MapCacheConnection extends BasicConnection  implements ObjectConnection {

	private MapCacheExecutionFactory factory = null;
	private BasicKeySearchCriteria visitor = null;
	
	public MapCacheConnection(MapCacheExecutionFactory factory) {
		super();
		this.factory = factory;
	}
	
	@Override
	public boolean isAlive() {
		return true;
	}


	@Override
	public void cleanUp() {
		factory = null;
		visitor = null;
	}
	

	@Override
	public void close() throws ResourceException {
		cleanUp();
		
	}

	public List<Object> performSearch(Select command) throws TranslatorException {
		visitor = BasicKeySearchCriteria.getInstance(factory,command);
		return SearchByKey.get(visitor.getCriterion(), factory.getCache(), factory.getRootClass());
	}

}
