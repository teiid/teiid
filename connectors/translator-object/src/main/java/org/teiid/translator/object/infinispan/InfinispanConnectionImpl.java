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

package org.teiid.translator.object.infinispan;

import java.util.List;

import org.infinispan.api.BasicCache;
import org.teiid.language.Select;
import org.teiid.resource.spi.BasicConnection;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.object.ObjectConnection;
import org.teiid.translator.object.infinispan.search.LuceneSearch;
import org.teiid.translator.object.infinispan.search.SearchByKey;


/** 
 * Represents an instance of a connection to an Infinispan cache.  More than one connection can
 * be created to query to the cache.
 */
public  class InfinispanConnectionImpl extends BasicConnection implements ObjectConnection { 

	private InfinispanBaseExecutionFactory factory = null;
	
	protected InfinispanConnectionImpl() {
		super();
	}
	public InfinispanConnectionImpl(InfinispanBaseExecutionFactory factory) {
		super();
		this.factory = factory;
	}

	
	/** 
	 * Close the connection, if a connection requires closing.
	 * (non-Javadoc)
	 */
	@Override
    public void close() {
		factory = null;
	}

	/** 
	 * Currently, this method always returns alive. We assume the connection is alive,
	 * and rely on proper timeout values to automatically clean up connections before
	 * any server-side timeout occurs. Rather than incur overhead by rebinding,
	 * we'll assume the connection is always alive, and throw an error when it is actually used,
	 * if the connection fails. This may be a more efficient way of handling failed connections,
	 * with the one tradeoff that stale connections will not be detected until execution time. In
	 * practice, there is no benefit to detecting stale connections before execution time.
	 * 
	 * One possible extension is to implement a UnsolicitedNotificationListener.
	 * (non-Javadoc)
	 */
	public boolean isAlive() {
		return factory.isAlive();
	}


	@Override
	public void cleanUp() {
		factory = null;

	}
	
	public InfinispanBaseExecutionFactory getFactory() {
		return this.factory;
	}
	
	@Override
	public List<Object> performSearch(Select command)
			throws TranslatorException {
		if (factory.isFullTextSearchingSupported()) {
			return LuceneSearch.performSearch(command, this);
		}
	    return SearchByKey.performSearch(command, this);	
	}
	
	public BasicCache<String, Object> getCache()
			throws TranslatorException {
		return factory.getCache();

	}
	
	
}
