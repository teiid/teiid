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

package org.teiid.resource.adapter.infinispan;

import java.util.Map;
import java.util.List;

import org.teiid.language.Select;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.resource.spi.BasicConnection;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.object.ObjectConnection;
import org.teiid.translator.object.ObjectExecutionFactory;
import org.teiid.translator.object.SearchByKey;

import org.teiid.translator.object.infinispan.InfinispanExecutionFactory;

/** 
 * Represents a connection to an Infinispan cache container. The <code>cacheName</code> that is specified will dictate the
 * cache to be accessed in the container.
 * 
 */
public class InfinispanConnectionImpl extends BasicConnection implements ObjectConnection { 

	private InfinispanManagedConnectionFactory config;
	
	public InfinispanConnectionImpl(InfinispanManagedConnectionFactory config)  {
		this.config = config;		

		LogManager.logDetail(LogConstants.CTX_CONNECTOR, "Infinispan Connection has been newly created."); //$NON-NLS-1$
	}
	
	/** 
	 * Close the connection, if a connection requires closing.
	 * (non-Javadoc)
	 */
	@Override
    public void close() {
		config = null;
	}

	/** 
	 * Will return <code>true</true> if the CacheContainer has been started.
	 * @return boolean true if CacheContainer has been started
	 */
	public boolean isAlive() {
		boolean alive = (config == null ? false : config.isAlive());
		LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Infinispan Cache Connection is alive:", alive); //$NON-NLS-1$
		return (alive);
	}	
	
    @Override
	public Map<?, ?> getMap(String cacheName) throws TranslatorException {
    	Map<?,?> m = null;
		LogManager.logTrace(LogConstants.CTX_CONNECTOR, "=== GetMap for cache:", (cacheName != null ? cacheName : "Default"), "==="); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

		m = config.getCache(cacheName);
   		
		if (m == null) {
            final String msg = InfinispanPlugin.Util.getString("InfinispanConnection.cacheNotDefined", (cacheName != null ? cacheName : "Default") ); //$NON-NLS-1$ //$NON-NLS-2$
            throw new TranslatorException(msg);
		}
		
		return m;
	}

	@Override
	public Class<?> getType(String cacheName) throws TranslatorException {		
		LogManager.logTrace(LogConstants.CTX_CONNECTOR, "=== GetType for cache :", cacheName,  "==="); //$NON-NLS-1$ //$NON-NLS-2$

		Class<?> type = config.getCacheType(cacheName);
		if (type != null) {
			return type;
		}
        final String msg = InfinispanPlugin.Util.getString("InfinispanConnection.typeNotFound", (cacheName != null ? cacheName : "Default") ); //$NON-NLS-1$ //$NON-NLS-2$
        throw new TranslatorException(msg);
	}
	
	/**
	 * Returns a map of all defined caches, and their respective root object class type,
	 * that are accessible using this connection.
	 * @return Map<String, Class>
	 */
	@Override
	public Map<String, Class<?>> getCacheNameClassTypeMapping() {
		return this.config.getCacheNameClassTypeMapping();
	}
	
	@Override
	public String getPkField(String cacheName) {
		return this.config.getPkMap(cacheName);
	}
	
	@Override
	public List<Object> search(Select command, String cacheName, ObjectExecutionFactory factory) throws TranslatorException {
		
		if (  ( (InfinispanExecutionFactory)factory).supportsLuceneSearching()) {
			return LuceneSearch.performSearch(command, getMap(cacheName), getType(cacheName));
		}
		return SearchByKey.get(command.getWhere(), getMap(cacheName), getType(cacheName));
	}

}
