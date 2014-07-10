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

package org.teiid.resource.adapter.infinispan.dsl.base;

import java.util.Map;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.dsl.QueryFactory;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.resource.spi.BasicConnection;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.infinispan.dsl.ClassRegistry;
import org.teiid.translator.infinispan.dsl.InfinispanConnection;
import org.teiid.translator.infinispan.dsl.InfinispanPlugin;

import com.google.protobuf.Descriptors.Descriptor;

/** 
 * Represents a connection to an Infinispan cache container. The <code>cacheName</code> that is specified will dictate the
 * cache to be accessed in the container.
 * 
 */
public class InfinispanConnectionImpl extends BasicConnection implements InfinispanConnection { 
	
	
	RemoteCacheManager rcm = null;
	SerializationContext ctx = null;
	AbstractInfinispanManagedConnectionFactory config = null;

	public InfinispanConnectionImpl(AbstractInfinispanManagedConnectionFactory config)   {
		this.config = config;
		
		this.rcm = config.getCacheContainer();
		this.ctx = config.getContext();

		LogManager.logDetail(LogConstants.CTX_CONNECTOR, "Infinispan Connection has been newly created."); //$NON-NLS-1$
	}
	
	/** 
	 * Close the connection, if a connection requires closing.
	 * (non-Javadoc)
	 */
	@Override
    public void close() {
		config = null;
		rcm = null;
		ctx = null;
	}

	/** 
	 * Will return <code>true</true> if the CacheContainer has been started.
	 * @return boolean true if CacheContainer has been started
	 */
	@Override
	public boolean isAlive() {
		boolean alive = (config == null ? false : config.isAlive());
		LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Infinispan Cache Connection is alive:", alive); //$NON-NLS-1$
		return (alive);
	}	

	@Override
	public Class<?> getType(String cacheName) throws TranslatorException {		
		LogManager.logTrace(LogConstants.CTX_CONNECTOR, "=== GetType for cache :", cacheName,  "==="); //$NON-NLS-1$ //$NON-NLS-2$

		Class<?> type = config.getCacheType(cacheName);
		if (type != null) {
			return type;
		}
		throw new TranslatorException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25040, (cacheName != null ? cacheName : "Default")));

	}
	
	@Override
	public String getPkField(String cacheName) {
		return this.config.getPkMap(cacheName);
	}

	@Override
	public Map<String, Class<?>> getCacheNameClassTypeMapping() {
		return this.config.getCacheNameClassTypeMapping();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public RemoteCache getCache(String cacheName) {

		if (cacheName == null) {
			return rcm.getCache();
		}
		return rcm.getCache(cacheName);

	}

	/**
	 * {@inheritDoc}
	 *
	 */
	@Override
	public Descriptor getDescriptor(String cacheName)
			throws TranslatorException {
		Descriptor d = ctx.getMessageDescriptor(config.getMessageDescriptor());
		if (d == null) {
			throw new TranslatorException(InfinispanPlugin.Util.gs(InfinispanPlugin.Event.TEIID25028,  config.getMessageDescriptor(), cacheName));			
		}
		
		return d;
	}
	
	@SuppressWarnings({ "unused", "rawtypes" })
	@Override
	public QueryFactory getQueryFactory(String cacheName) throws TranslatorException {
		
		return Search.getQueryFactory(getCache(cacheName));
	}
	
	@Override
	public  ClassRegistry getClassRegistry() {
		return config.getClassRegistry();
	}


}
