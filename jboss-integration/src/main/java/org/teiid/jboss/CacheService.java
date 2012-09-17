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
package org.teiid.jboss;

import org.jboss.msc.service.Service;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StartException;
import org.jboss.msc.service.StopContext;
import org.jboss.msc.value.InjectedValue;
import org.teiid.cache.CacheFactory;
import org.teiid.common.buffer.TupleBufferCache;
import org.teiid.dqp.internal.process.SessionAwareCache;
import org.teiid.dqp.internal.process.SessionAwareCache.Type;

class CacheService<T> implements Service<SessionAwareCache<T>> {
	
	private SessionAwareCache<T> cache;
	protected InjectedValue<TupleBufferCache> tupleBufferCacheInjector = new InjectedValue<TupleBufferCache>();
	protected InjectedValue<CacheFactory> cacheFactoryInjector = new InjectedValue<CacheFactory>();

	private SessionAwareCache.Type type;
	private String cacheName;
	private int maxStaleness;
	
	public CacheService(String cacheName, SessionAwareCache.Type type, int maxStaleness){
		this.cacheName = cacheName;
		this.type = type;
		this.maxStaleness = maxStaleness;
	}
	
	@Override
	public void start(StartContext context) throws StartException {
		this.cache = new SessionAwareCache<T>(this.cacheName, cacheFactoryInjector.getValue(), this.type, this.maxStaleness);
		if (type == Type.RESULTSET) {
			this.cache.setTupleBufferCache(this.tupleBufferCacheInjector.getValue());
		}
	}

	@Override
	public void stop(StopContext context) {
		this.cache = null;
	}

	@Override
	public SessionAwareCache<T> getValue() throws IllegalStateException, IllegalArgumentException {
		return this.cache;
	}
}
