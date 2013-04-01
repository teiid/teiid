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

package org.teiid.cache.infinispan;

import java.io.Serializable;

import org.infinispan.manager.EmbeddedCacheManager;
import org.teiid.cache.Cache;
import org.teiid.cache.CacheFactory;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.runtime.RuntimePlugin;

public class InfinispanCacheFactory implements CacheFactory, Serializable{
	private static final long serialVersionUID = -2767452034178675653L;
	private transient EmbeddedCacheManager cacheStore;
	private volatile boolean destroyed = false;
	private ClassLoader classLoader;
	
	public InfinispanCacheFactory(EmbeddedCacheManager cm, ClassLoader classLoader) {
		this.cacheStore = cm;
		this.classLoader = classLoader;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public <K, V> Cache<K, V> get(String cacheName) {
		if (!destroyed) {
			org.infinispan.Cache cache = this.cacheStore.getCache(cacheName, false);
			if (cache != null) {
				return new InfinispanCache<K, V>(cache, cacheName, this.classLoader);
			}
			return null;
		}
		throw new TeiidRuntimeException(RuntimePlugin.Event.TEIID40099, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40099));
	}
	
	public void destroy() {
		this.destroyed = true;		
		this.cacheStore.stop();
	}	
	
	public void stop() {
		destroy();
	}
	
}
