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
package org.teiid.cache;

import java.io.Serializable;

import org.teiid.cache.CacheConfiguration.Policy;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.LRUCache;
import org.teiid.query.QueryPlugin;

/**
 * Provides a non-thread safe simple map backed cache suitable for testing
 */
public class DefaultCacheFactory implements CacheFactory, Serializable {
	private static final long serialVersionUID = -5541424157695857527L;
	private static CacheConfiguration DEFAULT = new CacheConfiguration(Policy.LRU, 60*60, 100, "default"); // 1 hours with 100 nodes. //$NON-NLS-1$
	
	public static DefaultCacheFactory INSTANCE = new DefaultCacheFactory(DEFAULT);
	
	private volatile boolean destroyed = false;
	private CacheConfiguration config;
		
	public DefaultCacheFactory(CacheConfiguration config) {
		this.config = config;
	}
	
	@Override
	public void destroy() {
		this.destroyed = true;
	}

	@Override
	public <K, V> Cache<K, V> get(String cacheName) {
		if (!destroyed) {
			return new MockCache<K, V>(cacheName, config.getMaxEntries());
		}
		 throw new TeiidRuntimeException(QueryPlugin.Event.TEIID30562, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30562));
	}
	
	@SuppressWarnings("serial")
	private static class MockCache<K, V> extends LRUCache<K, V> implements Cache<K, V> {
	
		private String name;
		
		public MockCache(String cacheName, int maxSize) {
			super(maxSize<0?Integer.MAX_VALUE:maxSize);
			this.name = cacheName;
		}
		
		@Override
		public V put(K key, V value, Long ttl) {
			return put(key, value);
		}
	
		@Override
		public String getName() {
			return this.name;
		}
	
		@Override
		public boolean isTransactional() {
			return false;
		}
	}
}
