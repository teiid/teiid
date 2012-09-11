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
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.teiid.cache.CacheConfiguration.Policy;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.query.QueryPlugin;

@SuppressWarnings("nls")
public class DefaultCacheFactory implements CacheFactory, Serializable {
	private static final long serialVersionUID = -5541424157695857527L;
	private static CacheConfiguration DEFAULT = new CacheConfiguration(Policy.LRU, 60*60, 100, "default"); // 1 hours with 100 nodes. //$NON-NLS-1$
	
	public static DefaultCacheFactory INSTANCE = new DefaultCacheFactory(DEFAULT);
	
	private volatile boolean destroyed = false;
	EmbeddedCacheManager manager;
	
		
	public DefaultCacheFactory(CacheConfiguration config) {
		Configuration cacheConfig = new ConfigurationBuilder().eviction()
				.strategy(config.getPolicy()==Policy.LRU?EvictionStrategy.LRU:EvictionStrategy.NONE)
				.maxEntries(config.getMaxEntries())
				.expiration().lifespan(config.getMaxAgeInSeconds()*1000)
				.wakeUpInterval(200)
				.build();
		this.manager = new DefaultCacheManager(cacheConfig);
		this.manager.start();		
		this.manager.defineConfiguration("resultset", cacheConfig);
		this.manager.defineConfiguration("resultset-repl", cacheConfig);
		this.manager.defineConfiguration("preparedplan", cacheConfig);
		this.manager.getCache("resultset");
		this.manager.getCache("preparedplan");
		this.manager.getCache("resultset-repl");
	}
	
	@Override
	public void destroy() {
		this.destroyed = true;
		if (this.manager != null) {
			this.manager.stop();
		}
	}

	@Override
	public <K, V> Cache<K, V> get(String cacheName) {
		if (!destroyed) {
			manager.getCache(cacheName).clear();
			return new IspnCache(manager.getCache(cacheName), cacheName, getClass().getClassLoader());
		}
		 throw new TeiidRuntimeException(QueryPlugin.Event.TEIID30562, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30562));
	}
	
	@Override
	public boolean isReplicated() {
		return false;
	}


	private static  class IspnCache<K, V> implements Cache<K, V> {
	
		protected org.infinispan.AdvancedCache<K, V> cacheStore;
		private final String name; 
		private ClassLoader classloader;
		
		public IspnCache(org.infinispan.Cache<K, V> cacheStore, String cacheName, ClassLoader classloader) {
			assert(cacheStore != null);
			this.cacheStore = cacheStore.getAdvancedCache();
			this.name = cacheName;
			this.classloader = classloader;
		}
		
		@Override
		public V get(K key) {
			return this.cacheStore.with(this.classloader).get(key);
		}
		
		@Override
		public V put(K key, V value, Long ttl) {
			if (ttl != null) {
				return this.cacheStore.with(this.classloader).put(key, value, ttl, TimeUnit.MILLISECONDS);
			}
			return this.cacheStore.with(this.classloader).put(key, value);
		}
	
		@Override
		public V remove(K key) {
			return this.cacheStore.with(this.classloader).remove(key);
		}
		
		@Override
		public int size() {
			return this.cacheStore.with(this.classloader).size();
		}
		
		@Override
		public void clear() {
			this.cacheStore.with(this.classloader).clear();
		}
		
		@Override
		public String getName() {
			return this.name;
		}
	
		@Override
		public Set<K> keys() {
			return this.cacheStore.with(this.classloader).keySet();
		}
	}
}
