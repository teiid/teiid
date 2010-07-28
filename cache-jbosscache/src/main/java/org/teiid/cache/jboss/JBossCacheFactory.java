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

package org.teiid.cache.jboss;

import java.io.Serializable;

import org.jboss.cache.CacheManager;
import org.jboss.cache.Fqn;
import org.jboss.cache.Node;
import org.jboss.cache.Region;
import org.jboss.cache.config.EvictionAlgorithmConfig;
import org.jboss.cache.config.EvictionRegionConfig;
import org.jboss.cache.eviction.FIFOAlgorithmConfig;
import org.jboss.cache.eviction.LFUAlgorithmConfig;
import org.jboss.cache.eviction.LRUAlgorithmConfig;
import org.teiid.cache.Cache;
import org.teiid.cache.CacheConfiguration;
import org.teiid.cache.CacheFactory;
import org.teiid.cache.Cache.Type;
import org.teiid.cache.CacheConfiguration.Policy;
import org.teiid.core.TeiidRuntimeException;


public class JBossCacheFactory implements CacheFactory, Serializable{
	private static final long serialVersionUID = -2767452034178675653L;
	private transient org.jboss.cache.Cache cacheStore;
	private volatile boolean destroyed = false;
	

	public JBossCacheFactory(String name, Object cm) throws Exception {
		CacheManager cachemanager = (CacheManager)cm;
		this.cacheStore = cachemanager.getCache(name, true);
	}
	
	/**
	 * {@inheritDoc}
	 */
	public Cache get(Type type, CacheConfiguration config) {
		if (!destroyed) {
			
			if (!this.cacheStore.getCacheStatus().allowInvocations()) {
				this.cacheStore.start();
			}
			
			Node cacheRoot = this.cacheStore.getRoot().addChild(Fqn.fromString("Teiid")); //$NON-NLS-1$
			Node node = cacheRoot.addChild(Fqn.fromString(type.location()));
			
			Region cacheRegion = this.cacheStore.getRegion(node.getFqn(), true);
			EvictionRegionConfig erc = new EvictionRegionConfig(node.getFqn(), buildEvictionAlgorithmConfig(config));
			cacheRegion.setEvictionRegionConfig(erc);
			
			return new JBossCache(this.cacheStore, node.getFqn());
		}
		throw new TeiidRuntimeException("Cache system has been shutdown"); //$NON-NLS-1$
	}

	private EvictionAlgorithmConfig  buildEvictionAlgorithmConfig(CacheConfiguration config) {
		EvictionAlgorithmConfig  evictionConfig = null;
		
		if (config.getPolicy() == Policy.LRU) {
			LRUAlgorithmConfig lru = new LRUAlgorithmConfig();
			lru.setMaxNodes(config.getMaxNodes());
			lru.setMaxAge(config.getMaxAgeInSeconds()*1000);
			lru.setTimeToLive(-1); // -1 no limit
			evictionConfig = lru;
		}
		else if (config.getPolicy() == Policy.FIFO) {
			FIFOAlgorithmConfig fifo = new FIFOAlgorithmConfig();
			fifo.setMaxNodes(config.getMaxNodes());
			evictionConfig = fifo;
		}
		else if (config.getPolicy() == Policy.LFU) {
			LFUAlgorithmConfig lfu  = new LFUAlgorithmConfig();
			lfu.setMaxNodes(config.getMaxNodes());
			evictionConfig = lfu;
		}
		return evictionConfig;
	}    
	
	public void destroy() {
		this.destroyed = true;		
	}	
	
	// this will be called by microcontainer.
	public void stop() {
		destroy();
	}
}
