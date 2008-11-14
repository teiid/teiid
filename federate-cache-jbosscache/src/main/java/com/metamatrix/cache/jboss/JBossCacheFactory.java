/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.cache.jboss;

import org.jboss.cache.Fqn;
import org.jboss.cache.Node;
import org.jboss.cache.Region;
import org.jboss.cache.config.EvictionPolicyConfig;
import org.jboss.cache.eviction.FIFOConfiguration;
import org.jboss.cache.eviction.LFUConfiguration;
import org.jboss.cache.eviction.LRUConfiguration;
import org.jboss.cache.eviction.MRUConfiguration;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.metamatrix.cache.Cache;
import com.metamatrix.cache.CacheConfiguration;
import com.metamatrix.cache.CacheFactory;
import com.metamatrix.cache.Cache.Type;
import com.metamatrix.cache.CacheConfiguration.Policy;

@Singleton
public class JBossCacheFactory implements CacheFactory {
	private org.jboss.cache.Cache cacheStore;
	
	@Inject
	public JBossCacheFactory(org.jboss.cache.Cache cacheStore) {
		this.cacheStore = cacheStore;
	}

	/**
	 * {@inheritDoc}
	 */
	public Cache get(Type type, CacheConfiguration config) {
		Node cacheRoot = this.cacheStore.getRoot().addChild(Fqn.fromString("Federate")); //$NON-NLS-1$
		Node node = cacheRoot.addChild(Fqn.fromString(type.location()));
		
		
		Region cacheRegion = this.cacheStore.getRegion(node.getFqn(), true);
		cacheRegion.setEvictionPolicy(buildEvictionPolicy(config));
		
		return new JBossCache(this.cacheStore, node.getFqn());
	}

	private EvictionPolicyConfig buildEvictionPolicy(CacheConfiguration config) {
		EvictionPolicyConfig evictionConfig = null;
		
		if (config.getPolicy() == Policy.LRU) {
			LRUConfiguration lru = new LRUConfiguration();
			lru.setMaxNodes(config.getMaxNodes());
			lru.setMaxAgeSeconds(config.getMaxAgeInSeconds());
			evictionConfig = lru;
		}
		else if (config.getPolicy() == Policy.MRU) {
			MRUConfiguration mru = new MRUConfiguration();
			mru.setMaxNodes(config.getMaxNodes());
			evictionConfig = mru;
		}
		else if (config.getPolicy() == Policy.FIFO) {
			FIFOConfiguration fifo = new FIFOConfiguration();
			fifo.setMaxNodes(config.getMaxNodes());
			evictionConfig = fifo;
		}
		else if (config.getPolicy() == Policy.LFU) {
			LFUConfiguration lfu  = new LFUConfiguration();
			lfu.setMaxNodes(config.getMaxNodes());
			evictionConfig = lfu;
		}
		return evictionConfig;
	}    
}
