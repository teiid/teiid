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

import java.util.HashSet;
import java.util.Set;

import org.jboss.cache.Cache;
import org.jboss.cache.Fqn;
import org.jboss.cache.Node;
import org.jboss.cache.eviction.ExpirationAlgorithmConfig;
import org.teiid.cache.DefaultCache;

public class ExpirationAwareCache<K, V> extends JBossCache<K, V> {

	public ExpirationAwareCache(Cache cacheStore, Fqn fqn) {
		super(cacheStore, fqn);
	}
	
	@Override
	protected boolean validateNode(Node node) {
		Long future = (Long) node.get(ExpirationAlgorithmConfig.EXPIRATION_KEY);
		return future == null || future > System.currentTimeMillis();
	}

	@Override
	public V put(K key, V value) {
		return this.put(key, value, null);
	}
	
	@Override
	public V put(K key, V value, Long ttl) {
		Node<K, V> node = getRootNode();
		Node child = node.addChild(getFqn(key));
		
		long future = DefaultCache.getExpirationTime(config.getMaxAgeInSeconds()*1000, ttl);				
		child.put(ExpirationAlgorithmConfig.EXPIRATION_KEY, future);
		return (V)child.put(key, value);
	}
	
	@Override
	public Set<K> keys() {
		HashSet keys = new HashSet();
		Node<K, V> node = getRootNode();
		Set<Node<K, V>> children = node.getChildren();
		for (Node<K, V> child:children) {
			for (K key:child.getData().keySet()) {
				if ((key instanceof String) && (key.equals(ExpirationAlgorithmConfig.EXPIRATION_KEY))) {
					continue;
				}
				keys.add(key);
			}
		}
		return keys;
	}	
}
