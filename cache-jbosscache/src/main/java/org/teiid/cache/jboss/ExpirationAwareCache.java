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

import org.jboss.cache.Cache;
import org.jboss.cache.Fqn;
import org.jboss.cache.Node;
import org.jboss.cache.eviction.ExpirationAlgorithmConfig;

public class ExpirationAwareCache<K, V> extends JBossCache<K, V> {

	public ExpirationAwareCache(Cache cacheStore, Fqn fqn) {
		super(cacheStore, fqn);
	}

	@Override
	public V get(K key) {
		Node<K, V> node = getRootNode();
		Node child = node.getChild(Fqn.fromString(String.valueOf(key.getClass().getSimpleName()+key.hashCode())));
		if (child != null) {
			return (V)child.get(key);
		}
		return super.get(key);
	}

	@Override
	public V put(K key, V value) {
		Node<K, V> node = getRootNode();
		Node child = node.addChild(Fqn.fromString(String.valueOf(key.getClass().getSimpleName()+key.hashCode())));
		Long future = new Long(System.currentTimeMillis() + (config.getMaxAgeInSeconds()*1000));				
		child.put(ExpirationAlgorithmConfig.EXPIRATION_KEY, future);
		return (V)child.put(key, value);
	}

	@Override
	public org.teiid.cache.Cache<K, V> addChild(String name) {
		Node<K, V> node = getRootNode();
		Node child = node.addChild(Fqn.fromString(name));
		child.put(ExpirationAlgorithmConfig.EXPIRATION_KEY, Long.MAX_VALUE);		
		return new JBossCache<K, V>(this.cacheStore, child.getFqn());
	}

}
