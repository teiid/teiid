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

import org.jboss.cache.Fqn;
import org.jboss.cache.Node;
import org.teiid.cache.Cache;
import org.teiid.cache.CacheConfiguration;
import org.teiid.cache.CacheListener;


/**
 * Implementation of Cache using JBoss Cache
 */
public class JBossCache<K, V> implements Cache<K, V> {

	protected org.jboss.cache.Cache<K, V> cacheStore;
	protected Fqn rootFqn;
	protected JBossCacheListener cacheListener;
	protected CacheConfiguration config;
	
	public JBossCache(org.jboss.cache.Cache<K, V> cacheStore, Fqn fqn) {
		this.cacheStore = cacheStore;
		this.rootFqn = fqn;
	}
	
	@Override
	public V get(K key) {
		Node<K, V> node = getRootNode();
		Node child = node.getChild(getFqn(key));
		if (child != null) {
			if (validateNode(child)) {
				return (V)child.get(key);
			}
			remove(key);
		}
		return null;
	}
	
	protected boolean validateNode(Node node) {
		return true;
	}

	protected Fqn<String> getFqn(K key) {
		if (key.getClass().isPrimitive() || key instanceof String) {
			return Fqn.fromString(String.valueOf(key));
		}
		return Fqn.fromString(String.valueOf(key.getClass().getSimpleName()+key.hashCode()));
	}

	public V put(K key, V value) {
		Node<K, V> node = getRootNode();
		Node<K, V> child = node.addChild(getFqn(key));
		return child.put(key, value);
	}
	
	@Override
	public V put(K key, V value, Long ttl) {
		return this.put(key, value);
	}

	@Override
	public V remove(K key) {
		Node<K, V> node = getRootNode();
		Fqn<String> fqn = getFqn(key);
		Node child = node.getChild(fqn);
		if (child != null) {
			V value = (V)child.remove(key);
			node.removeChild(fqn);
			return value;
		}
		return null;
	}
	
	@Override
	public int size() {
		Node<K, V> node = getRootNode();
		int size = 0;
		Set<Node<K,V>> nodes = new HashSet<Node<K, V>>(node.getChildren());
		for (Node<K, V> child : nodes) {
			if (!child.getData().isEmpty()) {
				size++;
			}
		}
		return size;
	}
	
	@Override
	public void clear() {
		Node<K, V> node = getRootNode();
		node.clearData();
		Set<Node<K,V>> nodes = new HashSet<Node<K, V>>(node.getChildren());
		for (Node<K, V> child : nodes) {
			child.clearData();
			node.removeChild(child.getFqn());
		}
	}
	
	public synchronized void addListener(CacheListener listener) {
		this.cacheListener = new JBossCacheListener(this.rootFqn, listener);
		this.cacheStore.addCacheListener(this.cacheListener);
	}

	public synchronized void removeListener() {
		this.cacheStore.removeCacheListener(this.cacheListener);
		this.cacheListener = null;	
	}

	protected Node<K, V> getRootNode() {
		Node<K, V> node = this.cacheStore.getNode(this.rootFqn);
		if (node == null) {
			throw new IllegalStateException("Cache Node "+ this.rootFqn +" not found."); //$NON-NLS-1$ //$NON-NLS-2$
		}
		return node;
	}

	@Override
	public String getName() {
		return this.rootFqn.toString();
	}

	void setCacheConfiguration(CacheConfiguration config) {
		this.config = config;
	}

	@Override
	public Set<K> keys() {
		HashSet keys = new HashSet();
		Node<K, V> node = getRootNode();
		Set<Node<K, V>> children = node.getChildren();
		for (Node<K, V> child:children) {
			keys.addAll(child.getData().keySet());
		}
		return keys;
	}
}
