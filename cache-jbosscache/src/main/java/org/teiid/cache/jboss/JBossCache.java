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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
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
	
	public V get(K key) {
		return this.cacheStore.get(this.rootFqn, key);
	}

	public V put(K key, V value) {
		return this.cacheStore.put(this.rootFqn, key, value);
	}
	
	public V put(K key, V value, Long ttl) {
		return this.put(key, value);
	}

	public V remove(K key) {
		return this.cacheStore.remove(this.rootFqn, key);
	}

	public Set<K> keySet() {
		Node<K, V> node = this.cacheStore.getRoot().getChild(this.rootFqn);
		if (node != null) {
			return node.getKeys();
		}
		return Collections.emptySet();
	}
	
	public int size() {
		Node<K, V> node = this.cacheStore.getRoot().getChild(this.rootFqn);
		if (node != null) {
			return node.dataSize();
		}
		return 0;
	}
	
	public void clear() {
		Node<K, V> node = this.cacheStore.getRoot().getChild(this.rootFqn);
		if (node != null) {
			node.clearData();
		}
	}
	
	public Collection<V> values() {
		Node<K, V> node = this.cacheStore.getRoot().getChild(this.rootFqn);
		if (node != null) {
			return node.getData().values();
		}
		return Collections.emptyList();
	}
	
	/**
	 * {@inheritDoc}
	 */
	public synchronized void addListener(CacheListener listener) {
		this.cacheListener = new JBossCacheListener(this.rootFqn, listener);
		this.cacheStore.addCacheListener(this.cacheListener);
	}

	public synchronized void removeListener() {
		this.cacheStore.removeCacheListener(this.cacheListener);
		this.cacheListener = null;	
	}

	public Cache<K, V> addChild(String name) {
		Node<K, V> node = getRootNode();
		Node<K, V> childNode = node.addChild(Fqn.fromString(name));
		return new JBossCache<K, V>(this.cacheStore, childNode.getFqn());
	}

	public Cache<K, V> getChild(String name) {
		Node<K, V> node = getRootNode();
		Node<K, V> child = node.getChild(Fqn.fromString(name));
		if (child != null) {
			return new JBossCache<K, V>(this.cacheStore, child.getFqn());
		}
		return null;
	}

	protected Node<K, V> getRootNode() {
		Node<K, V> node = this.cacheStore.getNode(this.rootFqn);
		if (node == null) {
			throw new IllegalStateException("Cache Node "+ this.rootFqn +" not found."); //$NON-NLS-1$ //$NON-NLS-2$
		}
		return node;
	}
	
	public List<Cache> getChildren() {
		Node<K, V> node = getRootNode();
		Set<Node<K,V>> nodes = node.getChildren();
		if (nodes.isEmpty()) {
			return Collections.emptyList();
		}
		List<Cache> children = new ArrayList<Cache>();
		for(Node<K, V> child: nodes) {
			children.add(new JBossCache<K, V>(this.cacheStore, child.getFqn()));
		}
		return children;
	}

	public boolean removeChild(String name) {
		Node<K, V> node = getRootNode();
		return node.removeChild(Fqn.fromString(name));
	}

	@Override
	public String getName() {
		return this.rootFqn.toString();
	}

	void setCacheConfiguration(CacheConfiguration config) {
		this.config = config;
	}
}
