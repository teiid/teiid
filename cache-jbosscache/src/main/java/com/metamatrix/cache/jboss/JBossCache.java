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

package com.metamatrix.cache.jboss;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.jboss.cache.Fqn;
import org.jboss.cache.Node;

import com.metamatrix.cache.Cache;
import com.metamatrix.cache.CacheListener;

/**
 * Implementation of Cache using JBoss Cache
 */
public class JBossCache<K, V> implements Cache<K, V> {

	private org.jboss.cache.Cache<K, V> cacheStore;
	private Fqn rootFqn;
	private JBossCacheListener cacheListener;
	
	public JBossCache(org.jboss.cache.Cache<K, V> cacheStore, Fqn fqn) {
		this.cacheStore = cacheStore;
		this.rootFqn = fqn;
	}
	
	/**
	 * {@inheritDoc}
	 */
	public V get(K key) {
		return this.cacheStore.get(this.rootFqn, key);
	}

	/**
	 * {@inheritDoc}
	 */
	public V put(K key, V value) {
		return this.cacheStore.put(this.rootFqn, key, value);
	}

	/**
	 * {@inheritDoc}
	 */
	public V remove(K key) {
		return this.cacheStore.remove(this.rootFqn, key);
	}

	/**
	 * {@inheritDoc}
	 */
	public Set<K> keySet() {
		Node<K, V> node = this.cacheStore.getRoot().getChild(this.rootFqn);
		if (node != null) {
			return node.getKeys();
		}
		return Collections.emptySet();
	}
	
	/**
	 * {@inheritDoc}
	 */
	public int size() {
		Node<K, V> node = this.cacheStore.getRoot().getChild(this.rootFqn);
		if (node != null) {
			return node.dataSize();
		}
		return 0;
	}
	
	/**
	 * {@inheritDoc}
	 */
	public void clear() {
		Node<K, V> node = this.cacheStore.getRoot().getChild(this.rootFqn);
		if (node != null) {
			node.clearData();
		}
	}
	
	@Override
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

	/**
	 * {@inheritDoc}
	 */
	public synchronized void removeListener() {
		this.cacheStore.removeCacheListener(this.cacheListener);
		this.cacheListener = null;	
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Cache<K, V> addChild(String name) {
		Node<K, V> node = this.cacheStore.getNode(this.rootFqn);
		Node<K, V> childNode = node.addChild(Fqn.fromString(name));
		return new JBossCache<K, V>(this.cacheStore, childNode.getFqn());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Cache<K, V> getChild(String name) {
		Node<K, V> node = this.cacheStore.getNode(this.rootFqn);
		Node<K, V> child = node.getChild(Fqn.fromString(name));
		if (child != null) {
			return new JBossCache<K, V>(this.cacheStore, child.getFqn());
		}
		return null;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<Cache> getChildren() {
		Node<K, V> node = this.cacheStore.getNode(this.rootFqn);
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

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean removeChild(String name) {
		Node<K, V> node = this.cacheStore.getNode(this.rootFqn);
		return node.removeChild(Fqn.fromString(name));
	}

	@Override
	public String getName() {
		return this.rootFqn.toString();
	} 	
}
