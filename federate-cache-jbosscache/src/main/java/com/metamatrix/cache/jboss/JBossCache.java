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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jboss.cache.Fqn;
import org.jboss.cache.Node;
import org.jboss.cache.notifications.annotation.NodeCreated;
import org.jboss.cache.notifications.annotation.NodeModified;
import org.jboss.cache.notifications.annotation.NodeMoved;
import org.jboss.cache.notifications.annotation.NodeRemoved;
import org.jboss.cache.notifications.event.NodeEvent;

import com.metamatrix.cache.Cache;
import com.metamatrix.cache.CacheListener;

/**
 * Implementation of Cache using JBoss Cache
 */
@org.jboss.cache.notifications.annotation.CacheListener
public class JBossCache<K, V> implements Cache<K, V> {

	private org.jboss.cache.Cache cacheStore;
	private Fqn rootFqn;
	private List<CacheListener> listeners;
	private Map<String, Cache> children;

	
	public JBossCache(org.jboss.cache.Cache cacheStore, Fqn fqn) {
		this.cacheStore = cacheStore;
		this.rootFqn = fqn;
	}
	
	/**
	 * {@inheritDoc}
	 */
	public V get(K key) {
		return (V)this.cacheStore.get(this.rootFqn, key);
	}

	/**
	 * {@inheritDoc}
	 */
	public V put(K key, V value) {
		return (V)this.cacheStore.put(this.rootFqn, key, value);
	}

	/**
	 * {@inheritDoc}
	 */
	public V remove(K key) {
		return (V)this.cacheStore.remove(this.rootFqn, key);
	}

	/**
	 * {@inheritDoc}
	 */
	public Set<K> keySet() {
		Node node = this.cacheStore.getRoot().getChild(this.rootFqn);
		if (node != null) {
			return node.getKeys();
		}
		return Collections.EMPTY_SET;
	}
	
	/**
	 * {@inheritDoc}
	 */
	public int size() {
		Node node = this.cacheStore.getRoot().getChild(this.rootFqn);
		if (node != null) {
			return node.dataSize();
		}
		return 0;
	}
	
	/**
	 * {@inheritDoc}
	 */
	public void clear() {
		Node node = this.cacheStore.getRoot().getChild(this.rootFqn);
		if (node != null) {
			node.clearData();
		}
	}
	
	@Override
	public Collection<V> values() {
		Node node = this.cacheStore.getRoot().getChild(this.rootFqn);
		if (node != null) {
			return node.getData().values();
		}
		return Collections.emptyList();
	}
	
	/**
	 * {@inheritDoc}
	 */
	public synchronized void addListener(CacheListener listener) {
		if (this.listeners == null) {
			listeners = new ArrayList(2);
		}
		this.listeners.add(listener);
	}

	/**
	 * {@inheritDoc}
	 */
	public synchronized void removeListener(CacheListener listener) {
		if (this.listeners != null) {
			this.listeners.remove(listener);
		}		
	}

	
    @NodeCreated
	@NodeRemoved
	@NodeModified
	@NodeMoved
	public synchronized void cacheChanged(NodeEvent ne) {
    	if (listeners != null) {
	    	Fqn fqn = ne.getFqn();
	    	if (fqn.isChildOrEquals(rootFqn)) {
	    		for(CacheListener listener:this.listeners) {
	    			listener.cacheChanged();
	    		}
	    	}
    	}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Cache addChild(String name) {
		Node node = this.cacheStore.getNode(this.rootFqn);
		Node childNode = node.addChild(Fqn.fromString(name));
		if (this.children == null) {
			this.children = Collections.synchronizedMap(new HashMap<String, Cache>());
		}
		Cache child = new JBossCache(this.cacheStore, childNode.getFqn());
		this.children.put(name, child);
		return child;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Cache getChild(String name) {
		return this.children.get(name);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public Collection<Cache> getChildren() {
		if (this.children == null) {
			return Collections.EMPTY_SET;
		}
		return this.children.values();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Cache removeChild(String name) {
		if (this.children == null) {
			return null;
		}
		Node node = this.cacheStore.getNode(this.rootFqn);
		node.removeChild(Fqn.fromString(name));
		return this.children.remove(name);
	} 	
}
