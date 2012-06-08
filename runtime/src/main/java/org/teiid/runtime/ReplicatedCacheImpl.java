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

package org.teiid.runtime;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.Set;

import org.teiid.cache.Cache;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.query.ReplicatedObject;
import org.teiid.runtime.EmbeddedServer.ReplicatedCache;

class ReplicatedCacheImpl<K extends Serializable, V>
		implements ReplicatedCache<K, V>, ReplicatedObject<K> {
	private Cache<K, V> cache;

	public ReplicatedCacheImpl(Cache<K, V> cache) {
		this.cache = cache;
	}

	public void clear() {
		cache.clear();
	}

	public V get(K key) {
		return cache.get(key);
	}

	public String getName() {
		return cache.getName();
	}

	public Set<K> keys() {
		return cache.keys();
	}

	public V put(K key, V value, Long ttl) {
		return cache.put(key, value, ttl);
	}

	public V remove(K key) {
		return cache.remove(key);
	}

	public int size() {
		return cache.size();
	}

	@Override
	public void getState(K stateId, OutputStream ostream) {
		V value = get(stateId);
		if (value != null) {
			try {
				ObjectOutputStream oos = new ObjectOutputStream(ostream);
				oos.writeObject(value);
				oos.close();
			} catch (IOException e) {
				throw new TeiidRuntimeException(e);
			}
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void setState(K stateId, InputStream istream) {
		try {
			ObjectInputStream ois = new ObjectInputStream(istream);
			V value = (V) ois.readObject();
			this.put(stateId, value, null);
		} catch (IOException e) {
			throw new TeiidRuntimeException(e);
		} catch (ClassNotFoundException e) {
			throw new TeiidRuntimeException(e);
		}
	}

	@Override
	public boolean hasState(K stateId) {
		return cache.get(stateId) != null;
	}

	@Override
	public void droppedMembers(Collection<Serializable> addresses) {
	}

	@Override
	public void getState(OutputStream ostream) {
	}

	@Override
	public void setAddress(Serializable address) {
	}

	@Override
	public void setState(InputStream istream) {
	}

}