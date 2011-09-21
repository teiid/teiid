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

package org.teiid.common.buffer.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.teiid.common.buffer.Cache;
import org.teiid.common.buffer.CacheEntry;
import org.teiid.common.buffer.FileStore;
import org.teiid.common.buffer.Serializer;
import org.teiid.core.TeiidComponentException;


public class MemoryStorageManager implements Cache {
	
	public static final int MAX_FILE_SIZE = 1 << 17;
    
	private final class MemoryFileStore extends FileStore {
		private ByteBuffer buffer = ByteBuffer.allocate(MAX_FILE_SIZE);
		
		public MemoryFileStore() {
			buffer.limit(0);
		}

		@Override
		public synchronized void removeDirect() {
			removed.incrementAndGet();
			buffer = ByteBuffer.allocate(0);
		}
		
		@Override
		protected synchronized int readWrite(long fileOffset, byte[] b, int offSet,
				int length, boolean write) {
			if (!write) {
				if (fileOffset >= getLength()) {
					return -1;
				}
				int position = (int)fileOffset;
				buffer.position(position);
				length = Math.min(length, (int)getLength() - position);
				buffer.get(b, offSet, length);
				return length;	
			}
			int requiredLength = (int)(fileOffset + length);
			if (requiredLength > buffer.limit()) {
				buffer.limit(requiredLength);
			}
			buffer.position((int)fileOffset);
			buffer.put(b, offSet, length);
			return length;
		}

		@Override
		public synchronized void setLength(long length) {
			buffer.limit((int)length);
		}
		
		@Override
		public synchronized long getLength() {
			return buffer.limit();
		}

	}

	private Map<Long, Map<Long, CacheEntry>> groups = new ConcurrentHashMap<Long, Map<Long, CacheEntry>>();
	private AtomicInteger created = new AtomicInteger();
	private AtomicInteger removed = new AtomicInteger();
	
    public void initialize() {
    }

	@Override
	public FileStore createFileStore(String name) {
		created.incrementAndGet();
		return new MemoryFileStore();
	}
	
	public int getCreated() {
		return created.get();
	}
	
	public int getRemoved() {
		return removed.get();
	}
	
	@Override
	public void add(CacheEntry entry, Serializer<?> s) {
		Map<Long, CacheEntry> group = groups.get(s.getId());
		if (group != null) {
			group.put(entry.getId(), entry);
		}
	}
	
	@Override
	public void addToCacheGroup(Long gid, Long oid) {
		Map<Long, CacheEntry> group = groups.get(gid);
		if (group != null) {
			group.put(oid, null);
		}
	}
	
	@Override
	public void createCacheGroup(Long gid) {
		groups.put(gid, Collections.synchronizedMap(new HashMap<Long, CacheEntry>()));
	}
	
	@Override
	public CacheEntry get(Long id, Serializer<?> serializer)
			throws TeiidComponentException {
		Map<Long, CacheEntry> group = groups.get(id);
		if (group != null) {
			return group.get(id);
		}
		return null;
	}
		
	@Override
	public void remove(Long gid, Long id) {
		Map<Long, CacheEntry> group = groups.get(gid);
		if (group != null) {
			group.remove(id);
		}
	}
	
	@Override
	public Collection<Long> removeCacheGroup(Long gid) {
		Map<Long, CacheEntry> group = groups.remove(gid);
		if (group == null) {
			return Collections.emptySet();
		}
		synchronized (group) {
			return new ArrayList<Long>(group.keySet());
		}
	}
	
}