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

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.ref.WeakReference;

import org.junit.Test;
import org.teiid.common.buffer.CacheEntry;
import org.teiid.common.buffer.Serializer;
import org.teiid.core.TeiidComponentException;

public class TestBufferFrontedFileStoreCache {
	
	private final class SimpleSerializer implements Serializer<Integer> {
		@Override
		public Integer deserialize(ObjectInputStream ois)
				throws IOException, ClassNotFoundException {
			Integer result = ois.readInt();
			for (int i = 0; i < result; i++) {
				assertEquals(i, ois.readInt());
			}
			return result;
		}

		@Override
		public Long getId() {
			return 1l;
		}

		@Override
		public void serialize(Integer obj, ObjectOutputStream oos)
				throws IOException {
			oos.writeInt(obj);
			for (int i = 0; i < obj; i++) {
				oos.writeInt(i);
			}
		}

		@Override
		public boolean useSoftCache() {
			return false;
		}
	}

	@Test public void testAddGetMultiBlock() throws Exception {
		BufferFrontedFileStoreCache cache = createLayeredCache(1 << 26, 1 << 26);
		
		CacheEntry ce = new CacheEntry(2l);
		Serializer<Integer> s = new SimpleSerializer();
		cache.createCacheGroup(s.getId());
		Integer cacheObject = Integer.valueOf(2);
		ce.setObject(cacheObject);
		cache.addToCacheGroup(s.getId(), ce.getId());
		cache.add(ce, s);
		ce = get(cache, 2l, s);
		assertEquals(cacheObject, ce.getObject());
		
		//test something that exceeds the direct inode data blocks
		ce = new CacheEntry(3l);
		cacheObject = Integer.valueOf(80000);
		ce.setObject(cacheObject);
		cache.addToCacheGroup(s.getId(), ce.getId());
		cache.add(ce, s);
		
		ce = get(cache, 3l, s);
		assertEquals(cacheObject, ce.getObject());
		
		//repeat the test to ensure proper cleanup
		ce = new CacheEntry(4l);
		cacheObject = Integer.valueOf(60000);
		ce.setObject(cacheObject);
		cache.addToCacheGroup(s.getId(), ce.getId());
		cache.add(ce, s);
		
		ce = get(cache, 4l, s);
		assertEquals(cacheObject, ce.getObject());
		
		cache.removeCacheGroup(1l);
		
		assertEquals(0, cache.getDataBlocksInUse());
		assertEquals(0, cache.getInodesInUse());
		
		//test something that exceeds the indirect data blocks
		ce = new CacheEntry(3l);
		cache.createCacheGroup(s.getId());
		cacheObject = Integer.valueOf(5000000);
		ce.setObject(cacheObject);
		cache.addToCacheGroup(s.getId(), ce.getId());
		cache.add(ce, s);
		
		ce = get(cache, 3l, s);
		assertEquals(cacheObject, ce.getObject());

		cache.removeCacheGroup(1l);
		
		assertEquals(0, cache.getDataBlocksInUse());
		assertEquals(0, cache.getInodesInUse());

		//test something that exceeds the allowable object size
		ce = new CacheEntry(3l);
		cache.createCacheGroup(s.getId());
		cacheObject = Integer.valueOf(500000000);
		ce.setObject(cacheObject);
		cache.addToCacheGroup(s.getId(), ce.getId());
		cache.add(ce, s);
		
		ce = get(cache, 3l, s);
		assertNull(ce);

		cache.removeCacheGroup(1l);
		
		assertEquals(0, cache.getDataBlocksInUse());
		assertEquals(0, cache.getInodesInUse());
	}

	private CacheEntry get(BufferFrontedFileStoreCache cache, Long oid,
			Serializer<Integer> s) throws TeiidComponentException {
		PhysicalInfo o = cache.lockForLoad(oid, s);
		CacheEntry ce = cache.get(o, oid, new WeakReference<Serializer<?>>(s));
		cache.unlockForLoad(o);
		return ce;
	}
	
	@Test public void testEviction() throws Exception {
		BufferFrontedFileStoreCache cache = createLayeredCache(1<<15, 1<<15);
		
		CacheEntry ce = new CacheEntry(2l);
		Serializer<Integer> s = new SimpleSerializer();
		WeakReference<? extends Serializer<?>> ref = new WeakReference<Serializer<?>>(s);
		ce.setSerializer(ref);
		cache.createCacheGroup(s.getId());
		Integer cacheObject = Integer.valueOf(5000);
		ce.setObject(cacheObject);
		cache.addToCacheGroup(s.getId(), ce.getId());
		cache.add(ce, s);
		
		ce = new CacheEntry(3l);
		ce.setSerializer(ref);
		cacheObject = Integer.valueOf(5001);
		ce.setObject(cacheObject);
		cache.addToCacheGroup(s.getId(), ce.getId());
		cache.add(ce, s);

		assertEquals(3, cache.getDataBlocksInUse());
		assertEquals(1, cache.getInodesInUse());

		ce = get(cache, 2l, s);
		assertEquals(Integer.valueOf(5000), ce.getObject());
		
		ce = get(cache, 3l, s);
		assertEquals(Integer.valueOf(5001), ce.getObject());
	}

	private BufferFrontedFileStoreCache createLayeredCache(int bufferSpace, int objectSize) throws TeiidComponentException {
		BufferFrontedFileStoreCache fsc = new BufferFrontedFileStoreCache();
		fsc.setMemoryBufferSpace(bufferSpace);
		fsc.setMaxStorageObjectSize(objectSize);
		fsc.setDirect(false);
		SplittableStorageManager ssm = new SplittableStorageManager(new MemoryStorageManager());
		ssm.setMaxFileSizeDirect(MemoryStorageManager.MAX_FILE_SIZE);
		fsc.setStorageManager(ssm);
		fsc.initialize();
		return fsc;
	}
	
}
