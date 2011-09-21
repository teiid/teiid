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
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.teiid.common.buffer.CacheEntry;
import org.teiid.common.buffer.Serializer;

public class TestFileStoreCache {
	
	@Test public void testAddGetMultiBlock() throws Exception {
		FileStoreCache fsc = new FileStoreCache();
		fsc.setStorageManager(new MemoryStorageManager());
		fsc.initialize();
		
		CacheEntry ce = new CacheEntry(2l);
		Serializer<Object> s = new Serializer<Object>() {
			@Override
			public Object deserialize(ObjectInputStream ois)
					throws IOException, ClassNotFoundException {
				return ois.readObject();
			}
			
			@Override
			public Long getId() {
				return 1l;
			}
			
			@Override
			public void serialize(Object obj, ObjectOutputStream oos)
					throws IOException {
				oos.writeObject(obj);
			}
			
			@Override
			public boolean useSoftCache() {
				return false;
			}
		};
		fsc.createCacheGroup(s.getId());
		List<Object> cacheObject = Arrays.asList(new Object[10000]);
		ce.setObject(cacheObject);
		fsc.add(ce, s);
		
		ce = fsc.get(2l, s);
		assertEquals(cacheObject, ce.getObject());
	}
	
}
