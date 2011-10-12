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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

public class TestPartiallyOrderedCache {

	@Test public void testQueueMaintenance() {
		PartiallyOrderedCache<Integer, Integer> cache = new PartiallyOrderedCache<Integer, Integer>(16, .75f, 16) {
			
			@Override
			protected void recordAccess(Integer key, Integer value, boolean initial) {
				
			}
		};
		
		cache.setMaxOrderedSize(5);
		
		for (int i = 0; i < 10; i++) {
			cache.put(i, i);
		}
		
		cache.get(8);
		cache.get(1);
		
		List<Integer> evictions = new ArrayList<Integer>();
		for (int i = 0; i < 10; i++) {
			evictions.add(i);
		}
		//we expect natural order because the lru is converted into the sorted on natural key
		assertEquals(Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), evictions);
	}
	
}
