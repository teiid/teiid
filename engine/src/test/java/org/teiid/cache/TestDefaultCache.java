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

package org.teiid.cache;

import static org.junit.Assert.*;

import org.junit.Test;

@SuppressWarnings("nls")
public class TestDefaultCache {
	
	@Test public void testExpiration() throws InterruptedException {
		DefaultCache<Integer, Integer> cache = new DefaultCache<Integer, Integer>("foo", 2, 70);
		cache.put(1, 1);
		Thread.sleep(100);
		assertNull(cache.get(1));
		cache.put(2, 2);
		Thread.sleep(50);
		cache.put(3, 3);
		assertNotNull(cache.get(2));
		Thread.sleep(50);
		cache.put(4, 4);
		//preferred to purge 2 instead of 3
		assertNotNull(cache.get(3));
	}

}
