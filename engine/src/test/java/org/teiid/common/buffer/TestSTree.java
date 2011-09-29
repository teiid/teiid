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

package org.teiid.common.buffer;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.teiid.common.buffer.STree.InsertMode;
import org.teiid.common.buffer.impl.BufferManagerImpl;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.sql.symbol.ElementSymbol;

@SuppressWarnings({"nls", "unchecked"})
public class TestSTree {
	
	@Test public void testRemoveAll() throws TeiidComponentException {
		BufferManager bm = BufferManagerFactory.getStandaloneBufferManager();
		ElementSymbol e1 = new ElementSymbol("x");
		e1.setType(Integer.class);
		ElementSymbol e2 = new ElementSymbol("y");
		e2.setType(String.class);
		List<ElementSymbol> elements = Arrays.asList(e1, e2);
		STree map = bm.createSTree(elements, "1", 1);
		
		for (int i = 20000; i > 0; i--) {
			assertNull(map.insert(Arrays.asList(i, String.valueOf(i)), InsertMode.NEW, -1));
			assertEquals(20000 - i + 1, map.getRowCount());
		}
		
		for (int i = 20000; i > 0; i--) {
			assertNotNull(String.valueOf(i), map.remove(Arrays.asList(i)));
		}
		
		assertEquals(0, map.getRowCount());
		assertNull(map.insert(Arrays.asList(1, String.valueOf(1)), InsertMode.NEW, -1));
	}
	
	@Test public void testUnOrderedInsert() throws TeiidComponentException {
		BufferManagerImpl bm = BufferManagerFactory.createBufferManager();
		bm.setProcessorBatchSize(16);
		
		ElementSymbol e1 = new ElementSymbol("x");
		e1.setType(Integer.class);
		List elements = Arrays.asList(e1);
		STree map = bm.createSTree(elements, "1", 1);
		
		int size = (1<<16)+(1<<4)+1;
		int logSize = map.getExpectedHeight(size);

		for (int i = 0; i < size; i++) {
			assertNull(map.insert(Arrays.asList(i), InsertMode.NEW, logSize));
			assertEquals(i + 1, map.getRowCount());
		}
		assertTrue(5 >= map.getHeight());
	}

	@Test public void testOrderedInsert() throws TeiidComponentException {
		BufferManagerImpl bm = BufferManagerFactory.createBufferManager();
		bm.setProcessorBatchSize(4);
		
		ElementSymbol e1 = new ElementSymbol("x");
		e1.setType(Integer.class);
		List<ElementSymbol> elements = Arrays.asList(e1);
		STree map = bm.createSTree(elements, "1", 1);
		
		int size = (1<<16)+(1<<4)+1;
		
		for (int i = 0; i < size; i++) {
			assertNull(map.insert(Arrays.asList(i), InsertMode.ORDERED, size));
			assertEquals(i + 1, map.getRowCount());
		}
		
		assertEquals(4, map.getHeight());

		for (int i = 0; i < size; i++) {
			assertNotNull(map.remove(Arrays.asList(i)));
		}
				
	}
	
	/**
	 * Forces the logic through several compaction cycles by using large strings
	 * @throws TeiidComponentException
	 */
	@Test public void testCompaction() throws TeiidComponentException {
		BufferManagerImpl bm = BufferManagerFactory.createBufferManager();
		bm.setProcessorBatchSize(32);
		bm.setMaxReserveKB(0);//force all to disk
		bm.initialize();
		
		ElementSymbol e1 = new ElementSymbol("x");
		e1.setType(String.class);
		List<ElementSymbol> elements = Arrays.asList(e1);
		STree map = bm.createSTree(elements, "1", 1);
		
		int size = 1000;
		
		for (int i = 0; i < size; i++) {
			assertNull(map.insert(Arrays.asList(new String(new byte[1000])), InsertMode.ORDERED, size));
			assertEquals(i + 1, map.getRowCount());
		}
		
		for (int i = 0; i < size; i++) {
			assertNotNull(map.remove(Arrays.asList(new String(new byte[1000]))));
		}
				
	}
	
}
