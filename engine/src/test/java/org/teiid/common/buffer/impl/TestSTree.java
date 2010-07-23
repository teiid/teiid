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
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.common.buffer.STree;
import org.teiid.common.buffer.BufferManager.TupleSourceType;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.sql.symbol.ElementSymbol;

public class TestSTree {
	
	@Test public void testRemoveAll() throws TeiidComponentException, IOException {
		BufferManager bm = BufferManagerFactory.getStandaloneBufferManager();
		ElementSymbol e1 = new ElementSymbol("x");
		e1.setType(Integer.class);
		ElementSymbol e2 = new ElementSymbol("y");
		e2.setType(String.class);
		List elements = Arrays.asList(e1, e2);
		STree map = bm.createSTree(elements, "1", TupleSourceType.PROCESSOR, 1);
		
		for (int i = 20000; i > 0; i--) {
			map.insert(Arrays.asList(i, String.valueOf(i)), true);
		}
		
		for (int i = 20000; i > 0; i--) {
			map.remove(Arrays.asList(i));
		}
		
		assertEquals(0, map.getRowCount());
		assertNull(map.insert(Arrays.asList(1, String.valueOf(1)), true));
	}

}
