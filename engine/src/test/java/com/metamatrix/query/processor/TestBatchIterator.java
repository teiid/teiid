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

package com.metamatrix.query.processor;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.buffer.BufferManagerFactory;
import com.metamatrix.common.buffer.BufferManager.TupleSourceType;
import com.metamatrix.query.processor.relational.FakeRelationalNode;
import com.metamatrix.query.sql.symbol.ElementSymbol;

@SuppressWarnings("nls")
public class TestBatchIterator {

	@Test public void testReset() throws Exception {
		BatchIterator bi = new BatchIterator(new FakeRelationalNode(1, new List[] {
			Arrays.asList(1),
			Arrays.asList(1),
			Arrays.asList(1)
		}, 1));
		BufferManager bm = BufferManagerFactory.getStandaloneBufferManager();
		bi.setBuffer(bm.createTupleBuffer(Arrays.asList(new ElementSymbol("x")), "test", TupleSourceType.PROCESSOR), true);  //$NON-NLS-1$
		bi.mark();
		bi.nextTuple();
		bi.nextTuple();
		bi.reset();
		bi.nextTuple();
	}
	
}
