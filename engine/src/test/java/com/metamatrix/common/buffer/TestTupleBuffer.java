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

package com.metamatrix.common.buffer;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.query.sql.symbol.ElementSymbol;

public class TestTupleBuffer {

	@Test public void testForwardOnly() throws Exception {
		ElementSymbol x = new ElementSymbol("x"); //$NON-NLS-1$
		x.setType(DataTypeManager.DefaultDataClasses.INTEGER);
		List<ElementSymbol> schema = Arrays.asList(x);
		TupleBuffer tb = new TupleBuffer(new BatchManager() {
			
			@Override
			public void remove() {
				
			}
			
			@Override
			public ManagedBatch createManagedBatch(final TupleBatch batch)
					throws MetaMatrixComponentException {
				return new ManagedBatch() {
					
					@Override
					public void remove() {
						
					}
					
					@Override
					public TupleBatch getBatch(boolean cache, String[] types)
							throws MetaMatrixComponentException {
						return batch;
					}
				};
			}
		}, "x", schema, 32); //$NON-NLS-1$ 
		tb.setForwardOnly(true);
		tb.addTuple(Arrays.asList(1));
		TupleBatch batch = tb.getBatch(1);
		assertTrue(!batch.getTerminationFlag());
		assertEquals(1, batch.getBeginRow());
		try {
			tb.getBatch(1);
			fail("expected exception"); //$NON-NLS-1$
		} catch (AssertionError e) {
			
		}
		tb.addTuple(Arrays.asList(1));
		tb.close();
		batch = tb.getBatch(2);
		assertTrue(batch.getTerminationFlag());
		assertEquals(2, batch.getBeginRow());
	}
	
}
