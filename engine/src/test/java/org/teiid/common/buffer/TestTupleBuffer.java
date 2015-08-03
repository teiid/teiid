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

import javax.sql.rowset.serial.SerialClob;

import org.junit.Test;
import org.teiid.common.buffer.BufferManager.TupleSourceType;
import org.teiid.common.buffer.TupleBuffer.TupleBufferTupleSource;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.DataTypeManager;
import org.teiid.query.sql.symbol.ElementSymbol;

public class TestTupleBuffer {

	@Test public void testForwardOnly() throws Exception {
		ElementSymbol x = new ElementSymbol("x"); //$NON-NLS-1$
		x.setType(DataTypeManager.DefaultDataClasses.INTEGER);
		List<ElementSymbol> schema = Arrays.asList(x);
		TupleBuffer tb = BufferManagerFactory.getStandaloneBufferManager().createTupleBuffer(schema, "x", TupleSourceType.PROCESSOR); //$NON-NLS-1$ 
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
	
	@Test public void testReverseIteration() throws Exception {
		ElementSymbol x = new ElementSymbol("x"); //$NON-NLS-1$
		x.setType(DataTypeManager.DefaultDataClasses.INTEGER);
		List<ElementSymbol> schema = Arrays.asList(x);
		TupleBuffer tb = BufferManagerFactory.getStandaloneBufferManager().createTupleBuffer(schema, "x", TupleSourceType.PROCESSOR); //$NON-NLS-1$ 
		tb.addTuple(Arrays.asList(1));
		tb.addTuple(Arrays.asList(2));
		TupleBufferTupleSource tbts = tb.createIndexedTupleSource();
		tbts.setReverse(true);
		assertTrue(tbts.hasNext());
		assertEquals(2, tbts.nextTuple().get(0));
		assertEquals(1, tbts.nextTuple().get(0));
		assertFalse(tbts.hasNext());
	}
	
	@Test public void testTruncate() throws Exception {
		ElementSymbol x = new ElementSymbol("x"); //$NON-NLS-1$
		x.setType(DataTypeManager.DefaultDataClasses.INTEGER);
		List<ElementSymbol> schema = Arrays.asList(x);
		TupleBuffer tb = BufferManagerFactory.getStandaloneBufferManager().createTupleBuffer(schema, "x", TupleSourceType.PROCESSOR); //$NON-NLS-1$
		tb.setBatchSize(2);
		for (int i = 0; i < 5; i++) {
			tb.addTuple(Arrays.asList(1));
		}
		TupleBatch batch = tb.getBatch(1);
		assertTrue(!batch.getTerminationFlag());
		assertEquals(2, batch.getEndRow());
		tb.close();
		assertEquals(5, tb.getManagedRowCount());
		tb.truncateTo(3);
		assertEquals(3, tb.getManagedRowCount());
		assertEquals(3, tb.getRowCount());
		batch = tb.getBatch(3);
		assertTrue(batch.getTerminationFlag());
		tb.truncateTo(2);
		assertEquals(2, tb.getManagedRowCount());
		assertEquals(2, tb.getRowCount());
		batch = tb.getBatch(2);
		assertTrue(batch.getTerminationFlag());
	}
	
	@Test public void testTruncatePartial() throws Exception {
		ElementSymbol x = new ElementSymbol("x"); //$NON-NLS-1$
		x.setType(DataTypeManager.DefaultDataClasses.INTEGER);
		List<ElementSymbol> schema = Arrays.asList(x);
		TupleBuffer tb = BufferManagerFactory.getStandaloneBufferManager().createTupleBuffer(schema, "x", TupleSourceType.PROCESSOR); //$NON-NLS-1$
		tb.setBatchSize(64);
		for (int i = 0; i < 65; i++) {
			tb.addTuple(Arrays.asList(1));
		}
		TupleBatch batch = tb.getBatch(1);
		assertTrue(!batch.getTerminationFlag());
		assertEquals(65, tb.getManagedRowCount());
		tb.truncateTo(3);
		assertEquals(3, tb.getManagedRowCount());
		assertEquals(3, tb.getRowCount());
		batch = tb.getBatch(3);
	}
	
	@Test public void testTruncatePartial1() throws Exception {
		ElementSymbol x = new ElementSymbol("x"); //$NON-NLS-1$
		x.setType(DataTypeManager.DefaultDataClasses.INTEGER);
		List<ElementSymbol> schema = Arrays.asList(x);
		TupleBuffer tb = BufferManagerFactory.getStandaloneBufferManager().createTupleBuffer(schema, "x", TupleSourceType.PROCESSOR); //$NON-NLS-1$
		tb.setBatchSize(128);
		for (int i = 0; i < 131; i++) {
			tb.addTuple(Arrays.asList(1));
		}
		tb.truncateTo(129);
		assertEquals(129, tb.getManagedRowCount());
		assertEquals(129, tb.getRowCount());
	}
	
	@Test public void testTruncateMultiple() throws Exception {
		ElementSymbol x = new ElementSymbol("x"); //$NON-NLS-1$
		x.setType(DataTypeManager.DefaultDataClasses.INTEGER);
		List<ElementSymbol> schema = Arrays.asList(x);
		TupleBuffer tb = BufferManagerFactory.getStandaloneBufferManager().createTupleBuffer(schema, "x", TupleSourceType.PROCESSOR); //$NON-NLS-1$
		tb.setBatchSize(16);
		for (int i = 0; i < 131; i++) {
			tb.addTuple(Arrays.asList(1));
		}
		tb.truncateTo(17);
		assertEquals(17, tb.getManagedRowCount());
		assertEquals(17, tb.getRowCount());
	}
	
	@Test public void testLobHandling() throws Exception {
		ElementSymbol x = new ElementSymbol("x"); //$NON-NLS-1$
		x.setType(DataTypeManager.DefaultDataClasses.CLOB);
		List<ElementSymbol> schema = Arrays.asList(x);
		TupleBuffer tb = BufferManagerFactory.getStandaloneBufferManager().createTupleBuffer(schema, "x", TupleSourceType.PROCESSOR); //$NON-NLS-1$
		tb.setInlineLobs(false);
		ClobType c = new ClobType(new SerialClob(new char[0]));
		TupleBatch batch = new TupleBatch(1, new List[] {Arrays.asList(c)});
		tb.addTupleBatch(batch, false);
		assertNotNull(tb.getLobReference(c.getReferenceStreamId()));
	}
	
}
