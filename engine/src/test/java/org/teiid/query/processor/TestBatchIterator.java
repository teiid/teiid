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

package org.teiid.query.processor;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.BufferManager.TupleSourceType;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.query.processor.relational.FakeRelationalNode;
import org.teiid.query.sql.symbol.ElementSymbol;


@SuppressWarnings("nls")
public class TestBatchIterator {

	@Test public void testReset() throws Exception {
		BatchIterator bi = new BatchIterator(new FakeRelationalNode(1, new List[] {
			Arrays.asList(1),
			Arrays.asList(1),
			Arrays.asList(1)
		}, 1));
		BufferManager bm = BufferManagerFactory.getStandaloneBufferManager();
		bi.setBuffer(bm.createTupleBuffer(Arrays.asList(new ElementSymbol("x", null, DataTypeManager.DefaultDataClasses.INTEGER)), "test", TupleSourceType.PROCESSOR), true);  //$NON-NLS-1$
		bi.mark();
		bi.nextTuple();
		bi.nextTuple();
		bi.reset();
		bi.nextTuple();
	}
	
	@Test public void testReset1() throws Exception {
		BatchIterator bi = new BatchIterator(new FakeRelationalNode(1, new List[] {
			Arrays.asList(1),
			Arrays.asList(2),
			Arrays.asList(3)
		}, 2));
		BufferManager bm = BufferManagerFactory.getStandaloneBufferManager();
		TupleBuffer tb = bm.createTupleBuffer(Arrays.asList(new ElementSymbol("x", null, DataTypeManager.DefaultDataClasses.INTEGER)), "test", TupleSourceType.PROCESSOR);
		bi.setBuffer(tb, true);  //$NON-NLS-1$
		bi.nextTuple();
		bi.mark();
		bi.nextTuple();
		bi.reset();
		assertEquals(2, bi.getCurrentIndex());
		assertEquals(2, bi.nextTuple().get(0));
	}
	
	@Test public void testReset2() throws Exception {
		BatchIterator bi = new BatchIterator(new FakeRelationalNode(1, new List[] {
			Arrays.asList(1),
			Arrays.asList(2),
		}, 2));
		BufferManager bm = BufferManagerFactory.getStandaloneBufferManager();
		TupleBuffer tb = bm.createTupleBuffer(Arrays.asList(new ElementSymbol("x", null, DataTypeManager.DefaultDataClasses.INTEGER)), "test", TupleSourceType.PROCESSOR);
		bi.setBuffer(tb, true);  //$NON-NLS-1$
		bi.hasNext();
		bi.mark();
		bi.nextTuple();
		bi.nextTuple();
		assertNull(bi.nextTuple());
		bi.reset();
		bi.hasNext();
		assertEquals(1, bi.getCurrentIndex());
		assertEquals(1, bi.nextTuple().get(0));
	}
	
	@Test public void testBatchReadDuringMark() throws Exception {
		BatchIterator bi = new BatchIterator(new FakeRelationalNode(1, new List[] {
			Arrays.asList(1),
			Arrays.asList(1),
		}, 1));
		BufferManager bm = BufferManagerFactory.getStandaloneBufferManager();
		bi.setBuffer(bm.createTupleBuffer(Arrays.asList(new ElementSymbol("x", null, DataTypeManager.DefaultDataClasses.INTEGER)), "test", TupleSourceType.PROCESSOR), true);  //$NON-NLS-1$
		bi.mark();
		assertNotNull(bi.nextTuple());
		assertNotNull(bi.nextTuple());
		assertNull(bi.nextTuple());
		bi.reset();
		assertNotNull(bi.nextTuple());
		assertNotNull(bi.nextTuple());
		assertNull(bi.nextTuple());
	}
	
	@Test public void testDisableSave() throws Exception {
		BatchIterator bi = new BatchIterator(new FakeRelationalNode(1, new List[] {
				Arrays.asList(1),
				Arrays.asList(1),
				Arrays.asList(1),
				Arrays.asList(1),
				Arrays.asList(1),
				Arrays.asList(1),
		}, 2));
		BufferManager bm = BufferManagerFactory.getStandaloneBufferManager();
		TupleBuffer tb = bm.createTupleBuffer(Arrays.asList(new ElementSymbol("x", null, DataTypeManager.DefaultDataClasses.INTEGER)), "test", TupleSourceType.PROCESSOR);
		bi.setBuffer(tb, false);  //$NON-NLS-1$
		bi.setPosition(2);
		assertTrue(bi.hasNext());
		tb.setForwardOnly(true);
		bi.setPosition(1);
		bi.disableSave();
		for (int i = 0; i < 6; i++) {
			assertNotNull(bi.nextTuple());
		}
		assertNull(bi.nextTuple());
		assertEquals(0, tb.getManagedRowCount());
	}
	
	@Test public void testReadAhead() throws Exception {
		BatchIterator bi = new BatchIterator(new FakeRelationalNode(1, new List[] {
				Arrays.asList(1),
				Arrays.asList(1),
				Arrays.asList(1),
				Arrays.asList(1),
				Arrays.asList(1),
				Arrays.asList(1),
		}, 2));
		BufferManager bm = BufferManagerFactory.getStandaloneBufferManager();
		TupleBuffer tb = bm.createTupleBuffer(Arrays.asList(new ElementSymbol("x", null, DataTypeManager.DefaultDataClasses.INTEGER)), "test", TupleSourceType.PROCESSOR);
		bi.setBuffer(tb, false);  //$NON-NLS-1$
		bi.nextTuple();
		assertEquals(1, bi.available());
		assertEquals(2, bi.getBuffer().getRowCount());
		bi.readAhead(100);
		assertEquals(4, bi.getBuffer().getRowCount());
		//shouldn't keep reading
		bi.readAhead(3);
		assertEquals(4, bi.getBuffer().getRowCount());
		bi.readAhead(5);
		assertEquals(6, bi.getBuffer().getRowCount());
		bi.readAhead(8); //does nothing
		for (int i = 0; i < 5; i++) {
			assertNotNull(bi.nextTuple());
		}
		assertNull(bi.nextTuple());
	}
	
	@Test public void testReadAheadMark() throws Exception {
		BatchIterator bi = new BatchIterator(new FakeRelationalNode(1, new List[] {
				Arrays.asList(1),
				Arrays.asList(1),
				Arrays.asList(1),
				Arrays.asList(1),
				Arrays.asList(1),
				Arrays.asList(1),
				Arrays.asList(1),
		}, 2));
		BufferManager bm = BufferManagerFactory.getStandaloneBufferManager();
		TupleBuffer tb = bm.createTupleBuffer(Arrays.asList(new ElementSymbol("x", null, DataTypeManager.DefaultDataClasses.INTEGER)), "test", TupleSourceType.PROCESSOR);
		bi.setBuffer(tb, true);  //$NON-NLS-1$
		bi.nextTuple();
		assertEquals(1, bi.available());
		assertEquals(0, bi.getBuffer().getRowCount());
		bi.readAhead(100);
		assertEquals(4, bi.getBuffer().getRowCount());
		//shouldn't keep reading
		bi.readAhead(2);
		assertEquals(4, bi.getBuffer().getRowCount());
		
		bi.readAhead(5);
		assertEquals(6, bi.getBuffer().getRowCount());
		bi.readAhead(8); //does nothing
		for (int i = 0; i < 6; i++) {
			assertNotNull(bi.nextTuple());
		}
		assertNull(bi.nextTuple());
	}
	
	@Test public void testNoSaveForwardOnly() throws Exception {
		BatchIterator bi = new BatchIterator(new FakeRelationalNode(1, new List[] {
				Arrays.asList(1),
				Arrays.asList(1),
				Arrays.asList(1),
				Arrays.asList(1),
		}, 2) {
			@Override
			public TupleBatch nextBatchDirect() throws BlockedException,
					TeiidComponentException, TeiidProcessingException {
				TupleBatch tb = super.nextBatchDirect();
				tb.setRowOffset(tb.getBeginRow() + 3);
				return tb;
			}
			
		});
		BufferManager bm = BufferManagerFactory.getStandaloneBufferManager();
		TupleBuffer tb = bm.createTupleBuffer(Arrays.asList(new ElementSymbol("x", null, DataTypeManager.DefaultDataClasses.INTEGER)), "test", TupleSourceType.PROCESSOR);
		tb.setForwardOnly(true);
		bi.setBuffer(tb, false);  //$NON-NLS-1$
		
		tb.addTuple(Arrays.asList(2));
		tb.addTuple(Arrays.asList(2));
		tb.addTuple(Arrays.asList(2));
		assertEquals(3, bi.getBuffer().getManagedRowCount());
		bi.nextTuple();
		//pull the first batch
		assertEquals(2, bi.available()); 
		assertEquals(0, bi.getBuffer().getManagedRowCount());
		for (int i = 0; i < 2; i++) {
			assertNotNull(bi.nextTuple());
			assertEquals(0, bi.getBuffer().getManagedRowCount());
		}
		bi.readAhead(3);
		assertEquals(2, bi.getBuffer().getManagedRowCount());
		for (int i = 0; i < 4; i++) {
			assertNotNull(bi.nextTuple());
			assertEquals(0, bi.getBuffer().getManagedRowCount());
		}
		assertNull(bi.nextTuple());
		assertEquals(0, bi.getBuffer().getManagedRowCount());
	}
	
}
