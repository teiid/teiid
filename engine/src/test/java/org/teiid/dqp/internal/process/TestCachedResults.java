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
package org.teiid.dqp.internal.process;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.teiid.cache.Cache;
import org.teiid.cache.DefaultCache;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.common.buffer.TestTupleBuffer.FakeBatchManager;
import org.teiid.core.types.DataTypeManager;
import org.teiid.dqp.service.FakeBufferService;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.symbol.ElementSymbol;


public class TestCachedResults {

	
	@Test
	public void testCaching() throws Exception {
		FakeBufferService fbs = new FakeBufferService();
		
		ElementSymbol x = new ElementSymbol("x"); //$NON-NLS-1$
		x.setType(DataTypeManager.DefaultDataClasses.INTEGER);
		List<ElementSymbol> schema = Arrays.asList(x);
		TupleBuffer tb = new TupleBuffer(new FakeBatchManager(), "x", schema, null, 4); //$NON-NLS-1$ 
		tb.setForwardOnly(false);
		
		tb.addTuple(Arrays.asList(1));	
		tb.addTuple(Arrays.asList(2));
		tb.addTuple(Arrays.asList(3));
		tb.addTuple(Arrays.asList(4));
		tb.addTuple(Arrays.asList(5));
		tb.addTuple(Arrays.asList(6));
		tb.addTuple(Arrays.asList(7));
		tb.addTuple(Arrays.asList(8));
		tb.addTuple(Arrays.asList(9));
		tb.addTuple(Arrays.asList(10));
		
		tb.close();
		
		BufferManager bm = fbs.getBufferManager();
		CachedResults results = new CachedResults();
		results.setResults(tb);
		results.setCommand(new Query());
		Cache cache = new DefaultCache("dummy"); //$NON-NLS-1$
		
		// simulate the jboss-cache remote transport, where the batches are remotely looked up
		// in cache
		for (int row=1; row<=tb.getRowCount();row+=4) {
			cache.put(results.getId()+","+row, tb.getBatch(row), null); //$NON-NLS-1$ 
		}
		
		results.prepare(cache, bm);
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		oos.writeObject(results);
		oos.close();
		
		ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
		CachedResults cachedResults = (CachedResults)ois.readObject();
		ois.close();
		
		cachedResults.restore(cache, bm);
		
		// since restored, simulate a async cache flush
		cache.clear();
		
		TupleBuffer cachedTb = cachedResults.getResults();
		
		assertEquals(tb.getRowCount(), cachedTb.getRowCount());
		assertEquals(tb.getBatchSize(), cachedTb.getBatchSize());
		
		assertArrayEquals(tb.getBatch(1).getAllTuples(), cachedTb.getBatch(1).getAllTuples());
		assertArrayEquals(tb.getBatch(9).getAllTuples(), cachedTb.getBatch(9).getAllTuples());
	}	
}
