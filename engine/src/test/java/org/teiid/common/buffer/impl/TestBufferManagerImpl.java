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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.BufferManager.BufferReserveMode;
import org.teiid.common.buffer.BufferManager.TupleSourceType;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.common.buffer.FileStore;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.common.buffer.impl.BufferManagerImpl.BatchManagerImpl;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.util.CommandContext;
import org.teiid.query.util.Options;

@SuppressWarnings("nls")
public class TestBufferManagerImpl {
	
    @Test public void testReserve() throws Exception {
        BufferManagerImpl bufferManager = new BufferManagerImpl();
        bufferManager.setCache(new MemoryStorageManager());
        bufferManager.setMaxProcessingKB(1024);
        bufferManager.setMaxReserveKB(1024);
        bufferManager.initialize();
        bufferManager.setNominalProcessingMemoryMax(512000);
        
        //restricted by nominal max
        assertEquals(512000, bufferManager.reserveBuffers(1024000, BufferReserveMode.NO_WAIT));
		//forced
        assertEquals(1024000, bufferManager.reserveBuffersBlocking(1024000, new long[] {0,0}, true));
        
        //not forced, so we get noting
        assertEquals(0, bufferManager.reserveBuffersBlocking(1024000, new long[] {0,0}, false));
        
        bufferManager.releaseBuffers(512000);
        //the difference between 1mb and 1000k
        assertEquals(24576, bufferManager.reserveBuffers(1024000, BufferReserveMode.NO_WAIT));
    }
    
    @Test public void testLargeReserve() throws Exception {
        BufferManagerImpl bufferManager = new BufferManagerImpl();
        bufferManager.setCache(new MemoryStorageManager());
        bufferManager.setMaxReserveKB((1<<22) + 11);
        assertEquals(4194315, bufferManager.getMaxReserveKB());
    }
    
    @Test(expected=IOException.class) public void testFileStoreMax() throws Exception {
        BufferManagerImpl bufferManager = new BufferManagerImpl();
        bufferManager.setCache(new MemoryStorageManager() {
            @Override
            public long getMaxStorageSpace() {
                return 640;
            }
        });
        bufferManager.setMaxActivePlans(20);
        bufferManager.initialize();
        FileStore fs = bufferManager.createFileStore("x");
        fs.write(new byte[10], 0, 10);
    }
    
    @Test(expected=TeiidComponentException.class) public void testTupleBufferMax() throws Exception {
        BufferManagerImpl bufferManager = new BufferManagerImpl();
        bufferManager.setCache(new MemoryStorageManager() {
            @Override
            public long getMaxStorageSpace() {
                return 640;
            }
        });
        bufferManager.setMaxReserveKB(10);
        bufferManager.setMaxActivePlans(20);
        bufferManager.setEnforceMaxBatchManagerSizeEstimate(true);
        bufferManager.initialize();
        TupleBuffer tb = bufferManager.createTupleBuffer(Arrays.asList(new ElementSymbol("x", null, String.class)), "x", TupleSourceType.PROCESSOR);
        //fill one batch, which should then exceed the max
        for (int i = 0; i < 1024; i++) {
            tb.addTuple(Arrays.asList("a"));
        }
    }
    
    @Test public void testTupleBufferSessionMax() throws Exception {
        BufferManagerImpl bufferManager = new BufferManagerImpl();
        bufferManager.setCache(new MemoryStorageManager() {
            @Override
            public long getMaxStorageSpace() {
                return 64000;
            }
        });
        bufferManager.setMaxReserveKB(10);
        bufferManager.setMaxActivePlans(10);
        bufferManager.setOptions(new Options().maxSessionBufferSizeEstimate(100000));
        bufferManager.initialize();
        CommandContext context = new CommandContext();
        context.setSession(new SessionMetadata());
        CommandContext.pushThreadLocalContext(context);
        try {
            List<TupleBuffer> tupleBuffers = new ArrayList<TupleBuffer>();
            for (int i = 0; i < 36; i++) {
                TupleBuffer tb = bufferManager.createTupleBuffer(Arrays.asList(new ElementSymbol("x", null, String.class)), "x", TupleSourceType.PROCESSOR);
                try {
                    for (int j = 0; j < 50; j++) {
                        tb.addTuple(Arrays.asList("a"));
                    }
                    tb.saveBatch();
                    if (i%2==0) {
                        tb.remove();
                    }
                } catch (TeiidComponentException e) {
                    assertEquals(34, i);
                    return;
                }
                tupleBuffers.add(tb);
            }
        } finally {
            CommandContext.popThreadLocalContext();
        }
        fail();
    }
    
    @Test
    public void testProcessorBatchSize(){
    	BufferManager bm = BufferManagerFactory.createBufferManager();
    	
    	int processorBatchSize = bm.getProcessorBatchSize();
		
		List<ElementSymbol> elements = new ArrayList<ElementSymbol>();
		ElementSymbol a = new ElementSymbol("a");
		a.setType(DataTypeManager.DefaultDataClasses.INTEGER);
		
		//we use a somewhat high estimate of string size
		ElementSymbol b = new ElementSymbol("b");
		b.setType(DataTypeManager.DefaultDataClasses.STRING);
		
		elements.add(a);
		//fixed/small
		assertEquals(processorBatchSize * 8, bm.getProcessorBatchSize(elements));
		
		elements.add(b);
		//small
		assertEquals(processorBatchSize * 4, bm.getProcessorBatchSize(elements));
		
		elements.add(b);
		//moderately small
		assertEquals(processorBatchSize * 2, bm.getProcessorBatchSize(elements));
		
		elements.add(b);
		elements.add(b);
		//"normal"
		assertEquals(processorBatchSize, bm.getProcessorBatchSize(elements));
		
		elements.addAll(Collections.nCopies(28, b));
		//large
		assertEquals(processorBatchSize/2, bm.getProcessorBatchSize(elements));
		
		elements.addAll(Collections.nCopies(100, b));
		//huge
		assertEquals(processorBatchSize/4, bm.getProcessorBatchSize(elements));
		
		elements.addAll(Collections.nCopies(375, b));
		//extreme
		assertEquals(processorBatchSize/8, bm.getProcessorBatchSize(elements));
    }
    
    @Test public void testRemovedException() throws TeiidComponentException {
        BufferManagerImpl bufferManager = new BufferManagerImpl();
        bufferManager.setCache(new MemoryStorageManager());
        bufferManager.initialize();
        BatchManagerImpl batchManager = bufferManager.createBatchManager(1l, new Class<?>[] {Integer.class});
        batchManager.remove();
        try {
            batchManager.getBatch(1l, false);
            fail();
        } catch (TeiidComponentException e) {
            
        }
    }

}
