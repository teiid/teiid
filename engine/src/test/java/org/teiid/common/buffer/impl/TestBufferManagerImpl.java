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
import java.util.List;

import org.junit.Test;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.BufferManager.BufferReserveMode;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.core.types.DataTypeManager;
import org.teiid.query.sql.symbol.ElementSymbol;

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
    
    //TEIID-3583 -document for buffer service properties processor-batch-size
    @Test
    public void testProcessorBatchSize(){
    	
    	BufferManager bm = BufferManagerFactory.createBufferManager();
    	
    	int processorBatchSize = bm.getProcessorBatchSize();
		
		List<ElementSymbol> elements = new ArrayList<ElementSymbol>();
		ElementSymbol a = new ElementSymbol("a");
		a.setType(DataTypeManager.DefaultDataClasses.INTEGER);
		ElementSymbol b = new ElementSymbol("b");
		b.setType(DataTypeManager.DefaultDataClasses.STRING);
		ElementSymbol c = new ElementSymbol("c");
		c.setType(DataTypeManager.DefaultDataClasses.STRING);
		ElementSymbol d = new ElementSymbol("d");
		d.setType(DataTypeManager.DefaultDataClasses.STRING);
		ElementSymbol e = new ElementSymbol("e");
		e.setType(DataTypeManager.DefaultDataClasses.STRING);
		
		elements.add(a);
		assertEquals(2048, bm.getProcessorBatchSize(elements));
		
		elements.add(b);
		assertEquals(1024, bm.getProcessorBatchSize(elements));
		
		elements.add(c);
		assertEquals(processorBatchSize * 2, bm.getProcessorBatchSize(elements));
		
		elements.add(d);
		elements.add(e);
		assertEquals(processorBatchSize, bm.getProcessorBatchSize(elements));
    }

}
