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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.junit.Test;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.util.CommandContext;

/**
 */
public class TestQueryProcessor {

    public void helpTestProcessor(FakeProcessorPlan plan, List[] expectedResults) throws TeiidException {
        BufferManager bufferMgr = BufferManagerFactory.getStandaloneBufferManager();
        FakeDataManager dataManager = new FakeDataManager();

        CommandContext context = new CommandContext("pid", "group", null, null, 1); //$NON-NLS-1$ //$NON-NLS-2$
        QueryProcessor processor = new QueryProcessor(plan, context, bufferMgr, dataManager);
        BatchCollector collector = processor.createBatchCollector();
        TupleBuffer tsID = null;
        while(true) {
            try {
                tsID = collector.collectTuples();         
                break;
            } catch(BlockedException e) {
            }
        }
        
        // Compare # of rows in actual and expected
        assertEquals("Did not get expected # of rows", expectedResults.length, tsID.getRowCount()); //$NON-NLS-1$
        
        // Compare actual with expected results
        TupleSource actual = tsID.createIndexedTupleSource();
        if(expectedResults.length > 0) {
            for(int i=0; i<expectedResults.length; i++) {
                List actRecord = actual.nextTuple();
                List expRecord = expectedResults[i];                    
                assertEquals("Did not match row at row index " + i, expRecord, actRecord); //$NON-NLS-1$
            }
        }
        tsID.remove();
    }
    
    @Test public void testNoResults() throws Exception {
        List elements = new ArrayList();
        elements.add(new ElementSymbol("a")); //$NON-NLS-1$
        FakeProcessorPlan plan = new FakeProcessorPlan(elements, null);
        helpTestProcessor(plan, new List[0]);    
    }

    @Test public void testBlockNoResults() throws Exception {
        List elements = new ArrayList();
        elements.add(new ElementSymbol("a")); //$NON-NLS-1$
        
        List batches = new ArrayList();
        batches.add(BlockedException.INSTANCE);
        TupleBatch batch = new TupleBatch(1, new List[0]);
        batch.setTerminationFlag(true);
        batches.add(batch);
        
        FakeProcessorPlan plan = new FakeProcessorPlan(elements, batches);
        helpTestProcessor(plan, new List[0]);    
    }
    
    @Test public void testProcessWithOccasionalBlocks() throws Exception {
        List elements = new ArrayList();
        elements.add(new ElementSymbol("a")); //$NON-NLS-1$
                
        HashSet blocked = new HashSet(Arrays.asList(new Integer[] { new Integer(0), new Integer(2), new Integer(7) }));
        int numBatches = 10;
        int batchRow = 1;        
        int rowsPerBatch = 50;
        List[] expectedResults = new List[rowsPerBatch*(numBatches-blocked.size())];
        List batches = new ArrayList();
        for(int b=0; b<numBatches; b++) {
            if(blocked.contains(new Integer(b))) {
                batches.add(BlockedException.INSTANCE);
            } else {    
                List[] rows = new List[rowsPerBatch];
                for(int i=0; i<rowsPerBatch; i++) {
                    rows[i] = new ArrayList();
                    rows[i].add(new Integer(batchRow));
                    expectedResults[batchRow-1] = rows[i];
                    batchRow++;
                }
                                                
                TupleBatch batch = new TupleBatch(batchRow-rows.length, rows);
                if(b == numBatches-1) {
                    batch.setTerminationFlag(true);
                } 
                batches.add(batch);
            }
        }
        
        FakeProcessorPlan plan = new FakeProcessorPlan(elements, batches);
        helpTestProcessor(plan, expectedResults);                    
    }
    
    @Test public void testCloseBeforeInitialization() throws TeiidComponentException {
        BufferManager bufferMgr = BufferManagerFactory.getStandaloneBufferManager();
        FakeDataManager dataManager = new FakeDataManager();

        CommandContext context = new CommandContext("pid", "group", null, null, 1); //$NON-NLS-1$ //$NON-NLS-2$

    	QueryProcessor processor = new QueryProcessor(null, context, bufferMgr, dataManager);
    	processor.closeProcessing();
    }
    
    
}
