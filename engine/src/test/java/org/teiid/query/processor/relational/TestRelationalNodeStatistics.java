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

package org.teiid.query.processor.relational;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.util.CommandContext;

@SuppressWarnings({"rawtypes", "nls"})
public class TestRelationalNodeStatistics {
        
    @Test public void testBatchTimer() {
        RelationalNodeStatistics testnodeStatistics = new RelationalNodeStatistics();
        testnodeStatistics.startBatchTimer();
        assertTrue("The batch timer did not yield a start time", testnodeStatistics.getBatchStartTime()!= 0); //$NON-NLS-1$
        testnodeStatistics.stopBatchTimer();
        assertTrue("The batch timer did not yield an end time", testnodeStatistics.getBatchEndTime()!= 0); //$NON-NLS-1$
    }

    @Test public void testStatsCollection() throws TeiidComponentException, TeiidProcessingException {
        List[] data = createData(1000);
        FakeRelationalNode fakeNode = createFakeNode(data);
        
        // read from fake node
        while(true) {
            TupleBatch batch = fakeNode.nextBatch();
            if(batch.getTerminationFlag()) {
                break;
            } 
        }
        
         int actualNodeBlocks = fakeNode.getNodeStatistics().getNodeBlocks();
         int actualNodeNextBatchCalls = fakeNode.getNodeStatistics().getNodeNextBatchCalls();
         int actualNodeOutputRows = fakeNode.getNodeStatistics().getNodeOutputRows();
         
         assertEquals("The NodeOutputRows was Inccorrect. Correct: 1000 Actual: "+ actualNodeOutputRows, 1000, actualNodeOutputRows); //$NON-NLS-1$
         assertEquals("The NodeNextBatchCalls was Inccorrect. Correct: 10 Actual: "+ actualNodeNextBatchCalls, 10, actualNodeNextBatchCalls); //$NON-NLS-1$
         assertEquals("The NodeBlocks was Inccorrect. Correct: 0 Actual: "+ actualNodeBlocks, 0, actualNodeBlocks); //$NON-NLS-1$
     }
     
     @Test public void testCumulativeCalculation() throws TeiidComponentException, TeiidProcessingException {
     	RelationalNode[] children = new RelationalNode[2];
     	children[0] = createFakeNode(createData(1));
     	children[1] = createFakeNode(createData(1));
     	children[0].getNodeStatistics().setBatchEndTime(100);
     	children[0].getNodeStatistics().collectCumulativeNodeStats(new TupleBatch(1, Collections.EMPTY_LIST), RelationalNodeStatistics.BATCHCOMPLETE_STOP);
     	children[0].getNodeStatistics().collectNodeStats(new RelationalNode[0]);
     	children[1].getNodeStatistics().setBatchEndTime(200);
     	children[1].getNodeStatistics().collectCumulativeNodeStats(new TupleBatch(1, Collections.EMPTY_LIST), RelationalNodeStatistics.BATCHCOMPLETE_STOP);
     	children[1].getNodeStatistics().collectNodeStats(new RelationalNode[0]);
     	RelationalNodeStatistics stats = new RelationalNodeStatistics();
     	stats.setBatchEndTime(1000);
     	stats.collectNodeStats(children);
     	assertEquals(1000, stats.getNodeCumulativeProcessingTime());
     	assertEquals(700, stats.getNodeNextBatchProcessingTime());
     }

    @Test public void testDescriptionProperties() throws Exception {
        List[] data = createData(1000);
        FakeRelationalNode fakeNode = createFakeNode(data);
        
        // read from fake node
        while(true) {
            TupleBatch batch = fakeNode.nextBatch();
            if(batch.getTerminationFlag()) {
                break;
            } 
        }
        assertEquals("FakeRelationalNode", fakeNode.getDescriptionProperties().getName()); //$NON-NLS-1$
    }
    
    private FakeRelationalNode createFakeNode(List[] data) {
        // setup 
        ElementSymbol element = new ElementSymbol("a"); //$NON-NLS-1$
        element.setType(DataTypeManager.DefaultDataClasses.INTEGER);
        List elements = new ArrayList();
        elements.add(element);
        
        FakeRelationalNode fakeNode = new FakeRelationalNode(1, data, 100);
        fakeNode.setElements(elements);
        CommandContext context = new CommandContext("pid", "group", null, null, null, 1, true); //$NON-NLS-1$ //$NON-NLS-2$
        fakeNode.initialize(context, BufferManagerFactory.getStandaloneBufferManager(), null);
        return fakeNode;
    }
    
    private List[] createData(int rows) {
        List[] data = new List[rows];
        for(int i=0; i<rows; i++) { 
            data[i] = new ArrayList();
            data[i].add(new Integer(i));
        }   
        return data; 
    }   
}
