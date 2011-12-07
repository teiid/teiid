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

import java.util.ArrayList;
import java.util.List;

import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.query.processor.relational.RelationalNodeStatistics;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.util.CommandContext;

import junit.framework.TestCase;


/** 
 * @since 4.2
 */
public class TestRelationalNodeStatistics extends TestCase {

    private int actualNodeOutputRows;
    private int actualNodeNextBatchCalls;
    private int actualNodeCumulativeBlocks;
    private int actualNodeBlocks;
//    private long actualNodeProcessingTime;
//    private long actualNodeCumulativeProcessingTime;
//    private long actualNodeCumulativeNextBatchProcessingTime;
    
    
    public void testBatchTimer() {
        RelationalNodeStatistics testnodeStatistics = new RelationalNodeStatistics();
        testnodeStatistics.startBatchTimer();
        assertTrue("The batch timer did not yield a start time", testnodeStatistics.getBatchStartTime()!= 0); //$NON-NLS-1$
        testnodeStatistics.stopBatchTimer();
        assertTrue("The batch timer did not yield an end time", testnodeStatistics.getBatchEndTime()!= 0); //$NON-NLS-1$
    }

    public void testStatsCollection() throws TeiidComponentException, TeiidProcessingException {
        List[] data = createData(1000);
        FakeRelationalNode fakeNode = this.createFakeNode(data);
        
        // read from fake node
        while(true) {
            TupleBatch batch = fakeNode.nextBatch();
            if(batch.getTerminationFlag()) {
                break;
            } 
        }
        
        this.actualNodeBlocks = fakeNode.getNodeStatistics().getNodeBlocks();
        this.actualNodeNextBatchCalls = fakeNode.getNodeStatistics().getNodeNextBatchCalls();
        this.actualNodeOutputRows = fakeNode.getNodeStatistics().getNodeOutputRows();
//            this.actualNodeCumulativeNextBatchProcessingTime = fakeNode.getNodeStatistics().getNodeCumulativeNextBatchProcessingTime();
//            this.actualNodeCumulativeProcessingTime = fakeNode.getNodeStatistics().getNodeCumulativeProcessingTime();
//            this.actualNodeProcessingTime = fakeNode.getNodeStatistics().getNodeProcessingTime();
        
        //System.out.println("Actual NodeComulativeNextBatchProcessingTime: "+ this.actualNodeCumulativeNextBatchProcessingTime); //$NON-NLS-1$
        //System.out.println("Actual NodeComulativeProcessingTime: "+ this.actualNodeCumulativeProcessingTime); //$NON-NLS-1$
        //System.out.println("Actual NodeProcessingTime: "+ this.actualNodeProcessingTime); //$NON-NLS-1$
        
        assertEquals("The NodeOutputRows was Inccorrect. Correct: 1000 Actual: "+ this.actualNodeOutputRows, 1000, this.actualNodeOutputRows); //$NON-NLS-1$
        assertEquals("The NodeNextBatchCalls was Inccorrect. Correct: 10 Actual: "+ this.actualNodeNextBatchCalls, 10, this.actualNodeNextBatchCalls); //$NON-NLS-1$
        assertEquals("The NodeBlocks was Inccorrect. Correct: 0 Actual: "+ this.actualNodeBlocks, 0, this.actualNodeBlocks); //$NON-NLS-1$
        assertEquals("The NodeComulativeBlocks was Inccorrect. Correct: 0 Actual: "+ this.actualNodeCumulativeBlocks, 0, this.actualNodeCumulativeBlocks); //$NON-NLS-1$
    }

    public void testDescriptionProperties() throws Exception {
        List[] data = createData(1000);
        FakeRelationalNode fakeNode = this.createFakeNode(data);
        
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
