/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.query.processor.relational;

import static org.junit.Assert.*;

import java.util.ArrayList;
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
        long actualNodeOutputRows = fakeNode.getNodeStatistics().getNodeOutputRows();

        assertEquals("The NodeOutputRows was Inccorrect. Correct: 1000 Actual: "+ actualNodeOutputRows, 1000, actualNodeOutputRows); //$NON-NLS-1$
        assertEquals("The NodeNextBatchCalls was Inccorrect. Correct: 10 Actual: "+ actualNodeNextBatchCalls, 10, actualNodeNextBatchCalls); //$NON-NLS-1$
        assertEquals("The NodeBlocks was Inccorrect. Correct: 0 Actual: "+ actualNodeBlocks, 0, actualNodeBlocks); //$NON-NLS-1$
    }

    @Test public void testStatsCollectionBuffer() throws TeiidComponentException, TeiidProcessingException {
        List[] data = createData(1000);
        FakeRelationalNode fakeNode = createFakeNode(data);
        fakeNode.setUseBuffer(true);

        // read from fake node
        while(true) {
            TupleBatch batch = fakeNode.nextBatch();
            if(batch.getTerminationFlag()) {
                break;
            }
        }

        int actualNodeBlocks = fakeNode.getNodeStatistics().getNodeBlocks();
        int actualNodeNextBatchCalls = fakeNode.getNodeStatistics().getNodeNextBatchCalls();
        long actualNodeOutputRows = fakeNode.getNodeStatistics().getNodeOutputRows();

        assertEquals("The NodeOutputRows was Inccorrect. Correct: 1000 Actual: "+ actualNodeOutputRows, 1000, actualNodeOutputRows); //$NON-NLS-1$
        assertEquals("The NodeNextBatchCalls was Inccorrect. Correct: 10 Actual: "+ actualNodeNextBatchCalls, 10, actualNodeNextBatchCalls); //$NON-NLS-1$
        assertEquals("The NodeBlocks was Inccorrect. Correct: 0 Actual: "+ actualNodeBlocks, 0, actualNodeBlocks); //$NON-NLS-1$
    }

    @Test public void testCumulativeCalculation() {
        RelationalNode[] children = new RelationalNode[2];
        children[0] = createFakeNode(createData(1));
        children[1] = createFakeNode(createData(1));
        children[0].getNodeStatistics().setBatchEndTime(100);
        children[0].getNodeStatistics().collectCumulativeNodeStats(0L, RelationalNodeStatistics.BATCHCOMPLETE_STOP);
        children[0].getNodeStatistics().collectNodeStats(new RelationalNode[0]);
        children[1].getNodeStatistics().setBatchEndTime(200);
        children[1].getNodeStatistics().collectCumulativeNodeStats(0L, RelationalNodeStatistics.BATCHCOMPLETE_STOP);
        children[1].getNodeStatistics().collectNodeStats(new RelationalNode[0]);
        RelationalNodeStatistics stats = new RelationalNodeStatistics();
        stats.setBatchEndTime(1000);
        stats.setBatchStartTime(0);
        stats.collectCumulativeNodeStats(null, RelationalNodeStatistics.BLOCKEDEXCEPTION_STOP);
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
        List<ElementSymbol> elements = new ArrayList<ElementSymbol>();
        elements.add(element);

        FakeRelationalNode fakeNode = new FakeRelationalNode(1, data, 100);
        fakeNode.setElements(elements);
        CommandContext context = new CommandContext("group", null, null, null, 1, true);
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
