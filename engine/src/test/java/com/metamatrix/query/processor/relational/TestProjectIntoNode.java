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

package com.metamatrix.query.processor.relational;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.common.buffer.TupleSource;
import com.metamatrix.query.processor.FakeTupleSource;
import com.metamatrix.query.processor.ProcessorDataManager;
import com.metamatrix.query.processor.FakeTupleSource.FakeComponentException;
import com.metamatrix.query.sql.lang.BatchedUpdateCommand;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.Insert;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.tempdata.TempTableStoreImpl;
import com.metamatrix.query.util.CommandContext;


/** 
 * @since 4.2
 */
public class TestProjectIntoNode extends TestCase {
    
    // Rows should be a multiple of batch size for this test to work
    private static final int NUM_ROWS = 1000;

    private void helpTestNextBatch(int tupleBatchSize, boolean doBatching, boolean doBulkInsert, boolean exceptionOnClose) throws Exception {
        
        ProjectIntoNode node = new ProjectIntoNode(2);
        
        TupleSource tupleSource =  new FakeDataTupleSource(NUM_ROWS);
        RelationalNode child = new FakeRelationalNode(1,tupleSource, tupleBatchSize);
        node.addChild(child);
        node.setIntoGroup(new GroupSymbol("myGroup")); //$NON-NLS-1$
        ElementSymbol elementSymbol_1 = new ElementSymbol("myGroup.myElement1"); //$NON-NLS-1$
        ElementSymbol elementSymbol_2 = new ElementSymbol("myGroup.myElement2"); //$NON-NLS-1$
        elementSymbol_1.setType(Integer.class);
        elementSymbol_2.setType(String.class);
        ArrayList elements = new ArrayList();
        elements.add(elementSymbol_1);
        elements.add(elementSymbol_2);
        node.setIntoElements(elements); 
        node.setDoBatching(doBatching);
        node.setDoBulkInsert(doBulkInsert);
        node.setModelName("myModel"); //$NON-NLS-1$
        
        CommandContext context = new CommandContext();
        context.setProcessorID("processorID"); //$NON-NLS-1$
        BufferManager bm = NodeTestUtil.getTestBufferManager(tupleBatchSize, tupleBatchSize);
        ProcessorDataManager dataManager = new FakePDM(tupleBatchSize, exceptionOnClose);
        
        child.initialize(context, bm, dataManager);
        node.initialize(context, bm, dataManager);
        node.open();
        
        TupleBatch batch = null;
        // Do the remaining batches
        while(true) {
            try {
                batch = node.nextBatch();
                break;
            } catch (BlockedException e) {
                // Normal
            }
        }
        assertNotNull(batch);
        List[] tuples = batch.getAllTuples();
        assertEquals(1, tuples.length);
        Object[] columns = tuples[0].toArray();
        assertNotNull(columns);
        assertEquals(1, columns.length);
        // Should have inserted all rows
        assertEquals(new Integer(NUM_ROWS), columns[0]);
    }

    public void testNextBatch() throws Exception {
        helpTestNextBatch(100, true, false, false);
    }
    
    public void testNextBatch_BulkInsert() throws Exception {
        helpTestNextBatch(100, false, true, false);
    }
    
    public void testNextBatch_NoBatching() throws Exception {
        helpTestNextBatch(100, false, false, false);
    }

    public void testNextBatch_Size20Batches() throws Exception {
        helpTestNextBatch(20, true, false, false);
    }
    
    public void testNextBatch_ExceptionOnClose() throws Exception {
        try {
            helpTestNextBatch(100, true, false, true);
            fail("expected exception"); //$NON-NLS-1$
        } catch (FakeComponentException e) {
            //expected
        }
    }
    
    public void testNextBatch_ExceptionOnClose1() throws Exception {
        try {
            helpTestNextBatch(100, false, false, true);
            fail("expected exception"); //$NON-NLS-1$
        } catch (FakeComponentException e) {
            //expected
        }
    }

    private static final class FakePDM implements ProcessorDataManager {
        private int expectedBatchSize;
        private int callCount = 0;
        private boolean exceptionOnClose;
        private FakePDM(int expectedBatchSize, boolean exceptionOnClose) {
            this.expectedBatchSize = expectedBatchSize;
            this.exceptionOnClose = exceptionOnClose;
        }
        public Object lookupCodeValue(CommandContext context,String codeTableName,String returnElementName,String keyElementName,Object keyValue) throws BlockedException,MetaMatrixComponentException {return null;}
        public TupleSource registerRequest(Object processorID,Command command,String modelName,String connectorBindingId, int nodeID) throws MetaMatrixComponentException {
            callCount++;
            
            int batchSize = 1;
            
            // ensure that we have the right kind of insert, and that the data for this row is valid
            if (command instanceof Insert) {
            	Insert insert = (Insert)command;
            	if (insert.isBulk()) {
                    List batch = TempTableStoreImpl.getBulkRows(insert);
                    batchSize = batch.size();
                    assertEquals("Unexpected batch on call " + callCount, expectedBatchSize, batchSize); //$NON-NLS-1$
                    
                    for (int i = 0; i < batchSize; i++) {
                        ensureValue2((List)batch.get(i), 2, ((callCount-1) * batchSize) + i + 1);
                    }
            	} else {
            		ensureValue((Insert)command, 2, callCount);
            	}
            } else if ( command instanceof BatchedUpdateCommand ){
                BatchedUpdateCommand bu = (BatchedUpdateCommand)command;
                List batch = bu.getSubCommands();
  
                batchSize = batch.size();
                assertEquals("Unexpected batch on call " + callCount, expectedBatchSize, batchSize); //$NON-NLS-1$
            } else {
                fail("Unexpected command type"); //$NON-NLS-1$
            }
            List counts = Arrays.asList(new Object[] { new Integer(batchSize)});
            FakeTupleSource fakeTupleSource = new FakeTupleSource(null, new List[] {counts});
            fakeTupleSource.setExceptionOnClose(this.exceptionOnClose);
            return fakeTupleSource;
        }

        private void ensureValue(Insert command, int size, int value) {
            assertNotNull(command.getValues());
            assertEquals(size, command.getValues().size());
            assertEquals(new Integer(value), ((Constant)command.getValues().get(0)).getValue());
        }
        private void ensureValue2(List row, int size, int value) {
            assertNotNull(row);
            assertEquals(size, row.size());
            Object val = row.get(0);
            assertEquals(new Integer(value), val);
        }
        @Override
        public void clearCodeTables() {
        	
        }
    }
    
    private static final class FakeDataTupleSource implements TupleSource {
        private int currentRow = 0;
        private boolean block = true;
        private int rows;
        private FakeDataTupleSource(int rows) {
            this.rows = rows;
        }
        public void closeSource() throws MetaMatrixComponentException {}
        public List getSchema() {return null;}
        public List nextTuple() throws MetaMatrixComponentException {
            if (currentRow % 100 == 0 && block) {
                block = false;
                throw BlockedException.INSTANCE;
            }
            
            return (++currentRow > rows)
                    ? null
                    : Arrays.asList(new Object[] {new Integer(currentRow), Integer.toString(currentRow)});
        }
    }
}
