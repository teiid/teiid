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
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.events.EventDistributor;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.processor.CollectionTupleSource;
import org.teiid.query.processor.FakeTupleSource;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.processor.RegisterRequestParameter;
import org.teiid.query.processor.relational.ProjectIntoNode.Mode;
import org.teiid.query.sql.lang.BatchedUpdateCommand;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.util.CommandContext;

/**
 * @since 4.2
 */
public class TestProjectIntoNode {

    // Rows should be a multiple of batch size for this test to work
    private static final int NUM_ROWS = 1000;

    private void helpTestNextBatch(int tupleBatchSize, Mode mode) throws Exception {

        ProjectIntoNode node = new ProjectIntoNode(2);

        TupleSource tupleSource =  new FakeDataTupleSource(NUM_ROWS);
        RelationalNode child = new FakeRelationalNode(1,tupleSource, tupleBatchSize);
        node.addChild(child);
        node.setIntoGroup(new GroupSymbol("myGroup")); //$NON-NLS-1$
        ElementSymbol elementSymbol_1 = new ElementSymbol("myGroup.myElement1"); //$NON-NLS-1$
        ElementSymbol elementSymbol_2 = new ElementSymbol("myGroup.myElement2"); //$NON-NLS-1$
        elementSymbol_1.setType(Integer.class);
        elementSymbol_2.setType(String.class);
        List<ElementSymbol> elements = Arrays.asList(elementSymbol_1, elementSymbol_2);
        node.setIntoElements(elements);
        child.setElements(elements);
        node.setMode(mode);
        node.setModelName("myModel"); //$NON-NLS-1$

        CommandContext context = new CommandContext();
        BufferManager bm = BufferManagerFactory.getTestBufferManager(tupleBatchSize, tupleBatchSize);
        ProcessorDataManager dataManager = new FakePDM(tupleBatchSize);

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

    @Test public void testNextBatch() throws Exception {
        helpTestNextBatch(100, Mode.BATCH);
    }

    @Test public void testNextBatch_NoBatching() throws Exception {
        helpTestNextBatch(100, Mode.SINGLE);
    }

    @Test public void testNextBatch_Size20Batches() throws Exception {
        helpTestNextBatch(20, Mode.BATCH);
    }

    @Test public void testNextBatch_Iterator() throws Exception {
        helpTestNextBatch(100, Mode.ITERATOR);
    }

    private static final class FakePDM implements ProcessorDataManager {
        private int expectedBatchSize;
        private int callCount = 0;
        private FakePDM(int expectedBatchSize) {
            this.expectedBatchSize = expectedBatchSize;
        }
        public Object lookupCodeValue(CommandContext context,String codeTableName,String returnElementName,String keyElementName,Object keyValue) throws BlockedException,TeiidComponentException {return null;}
        public TupleSource registerRequest(CommandContext context,Command command,String modelName,RegisterRequestParameter parameterObject) throws TeiidComponentException, TeiidProcessingException {
            callCount++;

            int batchSize = 1;

            // ensure that we have the right kind of insert, and that the data for this row is valid
            if (command instanceof Insert) {
                Insert insert = (Insert)command;
                if (isBulk(insert)) {
                    List batch = getBulkRows(insert, insert.getVariables());
                    batchSize = batch.size();
                    assertEquals("Unexpected batch on call " + callCount, expectedBatchSize, batchSize); //$NON-NLS-1$

                    for (int i = 0; i < batchSize; i++) {
                        ensureValue2((List)batch.get(i), 2, ((callCount-1) * batchSize) + i + 1);
                    }
                } else if (insert.getTupleSource() != null) {
                    TupleSource ts = insert.getTupleSource();
                    List tuple = null;
                    int i = 0;
                    while ((tuple = ts.nextTuple()) != null) {
                        ensureValue2(tuple, 2, ++i);
                    }
                    batchSize = i;
                } else {
                    ensureValue(insert, 2, callCount);
                }
            } else if ( command instanceof BatchedUpdateCommand ){
                BatchedUpdateCommand bu = (BatchedUpdateCommand)command;
                List<Command> batch = bu.getUpdateCommands();

                batchSize = batch.size();
                assertEquals("Unexpected batch on call " + callCount, expectedBatchSize, batchSize); //$NON-NLS-1$
            } else {
                fail("Unexpected command type"); //$NON-NLS-1$
            }
            if (batchSize > 1) {
                return CollectionTupleSource.createUpdateCountArrayTupleSource(batchSize);
            }
            List counts = Arrays.asList(new Object[] { new Integer(batchSize)});
            FakeTupleSource fakeTupleSource = new FakeTupleSource(null, new List[] {counts});
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
        public EventDistributor getEventDistributor() {
            // TODO Auto-generated method stub
            return null;
        }
    }

    private static final class FakeDataTupleSource implements TupleSource {
        private int currentRow = 0;
        private boolean block = true;
        private int rows;
        private FakeDataTupleSource(int rows) {
            this.rows = rows;
        }
        public void closeSource() {}
        public List getSchema() {return null;}
        public List nextTuple() throws TeiidComponentException {
            if (currentRow % 100 == 0 && block) {
                block = false;
                throw BlockedException.INSTANCE;
            }

            return (++currentRow > rows)
                    ? null
                    : Arrays.asList(new Object[] {new Integer(currentRow), Integer.toString(currentRow)});
        }
    }

    public static List<List<Object>> getBulkRows(Insert insert, List<ElementSymbol> elements) throws ExpressionEvaluationException, BlockedException, TeiidComponentException {
        int bulkRowCount = 1;
        if (isBulk(insert)) {
            Constant c = (Constant)insert.getValues().get(0);
            bulkRowCount = ((List<?>)c.getValue()).size();
        }

        List<List<Object>> tuples = new ArrayList<List<Object>>(bulkRowCount);

        for (int row = 0; row < bulkRowCount; row++) {
            List<Object> currentRow = new ArrayList<Object>(insert.getValues().size());
            for (ElementSymbol symbol : elements) {
                int index = insert.getVariables().indexOf(symbol);
                Object value = null;
                if (index != -1) {
                    if (isBulk(insert)) {
                        Constant multiValue = (Constant)insert.getValues().get(index);
                        value = ((List<?>)multiValue.getValue()).get(row);
                    } else {
                        Expression expr = (Expression)insert.getValues().get(index);
                        value = Evaluator.evaluate(expr);
                    }
                }
                currentRow.add(value);
            }
            tuples.add(currentRow);
        }
        return tuples;
    }

    public static boolean isBulk(Insert insert) {
        if (insert.getValues() == null) {
            return false;
        }
        if (!(insert.getValues().get(0) instanceof Constant)) {
            return false;
        }
        return ((Constant)insert.getValues().get(0)).isMultiValued();
    }

}
