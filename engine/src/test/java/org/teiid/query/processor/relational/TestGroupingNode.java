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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.api.exception.query.FunctionExecutionException;
import org.teiid.client.plan.PlanNode;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.impl.BufferManagerImpl;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.query.function.FunctionDescriptor;
import org.teiid.query.function.aggregate.AggregateFunction;
import org.teiid.query.processor.FakeDataManager;
import org.teiid.query.processor.FakeTupleSource;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.Function;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.util.CommandContext;

@SuppressWarnings({"unchecked", "rawtypes", "nls"})
public class TestGroupingNode {

    public static FakeTupleSource createTupleSource1() {
        List<ElementSymbol> symbols = new ArrayList<ElementSymbol>();
        symbols.add(new ElementSymbol("col1")); //$NON-NLS-1$
        symbols.get(0).setType(DataTypeManager.DefaultDataClasses.INTEGER);
        symbols.add(new ElementSymbol("col2")); //$NON-NLS-1$
        symbols.get(1).setType(DataTypeManager.DefaultDataClasses.INTEGER);
        List[] tuples = new List[] {
            Arrays.asList(new Object[] { new Integer(5), new Integer(3) }),
            Arrays.asList(new Object[] { new Integer(2), new Integer(1) }),
            Arrays.asList(new Object[] { new Integer(4), null }),
            Arrays.asList(new Object[] { null, new Integer(3) }),
            Arrays.asList(new Object[] { new Integer(0), new Integer(4) }),
            Arrays.asList(new Object[] { new Integer(1), new Integer(2) }),
            Arrays.asList(new Object[] { new Integer(4), new Integer(2) }),
            Arrays.asList(new Object[] { new Integer(6), new Integer(4) }),
            Arrays.asList(new Object[] { new Integer(6), new Integer(3) }),
            Arrays.asList(new Object[] { new Integer(3), new Integer(0) }),
            Arrays.asList(new Object[] { new Integer(4), new Integer(3) }),
            Arrays.asList(new Object[] { new Integer(2), new Integer(1) }),
            Arrays.asList(new Object[] { new Integer(2), new Integer(1) }),
            Arrays.asList(new Object[] { new Integer(2), new Integer(2) }),
            Arrays.asList(new Object[] { null, null }),
        };

        return new FakeTupleSource(symbols, tuples);
    }

    private void helpProcess(BufferManager mgr,
                             GroupingNode node,
                             CommandContext context,
                             List[] expected, ProcessorDataManager dataMgr) throws TeiidComponentException,
                                             BlockedException,
                                             TeiidProcessingException {
        FakeTupleSource dataSource = createTupleSource1();
        helpProcess(mgr, node, context, expected, dataSource, dataMgr);
    }

    private void helpProcess(BufferManager mgr,
                             GroupingNode node,
                             CommandContext context,
                             List[] expected,
                             FakeTupleSource dataSource, ProcessorDataManager dataMgr) throws TeiidComponentException,
                                                    BlockedException,
                                                    TeiidProcessingException {
        RelationalNode dataNode = new FakeRelationalNode(0, dataSource, mgr.getProcessorBatchSize());
        dataNode.setElements(dataSource.getSchema());
        node.addChild(dataNode);
        node.initialize(context, mgr, dataMgr);
        node.open();

        int currentRow = 1;
        while(true) {
            try {
                TupleBatch batch = node.nextBatch();
                for(int row = currentRow; row <= batch.getEndRow(); row++) {
                    List tuple = batch.getTuple(row);
                    assertEquals("Rows don't match at " + row, expected[row-1], tuple); //$NON-NLS-1$
                }
                currentRow += batch.getRowCount();
                if(batch.getTerminationFlag()) {
                    break;
                }
            } catch (BlockedException e) {
                //ignore
            }
        }
        assertEquals(expected.length, currentRow -1);
    }

    // ################################## ACTUAL TESTS ################################

    @Test public void test1() throws Exception {
        BufferManager mgr = BufferManagerFactory.getStandaloneBufferManager();

        // Set up
        GroupingNode node = new GroupingNode(1);
        List outputElements = new ArrayList();
        ElementSymbol col1 = new ElementSymbol("col1"); //$NON-NLS-1$
        col1.setType(Integer.class);
        ElementSymbol col2 = new ElementSymbol("col2"); //$NON-NLS-1$
        col2.setType(Integer.class);
        outputElements.add(col1);
        outputElements.add(new AggregateSymbol("COUNT", false, null)); //$NON-NLS-1$ //$NON-NLS-2$
        outputElements.add(new AggregateSymbol("COUNT", false, col2)); //$NON-NLS-1$ //$NON-NLS-2$
        outputElements.add(new AggregateSymbol("COUNT", true, col2)); //$NON-NLS-1$ //$NON-NLS-2$
        outputElements.add(new AggregateSymbol("SUM", false, col2)); //$NON-NLS-1$ //$NON-NLS-2$
        outputElements.add(new AggregateSymbol("SUM", true, col2)); //$NON-NLS-1$ //$NON-NLS-2$
        outputElements.add(new AggregateSymbol("AVG", false, col2)); //$NON-NLS-1$ //$NON-NLS-2$
        outputElements.add(new AggregateSymbol("AVG", true, col2)); //$NON-NLS-1$ //$NON-NLS-2$
        outputElements.add(new AggregateSymbol("MIN", false, col2)); //$NON-NLS-1$ //$NON-NLS-2$
        outputElements.add(new AggregateSymbol("MIN", true, col2)); //$NON-NLS-1$ //$NON-NLS-2$
        outputElements.add(new AggregateSymbol("MAX", false, col2)); //$NON-NLS-1$ //$NON-NLS-2$
        outputElements.add(new AggregateSymbol("MAX", true, col2)); //$NON-NLS-1$ //$NON-NLS-2$
        node.setElements(outputElements);

        List groupingElements = new ArrayList();
        groupingElements.add(col1);
        node.setOrderBy(new OrderBy(groupingElements).getOrderByItems());
        CommandContext context = new CommandContext("pid", "test", null, null, 1);               //$NON-NLS-1$ //$NON-NLS-2$

        List[] expected = new List[] {
            Arrays.asList(new Object[] { null, new Integer(2), new Integer(1), new Integer(1), new Long(3), new Long(3), new BigDecimal(3.0), new BigDecimal(3.0), new Integer(3), new Integer(3), new Integer(3), new Integer(3) }),
            Arrays.asList(new Object[] { new Integer(0), new Integer(1), new Integer(1), new Integer(1), new Long(4), new Long(4), new BigDecimal(4.0), new BigDecimal(4.0), new Integer(4), new Integer(4), new Integer(4), new Integer(4) }),
            Arrays.asList(new Object[] { new Integer(1), new Integer(1), new Integer(1), new Integer(1), new Long(2), new Long(2), new BigDecimal(2.0), new BigDecimal(2.0), new Integer(2), new Integer(2), new Integer(2), new Integer(2) }),
            Arrays.asList(new Object[] { new Integer(2), new Integer(4), new Integer(4), new Integer(2), new Long(5), new Long(3), new BigDecimal(1.25), new BigDecimal(1.5), new Integer(1), new Integer(1), new Integer(2), new Integer(2) }),
            Arrays.asList(new Object[] { new Integer(3), new Integer(1), new Integer(1), new Integer(1), new Long(0), new Long(0), new BigDecimal(0.0), new BigDecimal(0.0), new Integer(0), new Integer(0), new Integer(0), new Integer(0) }),
            Arrays.asList(new Object[] { new Integer(4), new Integer(3), new Integer(2), new Integer(2), new Long(5), new Long(5), new BigDecimal(2.5), new BigDecimal(2.5), new Integer(2), new Integer(2), new Integer(3), new Integer(3) }),
            Arrays.asList(new Object[] { new Integer(5), new Integer(1), new Integer(1), new Integer(1), new Long(3), new Long(3), new BigDecimal(3.0), new BigDecimal(3.0), new Integer(3), new Integer(3), new Integer(3), new Integer(3) }),
            Arrays.asList(new Object[] { new Integer(6), new Integer(2), new Integer(2), new Integer(2), new Long(7), new Long(7), new BigDecimal(3.5), new BigDecimal(3.5), new Integer(3), new Integer(3), new Integer(4), new Integer(4) })
        };

        helpProcess(mgr, node, context, expected, null);

        //ensure that the distinct input type is correct
        AggregateFunction[][] functions = node.getFunctions();
        AggregateFunction countDist = functions[5][0];
        SortingFilter dup = (SortingFilter)countDist;
        assertEquals(DataTypeManager.DefaultDataClasses.INTEGER, dup.getElements().get(0).getType());
    }

    @Test public void test2() throws Exception {
        BufferManager mgr = BufferManagerFactory.getStandaloneBufferManager();

        GroupingNode node = getExampleGroupingNode();
        CommandContext context = new CommandContext("pid", "test", null, null, 1);               //$NON-NLS-1$ //$NON-NLS-2$

        List[] expected = new List[] {
            Arrays.asList(new Object[] { null, new Integer(1) }),
            Arrays.asList(new Object[] { new Integer(0), new Integer(1) }),
            Arrays.asList(new Object[] { new Integer(1), new Integer(1) }),
            Arrays.asList(new Object[] { new Integer(2), new Integer(2) }),
            Arrays.asList(new Object[] { new Integer(3), new Integer(1) }),
            Arrays.asList(new Object[] { new Integer(4), new Integer(2) }),
            Arrays.asList(new Object[] { new Integer(5), new Integer(1) }),
            Arrays.asList(new Object[] { new Integer(6), new Integer(2) })
        };

        helpProcess(mgr, node, context, expected, null);
    }

    // Same as test2, but uses processor batch size smaller than number of groups
    @Test public void test3() throws Exception {
        BufferManagerImpl mgr = BufferManagerFactory.createBufferManager();
        mgr.setProcessorBatchSize(5);

        GroupingNode node = getExampleGroupingNode();
        CommandContext context = new CommandContext("pid", "test", null, null,  1);               //$NON-NLS-1$ //$NON-NLS-2$

        List[] expected = new List[] {
            Arrays.asList(new Object[] { null, new Integer(1) }),
            Arrays.asList(new Object[] { new Integer(0), new Integer(1) }),
            Arrays.asList(new Object[] { new Integer(1), new Integer(1) }),
            Arrays.asList(new Object[] { new Integer(2), new Integer(2) }),
            Arrays.asList(new Object[] { new Integer(3), new Integer(1) }),
            Arrays.asList(new Object[] { new Integer(4), new Integer(2) }),
            Arrays.asList(new Object[] { new Integer(5), new Integer(1) }),
            Arrays.asList(new Object[] { new Integer(6), new Integer(2) })
        };

        helpProcess(mgr, node, context, expected, null);
    }

    @Test public void testDefect5769() throws Exception {
        BufferManager mgr = BufferManagerFactory.getStandaloneBufferManager();

        ElementSymbol bigDecimal = new ElementSymbol("value"); //$NON-NLS-1$
        bigDecimal.setType(DataTypeManager.DefaultDataClasses.BIG_DECIMAL);

        // Set up
        GroupingNode node = new GroupingNode(1);
        List outputElements = new ArrayList();
        outputElements.add(new AggregateSymbol("SUM", false, bigDecimal)); //$NON-NLS-1$ //$NON-NLS-2$
        outputElements.add(new AggregateSymbol("AVG", false, bigDecimal)); //$NON-NLS-1$ //$NON-NLS-2$
        node.setElements(outputElements);

        CommandContext context = new CommandContext("pid", "test", null, null, 1);               //$NON-NLS-1$ //$NON-NLS-2$

        List[] data = new List[] {
            Arrays.asList(new Object[] { new BigDecimal("0.0") }),     //$NON-NLS-1$
            Arrays.asList(new Object[] { new BigDecimal("1.0") }),     //$NON-NLS-1$
            Arrays.asList(new Object[] { new BigDecimal("2.0") }),     //$NON-NLS-1$
            Arrays.asList(new Object[] { new BigDecimal("3.0") }),     //$NON-NLS-1$
            Arrays.asList(new Object[] { new BigDecimal("4.0") }) //$NON-NLS-1$
        };

        List[] expected = new List[] {
            Arrays.asList(new Object[] { new BigDecimal("10.0"), new BigDecimal("2.0") }) //$NON-NLS-1$ //$NON-NLS-2$
        };

        List symbols = new ArrayList();
        symbols.add(bigDecimal);
        FakeTupleSource dataSource = new FakeTupleSource(symbols, data);
        helpProcess(mgr, node, context, expected, dataSource, null);
    }

    @Test public void testBlocking() throws Exception {
        BufferManager mgr = BufferManagerFactory.getStandaloneBufferManager();

        ElementSymbol bigDecimal = new ElementSymbol("value"); //$NON-NLS-1$
        bigDecimal.setType(DataTypeManager.DefaultDataClasses.BIG_DECIMAL);

        // Set up
        GroupingNode node = new GroupingNode(1) {
            boolean block = true;
            @Override
            protected void closeGroup(int colDiff, boolean reset,
                    CommandContext context) throws FunctionExecutionException,
                    ExpressionEvaluationException, TeiidComponentException,
                    TeiidProcessingException {
                if (block) {
                    block = false;
                    throw BlockedException.INSTANCE;
                }
                block = true;
                super.closeGroup(colDiff, reset, context);
            }
        };
        List outputElements = new ArrayList();
        outputElements.add(new AggregateSymbol("SUM", false, bigDecimal)); //$NON-NLS-1$ //$NON-NLS-2$
        outputElements.add(new AggregateSymbol("AVG", false, bigDecimal)); //$NON-NLS-1$ //$NON-NLS-2$
        node.setElements(outputElements);

        CommandContext context = new CommandContext("pid", "test", null, null, 1);               //$NON-NLS-1$ //$NON-NLS-2$

        List[] data = new List[] {
            Arrays.asList(new Object[] { new BigDecimal("0.0") }),     //$NON-NLS-1$
            Arrays.asList(new Object[] { new BigDecimal("1.0") }),     //$NON-NLS-1$
            Arrays.asList(new Object[] { new BigDecimal("2.0") }),     //$NON-NLS-1$
            Arrays.asList(new Object[] { new BigDecimal("3.0") }),     //$NON-NLS-1$
            Arrays.asList(new Object[] { new BigDecimal("4.0") }) //$NON-NLS-1$
        };

        List[] expected = new List[] {
            Arrays.asList(new Object[] { new BigDecimal("10.0"), new BigDecimal("2.0") }) //$NON-NLS-1$ //$NON-NLS-2$
        };

        List symbols = new ArrayList();
        symbols.add(bigDecimal);
        FakeTupleSource dataSource = new FakeTupleSource(symbols, data) {
            boolean end;
            @Override
            public List nextTuple() throws TeiidComponentException {
                List result = super.nextTuple();
                if (end) {
                    fail();
                }
                end = result == null;
                return result;
            }
        };
        helpProcess(mgr, node, context, expected, dataSource, null);
    }

    @Test public void testdefect9842() throws Exception {
        BufferManager mgr = BufferManagerFactory.getStandaloneBufferManager();

        ElementSymbol col1 = new ElementSymbol("col1"); //$NON-NLS-1$
        col1.setType(Integer.class);
        ElementSymbol bigDecimal = new ElementSymbol("value"); //$NON-NLS-1$
        bigDecimal.setType(DataTypeManager.DefaultDataClasses.BIG_DECIMAL);

        // Set up
        GroupingNode node = new GroupingNode(1);
        List outputElements = new ArrayList();
        outputElements.add(col1);
        outputElements.add(new AggregateSymbol("SUM", false, bigDecimal)); //$NON-NLS-1$ //$NON-NLS-2$
        outputElements.add(new AggregateSymbol("AVG", false, bigDecimal)); //$NON-NLS-1$ //$NON-NLS-2$
        node.setElements(outputElements);

        // Set grouping elements to null
        List groupingElements = new ArrayList();
        groupingElements.add(col1);
        node.setOrderBy(new OrderBy(groupingElements).getOrderByItems());
        CommandContext context = new CommandContext("pid", "test", null, null, 1);               //$NON-NLS-1$ //$NON-NLS-2$

        List[] data = new List[] {
            Arrays.asList(new Object[] { new Integer(1), new BigDecimal("0.0") }),     //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(1), new BigDecimal("1.0") }),     //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(2), new BigDecimal("2.0") }),     //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(2), new BigDecimal("3.0") }),     //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(2), new BigDecimal("4.0") }) //$NON-NLS-1$
        };

        List[] expected = new List[] {
            Arrays.asList(new Object[] { new Integer(1), new BigDecimal("1.0"), new BigDecimal(.5) }), //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(2), new BigDecimal("9.0"), new BigDecimal("3.0") }) //$NON-NLS-1$
        };

        List symbols = new ArrayList();
        symbols.add(col1);
        symbols.add(bigDecimal);
        FakeTupleSource dataSource = new FakeTupleSource(symbols, data);
        helpProcess(mgr, node, context, expected, dataSource, null);
    }

    private void helpTestLookupFunctionInAggregate(int batchSize) throws Exception {
        BufferManagerImpl mgr = BufferManagerFactory.createBufferManager();
        mgr.setProcessorBatchSize(batchSize);

        // Set up
        GroupingNode node = new GroupingNode(1);
        List outputElements = new ArrayList();
        ElementSymbol col1 = new ElementSymbol("col1"); //$NON-NLS-1$
        col1.setType(Integer.class);
        ElementSymbol col2 = new ElementSymbol("col2"); //$NON-NLS-1$
        col2.setType(Integer.class);

        Function func = new Function("lookup", new Expression[] { new Constant("pm1.g1"), new Constant("e2"), new Constant("e1"), col2 }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        FunctionDescriptor desc = RealMetadataFactory.SFM.getSystemFunctionLibrary().findFunction("lookup", new Class[] { String.class, String.class, String.class, Integer.class } ); //$NON-NLS-1$
        func.setFunctionDescriptor(desc);
        func.setType(DataTypeManager.DefaultDataClasses.INTEGER);

        outputElements.add(col1);
        outputElements.add(new AggregateSymbol("COUNT", false, func)); //$NON-NLS-1$ //$NON-NLS-2$
        outputElements.add(new AggregateSymbol("SUM", false, func)); //$NON-NLS-1$ //$NON-NLS-2$
        outputElements.add(new AggregateSymbol("SUM", true, func)); //$NON-NLS-1$ //$NON-NLS-2$
        node.setElements(outputElements);

        List groupingElements = new ArrayList();
        groupingElements.add(col1);
        node.setOrderBy(new OrderBy(groupingElements).getOrderByItems());
        CommandContext context = new CommandContext("pid", "test", null, null, 1);    //$NON-NLS-1$ //$NON-NLS-2$

        FakeDataManager dataMgr = new FakeDataManager();
        dataMgr.setThrowBlocked(true);
        Map valueMap = new HashMap();
        valueMap.put(new Integer(0), new Integer(1));
        valueMap.put(new Integer(1), new Integer(2));
        valueMap.put(new Integer(2), new Integer(3));
        valueMap.put(new Integer(3), new Integer(4));
        valueMap.put(new Integer(4), new Integer(5));
        dataMgr.defineCodeTable("pm1.g1", "e1", "e2", valueMap); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

        List[] expected = new List[] {
            Arrays.asList(new Object[] { null,           new Integer(1), new Long(4), new Long(4) }),
            Arrays.asList(new Object[] { new Integer(0), new Integer(1), new Long(5), new Long(5) }),
            Arrays.asList(new Object[] { new Integer(1), new Integer(1), new Long(3), new Long(3) }),
            Arrays.asList(new Object[] { new Integer(2), new Integer(4), new Long(9), new Long(5) }),
            Arrays.asList(new Object[] { new Integer(3), new Integer(1), new Long(1), new Long(1) }),
            Arrays.asList(new Object[] { new Integer(4), new Integer(2), new Long(7), new Long(7) }),
            Arrays.asList(new Object[] { new Integer(5), new Integer(1), new Long(4), new Long(4) }),
            Arrays.asList(new Object[] { new Integer(6), new Integer(2), new Long(9), new Long(9) })
        };

        helpProcess(mgr, node, context, expected, dataMgr);
    }

    public void helpTestEmptyGroup(boolean groupBy) throws Exception {
        BufferManager mgr = BufferManagerFactory.getStandaloneBufferManager();

        ElementSymbol col1 = new ElementSymbol("col1"); //$NON-NLS-1$
        col1.setType(Integer.class);
        ElementSymbol bigDecimal = new ElementSymbol("value"); //$NON-NLS-1$
        bigDecimal.setType(DataTypeManager.DefaultDataClasses.BIG_DECIMAL);

        // Set up
        GroupingNode node = new GroupingNode(1);
        List outputElements = new ArrayList();
        outputElements.add(new AggregateSymbol("SUM", false, bigDecimal)); //$NON-NLS-1$ //$NON-NLS-2$
        outputElements.add(new AggregateSymbol("AVG", false, bigDecimal)); //$NON-NLS-1$ //$NON-NLS-2$
        node.setElements(outputElements);

        // Set grouping elements to null
        if (groupBy) {
            List groupingElements = new ArrayList();
            groupingElements.add(col1.clone()); //$NON-NLS-1$
            node.setOrderBy(new OrderBy(groupingElements).getOrderByItems());
        }
        CommandContext context = new CommandContext("pid", "test", null, null, 1);               //$NON-NLS-1$ //$NON-NLS-2$

        List[] data = new List[] {
        };

        List[] expected = new List[] {
            Arrays.asList(new Object[] { null, null })
        };

        if (groupBy) {
            expected = new List[] {};
        }

        List symbols = new ArrayList();
        symbols.add(col1);
        symbols.add(bigDecimal);
        FakeTupleSource dataSource = new FakeTupleSource(symbols, data);
        helpProcess(mgr, node, context, expected, dataSource, null);
    }

    @Test public void testTestEmptyGroupWithoutGroupBy() throws Exception {
        helpTestEmptyGroup(false);
    }

    @Test public void testTestEmptyGroupWithGroupBy() throws Exception {
        helpTestEmptyGroup(true);
    }

    @Test public void testLookupFunctionMultipleBatches() throws Exception {
        helpTestLookupFunctionInAggregate(3);
    }

    @Test public void testDupSort() throws Exception {
        BufferManager mgr = BufferManagerFactory.getStandaloneBufferManager();

        GroupingNode node = getExampleGroupingNode();
        node.setRemoveDuplicates(true);
        CommandContext context = new CommandContext("pid", "test", null, null,  1);               //$NON-NLS-1$ //$NON-NLS-2$

        List[] expected = new List[] {
            Arrays.asList(new Object[] { null, new Integer(1) }),
            Arrays.asList(new Object[] { new Integer(0), new Integer(1) }),
            Arrays.asList(new Object[] { new Integer(1), new Integer(1) }),
            Arrays.asList(new Object[] { new Integer(2), new Integer(2) }),
            Arrays.asList(new Object[] { new Integer(3), new Integer(1) }),
            Arrays.asList(new Object[] { new Integer(4), new Integer(2) }),
            Arrays.asList(new Object[] { new Integer(5), new Integer(1) }),
            Arrays.asList(new Object[] { new Integer(6), new Integer(2) })
        };

        helpProcess(mgr, node, context, expected, null);
    }

    private GroupingNode getExampleGroupingNode() {
        GroupingNode node = new GroupingNode(1);
        List outputElements = new ArrayList();
        ElementSymbol col1 = new ElementSymbol("col1"); //$NON-NLS-1$
        col1.setType(Integer.class);
        ElementSymbol col2 = new ElementSymbol("col2"); //$NON-NLS-1$
        col2.setType(Integer.class);
        outputElements.add(col1);
        outputElements.add(new AggregateSymbol("COUNT", true, col2)); //$NON-NLS-1$ //$NON-NLS-2$
        node.setElements(outputElements);

        List groupingElements = new ArrayList();
        groupingElements.add(col1);
        node.setOrderBy(new OrderBy(groupingElements).getOrderByItems());
        return node;
    }

    @Test public void testDescriptionProperties() {
        GroupingNode node = getExampleGroupingNode();
        SymbolMap outputMapping = new SymbolMap();
        outputMapping.addMapping(new ElementSymbol("agg0"), new AggregateSymbol("count", false, null));
        node.setOutputMapping(outputMapping);
        PlanNode pn = node.getDescriptionProperties();
        assertTrue(pn.toString().contains("agg0=count(*)"));
    }

}
