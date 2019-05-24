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
import static org.teiid.query.optimizer.TestOptimizer.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;

import org.junit.Test;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.BufferManager.TupleSourceType;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.common.buffer.TupleSource;
import org.teiid.common.buffer.impl.BufferManagerImpl;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.language.SortSpecification.NullOrdering;
import org.teiid.query.optimizer.TestOptimizer.ComparisonMode;
import org.teiid.query.optimizer.TestOptimizer.DupRemoveSortNode;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.FakeCapabilitiesFinder;
import org.teiid.query.processor.FakeDataManager;
import org.teiid.query.processor.FakeDataStore;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.TestProcessor;
import org.teiid.query.processor.relational.SortUtility.Mode;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.util.CommandContext;

@SuppressWarnings({"rawtypes", "nls"})
public class TestSortNode {

    public static final int BATCH_SIZE = 100;

    private void helpTestSort(List elements, List[] data, List sortElements, List sortTypes, List[] expected, Mode mode) throws TeiidComponentException, TeiidProcessingException {
        BufferManagerImpl mgr = BufferManagerFactory.getTestBufferManager(10000, BATCH_SIZE);
        long reserve = mgr.getReserveBatchBytes();
        CommandContext context = new CommandContext ("pid", "test", null, null, 1);               //$NON-NLS-1$ //$NON-NLS-2$

        BlockingFakeRelationalNode dataNode = new BlockingFakeRelationalNode(2, data);
        dataNode.setReturnPeriod(3);
        dataNode.setElements(elements);
        dataNode.initialize(context, mgr, null);

        SortNode sortNode = new SortNode(1);
        sortNode.setSortElements(new OrderBy(sortElements, sortTypes).getOrderByItems());
        sortNode.setMode(mode);
        sortNode.setElements(elements);
        sortNode.addChild(dataNode);
        sortNode.initialize(context, mgr, null);

        sortNode.open();
        assertTrue(sortNode.hasBuffer());
        int currentRow = 1;
        while(true) {
            try {
                TupleBatch batch = sortNode.nextBatch();
                for(int row = currentRow; row <= batch.getEndRow(); row++) {
                    assertEquals("Rows don't match at " + row, expected[row-1], batch.getTuple(row)); //$NON-NLS-1$
                }
                currentRow += batch.getRowCount();
                if(batch.getTerminationFlag()) {
                    break;
                }
            } catch (BlockedException e) {

            }
        }
        assertEquals(expected.length, currentRow - 1);
        assertEquals(reserve, mgr.getReserveBatchBytes());
    }

    /*
     * 1 batch all in memory
     */
    private void helpTestBasicSort(List[] expected, Mode mode) throws Exception {
        ElementSymbol es1 = new ElementSymbol("e1"); //$NON-NLS-1$
        es1.setType(DataTypeManager.DefaultDataClasses.INTEGER);

        ElementSymbol es2 = new ElementSymbol("e2"); //$NON-NLS-1$
        es2.setType(DataTypeManager.DefaultDataClasses.STRING);

        List elements = new ArrayList();
        elements.add(es1);
        elements.add(es2);

        List[] data = new List[20];
        for(int i=0; i<20; i++) {
            data[i] = new ArrayList();
            data[i].add(new Integer((i*51) % 11));

            String str = String.valueOf(i*3);
            str = str.substring(0,1);
            data[i].add(str);
        }

        List sortElements = new ArrayList();
        sortElements.add(es1);

        List sortTypes = new ArrayList();
        sortTypes.add(new Boolean(OrderBy.ASC));

        helpTestSort(elements, data, sortElements, sortTypes, expected, mode);
    }

    private void helpTestAllSorts(int batches) throws Exception {
        ElementSymbol es1 = new ElementSymbol("e1"); //$NON-NLS-1$
        es1.setType(DataTypeManager.DefaultDataClasses.INTEGER);

        List elements = new ArrayList();
        elements.add(es1);

        int rows = batches * BATCH_SIZE;

        ListNestedSortComparator<Integer> comparator = new ListNestedSortComparator<Integer>(new int[] {0}, OrderBy.DESC);

        List<Integer>[] expected = new List[rows];
        List<Integer>[] data = new List[rows];
        TreeSet<List<Integer>> distinct = new TreeSet<List<Integer>>(comparator);
        for(int i=0; i<rows; i++) {
            Integer value = new Integer((i*51) % 11);
            data[i] = Arrays.asList(value);
            expected[i] = Arrays.asList(value);
            distinct.add(Arrays.asList(value));
        }
        List[] expectedDistinct = distinct.toArray(new List[distinct.size()]);

        List sortElements = new ArrayList();
        sortElements.add(es1);

        List sortTypes = new ArrayList();
        sortTypes.add(new Boolean(OrderBy.DESC));

        Arrays.sort(expected, comparator);

        for (Mode mode : Mode.values()) {
            helpTestSort(elements, data, sortElements, sortTypes, mode==Mode.SORT?expected:expectedDistinct, mode);
        }
    }

    @Test public void testComparatorNullOrdering() {
        ListNestedSortComparator<Integer> comparator = new ListNestedSortComparator<Integer>(new int[] {0}, OrderBy.DESC);
        comparator.setNullOrdering(Arrays.asList(NullOrdering.FIRST));
        List<Integer>[] data = new List[3];
        data[0] = Arrays.asList(1);
        data[1] = Arrays.asList((Integer)null);
        data[2] = Arrays.asList(2);
        Arrays.sort(data, comparator);
        assertNull(data[0].get(0));
        comparator.setNullOrdering(Arrays.asList(NullOrdering.LAST));
        Arrays.sort(data, comparator);
        assertNull(data[2].get(0));
        comparator = new ListNestedSortComparator<Integer>(new int[] {0}, OrderBy.ASC);
        Arrays.sort(data, comparator);
        assertNull(data[0].get(0));
        comparator.setNullOrdering(Arrays.asList(NullOrdering.LAST));
        Arrays.sort(data, comparator);
        assertNull(data[2].get(0));
    }

    @Test public void testNoSort() throws Exception {
        helpTestAllSorts(0);
    }

    @Test public void testBasicSort() throws Exception {
        List[] expected = new List[] {
            Arrays.asList(new Object[] { new Integer(0), "0" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(0), "3" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(1), "2" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(1), "5" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(2), "1" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(2), "4" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(3), "6" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(3), "3" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(4), "3" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(5), "2" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(5), "5" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(6), "1" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(6), "4" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(7), "3" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(7), "3" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(8), "2" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(9), "1" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(9), "5" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(10), "9" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(10), "4" })                //$NON-NLS-1$
        };

        helpTestBasicSort(expected, Mode.SORT);
    }

    @Test public void testDupSortDesc() throws Exception {
        List[] expected = new List[] {
            Arrays.asList(new Object[] { new Integer(0), "0" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(0), "3" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(1), "2" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(1), "5" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(2), "1" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(2), "4" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(3), "6" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(3), "3" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(4), "3" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(5), "2" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(5), "5" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(6), "1" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(6), "4" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(7), "3" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(7), "3" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(8), "2" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(9), "1" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(9), "5" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(10), "9" }),    //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(10), "4" })                //$NON-NLS-1$
        };

        helpTestBasicSort(expected, Mode.SORT);
    }

    @Test public void testBasicSortRemoveDupSort() throws Exception {
        List[] expected = new List[] {
                Arrays.asList(new Object[] { new Integer(0), "0" }),    //$NON-NLS-1$
                Arrays.asList(new Object[] { new Integer(0), "3" }),    //$NON-NLS-1$
                Arrays.asList(new Object[] { new Integer(1), "2" }),    //$NON-NLS-1$
                Arrays.asList(new Object[] { new Integer(1), "5" }),    //$NON-NLS-1$
                Arrays.asList(new Object[] { new Integer(2), "1" }),    //$NON-NLS-1$
                Arrays.asList(new Object[] { new Integer(2), "4" }),    //$NON-NLS-1$
                Arrays.asList(new Object[] { new Integer(3), "3" }),    //$NON-NLS-1$
                Arrays.asList(new Object[] { new Integer(3), "6" }),    //$NON-NLS-1$
                Arrays.asList(new Object[] { new Integer(4), "3" }),    //$NON-NLS-1$
                Arrays.asList(new Object[] { new Integer(5), "2" }),    //$NON-NLS-1$
                Arrays.asList(new Object[] { new Integer(5), "5" }),    //$NON-NLS-1$
                Arrays.asList(new Object[] { new Integer(6), "1" }),    //$NON-NLS-1$
                Arrays.asList(new Object[] { new Integer(6), "4" }),    //$NON-NLS-1$
                Arrays.asList(new Object[] { new Integer(7), "3" }),    //$NON-NLS-1$
                Arrays.asList(new Object[] { new Integer(8), "2" }),    //$NON-NLS-1$
                Arrays.asList(new Object[] { new Integer(9), "1" }),    //$NON-NLS-1$
                Arrays.asList(new Object[] { new Integer(9), "5" }),    //$NON-NLS-1$
                Arrays.asList(new Object[] { new Integer(10), "4" }),    //$NON-NLS-1$
                Arrays.asList(new Object[] { new Integer(10), "9" })                //$NON-NLS-1$
            };

        helpTestBasicSort(expected, Mode.DUP_REMOVE_SORT);
    }

    @Test public void testBiggerSort() throws Exception {
        helpTestAllSorts(100);
    }

    @Test public void testAllSort() throws Exception {
        helpTestAllSorts(1);
    }

    @Test public void testDistinct() throws Exception {
        ElementSymbol es1 = new ElementSymbol("e1"); //$NON-NLS-1$
        es1.setType(DataTypeManager.DefaultDataClasses.INTEGER);
        ElementSymbol es2 = new ElementSymbol("e2"); //$NON-NLS-1$
        es2.setType(DataTypeManager.DefaultDataClasses.INTEGER);
        BufferManager bm = BufferManagerFactory.getStandaloneBufferManager();
        TupleBuffer tsid = bm.createTupleBuffer(Arrays.asList(es1, es2), "test", TupleSourceType.PROCESSOR); //$NON-NLS-1$
        tsid.addTuple(Arrays.asList(1, 1));
        tsid.addTuple(Arrays.asList(1, 2));
        tsid.close();
        SortUtility su = new SortUtility(tsid.createIndexedTupleSource(), Arrays.asList(es1), Arrays.asList(Boolean.TRUE), Mode.DUP_REMOVE_SORT, bm, "test", tsid.getSchema()); //$NON-NLS-1$
        su.sort();
        assertFalse(su.isDistinct());
    }

    @Test public void testOnePass() throws Exception {
        ElementSymbol es1 = new ElementSymbol("e1"); //$NON-NLS-1$
        es1.setType(DataTypeManager.DefaultDataClasses.INTEGER);
        ElementSymbol es2 = new ElementSymbol("e2"); //$NON-NLS-1$
        es2.setType(DataTypeManager.DefaultDataClasses.INTEGER);
        BufferManager bm = BufferManagerFactory.getStandaloneBufferManager();
        TupleBuffer tsid = bm.createTupleBuffer(Arrays.asList(es1, es2), "test", TupleSourceType.PROCESSOR); //$NON-NLS-1$
        tsid.addTuple(Arrays.asList(1, 1));
        tsid.addTuple(Arrays.asList(1, 2));
        tsid.close();
        SortUtility su = new SortUtility(tsid.createIndexedTupleSource(), Arrays.asList(es1), Arrays.asList(Boolean.TRUE), Mode.SORT, bm, "test", tsid.getSchema()); //$NON-NLS-1$
        List<TupleBuffer> buffers = su.onePassSort(true);
        assertEquals(1, buffers.size());
        assertTrue(!buffers.get(0).isForwardOnly());
    }

    @Test public void testSortUsingWorkingBuffer() throws TeiidException {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        // Create query
        String sql = "select e1 from (select e1, e2 from pm1.g1 union select e1, e2 from pm1.g2 limit 1) as x order by e2"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql, RealMetadataFactory.example1Cached(), null, capFinder,
                                      new String[] {"SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1", "SELECT pm1.g2.e1, pm1.g2.e2 FROM pm1.g2"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        checkNodeTypes(plan, new int[] {1}, new Class[] {DupRemoveSortNode.class});
        checkNodeTypes(plan, new int[] {1}, new Class[] {SortNode.class});

        FakeDataManager dataMgr = new FakeDataManager();
        dataMgr.setBlockOnce();
        FakeDataStore.sampleData1(dataMgr, RealMetadataFactory.example1Cached());
        TestProcessor.helpProcess(plan, dataMgr, new List[]{Collections.singletonList(null)});
    }

    @Test public void testStableSort() throws Exception {
        ElementSymbol es1 = new ElementSymbol("e1"); //$NON-NLS-1$
        es1.setType(DataTypeManager.DefaultDataClasses.INTEGER);
        BufferManager bm = BufferManagerFactory.getStandaloneBufferManager();
        TupleBuffer tsid = bm.createTupleBuffer(Arrays.asList(es1, es1), "test", TupleSourceType.PROCESSOR); //$NON-NLS-1$
        tsid.addTuple(Arrays.asList(1, 1));
        tsid.addTuple(Arrays.asList(1, 2));
        tsid.addTuple(Arrays.asList(1, 3));
        tsid.close();
        SortUtility su = new SortUtility(tsid.createIndexedTupleSource(), Arrays.asList(es1), Arrays.asList(Boolean.TRUE), Mode.SORT, bm, "test", tsid.getSchema()); //$NON-NLS-1$
        su.setBatchSize(1);
        su.setStableSort(true);
        TupleBuffer out = su.sort();
        TupleSource ts = out.createIndexedTupleSource();
        assertEquals(Arrays.asList(1,1), ts.nextTuple());
        assertEquals(Arrays.asList(1,2), ts.nextTuple());
        assertEquals(Arrays.asList(1,3), ts.nextTuple());
        assertNull(ts.nextTuple());
    }

    @Test public void testSortLimit() throws Exception {
        ElementSymbol es1 = new ElementSymbol("e1"); //$NON-NLS-1$
        es1.setType(DataTypeManager.DefaultDataClasses.INTEGER);
        BufferManager bm = BufferManagerFactory.getStandaloneBufferManager();
        TupleBuffer tsid = bm.createTupleBuffer(Arrays.asList(es1, es1), "test", TupleSourceType.PROCESSOR); //$NON-NLS-1$
        tsid.addTuple(Arrays.asList(4));
        tsid.addTuple(Arrays.asList(3));
        tsid.addTuple(Arrays.asList(2));
        tsid.addTuple(Arrays.asList(1));
        tsid.close();
        SortUtility su = new SortUtility(tsid.createIndexedTupleSource(), Arrays.asList(es1), Arrays.asList(Boolean.TRUE), Mode.SORT, bm, "test", tsid.getSchema()); //$NON-NLS-1$
        su.setBatchSize(2);
        TupleBuffer out = su.sort(2);
        TupleSource ts = out.createIndexedTupleSource();
        assertEquals(Arrays.asList(1), ts.nextTuple());
        assertEquals(Arrays.asList(2), ts.nextTuple());
        assertNull(ts.nextTuple());


        su = new SortUtility(tsid.createIndexedTupleSource(), Arrays.asList(es1), Arrays.asList(Boolean.TRUE), Mode.SORT, bm, "test", tsid.getSchema()); //$NON-NLS-1$
        su.setBatchSize(10);
        out = su.sort(2);
        ts = out.createIndexedTupleSource();
        assertEquals(Arrays.asList(1), ts.nextTuple());
        assertEquals(Arrays.asList(2), ts.nextTuple());
        assertNull(ts.nextTuple());
    }

}
