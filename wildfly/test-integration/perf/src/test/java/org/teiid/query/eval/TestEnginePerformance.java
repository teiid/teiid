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

package org.teiid.query.eval;

import static org.junit.Assert.*;
import static org.teiid.query.processor.TestProcessor.*;

import java.io.ByteArrayInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Date;
import java.sql.Time;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.teiid.api.exception.query.QueryParserException;
import org.teiid.client.BatchSerializer;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.STree;
import org.teiid.common.buffer.STree.InsertMode;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.impl.BufferFrontedFileStoreCache;
import org.teiid.common.buffer.impl.BufferManagerImpl;
import org.teiid.common.buffer.impl.FileStorageManager;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.AccessibleByteArrayOutputStream;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.processor.FakeDataManager;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.TestTextTable;
import org.teiid.query.processor.relational.BlockingFakeRelationalNode;
import org.teiid.query.processor.relational.EnhancedSortMergeJoinStrategy;
import org.teiid.query.processor.relational.FakeRelationalNode;
import org.teiid.query.processor.relational.JoinNode;
import org.teiid.query.processor.relational.JoinStrategy;
import org.teiid.query.processor.relational.MergeJoinStrategy;
import org.teiid.query.processor.relational.MergeJoinStrategy.SortOption;
import org.teiid.query.processor.relational.RelationalNode;
import org.teiid.query.processor.relational.SortNode;
import org.teiid.query.processor.relational.SortUtility.Mode;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.util.CommandContext;

@FixMethodOrder(MethodSorters.JVM)
@SuppressWarnings("nls")
public class TestEnginePerformance {

    private static boolean debug = false;

    private static BufferManagerImpl bm;
    private static BufferFrontedFileStoreCache cache;
    private static ExecutorService es;
    private static Random r = new Random(0);

    private final class PreparedPlanTask extends Task {
        private final List<?> preparedValues;
        private final QueryMetadataInterface metadata;
        private final ProcessorPlan plan;
        private final Command command;
        private final int rowCount;
        ProcessorDataManager dataManager = new FakeDataManager();

        private PreparedPlanTask(List<?> preparedValues,
                QueryMetadataInterface metadata, ProcessorPlan plan,
                Command command, int rowCount) {
            this.preparedValues = preparedValues;
            this.metadata = metadata;
            this.plan = plan;
            this.command = command;
            this.rowCount = rowCount;
        }

        @Override
        public Void call() throws Exception {
            processPreparedPlan(preparedValues, command, metadata, dataManager, plan, rowCount);
            return null;
        }

        @Override
        public Task clone() {
            return new PreparedPlanTask(preparedValues, metadata, plan.clone(), command, rowCount);
        }
    }

    abstract class Task implements Callable<Void> {
        public Task clone() {
            return this;
        }
    }

    private void runTask(final int iterations, int threadCount,
            final Task task) throws InterruptedException, Exception {
        List<Callable<Void>> tasks = new ArrayList<Callable<Void>>(threadCount);
        for (int i = 0; i < threadCount; i++) {
            final Task threadTask = task.clone();
            tasks.add(new Callable<Void>() {

                @Override
                public Void call() throws Exception {
                    for (int j = 0; j < iterations; j++) {
                        threadTask.call();
                    }
                    return null;
                }

            });
        }
        List<Future<Void>> result = es.invokeAll(tasks);
        for (Future<Void> future : result) {
            future.get();
        }
        assertEquals(0, bm.getActiveBatchBytes());
        assertEquals(0, bm.getMemoryCacheEntries());
    }

    private void process(RelationalNode node, int expectedRows)
    throws TeiidComponentException, TeiidProcessingException {
        node.open();

        int currentRow = 1;
        while(true) {
            try {
                TupleBatch batch = node.nextBatch();
                currentRow += batch.getRowCount();
                if(batch.getTerminationFlag()) {
                    break;
                }
            } catch (BlockedException e) {

            }
        }
        assertEquals(expectedRows, currentRow - 1);
        node.close();
    }

    public void helpTestSort(final BufferManager bufferManager, final int rowCount, final int iterations, int threadCount, final Mode mode) throws Exception {
        final List<?>[] data = sampleData(rowCount);

        ElementSymbol elem1 = new ElementSymbol("e1");
        elem1.setType(DataTypeManager.DefaultDataClasses.INTEGER);
        ElementSymbol elem2 = new ElementSymbol("e2");
        elem2.setType(DataTypeManager.DefaultDataClasses.STRING);

        final List<ElementSymbol> sortElements = Arrays.asList(elem1);
        final List<ElementSymbol> elems = Arrays.asList(elem1, elem2);
        final Task task = new Task() {
            @Override
            public Void call() throws Exception {
                helpTestSort(mode, rowCount, sortElements, data, elems, bufferManager);
                return null;
            }
        };
        runTask(iterations, threadCount, task);
    }

    static List<?>[] sampleData(final int rowCount) {
        final List<?>[] data = new List<?>[rowCount];

        for (int i = 0; i < rowCount; i++) {
            data[i] = Arrays.asList(i, String.valueOf(i));
        }
        Collections.shuffle(Arrays.asList(data), r);
        return data;
    }

    public void helpTestSort(Mode mode, int expectedRowCount, List<? extends Expression> sortElements, List<?>[] data, List<? extends Expression> elems, BufferManager bufferManager) throws TeiidComponentException, TeiidProcessingException {
        CommandContext context = new CommandContext ("pid", "test", null, null, 1);               //$NON-NLS-1$ //$NON-NLS-2$

        BlockingFakeRelationalNode dataNode = new BlockingFakeRelationalNode(0, data);
        dataNode.setReturnPeriod(3);
        dataNode.setElements(elems);
        dataNode.initialize(context, bufferManager, null);

        SortNode sortNode = new SortNode(1);
        sortNode.setSortElements(new OrderBy(sortElements).getOrderByItems());
        sortNode.setMode(mode);
        sortNode.setElements(dataNode.getElements());
        sortNode.addChild(dataNode);
        sortNode.initialize(context, bufferManager, null);

        process(sortNode, expectedRowCount);
    }

    public void helpTestEquiJoin(int expectedRowCount, List<?>[] leftData, List<?>[] rightData, List<? extends Expression> elems, BufferManager bufferManager, JoinStrategy joinStrategy, JoinType joinType) throws TeiidComponentException, TeiidProcessingException {
        CommandContext context = new CommandContext ("pid", "test", null, null, 1);               //$NON-NLS-1$ //$NON-NLS-2$

        FakeRelationalNode dataNode1 = new FakeRelationalNode(1, leftData);
        dataNode1.setElements(elems);
        dataNode1.initialize(context, bufferManager, null);

        FakeRelationalNode dataNode2 = new FakeRelationalNode(2, rightData);
        dataNode2.setElements(elems);
        dataNode2.initialize(context, bufferManager, null);

        JoinNode join = new JoinNode(3);
        join.addChild(dataNode1);
        join.addChild(dataNode2);
        join.setJoinStrategy(joinStrategy.clone());
        join.setElements(elems);
        join.setJoinType(joinType);
        join.setJoinExpressions(elems.subList(0, 1), elems.subList(0, 1));
        join.initialize(context, bufferManager, null);

        process(join, expectedRowCount);
    }

    public void helpTestEquiJoin(final BufferManager bufferManager, int leftRowCount, int rightRowCount, final int iterations, int threadCount, final JoinStrategy joinStrategy, final JoinType joinType, final int expectedRowCount) throws Exception {
        final List<?>[] leftData = sampleData(leftRowCount);
        final List<?>[] rightData = sampleData(rightRowCount);

        ElementSymbol elem1 = new ElementSymbol("e1");
        elem1.setType(DataTypeManager.DefaultDataClasses.INTEGER);
        ElementSymbol elem2 = new ElementSymbol("e2");
        elem2.setType(DataTypeManager.DefaultDataClasses.STRING);

        final List<ElementSymbol> elems = Arrays.asList(elem1, elem2);
        final Task task = new Task() {
            @Override
            public Void call() throws Exception {
                helpTestEquiJoin(expectedRowCount, leftData, rightData, elems, bufferManager, joinStrategy, joinType);
                return null;
            }
        };
        runTask(iterations, threadCount, task);
    }

    @BeforeClass public static void oneTimeSetup() throws TeiidComponentException {
        bm = new BufferManagerImpl();

        bm.setMaxProcessingKB(1<<12);
        bm.setMaxReserveKB((1<<18)-(1<<16));
        bm.setMaxActivePlans(20);

        cache = new BufferFrontedFileStoreCache();
        cache.setMemoryBufferSpace(1<<26);
        FileStorageManager fsm = new FileStorageManager();
        fsm.setStorageDirectory(UnitTestUtil.getTestScratchPath() + "/data");
        cache.setStorageManager(fsm);
        cache.initialize();
        bm.setCache(cache);
        bm.initialize();
        bm.setMaxBatchManagerSizeEstimate(Long.MAX_VALUE);

        es = Executors.newCachedThreadPool();
    }

    @After public void tearDown() throws Exception {
        if (debug) {
            showStats();
        }
    }

    private void helpTestXMLTable(int iterations, int threadCount, String file, int expectedRowCount) throws QueryParserException,
        TeiidException, InterruptedException, Exception {
        String sql = "select * from xmltable('/root/child' passing xmlparse(document cast(? as clob) wellformed) columns x integer path '@id', y long path 'gc2') as x"; //$NON-NLS-1$
        List<?> preparedValues = Arrays.asList(TestTextTable.clobFromFile(file));
        Command command = QueryParser.getQueryParser().parseCommand(sql);
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        CapabilitiesFinder capFinder = new DefaultCapabilitiesFinder();

        ProcessorPlan plan = helpGetPlan(command, metadata, capFinder, createCommandContext());

        runTask(iterations, threadCount, new PreparedPlanTask(preparedValues, metadata, plan, command, expectedRowCount));
    }

    private void processPreparedPlan(List<?> values, Command command,
            QueryMetadataInterface metadata, ProcessorDataManager dataManager,
            ProcessorPlan plan, int rowCount) throws Exception {
        CommandContext context = createCommandContext();
        context.setMetadata(metadata);
        context.setExecutor(es);
        context.setBufferManager(bm);
        setParameterValues(values, command, context);
        plan.reset();
        assertEquals(rowCount, doProcess(plan, dataManager, null, context));
    }

    @Test public void runSort_1_100() throws Exception {
        helpTestSort(bm, 100, 20000, 1, Mode.SORT);
    }

    @Test public void runSort_4_5000() throws Exception {
        helpTestSort(bm, 5000, 1000, 4, Mode.SORT);
    }

    @Test public void runSort_16_250000() throws Exception {
        helpTestSort(bm, 250000, 10, 16, Mode.SORT);
    }

    @Test public void runSort_1_500000() throws Exception {
        helpTestSort(bm, 500000, 10, 1, Mode.SORT);
    }

    @Test public void runDupRemove_1_100() throws Exception {
        helpTestSort(bm, 100, 20000, 1, Mode.DUP_REMOVE);
    }

    @Test public void runDupRemove_4_5000() throws Exception {
        helpTestSort(bm, 5000, 1000, 4, Mode.DUP_REMOVE);
    }

    @Test public void runDupRemove_16_250000() throws Exception {
        helpTestSort(bm, 250000, 10, 16, Mode.DUP_REMOVE);
    }

    @Test public void runInnerEnhancedJoin_1_100_500() throws Exception {
        helpTestEquiJoin(bm, 100, 500, 10000, 1, new EnhancedSortMergeJoinStrategy(SortOption.SORT, SortOption.SORT), JoinType.JOIN_INNER, 100);
    }

    @Test public void runInnerEnhancedJoin_4_200_15000() throws Exception {
        helpTestEquiJoin(bm, 200, 15000, 500, 4, new EnhancedSortMergeJoinStrategy(SortOption.SORT, SortOption.SORT), JoinType.JOIN_INNER, 200);
    }

    @Test public void runInnerEnhancedJoin_16_400_500000() throws Exception {
        helpTestEquiJoin(bm, 400, 500000, 10, 16, new EnhancedSortMergeJoinStrategy(SortOption.SORT, SortOption.SORT), JoinType.JOIN_INNER, 400);
    }

    @Test public void runInnerMergeJoin_1_100_100() throws Exception {
        helpTestEquiJoin(bm, 100, 100, 10000, 1, new MergeJoinStrategy(SortOption.SORT, SortOption.SORT, false), JoinType.JOIN_INNER, 100);
    }

    @Test public void runOuterMergeJoin_1_1000_1000() throws Exception {
        helpTestEquiJoin(bm, 1000, 1000, 10000, 1, new MergeJoinStrategy(SortOption.SORT, SortOption.SORT, false), JoinType.JOIN_FULL_OUTER, 1000);
    }

    @Test public void runInnerMergeJoin_4_4000_4000() throws Exception {
        helpTestEquiJoin(bm, 4000, 4000, 500, 4, new MergeJoinStrategy(SortOption.SORT, SortOption.SORT, false), JoinType.JOIN_INNER, 4000);
    }

    @Test public void runInnerMergeJoin_16_100000_100000() throws Exception {
        helpTestEquiJoin(bm, 100000, 100000, 10, 16, new MergeJoinStrategy(SortOption.SORT, SortOption.SORT, false), JoinType.JOIN_INNER, 100000);
    }

    @Test public void runXMLTable_1_5mb() throws Exception {
        helpTestXMLTable(25, 1, "test.xml", 50000);
    }

    @Test public void runXMLTable_4_5mb() throws Exception {
        helpTestXMLTable(10, 4, "test.xml", 50000);
    }

    @Test public void runXMLTable_16_5mb() throws Exception {
        helpTestXMLTable(4, 16, "test.xml", 50000);
    }

    @Test public void runLike_1() throws Exception {
        helpTestLike(200000, 1);
    }

    @Test public void runLike_4() throws Exception {
        helpTestLike(100000, 4);
    }

    @Test public void runLike_16() throws Exception {
        helpTestLike(50000, 16);
    }

    @Test public void runBatchSerialization_String() throws Exception {
        String[] types = new String[] {DataTypeManager.DefaultDataTypes.STRING};
        int size = 1024;

        final List<List<?>> batch = new ArrayList<List<?>>();
        for (int i = 0; i < size; i++) {
            batch.add(Arrays.asList(String.valueOf(i)));
        }
        helpTestBatchSerialization(types, batch, 50000, 2);
    }

    @Test public void runBatchSerialization_StringRepeated() throws Exception {
        String[] types = new String[] {DataTypeManager.DefaultDataTypes.STRING};
        int size = 1024;

        final List<List<?>> batch = new ArrayList<List<?>>();
        for (int i = 0; i < size; i++) {
            batch.add(Arrays.asList("aaaaaaaa"));
        }
        helpTestBatchSerialization(types, batch, 50000, 2);
    }

    @Test public void runBatchSerialization_Time() throws Exception {
        final String[] types = new String[] {DataTypeManager.DefaultDataTypes.TIME};
        int size = 1024;

        final List<List<?>> batch = new ArrayList<List<?>>();
        for (int i = 0; i < size; i++) {
            batch.add(Arrays.asList(new Time(i)));
        }
        helpTestBatchSerialization(types, batch, 50000, 2);
    }

    @Test public void runBatchSerialization_Date() throws Exception {
        final String[] types = new String[] {DataTypeManager.DefaultDataTypes.DATE};
        int size = 1024;

        final List<List<?>> batch = new ArrayList<List<?>>();
        for (int i = 0; i < size; i++) {
            batch.add(Arrays.asList(new Date(i)));
        }
        helpTestBatchSerialization(types, batch, 50000, 2);
    }

    private void helpTestBatchSerialization(final String[] types,
            final List<List<?>> batch, int iterations, int threadCount)
            throws InterruptedException, Exception {
        runTask(iterations, threadCount, new Task() {

            @Override
            public Void call() throws Exception {
                writeReadBatch(types, batch);
                return null;
            }
        });
    }

    private List<List<Object>> writeReadBatch(String[] types, List<List<?>> batch)
            throws IOException, ClassNotFoundException {
        AccessibleByteArrayOutputStream baos = new AccessibleByteArrayOutputStream(5000);
        ObjectOutputStream out = new ObjectOutputStream(baos);
        BatchSerializer.writeBatch(out, types, batch);
        out.flush();

        byte[] bytes = baos.getBuffer();

        ByteArrayInputStream bytesIn = new ByteArrayInputStream(bytes, 0, baos.getCount());
        ObjectInputStream in = new ObjectInputStream(bytesIn);
        List<List<Object>> newBatch = BatchSerializer.readBatch(in, types);
        out.close();
        in.close();
        assertEquals(batch.size(), newBatch.size());
        return newBatch;
    }

    private void helpTestLike(int iterations, int threads) throws QueryParserException,
            InterruptedException, Exception {
        final Expression ex = QueryParser.getQueryParser().parseExpression("'abcdefg' like 'a%g'");
        runTask(iterations, threads, new Task() {
            @Override
            public Void call() throws Exception {
                Evaluator.evaluate(ex);
                return null;
            }
        });
    }

    private void helpTestLargeSort(int iterations, int threads, final int rows) throws InterruptedException, Exception {
        final List<ElementSymbol> elems = new ArrayList<ElementSymbol>();
        final int cols = 50;
        for (int i = 0; i < cols; i++) {
            ElementSymbol elem1 = new ElementSymbol("e" + i);
            elem1.setType(DataTypeManager.DefaultDataClasses.STRING);
            elems.add(elem1);
        }

        final List<ElementSymbol> sortElements = Arrays.asList(elems.get(0));

        final Task task = new Task() {
            @Override
            public Void call() throws Exception {
                CommandContext context = new CommandContext ("pid", "test", null, null, 1);               //$NON-NLS-1$ //$NON-NLS-2$
                SortNode sortNode = new SortNode(1);
                sortNode.setSortElements(new OrderBy(sortElements).getOrderByItems());
                sortNode.setMode(Mode.SORT);
                sortNode.setElements(elems);
                RelationalNode rn = new RelationalNode(2) {
                    int blockingPeriod = 3;
                    int count = 0;
                    int batches = 0;

                    @Override
                    protected TupleBatch nextBatchDirect() throws BlockedException,
                            TeiidComponentException, TeiidProcessingException {
                        if (count++%blockingPeriod==0) {
                            throw BlockedException.INSTANCE;
                        }
                        int batchSize = this.getBatchSize();
                        int batchRows = batchSize;
                        boolean done = false;
                        int start = batches++ * batchSize;
                        if (start + batchSize >= rows) {
                            done = true;
                            batchRows = rows - start;
                        }
                        ArrayList<List<?>> batch = new ArrayList<List<?>>(batchRows);
                        for (int i = 0; i < batchRows; i++) {
                            ArrayList<Object> row = new ArrayList<Object>();
                            for (int j = 0; j < cols; j++) {
                                if (j == 0) {
                                    row.add(String.valueOf((i * 279470273) % 4294967291L));
                                } else {
                                    row.add(i + "abcdefghijklmnop" + j);
                                }
                            }
                            batch.add(row);
                        }
                        TupleBatch result = new TupleBatch(start+1, batch);
                        if (done) {
                            result.setTerminationFlag(true);
                        }
                        return result;
                    }

                    @Override
                    public Object clone() {
                        return null;
                    }
                };
                rn.setElements(elems);
                sortNode.addChild(rn);
                sortNode.initialize(context, bm, null);
                rn.initialize(context, bm, null);
                process(sortNode, rows);
                return null;
            }
        };
        runTask(iterations, threads, task);
    }

    @Test public void runWideSort_1_100000() throws Exception {
        helpTestLargeSort(4, 1, 100000);
    }

    //tests a sort where the desired space is above 2 GB
    @Test public void runWideSort_1_500000() throws Exception {
        helpTestLargeSort(1, 1, 500000);
    }

    @Test public void runWideSort_4_100000() throws Exception {
        helpTestLargeSort(2, 4, 100000);
    }

    @Test public void largeRandomTable() throws Exception {
        assertEquals(0, bm.getActiveBatchBytes());
        ElementSymbol e1 = new ElementSymbol("x");
        e1.setType(Long.class);
        ElementSymbol e2 = new ElementSymbol("y");
        e2.setType(String.class);
        List<ElementSymbol> elements = Arrays.asList(e1, e2);
        STree map = bm.createSTree(elements, "1", 1);

        r.setSeed(0);

        int rows = 3000000;
        for (int i = 0; i < rows; i++) {
            assertNull(String.valueOf(i), map.insert(Arrays.asList(r.nextLong(), String.valueOf(i)), InsertMode.NEW, -1));
        }

        assertEquals(rows, map.getRowCount());
        r.setSeed(0);

        for (int i = 0; i < rows; i++) {
            assertNotNull(map.remove(Arrays.asList(r.nextLong())));
        }

        assertEquals(0, map.getRowCount());

        assertEquals(0, bm.getActiveBatchBytes());
    }

    private static void showStats() {
        System.out.println(bm.getBatchesAdded());
        System.out.println(bm.getReferenceHits());
        System.out.println(bm.getReadAttempts());
        System.out.println(bm.getReadCount());
        System.out.println(bm.getWriteCount());
        System.out.println(cache.getStorageReads());
        System.out.println(cache.getStorageWrites());
        System.out.println(cache.getMemoryInUseBytes());
    }

    /**
     * Generates a 5 MB document
     */
    public static void main(String[] args) throws Exception {
        FileOutputStream fos = new FileOutputStream(UnitTestUtil.getTestDataFile("test.xml"));
        XMLOutputFactory xof = XMLOutputFactory.newInstance();
        XMLStreamWriter xsw = xof.createXMLStreamWriter(fos);
        xsw.writeStartDocument();
        xsw.writeStartElement("root");
        for (int i = 0; i < 50000; i++) {
            xsw.writeStartElement("child");
            xsw.writeAttribute("id", String.valueOf(i));
            xsw.writeStartElement("gc1");
            xsw.writeCharacters(String.valueOf(r.nextLong()));
            xsw.writeEndElement();
            xsw.writeStartElement("gc2");
            xsw.writeCharacters(String.valueOf(r.nextLong()));
            xsw.writeEndElement();
            xsw.writeStartElement("gc3");
            xsw.writeCharacters(String.valueOf(r.nextLong()));
            xsw.writeEndElement();
            xsw.writeEndElement();
        }
        xsw.writeEndElement();
        xsw.writeEndDocument();
        xsw.close();
        fos.close();
    }

}
