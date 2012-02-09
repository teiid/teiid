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

package org.teiid.query.eval;

import static org.junit.Assert.*;
import static org.teiid.query.processor.TestProcessor.*;

import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;

import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.api.exception.query.QueryParserException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.impl.BufferFrontedFileStoreCache;
import org.teiid.common.buffer.impl.BufferManagerImpl;
import org.teiid.common.buffer.impl.FileStorageManager;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.DataTypeManager;
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
import org.teiid.query.processor.relational.RelationalNode;
import org.teiid.query.processor.relational.SortNode;
import org.teiid.query.processor.relational.MergeJoinStrategy.SortOption;
import org.teiid.query.processor.relational.SortUtility.Mode;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.SingleElementSymbol;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.util.CommandContext;

@SuppressWarnings("nls")
public class TestEnginePerformance {
	
	private static BufferManagerImpl bm;
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
		es.invokeAll(tasks);
		for (Callable<Void> callable : tasks) {
			callable.call();
		}
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

	private List<?>[] sampleData(final int rowCount) {
		final List<?>[] data = new List<?>[rowCount];

		for (int i = 0; i < rowCount; i++) {
			data[i] = Arrays.asList(i, String.valueOf(i));
		}
		Collections.shuffle(Arrays.asList(data), r);
		return data;
	}
	
	public void helpTestSort(Mode mode, int expectedRowCount, List<? extends SingleElementSymbol> sortElements, List<?>[] data, List<? extends SingleElementSymbol> elems, BufferManager bufferManager) throws TeiidComponentException, TeiidProcessingException {
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
	
	public void helpTestEquiJoin(int expectedRowCount, List<?>[] leftData, List<?>[] rightData, List<? extends SingleElementSymbol> elems, BufferManager bufferManager, JoinStrategy joinStrategy, JoinType joinType) throws TeiidComponentException, TeiidProcessingException {
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
		bm.setMaxReserveKB((1<<19)-(1<<17));
		bm.setMaxActivePlans(20);
		
		BufferFrontedFileStoreCache cache = new BufferFrontedFileStoreCache();
		cache.setMemoryBufferSpace(1<<27);
		FileStorageManager fsm = new FileStorageManager();
		fsm.setStorageDirectory(UnitTestUtil.getTestScratchPath() + "/data");
		cache.setStorageManager(fsm);
		cache.initialize();
		bm.setCache(cache);
		bm.initialize();
		
		es = Executors.newCachedThreadPool();
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
	
	/**
	 * Generates a 5 MB document
	 */
	public static void main(String[] args) throws Exception {
		FileOutputStream fos = new FileOutputStream(UnitTestUtil.getTestDataFile("test.xml"));
		XMLOutputFactory xof = XMLOutputFactory.newFactory();
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
