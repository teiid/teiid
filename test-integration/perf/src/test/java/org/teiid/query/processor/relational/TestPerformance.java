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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.impl.BufferFrontedFileStoreCache;
import org.teiid.common.buffer.impl.BufferManagerImpl;
import org.teiid.common.buffer.impl.FileStorageManager;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.query.processor.relational.MergeJoinStrategy.SortOption;
import org.teiid.query.processor.relational.SortUtility.Mode;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.SingleElementSymbol;
import org.teiid.query.util.CommandContext;

@SuppressWarnings("nls")
public class TestPerformance {
	
	private static BufferManagerImpl bm;
	private static ExecutorService es;
	private static Random r = new Random(0);
	
	private void runTask(final int iterations, int threadCount,
			final Callable<Void> task) throws InterruptedException, Exception {
		List<Callable<Void>> tasks = new ArrayList<Callable<Void>>(threadCount);
		for (int i = 0; i < threadCount; i++) {
			tasks.add(new Callable<Void>() {

				@Override
				public Void call() throws Exception {
					for (int j = 0; j < iterations; j++) {
						task.call();
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
	
	public void helpTestSort(final BufferManager bufferManager, final int rowCount, final int iterations, int threadCount) throws Exception {
		final List<?>[] data = sampleData(rowCount);
		
		ElementSymbol elem1 = new ElementSymbol("e1");
		elem1.setType(DataTypeManager.DefaultDataClasses.INTEGER);
		ElementSymbol elem2 = new ElementSymbol("e2");
		elem2.setType(DataTypeManager.DefaultDataClasses.STRING);

		final List<ElementSymbol> sortElements = Arrays.asList(elem1);
		final List<ElementSymbol> elems = Arrays.asList(elem1, elem2);
		final Callable<Void> task = new Callable<Void>() {
			@Override
			public Void call() throws Exception {
				helpTestSort(Mode.SORT, rowCount, sortElements, data, elems, bufferManager);
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
		
		FakeRelationalNode dataNode = new FakeRelationalNode(0, data);
		dataNode.setElements(elems);
		dataNode.initialize(context, bufferManager, null);
		
		SortNode sortNode = new SortNode(1);
    	sortNode.setSortElements(new OrderBy(sortElements).getOrderByItems());
        sortNode.setMode(mode);
		sortNode.setElements(dataNode.getElements());
        sortNode.addChild(dataNode);        
		sortNode.initialize(context, dataNode.getBufferManager(), null);    
        
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
		final Callable<Void> task = new Callable<Void>() {
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
	
	@Test public void runSort_1_100() throws Exception {
		helpTestSort(bm, 100, 20000, 1);
	}
	
	@Test public void runSort_4_5000() throws Exception {
		helpTestSort(bm, 5000, 1000, 4);
	}

	@Test public void runSort_16_250000() throws Exception {
		helpTestSort(bm, 250000, 10, 16);
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
	
}
