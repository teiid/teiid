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

package org.teiid.query.processor;

import static org.teiid.query.optimizer.TestOptimizer.*;
import static org.teiid.query.processor.TestProcessor.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.optimizer.TestOptimizer.ComparisonMode;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings({"nls", "unchecked"})
public class TestWindowFunctions {

    @Test public void testViewNotRemoved() throws Exception {
    	BasicSourceCapabilities caps = getTypicalCapabilities();
    	caps.setCapabilitySupport(Capability.ELEMENTARY_OLAP, true);
    	caps.setCapabilitySupport(Capability.QUERY_FROM_INLINE_VIEWS, true);
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT y FROM (select row_number() over (order by e1) as y from pm1.g1) as x where x.y = 10", //$NON-NLS-1$
                                      RealMetadataFactory.example1Cached(), null, new DefaultCapabilitiesFinder(caps),
                                      new String[] {
                                          "SELECT v_0.c_0 FROM (SELECT ROW_NUMBER() OVER (ORDER BY g_0.e1) AS c_0 FROM pm1.g1 AS g_0) AS v_0 WHERE v_0.c_0 = 10"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    
        checkNodeTypes(plan, FULL_PUSHDOWN);                                    
    }
    
    @Test public void testWindowFunctionPushdown() throws Exception {
    	BasicSourceCapabilities caps = getTypicalCapabilities();
    	caps.setCapabilitySupport(Capability.ELEMENTARY_OLAP, true);
    	caps.setCapabilitySupport(Capability.WINDOW_FUNCTION_ORDER_BY_AGGREGATES, true);
    	caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        ProcessorPlan plan = TestOptimizer.helpPlan("select max(e1) over (order by e1) as y from pm1.g1", //$NON-NLS-1$
                                      RealMetadataFactory.example1Cached(), null, new DefaultCapabilitiesFinder(caps),
                                      new String[] {
                                          "SELECT MAX(g_0.e1) OVER (ORDER BY g_0.e1) FROM pm1.g1 AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    
        checkNodeTypes(plan, FULL_PUSHDOWN);                                    
    }

    @Test public void testWindowFunctionPushdown1() throws Exception {
    	BasicSourceCapabilities caps = getTypicalCapabilities();
    	caps.setCapabilitySupport(Capability.ELEMENTARY_OLAP, true);
    	caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        ProcessorPlan plan = TestOptimizer.helpPlan("select max(e1) over (order by e1) as y from pm1.g1", //$NON-NLS-1$
                                      RealMetadataFactory.example1Cached(), null, new DefaultCapabilitiesFinder(caps),
                                      new String[] {
                                          "SELECT g_0.e1 FROM pm1.g1 AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    
        checkNodeTypes(plan, new int[] {
                1,      // Access
                0,      // DependentAccess
                0,      // DependentSelect
                0,      // DependentProject
                0,      // DupRemove
                0,      // Grouping
                0,      // NestedLoopJoinStrategy
                0,      // MergeJoinStrategy
                0,      // Null
                0,      // PlanExecution
                1,      // Project
                0,      // Select
                0,      // Sort
                0       // UnionAll
            });                                     
    }
    
	@Test public void testRanking() throws Exception {
    	String sql = "select e1, row_number() over (order by e1), rank() over (order by e1), dense_rank() over (order by e1 nulls last) from pm1.g1";
        
    	List<?>[] expected = new List[] {
        		Arrays.asList("a", 2, 2, 1),
        		Arrays.asList(null, 1, 1, 4),
        		Arrays.asList("a", 3, 2, 1),
        		Arrays.asList("c", 6, 6, 3),
        		Arrays.asList("b", 5, 5, 2),
        		Arrays.asList("a", 4, 2, 1),
        };
    	
    	FakeDataManager dataManager = new FakeDataManager();
    	sampleData1(dataManager);
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached(), TestOptimizer.getGenericFinder());
        
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testPartitionedMax() throws Exception {
    	String sql = "select e2, max(e1) over (partition by e2) as y from pm1.g1";
        
    	List<?>[] expected = new List[] {
        		Arrays.asList(0, "a"),
        		Arrays.asList(1, "c"),
        		Arrays.asList(3, "a"),
        		Arrays.asList(1, "c"),
        		Arrays.asList(2, "b"),
        		Arrays.asList(0, "a"),
        };
    	
    	FakeDataManager dataManager = new FakeDataManager();
    	sampleData1(dataManager);
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached(), TestOptimizer.getGenericFinder());
        
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testUnrelatedWindowFunctionOrderBy() throws Exception {
    	String sql = "select e2, e1 from pm1.g1 order by count(e1) over (partition by e3), e2";
        
    	List<?>[] expected = new List[] {
    			Arrays.asList(1, "c"),
        		Arrays.asList(3, "a"),
        		Arrays.asList(0, "a"),
        		Arrays.asList(0, "a"),
        		Arrays.asList(1, null),
        		Arrays.asList(2, "b"),
        };
    	
    	FakeDataManager dataManager = new FakeDataManager();
    	sampleData1(dataManager);
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached(), TestOptimizer.getGenericFinder());
        
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testWindowFunctionOrderBy() throws Exception {
    	String sql = "select e2, e1, count(e1) over (partition by e3) as c from pm1.g1 order by c, e2";
        
    	List<?>[] expected = new List[] {
        		Arrays.asList(1, "c", 2),
        		Arrays.asList(3, "a", 2),
        		Arrays.asList(0, "a", 3),
        		Arrays.asList(0, "a", 3),
        		Arrays.asList(1, null, 3),
        		Arrays.asList(2, "b", 3),
        };
    	
    	FakeDataManager dataManager = new FakeDataManager();
    	sampleData1(dataManager);
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached(), TestOptimizer.getGenericFinder());
        
        helpProcess(plan, dataManager, expected);
    }

    /**
     * Note that we've optimized the ordering to be performed prior to the windowing.
     * If we change the windowing logic to not preserve the incoming row ordering, then this optimization will need to change
     * @throws Exception
     */
    @Test public void testCountDuplicates() throws Exception {
    	String sql = "select e1, count(e1) over (order by e1) as c from pm1.g1 order by e1";
        
    	List<?>[] expected = new List[] {
        		Arrays.asList("a", 2),
        		Arrays.asList("a", 2),
        		Arrays.asList("b", 3),
        };
    	
    	HardcodedDataManager dataManager = new HardcodedDataManager();
    	dataManager.addData("SELECT g_0.e1 AS c_0 FROM pm1.g1 AS g_0 ORDER BY c_0", new List[] {Arrays.asList("a"), Arrays.asList("a"), Arrays.asList("b")});
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached(), TestOptimizer.getGenericFinder());
        
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testEmptyOver() throws Exception {
    	String sql = "select e1, max(e1) over () as c from pm1.g1";
        
    	List<?>[] expected = new List[] {
        		Arrays.asList("a", "c"),
        		Arrays.asList(null, "c"),
        		Arrays.asList("a", "c"),
        		Arrays.asList("c", "c"),
        		Arrays.asList("b", "c"),
        		Arrays.asList("a", "c"),
        };
    	
    	FakeDataManager dataManager = new FakeDataManager();
    	sampleData1(dataManager);
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached(), TestOptimizer.getGenericFinder());
        
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testRowNumberMedian() throws Exception {
    	String sql = "select e1, r, c from (select e1, row_number() over (order by e1) as r, count(*) over () c from pm1.g1) x where r = ceiling(c/2)";
        
    	List<?>[] expected = new List[] {
        		Arrays.asList("a", 3, 6),
        };
    	
    	FakeDataManager dataManager = new FakeDataManager();
    	sampleData1(dataManager);
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached(), TestOptimizer.getGenericFinder());
        
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testPartitionedRowNumber() throws Exception {
    	String sql = "select e1, e3, row_number() over (partition by e3 order by e1) as r from pm1.g1 order by r limit 2";
        
    	List<?>[] expected = new List[] {
        		Arrays.asList(null, Boolean.FALSE, 1),
        		Arrays.asList("a", Boolean.TRUE, 1),
        };
    	
    	FakeDataManager dataManager = new FakeDataManager();
    	sampleData1(dataManager);
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached(), TestOptimizer.getGenericFinder());
        
        helpProcess(plan, dataManager, expected);
    }
    
}
