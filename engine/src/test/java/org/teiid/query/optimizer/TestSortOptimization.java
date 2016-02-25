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

package org.teiid.query.optimizer;

import static org.junit.Assert.*;
import static org.teiid.query.optimizer.TestOptimizer.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.TestOptimizer.ComparisonMode;
import org.teiid.query.optimizer.TestOptimizer.DupRemoveSortNode;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.FakeCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.processor.HardcodedDataManager;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.TestProcessor;
import org.teiid.query.processor.relational.LimitNode;
import org.teiid.query.processor.relational.ProjectNode;
import org.teiid.query.processor.relational.RelationalPlan;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.translator.SourceSystemFunctions;

@SuppressWarnings("nls")
public class TestSortOptimization {

    @Test public void testSortDupCombination() { 
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        // Create query 
        String sql = "select distinct e1, e2 from pm1.g1 order by e2"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql, RealMetadataFactory.example1Cached(), null, capFinder, 
                                      new String[] {"SELECT e1, e2 FROM pm1.g1"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$
        
        checkNodeTypes(plan, FULL_PUSHDOWN); 
        checkNodeTypes(plan, new int[] {1}, new Class[] {DupRemoveSortNode.class});
    }
    
    @Test public void testSortDupCombination1() { 
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        // Create query 
        String sql = "select e1, e2 from pm1.g1 union select e1, e2 from pm1.g2 order by e2"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql, RealMetadataFactory.example1Cached(), null, capFinder, 
                                      new String[] {"SELECT e1, e2 FROM pm1.g1", "SELECT e1, e2 FROM pm1.g2"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$ //$NON-NLS-2$
        
        checkNodeTypes(plan, new int[] {
                2,      // Access
                0,      // DependentAccess
                0,      // DependentSelect
                0,      // DependentProject
                0,      // DupRemove
                0,      // Grouping
                0,      // NestedLoopJoinStrategy
                0,      // MergeJoinStrategy
                0,      // Null
                0,      // PlanExecution
                0,      // Project
                0,      // Select
                0,      // Sort
                1       // UnionAll
            });
        checkNodeTypes(plan, new int[] {1}, new Class[] {DupRemoveSortNode.class});
    }
	
    @Test public void testSortDupCombination2() { 
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        // Create query 
        String sql = "select x.*, y.* from (select distinct e1, e2 from pm1.g1) x, (select distinct e1, e2 from pm1.g2) y where x.e1 = y.e1"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql, RealMetadataFactory.example1Cached(), null, capFinder, 
                                      new String[] {"SELECT e1, e2 FROM pm1.g1", "SELECT e1, e2 FROM pm1.g2"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$ //$NON-NLS-2$
        
        checkNodeTypes(plan, new int[] {
                2,      // Access
                0,      // DependentAccess
                0,      // DependentSelect
                0,      // DependentProject
                0,      // DupRemove
                0,      // Grouping
                0,      // NestedLoopJoinStrategy
                1,      // MergeJoinStrategy
                0,      // Null
                0,      // PlanExecution
                1,      // Project
                0,      // Select
                0,      // Sort
                0       // UnionAll
            });
        checkNodeTypes(plan, new int[] {0}, new Class[] {DupRemoveSortNode.class});
    }
    
    @Test public void testGroupDupCombination() { 
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        // Create query 
        String sql = "select max(e1), e2 from (select distinct e1, e2 from pm1.g1) x group by e2"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql, RealMetadataFactory.example1Cached(), null, capFinder, 
                                      new String[] {"SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$ 
        
        checkNodeTypes(plan, new int[] {
                1,      // Access
                0,      // DependentAccess
                0,      // DependentSelect
                0,      // DependentProject
                0,      // DupRemove
                1,      // Grouping
                0,      // NestedLoopJoinStrategy
                0,      // MergeJoinStrategy
                0,      // Null
                0,      // PlanExecution
                1,      // Project
                0,      // Select
                0,      // Sort
                0       // UnionAll
            });
        checkNodeTypes(plan, new int[] {0}, new Class[] {DupRemoveSortNode.class});
    }

    /**
     * The grouping expression inhibits combining the dup removal.
     */
    @Test public void testGroupDupCombination1() { 
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        // Create query 
        String sql = "select max(e1), e2 || e1 from (select distinct e1, e2 from pm1.g1) x group by e2 || e1"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql, RealMetadataFactory.example1Cached(), null, capFinder, 
                                      new String[] {"SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$ 
        
        checkNodeTypes(plan, new int[] {
                1,      // Access
                0,      // DependentAccess
                0,      // DependentSelect
                0,      // DependentProject
                0,      // DupRemove
                1,      // Grouping
                0,      // NestedLoopJoinStrategy
                0,      // MergeJoinStrategy
                0,      // Null
                0,      // PlanExecution
                1,      // Project
                0,      // Select
                0,      // Sort
                0       // UnionAll
            });
        checkNodeTypes(plan, new int[] {1}, new Class[] {DupRemoveSortNode.class});
    }

    @Test public void testSortGroupCombination() { 
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        // Create query 
        String sql = "select max(e1), e2 from pm1.g1 x group by e2 order by e2"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql, RealMetadataFactory.example1Cached(), null, capFinder, 
                                      new String[] {"SELECT pm1.g1.e2, pm1.g1.e1 FROM pm1.g1"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$ 
        
        checkNodeTypes(plan, new int[] {
                1,      // Access
                0,      // DependentAccess
                0,      // DependentSelect
                0,      // DependentProject
                0,      // DupRemove
                1,      // Grouping
                0,      // NestedLoopJoinStrategy
                0,      // MergeJoinStrategy
                0,      // Null
                0,      // PlanExecution
                1,      // Project
                0,      // Select
                0,      // Sort
                0       // UnionAll
            });
        checkNodeTypes(plan, new int[] {0}, new Class[] {DupRemoveSortNode.class});
    }
    
    @Test public void testProjectionRaisingWithLimit() { 
        // Create query 
        String sql = "select e1, (select e1 from pm2.g1 where e2 = x.e2) from pm1.g1 as x order by e1 limit 2"; //$NON-NLS-1$

        RelationalPlan plan = (RelationalPlan)helpPlan(sql, RealMetadataFactory.example1Cached(), null, new DefaultCapabilitiesFinder(), 
                                      new String[] {"SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$
        
        assertTrue(plan.getRootNode() instanceof ProjectNode);
    }
    
    @Test public void testProjectionRaisingWithLimit1() { 
        // Create query 
        String sql = "select (select e1 from pm2.g1 where e2 = x.e2) as z from pm1.g1 as x order by z limit 2"; //$NON-NLS-1$

        RelationalPlan plan = (RelationalPlan)helpPlan(sql, RealMetadataFactory.example1Cached(), null, new DefaultCapabilitiesFinder(), 
                                      new String[] {"SELECT pm1.g1.e2 FROM pm1.g1"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$
        
        assertTrue(plan.getRootNode() instanceof LimitNode);
    }
    
    @Test public void testProjectionRaisingWithAccess() throws Exception { 
        // Create query 
        String sql = "select e1, (select e1 from pm2.g1 where e2 = x.e2) as z from pm1.g1 as x order by e1"; //$NON-NLS-1$

        helpPlan(sql, RealMetadataFactory.example1Cached(), null, TestOptimizer.getGenericFinder(), 
                                      new String[] {"SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm1.g1 AS g_0 ORDER BY c_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    }
    
    @Test public void testProjectionRaisingWithAccess1() throws Exception { 
        // Create query 
        String sql = "select e1, 1 as z from pm1.g1 as x group by e1 order by e1"; //$NON-NLS-1$
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_GROUP_BY, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, false);
        RelationalPlan plan = (RelationalPlan)helpPlan(sql, RealMetadataFactory.example1Cached(), null, new DefaultCapabilitiesFinder(caps), 
                                      new String[] {"SELECT g_0.e1 FROM pm1.g1 AS g_0 GROUP BY g_0.e1 ORDER BY g_0.e1"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        
        assertTrue(plan.getRootNode() instanceof ProjectNode);
        
    }
    
    @Test public void testProjectionRaisingWithAccessAndLimit() throws Exception { 
        // Create query 
        String sql = "select e1, (select e1 from pm2.g1 where e2 = x.e2) as z from pm1.g1 as x order by e1 limit 1"; //$NON-NLS-1$

        helpPlan(sql, RealMetadataFactory.example1Cached(), null, TestOptimizer.getGenericFinder(), 
                                      new String[] {"SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm1.g1 AS g_0 ORDER BY c_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    }
    
    @Test public void testProjectionRaisingForUnrelatedWithLimit() throws Exception { 
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$
        
        String sql = "select (select e1 from pm2.g1 where e2 = x.e2) as z from pm1.g1 as x order by e1 limit 1"; //$NON-NLS-1$

        helpPlan(sql, RealMetadataFactory.example1Cached(), null, capFinder, 
                                      new String[] {"SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1 ORDER BY pm1.g1.e1 LIMIT 1"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    }
    
    @Test public void testProjectionRaisingForUnrelated() throws Exception { 
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$
        
        String sql = "select e2 from pm1.g1 as x order by e1"; //$NON-NLS-1$

        helpPlan(sql, RealMetadataFactory.example1Cached(), null, capFinder, 
                                      new String[] {"SELECT pm1.g1.e2, pm1.g1.e1 FROM pm1.g1 ORDER BY pm1.g1.e1"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    }
    
    @Test public void testProjectionRaisingWithAlias() throws Exception { 
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$
        
        String sql = "select (select e1 from pm2.g1 where e2 = x.e2) as z, x.e1 as foo from pm1.g1 as x order by foo limit 1"; //$NON-NLS-1$

        helpPlan(sql, RealMetadataFactory.example1Cached(), null, capFinder, 
                                      new String[] {"SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm1.g1 AS g_0 ORDER BY c_0 LIMIT 1"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    }
    
    @Test public void testProjectionRaisingWithComplexOrdering() { 
        String sql = "select e1 || 1, e2 / 2 from pm1.g1 as x order by e1 || 1 limit 2"; //$NON-NLS-1$
        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        bsc.setFunctionSupport(SourceSystemFunctions.CONCAT, true);
        bsc.setCapabilitySupport(Capability.ROW_LIMIT, true);
        CapabilitiesFinder finder = new DefaultCapabilitiesFinder(bsc);
        RelationalPlan plan = (RelationalPlan)helpPlan(sql, RealMetadataFactory.example1Cached(), null, finder, 
                                      new String[] {"SELECT concat(g_0.e1, '1') AS c_0, g_0.e2 AS c_1 FROM pm1.g1 AS g_0 ORDER BY c_0 LIMIT 2"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$
        
        assertTrue(plan.getRootNode() instanceof ProjectNode);
    }
    
    @Test public void testProjectionRaisingWithComplexOrdering1() { 
        String sql = "select e1 || 1 as a, e2 / 2 from pm1.g1 as x order by a, e2 limit 2"; //$NON-NLS-1$
        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        bsc.setFunctionSupport(SourceSystemFunctions.CONCAT, true);
        bsc.setCapabilitySupport(Capability.ROW_LIMIT, true);
        CapabilitiesFinder finder = new DefaultCapabilitiesFinder(bsc);
        RelationalPlan plan = (RelationalPlan)helpPlan(sql, RealMetadataFactory.example1Cached(), null, finder, 
                                      new String[] {"SELECT concat(g_0.e1, '1') AS c_0, g_0.e2 AS c_1 FROM pm1.g1 AS g_0 ORDER BY c_0, c_1 LIMIT 2"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$
        
        assertTrue(plan.getRootNode() instanceof ProjectNode);
        HardcodedDataManager hdm = new HardcodedDataManager();
        hdm.addData("SELECT concat(g_0.e1, '1') AS c_0, g_0.e2 AS c_1 FROM pm1.g1 AS g_0 ORDER BY c_0, c_1 LIMIT 2", Arrays.asList("c1", 2), Arrays.asList("d1", 3));
        TestProcessor.helpProcess(plan, hdm, new List<?>[] {Arrays.asList("c1", 1), Arrays.asList("d1", 1)});
    }
    
    //TODO this should trigger another view removal and thus the combination of the grouping/dup operation
    @Test public void testGroupDupCombination1Pushdown() throws TeiidComponentException, TeiidProcessingException { 
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SELECT_DISTINCT, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_INLINE_VIEWS, true);
        caps.setCapabilitySupport(Capability.QUERY_GROUP_BY, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$

        // Create query 
        String sql = "select e1, (select e1 from pm2.g1 where e2 = x.e2) as z from (select distinct e1, e2 from pm1.g1) as x group by e1, e2 order by e1"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql, RealMetadataFactory.example1Cached(), null, capFinder, 
                                      new String[] {"SELECT v_0.c_0, v_0.c_1 FROM (SELECT DISTINCT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm1.g1 AS g_0) AS v_0 GROUP BY v_0.c_0, v_0.c_1 ORDER BY c_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ 
        
        checkNodeTypes(plan, new int[] {
                1,      // Access
                0,      // DependentAccess
                0,      // DependentSelect
                1,      // DependentProject
                0,      // DupRemove
                0,      // Grouping
                0,      // NestedLoopJoinStrategy
                0,      // MergeJoinStrategy
                0,      // Null
                0,      // PlanExecution
                0,      // Project
                0,      // Select
                0,      // Sort
                0       // UnionAll
            });
        checkNodeTypes(plan, new int[] {0}, new Class[] {DupRemoveSortNode.class});
    }
    
    @Test public void testSortDupCombinationUnrelated() throws TeiidComponentException, TeiidProcessingException { 
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        // Create query 
        String sql = "select e1 from (select e1, e2 from pm1.g1 union select e1, e2 from pm1.g2) as x order by e2"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql, RealMetadataFactory.example1Cached(), null, capFinder, 
                                      new String[] {"SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1", "SELECT pm1.g2.e1, pm1.g2.e2 FROM pm1.g2"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        checkNodeTypes(plan, new int[] {
                2,      // Access
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
                1       // UnionAll
            });
        checkNodeTypes(plan, new int[] {1}, new Class[] {DupRemoveSortNode.class});
    }
    
    /**
     * TODO: we currently don't optimize this case as it requires pushing the sort onto the dup removal
     * which the logic isn't well suited to handle
     */
    @Test public void testSortDupCombinationUnrelated1() throws TeiidComponentException, TeiidProcessingException { 
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        // Create query 
        String sql = "select e1 || 'a' from (select e1, e2 from pm1.g1 union select e1, e2 from pm1.g2) as x order by e2"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql, RealMetadataFactory.example1Cached(), null, capFinder, 
                                      new String[] {"SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1", "SELECT pm1.g2.e1, pm1.g2.e2 FROM pm1.g2"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        checkNodeTypes(plan, new int[] {
                2,      // Access
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
                1,      // Sort
                1       // UnionAll
            });
        checkNodeTypes(plan, new int[] {1}, new Class[] {DupRemoveSortNode.class});
    }
    
    /**
     * Previously failing with a planning exception - needed to ensure that all non-constants are symbols in OrderByItem.
     */
    @Test public void testUnrelatedSortFunctionOverUnion() throws Exception { 
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        // Create query 
        String sql = "select e1 || 'a' from (select e1, e2 from pm1.g1 union select e1, e2 from pm1.g2) as x order by e2 || 'b'"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql, RealMetadataFactory.example1Cached(), null, capFinder, 
                                      new String[] {"SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1", "SELECT pm1.g2.e1, pm1.g2.e2 FROM pm1.g2"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        
        checkNodeTypes(plan, new int[] {
                2,      // Access
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
                1,      // Sort
                1       // UnionAll
            });
        checkNodeTypes(plan, new int[] {1}, new Class[] {DupRemoveSortNode.class});
    }
    
    @Test public void testUnionWithAggregation() throws Exception{
    	QueryMetadataInterface metadata = RealMetadataFactory.fromDDL("create foreign table items (item_id varchar)", "x", "y");

        String sql = "select FOO.SOURCE SOURCE, FOO.FOO_ID FOO_ID from ("
                + "(select 'X' SOURCE, ITEMS.ITEM_ID FOO_ID from ITEMS ITEMS group by ITEMS.ITEM_ID) "
                + "union all (select 'Y' SOURCE, ITEMS.ITEM_ID FOO_ID from ITEMS ITEMS) union all"
                + " (select 'Z' SOURCE, '123' FOO_ID) ) FOO order by FOO_ID desc limit 50";
        
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
        caps.setCapabilitySupport(Capability.QUERY_UNION, true);
        caps.setCapabilitySupport(Capability.QUERY_GROUP_BY, true);
        caps.setCapabilitySupport(Capability.QUERY_SET_ORDER_BY, true);
        
        helpPlan(sql, metadata, null, new DefaultCapabilitiesFinder(caps), 
                new String[] {"SELECT 'X' AS c_0, g_1.item_id AS c_1 FROM y.items AS g_1 GROUP BY g_1.item_id UNION ALL SELECT 'Y' AS c_0, g_0.item_id AS c_1 FROM y.items AS g_0 ORDER BY c_1 DESC LIMIT 50"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    }
    
    @Test public void testDistinctPushdownUnionWithConstant() throws Exception{
        String sql = "select distinct e1, 1 from (select 'a' as e1 from pm1.g1 union all select 'b' from pm1.g2) as x";
        
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SELECT_DISTINCT, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, false);
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
        
        helpPlan(sql, RealMetadataFactory.example1Cached(), null, new DefaultCapabilitiesFinder(caps), 
                new String[] {"SELECT g_0.e1 FROM pm1.g1 AS g_0 LIMIT 1", "SELECT g_0.e1 FROM pm1.g2 AS g_0 LIMIT 1"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        
    }
    
    @Test public void testDistinctPushdown() throws Exception{
        String sql = "select distinct e1, 1 from pm1.g1";
        
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SELECT_DISTINCT, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, false);
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
        
        helpPlan(sql, RealMetadataFactory.example1Cached(), null, new DefaultCapabilitiesFinder(caps), 
                new String[] {"SELECT DISTINCT g_0.e1 FROM pm1.g1 AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    }
    
    //same as above but with a different plan root
    @Test public void testDistinctPushdown1() throws Exception{
        String sql = "select distinct e1, 1 from pm1.g1 limit 1";
        
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SELECT_DISTINCT, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, false);
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
        
        helpPlan(sql, RealMetadataFactory.example1Cached(), null, new DefaultCapabilitiesFinder(caps), 
                new String[] {"SELECT DISTINCT g_0.e1 FROM pm1.g1 AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    }
    
    @Test public void testDistinctPushdownWithGrouping() throws Exception{
        String sql = "select distinct e1, e2, 1, 2 from (select e1, e2 from pm1.g1 group by e1, e2) v";
        
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SELECT_DISTINCT, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, false);
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
        
        ProcessorPlan plan = helpPlan(sql, RealMetadataFactory.example1Cached(), null, new DefaultCapabilitiesFinder(caps), 
                new String[] {"SELECT DISTINCT g_0.e1, g_0.e2 FROM pm1.g1 AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        
        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT DISTINCT g_0.e1, g_0.e2 FROM pm1.g1 AS g_0", Arrays.asList("a", 1), Arrays.asList("b", 2));
        
        List<?>[] expectedResults = new List[] {Arrays.asList("a", 1, 1, 2), Arrays.asList("b", 2, 1, 2)};
        
        TestProcessor.helpProcess(plan, dataManager, expectedResults);
    }

}
