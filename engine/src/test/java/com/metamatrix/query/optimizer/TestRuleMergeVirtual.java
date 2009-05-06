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

package com.metamatrix.query.optimizer;

import junit.framework.TestCase;

import com.metamatrix.query.optimizer.capabilities.BasicSourceCapabilities;
import com.metamatrix.query.optimizer.capabilities.FakeCapabilitiesFinder;
import com.metamatrix.query.optimizer.capabilities.SourceCapabilities.Capability;
import com.metamatrix.query.processor.ProcessorPlan;
import com.metamatrix.query.processor.relational.RelationalPlan;
import com.metamatrix.query.processor.relational.SortNode;
import com.metamatrix.query.unittest.FakeMetadataFacade;
import com.metamatrix.query.unittest.FakeMetadataFactory;

public class TestRuleMergeVirtual extends TestCase {
    
    public void testSimpleMergeGroupBy() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
         
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT x FROM (SELECT e1, max(e2) as x FROM pm1.g1 GROUP BY e1) AS z", //$NON-NLS-1$
                                      FakeMetadataFactory.example1Cached(), null, capFinder,
                                      new String[] {
                                          "SELECT MAX(e2) AS x FROM pm1.g1 GROUP BY e1"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$
    
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);                                    
    }
    
    public void testSimpleMergeGroupBy1() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_DISTINCT, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
         
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT x FROM (SELECT distinct e1, max(e2) as x FROM pm1.g1 GROUP BY e1) AS z", //$NON-NLS-1$
                                      FakeMetadataFactory.example1Cached(), null, capFinder,
                                      new String[] {
                                          "SELECT DISTINCT e1, MAX(e2) AS x FROM pm1.g1 GROUP BY e1"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$
    
        TestOptimizer.checkNodeTypes(plan, new int[] {
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

    /**
     * Same as above but all required symbols are selected
     */
    public void testSimpleMergeGroupBy2() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_DISTINCT, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
         
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT x, e1 FROM (SELECT distinct e1, max(e2) as x FROM pm1.g1 GROUP BY e1) AS z", //$NON-NLS-1$
                                      metadata, null, capFinder,
                                      new String[] {
                                          "SELECT DISTINCT MAX(e2) AS x, e1 FROM pm1.g1 GROUP BY e1"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$
    
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);                                    
    }
    
    public void testSimpleMergeGroupBy3() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_DISTINCT, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
         
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT distinct x, e1 FROM (SELECT e1, max(e2) as x FROM pm1.g1 GROUP BY e1) AS z", //$NON-NLS-1$
                                      metadata, null, capFinder,
                                      new String[] {
                                          "SELECT DISTINCT MAX(e2) AS x, e1 FROM pm1.g1 GROUP BY e1"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$
    
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);                                    
    }
    
    public void testSimpleMergeGroupBy4() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_DISTINCT, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
         
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT x, x FROM (SELECT e1, max(e2) as x FROM pm1.g1 GROUP BY e1) AS z", //$NON-NLS-1$
                                      FakeMetadataFactory.example1Cached(), null, capFinder,
                                      new String[] {
                                          "SELECT MAX(e2) AS x FROM pm1.g1 GROUP BY e1"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$
    
        TestOptimizer.checkNodeTypes(plan, new int[] {
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
    
    public void testSimpleMergeGroupBy5() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_DISTINCT, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
         
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT x FROM (SELECT e1, max(e2) as x FROM pm1.g1 GROUP BY e1) AS z where z.x = 1", //$NON-NLS-1$
                                      FakeMetadataFactory.example1Cached(), null, capFinder,
                                      new String[] {
                                          "SELECT MAX(e2) AS x FROM pm1.g1 GROUP BY e1 HAVING MAX(e2) = 1"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$
    
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);                                    
    }
    
    public void testSimpleMergeGroupBy6() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_DISTINCT, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
         
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT x FROM (SELECT e1, max(e2) as x FROM pm1.g1 GROUP BY e1) AS z where z.x = 1", //$NON-NLS-1$
                                      FakeMetadataFactory.example1Cached(), null, capFinder,
                                      new String[] {
                                          "SELECT MAX(e2) AS x FROM pm1.g1 GROUP BY e1 HAVING MAX(e2) = 1"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$
    
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);                                    
    }
    
    public void testSimpleMergeGroupBy7() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_DISTINCT, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
         
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT distinct x, e1 FROM (SELECT distinct e1, max(e2) as x FROM pm1.g1 GROUP BY e1) AS z", //$NON-NLS-1$
                                      metadata, null, capFinder,
                                      new String[] {
                                          "SELECT DISTINCT MAX(e2) AS x, e1 FROM pm1.g1 GROUP BY e1"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$
    
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);                                    
    }
    
    public void testSimpleMergeUnion() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_UNION, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
         
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT x FROM (select '1' as x, e2 from pm1.g1 union all select e1, 1 from pm1.g2) x", //$NON-NLS-1$
                                      FakeMetadataFactory.example1Cached(), null, capFinder,
                                      new String[] {
                                          "SELECT '1' AS x FROM pm1.g1 UNION ALL SELECT e1 FROM pm1.g2"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$
    
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);                                    
    }
    
    public void testSimpleMergeUnion1() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_UNION, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
         
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT distinct x FROM (select '1' as x, e2 from pm1.g1 union all select e1, 1 from pm1.g2) x", //$NON-NLS-1$
                                      FakeMetadataFactory.example1Cached(), null, capFinder,
                                      new String[] {
                                          "SELECT '1' AS x FROM pm1.g1 UNION SELECT e1 FROM pm1.g2"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$
    
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);                                    
    }  
    
    /**
     * Same as above, but the expression will prevent the source removal
     */
    public void testSimpleMergeUnion2() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_UNION, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
         
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT distinct x || 'b' FROM (select '1' as x, e2 from pm1.g1 union all select e1, 1 from pm1.g2) x", //$NON-NLS-1$
                                      FakeMetadataFactory.example1Cached(), null, capFinder,
                                      new String[] {
                                          "SELECT '1' AS x FROM pm1.g1 UNION ALL SELECT e1 FROM pm1.g2"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$
    
        TestOptimizer.checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            1,      // DupRemove
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
    
    public void testSimpleMergeUnion3() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_UNION, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
         
        ProcessorPlan plan = TestOptimizer.helpPlan("select * from (SELECT distinct x FROM (select '1' as x, e2 from pm1.g1 union all select e1, 1 from pm1.g2) x) y, pm1.g2", //$NON-NLS-1$
                                      FakeMetadataFactory.example1Cached(), null, capFinder,
                                      new String[] {
                                          "SELECT '1' AS x FROM pm1.g1 UNION SELECT e1 FROM pm1.g2", "SELECT pm1.g2.e1, pm1.g2.e2, pm1.g2.e3, pm1.g2.e4 FROM pm1.g2"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$ //$NON-NLS-2$
    
        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            1,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });                                    
    }
    
    public void testSimpleMergeWithLimit() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
         
        ProcessorPlan plan = TestOptimizer.helpPlan("select * from (select e1 from pm1.g1 limit 1) x", //$NON-NLS-1$
                                      FakeMetadataFactory.example1Cached(), null, capFinder,
                                      new String[] {
                                          "SELECT e1 FROM pm1.g1 LIMIT 1"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$
    
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);                                    
    }
    
    public void testSimpleMergeWithLimit1() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
         
        ProcessorPlan plan = TestOptimizer.helpPlan("select * from (select e1 from pm1.g1 limit 1) x order by e1", //$NON-NLS-1$
                                      FakeMetadataFactory.example1Cached(), null, capFinder,
                                      new String[] {
                                        "SELECT e1 FROM pm1.g1 LIMIT 1"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$
    
        TestOptimizer.checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // Limit
            0,      // NestedLoopJoinStrategy
            
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            1,      // Sort
            0       // UnionAll
        }, TestLimit.NODE_TYPES);                                    
    }
    
    /**
     * Note that the merge is not performed since it would create an expression in the group by clause
     */
    public void testViewPreservationWithGroupByExpression() throws Exception {
        String sql = "SELECT gbl_date " + //$NON-NLS-1$
            "FROM " + //$NON-NLS-1$
            "(SELECT a.intkey as x, convert(a.TimestampValue, date) AS gbl_date, b.intkey as y " + //$NON-NLS-1$
            "FROM bqt1.smalla a INNER JOIN bqt1.smallb b on a.stringkey=b.stringkey) as z " + //$NON-NLS-1$
            "GROUP BY gbl_date"; //$NON-NLS-1$

        // Create capabilities
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT_STAR, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_INLINE_VIEWS, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setFunctionSupport("convert", true); //$NON-NLS-1$
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$
        
        FakeMetadataFacade metadata = FakeMetadataFactory.exampleBQTCached();

        // Plan query
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, metadata, 
            null, capFinder,
            new String[] { "SELECT v_0.c_0 FROM (SELECT convert(g_0.TimestampValue, date) AS c_0 FROM bqt1.smalla AS g_0, bqt1.smallb AS g_1 WHERE g_0.stringkey = g_1.stringkey) AS v_0 GROUP BY v_0.c_0" },  //$NON-NLS-1$
            TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING);
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);         
    } 
    
    public void testSortAliasWithSameName() throws Exception { 
        String sql = "select e1 from (select distinct pm1.g1.e1 as e1 from pm1.g1) x order by e1"; //$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        RelationalPlan plan = (RelationalPlan)TestOptimizer.helpPlan(sql, FakeMetadataFactory.example1Cached(),  
        		new String[] {"SELECT g_0.e1 FROM pm1.g1 AS g_0"}, capFinder, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$  
        
        SortNode node = (SortNode)plan.getRootNode();
        assertTrue("Alias was not accounted for in sort node", node.getElements().containsAll(node.getSortElements())); //$NON-NLS-1$
    }

}
