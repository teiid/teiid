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

import static junit.framework.Assert.*;

import org.junit.Test;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.TestOptimizer.ComparisonMode;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.FakeCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.relational.RelationalPlan;
import org.teiid.query.processor.relational.SortNode;
import org.teiid.query.unittest.FakeMetadataFacade;
import org.teiid.query.unittest.FakeMetadataFactory;

@SuppressWarnings("nls")
public class TestRuleMergeVirtual {
    
    @Test public void testSimpleMergeGroupBy() {
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT x FROM (SELECT e1, max(e2) as x FROM pm1.g1 GROUP BY e1) AS z", //$NON-NLS-1$
                                      FakeMetadataFactory.example1Cached(), null, TestAggregatePushdown.getAggregatesFinder(),
                                      new String[] {
                                          "SELECT MAX(e2) AS x FROM pm1.g1 GROUP BY e1"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$
    
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);                                    
    }
    
    @Test public void testSimpleMergeGroupBy1() {
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT x FROM (SELECT distinct min(e1), max(e2) as x FROM pm1.g1 GROUP BY e1) AS z", //$NON-NLS-1$
                                      FakeMetadataFactory.example1Cached(), null, TestAggregatePushdown.getAggregatesFinder(),
                                      new String[] {
                                          "SELECT v_0.c_1 FROM (SELECT DISTINCT MIN(g_0.e1) AS c_0, MAX(g_0.e2) AS c_1 FROM pm1.g1 AS g_0 GROUP BY g_0.e1) AS v_0"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$
    
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);                                    
    }

    /**
     * Same as above but all required symbols are selected
     */
    @Test public void testSimpleMergeGroupBy2() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
         
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT x, e1 FROM (SELECT distinct e1, max(e2) as x FROM pm1.g1 GROUP BY e1) AS z", //$NON-NLS-1$
                                      metadata, null, TestAggregatePushdown.getAggregatesFinder(),
                                      new String[] {
                                          "SELECT DISTINCT MAX(e2) AS x, e1 FROM pm1.g1 GROUP BY e1"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$
    
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);                                    
    }
    
    @Test public void testSimpleMergeGroupBy3() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
         
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT distinct x, e1 FROM (SELECT min(e1) as e1, max(e2) as x FROM pm1.g1 GROUP BY e1) AS z", //$NON-NLS-1$
                                      metadata, null, TestAggregatePushdown.getAggregatesFinder(),
                                      new String[] {
                                          "SELECT DISTINCT MAX(e2) AS x, MIN(e1) FROM pm1.g1 GROUP BY e1"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$
    
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);                                    
    }
    
    @Test public void testSimpleMergeGroupBy4() {
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT x, x FROM (SELECT e1, max(e2) as x FROM pm1.g1 GROUP BY e1) AS z", //$NON-NLS-1$
                                      FakeMetadataFactory.example1Cached(), null, TestAggregatePushdown.getAggregatesFinder(),
                                      new String[] {
                                          "SELECT v_0.c_0, v_0.c_0 FROM (SELECT MAX(g_0.e2) AS c_0 FROM pm1.g1 AS g_0 GROUP BY g_0.e1) AS v_0"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$
    
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);                                    
    }
    
    @Test public void testSimpleMergeGroupBy5() {
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT x FROM (SELECT e1, max(e2) as x FROM pm1.g1 GROUP BY e1) AS z where z.x = 1", //$NON-NLS-1$
                                      FakeMetadataFactory.example1Cached(), null, TestAggregatePushdown.getAggregatesFinder(),
                                      new String[] {
                                          "SELECT MAX(e2) AS x FROM pm1.g1 GROUP BY e1 HAVING MAX(e2) = 1"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$
    
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);                                    
    }
    
    @Test public void testSimpleMergeGroupBy6() {
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT x FROM (SELECT e1, max(e2) as x FROM pm1.g1 GROUP BY e1) AS z where z.x = 1", //$NON-NLS-1$
                                      FakeMetadataFactory.example1Cached(), null, TestAggregatePushdown.getAggregatesFinder(),
                                      new String[] {
                                          "SELECT MAX(e2) AS x FROM pm1.g1 GROUP BY e1 HAVING MAX(e2) = 1"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$
    
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);                                    
    }
    
    @Test public void testSimpleMergeGroupBy7() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
         
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT distinct x, e1 FROM (SELECT distinct min(e1) as e1, max(e2) as x FROM pm1.g1 GROUP BY e1) AS z", //$NON-NLS-1$
                                      metadata, null, TestAggregatePushdown.getAggregatesFinder(),
                                      new String[] {
                                          "SELECT DISTINCT MAX(e2) AS x, MIN(e1) FROM pm1.g1 GROUP BY e1"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$
    
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);                                    
    }
    
    @Test public void testSimpleMergeUnion() {
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
    
    @Test public void testSimpleMergeUnion1() {
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
    @Test public void testSimpleMergeUnion2() {
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
    
    //see TEIID-1562
    @Test public void testSimpleMergeUnderUnionWithJoin() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_UNION, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
         
        TestOptimizer.helpPlan("select * from (SELECT x.x, x.e2 FROM (select '1' as x, pm1.g1.e2 from pm1.g1, pm1.g2 where pm1.g1.e1 = pm1.g2.e1 group by pm1.g1.e2, pm1.g1.e3 || '1') x union all select e1, 1 from pm1.g2) as y where x = '1'", //$NON-NLS-1$
                                      FakeMetadataFactory.example1Cached(), null, capFinder,
                                      new String[] {
                                          "SELECT pm1.g2.e1 FROM pm1.g2", "SELECT pm1.g1.e1, pm1.g1.e2, pm1.g1.e3 FROM pm1.g1"}, TestOptimizer.SHOULD_SUCCEED); 
    }
    
    @Test public void testSimpleMergeUnion3() {
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
    
    @Test public void testSimpleMergeWithLimit() {
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
    
    @Test public void testSimpleMergeWithLimit1() {
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
    @Test public void testViewPreservationWithGroupByExpression() throws Exception {
        String sql = "SELECT gbl_date " + //$NON-NLS-1$
            "FROM " + //$NON-NLS-1$
            "(SELECT a.intkey as x, convert(a.TimestampValue, date) AS gbl_date, b.intkey as y " + //$NON-NLS-1$
            "FROM bqt1.smalla a INNER JOIN bqt1.smallb b on a.stringkey=b.stringkey) as z " + //$NON-NLS-1$
            "GROUP BY gbl_date"; //$NON-NLS-1$

        // Create capabilities
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT_STAR, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_INLINE_VIEWS, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setFunctionSupport("convert", true); //$NON-NLS-1$
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$
        
        QueryMetadataInterface metadata = FakeMetadataFactory.exampleBQTCached();

        // Plan query
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, metadata, 
            null, capFinder,
            new String[] { "SELECT v_0.c_0 FROM (SELECT convert(g_0.TimestampValue, date) AS c_0 FROM bqt1.smalla AS g_0, bqt1.smallb AS g_1 WHERE g_0.stringkey = g_1.stringkey) AS v_0 GROUP BY v_0.c_0" },  //$NON-NLS-1$
            TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING);
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);         
    } 
    
    @Test public void testSortAliasWithSameName() throws Exception { 
        String sql = "select e1 from (select distinct pm1.g1.e1 as e1 from pm1.g1) x order by e1"; //$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        RelationalPlan plan = (RelationalPlan)TestOptimizer.helpPlan(sql, FakeMetadataFactory.example1Cached(),  
        		new String[] {"SELECT g_0.e1 FROM pm1.g1 AS g_0"}, capFinder, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$  
        
        SortNode node = (SortNode)plan.getRootNode();
        assertTrue("Alias was not accounted for in sort node", node.getElements().get(0).equals(node.getSortElements().get(0).getSymbol())); //$NON-NLS-1$
    }
    
    @Test public void testMergeImplicitGroupBy() throws Exception {
    	BasicSourceCapabilities caps = TestAggregatePushdown.getAggregateCapabilities();
    	caps.setFunctionSupport("+", true); //$NON-NLS-1$
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT x FROM (SELECT min(y), max(x) as x FROM (select e1 x, e2 + 1 y from pm1.g1) a) AS b", //$NON-NLS-1$
                                      FakeMetadataFactory.example1Cached(), null, new DefaultCapabilitiesFinder(caps),
                                      new String[] {
                                          "SELECT MAX(g_0.e1) FROM pm1.g1 AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);                                    
    }

}
