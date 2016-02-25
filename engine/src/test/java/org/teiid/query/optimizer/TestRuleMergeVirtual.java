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
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings("nls")
public class TestRuleMergeVirtual {
    
    @Test public void testSimpleMergeGroupBy() {
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT x FROM (SELECT e1, max(e2) as x FROM pm1.g1 GROUP BY e1) AS z", //$NON-NLS-1$
                                      RealMetadataFactory.example1Cached(), null, TestAggregatePushdown.getAggregatesFinder(),
                                      new String[] {
                                          "SELECT MAX(e2) AS x FROM pm1.g1 GROUP BY e1"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$
    
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);                                    
    }
    
    @Test public void testNoUnnest() {
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT x FROM /*+ no_unnest */ (SELECT e1, max(e2) as x FROM pm1.g1 GROUP BY e1) AS z", //$NON-NLS-1$
                                      RealMetadataFactory.example1Cached(), null, TestAggregatePushdown.getAggregatesFinder(),
                                      new String[] {
                                          "SELECT v_0.c_0 FROM (SELECT MAX(g_0.e2) AS c_0 FROM pm1.g1 AS g_0 GROUP BY g_0.e1) AS v_0"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$
    
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);                                    
    }
    
    @Test public void testNoUnnestView() {
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT e1 FROM /*+ no_unnest */ vm1.g1", //$NON-NLS-1$
                                      RealMetadataFactory.example1Cached(), null, TestAggregatePushdown.getAggregatesFinder(),
                                      new String[] {
                                          "SELECT v_0.c_0 FROM (SELECT g_0.e1 AS c_0 FROM pm1.g1 AS g_0) AS v_0"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$
    
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);                                    
    }
    
    @Test public void testSimpleMergeGroupBy1() {
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT x FROM (SELECT distinct min(e1), max(e2) as x FROM pm1.g1 GROUP BY e1) AS z", //$NON-NLS-1$
                                      RealMetadataFactory.example1Cached(), null, TestAggregatePushdown.getAggregatesFinder(),
                                      new String[] {
                                          "SELECT v_0.c_1 FROM (SELECT DISTINCT MIN(g_0.e1) AS c_0, MAX(g_0.e2) AS c_1 FROM pm1.g1 AS g_0 GROUP BY g_0.e1) AS v_0"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$
    
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);                                    
    }

    /**
     * Same as above but all required symbols are selected
     */
    @Test public void testSimpleMergeGroupBy2() {
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
         
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT x, e1 FROM (SELECT distinct e1, max(e2) as x FROM pm1.g1 GROUP BY e1) AS z", //$NON-NLS-1$
                                      metadata, null, TestAggregatePushdown.getAggregatesFinder(),
                                      new String[] {
                                          "SELECT DISTINCT MAX(e2) AS x, e1 FROM pm1.g1 GROUP BY e1"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$
    
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);                                    
    }
    
    @Test public void testSimpleMergeGroupBy3() {
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
         
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT distinct x, e1 FROM (SELECT min(e1) as e1, max(e2) as x FROM pm1.g1 GROUP BY e1) AS z", //$NON-NLS-1$
                                      metadata, null, TestAggregatePushdown.getAggregatesFinder(),
                                      new String[] {
                                          "SELECT DISTINCT MAX(e2) AS x, MIN(e1) FROM pm1.g1 GROUP BY e1"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$
    
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);                                    
    }
    
    @Test public void testSimpleMergeGroupBy4() {
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT x, x FROM (SELECT e1, max(e2) as x FROM pm1.g1 GROUP BY e1) AS z", //$NON-NLS-1$
                                      RealMetadataFactory.example1Cached(), null, TestAggregatePushdown.getAggregatesFinder(),
                                      new String[] {
                                          "SELECT v_0.c_0 FROM (SELECT MAX(g_0.e2) AS c_0 FROM pm1.g1 AS g_0 GROUP BY g_0.e1) AS v_0"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$
    
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);                                    
    }
    
    @Test public void testSimpleMergeGroupBy5() {
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT x FROM (SELECT e1, max(e2) as x FROM pm1.g1 GROUP BY e1) AS z where z.x = 1", //$NON-NLS-1$
                                      RealMetadataFactory.example1Cached(), null, TestAggregatePushdown.getAggregatesFinder(),
                                      new String[] {
                                          "SELECT MAX(e2) AS x FROM pm1.g1 GROUP BY e1 HAVING MAX(e2) = 1"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$
    
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);                                    
    }
    
    @Test public void testSimpleMergeGroupBy6() {
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT x FROM (SELECT e1, max(e2) as x FROM pm1.g1 GROUP BY e1) AS z where z.x = 1", //$NON-NLS-1$
                                      RealMetadataFactory.example1Cached(), null, TestAggregatePushdown.getAggregatesFinder(),
                                      new String[] {
                                          "SELECT MAX(e2) AS x FROM pm1.g1 GROUP BY e1 HAVING MAX(e2) = 1"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$
    
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);                                    
    }
    
    @Test public void testSimpleMergeGroupBy7() {
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
         
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
                                      RealMetadataFactory.example1Cached(), null, capFinder,
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
                                      RealMetadataFactory.example1Cached(), null, capFinder,
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
                                      RealMetadataFactory.example1Cached(), null, capFinder,
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
                                      RealMetadataFactory.example1Cached(), null, capFinder,
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
                                      RealMetadataFactory.example1Cached(), null, capFinder,
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
                                      RealMetadataFactory.example1Cached(), null, capFinder,
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
                                      RealMetadataFactory.example1Cached(), null, capFinder,
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
    
    @Test public void testSimpleMergeUnionSecondBranchWithOrderBy() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_UNION, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
        caps.setCapabilitySupport(Capability.QUERY_SET_ORDER_BY, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
         
        ProcessorPlan plan = TestOptimizer.helpPlan("select '1' as x, e2 from pm1.g1 union all select e1, e2 from (select e1, 1 as e2 from pm1.g2 limit 1) as x order by x", //$NON-NLS-1$
                                      RealMetadataFactory.example1Cached(), null, capFinder,
                                      new String[] {"SELECT '1' AS c_0, pm1.g1.e2 AS c_1 FROM pm1.g1 UNION ALL (SELECT pm1.g2.e1 AS c_0, 1 AS c_1 FROM pm1.g2 LIMIT 1) ORDER BY c_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);                                    
    }
    
    @Test public void testSimpleMergeUnionSecondBranchWithOrderBy1() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_UNION, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
        caps.setCapabilitySupport(Capability.QUERY_SET_ORDER_BY, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_DISTINCT, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
         
        ProcessorPlan plan = TestOptimizer.helpPlan("select '1' as x, e2 from pm1.g1 union all (select e1, e2 from (select distinct e1, 1 as e2 from pm1.g2) as x order by e1 limit 1) order by x", //$NON-NLS-1$
                                      RealMetadataFactory.example1Cached(), null, capFinder,
                                      new String[] {"SELECT '1' AS c_0, pm1.g1.e2 AS c_1 FROM pm1.g1 UNION ALL (SELECT DISTINCT pm1.g2.e1 AS c_0, 1 AS c_1 FROM pm1.g2 ORDER BY c_0 LIMIT 1) ORDER BY c_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);                                    
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
        
        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();

        // Plan query
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, metadata, 
            null, capFinder,
            new String[] { "SELECT v_0.c_0 FROM (SELECT convert(g_0.TimestampValue, date) AS c_0 FROM BQT1.SmallA AS g_0, BQT1.SmallB AS g_1 WHERE g_0.StringKey = g_1.StringKey) AS v_0 GROUP BY v_0.c_0" },  //$NON-NLS-1$
            TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING);
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);         
    } 
    
    @Test public void testSortAliasWithSameName() throws Exception { 
        String sql = "select e1 from (select distinct pm1.g1.e1 as e1 from pm1.g1) x order by e1"; //$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        RelationalPlan plan = (RelationalPlan)TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(),  
        		new String[] {"SELECT g_0.e1 FROM pm1.g1 AS g_0"}, capFinder, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$  
        
        SortNode node = (SortNode)plan.getRootNode();
        assertTrue("Alias was not accounted for in sort node", node.getElements().get(0).equals(node.getSortElements().get(0).getSymbol())); //$NON-NLS-1$
    }
    
    @Test public void testSortAliasWithSameNameUnion() throws Exception { 
        String sql = "select e1 from (select distinct pm1.g1.e1 as e1 from pm1.g1) x union all select e1 from pm1.g2 order by e1"; //$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        RelationalPlan plan = (RelationalPlan)TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(),  
        		new String[] {"SELECT g_0.e1 FROM pm1.g1 AS g_0", "SELECT g_0.e1 FROM pm1.g2 AS g_0"}, capFinder, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$  
        
        SortNode node = (SortNode)plan.getRootNode();
        assertTrue("Alias was not accounted for in sort node", node.getElements().get(0).equals(node.getSortElements().get(0).getSymbol())); //$NON-NLS-1$
    }
    
    @Test public void testMergeImplicitGroupBy() throws Exception {
    	BasicSourceCapabilities caps = TestAggregatePushdown.getAggregateCapabilities();
    	caps.setFunctionSupport("+", true); //$NON-NLS-1$
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT x FROM (SELECT min(y), max(x) as x FROM (select e1 x, e2 + 1 y from pm1.g1) a) AS b", //$NON-NLS-1$
                                      RealMetadataFactory.example1Cached(), null, new DefaultCapabilitiesFinder(caps),
                                      new String[] {
                                          "SELECT MAX(g_0.e1) FROM pm1.g1 AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);                                    
    }
    
    @Test public void testMergeGroupBy() throws Exception {
    	BasicSourceCapabilities caps = TestAggregatePushdown.getAggregateCapabilities();
    	caps.setFunctionSupport("+", true); //$NON-NLS-1$
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT x FROM (select c.e1 as x from (select e1 from pm1.g1) as c, pm1.g2 as d) as a group by x", //$NON-NLS-1$
                                      RealMetadataFactory.example1Cached(), null, new DefaultCapabilitiesFinder(caps),
                                      new String[] {
                                          "SELECT g_0.e1 FROM pm1.g1 AS g_0, pm1.g2 AS g_1 GROUP BY g_0.e1"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);                                    
    }
    
    @Test public void testSortOverUnion() throws Exception {
        ProcessorPlan plan = TestOptimizer.helpPlan("select e1 from (select max(e1) as e1 from pm1.g1 having 1 = 0) as y union all select e2 from pm1.g1 union all select e1 from pm1.g1 order by e1", //$NON-NLS-1$
                                      RealMetadataFactory.example1Cached(), null, new DefaultCapabilitiesFinder(),
                                      new String[] {
                                          "SELECT pm1.g1.e2 FROM pm1.g1", "SELECT pm1.g1.e1 FROM pm1.g1"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        TestOptimizer.checkNodeTypes(plan, new int[] {
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
    }
    
    @Test public void testNoSourcesMerge() throws Exception {
        ProcessorPlan plan = TestOptimizer.helpPlan("select z.* from pm1.g1, (select 1 as a, 2, 3) as z where pm1.g1.e2 = z.a", //$NON-NLS-1$
                                      RealMetadataFactory.example1Cached(),
                                      new String[] {
                                          "SELECT 3 FROM pm1.g1 AS g_0 WHERE g_0.e2 = 1"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);                                    
    }

    /*
     * TODO: should be able to remove the limit
     */
    @Test public void testNoSourcesMerge1() throws Exception {
    	TestOptimizer.helpPlan("select z.* from pm1.g1, (select 1 as a, 2, 3 limit 2) as z where pm1.g1.e2 = z.a", //$NON-NLS-1$
                                      RealMetadataFactory.example1Cached(),
                                      new String[] {
                                          "SELECT g_0.e2 AS c_0 FROM pm1.g1 AS g_0 WHERE g_0.e2 IN (<dependent values>) ORDER BY c_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    }
    
    @Test public void testNoSourcesMerge2() throws Exception {
    	ProcessorPlan plan = TestOptimizer.helpPlan("select z.* from pm1.g1, (select 1 as a, lookup('pm1.g2', 'e1', 'e1', 'a') as b, 3) as z where pm1.g1.e2 = z.a and z.b = 'a'", //$NON-NLS-1$
                                      RealMetadataFactory.example1Cached(),
                                      new String[] {
                                          "SELECT 1, lookup('pm1.g2', 'e1', 'e1', 'a'), 3 FROM pm1.g1 AS g_0 WHERE (g_0.e2 = 1) AND (lookup('pm1.g2', 'e1', 'e1', 'a') = 'a')"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }
    
    @Test public void testNoSourcesMerge3() throws Exception {
        TestOptimizer.helpPlan("select z.* from pm1.g1 left outer join (select 1 as a, 2, 3) as z on pm1.g1.e2 = z.a", //$NON-NLS-1$
                                      RealMetadataFactory.example1Cached(),
                                      new String[] {
                                          "SELECT 3 FROM pm1.g1 AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        
    }
    
    @Test public void testNoSourcesMerge4() throws Exception {
        TestOptimizer.helpPlan("select z.* from pm1.g1 right outer join (select 1 as a, 2, 3) as z on pm1.g1.e2 = z.a", //$NON-NLS-1$
                                      RealMetadataFactory.example1Cached(),
                                      new String[] {
                                          "SELECT g_0.e2 AS c_0 FROM pm1.g1 AS g_0 WHERE g_0.e2 IN (<dependent values>) ORDER BY c_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    }
    
    @Test public void testNestedTableNoSourcesMerge() throws Exception {
    	BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
    	caps.setFunctionSupport("convert", true); //$NON-NLS-1$
    	caps.setFunctionSupport("array_get", true); //$NON-NLS-1$
    	ProcessorPlan plan = TestOptimizer.helpPlan("select z.* from pm1.g1, arraytable(cast(pm1.g1.e1 as object) COLUMNS one integer, two integer, three integer) as z", //$NON-NLS-1$
                RealMetadataFactory.example1Cached(),
                new String[] {
                    "SELECT convert(array_get(convert(g_0.e1, object), 1), integer), convert(array_get(convert(g_0.e1, object), 2), integer), convert(array_get(convert(g_0.e1, object), 3), integer) FROM pm1.g1 AS g_0 WHERE convert(g_0.e1, object) IS NOT NULL"}, new DefaultCapabilitiesFinder(caps), ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    	
    	TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }
    
    @Test public void testNestedTableNoSourcesMerge1() throws Exception {
    	BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
    	caps.setFunctionSupport("convert", true); //$NON-NLS-1$
    	caps.setFunctionSupport("array_get", true); //$NON-NLS-1$
    	ProcessorPlan plan = TestOptimizer.helpPlan("select z.* from pm1.g1 left outer join arraytable(cast(pm1.g1.e1 as object) COLUMNS one integer, two integer, three integer) as z on (pm1.g1.e2 = z.one)", //$NON-NLS-1$
                RealMetadataFactory.example1Cached(),
                new String[] {
                    "SELECT convert(array_get(convert(g_0.e1, object), 1), integer), convert(array_get(convert(g_0.e1, object), 2), integer), convert(array_get(convert(g_0.e1, object), 3), integer) FROM pm1.g1 AS g_0"}, new DefaultCapabilitiesFinder(caps), ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }
    
    @Test public void testUnrelated() throws Exception {
        BasicSourceCapabilities caps = TestAggregatePushdown.getAggregateCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FUNCTIONS_IN_GROUP_BY, false);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY_UNRELATED, false);

        String sql = "select a from (SELECT intkey as a, stringkey as b FROM BQT1.SmallA group by intkey, stringkey) as v order by b";
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(), //$NON-NLS-1$
            new String[]{"SELECT v_0.c_0, v_0.c_1 FROM (SELECT g_0.IntKey AS c_0, g_0.StringKey AS c_1 FROM BQT1.SmallA AS g_0 GROUP BY g_0.IntKey, g_0.StringKey) AS v_0 ORDER BY c_1"}, new DefaultCapabilitiesFinder(caps), ComparisonMode.EXACT_COMMAND_STRING);
        
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
    
}
