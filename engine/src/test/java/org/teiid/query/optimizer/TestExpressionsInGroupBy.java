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

import org.junit.Test;
import org.teiid.query.optimizer.TestOptimizer.ComparisonMode;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.FakeCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.unittest.RealMetadataFactory;


/**
 * expressions in group use lacks robust support in MySQL, PostGres, and Derby, so a compensation step must be taken to create an inline view
 */
public class TestExpressionsInGroupBy {

    @Test public void testCase1565() throws Exception {
        // Create query
        String sql = "SELECT x, COUNT(*) FROM (SELECT convert(TimestampValue, date) AS x FROM bqt1.smalla) as y GROUP BY x"; //$NON-NLS-1$

        // Create capabilities
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        caps.setCapabilitySupport(Capability.QUERY_GROUP_BY, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT_STAR, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_INLINE_VIEWS, true);
        caps.setFunctionSupport("convert", true); //$NON-NLS-1$
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        // Plan query
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(), 
            null, capFinder,
            new String[] { "SELECT v_0.c_0, COUNT(*) FROM (SELECT convert(g_0.TimestampValue, date) AS c_0 FROM bqt1.smalla AS g_0) AS v_0 GROUP BY v_0.c_0" },  //$NON-NLS-1$
            true);
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);         
    }   
    
    // Merge across multiple virtual groups - should be same outcome as testCase1565
    @Test public void testCase1565_2() throws Exception {
        // Create query
        String sql = "SELECT x, COUNT(*) FROM (SELECT convert(TimestampValue, date) AS x FROM (SELECT TimestampValue from bqt1.smalla) as z) as y GROUP BY x"; //$NON-NLS-1$

        // Create capabilities
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        caps.setCapabilitySupport(Capability.QUERY_GROUP_BY, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT_STAR, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_INLINE_VIEWS, true);
        caps.setFunctionSupport("convert", true); //$NON-NLS-1$
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        // Plan query
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(), 
            null, capFinder,
            new String[] { "SELECT v_0.c_0, COUNT(*) FROM (SELECT convert(g_0.TimestampValue, date) AS c_0 FROM bqt1.smalla AS g_0) AS v_0 GROUP BY v_0.c_0" },  //$NON-NLS-1$
            true);
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);         
    }   
    
    // Merge across multiple virtual groups above the physical
    @Test public void testCase1565_3() throws Exception {
        String sql = "SELECT x, COUNT(*) FROM (SELECT convert(TimestampValue, date) AS x FROM (SELECT TimestampValue from bqt1.smalla) as z) as y GROUP BY x"; //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(), 
            null, TestOptimizer.getGenericFinder(),
            new String[] { "SELECT TimestampValue FROM bqt1.smalla" },  //$NON-NLS-1$
            true);
        TestOptimizer.checkNodeTypes(plan, new int[] {
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
             
    }           

    // Test what happens when not all the functions in the virtual SELECT can be pushed
    @Test public void testCase1565_4() throws Exception {
        // Create query
        String sql = "SELECT x, y FROM (SELECT convert(TimestampValue, date) as x, length(stringkey) as y from bqt1.smalla) as z GROUP BY x, y"; //$NON-NLS-1$

        // Create capabilities
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT_STAR, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$
        caps.setFunctionSupport("convert", true); //$NON-NLS-1$

        // Plan query
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(), 
            null, capFinder,
            new String[] { "SELECT TimestampValue, stringkey FROM bqt1.smalla" },  //$NON-NLS-1$
            true);
        TestOptimizer.checkNodeTypes(plan, new int[] {
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
             
    }       
    
    // Test nested functions
    @Test public void testCase1565_5() throws Exception {
        // Create query
        String sql = "SELECT x, COUNT(*) FROM (SELECT convert(intkey + 5, string) AS x FROM bqt1.smalla) as y GROUP BY x"; //$NON-NLS-1$

        // Create capabilities
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT_STAR, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setFunctionSupport("convert", true); //$NON-NLS-1$
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        // Plan query
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(), 
            null, capFinder,
            new String[] { "SELECT intkey FROM bqt1.smalla" },  //$NON-NLS-1$
            true);
        TestOptimizer.checkNodeTypes(plan, new int[] {
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
             
    }     
    
    // SELECT SUM(x) FROM (SELECT IntKey+1 AS x FROM BQT1.SmallA) AS g
    @Test public void testAggregateNoGroupByWithNestedFunction() {
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT SUM(x) FROM (SELECT IntKey+1 AS x FROM BQT1.SmallA) AS g", RealMetadataFactory.exampleBQTCached(), //$NON-NLS-1$
            new String[] { "SELECT IntKey FROM BQT1.SmallA"  }); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, new int[] {
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
    }     
    
    /**
     * Without inline view support or functions in group by the agg is not pushed down
     */
    @Test public void testFunctionInGroupBy() {
        String sql = "SELECT sum (IntKey), case when IntKey>=5000 then '5000 +' else '0-999' end " + //$NON-NLS-1$
            "FROM BQT1.SmallA GROUP BY case when IntKey>=5000 then '5000 +' else '0-999' end"; //$NON-NLS-1$

        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        caps.setCapabilitySupport(Capability.QUERY_CASE, true);
        caps.setCapabilitySupport(Capability.QUERY_SEARCHED_CASE, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_SUM, true);
        caps.setCapabilitySupport(Capability.QUERY_GROUP_BY, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_ORDERED, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan(sql,  
                                      RealMetadataFactory.exampleBQTCached(),
                                      null, capFinder,
                                      new String[] {"SELECT BQT1.SmallA.IntKey FROM BQT1.SmallA"}, //$NON-NLS-1$ 
                                      TestOptimizer.SHOULD_SUCCEED );

        TestOptimizer.checkNodeTypes(plan, new int[] {
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
    }
    
    @Test public void testFunctionInGroupBy1() {
        String sql = "SELECT sum (IntKey), case when IntKey>=5000 then '5000 +' else '0-999' end " + //$NON-NLS-1$
            "FROM BQT1.SmallA GROUP BY case when IntKey>=5000 then '5000 +' else '0-999' end"; //$NON-NLS-1$

        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        caps.setCapabilitySupport(Capability.QUERY_CASE, true);
        caps.setCapabilitySupport(Capability.QUERY_SEARCHED_CASE, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_SUM, true);
        caps.setCapabilitySupport(Capability.QUERY_FUNCTIONS_IN_GROUP_BY, true);
        caps.setCapabilitySupport(Capability.QUERY_GROUP_BY, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_ORDERED, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan(sql,  
                                      RealMetadataFactory.exampleBQTCached(),
                                      null, capFinder,
                                      new String[] {"SELECT SUM(BQT1.SmallA.IntKey), CASE WHEN BQT1.SmallA.IntKey >= 5000 THEN '5000 +' ELSE '0-999' END FROM BQT1.SmallA GROUP BY CASE WHEN BQT1.SmallA.IntKey >= 5000 THEN '5000 +' ELSE '0-999' END"}, //$NON-NLS-1$ 
                                      TestOptimizer.SHOULD_SUCCEED );

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);        
    }
    
    @Test public void testFunctionInGroupBy2() {
        String sql = "SELECT sum (IntKey), case when IntKey>=5000 then '5000 +' else '0-999' end " + //$NON-NLS-1$
            "FROM BQT1.SmallA GROUP BY case when IntKey>=5000 then '5000 +' else '0-999' end"; //$NON-NLS-1$

        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        caps.setCapabilitySupport(Capability.QUERY_CASE, true);
        caps.setCapabilitySupport(Capability.QUERY_SEARCHED_CASE, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_SUM, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_INLINE_VIEWS, true);
        caps.setCapabilitySupport(Capability.QUERY_GROUP_BY, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_ORDERED, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan(sql,  
                                      RealMetadataFactory.exampleBQTCached(),
                                      null, capFinder,
                                      new String[] {"SELECT SUM(v_0.c_1), v_0.c_0 FROM (SELECT CASE WHEN g_0.IntKey >= 5000 THEN '5000 +' ELSE '0-999' END AS c_0, g_0.IntKey AS c_1 FROM BQT1.SmallA AS g_0) AS v_0 GROUP BY v_0.c_0"}, //$NON-NLS-1$ 
                                      TestOptimizer.SHOULD_SUCCEED );

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);        
    }

    
    /**
     * Test what happens when we have a CASE in the GROUP BY and source has aggregate capability but 
     * does not have CASE capability.  Should not be able to push down GROUP BY.  
     * 
     * @since 4.2
     */
    @Test public void testFunctionInGroupByCantPush() {
        String sql = "SELECT sum (IntKey), case when IntKey>=5000 then '5000 +' else '0-999' end " + //$NON-NLS-1$
            "FROM BQT1.SmallA GROUP BY case when IntKey>=5000 then '5000 +' else '0-999' end"; //$NON-NLS-1$

        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_SUM, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_ORDERED, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_CASE, false);
        caps.setCapabilitySupport(Capability.QUERY_SEARCHED_CASE, false);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan(sql,  
                                      RealMetadataFactory.exampleBQTCached(),
                                      null, capFinder,
                                      new String[] {"SELECT IntKey FROM BQT1.SmallA"}, //$NON-NLS-1$ 
                                      TestOptimizer.SHOULD_SUCCEED );

        TestOptimizer.checkNodeTypes(plan, new int[] {
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
    }

    /**
     * Test what happens when we have a CASE in the GROUP BY and source has aggregate capability but 
     * does not have CASE capability.  Should not be able to push down GROUP BY.  
     * 
     * @since 4.2
     */
    @Test public void testFunctionInGroupByHavingCantPush() {
        String sql = "SELECT sum (IntKey), case when IntKey>=5000 then '5000 +' else '0-999' end " + //$NON-NLS-1$
            "FROM BQT1.SmallA GROUP BY case when IntKey>=5000 then '5000 +' else '0-999' end " + //$NON-NLS-1$
            "HAVING case when IntKey>=5000 then '5000 +' else '0-999' end = '5000 +'"; //$NON-NLS-1$

        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_SUM, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_ORDERED, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_CASE, false);
        caps.setCapabilitySupport(Capability.QUERY_SEARCHED_CASE, false);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan(sql,  
                                      RealMetadataFactory.exampleBQTCached(),
                                      null, capFinder,
                                      new String[] {"SELECT IntKey FROM BQT1.SmallA"}, //$NON-NLS-1$ 
                                      TestOptimizer.SHOULD_SUCCEED );

        TestOptimizer.checkNodeTypes(plan, new int[] {
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
                                        1,      // Select
                                        0,      // Sort
                                        0       // UnionAll
                                    });        
    }
    /**
     * Test what happens when we have a CASE in the GROUP BY and source has aggregate capability but 
     * does not have CASE capability.  Should not be able to push down GROUP BY.  
     * 
     * @since 4.2
     */
    @Test public void testFunctionInGroupByCantPushRewritten() {
        String sql = "SELECT SUM(IntKey), c FROM (SELECT IntKey, case when IntKey>=5000 then '5000 +' else '0-999' end AS c FROM BQT1.SmallA) AS temp GROUP BY c"; //$NON-NLS-1$

        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_SUM, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_ORDERED, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_CASE, false);
        caps.setCapabilitySupport(Capability.QUERY_SEARCHED_CASE, false);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan(sql,  
                                      RealMetadataFactory.exampleBQTCached(),
                                      null, capFinder,
                                      new String[] {"SELECT IntKey FROM BQT1.SmallA"}, //$NON-NLS-1$ 
                                      TestOptimizer.SHOULD_SUCCEED );

        TestOptimizer.checkNodeTypes(plan, new int[] {
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
    }
    
    @Test public void testFunctionOfAggregateCantPush2() {
        String sql = "SELECT SUM(length(StringKey || 'x')) + 1 AS x FROM BQT1.SmallA GROUP BY StringKey || 'x' HAVING space(MAX(length((StringKey || 'x') || 'y'))) = '   '"; //$NON-NLS-1$

        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan(sql,  
                                      RealMetadataFactory.exampleBQTCached(),
                                      null, capFinder,
                                      new String[] {"SELECT StringKey FROM BQT1.SmallA"}, //$NON-NLS-1$ 
                                      TestOptimizer.SHOULD_SUCCEED );

        TestOptimizer.checkNodeTypes(plan, new int[] {
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
                                        1,      // Select
                                        0,      // Sort
                                        0       // UnionAll
                                    });        
    }
    
    
    @Test public void testDontPushGroupByUnsupportedFunction() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_GROUP_BY, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
         
        ProcessorPlan plan = TestOptimizer.helpPlan(
            "SELECT e2 as x FROM pm1.g1 GROUP BY upper(e1), e2",  //$NON-NLS-1$
            RealMetadataFactory.example1Cached(),
            null, capFinder,
            new String[] {"SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1"}, //$NON-NLS-1$
            ComparisonMode.EXACT_COMMAND_STRING );
    
        TestOptimizer.checkNodeTypes(plan, new int[] {
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
                               
    }
        
}
