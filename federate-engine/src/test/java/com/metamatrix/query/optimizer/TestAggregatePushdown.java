/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
import com.metamatrix.query.unittest.FakeMetadataFacade;
import com.metamatrix.query.unittest.FakeMetadataFactory;

public class TestAggregatePushdown extends TestCase {
    
    private FakeCapabilitiesFinder getAggregatesFinder() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_SUM, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        capFinder.addCapabilities("m1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("m2", caps); //$NON-NLS-1$

        return capFinder;
    }

    public void testCase6327() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_SUM, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT_STAR, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_FUNCTIONS_IN_GROUP_BY, true);
        caps.setFunctionSupport("convert", true); //$NON-NLS-1$
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("BQT2", caps); //$NON-NLS-1$
        
        String sql = "SELECT a12.intkey AS REGION_NBR, SUM(a11.intnum) AS WJXBFS1 FROM bqt1.smalla AS a11 INNER JOIN bqt2.smalla AS a12 ON a11.stringkey = a12.stringkey WHERE a11.stringkey = 0 GROUP BY a12.intkey"; //$NON-NLS-1$
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, FakeMetadataFactory.exampleBQTCached(), null, capFinder, 
                                      new String[] {"SELECT SUM(a11.intnum) FROM bqt1.smalla AS a11 WHERE a11.stringkey = '0' HAVING COUNT(*) > 0", "SELECT a12.intkey FROM bqt2.smalla AS a12 WHERE a12.stringkey = '0' group by a12.intkey"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$ //$NON-NLS-2$ 
        
        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            1,      // Grouping
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
    
    /**
     * Note that intnum is retrieved from each source
     * 
     * Note also that this test shows that the max aggregate is not placed on the bqt2 query since it would be on one of the group by expressions
     */
    public void testAggregateOfJoinExpression() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_SUM, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT_STAR, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_FUNCTIONS_IN_GROUP_BY, true);
        caps.setFunctionSupport("convert", true); //$NON-NLS-1$
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("BQT2", caps); //$NON-NLS-1$
        
        String sql = "SELECT a12.intkey, MAX(a12.stringkey), SUM(a11.intnum+a12.intnum) FROM bqt1.smalla AS a11 INNER JOIN bqt2.smalla AS a12 ON a11.stringkey = a12.stringkey GROUP BY a12.intkey"; //$NON-NLS-1$
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, FakeMetadataFactory.exampleBQTCached(), null, capFinder, 
                                      new String[] {"SELECT g_0.stringkey, g_0.intkey, g_0.intnum FROM bqt2.smalla AS g_0 GROUP BY g_0.stringkey, g_0.intkey, g_0.intnum", "SELECT g_0.stringkey, g_0.intnum FROM bqt1.smalla AS g_0"}, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$ 
        
        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            1,      // Grouping
            0,      // NestedLoopJoinStrategy
            1,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        }); 
    }
    
    /**
     * Note that even though this grouping is join invariant, we still do not remove the top level group by
     * since we are not checking the uniqueness of the x side join expressions 
     */
    public void testInvariantAggregate() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$
        
        String sql = "SELECT max(y.e2) from pm1.g1 x, pm2.g1 y where x.e3 = y.e3 group by y.e3"; //$NON-NLS-1$
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, FakeMetadataFactory.example1Cached(), null, capFinder, 
                                      new String[] {"SELECT g_0.e3 FROM pm1.g1 AS g_0", "SELECT g_0.e3, MAX(g_0.e2) FROM pm2.g1 AS g_0 GROUP BY g_0.e3"}, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$ 
        
        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            1,      // Grouping
            0,      // NestedLoopJoinStrategy
            1,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        }); 
    }

    /**
     * Test of an aggregate nested in an expression symbol
     */
    public void testCase6211() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_SUM, true);
        caps.setCapabilitySupport(Capability.QUERY_FUNCTIONS_IN_GROUP_BY, true);
        caps.setFunctionSupport("convert", true); //$NON-NLS-1$
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("BQT2", caps); //$NON-NLS-1$
        
        String sql = "select sum(a11.intnum) Profit, (sum(a11.intnum) / sum(a11.floatnum)) WJXBFS2 from bqt1.smalla a11 join bqt2.smallb a12 on a11.intkey=a12.intkey group by a12.intkey"; //$NON-NLS-1$
        
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, FakeMetadataFactory.exampleBQTCached(), null, capFinder, 
                                      new String[] {"SELECT g_0.intkey, SUM(g_0.intnum), SUM(g_0.floatnum) FROM bqt1.smalla AS g_0 GROUP BY g_0.intkey", "SELECT g_0.intkey FROM bqt2.smallb AS g_0 GROUP BY g_0.intkey"}, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$ 
        
        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            1,      // Grouping
            0,      // NestedLoopJoinStrategy
            1,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        }); 
    }
    
    /**
     * Note that until we can test the other side cardinality, we cannot fully push the group node
     */ 
    public void testAggregatePushdown1() throws Exception {        
        FakeMetadataFacade metadata = FakeMetadataFactory.exampleAggregatesCached();
        String sql = "SELECT o_dealerid, o_productid, sum(o_amount) FROM m1.order, m1.dealer, m2.product " +  //$NON-NLS-1$
            "WHERE o_dealerid=d_dealerid AND o_productid=p_productid AND d_state = 'CA' AND p_divid = 100 " +  //$NON-NLS-1$
            "GROUP BY o_dealerid, o_productid"; //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan(sql,  
                                      metadata,
                                      null, getAggregatesFinder(),
                                      new String[] {"SELECT g_0.p_productid AS c_0 FROM m2.product AS g_0 WHERE g_0.p_divid = 100 ORDER BY c_0", "SELECT g_0.o_productid AS c_0, g_0.o_dealerid AS c_1, SUM(g_0.o_amount) AS c_2 FROM m1.\"order\" AS g_0, m1.dealer AS g_1 WHERE (g_0.o_productid IN (<dependent values>)) AND (g_0.o_dealerid = g_1.d_dealerid) AND (g_1.d_state = 'CA') GROUP BY g_0.o_productid, g_0.o_dealerid ORDER BY c_0"},  //$NON-NLS-1$ //$NON-NLS-2$
                                                    TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING );

        TestOptimizer.checkNodeTypes(plan, new int[] {
                                        1,      // Access
                                        1,      // DependentAccess
                                        0,      // DependentSelect
                                        0,      // DependentProject
                                        0,      // DupRemove
                                        1,      // Grouping
                                        0,      // NestedLoopJoinStrategy
                                        1,      // MergeJoinStrategy
                                        0,      // Null
                                        0,      // PlanExecution
                                        1,      // Project
                                        0,      // Select
                                        0,      // Sort
                                        0       // UnionAll
                                    });        
    }

    public void testAggregatePushdown2() throws Exception {        
        FakeMetadataFacade metadata = FakeMetadataFactory.exampleAggregatesCached();
        String sql = "SELECT o_dealerid, o_productid, sum(o_amount) FROM m1.order, m1.dealer, m2.product " +  //$NON-NLS-1$
            "WHERE o_dealerid=d_dealerid AND o_productid=p_productid AND d_state = 'CA' AND p_divid = 100 " +  //$NON-NLS-1$
            "GROUP BY o_dealerid, o_productid having max(o_amount) < 100"; //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan(sql,  
                                      metadata,
                                      null, getAggregatesFinder(),
                                      new String[] {"SELECT g_0.p_productid AS c_0 FROM m2.product AS g_0 WHERE g_0.p_divid = 100 ORDER BY c_0", "SELECT g_0.o_productid AS c_0, g_0.o_dealerid AS c_1, MAX(g_0.o_amount) AS c_2, SUM(g_0.o_amount) AS c_3 FROM m1.\"order\" AS g_0, m1.dealer AS g_1 WHERE (g_0.o_productid IN (<dependent values>)) AND (g_0.o_dealerid = g_1.d_dealerid) AND (g_1.d_state = 'CA') GROUP BY g_0.o_productid, g_0.o_dealerid ORDER BY c_0"},  //$NON-NLS-1$ //$NON-NLS-2$
                                                    TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING );

        TestOptimizer.checkNodeTypes(plan, new int[] {
                                        1,      // Access
                                        1,      // DependentAccess
                                        0,      // DependentSelect
                                        0,      // DependentProject
                                        0,      // DupRemove
                                        1,      // Grouping
                                        0,      // NestedLoopJoinStrategy
                                        1,      // MergeJoinStrategy
                                        0,      // Null
                                        0,      // PlanExecution
                                        1,      // Project
                                        1,      // Select
                                        0,      // Sort
                                        0       // UnionAll
                                    });        
    }
    
    /**
     * Average requires the creation of staged sum and count aggregates
     */
    public void testAvgAggregate() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_SUM, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$
        
        String sql = "SELECT avg(y.e2) from pm1.g1 x, pm2.g1 y where x.e3 = y.e3 group by x.e2, y.e1"; //$NON-NLS-1$
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, FakeMetadataFactory.example1Cached(), null, capFinder, 
                                      new String[] {"SELECT g_0.e3, g_0.e2 FROM pm1.g1 AS g_0 GROUP BY g_0.e3, g_0.e2", "SELECT g_0.e3, g_0.e1, SUM(g_0.e2), COUNT(g_0.e2) FROM pm2.g1 AS g_0 GROUP BY g_0.e3, g_0.e1"}, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$ 
        
        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            1,      // Grouping
            0,      // NestedLoopJoinStrategy
            1,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        }); 
    }
    
    public void testCountAggregate() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_SUM, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$
        
        String sql = "SELECT count(y.e2) from pm1.g1 x, pm2.g1 y where x.e3 = y.e3 group by x.e2, y.e1"; //$NON-NLS-1$
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, FakeMetadataFactory.example1Cached(), null, capFinder, 
                                      new String[] {"SELECT g_0.e3, g_0.e2 FROM pm1.g1 AS g_0 GROUP BY g_0.e3, g_0.e2", "SELECT g_0.e3, g_0.e1, COUNT(g_0.e2) FROM pm2.g1 AS g_0 GROUP BY g_0.e3, g_0.e1"}, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$ 
        
        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            1,      // Grouping
            0,      // NestedLoopJoinStrategy
            1,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        }); 
    }
    
    public void testOuterJoinPreventsPushdown() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_SUM, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$
        
        String sql = "SELECT count(y.e2) from pm1.g1 x left outer join pm2.g1 y on x.e3 = y.e3 group by x.e2, y.e1"; //$NON-NLS-1$
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, FakeMetadataFactory.example1Cached(), null, capFinder, 
                                      new String[] {"SELECT g_0.e3, g_0.e2 FROM pm1.g1 AS g_0", "SELECT g_0.e3, g_0.e1, g_0.e2 FROM pm2.g1 AS g_0"}, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$ 
        
        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            1,      // Grouping
            0,      // NestedLoopJoinStrategy
            1,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        }); 
    }
    
    /**
     * Test to ensure count(*) isn't mistakenly pushed to either side, but that
     * grouping can still be.
     */
    public void testCase5724() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT_STAR, true);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("BQT2", caps); //$NON-NLS-1$
        FakeMetadataFacade metadata = FakeMetadataFactory.exampleBQTCached();
        
        ProcessorPlan plan = TestOptimizer.helpPlan(
              "select count(*), a.intnum from bqt1.smalla as a, bqt2.smallb as b where a.intkey = b.intkey group by a.intnum",  //$NON-NLS-1$
              metadata, null, capFinder,
              new String[] { 
                "SELECT a.intkey, a.intnum FROM bqt1.smalla AS a group by a.intkey, a.intnum", "SELECT b.intkey FROM bqt2.smallb AS b"},  //$NON-NLS-1$ //$NON-NLS-2$
                true); 
                  
        TestOptimizer.checkNodeTypes(plan, new int[] {
             2,      // Access
             0,      // DependentAccess
             0,      // DependentSelect
             0,      // DependentProject
             0,      // DupRemove
             1,      // Grouping
             0,      // NestedLoopJoinStrategy
             1,      // MergeJoinStrategy
             0,      // Null
             0,      // PlanExecution
             1,      // Project
             0,      // Select
             0,      // Sort
             0       // UnionAll
        });                                    
    }

    public void testCase6210() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_SUM, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_FUNCTIONS_IN_GROUP_BY, true);
        caps.setFunctionSupport("convert", true); //$NON-NLS-1$
        caps.setFunctionSupport("/", true); //$NON-NLS-1$
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("BQT2", TestOptimizer.getTypicalCapabilities()); //$NON-NLS-1$
        
        String sql = "select a11.intkey ITEM_ID, sum(a11.intnum) WJXBFS1 from bqt1.smalla a11 join bqt2.smalla a12 on (a11.stringkey = a12.stringkey) join bqt2.smallb a13 on (a11.intkey = a13.intkey) where a13.intnum in (10) group by a11.intkey"; //$NON-NLS-1$
        
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, FakeMetadataFactory.exampleBQT(), null, capFinder, 
                                      new String[] {"SELECT g_0.stringkey FROM bqt2.smalla AS g_0", "SELECT g_0.stringkey, g_0.intkey, SUM(g_0.intnum) FROM bqt1.smalla AS g_0 GROUP BY g_0.stringkey, g_0.intkey", "SELECT g_0.intkey FROM bqt2.smallb AS g_0 WHERE g_0.intnum = 10"}, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
        
        TestOptimizer.checkNodeTypes(plan, new int[] {
            3,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            1,      // Grouping
            0,      // NestedLoopJoinStrategy
            2,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        }); 
    }               
    
}
