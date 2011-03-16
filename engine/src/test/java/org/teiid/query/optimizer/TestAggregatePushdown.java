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

import static org.teiid.query.optimizer.TestOptimizer.*;

import org.junit.Test;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.TestOptimizer.ComparisonMode;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.FakeCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.unittest.FakeMetadataFacade;
import org.teiid.query.unittest.FakeMetadataFactory;
import org.teiid.translator.SourceSystemFunctions;

@SuppressWarnings("nls")
public class TestAggregatePushdown {

	public static BasicSourceCapabilities getAggregateCapabilities() {
		BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_INLINE_VIEWS, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_SUM, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MIN, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_AVG, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT_STAR, true);
        caps.setCapabilitySupport(Capability.QUERY_GROUP_BY, true);
        caps.setCapabilitySupport(Capability.QUERY_HAVING, true);
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
		return caps;
	}

    public static CapabilitiesFinder getAggregatesFinder() {
    	return new DefaultCapabilitiesFinder(getAggregateCapabilities());
    }

    @Test public void testCase6327() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getAggregateCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
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
    @Test public void testAggregateOfJoinExpression() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getAggregateCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, false);
        caps.setFunctionSupport("convert", true); //$NON-NLS-1$
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("BQT2", caps); //$NON-NLS-1$
        
        String sql = "SELECT a12.intkey, MAX(a12.stringkey), MIN(a11.intnum+a12.intnum) FROM bqt1.smalla AS a11 INNER JOIN bqt2.smalla AS a12 ON a11.stringkey = a12.stringkey GROUP BY a12.intkey"; //$NON-NLS-1$
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
    
    @Test public void testAggregateOfJoinExpression1() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getAggregateCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, false);
        caps.setFunctionSupport("convert", true); //$NON-NLS-1$
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("BQT2", caps); //$NON-NLS-1$
        
        String sql = "SELECT a12.intkey, MAX(a12.stringkey), SUM(a11.intnum+a12.intnum) FROM bqt1.smalla AS a11 INNER JOIN bqt2.smalla AS a12 ON a11.stringkey = a12.stringkey GROUP BY a12.intkey"; //$NON-NLS-1$
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, FakeMetadataFactory.exampleBQTCached(), null, capFinder, 
                                      new String[] {"SELECT g_0.stringkey, g_0.intkey, g_0.intnum FROM bqt2.smalla AS g_0", "SELECT g_0.stringkey, g_0.intnum FROM bqt1.smalla AS g_0"}, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$ 
        
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
    @Test public void testInvariantAggregate() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getAggregateCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, false);
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
    @Test public void testCase6211() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getAggregateCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, false);
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
    @Test public void testAggregatePushdown1() throws Exception {        
        FakeMetadataFacade metadata = FakeMetadataFactory.exampleAggregatesCached();
        String sql = "SELECT o_dealerid, o_productid, sum(o_amount) FROM m1.order, m1.dealer, m2.product " +  //$NON-NLS-1$
            "WHERE o_dealerid=d_dealerid AND o_productid=p_productid AND d_state = 'CA' AND p_divid = 100 " +  //$NON-NLS-1$
            "GROUP BY o_dealerid, o_productid"; //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan(sql,  
                                      metadata,
                                      null, getAggregatesFinder(),
                                      new String[] {"SELECT g_0.p_productid AS c_0 FROM m2.product AS g_0 WHERE g_0.p_divid = 100 ORDER BY c_0", "SELECT g_0.o_productid AS c_0, g_0.o_dealerid AS c_1, SUM(g_0.o_amount) AS c_2 FROM m1.\"order\" AS g_0, m1.dealer AS g_1 WHERE (g_0.o_dealerid = g_1.d_dealerid) AND (g_1.d_state = 'CA') AND (g_0.o_productid IN (<dependent values>)) GROUP BY g_0.o_productid, g_0.o_dealerid ORDER BY c_0"},  //$NON-NLS-1$ //$NON-NLS-2$
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

    @Test public void testAggregatePushdown2() throws Exception {        
        FakeMetadataFacade metadata = FakeMetadataFactory.exampleAggregatesCached();
        String sql = "SELECT o_dealerid, o_productid, sum(o_amount) FROM m1.order, m1.dealer, m2.product " +  //$NON-NLS-1$
            "WHERE o_dealerid=d_dealerid AND o_productid=p_productid AND d_state = 'CA' AND p_divid = 100 " +  //$NON-NLS-1$
            "GROUP BY o_dealerid, o_productid having max(o_amount) < 100"; //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan(sql,  
                                      metadata,
                                      null, getAggregatesFinder(),
                                      new String[] {"SELECT g_0.p_productid AS c_0 FROM m2.product AS g_0 WHERE g_0.p_divid = 100 ORDER BY c_0", "SELECT g_0.o_productid AS c_0, g_0.o_dealerid AS c_1, MAX(g_0.o_amount) AS c_2, SUM(g_0.o_amount) AS c_3 FROM m1.\"order\" AS g_0, m1.dealer AS g_1 WHERE (g_0.o_dealerid = g_1.d_dealerid) AND (g_1.d_state = 'CA') AND (g_0.o_productid IN (<dependent values>)) GROUP BY g_0.o_productid, g_0.o_dealerid ORDER BY c_0"},  //$NON-NLS-1$ //$NON-NLS-2$
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
    @Test public void testAvgAggregate() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getAggregateCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, false);
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
    
    @Test public void testStddevAggregate() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getAggregateCapabilities();
        caps.setFunctionSupport(SourceSystemFunctions.POWER, true);
        caps.setFunctionSupport(SourceSystemFunctions.CONVERT, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$
        
        String sql = "SELECT stddev_pop(y.e2) from pm1.g1 x, pm2.g1 y where x.e3 = y.e3 group by x.e2, y.e1"; //$NON-NLS-1$
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, FakeMetadataFactory.example1Cached(), null, capFinder, 
                                      new String[] {"SELECT g_0.e3 AS c_0, g_0.e1 AS c_1, COUNT(g_0.e2) AS c_2, SUM(power(g_0.e2, 2)) AS c_3, SUM(g_0.e2) AS c_4 FROM pm2.g1 AS g_0 GROUP BY g_0.e3, g_0.e1 ORDER BY c_0", "SELECT g_0.e3 AS c_0, g_0.e2 AS c_1 FROM pm1.g1 AS g_0 GROUP BY g_0.e3, g_0.e2 ORDER BY c_0"}, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$ 
        
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
    
    @Test public void testCountAggregate() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getAggregateCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, false);
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
    
    @Test public void testOuterJoinPreventsPushdown() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getAggregateCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, false);
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
     * Test to ensure count(*) isn't mistakenly pushed to either side
     */
    @Test public void testCase5724() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getAggregateCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, false);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("BQT2", caps); //$NON-NLS-1$
        QueryMetadataInterface metadata = FakeMetadataFactory.exampleBQTCached();
        
        ProcessorPlan plan = TestOptimizer.helpPlan(
              "select count(*), a.intnum from bqt1.smalla as a, bqt2.smallb as b where a.intkey = b.intkey group by a.intnum",  //$NON-NLS-1$
              metadata, null, capFinder,
              new String[] { 
                "SELECT a.intkey, a.intnum FROM bqt1.smalla AS a", "SELECT b.intkey FROM bqt2.smallb AS b"},  //$NON-NLS-1$ //$NON-NLS-2$
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

    @Test public void testCase6210() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getAggregateCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, false);
        caps.setFunctionSupport("convert", true); //$NON-NLS-1$
        caps.setFunctionSupport("/", true); //$NON-NLS-1$
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("BQT2", caps); //$NON-NLS-1$
        
        String sql = "select a11.intkey ITEM_ID, sum(a11.intnum) WJXBFS1 from bqt1.smalla a11 join bqt2.smalla a12 on (a11.stringkey = a12.stringkey) join bqt2.smallb a13 on (a11.intkey = a13.intkey) where a13.intnum in (10) group by a11.intkey"; //$NON-NLS-1$
        
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, FakeMetadataFactory.exampleBQTCached(), null, capFinder, 
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
    
    @Test public void testNoGroupAggregatePushdown() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_GROUP_BY, false);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT_STAR, true);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$
        QueryMetadataInterface metadata = FakeMetadataFactory.exampleBQTCached();
        
        ProcessorPlan plan = TestOptimizer.helpPlan(
              "select count(*) from bqt1.smalla",  //$NON-NLS-1$
              metadata, null, capFinder,
              new String[] { 
                "SELECT count(*) from bqt1.smalla"},  //$NON-NLS-1$
                true); 
                  
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);                                    
    }
    
    @Test public void testNoHavingAggregate() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_GROUP_BY, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$
        QueryMetadataInterface metadata = FakeMetadataFactory.exampleBQTCached();
        
        ProcessorPlan plan = TestOptimizer.helpPlan(
              "select stringkey, max(intkey) from bqt1.smalla group by stringkey having count(intkey) = 1",  //$NON-NLS-1$
              metadata, null, capFinder,
              new String[] { 
                "SELECT COUNT(g_0.intkey), g_0.stringkey, MAX(g_0.intkey) FROM bqt1.smalla AS g_0 GROUP BY g_0.stringkey"},  //$NON-NLS-1$
                ComparisonMode.EXACT_COMMAND_STRING); 
                  
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
                1,      // Select
                0,      // Sort
                0       // UnionAll
            });                                    
    }
    
    @Test public void testHavingCriteriaPushDown() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_GROUP_BY, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        ProcessorPlan plan = TestOptimizer.helpPlan("select X.e1 FROM vm1.g1 X group by X.e1 having X.e1 = 1 and sum(X.e2) = 2", FakeMetadataFactory.example1Cached(), null, capFinder,  //$NON-NLS-1$
            new String[]{"SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1 WHERE pm1.g1.e1 = '1'"}, true); //$NON-NLS-1$
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
    
    @Test public void testBusObjQuestion1() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);    
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, true);    
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, true);    
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);    
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_SUM, true);    
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_AVG, true);    
        
        capFinder.addCapabilities("db2model", caps); //$NON-NLS-1$
        capFinder.addCapabilities("oraclemodel", caps); //$NON-NLS-1$
        capFinder.addCapabilities("msmodel", caps); //$NON-NLS-1$
        
        FakeMetadataFacade metadata = FakeMetadataFactory.exampleBusObj();

        String sql = "SELECT Q1.S, Q2.C, Q1.PRODUCT, Q1.REGION AS Q1R, Q2.REGION AS Q2R FROM " + //$NON-NLS-1$
            "(SELECT SUM(SALES) AS S, REGION, PRODUCT FROM DB2_TABLE WHERE PRODUCT IN ('GUNS', 'TOYS', 'VIDEOTAPES') GROUP BY REGION, PRODUCT) Q1 " + //$NON-NLS-1$
            "FULL OUTER JOIN " +  //$NON-NLS-1$
            "(SELECT SUM(COSTS) AS C, REGION FROM ORACLE_TABLE WHERE \"YEAR\" = '1999' GROUP BY REGION) Q2 " + //$NON-NLS-1$
            "ON Q1.REGION = Q2.REGION"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql,  
                                      metadata,
                                      null, capFinder,
                                      new String[] {"SELECT REGION, SUM(SALES), PRODUCT FROM db2model.DB2_TABLE WHERE PRODUCT IN ('GUNS', 'TOYS', 'VIDEOTAPES') GROUP BY REGION, PRODUCT", //$NON-NLS-1$
                                                    "SELECT REGION, SUM(COSTS) FROM oraclemodel.Oracle_table WHERE \"YEAR\" = '1999' GROUP BY REGION"},  //$NON-NLS-1$
                                      SHOULD_SUCCEED );

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
    }        

    @Test public void testBusObjQuestion2() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);    
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, true);    
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, true);    
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);    
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_SUM, true);    
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_AVG, true);    
        
        capFinder.addCapabilities("db2model", caps); //$NON-NLS-1$
        capFinder.addCapabilities("oraclemodel", caps); //$NON-NLS-1$
        capFinder.addCapabilities("msmodel", caps); //$NON-NLS-1$
        
        FakeMetadataFacade metadata = FakeMetadataFactory.exampleBusObj();

        String sql = "SELECT SUM(F.SALES), G.REGION, T.YEAR " +  //$NON-NLS-1$
            "FROM SALES F, GEOGRAPHY G, msModel.TIME T " + //$NON-NLS-1$
            "WHERE (F.CITY = G.CITY) AND (F.MONTH = T.MONTH) " +  //$NON-NLS-1$
            "AND G.REGION IN ('BORDEAUX', 'POLINESIA') AND T.YEAR = '1999' " +  //$NON-NLS-1$
            "GROUP BY G.REGION, T.YEAR"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql,  
                                      metadata,
                                      null, capFinder,
                                      new String[] {"SELECT g_0.\"MONTH\" AS c_0, g_0.\"YEAR\" AS c_1 FROM msModel.\"TIME\" AS g_0 WHERE g_0.\"YEAR\" = '1999' ORDER BY c_0", //$NON-NLS-1$
                                                    "SELECT g_0.\"MONTH\" AS c_0, g_0.CITY AS c_1, SUM(g_0.SALES) AS c_2 FROM db2model.SALES AS g_0 WHERE (g_0.\"MONTH\" IN (<dependent values>)) AND (g_0.CITY IN (<dependent values>)) GROUP BY g_0.\"MONTH\", g_0.CITY ORDER BY c_0", //$NON-NLS-1$ 
                                                    "SELECT g_0.CITY, g_0.REGION FROM oraclemodel.GEOGRAPHY AS g_0 WHERE g_0.REGION IN ('BORDEAUX', 'POLINESIA')"},  //$NON-NLS-1$
                                      ComparisonMode.EXACT_COMMAND_STRING );

        checkNodeTypes(plan, new int[] {
                                        2,      // Access
                                        1,      // DependentAccess
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

    @Test public void testBusObjQuestion2Hint() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);    
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, true);    
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, true);    
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);    
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_SUM, true);    
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_AVG, true);    
        
        capFinder.addCapabilities("db2model", caps); //$NON-NLS-1$
        capFinder.addCapabilities("oraclemodel", caps); //$NON-NLS-1$
        capFinder.addCapabilities("msmodel", caps); //$NON-NLS-1$
        
        FakeMetadataFacade metadata = FakeMetadataFactory.exampleBusObj();

        String sql = "SELECT SUM(F.SALES), G.REGION, T.YEAR " +  //$NON-NLS-1$
            "FROM SALES F MAKEDEP, GEOGRAPHY G, msModel.TIME T " + //$NON-NLS-1$
            "WHERE (F.CITY = G.CITY) AND (F.MONTH = T.MONTH) " +  //$NON-NLS-1$
            "AND G.REGION IN ('BORDEAUX', 'POLINESIA') AND T.YEAR = '1999' " +  //$NON-NLS-1$
            "GROUP BY G.REGION, T.YEAR"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql,  
                                      metadata,
                                      null, capFinder,
                                      new String[] {"SELECT g_0.\"MONTH\" AS c_0, g_0.\"YEAR\" AS c_1 FROM msModel.\"TIME\" AS g_0 WHERE g_0.\"YEAR\" = '1999' ORDER BY c_0", //$NON-NLS-1$
                                          "SELECT g_0.\"MONTH\" AS c_0, g_0.CITY AS c_1, SUM(g_0.SALES) AS c_2 FROM db2model.SALES AS g_0 WHERE (g_0.\"MONTH\" IN (<dependent values>)) AND (g_0.CITY IN (<dependent values>)) GROUP BY g_0.\"MONTH\", g_0.CITY ORDER BY c_0", //$NON-NLS-1$ 
                                          "SELECT g_0.CITY, g_0.REGION FROM oraclemodel.GEOGRAPHY AS g_0 WHERE g_0.REGION IN ('BORDEAUX', 'POLINESIA')"},  //$NON-NLS-1$
                                      ComparisonMode.EXACT_COMMAND_STRING );

        checkNodeTypes(plan, new int[] {
                                        2,      // Access
                                        1,      // DependentAccess
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

    @Test public void testBusObjQuestion2HintVariation() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);    
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, true);    
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, true);    
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);    
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_SUM, true);    
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_AVG, true);    
        
        capFinder.addCapabilities("db2model", caps); //$NON-NLS-1$
        capFinder.addCapabilities("oraclemodel", caps); //$NON-NLS-1$
        capFinder.addCapabilities("msmodel", caps); //$NON-NLS-1$
        
        FakeMetadataFacade metadata = FakeMetadataFactory.exampleBusObj();

        String sql = "SELECT SUM(F.SALES), G.REGION, T.YEAR " +  //$NON-NLS-1$
            "FROM SALES F MAKEDEP, GEOGRAPHY2 G, msModel.TIME T " + //$NON-NLS-1$
            "WHERE (F.CITY = G.CITY) AND (F.MONTH = T.MONTH) " +  //$NON-NLS-1$
            "AND G.REGION IN ('BORDEAUX', 'POLINESIA') AND T.YEAR = '1999' " +  //$NON-NLS-1$
            "GROUP BY G.REGION, T.YEAR"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql,  
                                      metadata,
                                      null, capFinder,
                                      new String[] {"SELECT g_0.\"MONTH\" AS c_0, g_1.REGION AS c_1, SUM(g_0.SALES) AS c_2 FROM db2model.SALES AS g_0, db2model.GEOGRAPHY2 AS g_1 WHERE (g_0.CITY = g_1.CITY) AND (g_1.REGION IN ('BORDEAUX', 'POLINESIA')) AND (g_0.\"MONTH\" IN (<dependent values>)) GROUP BY g_0.\"MONTH\", g_1.REGION ORDER BY c_0", //$NON-NLS-1$ 
                                                    "SELECT g_0.\"MONTH\" AS c_0, g_0.\"YEAR\" AS c_1 FROM msModel.\"TIME\" AS g_0 WHERE g_0.\"YEAR\" = '1999' ORDER BY c_0"},  //$NON-NLS-1$
                                                    ComparisonMode.EXACT_COMMAND_STRING );

        checkNodeTypes(plan, new int[] {
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

    @Test public void testBusObjQuestion3() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);    
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, true);    
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, true);    
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);    
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_SUM, true);    
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_AVG, true);    
        
        capFinder.addCapabilities("db2model", caps); //$NON-NLS-1$
        capFinder.addCapabilities("oraclemodel", caps); //$NON-NLS-1$
        capFinder.addCapabilities("msmodel", caps); //$NON-NLS-1$
        
        FakeMetadataFacade metadata = FakeMetadataFactory.exampleBusObj();

        String sql = "select sum(c0), sum(b0), c1, b2 FROM db2Table, OraTable where c2=b2 group by c1, b2"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql,  
                                      metadata,
                                      null, capFinder,
                                      new String[] {"SELECT c2, c1, c0 FROM db2model.DB2TABLE", //$NON-NLS-1$
                                                    "SELECT DISTINCT b2, sum(b0) FROM oraclemodel.OraTable GROUP BY b2 ORDER BY b2"},  //$NON-NLS-1$
                                      SHOULD_SUCCEED );

        checkNodeTypes(plan, new int[] {
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
                                        2,      // Project
                                        0,      // Select
                                        0,      // Sort
                                        0       // UnionAll
                                    });
    }
    
    @Test public void testPushDownOverUnion() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getAggregateCapabilities();
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        ProcessorPlan plan = TestOptimizer.helpPlan("select e1, max(e2) from (select e1, e2 from pm1.g1 union all select e1, e2 from pm1.g2) y group by e1", FakeMetadataFactory.example1Cached(), null, capFinder,  //$NON-NLS-1$
            new String[]{"SELECT v_0.c_0, MAX(v_0.c_1) FROM (SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm1.g1 AS g_0) AS v_0 GROUP BY v_0.c_0", //$NON-NLS-1$
        	"SELECT v_0.c_0, MAX(v_0.c_1) FROM (SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm1.g2 AS g_0) AS v_0 GROUP BY v_0.c_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
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
            1       // UnionAll
        }); 
    }
    
    @Test public void testPushDownOverUnion1() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getAggregateCapabilities();
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        ProcessorPlan plan = TestOptimizer.helpPlan("select max(e2) from (select e1, e2 from pm1.g1 union all select e1, e2 from pm1.g2) z", FakeMetadataFactory.example1Cached(), null, capFinder,  //$NON-NLS-1$
            new String[]{"SELECT MAX(v_0.c_0) FROM (SELECT g_0.e2 AS c_0 FROM pm1.g2 AS g_0) AS v_0 HAVING COUNT(*) > 0", //$NON-NLS-1$
        	"SELECT MAX(v_0.c_0) FROM (SELECT g_0.e2 AS c_0 FROM pm1.g1 AS g_0) AS v_0 HAVING COUNT(*) > 0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
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
            1       // UnionAll
        }); 
    }
    
    /**
     * We won't do the pushdown here since the aggregate depends upon the cardinality
     */
    @Test public void testPushDownOverUnion2() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getAggregateCapabilities();
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        ProcessorPlan plan = TestOptimizer.helpPlan("select count(e2) from (select e1, e2 from pm1.g1 union select e1, e2 from pm1.g2) z", FakeMetadataFactory.example1Cached(), null, capFinder,  //$NON-NLS-1$
            new String[]{"SELECT g_0.e1, g_0.e2 FROM pm1.g2 AS g_0", //$NON-NLS-1$
        	"SELECT g_0.e1, g_0.e2 FROM pm1.g1 AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            1,      // DupRemove
            1,      // Grouping
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            1       // UnionAll
        }); 
    }
    
    @Test public void testPushDownOverUnionMixed() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getAggregateCapabilities();
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", TestOptimizer.getTypicalCapabilities()); //$NON-NLS-1$
        
        ProcessorPlan plan = TestOptimizer.helpPlan("select max(e2), count(*) from (select e1, e2 from pm1.g1 union all select e1, e2 from pm2.g2) z", FakeMetadataFactory.example1Cached(), null, capFinder,  //$NON-NLS-1$
            new String[]{"SELECT MAX(v_0.c_0), COUNT(*) FROM (SELECT g_0.e2 AS c_0 FROM pm1.g1 AS g_0) AS v_0 HAVING COUNT(*) > 0", //$NON-NLS-1$
        	"SELECT g_0.e2 FROM pm2.g2 AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            1,      // Grouping
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            2,      // Project
            0,      // Select
            0,      // Sort
            1       // UnionAll
        }); 
    }
    
    /**
     * Ensures that we do not raise criteria over a group by
     * TODO: check if the criteria only depends on grouping columns
     */
    @Test public void testForCase836073GroupBy() throws Exception {
        String sql = "select count(*) from bqt1.smallb where formatdate(bqt1.smallb.DateValue,'yyyyMM') = '200309'"; 
        
        // Plan query
        ProcessorPlan plan = TestOptimizer.helpPlan(sql,  
                FakeMetadataFactory.exampleBQTCached(),
                null, getAggregatesFinder(),
                new String[] {"SELECT g_0.DateValue FROM bqt1.smallb AS g_0"},  
                              TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING );

        TestOptimizer.checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            1,      // Grouping
            0,      // Join
            0,      // MergeJoin
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            1,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }
        
}
