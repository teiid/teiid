/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.query.optimizer;

import static org.teiid.query.optimizer.TestOptimizer.SHOULD_SUCCEED;
import static org.teiid.query.optimizer.TestOptimizer.checkNodeTypes;
import static org.teiid.query.optimizer.TestOptimizer.getTypicalCapabilities;
import static org.teiid.query.optimizer.TestOptimizer.helpPlan;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.teiid.query.function.FunctionTree;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.TestOptimizer.ComparisonMode;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.FakeCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.processor.HardcodedDataManager;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.TestProcessor;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.util.CommandContext;
import org.teiid.translator.SourceSystemFunctions;

@SuppressWarnings({"nls", "unchecked"})
public class TestAggregatePushdown {

    public static BasicSourceCapabilities getAggregateCapabilities() {
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_INLINE_VIEWS, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_SUM, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MIN, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_AVG, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT_BIG, true);
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
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(), null, capFinder,
                                      new String[] {"SELECT SUM(g_0.IntNum) FROM BQT1.SmallA AS g_0 WHERE g_0.StringKey = '0'", "SELECT g_0.IntKey FROM BQT2.SmallA AS g_0 WHERE g_0.StringKey = '0'"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$ //$NON-NLS-2$

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
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(), null, capFinder,
                                      new String[] {"SELECT g_0.StringKey, g_0.IntKey, g_0.IntNum FROM BQT2.SmallA AS g_0 GROUP BY g_0.StringKey, g_0.IntKey, g_0.IntNum", "SELECT g_0.StringKey, g_0.IntNum FROM BQT1.SmallA AS g_0"}, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$

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
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(), null, capFinder,
                                      new String[] {"SELECT g_0.StringKey, g_0.IntKey, g_0.IntNum FROM BQT2.SmallA AS g_0", "SELECT g_0.StringKey, g_0.IntNum FROM BQT1.SmallA AS g_0"}, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$

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

        String sql = "SELECT max(y.e2) from pm1.g1 x, pm2.g1 y where x.e4 = y.e4 group by y.e4"; //$NON-NLS-1$
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), null, capFinder,
                                      new String[] {"SELECT g_0.e4 FROM pm1.g1 AS g_0", "SELECT g_0.e4, MAX(g_0.e2) FROM pm2.g1 AS g_0 GROUP BY g_0.e4"}, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$

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

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(), null, capFinder,
                                      new String[] {"SELECT g_0.IntKey FROM BQT2.SmallB AS g_0", "SELECT g_0.IntKey, SUM(g_0.IntNum), SUM(g_0.FloatNum) FROM BQT1.SmallA AS g_0 GROUP BY g_0.IntKey"}, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$

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
        QueryMetadataInterface metadata = RealMetadataFactory.exampleAggregatesCached();
        String sql = "SELECT o_dealerid, o_productid, sum(o_amount) FROM m1.order, m1.dealer, m2.product " +  //$NON-NLS-1$
            "WHERE o_dealerid=d_dealerid AND o_productid=p_productid AND d_state = 'CA' AND p_divid = 100 " +  //$NON-NLS-1$
            "GROUP BY o_dealerid, o_productid"; //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan(sql,
                                      metadata,
                                      null, getAggregatesFinder(),
                                      new String[] {"SELECT g_0.O_ProductID, g_0.O_DealerID, SUM(g_0.O_Amount) FROM m1.\"order\" AS g_0, m1.dealer AS g_1 WHERE (g_0.O_DealerID = g_1.D_DealerID) AND (g_1.D_State = 'CA') AND (g_0.O_ProductID IN (<dependent values>)) GROUP BY g_0.O_ProductID, g_0.O_DealerID", "SELECT g_0.P_ProductID AS c_0 FROM m2.product AS g_0 WHERE g_0.P_DivID = 100 ORDER BY c_0"},  //$NON-NLS-1$ //$NON-NLS-2$
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
        QueryMetadataInterface metadata = RealMetadataFactory.exampleAggregatesCached();
        String sql = "SELECT o_dealerid, o_productid, sum(o_amount) FROM m1.order, m1.dealer, m2.product " +  //$NON-NLS-1$
            "WHERE o_dealerid=d_dealerid AND o_productid=p_productid AND d_state = 'CA' AND p_divid = 100 " +  //$NON-NLS-1$
            "GROUP BY o_dealerid, o_productid having max(o_amount) < 100"; //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan(sql,
                                      metadata,
                                      null, getAggregatesFinder(),
                                      new String[] {"SELECT g_0.P_ProductID AS c_0 FROM m2.product AS g_0 WHERE g_0.P_DivID = 100 ORDER BY c_0", "SELECT g_0.O_ProductID, g_0.O_DealerID, MAX(g_0.O_Amount), SUM(g_0.O_Amount) FROM m1.\"order\" AS g_0, m1.dealer AS g_1 WHERE (g_0.O_DealerID = g_1.D_DealerID) AND (g_1.D_State = 'CA') AND (g_0.O_ProductID IN (<dependent values>)) GROUP BY g_0.O_ProductID, g_0.O_DealerID"},  //$NON-NLS-1$ //$NON-NLS-2$
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

        String sql = "SELECT avg(y.e2) from pm1.g1 x, pm2.g1 y where x.e4 = y.e4 group by x.e2, y.e1"; //$NON-NLS-1$
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), null, capFinder,
                                      new String[] {"SELECT g_0.e4, g_0.e2 FROM pm1.g1 AS g_0", "SELECT g_0.e4, g_0.e1, SUM(g_0.e2), COUNT(g_0.e2) FROM pm2.g1 AS g_0 GROUP BY g_0.e4, g_0.e1"}, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$

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

    @Test public void testAvgAggregateFiltered() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getAggregateCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$

        String sql = "SELECT avg(y.e2) filter (where y.e1 = 1) from pm1.g1 x, pm2.g1 y where x.e4 = y.e4 group by x.e2, y.e1"; //$NON-NLS-1$
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), null, capFinder,
                                      new String[] {"SELECT g_0.e4, g_0.e2 FROM pm1.g1 AS g_0",
            "SELECT g_0.e4, g_0.e1, g_0.e2 FROM pm2.g1 AS g_0"}, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$

        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            2,      // Grouping
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
     * Agg can only be computed after the join
     */
    @Test public void testAvgAggregateFiltered1() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getAggregateCapabilities();
        caps.setCapabilitySupport(Capability.ADVANCED_OLAP, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$

        String sql = "SELECT avg(y.e2) filter (where x.e1 = 1) from pm1.g1 x, pm2.g1 y where x.e4 = y.e4 group by x.e2, y.e1"; //$NON-NLS-1$
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), null, capFinder,
                new String[] {"SELECT g_0.e4, g_0.e2, g_0.e1 FROM pm1.g1 AS g_0",
        "SELECT g_0.e4, g_0.e1, g_0.e2 FROM pm2.g1 AS g_0"}, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$

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

        String sql = "SELECT stddev_pop(y.e2) from pm1.g1 x, pm2.g1 y where x.e4 = y.e4 group by x.e2, y.e1"; //$NON-NLS-1$
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), null, capFinder,
                                      new String[] {"SELECT g_0.e4 AS c_0, g_0.e1 AS c_1, COUNT(g_0.e2) AS c_2, SUM(power(g_0.e2, 2)) AS c_3, SUM(g_0.e2) AS c_4 FROM pm2.g1 AS g_0 GROUP BY g_0.e4, g_0.e1 ORDER BY c_0", "SELECT g_0.e4 AS c_0, g_0.e2 AS c_1 FROM pm1.g1 AS g_0 ORDER BY c_0"}, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$

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

        String sql = "SELECT count(y.e2) from pm1.g1 x, pm2.g1 y where x.e4 = y.e4 group by x.e2, y.e1"; //$NON-NLS-1$
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), null, capFinder,
                                      new String[] {"SELECT g_0.e4, g_0.e2 FROM pm1.g1 AS g_0", "SELECT g_0.e4, g_0.e1, COUNT(g_0.e2) FROM pm2.g1 AS g_0 GROUP BY g_0.e4, g_0.e1"}, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$

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

    @Test public void testOuterJoinPushdown() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getAggregateCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$

        String sql = "SELECT count(y.e2) from pm1.g1 x left outer join pm2.g1 y on x.e4 = y.e4 group by x.e2, y.e1"; //$NON-NLS-1$
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), null, capFinder,
                                      new String[] {"SELECT g_0.e4, g_0.e2 FROM pm1.g1 AS g_0", "SELECT g_0.e4, g_0.e1, COUNT(g_0.e2) FROM pm2.g1 AS g_0 GROUP BY g_0.e4, g_0.e1"}, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$

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

    @Test public void testOuterJoinPushdown1() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getAggregateCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$

        String sql = "SELECT count(x.e1) from pm1.g1 x left outer join pm2.g1 y on x.e4 = y.e4 group by x.e2, y.e1"; //$NON-NLS-1$
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), null, capFinder,
                                      new String[] {"SELECT g_0.e4, g_0.e2, COUNT(g_0.e1) FROM pm1.g1 AS g_0 GROUP BY g_0.e4, g_0.e2", "SELECT g_0.e4, g_0.e1 FROM pm2.g1 AS g_0"}, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$

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

    @Test public void testOuterJoinPushdown2() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getAggregateCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$

        String sql = "SELECT x.e2, y.e1, count(*) from pm1.g1 x full outer join pm2.g1 y on x.e3 = y.e3 group by x.e2, y.e1"; //$NON-NLS-1$
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), null, capFinder,
                                      new String[] {"SELECT g_0.e3, g_0.e2, COUNT(*) FROM pm1.g1 AS g_0 GROUP BY g_0.e3, g_0.e2", "SELECT g_0.e3, g_0.e1 FROM pm2.g1 AS g_0"}, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$

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

        HardcodedDataManager hdm = new HardcodedDataManager();

        hdm.addData("SELECT g_0.e3, g_0.e2, COUNT(*) FROM pm1.g1 AS g_0 GROUP BY g_0.e3, g_0.e2", Arrays.asList(true, 1, 3), Arrays.asList(true, 2, 2), Arrays.asList(null, 3, 4));
        hdm.addData("SELECT g_0.e3, g_0.e1 FROM pm2.g1 AS g_0", Arrays.asList(false, "a"), Arrays.asList(true, "b"), Arrays.asList(true, "b"));

        TestProcessor.helpProcess(plan, hdm, new List[] {Arrays.asList(null, "a", 1), Arrays.asList(1, "b", 6), Arrays.asList(2, "b", 4), Arrays.asList(3, null, 4)});

        sql = "SELECT x.e2, y.e1, count(1) from pm1.g1 x full outer join pm2.g1 y on x.e3 = y.e3 group by x.e2, y.e1"; //$NON-NLS-1$
        plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), null, capFinder,
                                      new String[] {"SELECT g_0.e3, g_0.e2, COUNT(1) FROM pm1.g1 AS g_0 GROUP BY g_0.e3, g_0.e2", "SELECT g_0.e3, g_0.e1 FROM pm2.g1 AS g_0"}, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$

        hdm.addData("SELECT g_0.e3, g_0.e2, COUNT(1) FROM pm1.g1 AS g_0 GROUP BY g_0.e3, g_0.e2", Arrays.asList(true, 1, 3), Arrays.asList(true, 2, 2), Arrays.asList(null, 3, 4));
        TestProcessor.helpProcess(plan, hdm, new List[] {Arrays.asList(null, "a", 1), Arrays.asList(1, "b", 6), Arrays.asList(2, "b", 4), Arrays.asList(3, null, 4)});
    }

    /**
     * Here we'll prevent pushdown since the agg expression is dependent upon nulls
     * @throws Exception
     */
    @Test public void testOuterJoinPushdown3() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getAggregateCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, false);
        caps.setFunctionSupport("ifnull", true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$

        String sql = "SELECT x.e2, y.e1, count(ifnull(y.e4, 1)), count(x.e4) from pm1.g1 x full outer join pm2.g1 y on x.e3 = y.e3 group by x.e2, y.e1"; //$NON-NLS-1$
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), null, capFinder,
                                      new String[] {"SELECT g_0.e3, g_0.e2, g_0.e4 FROM pm1.g1 AS g_0", "SELECT g_0.e3, g_0.e1, g_0.e4 FROM pm2.g1 AS g_0"}, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$

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
        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();

        ProcessorPlan plan = TestOptimizer.helpPlan(
              "select count(*), a.intnum from bqt1.smalla as a, bqt2.smallb as b where a.intkey = b.intkey group by a.intnum",  //$NON-NLS-1$
              metadata, null, capFinder,
              new String[] {
                "SELECT g_0.IntKey, g_0.IntNum, COUNT(*) FROM BQT1.SmallA AS g_0 GROUP BY g_0.IntKey, g_0.IntNum", "SELECT g_0.IntKey FROM BQT2.SmallB AS g_0"},  //$NON-NLS-1$ //$NON-NLS-2$
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

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(), null, capFinder,
                                      new String[] {"SELECT g_0.IntKey FROM BQT2.SmallB AS g_0 WHERE g_0.IntNum = 10", "SELECT g_0.StringKey FROM BQT2.SmallA AS g_0", "SELECT g_0.StringKey, g_0.IntKey, SUM(g_0.IntNum) FROM BQT1.SmallA AS g_0 GROUP BY g_0.StringKey, g_0.IntKey"}, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

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
        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();

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
        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();

        ProcessorPlan plan = TestOptimizer.helpPlan(
              "select stringkey, max(intkey) from bqt1.smalla group by stringkey having count(intkey) = 1",  //$NON-NLS-1$
              metadata, null, capFinder,
              new String[] {
                "SELECT COUNT(g_0.IntKey), g_0.StringKey, MAX(g_0.IntKey) FROM BQT1.SmallA AS g_0 GROUP BY g_0.StringKey"},  //$NON-NLS-1$
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

        ProcessorPlan plan = TestOptimizer.helpPlan("select X.e1 FROM vm1.g1 X group by X.e1 having X.e1 = 1 and sum(X.e2) = 2", RealMetadataFactory.example1Cached(), null, capFinder,  //$NON-NLS-1$
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

        QueryMetadataInterface metadata = RealMetadataFactory.exampleBusObj();

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

        QueryMetadataInterface metadata = RealMetadataFactory.exampleBusObj();

        String sql = "SELECT SUM(F.SALES), G.REGION, T.YEAR " +  //$NON-NLS-1$
            "FROM SALES F, GEOGRAPHY G, msModel.TIME T " + //$NON-NLS-1$
            "WHERE (F.CITY = G.CITY) AND (F.MONTH = T.MONTH) " +  //$NON-NLS-1$
            "AND G.REGION IN ('BORDEAUX', 'POLINESIA') AND T.YEAR = '1999' " +  //$NON-NLS-1$
            "GROUP BY G.REGION, T.YEAR"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql,
                                      metadata,
                                      null, capFinder,
                                      new String[] {"SELECT g_0.\"MONTH\" AS c_0, g_0.\"YEAR\" AS c_1 FROM msmodel.\"TIME\" AS g_0 WHERE g_0.\"YEAR\" = '1999' ORDER BY c_0",
                "SELECT g_0.\"MONTH\", g_0.CITY, SUM(g_0.SALES) FROM db2model.SALES AS g_0 WHERE (g_0.\"MONTH\" IN (<dependent values>)) AND (g_0.CITY IN (<dependent values>)) GROUP BY g_0.\"MONTH\", g_0.CITY",
                "SELECT g_0.CITY AS c_0, g_0.REGION AS c_1 FROM oraclemodel.GEOGRAPHY AS g_0 WHERE g_0.REGION IN ('BORDEAUX', 'POLINESIA') ORDER BY c_0"},  //$NON-NLS-1$
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

        QueryMetadataInterface metadata = RealMetadataFactory.exampleBusObj();

        String sql = "SELECT SUM(F.SALES), G.REGION, T.YEAR " +  //$NON-NLS-1$
            "FROM SALES F MAKEDEP, GEOGRAPHY G, msModel.TIME T " + //$NON-NLS-1$
            "WHERE (F.CITY = G.CITY) AND (F.MONTH = T.MONTH) " +  //$NON-NLS-1$
            "AND G.REGION IN ('BORDEAUX', 'POLINESIA') AND T.YEAR = '1999' " +  //$NON-NLS-1$
            "GROUP BY G.REGION, T.YEAR"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql,
                                      metadata,
                                      null, capFinder,
                                      new String[] {"SELECT g_0.\"MONTH\" AS c_0, g_0.\"YEAR\" AS c_1 FROM msmodel.\"TIME\" AS g_0 WHERE g_0.\"YEAR\" = '1999' ORDER BY c_0",
                "SELECT g_0.\"MONTH\", g_0.CITY, SUM(g_0.SALES) FROM db2model.SALES AS g_0 WHERE (g_0.\"MONTH\" IN (<dependent values>)) AND (g_0.CITY IN (<dependent values>)) GROUP BY g_0.\"MONTH\", g_0.CITY",
                "SELECT g_0.CITY AS c_0, g_0.REGION AS c_1 FROM oraclemodel.GEOGRAPHY AS g_0 WHERE g_0.REGION IN ('BORDEAUX', 'POLINESIA') ORDER BY c_0"},  //$NON-NLS-1$
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

        QueryMetadataInterface metadata = RealMetadataFactory.exampleBusObj();

        String sql = "SELECT SUM(F.SALES), G.REGION, T.YEAR " +  //$NON-NLS-1$
            "FROM SALES F MAKEDEP, GEOGRAPHY2 G, msModel.TIME T " + //$NON-NLS-1$
            "WHERE (F.CITY = G.CITY) AND (F.MONTH = T.MONTH) " +  //$NON-NLS-1$
            "AND G.REGION IN ('BORDEAUX', 'POLINESIA') AND T.YEAR = '1999' " +  //$NON-NLS-1$
            "GROUP BY G.REGION, T.YEAR"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql,
                                      metadata,
                                      null, capFinder,
                                      new String[] {"SELECT g_0.\"MONTH\" AS c_0, g_0.\"YEAR\" AS c_1 FROM msmodel.\"TIME\" AS g_0 WHERE g_0.\"YEAR\" = '1999' ORDER BY c_0",
                "SELECT g_0.\"MONTH\", g_1.REGION, SUM(g_0.SALES) FROM db2model.SALES AS g_0, db2model.GEOGRAPHY2 AS g_1 WHERE (g_0.CITY = g_1.CITY) AND (g_1.REGION IN ('BORDEAUX', 'POLINESIA')) AND (g_0.\"MONTH\" IN (<dependent values>)) GROUP BY g_0.\"MONTH\", g_1.REGION"},  //$NON-NLS-1$
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

        QueryMetadataInterface metadata = RealMetadataFactory.exampleBusObj();

        String sql = "select sum(c0), sum(b0), c1, b2 FROM db2Table, OraTable where c2=b2 group by c1, b2"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql,
                                      metadata,
                                      null, capFinder,
                                      new String[] {"SELECT g_0.c2, g_0.c1, g_0.c0 FROM db2model.DB2TABLE AS g_0", //$NON-NLS-1$
                                                    "SELECT g_0.b2 AS c_0, g_0.b0 AS c_1 FROM oraclemodel.OraTable AS g_0 ORDER BY c_0"},  //$NON-NLS-1$
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

        ProcessorPlan plan = TestOptimizer.helpPlan("select e1, max(e2) from (select e1, e2 from pm1.g1 union all select e1, e2 from pm1.g2) y group by e1", RealMetadataFactory.example1Cached(), null, capFinder,  //$NON-NLS-1$
            new String[]{"SELECT g_0.e1, MAX(g_0.e2) FROM pm1.g1 AS g_0 GROUP BY g_0.e1", "SELECT g_0.e1, MAX(g_0.e2) FROM pm1.g2 AS g_0 GROUP BY g_0.e1"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
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
     * olap not supported
     */
    @Test public void testPushDownOverUnionFiltered() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getAggregateCapabilities();
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan("select e1, count(*) filter (where e3) from (select e1, e2, e3 from pm1.g1 union all select e1, e2, e3 from pm1.g2) y group by e1", RealMetadataFactory.example1Cached(), null, capFinder,  //$NON-NLS-1$
            new String[]{"SELECT g_0.e1, g_0.e3 FROM pm1.g1 AS g_0", "SELECT g_0.e1, g_0.e3 FROM pm1.g2 AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
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

    @Test public void testPushDownOverUnionFiltered1() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getAggregateCapabilities();
        caps.setCapabilitySupport(Capability.ADVANCED_OLAP, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan("select e1, count(*) filter (where e3) from (select e1, e2, e3 from pm1.g1 union all select e1, e2, e3 from pm1.g2) y group by e1", RealMetadataFactory.example1Cached(), null, capFinder,  //$NON-NLS-1$
            new String[]{"SELECT g_0.e1, COUNT(*) FILTER(WHERE g_0.e3 = TRUE) FROM pm1.g1 AS g_0 GROUP BY g_0.e1", "SELECT g_0.e1, COUNT(*) FILTER(WHERE g_0.e3 = TRUE) FROM pm1.g2 AS g_0 GROUP BY g_0.e1"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
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
     * rand pushdown not supported
     */
    @Test public void testPushDownOverUnionFiltered3() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getAggregateCapabilities();
        caps.setCapabilitySupport(Capability.ADVANCED_OLAP, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan("select e1, count(*) filter (where e3 > rand()) from (select e1, e2, e3 from pm1.g1 union all select e1, e2, e3 from pm1.g2) y group by e1", RealMetadataFactory.example1Cached(), null, capFinder,  //$NON-NLS-1$
                new String[]{"SELECT g_0.e1, g_0.e3 FROM pm1.g1 AS g_0", "SELECT g_0.e1, g_0.e3 FROM pm1.g2 AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
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

        ProcessorPlan plan = TestOptimizer.helpPlan("select max(e2) from (select e1, e2 from pm1.g1 union all select e1, e2 from pm1.g2) z", RealMetadataFactory.example1Cached(), null, capFinder,  //$NON-NLS-1$
            new String[]{"SELECT MAX(g_0.e2) FROM pm1.g2 AS g_0", "SELECT MAX(g_0.e2) FROM pm1.g1 AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
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

        ProcessorPlan plan = TestOptimizer.helpPlan("select count(e2) from (select e1, e2 from pm1.g1 union select e1, e2 from pm1.g2) z", RealMetadataFactory.example1Cached(), null, capFinder,  //$NON-NLS-1$
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

        ProcessorPlan plan = TestOptimizer.helpPlan("select max(e2), count(*) from (select e1, e2 from pm1.g1 union all select e1, e2 from pm2.g2) z", RealMetadataFactory.example1Cached(), null, capFinder,  //$NON-NLS-1$
            new String[]{"SELECT g_0.e2 FROM pm2.g2 AS g_0", "SELECT MAX(g_0.e2), COUNT(*) FROM pm1.g1 AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
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
     * pushdown won't happen since searched case is not supported
     */
    @Test public void testPushDownOverUnionGroupingExpression() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getAggregateCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SEARCHED_CASE, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", getAggregateCapabilities()); //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan("select max(e2), case when e1 is null then 0 else 1 end from (select e1, e2 from pm1.g1 union all select e1, e2 from pm2.g2) z group by case when e1 is null then 0 else 1 end", RealMetadataFactory.example1Cached(), null, capFinder,  //$NON-NLS-1$
            new String[]{"SELECT g_0.e1, g_0.e2 FROM pm2.g2 AS g_0", "SELECT v_0.c_0, MAX(v_0.c_1) FROM (SELECT CASE WHEN g_0.e1 IS NULL THEN 0 ELSE 1 END AS c_0, g_0.e2 AS c_1 FROM pm1.g1 AS g_0) AS v_0 GROUP BY v_0.c_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
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

    @Test public void testPushDownOverUnionGroupingExpressionPartitioned() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getAggregateCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SEARCHED_CASE, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan("select max(e2), case when e1 is null then 0 else 1 end from (select e1, e2, 1 as part from pm1.g1 union all select e1, e2, 2 as part from pm1.g2) z group by case when e1 is null then 0 else 1 end, part", RealMetadataFactory.example1Cached(), null, capFinder,  //$NON-NLS-1$
            new String[]{"SELECT MAX(v_0.c_2), v_0.c_0 FROM (SELECT CASE WHEN g_0.e1 IS NULL THEN 0 ELSE 1 END AS c_0, 1 AS c_1, g_0.e2 AS c_2 FROM pm1.g1 AS g_0) AS v_0 GROUP BY v_0.c_0, v_0.c_1", "SELECT MAX(v_0.c_2), v_0.c_0 FROM (SELECT CASE WHEN g_0.e1 IS NULL THEN 0 ELSE 1 END AS c_0, 2 AS c_1, g_0.e2 AS c_2 FROM pm1.g2 AS g_0) AS v_0 GROUP BY v_0.c_0, v_0.c_1"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
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
                RealMetadataFactory.exampleBQTCached(),
                null, getAggregatesFinder(),
                new String[] {"SELECT g_0.DateValue FROM BQT1.SmallB AS g_0"},
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

    @Test public void testSingleTableRestriction() throws Exception {
        String sql = "select count(*) from bqt1.smallb, bqt1.smalla";
        BasicSourceCapabilities bsc = getAggregateCapabilities();
        bsc.setCapabilitySupport(Capability.QUERY_ONLY_SINGLE_TABLE_GROUP_BY, true);
        // Plan query
        ProcessorPlan plan = TestOptimizer.helpPlan(sql,
                RealMetadataFactory.exampleBQTCached(),
                null, new DefaultCapabilitiesFinder(bsc),
                new String[] {"SELECT 1 FROM BQT1.SmallB AS g_0, BQT1.SmallA AS g_1"},
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
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    @Test public void testPushDownOverUnionGroupingWithCount() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getAggregateCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SEARCHED_CASE, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan("select count(*) from (select e1, e2, 1 as part from pm1.g1 union all select e1, e2, 2 as part from pm1.g2) z group by e1", RealMetadataFactory.example1Cached(), null, capFinder,  //$NON-NLS-1$
            new String[]{"SELECT g_0.e1, COUNT(*) FROM pm1.g1 AS g_0 GROUP BY g_0.e1", "SELECT g_0.e1, COUNT(*) FROM pm1.g2 AS g_0 GROUP BY g_0.e1"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
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
     * ensure the agg is not decomposed as we do not have rollup compensation
     */
    @Test public void testUnionGroupingWithRollup() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getAggregateCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SEARCHED_CASE, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan("select count(*) from (select e1, e2, 1 as part from pm1.g1 union all select e1, e2, 2 as part from pm1.g2) z group by rollup(e1)", RealMetadataFactory.example1Cached(), null, capFinder,  //$NON-NLS-1$
            new String[]{"SELECT g_0.e1 FROM pm1.g2 AS g_0", "SELECT g_0.e1 FROM pm1.g1 AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    }

    @Test public void testRollupPushdown() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getAggregateCapabilities();
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan("select max(e1), e2 from pm1.g2 group by rollup(e2)", RealMetadataFactory.example1Cached(), null, capFinder,  //$NON-NLS-1$
            new String[]{"SELECT g_0.e2, g_0.e1 FROM pm1.g2 AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
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

        caps.setCapabilitySupport(Capability.QUERY_GROUP_BY_ROLLUP, true);
        plan = TestOptimizer.helpPlan("select max(e1), e2 from pm1.g2 group by rollup(e2)", RealMetadataFactory.example1Cached(), null, capFinder,  //$NON-NLS-1$
                new String[]{"SELECT MAX(g_0.e1), g_0.e2 FROM pm1.g2 AS g_0 GROUP BY ROLLUP(g_0.e2)"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    @Test public void testRollupOrderByPushdown() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getAggregateCapabilities();
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        caps.setCapabilitySupport(Capability.QUERY_GROUP_BY_ROLLUP, true);

        ProcessorPlan plan = TestOptimizer.helpPlan("select max(e1), e2 from pm1.g2 group by rollup(e2) order by e2", RealMetadataFactory.example1Cached(), null, capFinder,  //$NON-NLS-1$
            new String[]{"SELECT MAX(v_0.c_1) AS c_0, v_0.c_0 AS c_1 FROM (SELECT g_0.e2 AS c_0, g_0.e1 AS c_1 FROM pm1.g2 AS g_0 GROUP BY ROLLUP(g_0.e2)) AS v_0 ORDER BY c_1"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);

        caps.setCapabilitySupport(Capability.QUERY_ORDERBY_EXTENDED_GROUPING, true);

        plan = TestOptimizer.helpPlan("select max(e1), e2 from pm1.g2 group by rollup(e2) order by e2", RealMetadataFactory.example1Cached(), null, capFinder,  //$NON-NLS-1$
                new String[]{"SELECT MAX(g_0.e1) AS c_0, g_0.e2 AS c_1 FROM pm1.g2 AS g_0 GROUP BY ROLLUP(g_0.e2) ORDER BY c_1"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    /**
     * Note the pre/post affect of the having in the source query
     */
    @Test public void testRollupHaving() throws Exception {
        String sql = "select e1, sum(e2) from pm1.g1 group by rollup(e1) having e1 is not null"; //$NON-NLS-1$
        BasicSourceCapabilities caps = getAggregateCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_GROUP_BY_ROLLUP, true);
        helpPlan(sql, RealMetadataFactory.example1Cached(), new String[] {"SELECT g_0.e1, SUM(g_0.e2) FROM pm1.g1 AS g_0 WHERE g_0.e1 IS NOT NULL GROUP BY ROLLUP(g_0.e1) HAVING g_0.e1 IS NOT NULL"}, new DefaultCapabilitiesFinder(caps), ComparisonMode.EXACT_COMMAND_STRING);
    }

    @Test public void testStringAggPushdown() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getAggregateCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_STRING, true);
        caps.setCapabilitySupport(Capability.WINDOW_FUNCTION_ORDER_BY_AGGREGATES, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan("select string_agg(e1, ',') from pm1.g2 group by e1", RealMetadataFactory.example1Cached(), null, capFinder,  //$NON-NLS-1$
            new String[]{"SELECT string_agg(g_0.e1, ',') FROM pm1.g2 AS g_0 GROUP BY g_0.e1"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    @Test public void testListAggPushdown() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getAggregateCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_LIST, true);
        caps.setCapabilitySupport(Capability.WINDOW_FUNCTION_ORDER_BY_AGGREGATES, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        //should fully push as it originates as listagg
        ProcessorPlan plan = TestOptimizer.helpPlan("select listagg(e1, ',') within group (order by e2) from pm1.g2 group by e1", RealMetadataFactory.example1Cached(), null, capFinder,  //$NON-NLS-1$
            new String[]{"SELECT string_agg(g_0.e1, ',' ORDER BY g_0.e2) FROM pm1.g2 AS g_0 GROUP BY g_0.e1"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);

        TestOptimizer.helpPlan("select string_agg(e1, ',' order by e2) from pm1.g2 group by e3", RealMetadataFactory.example1Cached(), null, capFinder,  //$NON-NLS-1$
                new String[]{"SELECT string_agg(g_0.e1, ',' ORDER BY g_0.e2) FROM pm1.g2 AS g_0 GROUP BY g_0.e3"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        //should not push as it's not generally supported
        TestOptimizer.helpPlan("select string_agg(e1, e1 order by e2) from pm1.g2 group by e3", RealMetadataFactory.example1Cached(), null, capFinder,  //$NON-NLS-1$
                new String[]{"SELECT g_0.e3, g_0.e1, g_0.e2 FROM pm1.g2 AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        TestOptimizer.helpPlan("select string_agg(X'AABB', X'AABB' order by e2) from pm1.g2 group by e3", RealMetadataFactory.example1Cached(), null, capFinder,  //$NON-NLS-1$
                new String[]{"SELECT g_0.e3, g_0.e2 FROM pm1.g2 AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    }

    @Test public void testUserDefinedAggPushdown() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getAggregateCapabilities();
        caps.setCapabilitySupport(Capability.ELEMENTARY_OLAP, true);
        caps.setCapabilitySupport(Capability.WINDOW_FUNCTION_DISTINCT_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.WINDOW_FUNCTION_ORDER_BY_AGGREGATES, true);
        caps.setFunctionSupport("FIRST_VALUE", true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.createTransformationMetadata(RealMetadataFactory.example1Cached().getMetadataStore(), "example1", new FunctionTree("foo", new FakeFunctionMetadataSource(), true));

        ProcessorPlan plan = TestOptimizer.helpPlan("select foo.FIRST_VALUE(e1) OVER (ORDER BY e2) from pm1.g2", metadata, null, capFinder,  //$NON-NLS-1$
            new String[]{"SELECT foo.FIRST_VALUE(ALL g_0.e1) OVER (ORDER BY g_0.e2) FROM pm1.g2 AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    @Test public void testHavingPushdown() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getAggregateCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_HAVING, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan("select max(e2) from pm1.g2 having min(e2) > 1", RealMetadataFactory.example1Cached(), null, capFinder,  //$NON-NLS-1$
            new String[]{"SELECT v_0.c_0 FROM (SELECT MAX(g_0.e2) AS c_0, MIN(g_0.e2) AS c_1 FROM pm1.g2 AS g_0) AS v_0 WHERE v_0.c_1 > 1"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    @Test public void testHavingPushdown1() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getAggregateCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_HAVING, false);
        caps.setFunctionSupport("concat", true);
        caps.setFunctionSupport("convert", true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan("select max(e2) from pm1.g2 group by e1 having min(e2) || e1 > '1'", RealMetadataFactory.example1Cached(), null, capFinder,  //$NON-NLS-1$
            new String[]{"SELECT v_0.c_0 FROM (SELECT MAX(g_0.e2) AS c_0, MIN(g_0.e2) AS c_1, g_0.e1 AS c_2 FROM pm1.g2 AS g_0 GROUP BY g_0.e1) AS v_0 WHERE concat(convert(v_0.c_1, string), v_0.c_2) > '1'"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    @Test public void testJoinOr() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getAggregateCapabilities();
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$

        TestOptimizer.helpPlan("SELECT A.e1 FROM pm1.g1 A, (select e2 from pm2.g1) B WHERE A.e2 = 1 OR B.e2 IS NULL GROUP BY A.e1", RealMetadataFactory.example1Cached(), null, capFinder,  //$NON-NLS-1$
            new String[]{"SELECT g_0.e2 FROM pm2.g1 AS g_0", "SELECT g_0.e2, g_0.e1 FROM pm1.g1 AS g_0 GROUP BY g_0.e2, g_0.e1"}, ComparisonMode.EXACT_COMMAND_STRING);
    }

    @Test public void testGroupingOrderByUnionPushed() throws Exception {
        String sql = "SELECT x FROM (select count(*) as x from BQT1.SmallA union all select intkey from BQT1.SmallA) x order by x"; //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();
        BasicSourceCapabilities bsc = TestAggregatePushdown.getAggregateCapabilities();
        bsc.setCapabilitySupport(Capability.QUERY_UNION, true);
        bsc.setCapabilitySupport(Capability.QUERY_SET_ORDER_BY, true);

        TestOptimizer.helpPlan(sql, metadata, null, new DefaultCapabilitiesFinder(bsc),  //$NON-NLS-1$
                new String[]{"SELECT COUNT(*) AS c_0 FROM BQT1.SmallA AS g_1 UNION ALL SELECT g_0.IntKey AS c_0 FROM BQT1.SmallA AS g_0 ORDER BY c_0"}, ComparisonMode.EXACT_COMMAND_STRING);
    }

    @Test public void testOrderByUnrelatedInSubquery() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getAggregateCapabilities();
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$
        String sql = "SELECT g1.e1 from pm1.g1 LEFT OUTER JOIN (SELECT g2.e1, AVG(g2.e2) from pm1.g2 GROUP BY g2.e1 ORDER BY AVG(g2.e2) LIMIT 2) j_Sub on g1.e1 = j_Sub.e1";
        TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[]{"SELECT g_0.e1 FROM pm1.g1 AS g_0 LEFT OUTER JOIN (SELECT g_1.e1 AS c_0 FROM pm1.g2 AS g_1 GROUP BY g_1.e1 ORDER BY AVG(g_1.e2) LIMIT 2) AS v_0 ON g_0.e1 = v_0.c_0"}, capFinder, ComparisonMode.EXACT_COMMAND_STRING);
    }

    @Test public void testGroupByConstant() throws Exception {
        BasicSourceCapabilities caps = getAggregateCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FUNCTIONS_IN_GROUP_BY, true);
        String sql = "SELECT x from (select 'aaa' as x FROM pm1.g1) AS g_0 GROUP BY x";
        TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[]{"SELECT v_0.c_0 FROM (SELECT 'aaa' AS c_0 FROM pm1.g1 AS g_0) AS v_0 GROUP BY v_0.c_0"}, new DefaultCapabilitiesFinder(caps), ComparisonMode.EXACT_COMMAND_STRING);
    }

    @Test public void testExpressions() throws Exception {
        String sql = "SELECT stringkey, sum(intnum),count(distinct case when floatnum >= 0 then 1 end)"
                + " FROM bqt1.smalla WHERE intkey=6 GROUP BY stringkey HAVING sum(intnum)>100 AND count(distinct case when floatnum >= 0 then 1 end)=0";

        BasicSourceCapabilities caps = getAggregateCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FUNCTIONS_IN_GROUP_BY, true);
        caps.setCapabilitySupport(Capability.QUERY_SEARCHED_CASE, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_DISTINCT, true);
        TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(), //$NON-NLS-1$
            new String[]{"SELECT g_0.StringKey, SUM(g_0.IntNum), COUNT(DISTINCT CASE WHEN g_0.FloatNum >= 0.0 THEN 1 END) FROM BQT1.SmallA AS g_0 WHERE g_0.IntKey = 6 GROUP BY g_0.StringKey HAVING (SUM(g_0.IntNum) > 100) AND (COUNT(DISTINCT CASE WHEN g_0.FloatNum >= 0.0 THEN 1 END) = 0)"}, new DefaultCapabilitiesFinder(caps), ComparisonMode.EXACT_COMMAND_STRING);
    }

    @Test public void testRemoveRedundantDistinct() throws Exception {
        String sql = "SELECT distinct count(*) from bqt1.smalla";

        BasicSourceCapabilities caps = getAggregateCapabilities();
        TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(), //$NON-NLS-1$
            new String[]{"SELECT COUNT(*) FROM BQT1.SmallA AS g_0"}, new DefaultCapabilitiesFinder(caps), ComparisonMode.EXACT_COMMAND_STRING);
    }

    @Test public void testPushdownAvoidance() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getAggregateCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, false);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, false);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$
        String sql = "SELECT max(bqt1.smallb.stringkey) from bqt1.smalla, bqt1.smallb where bqt1.smalla.intkey=bqt1.smallb.intkey and bqt1.smallb.bytenum <> bqt1.smalla.doublenum GROUP BY bqt1.smallb.stringnum";
        TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(), //$NON-NLS-1$
            new String[]{"SELECT DISTINCT g_0.IntKey AS c_0, g_0.DoubleNum AS c_1 FROM BQT1.SmallA AS g_0 ORDER BY c_0", "SELECT DISTINCT g_0.IntKey AS c_0, g_0.ByteNum AS c_1, g_0.StringNum AS c_2, g_0.StringKey AS c_3 FROM BQT1.SmallB AS g_0 ORDER BY c_0"}, capFinder, ComparisonMode.EXACT_COMMAND_STRING);
    }

    @Test public void testMultipleDistinct() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getAggregateCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_GROUP_BY_MULTIPLE_DISTINCT_AGGREGATES, false);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_DISTINCT, true);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$
        String sql = "SELECT count(distinct bqt1.smallb.stringkey), count(distinct bqt1.smallb.stringnum) from bqt1.smallb group by intkey";
        TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(), //$NON-NLS-1$
            new String[]{"SELECT g_0.IntKey, g_0.StringKey, g_0.StringNum FROM BQT1.SmallB AS g_0"}, capFinder, ComparisonMode.EXACT_COMMAND_STRING);

        //same distinct args are fine
        sql = "SELECT count(distinct bqt1.smallb.intkey), avg(distinct bqt1.smallb.intkey) from bqt1.smallb group by stringnum";
        TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(), //$NON-NLS-1$
            new String[]{"SELECT COUNT(DISTINCT g_0.IntKey), AVG(DISTINCT g_0.IntKey) FROM BQT1.SmallB AS g_0 GROUP BY g_0.StringNum"}, capFinder, ComparisonMode.EXACT_COMMAND_STRING);
    }

    @Test public void testSinglePredicatePushedWithUnionAndGrouping() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getAggregateCapabilities();
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$
        String sql = "select * from (select intnum as a from bqt1.smalla group by intnum union all select 1 as a) as x where a = 1;";
        TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(), //$NON-NLS-1$
            new String[]{"SELECT g_0.IntNum FROM BQT1.SmallA AS g_0 WHERE g_0.IntNum = 1 GROUP BY g_0.IntNum"}, capFinder, ComparisonMode.EXACT_COMMAND_STRING);
    }

    @Test public void testHavingWithSubqueryPlacement() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.exampleBQTCached();

        String sql = "SELECT INTKEY, STRINGKEY \n" +
                "  FROM BQT1.SMALLA AS A \n" +
                "  WHERE NOT (INTKEY IN (10)) \n" +
                "  GROUP BY INTKEY, STRINGKEY \n" +
                "  HAVING INTKEY = (SELECT MIN(STRINGKEY) FROM BQT1.SMALLA AS B WHERE A.INTKEY = B.INTKEY)";

        BasicSourceCapabilities aggregateCapabilities = getAggregateCapabilities();
        aggregateCapabilities.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, true);
        aggregateCapabilities.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, true);
        aggregateCapabilities.setFunctionSupport(SourceSystemFunctions.CONVERT, true);

        TestOptimizer.helpPlan(sql, metadata, null, new DefaultCapabilitiesFinder(aggregateCapabilities),
                new String[] {"SELECT g_0.IntKey, g_0.StringKey FROM BQT1.SmallA AS g_0 WHERE (g_0.IntKey <> 10) AND (convert(g_0.IntKey, string) = (SELECT MIN(g_1.StringKey) FROM BQT1.SmallA AS g_1 WHERE g_1.IntKey = g_0.IntKey)) GROUP BY g_0.IntKey, g_0.StringKey"}, ComparisonMode.EXACT_COMMAND_STRING);
    }

    @Test public void testCountBigPushdown() throws Exception {
        String sql = "select count_big(e1) from pm1.g1"; //$NON-NLS-1$

        CommandContext cc = TestProcessor.createCommandContext();
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        BasicSourceCapabilities aggregateCapabilities = getAggregateCapabilities();
        aggregateCapabilities.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT_BIG, true);

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, metadata, null, new DefaultCapabilitiesFinder(aggregateCapabilities),
                new String[] {"SELECT COUNT_BIG(g_0.e1) FROM pm1.g1 AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING);
        HardcodedDataManager hdm = new HardcodedDataManager(metadata, cc, aggregateCapabilities);
        hdm.addData("SELECT COUNT_BIG(g_0.e1) FROM g1 AS g_0", Arrays.asList(1L));

        TestProcessor.helpProcess(plan, hdm, new List[] {Arrays.asList(1L)});

        //same as before, but the source will just use count
        aggregateCapabilities.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT_BIG, false);

        plan = TestOptimizer.helpPlan(sql, metadata, null, new DefaultCapabilitiesFinder(aggregateCapabilities),
                new String[] {"SELECT COUNT_BIG(g_0.e1) FROM pm1.g1 AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING);
        hdm = new HardcodedDataManager(metadata, cc, aggregateCapabilities);
        hdm.addData("SELECT COUNT(g_0.e1) FROM g1 AS g_0", Arrays.asList(1L));

        TestProcessor.helpProcess(plan, hdm, new List[] {Arrays.asList(1L)});
    }

    @Test public void testAggregatePushdownOverNestedLateralJoin() throws Exception {

        String sql = "SELECT xt.e2, count(*) FROM pm1.g1 AS d, \n" +
                "    TABLE (select d.e2 from pm1.g1 limit 1) xt\n" +
                "      LEFT JOIN TABLE (select pm1.g1.e1 as some_col from pm1.g1 where e1 = 'a' || d.e1) xt2\n" +
                "      ON xt.e2 = -1\n" +
                "group by xt.e2";

        BasicSourceCapabilities aggregateCapabilities = getAggregateCapabilities();
        CommandContext cc = TestProcessor.createCommandContext();
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        cc.setMetadata(metadata);

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), null, new DefaultCapabilitiesFinder(aggregateCapabilities),  //$NON-NLS-1$
            new String[]{"SELECT g_0.e2, g_0.e1 FROM pm1.g1 AS g_0",
            "SELECT 1 FROM pm1.g1 AS g_0 WHERE g_0.e1 = concat('a', d.e1)",
            "SELECT d.e2 AS c_0 FROM pm1.g1 AS g_0 LIMIT 1"}, ComparisonMode.EXACT_COMMAND_STRING);

        HardcodedDataManager hdm = new HardcodedDataManager(metadata, cc, aggregateCapabilities);
        hdm = new HardcodedDataManager(metadata, cc, aggregateCapabilities);
        hdm.addData("SELECT g_0.e2, g_0.e1 FROM g1 AS g_0", Arrays.asList(1, "a"));
        hdm.addData("SELECT 1 AS c_0 FROM g1 AS g_0 LIMIT 1", Arrays.asList(1));
        hdm.addData("SELECT 1 FROM g1 AS g_0 WHERE g_0.e1 = 'aa'");

        TestProcessor.helpProcess(plan, cc, hdm, new List[] {Arrays.asList(1, 1)});
    }

}

