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

import static junit.framework.Assert.*;

import org.junit.Test;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
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
import org.teiid.translator.SourceSystemFunctions;

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
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT x FROM /*+ no_unnest */ (SELECT e1, max(e2) as x FROM pm1.g1 GROUP BY e1) AS z order by x", //$NON-NLS-1$
                                      RealMetadataFactory.example1Cached(), null, TestAggregatePushdown.getAggregatesFinder(),
                                      new String[] {
                                          "SELECT v_0.c_0 FROM (SELECT MAX(g_0.e2) AS c_0 FROM pm1.g1 AS g_0 GROUP BY g_0.e1) AS v_0 ORDER BY c_0"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    @Test public void testNoUnnestView() throws TeiidComponentException, TeiidProcessingException {
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT e1 FROM /*+ no_unnest */ vm1.g1 limit 1", //$NON-NLS-1$
                                      RealMetadataFactory.example1Cached(), null, TestAggregatePushdown.getAggregatesFinder(),
                                      new String[] {
                                          "SELECT v_0.c_0 AS c_0 FROM (SELECT g_0.e1 AS c_0 FROM pm1.g1 AS g_0) AS v_0 LIMIT 1"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

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

    //should not allow the merge since it's an outer join
    @Test public void testNoSourcesMerge3() throws Exception {
        TestOptimizer.helpPlan("select z.* from pm1.g1 left outer join (select 1 as a, 2, 3) as z on pm1.g1.e2 = z.a", //$NON-NLS-1$
                                      RealMetadataFactory.example1Cached(),
                                      new String[] {
                                          "SELECT g_0.e2 AS c_0 FROM pm1.g1 AS g_0 ORDER BY c_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

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
        ProcessorPlan plan = TestOptimizer.helpPlan("select z.* from pm1.g1 inner join arraytable(cast(pm1.g1.e1 as object) COLUMNS one integer, two integer, three integer) as z on (pm1.g1.e2 = z.one)", //$NON-NLS-1$
                RealMetadataFactory.example1Cached(),
                new String[] {
                    "SELECT convert(array_get(convert(g_0.e1, object), 1), integer), convert(array_get(convert(g_0.e1, object), 2), integer), convert(array_get(convert(g_0.e1, object), 3), integer) FROM pm1.g1 AS g_0 WHERE (convert(g_0.e1, object) IS NOT NULL) AND (g_0.e2 = convert(array_get(convert(g_0.e1, object), 1), integer))"}, new DefaultCapabilitiesFinder(caps), ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

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

    @Test public void testSingleOrPredicate() throws Exception {
        String sql = "SELECT alias3.a1 FROM (select e2 as a from pm1.g1) as alias2 INNER JOIN (SELECT t2.a AS a1, t1.a "
                + "FROM (SELECT 1 AS a) AS t1 INNER JOIN (select e2 as a from pm1.g1) as t2 ON t1.a = t2.a) "
                + "AS alias3 ON ((alias3.a = alias2.a) OR (alias3.a > alias2.a))";
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();

        TestOptimizer.helpPlan(sql, //$NON-NLS-1$
                RealMetadataFactory.example1Cached(),
                new String[] {
                    "SELECT 1 FROM pm1.g1 AS g_0 WHERE (g_0.e2 = 1) OR (g_0.e2 < 1)", "SELECT g_0.e2 FROM pm1.g1 AS g_0 WHERE g_0.e2 = 1"}, new DefaultCapabilitiesFinder(caps), ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    }

    @Test public void testPredicatePlacementUnderFullOuter() throws TeiidComponentException, TeiidProcessingException, Exception {
        String ddl = "create foreign table table1_m (date_value date, id integer);"
                + "create foreign table table2_m (date_value date, id integer);"
                + "create foreign table calendar_m (date_value date); "
                + "CREATE VIEW view1_m\n" +
                "        AS\n" +
                "          SELECT\n" +
                "            days.date_value as date_value1\n" +
                "            ,table1_m.*\n" +
                "          FROM \n" +
                "            (SELECT *\n" +
                "             FROM calendar_m\n" +
                "             WHERE year(date_value) BETWEEN 2018 AND year(NOW())+2) days\n" +
                "             LEFT JOIN (select * from table1_m) table1_m on days.date_value = table1_m.date_value;\n" +
                "        CREATE VIEW view2_m\n" +
                "        AS\n" +
                "          SELECT\n" +
                "            days.date_value as date_value2\n" +
                "            ,table2_m.*\n" +
                "          FROM \n" +
                "            (SELECT *\n" +
                "             FROM calendar_m\n" +
                "             WHERE year(date_value) BETWEEN 2018 AND year(NOW())+2) days\n" +
                "             LEFT JOIN (select * from table2_m) table2_m on days.date_value = table2_m.date_value";

        String sql = "SELECT COUNT(*) \n" +
                "FROM \n" +
                "    (\n" +
                "        SELECT \n" +
                "            date_value1 \n" +
                "            , id\n" +
                "        FROM view1_m \n" +
                "        --Limit 1000000000\n" +
                "    ) v1 \n" +
                "full \n" +
                "JOIN  \n" +
                "    (\n" +
                "        SELECT \n" +
                "            date_value2 \n" +
                "            , id\n" +
                "        FROM view2_m \n" +
                "        --LIMIT 1000000000\n" +
                "    ) v2 \n" +
                "    ON \n" +
                "        v1.date_value1 = v2.date_value2 \n" +
                "        and v1.id = v2.id";


        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER_FULL, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
        caps.setCapabilitySupport(Capability.QUERY_GROUP_BY, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT_STAR, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_INLINE_VIEWS, true);
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
        caps.setFunctionSupport(SourceSystemFunctions.YEAR, true);

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.fromDDL(ddl, "x", "y"),
                new String[] {
                        "SELECT COUNT(*) FROM ((SELECT g_0.date_value AS c_0 FROM y.calendar_m AS g_0 WHERE (year(g_0.date_value) >= 2018) AND (year(g_0.date_value) <= (year(NOW()) + 2))) AS v_0 LEFT OUTER JOIN y.table1_m AS g_1 ON v_0.c_0 = g_1.date_value) "
                        + "FULL OUTER JOIN ((SELECT g_2.date_value AS c_0 FROM y.calendar_m AS g_2 WHERE (year(g_2.date_value) >= 2018) AND (year(g_2.date_value) <= (year(NOW()) + 2))) AS v_1 LEFT OUTER JOIN y.table2_m AS g_3 ON v_1.c_0 = g_3.date_value) ON v_0.c_0 = v_1.c_0 AND g_1.id = g_3.id" }, //$NON-NLS-1$
                new DefaultCapabilitiesFinder(caps),
                ComparisonMode.EXACT_COMMAND_STRING);

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    @Test public void testNullDependentPredicateUnderLeftOuter() throws Exception {
        String ddl = "create foreign table test_1 (c1 string);"
                + "\ncreate foreign table test_2 (c1 string);"
                + "\ncreate foreign table test_3 (c1 string);";
        String sql = "SELECT *\n" +
                "FROM test_1 a\n" +
                "    LEFT JOIN ( SELECT x.c1 \n" +
                "                FROM test_2 x\n" +
                "                    LEFT JOIN (SELECT * FROM test_3 WHERE c1 <> '123')  y  ON x.c1 = y.c1 WHERE y.c1 IS NULL\n" +
                //" LIMIT 1000000000 "+
                "     ) b ON a.c1 = b.c1\n" +
                "WHERE b.c1 IS NULL";

        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER_FULL, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
        caps.setCapabilitySupport(Capability.QUERY_GROUP_BY, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT_STAR, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_INLINE_VIEWS, true);
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.fromDDL(ddl, "x", "y"),
                new String[] {"SELECT g_0.c1, v_0.c_0 FROM y.test_1 AS g_0 LEFT OUTER JOIN (SELECT g_1.c1 AS c_0 FROM y.test_2 AS g_1 LEFT OUTER JOIN y.test_3 AS g_2 ON g_1.c1 = g_2.c1 AND g_2.c1 <> '123' WHERE g_2.c1 IS NULL) AS v_0 ON g_0.c1 = v_0.c_0 WHERE v_0.c_0 IS NULL"}, //$NON-NLS-1$
                new DefaultCapabilitiesFinder(caps),
                ComparisonMode.EXACT_COMMAND_STRING);

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

}
