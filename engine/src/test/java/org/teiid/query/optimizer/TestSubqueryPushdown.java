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

import static org.junit.Assert.*;
import static org.teiid.query.optimizer.TestOptimizer.*;
import static org.teiid.query.processor.TestProcessor.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.ArrayImpl;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.TestOptimizer.AntiSemiJoin;
import org.teiid.query.optimizer.TestOptimizer.ComparisonMode;
import org.teiid.query.optimizer.TestOptimizer.SemiJoin;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.FakeCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.processor.HardcodedDataManager;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.RegisterRequestParameter;
import org.teiid.query.processor.TestProcessor;
import org.teiid.query.rewriter.TestQueryRewriter;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.unittest.RealMetadataFactory.DDLHolder;
import org.teiid.query.util.CommandContext;
import org.teiid.query.util.Options;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.ExecutionFactory.TransactionSupport;
import org.teiid.translator.SourceSystemFunctions;

@SuppressWarnings("nls")
public class TestSubqueryPushdown {

    @Test public void testPushSubqueryBelowVirtual() throws Exception {
        String sql = "select g3.e1 from (select e1, max(e2) y from pm1.g1 group by e1) x, pm1.g3 where exists (select e1 from pm1.g2 where x.e1 = e1)"; //$NON-NLS-1$

        // Create capabilities
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, true);
        caps.setCapabilitySupport(Capability.CRITERIA_EXISTS, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        // Plan query
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, metadata,
            null, capFinder,
            new String[] { "SELECT g_0.e1 FROM pm1.g1 AS g_0 WHERE EXISTS (SELECT g_1.e1 FROM pm1.g2 AS g_1 WHERE g_1.e1 = g_0.e1)", //$NON-NLS-1$
                "SELECT g_0.e1 FROM pm1.g3 AS g_0" },  //$NON-NLS-1$
            TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING);
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
                2,      // Project
                0,      // Select
                0,      // Sort
                0       // UnionAll
            });
    }

    /**
     * Same as above, but using a correlated variable based on an aggregate
     * @throws Exception
     */
    @Test public void testDontPushSubqueryBelowVirtual() throws Exception {
        String sql = "select g3.e1 from (select e1, max(e2) y from pm1.g1 group by e1) x, pm1.g3 where exists (select e1 from pm1.g2 where x.y = e1)"; //$NON-NLS-1$

        // Create capabilities
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, true);
        caps.setCapabilitySupport(Capability.CRITERIA_EXISTS, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        // Plan query
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, metadata,
            null, capFinder,
            new String[] { "SELECT g_0.e1 FROM pm1.g3 AS g_0", //$NON-NLS-1$
                "SELECT g_0.e1, g_0.e2 FROM pm1.g1 AS g_0" },  //$NON-NLS-1$
            TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING);
        TestOptimizer.checkNodeTypes(plan, new int[] {
                2,      // Access
                0,      // DependentAccess
                1,      // DependentSelect
                0,      // DependentProject
                0,      // DupRemove
                1,      // Grouping
                1,      // NestedLoopJoinStrategy
                0,      // MergeJoinStrategy
                0,      // Null
                0,      // PlanExecution
                2,      // Project
                0,      // Select
                0,      // Sort
                0       // UnionAll
            });
    }

    @Test public void testPushCorrelatedSubquery1() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.CRITERIA_QUANTIFIED_ALL, true);
        caps.setCapabilitySupport(Capability.CRITERIA_QUANTIFIED_SOME, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan("SELECT intkey FROM bqt1.smalla AS n WHERE intkey = /*+ NO_UNNEST */ (SELECT MAX(intkey) FROM bqt1.smallb AS s WHERE s.stringkey = n.stringkey )", RealMetadataFactory.exampleBQTCached(),  //$NON-NLS-1$
            null, capFinder,
            new String[] { "SELECT g_0.IntKey FROM BQT1.SmallA AS g_0 WHERE g_0.IntKey = /*+ NO_UNNEST */ (SELECT MAX(g_1.IntKey) FROM BQT1.SmallB AS g_1 WHERE g_1.StringKey = g_0.StringKey)" }, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        checkNodeTypes(plan, FULL_PUSHDOWN);

        assertNull(plan.requiresTransaction(true));
        assertFalse(plan.requiresTransaction(false));
    }

    @Test public void testPushCorrelatedSubquery2() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.CRITERIA_LIKE, true);
        caps.setCapabilitySupport(Capability.CRITERIA_QUANTIFIED_ALL, true);
        caps.setCapabilitySupport(Capability.CRITERIA_QUANTIFIED_SOME, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
        caps.setFunctionSupport(SourceSystemFunctions.CONCAT, true);
        caps.setFunctionSupport("convert", true); //$NON-NLS-1$
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        String sqlIn =
            "SELECT c37n.intkey " + //$NON-NLS-1$
            "FROM bqt1.mediuma AS c37n, bqt1.smallb AS m37n " + //$NON-NLS-1$
            "WHERE (m37n.stringkey LIKE '%0') AND " + //$NON-NLS-1$
            "(c37n.stringkey = ('1' || (m37n.intkey || '0'))) AND " + //$NON-NLS-1$
            "(c37n.datevalue = /*+ NO_UNNEST */ (" + //$NON-NLS-1$
            "SELECT MAX(c37s.datevalue) " + //$NON-NLS-1$
            "FROM bqt1.mediuma AS c37s, bqt1.smallb AS m37s " + //$NON-NLS-1$
            "WHERE (m37s.stringkey LIKE '%0') AND " + //$NON-NLS-1$
            "(c37s.stringkey = ('1' || (m37s.intkey || '0'))) AND " + //$NON-NLS-1$
            "(m37s.stringkey = m37n.stringkey) ))"; //$NON-NLS-1$

        String sqlOut = "SELECT g_0.intkey FROM bqt1.mediuma AS g_0, bqt1.smallb AS g_1 WHERE (g_0.stringkey = concat('1', concat(g_1.intkey, '0'))) AND (g_1.stringkey LIKE '%0') AND (g_0.datevalue = /*+ NO_UNNEST */ (SELECT MAX(g_2.datevalue) FROM bqt1.mediuma AS g_2, bqt1.smallb AS g_3 WHERE (g_2.stringkey = concat('1', concat(g_3.intkey, '0'))) AND (g_3.stringkey LIKE '%0') AND (g_3.stringkey = g_1.stringkey)))"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sqlIn, RealMetadataFactory.exampleBQTCached(),
            null, capFinder,
            new String[] { sqlOut }, SHOULD_SUCCEED);
        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testPushCorrelatedSubquery3() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.CRITERIA_LIKE, true);
        caps.setCapabilitySupport(Capability.CRITERIA_QUANTIFIED_ALL, true);
        caps.setCapabilitySupport(Capability.CRITERIA_QUANTIFIED_SOME, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
        caps.setFunctionSupport("||", true); //$NON-NLS-1$
        caps.setFunctionSupport("convert", true); //$NON-NLS-1$
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        String sqlIn =
            "SELECT intkey " + //$NON-NLS-1$
            "FROM vqt.smalla AS e " + //$NON-NLS-1$
            "WHERE (stringkey = 'VOD.L') AND " + //$NON-NLS-1$
            "(datevalue = /*+ NO_UNNEST */ (" + //$NON-NLS-1$
            "SELECT MAX(datevalue) " + //$NON-NLS-1$
            "FROM vqt.smalla " + //$NON-NLS-1$
            "WHERE (stringkey = e.stringkey) ))"; //$NON-NLS-1$

        String sqlOut =
            "SELECT SmallA__1.IntKey FROM BQT1.SmallA AS SmallA__1 WHERE (SmallA__1.StringKey = 'VOD.L') AND (SmallA__1.DateValue = /*+ NO_UNNEST */ (SELECT MAX(BQT1.SmallA.DateValue) FROM BQT1.SmallA WHERE BQT1.SmallA.StringKey = SmallA__1.StringKey))"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sqlIn, RealMetadataFactory.exampleBQTCached(),
            null, capFinder,
            new String[] { sqlOut }, SHOULD_SUCCEED);
        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    /**
     * Check that scalar subquery in select is pushed
     */
    public void testPushSubqueryInSelectClause1() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, true);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan("SELECT stringkey, (SELECT intkey FROM BQT1.SmallA AS b WHERE Intnum = 22) FROM BQT1.SmallA", RealMetadataFactory.exampleBQTCached(),  //$NON-NLS-1$
            null, capFinder,
            new String[] { "SELECT stringkey, (SELECT intkey FROM BQT1.SmallA AS b WHERE Intnum = 22) FROM BQT1.SmallA" }, SHOULD_SUCCEED); //$NON-NLS-1$
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

    @Test public void testCorrelatedSubquery1() {
        ProcessorPlan plan = helpPlan("Select e1 from pm1.g1 where e1 in (select e1 FROM pm2.g1 WHERE pm1.g1.e2 = pm2.g1.e2)", RealMetadataFactory.example1Cached(),  //$NON-NLS-1$
            new String[] { "SELECT e1, pm1.g1.e2 FROM pm1.g1" }); //$NON-NLS-1$
        checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            1,      // DependentSelect
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

        assertTrue(plan.requiresTransaction(true));
        assertFalse(plan.requiresTransaction(false));
    }

    @Test public void testCorrelatedSubquery2() {
        ProcessorPlan plan = helpPlan("Select e1, (select e1 FROM pm2.g1 WHERE pm1.g1.e2 = pm2.g1.e2) from pm1.g1", RealMetadataFactory.example1Cached(),  //$NON-NLS-1$
            new String[] { "SELECT e1, pm1.g1.e2 FROM pm1.g1" }); //$NON-NLS-1$
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
    }

    @Test public void testCorrelatedSubqueryVirtualLayer1() {
        ProcessorPlan plan = helpPlan("Select e1 from vm1.g6 where e1 in (select e1 FROM pm2.g1 WHERE vm1.g6.e3 = pm2.g1.e2)", example1(),  //$NON-NLS-1$
            new String[] { "SELECT e1 FROM pm1.g1" }); //$NON-NLS-1$
        checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            1,      // DependentSelect
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

    @Test public void testCorrelatedSubqueryVirtualLayer2() {
        ProcessorPlan plan = helpPlan("Select e1 from vm1.g6 where e1 in (select e1 FROM pm2.g1 WHERE vm1.g6.e4 = pm2.g1.e4)", example1(),  //$NON-NLS-1$
            new String[] { "SELECT e1, e2, e4 FROM pm1.g1" }); //$NON-NLS-1$
        checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            1,      // DependentSelect
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

    @Test public void testCorrelatedSubqueryVirtualLayer3() {
        ProcessorPlan plan = helpPlan("Select e1, (select e1 FROM pm2.g1 WHERE vm1.g6.e4 = pm2.g1.e4) from vm1.g6", example1(),  //$NON-NLS-1$
            new String[] { "SELECT e1, e2, e4 FROM pm1.g1" }); //$NON-NLS-1$
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
    }

    @Test public void testCorrelatedSubqueryInTransformation2() {
        String sql = "Select * from vm1.g20"; //$NON-NLS-1$
        ProcessorPlan plan = helpPlan(sql, RealMetadataFactory.example1Cached(),
            new String[] { "SELECT pm1.g1.e1, pm1.g1.e2, pm1.g1.e3, pm1.g1.e4 FROM pm1.g1" }); //$NON-NLS-1$
        checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            1,      // DependentSelect
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
     * Check that subquery is not pushed if the subquery cannot all be pushed to the source.  Automatically converted to a merge join
     */
    @Test public void testNoPushSubqueryInWhereClause1() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan("Select e1 from pm1.g1 where e1 in /*+ MJ */ (select max(e1) FROM pm1.g2)", RealMetadataFactory.example1Cached(),  //$NON-NLS-1$
            null, capFinder,
            new String[] { "SELECT g_0.e1 FROM pm1.g1 AS g_0 WHERE g_0.e1 IN (<dependent values>)", "SELECT g_0.e1 FROM pm1.g2 AS g_0" }, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
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
            3,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    /**
     * Check that subquery is not pushed if the subquery is from a different model
     * than the outer query.
     */
    @Test public void testNoPushSubqueryInWhereClause2() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", getTypicalCapabilities()); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan("Select e1 from pm1.g1 where e1 in /*+ no_unnest */ (select e1 FROM pm2.g1)", RealMetadataFactory.example1Cached(),  //$NON-NLS-1$
            null, capFinder,
            new String[] { "SELECT e1 FROM pm1.g1" }, SHOULD_SUCCEED); //$NON-NLS-1$
        checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            1,      // DependentSelect
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
     * Do not support XML query as subquery
     * Check that subquery is not pushed if the subquery is not relational.
     */
    public void defer_testNoPushSubqueryInWhereClause3() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", new BasicSourceCapabilities()); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan("Select e1 from pm1.g1 where e1 in (select * from xmltest.doc1)", RealMetadataFactory.example1Cached(),  //$NON-NLS-1$
            null, capFinder,
            new String[] { "SELECT e1 FROM pm1.g1" }, SHOULD_SUCCEED); //$NON-NLS-1$
        checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            1,      // DependentSelect
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
     * Check that subquery is not pushed if the subquery has a function that can't be pushed
     * in the SELECT clause
     */
    @Test public void testNoPushSubqueryInWhereClause4() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", new BasicSourceCapabilities()); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan("Select e1 from pm1.g1 where e1 in /*+ no_unnest */ (SELECT ltrim(e1) FROM pm1.g2)", RealMetadataFactory.example1Cached(),  //$NON-NLS-1$
            null, capFinder,
            new String[] { "SELECT e1 FROM pm1.g1" }, SHOULD_SUCCEED); //$NON-NLS-1$
        checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            1,      // DependentSelect
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
     * Check that subquery is not pushed if the subquery selects a constant value
     */
    @Test public void testNoPushSubqueryInWhereClause5() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", new BasicSourceCapabilities()); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan("Select e1 from pm1.g1 where e1 in (SELECT 'xyz' FROM pm1.g2)", RealMetadataFactory.example1Cached(),  //$NON-NLS-1$
            null, capFinder,
            new String[] { "SELECT e1 FROM pm1.g1" }, SHOULD_SUCCEED); //$NON-NLS-1$
        checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            1,      // DependentSelect
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
     * Check that subquery is not pushed if the subquery does ORDER BY
     */
    @Test public void testNoPushSubqueryInWhereClause6() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", new BasicSourceCapabilities()); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan("Select e1 from pm1.g1 where e1 in (SELECT e1 FROM pm1.g2 ORDER BY e1 limit 2)", RealMetadataFactory.example1Cached(),  //$NON-NLS-1$
            null, capFinder,
            new String[] { "SELECT e1 FROM pm1.g1" }, SHOULD_SUCCEED); //$NON-NLS-1$
        checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            1,      // DependentSelect
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
     * Check that subquery is not pushed if the subquery has a function that can't be pushed
     * in the SELECT clause
     */
    @Test public void testNoPushSubqueryInWhereClause7() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);
        caps.setFunctionSupport("ltrim", true); //$NON-NLS-1$
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", new BasicSourceCapabilities()); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan("Select e1 from pm1.g1 where e1 in /*+ no_unnest */ (SELECT rtrim(ltrim(e1)) FROM pm1.g2)", RealMetadataFactory.example1Cached(),  //$NON-NLS-1$
            null, capFinder,
            new String[] { "SELECT e1 FROM pm1.g1" }, SHOULD_SUCCEED); //$NON-NLS-1$
        checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            1,      // DependentSelect
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
     * Check that subquery is not pushed if the subquery holds non-query access node.
     */
    @Test public void testNoPushSubqueryInWhereClause8() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", new BasicSourceCapabilities()); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan("Select e1 from pm1.g1 where e1 in /*+ no_unnest */ (EXEC pm1.sqsp1())", RealMetadataFactory.example1Cached(),  //$NON-NLS-1$
            null, capFinder,
            new String[] { "SELECT e1 FROM pm1.g1" }, SHOULD_SUCCEED); //$NON-NLS-1$
        checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            1,      // DependentSelect
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
     * Check that subquery is not pushed if the subquery is correlated and correlated not supported
     */
    @Test public void testNoPushSubqueryInWhereClause9() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan("Select e1 from pm1.g1 where e1 in (SELECT pm1.g2.e1 FROM pm1.g2 WHERE pm1.g2.e1 = pm1.g1.e1)", RealMetadataFactory.example1Cached(),  //$NON-NLS-1$
            null, capFinder,
            new String[] { "SELECT e1 FROM pm1.g1" }, SHOULD_SUCCEED); //$NON-NLS-1$
        checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            1,      // DependentSelect
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

    @Test public void testPushMultipleCorrelatedSubquery1() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_OR, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.CRITERIA_QUANTIFIED_ALL, true);
        caps.setCapabilitySupport(Capability.CRITERIA_QUANTIFIED_SOME, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MIN, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan("SELECT intkey FROM bqt1.smalla AS n WHERE intkey = (SELECT MAX(intkey) FROM bqt1.smallb AS s WHERE s.stringkey = n.stringkey ) or intkey = (SELECT MIN(intkey) FROM bqt1.smallb AS s WHERE s.stringkey = n.stringkey )", RealMetadataFactory.exampleBQTCached(),  //$NON-NLS-1$
            null, capFinder,
            new String[] { "SELECT g_0.intkey FROM bqt1.smalla AS g_0 WHERE (g_0.intkey = (SELECT MAX(g_1.intkey) FROM bqt1.smallb AS g_1 WHERE g_1.stringkey = g_0.stringkey)) OR (g_0.intkey = (SELECT MIN(g_2.IntKey) FROM bqt1.smallb AS g_2 WHERE g_2.StringKey = g_0.stringkey))" }, SHOULD_SUCCEED); //$NON-NLS-1$
        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    /*
     * Expressions containing subqueries can be pushed down
     */
    @Test public void testProjectSubqueryPushdown() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        BasicSourceCapabilities caps = getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        caps.setFunctionSupport("+", true); //$NON-NLS-1$
        caps.setFunctionSupport("convert", true); //$NON-NLS-1$
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan("select pm1.g1.e1, convert((select max(vm1.g1.e1) from vm1.g1), integer) + 1 from pm1.g1", metadata,  //$NON-NLS-1$
                                      null, capFinder,
            new String[] { "SELECT g_0.e1, (convert((SELECT MAX(g_0.e1) FROM pm1.g1 AS g_0), integer) + 1) FROM pm1.g1 AS g_0" }, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        assertNotNull(plan.getDescriptionProperties().getProperty("Query Subplan 0"));

        HardcodedDataManager hcdm = new HardcodedDataManager(true);
        hcdm.addData("SELECT MAX(g_0.e1) FROM pm1.g1 AS g_0", Arrays.asList("13"));
        hcdm.addData("SELECT g_0.e1 FROM pm1.g1 AS g_0", Arrays.asList("10"), Arrays.asList("13"));
        CommandContext cc = TestProcessor.createCommandContext();
        cc.setMetadata(metadata);
        TestProcessor.helpProcess(plan, cc, hcdm, new List<?>[] {Arrays.asList("10", 14), Arrays.asList("13", 14)});

        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR_PROJECTION, true);

        plan = helpPlan("select pm1.g1.e1, convert((select max(vm1.g1.e1) from vm1.g1), integer) + 1 from pm1.g1", metadata,  //$NON-NLS-1$
                null, capFinder,
        new String[] { "SELECT g_0.e1, (convert((SELECT MAX(g_1.e1) FROM pm1.g1 AS g_1), integer) + 1) FROM pm1.g1 AS g_0" }, SHOULD_SUCCEED); //$NON-NLS-1$
        checkNodeTypes(plan, FULL_PUSHDOWN);

        assertNull(plan.getDescriptionProperties().getProperty("Query Subplan 0"));
    }

    @Test public void testScalarSubquery2() {
        ProcessorPlan plan = helpPlan("Select e1, (select e1 FROM pm2.g1 where pm1.g1.e1 = 'x') as X from pm1.g1", RealMetadataFactory.example1Cached(),  //$NON-NLS-1$
            new String[] { "SELECT e1 FROM pm1.g1" }); //$NON-NLS-1$
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
    }

    /**
     * Technically this is not a full push-down, but the subquery will be evaluated prior to pushdown
     */
    @Test public void testCompareSubquery4() throws TeiidComponentException, TeiidProcessingException {
        ProcessorPlan plan = helpPlan("Select e1 from pm1.g1 where e1 > (select e1 FROM pm2.g1 where e2 = 13)", RealMetadataFactory.example1Cached(),  //$NON-NLS-1$
            new String[] { "SELECT g_0.e1 FROM pm1.g1 AS g_0 WHERE g_0.e1 > (SELECT g_0.e1 FROM pm2.g1 AS g_0 WHERE g_0.e2 = 13)" }, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        checkNodeTypes(plan, FULL_PUSHDOWN);

        assertTrue(plan.requiresTransaction(true));
        assertFalse(plan.requiresTransaction(false));

        BasicSourceCapabilities bsc = getTypicalCapabilities();
        bsc.setSourceProperty(Capability.TRANSACTION_SUPPORT, TransactionSupport.NONE);

        plan = helpPlan("Select e1 from pm1.g1 where e1 > (select e1 FROM pm2.g1 where e2 = 13)", RealMetadataFactory.example1Cached(),  //$NON-NLS-1$
                new String[] { "SELECT g_0.e1 FROM pm1.g1 AS g_0 WHERE g_0.e1 > (SELECT g_0.e1 FROM pm2.g1 AS g_0 WHERE g_0.e2 = 13)" }, new DefaultCapabilitiesFinder(bsc), ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
            checkNodeTypes(plan, FULL_PUSHDOWN);

        assertFalse(plan.requiresTransaction(true));
        assertFalse(plan.requiresTransaction(false));
    }

    @Test public void testScalarSubquery1() throws TeiidComponentException, TeiidProcessingException {
        ProcessorPlan plan = helpPlan("Select e1, (select e1 FROM pm2.g1 where e1 = 'x') from pm1.g1", RealMetadataFactory.example1Cached(),  //$NON-NLS-1$
            new String[] { "SELECT g_0.e1, (SELECT g_0.e1 FROM pm2.g1 AS g_0 WHERE g_0.e1 = 'x') FROM pm1.g1 AS g_0" }, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testSubqueryRewriteToJoinDistinct() throws Exception {
        CommandContext cc = new CommandContext();
        cc.setOptions(new Options().subqueryUnnestDefault(true));
        TestQueryRewriter.helpTestRewriteCommand("Select distinct e1 from pm1.g1 as x where exists (select pm1.g1.e1 FROM pm1.g1 where e1 = x.e1)", "SELECT DISTINCT e1 FROM pm1.g1 AS x, (SELECT e1 FROM pm1.g1) AS X__1 WHERE x.e1 = X__1.e1", RealMetadataFactory.example1Cached(), cc);
    }

    //won't rewrite since we need distinct and don't have all equi join predicates
    @Test public void testSubqueryRewriteToJoinDistinct1() throws Exception {
        CommandContext cc = new CommandContext();
        cc.setOptions(new Options().subqueryUnnestDefault(true));
        TestQueryRewriter.helpTestRewriteCommand("Select e1 from pm1.g1 as x where exists (select pm1.g1.e1 FROM pm1.g1 where e1 = x.e1 and e2 < x.e2)", "SELECT e1 FROM pm1.g1 AS x WHERE EXISTS (SELECT pm1.g1.e1 FROM pm1.g1 WHERE (e1 = x.e1) AND (e2 < x.e2) LIMIT 1)", RealMetadataFactory.example1Cached(), cc);
    }

    /**
     * Agg does not depend on cardinality
     */
    @Test public void testSubqueryRewriteToJoinGroupBy() throws Exception {
        CommandContext cc = new CommandContext();
        cc.setOptions(new Options().subqueryUnnestDefault(true));
        TestQueryRewriter.helpTestRewriteCommand("Select max(e1) from pm1.g1 as x where exists (select pm1.g1.e1 FROM pm1.g1 where e1 = x.e1) group by e2", "SELECT MAX(e1) FROM pm1.g1 AS x, (SELECT e1 FROM pm1.g1) AS X__1 WHERE x.e1 = X__1.e1 GROUP BY e2", RealMetadataFactory.example1Cached(), cc);
    }

    /**
     * Agg does depend on cardinality
     */
    @Test public void testSubqueryRewriteToJoinGroupBy1() throws Exception {
        TestQueryRewriter.helpTestRewriteCommand("Select avg(e1) from pm1.g1 as x where exists (select pm1.g1.e1 FROM pm1.g1 where e1 = x.e1) group by e2", "SELECT AVG(e1) FROM pm1.g1 AS x WHERE EXISTS (SELECT pm1.g1.e1 FROM pm1.g1 WHERE e1 = x.e1 LIMIT 1) GROUP BY e2", RealMetadataFactory.example1Cached());
    }

    @Test public void testSubqueryDoNotRewriteToJoin() throws Exception {
        CommandContext cc = new CommandContext();
        cc.setOptions(new Options().subqueryUnnestDefault(true));
        TestQueryRewriter.helpTestRewriteCommand("Select e1 from pm3.g1 where not exists (select pm1.g1.e1 FROM pm1.g1 where e1 = pm3.g1.e1)", "SELECT e1 FROM pm3.g1 WHERE NOT EXISTS (SELECT pm1.g1.e1 FROM pm1.g1 WHERE e1 = pm3.g1.e1 LIMIT 1)", RealMetadataFactory.example4(), cc);
    }

    @Test public void testSubqueryDoNotRewriteToJoin2() throws Exception {
        CommandContext cc = new CommandContext();
        cc.setOptions(new Options().subqueryUnnestDefault(true));
        TestQueryRewriter.helpTestRewriteCommand("Select e1 from pm3.g1 where e2 < some (select pm1.g1.e2 FROM pm1.g1)", "SELECT e1 FROM pm3.g1 WHERE e2 < (SELECT MAX(X.e2) FROM (SELECT pm1.g1.e2 FROM pm1.g1) AS X)", RealMetadataFactory.example4(), cc);
    }

    @Test public void testSubqueryDoNotRewriteToJoin3() throws Exception {
        CommandContext cc = new CommandContext();
        cc.setOptions(new Options().subqueryUnnestDefault(true));
        TestQueryRewriter.helpTestRewriteCommand("Select e1 from pm3.g1 where e2 < some (select pm1.g1.e2 FROM pm1.g1 where pm3.g1.e3 <> e3)", "SELECT e1 FROM pm3.g1 WHERE e2 < SOME (SELECT MAX(pm1.g1.e2) FROM pm1.g1 WHERE e3 <> pm3.g1.e3)", RealMetadataFactory.example4(), cc);
        //should rewrite as we have an equi join predicate
        TestQueryRewriter.helpTestRewriteCommand("Select e1 from pm3.g1 where e2 < some (select pm1.g1.e2 FROM pm1.g1 where pm3.g1.e3 = e3)", "SELECT e1 FROM pm3.g1, (SELECT MAX(pm1.g1.e2) AS expr1, e3 FROM pm1.g1 GROUP BY e3) AS X__1 WHERE (e2 < X__1.expr1) AND (pm3.g1.e3 = X__1.e3)", RealMetadataFactory.example4(), cc);
    }

    @Test public void testSubqueryRewriteToJoin() throws Exception {
        CommandContext cc = new CommandContext();
        cc.setOptions(new Options().subqueryUnnestDefault(true));
        TestQueryRewriter.helpTestRewriteCommand("Select e1 from pm3.g1 where exists (select pm1.g1.e1 FROM pm1.g1 where e1 = pm3.g1.e1)", "SELECT e1 FROM pm3.g1, (SELECT e1 FROM pm1.g1) AS X__1 WHERE pm3.g1.e1 = X__1.e1", RealMetadataFactory.example4(), cc);
    }

    @Test public void testSubqueryRewriteToJoin1() throws Exception {
        TestQueryRewriter.helpTestRewriteCommand("Select e1 from pm3.g1 where pm3.g1.e1 in /*+ mj */ (select pm1.g1.e1 as x FROM pm1.g1)", "SELECT e1 FROM pm3.g1, (SELECT pm1.g1.e1 AS x FROM pm1.g1) AS X__1 WHERE pm3.g1.e1 = X__1.x", RealMetadataFactory.example4());

        //won't rewrite because of the limit
        TestQueryRewriter.helpTestRewriteCommand("Select e1 from pm3.g1 where pm3.g1.e1 in /*+ mj */ (select pm1.g1.e1 as x FROM pm1.g1 limit 1)", "SELECT e1 FROM pm3.g1 WHERE pm3.g1.e1 IN /*+ MJ */ (SELECT pm1.g1.e1 AS x FROM pm1.g1 LIMIT 1)", RealMetadataFactory.example4());
    }

    @Test public void testSubqueryRewriteToJoin2() throws Exception {
        TestQueryRewriter.helpTestRewriteCommand("Select e1 from pm3.g1 where pm3.g1.e1 in /*+ mj */ (select distinct pm1.g1.e1 || 1 FROM pm1.g1)", "SELECT e1 FROM pm3.g1, (SELECT DISTINCT concat(pm1.g1.e1, '1') AS expr1 FROM pm1.g1) AS X__1 WHERE pm3.g1.e1 = X__1.expr1", RealMetadataFactory.example4());
    }

    @Test public void testSubqueryRewriteToJoin2a() throws Exception {
        TestQueryRewriter.helpTestRewriteCommand("Select e1 from pm3.g1 where pm3.g1.e1 in /*+ mj */ (select pm1.g1.e1 || 1 FROM pm1.g1)", "SELECT e1 FROM pm3.g1, (SELECT DISTINCT concat(pm1.g1.e1, '1') AS expr1 FROM pm1.g1) AS X__1 WHERE pm3.g1.e1 = X__1.expr1", RealMetadataFactory.example4());
    }

    @Test public void testSubqueryRewriteToJoin2b() throws Exception {
        TestQueryRewriter.helpTestRewriteCommand("Select e1 from pm3.g1 where pm3.g1.e2 in /*+ mj */ (select pm1.g1.e2 FROM pm1.g1 where e3 = pm3.g1.e3)", "SELECT e1 FROM pm3.g1 WHERE pm3.g1.e2 IN /*+ MJ */ (SELECT pm1.g1.e2 FROM pm1.g1 WHERE e3 = pm3.g1.e3)", RealMetadataFactory.example4());
    }

    @Test public void testSubqueryRewriteToJoin2c() throws Exception {
        TestQueryRewriter.helpTestRewriteCommand("Select e1 from pm3.g1 where pm3.g1.e2 in /*+ mj */ (select pm1.g1.e2 FROM pm1.g1)", "SELECT e1 FROM pm3.g1, (SELECT DISTINCT pm1.g1.e2 FROM pm1.g1) AS X__1 WHERE pm3.g1.e2 = X__1.e2", RealMetadataFactory.example4());
    }

    /* the uniqueness must be on the IN and not the correlated variables */
    @Test public void testSubqueryRewriteToJoin2d() throws Exception {
        TestQueryRewriter.helpTestRewriteCommand("Select e1 from pm3.g1 where pm3.g1.e3 in /*+ mj */ (select pm1.g1.e3 FROM pm1.g1 where e1 = pm3.g1.e1)", "SELECT e1 FROM pm3.g1 WHERE pm3.g1.e3 IN /*+ MJ */ (SELECT pm1.g1.e3 FROM pm1.g1 WHERE e1 = pm3.g1.e1)", RealMetadataFactory.example4());
    }

    /**
     * Even though this situation is essentially the same as above, we don't yet handle it
     */
    @Test public void testSubqueryRewriteToJoin3() throws Exception {
        TestQueryRewriter.helpTestRewriteCommand("Select e1 from pm3.g1 where exists (select pm1.g1.e2 FROM pm1.g1 WHERE pm3.g1.e1 = pm1.g1.e1 || 1)", "SELECT e1 FROM pm3.g1 WHERE EXISTS (SELECT pm1.g1.e2 FROM pm1.g1 WHERE concat(pm1.g1.e1, '1') = pm3.g1.e1 LIMIT 1)", RealMetadataFactory.example4());
    }

    @Test public void testSubqueryRewriteToJoinWithOtherCriteria() throws Exception {
        TestQueryRewriter.helpTestRewriteCommand("Select e1 from pm3.g1 where pm3.g1.e1 in /*+ mj */ (select pm1.g1.e1 FROM pm1.g1 where e2 < pm3.g1.e2)", "SELECT e1 FROM pm3.g1, (SELECT pm1.g1.e1, e2 FROM pm1.g1) AS X__1 WHERE (X__1.e2 < pm3.g1.e2) AND (pm3.g1.e1 = X__1.e1)", RealMetadataFactory.example4());
    }

    @Test public void testDontRewriteToJoinWithOtherCriteria() throws Exception {
        CommandContext cc = new CommandContext();
        cc.setOptions(new Options().subqueryUnnestDefault(true));
        TestQueryRewriter.helpTestRewriteCommand("Select e1 from pm3.g1 where pm3.g1.e1 in /*+ NO_UNNEST */ (select pm1.g1.e1 FROM pm1.g1 where e2 < pm3.g1.e2)", "SELECT e1 FROM pm3.g1 WHERE pm3.g1.e1 IN /*+ NO_UNNEST */ (SELECT pm1.g1.e1 FROM pm1.g1 WHERE e2 < pm3.g1.e2)", RealMetadataFactory.example4(), cc);
    }

    @Test public void testSubqueryRewriteToJoinWithAggregate() throws Exception {
        CommandContext cc = new CommandContext();
        cc.setOptions(new Options().subqueryUnnestDefault(true));
        TestQueryRewriter.helpTestRewriteCommand("Select e1 from pm3.g1 where pm3.g1.e2 < (select max(e2) FROM pm1.g1 where pm3.g1.e1 = e1)", "SELECT e1 FROM pm3.g1, (SELECT MAX(e2) AS expr1, e1 FROM pm1.g1 GROUP BY e1) AS X__1 WHERE (pm3.g1.e2 < X__1.expr1) AND (pm3.g1.e1 = X__1.e1)", RealMetadataFactory.example4(), cc);
    }

    /**
     * A join will not be used since the predicate cannot be applied after the grouping
     * @throws Exception
     */
    @Test public void testSubqueryRewriteToJoinWithAggregate1() throws Exception {
        TestQueryRewriter.helpTestRewriteCommand("Select e1 from pm3.g1 where pm3.g1.e2 < (select max(e2) FROM pm1.g1 where pm3.g1.e1 = e1 and pm3.g1.e3 > e3)", "SELECT e1 FROM pm3.g1 WHERE pm3.g1.e2 < (SELECT MAX(e2) FROM pm1.g1 WHERE (e1 = pm3.g1.e1) AND (e3 < pm3.g1.e3))", RealMetadataFactory.example4());
    }

    @Test public void testSubqueryRewriteToJoinWithAggregate2() throws Exception {
        CommandContext cc = new CommandContext();
        cc.setOptions(new Options().subqueryUnnestDefault(true));
        TestQueryRewriter.helpTestRewriteCommand("Select e1 from pm3.g1 where pm3.g1.e2 < (select max(e2) FROM pm1.g1 WHERE pm3.g1.e1 = e1 HAVING min(e3) < pm3.g1.e3)", "SELECT e1 FROM pm3.g1, (SELECT MAX(e2) AS expr1, e1, MIN(e3) AS expr3 FROM pm1.g1 GROUP BY e1) AS X__1 WHERE (X__1.expr3 < pm3.g1.e3) AND (pm3.g1.e2 < X__1.expr1) AND (pm3.g1.e1 = X__1.e1)", RealMetadataFactory.example4(), cc);
    }

    @Test public void testSubqueryRewriteToJoinWithGroupingExpression() throws Exception {
        CommandContext cc = new CommandContext();
        cc.setOptions(new Options().subqueryUnnestDefault(true));
        TestQueryRewriter.helpTestRewriteCommand("Select distinct e1 from pm3.g1 where exists (select 1 FROM pm1.g1 group by e4 || 'x' HAVING min(e3) || (e4 || 'x') = pm3.g1.e3)", "SELECT DISTINCT e1 FROM pm3.g1, (SELECT MIN(e3) AS expr1, concat(convert(e4, string), 'x') AS expr2, concat(convert(MIN(e3), string), concat(convert(e4, string), 'x')) AS expr FROM pm1.g1 GROUP BY concat(convert(e4, string), 'x')) AS X__1 WHERE convert(pm3.g1.e3, string) = X__1.expr", RealMetadataFactory.example4(), cc);
    }

    @Test public void testSubqueryRewriteToJoinExistsNoKey() throws Exception {
        CommandContext cc = new CommandContext();
        cc.setOptions(new Options().subqueryUnnestDefault(true));
        TestQueryRewriter.helpTestRewriteCommand("Select e1 from pm1.g1 x where exists (select 1 FROM pm1.g2 where pm1.g2.e1 = x.e1)", "SELECT e1 FROM pm1.g1 AS x, (SELECT DISTINCT pm1.g2.e1 FROM pm1.g2) AS X__1 WHERE x.e1 = X__1.e1", RealMetadataFactory.example4(), cc);
    }

    /**
     * A join will not be used here because of the not
     * @throws Exception
     */
    @Test public void testSubqueryRewriteNot() throws Exception {
        TestQueryRewriter.helpTestRewriteCommand("Select e1 from pm3.g1 where pm3.g1.e2 not in (select e2 FROM pm1.g1 where pm3.g1.e1 = e1)", "SELECT e1 FROM pm3.g1 WHERE pm3.g1.e2 NOT IN (SELECT e2 FROM pm1.g1 WHERE e1 = pm3.g1.e1)", RealMetadataFactory.example4());
    }

    /**
     * A join will not be used here because of the all
     * @throws Exception
     */
    @Test public void testSubqueryRewriteAll() throws Exception {
        TestQueryRewriter.helpTestRewriteCommand("Select e1 from pm3.g1 where pm3.g1.e2 = all (select e2 FROM pm1.g1 where pm3.g1.e1 = e1)", "SELECT e1 FROM pm3.g1 WHERE pm3.g1.e2 = ALL (SELECT e2 FROM pm1.g1 WHERE e1 = pm3.g1.e1)", RealMetadataFactory.example4());
    }

    @Test public void testRewriteSubqueryCompare() throws Exception {
        TestQueryRewriter.helpTestRewriteCommand("select e1 from pm1.g1 where e1 <> ANY (select e1 from pm1.g1)", "SELECT e1 FROM pm1.g1 WHERE e1 <> SOME (SELECT e1 FROM pm1.g1)", RealMetadataFactory.example1Cached()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRewriteSubqueryCompare1() throws Exception {
        TestQueryRewriter.helpTestRewriteCommand("select e1 from pm1.g1 where e1 <> ALL (select e1 from pm1.g1)", "SELECT e1 FROM pm1.g1 WHERE e1 NOT IN (SELECT e1 FROM pm1.g1)", RealMetadataFactory.example1Cached()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRewriteSubqueryCompare2() throws Exception {
        TestQueryRewriter.helpTestRewriteCommand("select e1 from pm1.g1 where e1 = ANY (select e1 from pm1.g1)", "SELECT e1 FROM pm1.g1 WHERE e1 IN (SELECT e1 FROM pm1.g1)", RealMetadataFactory.example1Cached()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRewriteSubqueryCompare3() throws Exception {
        TestQueryRewriter.helpTestRewriteCommand("select e1 from pm1.g1 where e1 = ALL (select e1 from pm1.g1)", "SELECT e1 FROM pm1.g1 WHERE e1 = ALL (SELECT e1 FROM pm1.g1)", RealMetadataFactory.example1Cached()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testSubqueryExpressionJoin() throws Exception {
        CommandContext cc = new CommandContext();
        cc.setOptions(new Options().subqueryUnnestDefault(true));
        TestQueryRewriter.helpTestRewriteCommand("Select e1 from pm3.g1 where pm3.g1.e2 < (Select max(e2) from pm2.g2 where e1 = pm3.g1.e1 having convert(min(e2), string) > pm3.g1.e1)", "SELECT e1 FROM pm3.g1, (SELECT MAX(e2) AS expr1, e1, MIN(e2) AS expr3 FROM pm2.g2 GROUP BY e1) AS X__1 WHERE (convert(X__1.expr3, string) > pm3.g1.e1) AND (pm3.g1.e2 < X__1.expr1) AND (pm3.g1.e1 = X__1.e1)", RealMetadataFactory.example4(), cc);
    }

    /**
     * Must be handled as a semi-join, rather than a regular join
     */
    @Test public void testSemiJoin() throws Exception {
        ProcessorPlan plan = helpPlan("Select e1 from pm2.g2 where e2 in /*+ mj */ (select count(e2) FROM pm1.g2 group by e1 having e1 < pm2.g2.e3)", RealMetadataFactory.example4(),  //$NON-NLS-1$
            new String[] { "SELECT g_0.e2 AS c_0, g_0.e3 AS c_1, g_0.e1 AS c_2 FROM pm2.g2 AS g_0 WHERE g_0.e2 IN (<dependent values>) ORDER BY c_0" }, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        checkNodeTypes(plan, new int[] {
            0,      // Access
            1,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            1,      // MergeJoinStrategy
            0,      // Null
            1,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
        checkJoinCounts(plan, 1, 0);
    }

    /**
     * Here the merge join prevents us from using a semi join merge join
     * @throws TeiidProcessingException
     * @throws TeiidComponentException
     */
    @Test public void testSemiJoinUnderJoin() throws TeiidComponentException, TeiidProcessingException {
        ProcessorPlan plan = helpPlan("Select pm2.g2.e1 from pm1.g1 inner join pm2.g2 on (pm1.g1.e1 = pm2.g2.e1) where pm2.g2.e2 in /*+ mj */ (select count(e2) FROM pm1.g2 group by e1 having e1 < pm2.g2.e3)", RealMetadataFactory.example1Cached(),  //$NON-NLS-1$
            new String[] { "SELECT g_0.e2 AS c_0, g_0.e3 AS c_1, g_0.e1 AS c_2 FROM pm2.g2 AS g_0 ORDER BY c_2", "SELECT g_0.e1 AS c_0 FROM pm1.g1 AS g_0 ORDER BY c_0" }, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            1,      // DependentSelect
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
        checkJoinCounts(plan, 0, 0);
    }

    /**
     * This will not plan as a anti semi-join since the cost seems too high
     */
    @Test public void testNoAntiSemiJoinExistsCosting() {
        ProcessorPlan plan = helpPlan("Select e1 from pm1.g2 as o where not exists (select 1 from pm3.g1 where e1 = o.e1 having o.e2 = count(e2))", RealMetadataFactory.example4(),  //$NON-NLS-1$
            new String[] { "SELECT g_0.e1, g_0.e2 FROM pm1.g2 AS g_0" }); //$NON-NLS-1$
        checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            1,      // DependentSelect
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
     * Same as above, but the source is much larger, so a semi-join is favorable
     */
    @Test public void testSemiJoinExistsCosting() {
        ProcessorPlan plan = helpPlan("Select e1 from pm2.g2 as o where not exists (select 1 from pm3.g1 where e1 = o.e1 having o.e2 = count(e2))", RealMetadataFactory.example4(),  //$NON-NLS-1$
            new String[] { "SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm2.g2 AS g_0 ORDER BY c_0, c_1" }); //$NON-NLS-1$
        checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            1,      // MergeJoinStrategy
            0,      // Null
            1,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
        checkJoinCounts(plan, 0, 1);
    }

    @Test public void testAntiSemiJoinExistsHint() {
        ProcessorPlan plan = helpPlan("Select e1 from pm1.g2 as o where not exists /*+ MJ */ (select 1 from pm3.g1 where e1 = o.e1 having o.e2 = count(e2))", RealMetadataFactory.example4(),  //$NON-NLS-1$
            new String[] { "SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm1.g2 AS g_0 ORDER BY c_0, c_1" }); //$NON-NLS-1$
        checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            1,      // MergeJoinStrategy
            0,      // Null
            1,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
        checkJoinCounts(plan, 0, 1);
    }

    @Test public void testAntiSemiJoinInHint() throws Exception {
        TransformationMetadata example4 = RealMetadataFactory.example4();
        example4.getElementID("pm3.g1.e2").setNullType(NullType.No_Nulls);
        ProcessorPlan plan = helpPlan("Select e1 from pm1.g2 as o where e2 NOT IN /*+ MJ */ (select e2 from pm3.g1 where e1 = o.e1)", example4,  //$NON-NLS-1$
            new String[] { "SELECT g_0.e2 AS c_0, g_0.e1 AS c_1 FROM pm1.g2 AS g_0 ORDER BY c_1, c_0" }, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            1,      // MergeJoinStrategy
            0,      // Null
            1,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
        checkJoinCounts(plan, 0, 1);
    }

    @Test public void testAntiSemiJoinInHint1() throws Exception {
        ProcessorPlan plan = helpPlan("Select e1 from pm1.g2 as o where e2 <> /*+ MJ */ (select max(e2) from pm3.g1 where e1 = o.e1)", RealMetadataFactory.example4(),  //$NON-NLS-1$
            new String[] { "SELECT g_0.e2 AS c_0, g_0.e1 AS c_1 FROM pm1.g2 AS g_0 ORDER BY c_1", "SELECT g_0.e1, g_0.e2 FROM pm3.g1 AS g_0 WHERE g_0.e1 IN (<dependent values>)" }, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
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
            2,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
        //can be just a join as only one row from the right is possible for each row on the left
        checkJoinCounts(plan, 0, 0);
    }

    void checkJoinCounts(ProcessorPlan plan, int semi, int antiSemi) {
        checkNodeTypes(plan, new int[] {semi, antiSemi}, new Class[] {SemiJoin.class, AntiSemiJoin.class});
    }

    @Test public void testNonSemiJoin() throws Exception {
        ProcessorPlan plan = helpPlan("Select x from texttable('a,b,c' COLUMNS x string, c2 string) as t where x = (select count(e2) FROM pm1.g2)", RealMetadataFactory.example4(),  //$NON-NLS-1$
            new String[] {}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        checkNodeTypes(plan, new int[] {
            0,      // Access
            0,      // DependentAccess
            1,      // DependentSelect
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
     * Test to ensure that we don't create an invalid semijoin query when attempting to convert the subquery to a semijoin
     */
    @Test public void testGeneratedSemijoinQuery() throws Exception {
        String sql = "SELECT intkey FROM BQT1.SmallA AS A WHERE convert(shortvalue, integer) = /*+ MJ */ (SELECT MAX(convert(shortvalue, integer)) FROM (select * from BQT1.SmallA) AS B WHERE b.intnum = a.intnum) ORDER BY intkey";
        TestQueryRewriter.helpTestRewriteCommand(sql, "SELECT intkey FROM BQT1.SmallA AS A, (SELECT MAX(convert(shortvalue, integer)) AS expr1, b.intnum FROM (SELECT BQT1.SmallA.IntKey, BQT1.SmallA.StringKey, BQT1.SmallA.IntNum, BQT1.SmallA.StringNum, BQT1.SmallA.FloatNum, BQT1.SmallA.LongNum, BQT1.SmallA.DoubleNum, BQT1.SmallA.ByteNum, BQT1.SmallA.DateValue, BQT1.SmallA.TimeValue, BQT1.SmallA.TimestampValue, BQT1.SmallA.BooleanValue, BQT1.SmallA.CharValue, BQT1.SmallA.ShortValue, BQT1.SmallA.BigIntegerValue, BQT1.SmallA.BigDecimalValue, BQT1.SmallA.ObjectValue FROM BQT1.SmallA) AS B GROUP BY b.intnum) AS X__1 WHERE (a.intnum = X__1.IntNum) AND (convert(shortvalue, integer) = X__1.expr1) ORDER BY intkey", RealMetadataFactory.exampleBQTCached());
    }

    @Test public void testGeneratedSemijoinQuery1() throws Exception {
        TestQueryRewriter.helpTestRewriteCommand("Select e1 from pm3.g1 where pm3.g1.e2 IN /*+ mj */ (Select max(e2) from pm2.g2 where e1 = pm3.g1.e1)", "SELECT e1 FROM pm3.g1, (SELECT MAX(e2) AS expr1, e1 FROM pm2.g2 GROUP BY e1) AS X__1 WHERE (pm3.g1.e1 = X__1.e1) AND (pm3.g1.e2 = X__1.expr1)", RealMetadataFactory.example4());
    }

    @Test public void testCompareSubquery2() throws Exception {
        ProcessorPlan plan = helpPlan("Select e1 from pm1.g1 where e1 <= some (select e1 FROM pm2.g1)", example1(),  //$NON-NLS-1$
            new String[] { "SELECT g_0.e1 FROM pm1.g1 AS g_0 WHERE g_0.e1 <= (SELECT MAX(X.e1) FROM (SELECT e1 FROM pm2.g1) AS X)" }, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testUncorrelatedSet() {
        ProcessorPlan plan = helpPlan("Select e1 from pm1.g1 where e1 in /*+ mj */ (select e1 FROM pm2.g1)", RealMetadataFactory.example1Cached(),  //$NON-NLS-1$
            new String[] { "SELECT DISTINCT g_0.e1 FROM pm2.g1 AS g_0", "SELECT g_0.e1 AS c_0 FROM pm1.g1 AS g_0 ORDER BY c_0" }); //$NON-NLS-1$
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
        checkJoinCounts(plan, 0, 0);
    }

    /**
     * Shows the default preference against on subquery
     */
    @Test public void testSubuqeryOn() throws Exception {
        BasicSourceCapabilities bsc = getTypicalCapabilities();
        bsc.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        bsc.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, true);
        bsc.setSourceProperty(Capability.JOIN_CRITERIA_ALLOWED, ExecutionFactory.SupportedJoinCriteria.ANY);
        bsc.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, true);
        bsc.setCapabilitySupport(Capability.CRITERIA_EXISTS, true);
        bsc.setCapabilitySupport(Capability.QUERY_FROM_ANSI_JOIN, true);
        bsc.setCapabilitySupport(Capability.CRITERIA_ON_SUBQUERY, true);
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT 1 FROM bqt1.smalla as Y93 INNER JOIN bqt1.smallb as AG5 ON 1 = 1 WHERE EXISTS (SELECT 'Y' FROM bqt1.mediuma WHERE AG5.intkey = 1 AND Y93.intkey = 1 )", //$NON-NLS-1$
                                      RealMetadataFactory.exampleBQTCached(), null, new DefaultCapabilitiesFinder(bsc),
                                      new String[] {
                                          "SELECT 1 FROM BQT1.SmallA AS g_0 CROSS JOIN BQT1.SmallB AS g_1 WHERE EXISTS (SELECT 'Y' FROM BQT1.MediumA AS g_2 WHERE (g_1.IntKey = 1) AND (g_0.IntKey = 1))"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        TestOptimizer.checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    /**
     * Shows the pushdown is inhibited due to lack of support
     */
    @Test public void testSubuqeryOn1() throws Exception {
        BasicSourceCapabilities bsc = getTypicalCapabilities();
        bsc.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        bsc.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, true);
        bsc.setSourceProperty(Capability.JOIN_CRITERIA_ALLOWED, ExecutionFactory.SupportedJoinCriteria.ANY);
        bsc.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, true);
        bsc.setCapabilitySupport(Capability.CRITERIA_EXISTS, true);
        bsc.setCapabilitySupport(Capability.QUERY_FROM_ANSI_JOIN, true);
        TestOptimizer.helpPlan("SELECT 1 FROM bqt1.smalla as Y93 LEFT OUTER JOIN bqt1.smallb as AG5 ON EXISTS (SELECT 'Y' FROM bqt1.mediuma WHERE AG5.intkey = 1 AND Y93.intkey = 1 )", //$NON-NLS-1$
                                      RealMetadataFactory.exampleBQTCached(), null, new DefaultCapabilitiesFinder(bsc),
                                      new String[] {
                                          "SELECT g_0.IntKey FROM BQT1.SmallA AS g_0", "SELECT g_0.IntKey FROM BQT1.SmallB AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    }

    /**
     * Shows pushdown of on subquery with support
     */
    @Test public void testSubuqeryOn2() throws Exception {
        BasicSourceCapabilities bsc = getTypicalCapabilities();
        bsc.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        bsc.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, true);
        bsc.setSourceProperty(Capability.JOIN_CRITERIA_ALLOWED, ExecutionFactory.SupportedJoinCriteria.ANY);
        bsc.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, true);
        bsc.setCapabilitySupport(Capability.CRITERIA_EXISTS, true);
        bsc.setCapabilitySupport(Capability.QUERY_FROM_ANSI_JOIN, true);
        bsc.setCapabilitySupport(Capability.CRITERIA_ON_SUBQUERY, true);
        TestOptimizer.helpPlan("SELECT 1 FROM bqt1.smalla as Y93 LEFT OUTER JOIN bqt1.smallb as AG5 ON EXISTS (SELECT 'Y' FROM bqt1.mediuma WHERE AG5.intkey = 1 AND Y93.intkey = 1 )", //$NON-NLS-1$
                                      RealMetadataFactory.exampleBQTCached(), null, new DefaultCapabilitiesFinder(bsc),
                                      new String[] {
                                          "SELECT 1 FROM BQT1.SmallA AS g_0 LEFT OUTER JOIN BQT1.SmallB AS g_1 ON EXISTS (SELECT 'Y' FROM BQT1.MediumA AS g_2 WHERE (g_1.IntKey = 1) AND (g_0.IntKey = 1))"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$


    }

    /**
     * Shows the uncorrelated subquery is evaluated ahead of time
     */
    @Test public void testCorrelatedOnly() throws Exception {
        BasicSourceCapabilities bsc = getTypicalCapabilities();
        bsc.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, true);
        bsc.setCapabilitySupport(Capability.QUERY_SUBQUERIES_ONLY_CORRELATED, true);
        bsc.setCapabilitySupport(Capability.CRITERIA_EXISTS, true);
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT 1 FROM bqt1.smalla where EXISTS (SELECT 'Y' FROM bqt1.mediuma)", //$NON-NLS-1$
                                      RealMetadataFactory.exampleBQTCached(), null, new DefaultCapabilitiesFinder(bsc),
                                      new String[] {
                                          "SELECT 1 FROM BQT1.SmallA AS g_0 WHERE EXISTS (SELECT 'Y' FROM BQT1.MediumA AS g_0)"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        HardcodedDataManager hcdm = new HardcodedDataManager(false);
        TestProcessor.helpProcess(plan, hcdm, null);
        assertEquals("SELECT 'Y' FROM BQT1.MediumA AS g_0", hcdm.getCommandHistory().get(0).toString());
        assertEquals("SELECT 1 FROM BQT1.SmallA AS g_0", hcdm.getCommandHistory().get(1).toString());
    }

    /**
     * Detect if a subquery should prevent pushdown
     */
    @Test public void testDeleteSubquery() throws Exception {
        BasicSourceCapabilities bsc = getTypicalCapabilities();
        bsc.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);
        TestOptimizer.helpPlan("delete FROM bqt1.smalla where intkey in (select cast(stringkey as integer) from bqt1.smallb)", //$NON-NLS-1$
                                      RealMetadataFactory.exampleBQTCached(), null, new DefaultCapabilitiesFinder(bsc),
                                      null, false); //$NON-NLS-1$
    }

    @Test public void testDeleteSubqueryCorrelated() throws Exception {
        BasicSourceCapabilities bsc = getTypicalCapabilities();
        bsc.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, true);
        bsc.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, true);
        TestOptimizer.helpPlan("delete FROM bqt1.smalla x where intkey = (select intkey from bqt1.smallb where intkey < x.intkey)", //$NON-NLS-1$
                                      RealMetadataFactory.exampleBQTCached(), null, new DefaultCapabilitiesFinder(bsc),
                                      new String[] {"DELETE FROM BQT1.SmallA WHERE BQT1.SmallA.IntKey = (SELECT g_0.IntKey FROM BQT1.SmallB AS g_0 WHERE g_0.IntKey < BQT1.SmallA.IntKey)"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    }

    @Test public void testDeleteSubqueryCorrelatedCompensated() throws Exception {
        BasicSourceCapabilities bsc = getTypicalCapabilities();
        ProcessorPlan plan = TestOptimizer.helpPlan("delete FROM pm1.g1 x where e1 = 'a' and e3 = (select e3 from pm1.g2 where e2 < x.e2)", //$NON-NLS-1$
                                      RealMetadataFactory.example4(), null, new DefaultCapabilitiesFinder(bsc),
                                      new String[] {}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        HardcodedDataManager hcdm = new HardcodedDataManager();
        hcdm.addData("SELECT g_0.e3, g_0.e2, g_0.e1 FROM pm1.g1 AS g_0 WHERE g_0.e1 = 'a'", Arrays.asList(true, 1, 'a'));
        hcdm.addData("SELECT g_0.e3 FROM pm1.g2 AS g_0 WHERE g_0.e2 < 1", Arrays.asList(true));
        hcdm.addData("DELETE FROM pm1.g1 WHERE pm1.g1.e1 = 'a'", Arrays.asList(1));
        TestProcessor.helpProcess(plan, hcdm, null);
    }

    @Test public void testSubqueryPlan() throws Exception {
        BasicSourceCapabilities bsc = getTypicalCapabilities();
        ProcessorPlan plan = TestOptimizer.helpPlan("select 1, (select cast(stringkey as integer) from bqt1.smallb where intkey = smalla.intkey) from bqt1.smalla", //$NON-NLS-1$
                                      RealMetadataFactory.exampleBQTCached(), null, new DefaultCapabilitiesFinder(bsc),
                                      new String[] {"SELECT g_0.IntKey FROM BQT1.SmallA AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        assertEquals(6, plan.getDescriptionProperties().getProperties().size());
    }

    @Test public void testCorrelatedGroupingExpression() throws Exception {
        BasicSourceCapabilities bsc = getTypicalCapabilities();
        bsc.setCapabilitySupport(Capability.QUERY_AGGREGATES_AVG, true);
        bsc.setCapabilitySupport(Capability.QUERY_GROUP_BY, true);
        bsc.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, true);
        bsc.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, true);
        bsc.setCapabilitySupport(Capability.QUERY_FROM_INLINE_VIEWS, true);
        bsc.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR_PROJECTION, true);
        ProcessorPlan plan = TestOptimizer.helpPlan("select intkey, (select avg(intkey) from bqt1.smallb where intkey = smalla.intkey) from bqt1.smalla group by intkey", //$NON-NLS-1$
                                      RealMetadataFactory.exampleBQTCached(), null, new DefaultCapabilitiesFinder(bsc),
                                      new String[] {"SELECT g_0.IntKey, (SELECT AVG(g_1.IntKey) FROM BQT1.SmallB AS g_1 WHERE g_1.IntKey = g_0.IntKey) FROM BQT1.SmallA AS g_0 GROUP BY g_0.IntKey"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    @Test public void testSubqueryInWhereClause1() throws TeiidComponentException, TeiidProcessingException {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_INLINE_VIEWS, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan("Select e1 from pm1.g1 where e1 in /*+ mj */ (select e1 FROM pm1.g2)", example1(),  //$NON-NLS-1$
            null, capFinder,
            new String[] { "SELECT g_0.e1 FROM pm1.g1 AS g_0, (SELECT DISTINCT g_1.e1 AS c_0 FROM pm1.g2 AS g_1) AS v_0 WHERE g_0.e1 = v_0.c_0"}, ComparisonMode.EXACT_COMMAND_STRING ); //$NON-NLS-1$
        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testPushSubqueryInWhereClause2() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan("Select e1 from pm1.g1 where e1 in /*+ no_unnest */ (select max(e1) FROM pm1.g2)", example1(),  //$NON-NLS-1$
            null, capFinder,
            new String[] { "SELECT e1 FROM pm1.g1 WHERE e1 IN /*+ NO_UNNEST */ (SELECT MAX(e1) FROM pm1.g2)" }, SHOULD_SUCCEED); //$NON-NLS-1$
        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    /**
     * Check that subquery is pushed if the subquery selects a function that is pushed
     */
    @Test public void testPushSubqueryInWhereClause3() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);
        caps.setFunctionSupport("ltrim", true); //$NON-NLS-1$
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", new BasicSourceCapabilities()); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan("Select e1 from pm1.g1 where e1 in /*+ NO_UNNEST */ (SELECT ltrim(e1) FROM pm1.g2)", RealMetadataFactory.example1Cached(),  //$NON-NLS-1$
            null, capFinder,
            new String[] { "SELECT e1 FROM pm1.g1 WHERE e1 IN /*+ NO_UNNEST */ (SELECT ltrim(e1) FROM pm1.g2)" }, SHOULD_SUCCEED); //$NON-NLS-1$
        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    /**
     * Check that subquery is pushed if the subquery selects an aliased function that is pushed
     */
    @Test public void testPushSubqueryInWhereClause4() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);
        caps.setFunctionSupport("ltrim", true); //$NON-NLS-1$
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", new BasicSourceCapabilities()); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan("Select e1 from pm1.g1 where e1 in /*+ NO_UNNEST */ (SELECT ltrim(e1) as m FROM pm1.g2)", RealMetadataFactory.example1Cached(),  //$NON-NLS-1$
            null, capFinder,
            new String[] { "SELECT e1 FROM pm1.g1 WHERE e1 IN /*+ NO_UNNEST */ (SELECT ltrim(e1) FROM pm1.g2)" }, SHOULD_SUCCEED); //$NON-NLS-1$

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    /** Case 1456, defect 10492 */
    @Test public void testAliasingDefect1() throws Exception{
        // Create query
        String sql = "SELECT e1 FROM vm1.g1 X WHERE e2 = /*+ NO_UNNEST */ (SELECT MAX(e2) FROM vm1.g1 Y WHERE X.e1 = Y.e1)";//$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql, RealMetadataFactory.example1Cached(),
            null, capFinder,
            new String[] { "SELECT g_0.e1 FROM pm1.g1 AS g_0 WHERE g_0.e2 = /*+ NO_UNNEST */ (SELECT MAX(g_1.e2) FROM pm1.g1 AS g_1 WHERE g_1.e1 = g_0.e1)" }, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    /** Case 1456, defect 10492*/
    @Test public void testAliasingDefect2() throws TeiidComponentException, TeiidProcessingException{
        // Create query
        String sql = "SELECT X.e1 FROM vm1.g1 X, vm1.g1 Z WHERE X.e2 = /*+ NO_UNNEST */ (SELECT MAX(e2) FROM vm1.g1 Y WHERE X.e1 = Y.e1 AND Y.e2 = Z.e2) AND X.e1 = Z.e1";//$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        ProcessorPlan plan = helpPlan(sql, metadata,
            null, capFinder,
            new String[] { "SELECT g_0.e1 FROM pm1.g1 AS g_0, pm1.g1 AS g_1 WHERE (g_0.e1 = g_1.e1) AND (g_0.e2 = /*+ NO_UNNEST */ (SELECT MAX(g_2.e2) FROM pm1.g1 AS g_2 WHERE (g_2.e1 = g_0.e1) AND (g_2.e2 = g_1.e2)))" }, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    /** Case 1456, defect 10492*/
    @Test public void testAliasingDefect3() throws Exception {
        // Create query
        String sql = "SELECT X.e1 FROM pm1.g2, vm1.g1 X WHERE X.e2 = ALL (SELECT MAX(e2) FROM vm1.g1 Y WHERE X.e1 = Y.e1) AND X.e1 = pm1.g2.e1";//$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.CRITERIA_QUANTIFIED_ALL, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        ProcessorPlan plan = helpPlan(sql, metadata,
            null, capFinder,
            new String[] { "SELECT g_1.e1 FROM pm1.g2 AS g_0, pm1.g1 AS g_1 WHERE (g_1.e1 = g_0.e1) AND (g_1.e2 = ALL (SELECT MAX(g_2.e2) FROM pm1.g1 AS g_2 WHERE g_2.e1 = g_1.e1))" }, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    /**
     * Shows the default preference against on subquery
     */
    @Test public void testSubuqeryLimit() throws Exception {
        BasicSourceCapabilities bsc = getTypicalCapabilities();
        bsc.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, true);
        bsc.setCapabilitySupport(Capability.CRITERIA_EXISTS, true);
        bsc.setCapabilitySupport(Capability.ROW_LIMIT, true);
        TestOptimizer.helpPlan("SELECT 1 FROM bqt1.smalla WHERE EXISTS (SELECT 'Y' FROM bqt1.mediuma WHERE bqt1.smalla.intkey = bqt1.mediuma.intnum order by stringkey limit 1 )", //$NON-NLS-1$
                                      RealMetadataFactory.exampleBQTCached(), null, new DefaultCapabilitiesFinder(bsc),
                                      new String[] {
                                          "SELECT g_0.IntKey FROM BQT1.SmallA AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        bsc.setCapabilitySupport(Capability.SUBQUERY_CORRELATED_LIMIT, true);
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT 1 FROM bqt1.smalla WHERE EXISTS (SELECT 'Y' FROM bqt1.mediuma WHERE bqt1.smalla.intkey = bqt1.mediuma.intnum order by stringkey limit 1 )", //$NON-NLS-1$
                                      RealMetadataFactory.exampleBQTCached(), null, new DefaultCapabilitiesFinder(bsc),
                                      new String[] {
                                          "SELECT 1 FROM BQT1.SmallA AS g_0 WHERE EXISTS (SELECT 'Y' AS c_0 FROM BQT1.MediumA AS g_1 WHERE g_1.IntNum = g_0.IntKey ORDER BY g_1.StringKey LIMIT 1)"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testNestedSubquerySemiJoin() throws Exception {
        String sql = "SELECT intkey FROM BQT1.SmallA AS A WHERE INTKEY IN /*+ mj */ (SELECT CONVERT(STRINGKEY, INTEGER) FROM BQT1.SMALLA AS A WHERE STRINGKEY IN (SELECT CONVERT(INTKEY, STRING) FROM BQT1.SMALLA AS B WHERE A.INTNUM = B.INTNUM))";

        BasicSourceCapabilities bsc = getTypicalCapabilities();

        ProcessorPlan plan = TestOptimizer.helpPlan(sql,
                RealMetadataFactory.exampleBQTCached(), null, new DefaultCapabilitiesFinder(bsc),
                new String[] {
                    "SELECT g_0.IntKey AS c_0 FROM BQT1.SmallA AS g_0 WHERE g_0.IntKey IN (<dependent values>) ORDER BY c_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        HardcodedDataManager hdm = new HardcodedDataManager();
        hdm.addData("SELECT g_0.StringKey, g_0.IntNum FROM BQT1.SmallA AS g_0", Arrays.asList("1", 1), Arrays.asList("2", 2));
        hdm.addData("SELECT g_0.IntKey AS c_0 FROM BQT1.SmallA AS g_0 WHERE g_0.IntKey IN (1, 2) ORDER BY c_0", Arrays.asList(1));
        hdm.addData("SELECT g_0.IntKey FROM BQT1.SmallA AS g_0 WHERE g_0.IntNum = 1", Arrays.asList(1));
        hdm.addData("SELECT g_0.IntKey FROM BQT1.SmallA AS g_0 WHERE g_0.IntNum = 2", Arrays.asList(2));

        TestProcessor.helpProcess(plan, hdm, new List[] {Arrays.asList(1)} );

    }

    /**
     * Similar to the above, but uses a view for the most outer reference, which was
     * causing the middle table reference to be inappropriately replaced.
     * @throws Exception
     */
    @Test public void testNestedSubquerySemiJoin1() throws Exception {
        String sql = "SELECT intkey FROM (select * from bqt1.smalla) AS A WHERE INTKEY IN /*+ mj */ (SELECT CONVERT(STRINGKEY, INTEGER) FROM bqt1.smalla AS A WHERE STRINGKEY IN (SELECT CONVERT(INTKEY, STRING) FROM BQT1.SMALLA AS B WHERE A.INTNUM = B.INTNUM))";

        BasicSourceCapabilities bsc = getTypicalCapabilities();

        ProcessorPlan plan = TestOptimizer.helpPlan(sql,
                RealMetadataFactory.exampleBQTCached(), null, new DefaultCapabilitiesFinder(bsc),
                new String[] {
                    "SELECT g_0.IntKey AS c_0 FROM BQT1.SmallA AS g_0 WHERE g_0.IntKey IN (<dependent values>) ORDER BY c_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        HardcodedDataManager hdm = new HardcodedDataManager();
        hdm.addData("SELECT g_0.StringKey, g_0.IntNum FROM BQT1.SmallA AS g_0", Arrays.asList("1", 1), Arrays.asList("2", 2));
        hdm.addData("SELECT g_0.IntKey AS c_0 FROM BQT1.SmallA AS g_0 WHERE g_0.IntKey IN (1, 2) ORDER BY c_0", Arrays.asList(1));
        hdm.addData("SELECT g_0.IntKey FROM BQT1.SmallA AS g_0 WHERE g_0.IntNum = 1", Arrays.asList(1));
        hdm.addData("SELECT g_0.IntKey FROM BQT1.SmallA AS g_0 WHERE g_0.IntNum = 2", Arrays.asList(2));

        TestProcessor.helpProcess(plan, hdm, new List[] {Arrays.asList(1)} );

    }

    @Test public void testNestedSubquerySemiJoin2() throws Exception {
        String sql = "SELECT intkey FROM bqt1.smalla AS A WHERE INTKEY IN /*+ mj */ (SELECT CONVERT(STRINGKEY, INTEGER) FROM (select * from bqt1.smalla) AS A WHERE STRINGKEY IN (SELECT CONVERT(INTKEY, STRING) FROM BQT1.SMALLA AS B WHERE A.INTNUM = B.INTNUM))";

        BasicSourceCapabilities bsc = getTypicalCapabilities();

        ProcessorPlan plan = TestOptimizer.helpPlan(sql,
                RealMetadataFactory.exampleBQTCached(), null, new DefaultCapabilitiesFinder(bsc),
                new String[] {
                    "SELECT g_0.IntKey AS c_0 FROM BQT1.SmallA AS g_0 WHERE g_0.IntKey IN (<dependent values>) ORDER BY c_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        HardcodedDataManager hdm = new HardcodedDataManager();
        hdm.addData("SELECT g_0.StringKey, g_0.IntNum FROM BQT1.SmallA AS g_0", Arrays.asList("1", 1), Arrays.asList("2", 2));
        hdm.addData("SELECT g_0.IntKey AS c_0 FROM BQT1.SmallA AS g_0 WHERE g_0.IntKey IN (1, 2) ORDER BY c_0", Arrays.asList(1));
        hdm.addData("SELECT g_0.IntKey FROM BQT1.SmallA AS g_0 WHERE g_0.IntNum = 1", Arrays.asList(1));
        hdm.addData("SELECT g_0.IntKey FROM BQT1.SmallA AS g_0 WHERE g_0.IntNum = 2", Arrays.asList(2));

        TestProcessor.helpProcess(plan, hdm, new List[] {Arrays.asList(1)} );

    }

    @Test public void testAliasConflict() throws Exception {
        String sql = "select * from ( SELECT ( SELECT x.e1 FROM pm1.g1 AS x WHERE x.e2 = g_0.e2 ) AS c_2 FROM pm1.g2 AS g_0 ) AS v_0";

        BasicSourceCapabilities bsc = getTypicalCapabilities();
        bsc.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, true);
        bsc.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, true);
        bsc.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR_PROJECTION, true);
        TestOptimizer.helpPlan(sql, //$NON-NLS-1$
                                      RealMetadataFactory.example1Cached(), null, new DefaultCapabilitiesFinder(bsc),
                                      new String[] {
                                          "SELECT (SELECT g_1.e1 FROM pm1.g1 AS g_1 WHERE g_1.e2 = g_0.e2) FROM pm1.g2 AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    }

    @Test public void testPreEvaluationInAggregate() throws Exception {
        TransformationMetadata tm = RealMetadataFactory.fromDDL("x",
                new DDLHolder("my", "CREATE foreign TABLE test_b (b integer, c integer)"),
                new DDLHolder("pg", "CREATE foreign TABLE test_a (a integer, b integer); CREATE foreign TABLE test_only_pg (a integer, b integer);"));

        String sql = "SELECT SUM(x.b - (SELECT a FROM pg.test_only_pg WHERE b = 1)) FROM my.test_b x INNER JOIN pg.test_a y ON x.b = y.b";

        BasicSourceCapabilities bsc = getTypicalCapabilities();
        bsc.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, true);
        bsc.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR_PROJECTION, true);
        bsc.setCapabilitySupport(Capability.QUERY_GROUP_BY, true);
        bsc.setCapabilitySupport(Capability.QUERY_AGGREGATES_SUM, true);
        bsc.setFunctionSupport("-", true);
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, //$NON-NLS-1$
                                      tm, null, new DefaultCapabilitiesFinder(bsc),
                                      new String[] {
                                          "SELECT g_0.b AS c_0, SUM((g_0.b - (SELECT a FROM pg.test_only_pg WHERE b = 1 LIMIT 2))) AS c_1 FROM my.test_b AS g_0 GROUP BY g_0.b ORDER BY c_0", "SELECT g_0.b AS c_0 FROM pg.test_a AS g_0 ORDER BY c_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        HardcodedDataManager hdm = new HardcodedDataManager(tm);
        hdm.addData("SELECT g_0.a FROM test_only_pg AS g_0 WHERE g_0.b = 1", Arrays.asList(2));
        hdm.addData("SELECT g_0.b AS c_0, SUM((g_0.b - 2)) AS c_1 FROM test_b AS g_0 GROUP BY g_0.b ORDER BY c_0", Arrays.asList(3, 1));
        hdm.addData("SELECT g_0.b AS c_0 FROM test_a AS g_0 ORDER BY c_0", Arrays.asList(3));
        CommandContext cc = TestProcessor.createCommandContext();
        cc.setMetadata(tm);
        TestProcessor.helpProcess(plan, cc, hdm, new List[] {Arrays.asList(Long.valueOf(1))} );
    }

    @Test public void testPreEvaluationInAggregate1() throws Exception {
        TransformationMetadata tm = RealMetadataFactory.fromDDL("x",
                new DDLHolder("my", "CREATE foreign TABLE test_b (b integer, c integer)"),
                new DDLHolder("pg", "CREATE foreign TABLE test_a (a integer, b integer); CREATE foreign TABLE test_only_pg (a integer, b integer);"));

        String sql = "SELECT SUM(x.b - (SELECT a FROM pg.test_only_pg WHERE b = 1)) FROM my.test_b x";

        BasicSourceCapabilities bsc = getTypicalCapabilities();
        bsc.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, true);
        bsc.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR_PROJECTION, true);
        bsc.setCapabilitySupport(Capability.QUERY_GROUP_BY, true);
        bsc.setCapabilitySupport(Capability.QUERY_AGGREGATES_SUM, true);
        bsc.setFunctionSupport("-", true);
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, //$NON-NLS-1$
                                      tm, null, new DefaultCapabilitiesFinder(bsc),
                                      new String[] {
                                          "SELECT SUM((g_0.b - (SELECT a FROM pg.test_only_pg WHERE b = 1 LIMIT 2))) FROM my.test_b AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        HardcodedDataManager hdm = new HardcodedDataManager(tm);
        hdm.addData("SELECT g_0.a FROM test_only_pg AS g_0 WHERE g_0.b = 1", Arrays.asList(2));
        hdm.addData("SELECT SUM((g_0.b - 2)) FROM test_b AS g_0", Arrays.asList(Long.valueOf(3)));
        CommandContext cc = TestProcessor.createCommandContext();
        cc.setMetadata(tm);
        TestProcessor.helpProcess(plan, cc, hdm, new List[] {Arrays.asList(Long.valueOf(3))} );
    }

    @Test public void testNestedCorrelation() throws Exception {
        TransformationMetadata tm = RealMetadataFactory.fromDDL("CREATE foreign TABLE a (c1 integer, c2 integer); "
                + "CREATE foreign TABLE b (c3 integer, c4 integer); CREATE foreign TABLE c (c5 integer, c6 integer);", "x", "y");

        String sql = "SELECT (select c2 from b where c3 = (select c5 from c where c6 = c1)) FROM a group by c1, c2";

        BasicSourceCapabilities bsc = getTypicalCapabilities();
        /*ProcessorPlan plan = TestOptimizer.helpPlan(sql, //$NON-NLS-1$
                                      tm, null, new DefaultCapabilitiesFinder(bsc),
                                      new String[] {
                                          "SELECT g_0.c1, g_0.c2 FROM y.a AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        */
        HardcodedDataManager hdm = new HardcodedDataManager(tm);
        hdm.addData("SELECT g_0.c1, g_0.c2 FROM a AS g_0", Arrays.asList(1, 2));
        hdm.addData("SELECT g_0.c5 FROM c AS g_0 WHERE g_0.c6 = 1", Arrays.asList(1));
        hdm.addData("SELECT 2 FROM b AS g_0 WHERE g_0.c3 = 1", Arrays.asList(2));
        CommandContext cc = TestProcessor.createCommandContext();
        cc.setMetadata(tm);
        //TestProcessor.helpProcess(plan, cc, hdm, new List[] {Arrays.asList(2)} );

        //with conflicting aliases it should still work
        sql = "SELECT (select c2 from b where c3 = (select c5 from c as x where c6 = c1)) FROM a as x group by c1, c2";

    /*    plan = TestOptimizer.helpPlan(sql, //$NON-NLS-1$
                                      tm, null, new DefaultCapabilitiesFinder(bsc),
                                      new String[] {
                                          "SELECT g_0.c1, g_0.c2 FROM y.a AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        TestProcessor.helpProcess(plan, cc, hdm, new List[] {Arrays.asList(2)} );
        */
        //with conflicting aliases it should still work
        sql = "SELECT (select c2 from b as x where c3 = (select c5 from c as x where c6 = c1)) FROM a as x group by c1, c2";

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, //$NON-NLS-1$
                                      tm, null, new DefaultCapabilitiesFinder(bsc),
                                      new String[] {
                                          "SELECT g_0.c1, g_0.c2 FROM y.a AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        TestProcessor.helpProcess(plan, cc, hdm, new List[] {Arrays.asList(2)} );
    }

    @Test public void testNestedCorrelationInAggregate() throws Exception {
        TransformationMetadata tm = RealMetadataFactory.fromDDL("CREATE foreign TABLE a (c1 integer, c2 integer); "
                + "CREATE foreign TABLE b (c3 integer, c4 integer); CREATE foreign TABLE c (c5 integer, c6 integer);", "x", "y");

        String sql = "SELECT max((select c2 from (select * from b as x) as b where c3 = (select c5 from c as x where c6 = c1))) FROM a as x group by c1, c2";

        BasicSourceCapabilities bsc = getTypicalCapabilities();
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, //$NON-NLS-1$
                                      tm, null, new DefaultCapabilitiesFinder(bsc),
                                      new String[] {
                                          "SELECT g_0.c1, g_0.c2 FROM y.a AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        HardcodedDataManager hdm = new HardcodedDataManager(tm);
        hdm.addData("SELECT g_0.c1, g_0.c2 FROM a AS g_0", Arrays.asList(1, 2));
        hdm.addData("SELECT g_0.c5 FROM c AS g_0 WHERE g_0.c6 = 1", Arrays.asList(1));
        hdm.addData("SELECT 2 FROM b AS g_0 WHERE g_0.c3 = 1", Arrays.asList(2));
        CommandContext cc = TestProcessor.createCommandContext();
        cc.setMetadata(tm);
        TestProcessor.helpProcess(plan, cc, hdm, new List[] {Arrays.asList(2)} );
    }

    @Test public void testSubqueryProducingBuffer() throws Exception {
        TransformationMetadata tm = RealMetadataFactory.example1Cached();

        String sql = "SELECT e1, (select e2 from pm2.g1 where e1 = pm1.g1.e1 order by e2 limit 1) from pm1.g1 limit 1";

        BasicSourceCapabilities bsc = getTypicalCapabilities();
        bsc.setCapabilitySupport(Capability.QUERY_ORDERBY, false);

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, //$NON-NLS-1$
                                      tm, null, new DefaultCapabilitiesFinder(bsc),
                                      new String[] {
                                          "SELECT g_0.e1 FROM pm1.g1 AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        HardcodedDataManager hdm = new HardcodedDataManager(tm) {

            @Override
            public TupleSource registerRequest(CommandContext context,
                    Command command, String modelName,
                    RegisterRequestParameter parameterObject)
                    throws TeiidComponentException {
                if (command.toString().equals("SELECT g_0.e2 FROM pm2.g1 AS g_0 WHERE g_0.e1 = 'a'")) {
                    return new TupleSource() {

                        @Override
                        public List<?> nextTuple() throws TeiidComponentException,
                                TeiidProcessingException {
                            throw new TeiidProcessingException("something's wrong");
                        }

                        @Override
                        public void closeSource() {

                        }
                    };
                }
                return super.registerRequest(context, command, modelName, parameterObject);
            }

        };
        hdm.addData("SELECT g_0.e1 FROM g1 AS g_0", Arrays.asList("a"));
        hdm.setBlockOnce(true);

        CommandContext cc = TestProcessor.createCommandContext();
        cc.setMetadata(tm);
        try {
            TestProcessor.helpProcess(plan, cc, hdm, new List[] {Arrays.asList(2)} );
            fail();
        } catch (TeiidProcessingException e) {
            assert(e.getMessage().contains("something's wrong"));
        }
    }

    @Test public void testAggNestedSubquery() throws Exception {
        String sql = "SELECT g0.a, g0.b, (SELECT max((SELECT g2.a FROM m.z AS g2 WHERE g2.b = g1.a)) FROM m.y AS g1 WHERE g0.a = g1.b) FROM m.x AS g0"; //$NON-NLS-1$

        TransformationMetadata metadata = RealMetadataFactory.fromDDL("create foreign table x ("
                + " a string, "
                + " b string, "
                + " primary key (a)"
                + ") options (updatable true);"
                + "create foreign table y ("
                + " a string, "
                + " b string, "
                + " primary key (a)"
                + ") options (updatable true);"
                + "create foreign table z ("
                + " a string, "
                + " b string, "
                + " primary key (a)"
                + ") options (updatable true);", "x", "m");

        ProcessorPlan pp = TestProcessor.helpGetPlan(sql, metadata, TestOptimizer.getGenericFinder());
        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT g_0.a, g_0.b FROM m.x AS g_0", Arrays.asList("a", "b"), Arrays.asList("a1", "b1"));
        dataManager.addData("SELECT g_0.a FROM m.y AS g_0 WHERE g_0.b = 'a'", Arrays.asList("a"));
        dataManager.addData("SELECT g_0.a FROM m.y AS g_0 WHERE g_0.b = 'a1'", Arrays.asList("b"));
        dataManager.addData("SELECT g_0.a FROM m.z AS g_0 WHERE g_0.b = 'b'", Arrays.asList("b2"));
        dataManager.addData("SELECT g_0.a FROM m.z AS g_0 WHERE g_0.b = 'a'", Arrays.asList("a2"));
        TestProcessor.helpProcess(pp, dataManager, new List[] {Arrays.asList("a", "b", "a2"), Arrays.asList("a1", "b1", "b2")});
    }

    @Test public void testAggSubqueryAsJoin() throws Exception {
        String sql = "SELECT INTKEY, LONGNUM FROM BQT1.SMALLA AS A WHERE LONGNUM > (SELECT SUM(LONGNUM) FROM BQT1.SMALLA AS B WHERE A.INTKEY = B.INTKEY) ORDER BY INTKEY"; //$NON-NLS-1$

        TransformationMetadata metadata = RealMetadataFactory.exampleBQT();
        RealMetadataFactory.setCardinality("BQT1.smalla", 1000, metadata);

        HardcodedDataManager dataMgr = new HardcodedDataManager();

        dataMgr.addData("SELECT g_0.LongNum AS c_0, g_0.IntKey AS c_1 FROM BQT1.SmallA AS g_0 ORDER BY c_1", Arrays.asList(1L, 1));
        dataMgr.addData("SELECT SUM(g_0.LongNum) AS c_0, g_0.IntKey AS c_1 FROM BQT1.SmallA AS g_0 GROUP BY g_0.IntKey ORDER BY c_1", Arrays.asList(1L, 1));

        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        bsc.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        bsc.setCapabilitySupport(Capability.QUERY_GROUP_BY, true);
        bsc.setCapabilitySupport(Capability.QUERY_AGGREGATES_SUM, true);

        ProcessorPlan pp = TestProcessor.helpGetPlan(sql, metadata, new DefaultCapabilitiesFinder(bsc));

        TestProcessor.helpProcess(pp, dataMgr, new List[] {});
    }

    @Test public void testProjectSubqueryRewriteToJoin() throws Exception {
        TestQueryRewriter.helpTestRewriteCommand("Select e1, /*+ mj */ (select max(pm1.g1.e1) as x FROM pm1.g1 where e2 = pm2.g1.e2) as e2 from pm2.g1",
                "SELECT e1, X__1.x AS e2 FROM pm2.g1 LEFT OUTER JOIN (SELECT MAX(pm1.g1.e1) AS x, e2 FROM pm1.g1 GROUP BY e2) AS X__1 ON pm2.g1.e2 = X__1.e2", RealMetadataFactory.example1Cached());
    }

    @Test public void testProjectSubqueryRewriteToJoin1() throws Exception {
        String sql = "SELECT A.INTKEY, C.LONGNUM, "
                + "/*+ mj */ (SELECT SUM(LONGNUM) FROM BQT1.SMALLB AS B WHERE A.INTKEY = B.INTKEY), "
                + "/*+ mj */ (SELECT MIN(LONGNUM) FROM BQT1.SMALLB AS B WHERE A.INTKEY = B.INTKEY) "
                + " FROM BQT1.SMALLA AS A, BQT2.SMALLA AS C WHERE A.INTNUM = C.INTNUM ORDER BY INTKEY"; //$NON-NLS-1$

        TestQueryRewriter.helpTestRewriteCommand(sql,
                "SELECT A.INTKEY, C.LONGNUM, X__1.expr1 AS expr3, X__2.expr1 AS expr4 FROM ((BQT1.SMALLA AS A CROSS JOIN BQT2.SMALLA AS C) LEFT OUTER JOIN (SELECT SUM(LONGNUM) AS expr1, B.INTKEY FROM BQT1.SMALLB AS B GROUP BY B.INTKEY) AS X__1 ON A.INTKEY = X__1.IntKey) LEFT OUTER JOIN (SELECT MIN(LONGNUM) AS expr1, B.INTKEY FROM BQT1.SMALLB AS B GROUP BY B.INTKEY) AS X__2 ON A.INTKEY = X__2.IntKey WHERE A.INTNUM = C.INTNUM ORDER BY A.INTKEY", RealMetadataFactory.exampleBQTCached());
    }

    /*
     * Not yet implemented per TEIID-5569
     */
    @Test public void testProjectSubqueryRewriteToJoinCountStar() throws Exception {
        TestQueryRewriter.helpTestRewriteCommand("SELECT pm1.g1.e1, pm1.g1.e2, /*+ MJ */ (select count(*) from pm1.g2 where e1 = pm1.g1.e1) FROM pm1.g1",
                "SELECT pm1.g1.e1, pm1.g1.e2, /*+ MJ */ (SELECT COUNT(*) FROM pm1.g2 WHERE e1 = pm1.g1.e1) FROM pm1.g1", RealMetadataFactory.example1Cached());
    }

    /*
     * Not yet implemented per TEIID-5569
     */
    @Test public void testWhereSubqueryRewriteToJoinCountStar() throws Exception {
        TestQueryRewriter.helpTestRewriteCommand("SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1 where e2 = /*+ MJ */ (select count(*) from pm1.g2 where e1 = pm1.g1.e1)",
                "SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1 WHERE e2 = /*+ MJ */ (SELECT COUNT(*) FROM pm1.g2 WHERE e1 = pm1.g1.e1)", RealMetadataFactory.example1Cached());
    }

    /*
     * Not yet implemented per TEIID-5569 - should be optimized to just a true predicate
     */
    @Test public void testWhereSubqueryRewriteToJoinCountStar1() throws Exception {
        TestQueryRewriter.helpTestRewriteCommand("SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1 where exists /*+ MJ */ (select count(*) from pm1.g2 where e1 = pm1.g1.e1)",
                "SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1 WHERE EXISTS /*+ MJ */ (SELECT COUNT(*) FROM pm1.g2 WHERE e1 = pm1.g1.e1)", RealMetadataFactory.example1Cached());
    }

    @Test public void testNManySubqueryProcessingFalsePredicate() throws Exception {
        String sql = "SELECT INTKEY, FLOATNUM FROM BQT1.SMALLA AS A WHERE FLOATNUM = /*+ NO_UNNEST */ (SELECT MIN(FLOATNUM) FROM BQT1.SMALLA AS B WHERE (INTKEY >= 9) AND (A.INTKEY = B.INTKEY))"; //$NON-NLS-1$

        TransformationMetadata metadata = RealMetadataFactory.exampleBQT();

        HardcodedDataManager dataMgr = new HardcodedDataManager(metadata);

        dataMgr.addData("SELECT g_0.FloatNum, g_0.IntKey FROM SmallA AS g_0", Arrays.asList(.1f, 1));
        dataMgr.addData("SELECT MIN(g_0.FloatNum) FROM SmallA AS g_0 WHERE g_0.IntKey >= 9 AND g_0.IntKey = 1");

        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        bsc.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        bsc.setCapabilitySupport(Capability.QUERY_AGGREGATES_MIN, true);
        bsc.setCapabilitySupport(Capability.CRITERIA_ONLY_LITERAL_COMPARE, true);

        ProcessorPlan pp = TestProcessor.helpGetPlan(sql, metadata, new DefaultCapabilitiesFinder(bsc));

        TestProcessor.helpProcess(pp, dataMgr, new List[] {});

        sql = "SELECT INTKEY, STRINGKEY, DOUBLENUM FROM BQT1.SMALLA GROUP BY INTKEY, STRINGKEY, DOUBLENUM HAVING DOUBLENUM = /*+ NO_UNNEST */ (SELECT DOUBLENUM FROM BQT1.SMALLA WHERE STRINGKEY = 20)";
        pp = TestProcessor.helpGetPlan(sql, metadata, new DefaultCapabilitiesFinder(bsc));
        dataMgr.clearData();
        dataMgr.addData("SELECT g_0.DoubleNum FROM SmallA AS g_0 WHERE g_0.StringKey = '20'");

        TestProcessor.helpProcess(pp, dataMgr, new List[] {});
    }

    @Test public void testSubqueryPredicatePushdown() throws Exception {
        String sql = "SELECT array_agg((select intkey from bqt1.smallb where stringkey = lower(a.stringkey))) from bqt1.smalla a"; //$NON-NLS-1$

        TransformationMetadata metadata = RealMetadataFactory.exampleBQTCached();

        HardcodedDataManager dataMgr = new HardcodedDataManager(metadata);

        dataMgr.addData("SELECT g_0.StringKey FROM SmallA AS g_0", Arrays.asList("a"));
        dataMgr.addData("SELECT g_0.IntKey FROM SmallB AS g_0 WHERE g_0.StringKey = 'a'", Arrays.asList(1));

        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();

        CommandContext cc = TestProcessor.createCommandContext();

        ProcessorPlan pp = TestProcessor.helpGetPlan(TestOptimizer.helpGetCommand(sql, metadata), metadata, new DefaultCapabilitiesFinder(bsc), cc);

        TestProcessor.helpProcess(pp, cc, dataMgr, new List[] {Arrays.asList(new ArrayImpl(1))});
    }

    /**
     * Uses n-many processing despite the hint because of TEIID-5569
     * @throws Exception
     */
    @Test public void testSelectSubqueryJoin() throws Exception {
        String sql = "SELECT pm1.g1.e1, pm1.g1.e2, /*+ MJ */ (select count(*) from pm1.g2 where e1 = pm1.g1.e1) FROM pm1.g1"; //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1();
        RealMetadataFactory.setCardinality("pm1.g2", 1000, metadata);
        RealMetadataFactory.setCardinality("pm1.g1", 10, metadata);
        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        CommandContext cc = TestProcessor.createCommandContext();
        ProcessorPlan pp = TestProcessor.helpGetPlan(TestOptimizer.helpGetCommand(sql, metadata), metadata, new DefaultCapabilitiesFinder(bsc), cc);

        List[] expected = new List[] { Arrays.asList("a", 1, 3), Arrays.asList("b", 2, 1), Arrays.asList("c", 3, 0) };

        HardcodedDataManager manager = new HardcodedDataManager(metadata);
        manager.addData("SELECT g_0.e1, g_0.e2 FROM g1 AS g_0", new List[] {Arrays.asList("a", 1), Arrays.asList("b", 2), Arrays.asList("c", 3)});
        manager.addData("SELECT 1 FROM g2 AS g_0 WHERE g_0.e1 = 'a'", new List[] {Arrays.asList(1), Arrays.asList(1), Arrays.asList(1)});
        manager.addData("SELECT 1 FROM g2 AS g_0 WHERE g_0.e1 = 'b'", new List[] {Arrays.asList(1)});
        manager.addData("SELECT 1 FROM g2 AS g_0 WHERE g_0.e1 = 'c'", new List[] {});

        helpProcess(pp, manager, expected);
    }

    @Test public void testSelectSubqueryJoin2() throws Exception {
        String sql = "SELECT pm1.g1.e1, pm1.g1.e2, (select max(e2) from pm1.g2 where e1 = pm1.g1.e1) FROM pm1.g1"; //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1();
        RealMetadataFactory.setCardinality("pm1.g2", 1000, metadata);
        RealMetadataFactory.setCardinality("pm1.g1", 10, metadata);
        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        CommandContext cc = TestProcessor.createCommandContext();
        ProcessorPlan pp = TestProcessor.helpGetPlan(TestOptimizer.helpGetCommand(sql, metadata), metadata, new DefaultCapabilitiesFinder(bsc), cc);

        List[] expected = new List[] { Arrays.asList("a", 1, 2), Arrays.asList("b", 2, 1), Arrays.asList("c", 3, null) };

        HardcodedDataManager manager = new HardcodedDataManager(metadata);
        manager.addData("SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM g1 AS g_0 ORDER BY c_0", new List[] {Arrays.asList("a", 1), Arrays.asList("b", 2), Arrays.asList("c", 3)});
        manager.addData("SELECT g_0.e1, g_0.e2 FROM g2 AS g_0 WHERE g_0.e1 IN ('a', 'b', 'c')", new List[] {Arrays.asList("a", 1), Arrays.asList("a", 2), Arrays.asList("b", 1)});

        helpProcess(pp, manager, expected);
    }

    @Test public void testTwoSelectSubqueryJoin() throws Exception {
        String sql = "SELECT pm1.g1.e1, pm1.g1.e2, (select max(e1) from pm1.g2 where e2 = pm1.g1.e2), (select min(e2) from pm1.g2 where e1 = pm1.g1.e1) FROM pm1.g1"; //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1();
        RealMetadataFactory.setCardinality("pm1.g2", 1000, metadata);
        RealMetadataFactory.setCardinality("pm1.g1", 10, metadata);
        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        CommandContext cc = TestProcessor.createCommandContext();
        ProcessorPlan pp = TestProcessor.helpGetPlan(TestOptimizer.helpGetCommand(sql, metadata), metadata, new DefaultCapabilitiesFinder(bsc), cc);

        List[] expected = new List[] { Arrays.asList("a", 1, "b", 1), Arrays.asList("b", 2, null, 1), Arrays.asList("c", 3, null, 4) };

        HardcodedDataManager manager = new HardcodedDataManager(metadata);
        manager.addData("SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM g1 AS g_0 ORDER BY c_1", new List[] {Arrays.asList("a", 1), Arrays.asList("b", 2), Arrays.asList("c", 3)});
        manager.addData("SELECT g_0.e2, g_0.e1 FROM g2 AS g_0 WHERE g_0.e2 IN (1, 2, 3)", new List[] {Arrays.asList(1, "a"), Arrays.asList(1, "b"), Arrays.asList(1, "a"), Arrays.asList(1, "a")});
        manager.addData("SELECT g_0.e1, g_0.e2 FROM g2 AS g_0 WHERE g_0.e1 IN ('a', 'b', 'c')", new List[] {Arrays.asList("a", 1), Arrays.asList("b", 1), Arrays.asList("a", 1), Arrays.asList("a", 1), Arrays.asList("c", 4)});

        helpProcess(pp, manager, expected);
    }

    @Test public void testSelectSubqueryJoinNotPushed() throws Exception {
        String sql = "SELECT pm1.g1.e1, pm1.g1.e2, (select max(e2) from pm1.g2 where e1 = pm1.g1.e1) FROM pm1.g1"; //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1();
        RealMetadataFactory.setCardinality("pm1.g2", 1000, metadata);
        RealMetadataFactory.setCardinality("pm1.g1", 10, metadata);
        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        bsc.setCapabilitySupport(Capability.CRITERIA_IN, false);
        CommandContext cc = TestProcessor.createCommandContext();
        ProcessorPlan pp = TestProcessor.helpGetPlan(TestOptimizer.helpGetCommand(sql, metadata), metadata, new DefaultCapabilitiesFinder(bsc), cc);

        List[] expected = new List[] { Arrays.asList("a", 1, 2), Arrays.asList("b", 2, 1), Arrays.asList("c", 3, null) };

        HardcodedDataManager manager = new HardcodedDataManager(metadata);
        manager.addData("SELECT g_0.e1, g_0.e2 FROM g1 AS g_0", new List[] {Arrays.asList("a", 1), Arrays.asList("b", 2), Arrays.asList("c", 3)});
        manager.addData("SELECT g_0.e2 FROM g2 AS g_0 WHERE g_0.e1 = 'a'", new List[] {Arrays.asList(1), Arrays.asList(2)});
        manager.addData("SELECT g_0.e2 FROM g2 AS g_0 WHERE g_0.e1 = 'b'", new List[] {Arrays.asList(1)});
        manager.addData("SELECT g_0.e2 FROM g2 AS g_0 WHERE g_0.e1 = 'c'", new List[] {});

        helpProcess(pp, manager, expected);
    }

    /**
     * We're allowed to use a join instead as there will only be 1 row from the subquery for every outer row
     */
    @Test public void testSelectSubqueryKeyJoin() throws Exception {
        String sql = "SELECT g2.e1, g2.e2, /*+ MJ */ (select e2 from g1 where e1 = g2.e1) FROM g2"; //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.fromDDL("create foreign table g1 (e1 integer primary key, e2 integer);"
                + " create foreign table g2 (e1 integer, e2 integer, foreign key (e1) references g1);", "x", "y");

        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        ProcessorPlan pp = TestOptimizer.helpPlan(sql, metadata, new String[] {"SELECT g_0.e1, g_0.e2, g_1.e2 FROM y.g2 AS g_0 LEFT OUTER JOIN y.g1 AS g_1 ON g_0.e1 = g_1.e1"}, new DefaultCapabilitiesFinder(bsc), ComparisonMode.EXACT_COMMAND_STRING);
        TestOptimizer.checkNodeTypes(pp, TestOptimizer.FULL_PUSHDOWN);
    }

    /**
     * Not allowed to optimize via rewrite as the key is on e1, but the predicate is on e2
     */
    @Test public void testSelectSubqueryKeyJoin1() throws Exception {
        String sql = "SELECT g2.e1, g2.e2, /*+ MJ */ (select e1 from g1 where e2 = g2.e2) FROM g2"; //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.fromDDL("create foreign table g1 (e1 integer primary key, e2 integer);"
                + " create foreign table g2 (e1 integer, e2 integer, foreign key (e1) references g1);", "x", "y");

        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        TestOptimizer.helpPlan(sql, metadata, new String[] {"SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM y.g2 AS g_0 ORDER BY c_1"}, new DefaultCapabilitiesFinder(bsc), ComparisonMode.EXACT_COMMAND_STRING);
    }

    /**
     * TODO this should detect the usage of the primary key, but does not due to the simplifying assumption of looking only for a single
     * correlation - and in the case we effectively have two
     */
    @Test public void testWhereSubqueryKeyJoin() throws Exception {
        String sql = "SELECT e1, e2 FROM g2 where e2 = /*+ MJ */ (select e2 from g1 where e1 = g2.e1)"; //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.fromDDL("create foreign table g1 (e1 integer primary key, e2 integer);"
                + " create foreign table g2 (e1 integer, e2 integer, foreign key (e1) references g1);", "x", "y");

        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        ProcessorPlan pp = TestOptimizer.helpPlan(sql, metadata, new String[] {"SELECT g_0.e2 AS c_0, g_0.e1 AS c_1 FROM y.g2 AS g_0 WHERE (g_0.e1 IN (<dependent values>)) AND (g_0.e2 IN (<dependent values>)) ORDER BY c_1, c_0"}, new DefaultCapabilitiesFinder(bsc), ComparisonMode.EXACT_COMMAND_STRING);
        checkJoinCounts(pp, 1, 0);
    }

    /**
     * We're allowed to use a join instead as there will only be 1 row from the subquery for every outer row
     */
    @Test public void testWhereSubqueryKeyJoin1() throws Exception {
        String sql = "SELECT e1, e2 FROM g2 where 0 = /*+ MJ */ (select e2 from g1 where e1 = g2.e1)"; //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.fromDDL("create foreign table g1 (e1 integer primary key, e2 integer);"
                + " create foreign table g2 (e1 integer, e2 integer);", "x", "y");

        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        ProcessorPlan pp = TestOptimizer.helpPlan(sql, metadata, new String[] {"SELECT g_0.e1, g_0.e2 FROM y.g2 AS g_0, y.g1 AS g_1 WHERE (g_0.e1 = g_1.e1) AND (g_1.e2 = 0)"}, new DefaultCapabilitiesFinder(bsc), ComparisonMode.EXACT_COMMAND_STRING);
        TestOptimizer.checkNodeTypes(pp, TestOptimizer.FULL_PUSHDOWN);
    }

    @Test public void testWhereSubqueryKeyJoin1a() throws Exception {
        String sql = "SELECT e1, e2 FROM g2 where 0 = /*+ MJ */ (select e2 from g1 where e1 = g2.e1)"; //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.fromDDL("create foreign table g2 (e1 integer primary key, e2 integer);"
                + " create foreign table g1 (e1 integer, e2 integer, foreign key (e1) references g2) options (cardinality 100);", "x", "y");

        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        TestOptimizer.helpPlan(sql, metadata, new String[] {"SELECT g_0.e1, g_0.e2 FROM y.g2 AS g_0 WHERE g_0.e1 IN (<dependent values>)", "SELECT DISTINCT g_0.e1 FROM y.g1 AS g_0 WHERE g_0.e2 = 0"}, new DefaultCapabilitiesFinder(bsc), ComparisonMode.EXACT_COMMAND_STRING);
    }

    @Test public void testWhereSubqueryJoin2() throws Exception {
        String sql = "SELECT e1, e2 FROM g2 where 0 < /*+ MJ */ (select e2 from g1 where e1 = g2.e1)"; //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.fromDDL("create foreign table g1 (e1 integer, e2 integer);"
                + " create foreign table g2 (e1 integer, e2 integer);", "x", "y");

        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();

        TestOptimizer.helpPlan(sql, metadata, new String[] {"SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM y.g2 AS g_0 ORDER BY c_0", "SELECT DISTINCT g_0.e1 FROM y.g1 AS g_0 WHERE g_0.e2 > 0"}, new DefaultCapabilitiesFinder(bsc), ComparisonMode.EXACT_COMMAND_STRING);
    }

    /**
     * Test the enforcement of the semi join single row
     * @throws Exception
     */
    @Test public void testSelectSubqueryKeyJoin2() throws Exception {
        String sql = "SELECT e1, e2, /*+ MJ */ (select e2 from g1 where e1 = g2.e1) FROM g2"; //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.fromDDL("create foreign table g1 (e1 integer, e2 integer);"
                + " create foreign table g2 (e1 integer, e2 integer);", "x", "y");

        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        CommandContext cc = TestProcessor.createCommandContext();
        ProcessorPlan pp = TestProcessor.helpGetPlan(TestOptimizer.helpGetCommand(sql, metadata), metadata, new DefaultCapabilitiesFinder(bsc), cc);

        List[] expected = new List[] { Arrays.asList("a", 1, 1), Arrays.asList("b", 2, 2), Arrays.asList("c", 3, null) };

        //first without duplicates, succeeds
        try {
            HardcodedDataManager manager = new HardcodedDataManager(metadata);
            manager.addData("SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM g2 AS g_0 ORDER BY c_0", new List[] {Arrays.asList("a", 1), Arrays.asList("b", 2), Arrays.asList("c", 3)});
            manager.addData("SELECT g_0.e2, g_0.e1 FROM g1 AS g_0 WHERE g_0.e1 IN (a, b, c)", new List[] {Arrays.asList(1, "a"), Arrays.asList(2, "b")});

            helpProcess(pp, cc, manager, expected);
        } catch (TeiidProcessingException e) {
            fail(e.getMessage());
        }

        pp.close();
        pp.reset();

        HardcodedDataManager manager = new HardcodedDataManager(metadata);
        manager.addData("SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM g2 AS g_0 ORDER BY c_0", new List[] {Arrays.asList("a", 1), Arrays.asList("b", 2), Arrays.asList("c", 3)});
        manager.addData("SELECT g_0.e2, g_0.e1 FROM g1 AS g_0 WHERE g_0.e1 IN (a, b, c)", new List[] {Arrays.asList(1, "a"), Arrays.asList(2, "a")});

        try {
            helpProcess(pp, cc, manager, expected);
            fail();
        } catch (ExpressionEvaluationException e) {
            assertTrue(e.getMessage().contains("TEIID31293"));
        }
    }

    @Test public void testProcedureInSelectWithNoFrom() throws Exception {
        String sql = "SELECT (EXEC pm1.sqsp1())"; //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        CommandContext cc = TestProcessor.createCommandContext();
        ProcessorPlan pp = TestProcessor.helpGetPlan(TestOptimizer.helpGetCommand(sql, metadata), metadata, new DefaultCapabilitiesFinder(bsc), cc);

        HardcodedDataManager manager = new HardcodedDataManager(metadata);
        manager.addData("EXEC sp1()", Arrays.asList("a"));

        List<?>[] expected = new List[] { Arrays.asList("a") };

        helpProcess(pp, cc, manager, expected);
    }

}

