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

import org.junit.Test;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.TestOptimizer.ComparisonMode;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.validator.TestValidator;



public class TestAccessPatterns {

    /**
     * The virtual access patterns should get satisfied
     */
    @Test public void testVirtualAccessPatternPassing() {
        String sql = "SELECT e0, e2 FROM vTest.vGroup2 where e0=1 and e1='2'"; //$NON-NLS-1$
        TestOptimizer.helpPlan(sql, TestValidator.exampleMetadata4(), new String[] {"SELECT test.\"group\".e0, test.\"group\".e2 FROM test.\"group\" WHERE (test.\"group\".e0 = 1) AND (test.\"group\".e1 = '2')"}); //$NON-NLS-1$
    }

    @Test public void testVirtualAccessPatternPassing1() {
        String sql = "delete from vm1.g37 where e1 = 1"; //$NON-NLS-1$
        TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), new String[] {});
    }

    @Test public void testVirtualAccessPatternFailing() {
        String sql = "SELECT e0, e2 FROM vTest.vGroup2 where e0=1"; //$NON-NLS-1$
        TestOptimizer.helpPlan(sql, TestValidator.exampleMetadata4(), null, null, null, TestOptimizer.SHOULD_FAIL);
    }

    @Test public void testVirtualAccessPatternFailing1() {
        String sql = "delete from vm1.g37"; //$NON-NLS-1$
        TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), null, null, null, TestOptimizer.SHOULD_FAIL);
    }

    @Test public void testAccessPattern1() throws Exception {
        String sql = "SELECT e0, e2 FROM vTest.vGroup where e0=1 and e1='2'"; //$NON-NLS-1$
        TestOptimizer.helpPlan(sql,
                               TestValidator.exampleMetadata4(),
                               new String[] {"SELECT g_0.e0, g_0.e2 FROM test.\"group\" AS g_0 WHERE (g_0.e0 = 1) AND (g_0.e1 = '2')" }, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING ); //$NON-NLS-1$
    }

    @Test public void testAccessPattern2() {
        String sql = "SELECT e0, e2 FROM vTest.vGroup where e0=1"; //$NON-NLS-1$
        TestOptimizer.helpPlan(sql, TestValidator.exampleMetadata4(), null, null, null, TestOptimizer.SHOULD_FAIL);
    }

    @Test public void testAccessPattern3() {
        String sql = "SELECT e0, e2 FROM vTest.vGroup where e0=1 and e2='2'"; //$NON-NLS-1$
        TestOptimizer.helpPlan(sql, TestValidator.exampleMetadata4(), null, null, null, TestOptimizer.SHOULD_FAIL);
    }

    @Test public void testAccessPattern4() throws Exception {
        String sql = "(SELECT e0, e2 FROM vTest.vGroup where e0=1 and e1='2') union all (SELECT e0, e2 FROM vTest.vGroup where e0=1 and e1='2')"; //$NON-NLS-1$
        TestOptimizer.helpPlan(sql, TestValidator.exampleMetadata4(), new String[] {"SELECT g_0.e0, g_0.e2 FROM test.\"group\" AS g_0 WHERE (g_0.e0 = 1) AND (g_0.e1 = '2')"}, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    }

    @Test public void testAccessPattern5() {
        String sql = "(SELECT e0, e2 FROM vTest.vGroup where e0=1 and e1='2') union all (SELECT e0, e2 FROM vTest.vGroup where e0=1)"; //$NON-NLS-1$
        TestOptimizer.helpPlan(sql, TestValidator.exampleMetadata4(), null, null, null, TestOptimizer.SHOULD_FAIL);
    }

    @Test public void testAccessPattern6() {
        String sql = "SELECT e0, e2 FROM test.group where e1 IN /*+ no_unnest */ (SELECT e2 FROM vTest.vGroup where e0=1 and e1='2')"; //$NON-NLS-1$
        TestOptimizer.helpPlan(sql, TestValidator.exampleMetadata4(), new String[] {"SELECT e1, e0, e2 FROM test.\"group\""}); //$NON-NLS-1$
    }

    @Test public void testAccessPattern7() {
        String sql = "SELECT e0, e2 FROM test.group where e1 IN (SELECT e2 FROM vTest.vGroup where e0=1)"; //$NON-NLS-1$
        TestOptimizer.helpPlan(sql, TestValidator.exampleMetadata4(), null, null, null, TestOptimizer.SHOULD_FAIL);
    }

    @Test public void testAccessPattern8() {
        String sql = "SELECT e0, e2 FROM vTest.vGroup"; //$NON-NLS-1$
        TestOptimizer.helpPlan(sql, TestValidator.exampleMetadata4(), null, null, null, TestOptimizer.SHOULD_FAIL);
    }

    /**
     * Tests two access nodes, each with access patterns, but one already
     * satisfied by user criteria - the other should be made dependent
     */
    @Test public void testNodesBothHaveAccessPatterns1() throws Exception {
        ProcessorPlan plan = TestOptimizer.helpPlan("select pm4.g1.e1 from pm4.g1, pm4.g2 where pm4.g2.e5 = 'abc' and pm4.g1.e1 = pm4.g2.e1", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "SELECT g_0.e1 FROM pm4.g1 AS g_0 WHERE g_0.e1 IN (<dependent values>)", "SELECT g_0.e1 FROM pm4.g2 AS g_0 WHERE g_0.e5 = 'abc'"}, TestOptimizer.getGenericFinder(false), TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$
        TestDependentJoins.checkDependentGroups(plan, new String[] {"pm4.g1"}); //$NON-NLS-1$
    }

    /**
     * Tests two access nodes, each with access patterns, but one already
     * satisfied by user criteria - the other should be made dependent
     * (same query written slightly different).
     */
    @Test public void testNodesBothHaveAccessPatterns1a() throws Exception {
        ProcessorPlan plan = TestOptimizer.helpPlan("select pm4.g1.e1 from pm4.g2, pm4.g1 where pm4.g2.e1 = pm4.g1.e1 and pm4.g2.e5 = 'abc'", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "SELECT g_0.e1 FROM pm4.g1 AS g_0 WHERE g_0.e1 IN (<dependent values>)", "SELECT g_0.e1 FROM pm4.g2 AS g_0 WHERE g_0.e5 = 'abc'"}, TestOptimizer.getGenericFinder(false), TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$
        TestDependentJoins.checkDependentGroups(plan, new String[] {"pm4.g1"}); //$NON-NLS-1$
    }

    /**
     * Self join - tests that both access nodes are satisfied by the select
     * criteria (therefore merge join should be used)
     */
    @Test public void testSelfJoinAccessPatterns() throws Exception {
        ProcessorPlan plan = TestOptimizer.helpPlan("select pm4.g1.e1 from pm4.g1, pm4.g1 as g1A where pm4.g1.e1 = 'abc' and g1A.e1 = 'abc' and pm4.g1.e2 = g1A.e2", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "SELECT g1A.e2 FROM pm4.g1 AS g1A WHERE g1A.e1 = 'abc'", "SELECT pm4.g1.e2, pm4.g1.e1 FROM pm4.g1 WHERE pm4.g1.e1 = 'abc'" }, TestOptimizer.getGenericFinder(false), ComparisonMode.CORRECTED_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$

        TestOptimizer.checkNodeTypes(plan, new int[] {
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

    @Test public void testAccessPatternsFails() {
        TestOptimizer.helpPlan("select pm4.g2.e1 from pm4.g2, pm4.g2 as g2A where pm4.g2.e2 = 123 and pm4.g2.e1 = g2A.e5", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            null, null, null,
            TestOptimizer.SHOULD_FAIL);
    }

    @Test public void testAccessPatternsFails2() {
        TestOptimizer.helpPlan("select pm4.g2.e1 from pm4.g2", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            null, null, null,
            TestOptimizer.SHOULD_FAIL);
    }

    @Test public void testUnionWithAccessPatternFails() {
        TestOptimizer.helpPlan("select pm1.g1.e1 from pm1.g1 UNION select pm4.g1.e1 from pm4.g1 where pm4.g1.e2 = 1", //$NON-NLS-1$
            RealMetadataFactory.example1Cached(),
            null, null, null, TestOptimizer.SHOULD_FAIL);
    }

    @Test public void testUnionWithAccessPatternFails2() {
        TestOptimizer.helpPlan("select pm1.g1.e1 from pm1.g1 UNION select pm4.g1.e1 from pm4.g1", //$NON-NLS-1$
            RealMetadataFactory.example1Cached(),
            null, null, null, TestOptimizer.SHOULD_FAIL);
    }

    @Test public void testUnionWithAccessPattern() {
        TestOptimizer.helpPlan("select pm1.g1.e1 from pm1.g1 UNION ALL select pm4.g1.e1 from pm4.g1 where pm4.g1.e1 = 'abc'", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "SELECT pm1.g1.e1 FROM pm1.g1", "SELECT pm4.g1.e1 FROM pm4.g1 WHERE pm4.g1.e1 = 'abc'" }); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testUnionWithAccessPattern2() {
        TestOptimizer.helpPlan("select pm1.g1.e1 from pm1.g1 UNION ALL select pm4.g1.e1 from pm4.g1 where pm4.g1.e1 = 'abc' and pm4.g1.e2 = 1", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "SELECT pm1.g1.e1 FROM pm1.g1", "SELECT pm4.g1.e1 FROM pm4.g1 WHERE (pm4.g1.e1 = 'abc') AND (pm4.g1.e2 = 1)" }); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testAccessPatternPartialMatch() throws Exception {
        TestOptimizer.helpPlan("select pm1.g1.e1 from pm1.g1, pm4.g2 where pm1.g1.e1 = pm4.g2.e1 and pm4.g2.e2 = 123", //$NON-NLS-1$
            RealMetadataFactory.example1Cached(),
            new String[] { "SELECT g_0.e1 FROM pm4.g2 AS g_0 WHERE (g_0.e2 = 123) AND (g_0.e1 IN (<dependent values>))", "SELECT g_0.e1 FROM pm1.g1 AS g_0" }, TestOptimizer.getGenericFinder(false), TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Similar to the query above, except the OR instead of an AND produces a
     * completely different query plan which can't satisfy the access pattern.
     * @see #testAccessPatternPartialMatch
     */
    @Test public void testAccessPatternFails3() {
        TestOptimizer.helpPlan("select pm1.g1.e1 from pm1.g1, pm4.g2 where pm1.g1.e1 = pm4.g2.e1 or pm4.g2.e2 = 123",             //$NON-NLS-1$
            RealMetadataFactory.example1Cached(),
            null, null, null,
            TestOptimizer.SHOULD_FAIL);
    }

    /**
     * Access patterns on models that support joins requires that the access patterns are satisfied prior to
     * RulePlanJoins
     */
    @Test public void testAccessPatternsGroupsInSameModelFails() {
        TestOptimizer.helpPlan("select pm5.g1.e1 from pm5.g1, pm5.g2 where pm5.g1.e1 = pm5.g2.e1",              //$NON-NLS-1$
            RealMetadataFactory.example1Cached(),
            null, null, null, TestOptimizer.SHOULD_FAIL);
    }

    // ==================================================================================
    // ACCESS PATTERNS
    // ==================================================================================

    @Test public void testPushingCriteriaThroughFrameAccessPattern0() {
        TestOptimizer.helpPlan("select * from vm1.g9 where vm1.g9.e1='abc'", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "SELECT 1 FROM pm4.g1 WHERE pm4.g1.e1 = 'abc'", //$NON-NLS-1$
                            "SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1 WHERE pm1.g1.e1 = 'abc'" } ); //$NON-NLS-1$
    }

    /**
     * pm4.g2.e5 or pm4.g2.e2 also need to be in criteria
     */
    @Test public void testPushingCriteriaThroughFrameAccessPattern1() {
        TestOptimizer.helpPlan("select * from vm1.g1, vm1.g10 where vm1.g1.e1='abc' and vm1.g1.e1=vm1.g10.e1", RealMetadataFactory.example1Cached(), null, TestOptimizer.getGenericFinder(), //$NON-NLS-1$
            null, TestOptimizer.SHOULD_FAIL );
    }

    @Test public void testPushingCriteriaThroughFrameAccessPattern2() {
        TestOptimizer.helpPlan("select e1 from vm1.g11 where vm1.g11.e1='abc' and vm1.g11.e2=123", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "SELECT pm4.g2.e1 FROM pm4.g2 WHERE (pm4.g2.e1 = 'abc') AND (pm4.g2.e2 = 123)" }); //$NON-NLS-1$
    }

    @Test public void testPushingCriteriaThroughFrameAccessPattern3() {
        TestOptimizer.helpPlan("select * from vm1.g1, vm1.g9 where vm1.g1.e1='abc' and vm1.g1.e1=vm1.g9.e1", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] {"SELECT 1 FROM pm4.g1 WHERE pm4.g1.e1 = 'abc'", //$NON-NLS-1$
                          "SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1 WHERE pm1.g1.e1 = 'abc'", //$NON-NLS-1$
                          "SELECT g1__1.e1, g1__1.e2, g1__1.e3, g1__1.e4 FROM pm1.g1 AS g1__1 WHERE g1__1.e1 = 'abc'"} ); //$NON-NLS-1$
    }

    /**
     * pm4.g2.e5 or pm4.g2.e2 also need to be in criteria
     */
    @Test public void testPushingCriteriaThroughFrameAccessPattern4() {
        TestOptimizer.helpPlan("select * from vm1.g10 where vm1.g10.e1='abc'", RealMetadataFactory.example1Cached(), null, TestOptimizer.getGenericFinder(), //$NON-NLS-1$
           null, TestOptimizer.SHOULD_FAIL );
    }

    /**
     * TODO: in this case we should perform a criteria optimization to create set criteria
     */
    @Test public void testCase6425() {
        String sql = "SELECT e1 FROM pm4.g1 WHERE e1 = '1' OR e1 = '2'"; //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, metadata, new String[] {"SELECT e1 FROM pm4.g1 WHERE (e1 = '1') OR (e1 = '2')"}); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    @Test public void testCase6425_2() {
        String sql = "SELECT e1 FROM pm4.g1 WHERE e1 = '1' OR (e1 = '2' AND e2 = 3)"; //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, metadata, new String[] {"SELECT e1 FROM pm4.g1 WHERE (e1 = '1') OR ((e1 = '2') AND (e2 = 3))"}); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    @Test public void testCase6425_4() throws Exception {
        String sql = "SELECT e1 FROM pm4.g1 WHERE e1 = '1' OR e2 = '2'"; //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        TestOptimizer.helpPlan(sql, metadata, null, TestOptimizer.ComparisonMode.FAILED_PLANNING);
    }

    /*
     * Criteria was preventing rule choose dependent from creating the appropriate dependent join
     */
    @Test public void testMultiAccessPatternWithCriteria() throws Exception {
        String sql = "SELECT pm1.g1.* FROM pm4.g1, pm5.g1, pm1.g1 where pm4.g1.e1 = pm1.g1.e1 and pm5.g1.e1 = pm1.g1.e1 and pm5.g1.e2 like '%x' "; //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        AnalysisRecord record = new AnalysisRecord(true, true);

        TestOptimizer.helpPlanCommand(TestOptimizer.helpGetCommand(sql, metadata), metadata, TestOptimizer.getGenericFinder(false), record,
                        new String[] {
                                "SELECT g_0.e2, g_0.e1 FROM pm5.g1 AS g_0 WHERE g_0.e1 IN (<dependent values>)", //$NON-NLS-1$
                                "SELECT g_0.e1, g_0.e2, g_0.e3, g_0.e4 FROM pm1.g1 AS g_0", //$NON-NLS-1$
                                "SELECT g_0.e1 FROM pm4.g1 AS g_0 WHERE g_0.e1 IN (<dependent values>)" }, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        assertTrue(record.getAnnotations().toString().contains("access pattern not satisfied by join"));
    }

}
