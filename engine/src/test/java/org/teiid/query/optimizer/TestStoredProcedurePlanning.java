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

import static org.teiid.query.processor.TestProcessor.*;

import java.util.List;

import org.junit.Ignore;
import org.junit.Test;
import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.metadata.TempMetadataStore;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.processor.HardcodedDataManager;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.util.CommandContext;

@SuppressWarnings("nls")
public class TestStoredProcedurePlanning {

    /**
     * Test planning stored queries. GeminiStoredQueryTestPlan - 1a
     */
    @Ignore("stored procedure wrapper removal logic has been removed")
    @Test public void testStoredQuery1() {
        ProcessorPlan plan = TestOptimizer.helpPlan("EXEC pm1.sq1()", new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore()), //$NON-NLS-1$
            new String[] { "SELECT e1, e2 FROM pm1.g1" }); //$NON-NLS-1$
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    /**
     * Test planning stored queries
     */
    @Ignore("stored procedure wrapper removal logic has been removed")
    @Test public void testStoredQuery2() {
        ProcessorPlan plan = TestOptimizer.helpPlan("EXEC pm1.sq1()", new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore()), //$NON-NLS-1$
            new String[] { "SELECT e1, e2 FROM pm1.g1" }); //$NON-NLS-1$
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    /**
     * Test planning stored queries. GeminiStoredQueryTestPlan - 1b
     */
    @Ignore("stored procedure wrapper removal logic has been removed")
    @Test public void testStoredQuery3() {
        ProcessorPlan plan = TestOptimizer.helpPlan("EXEC pm1.sq2('1')", new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore()), //$NON-NLS-1$
            new String[] { "SELECT e1, e2 FROM pm1.g1 WHERE e1 = '1'" }); //$NON-NLS-1$
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }
    @Ignore("stored procedure wrapper removal logic has been removed")
    @Test public void testStoredQuery4() {
        ProcessorPlan plan = TestOptimizer.helpPlan("select x.e1 from (EXEC pm1.sq1()) as x", new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore()), //$NON-NLS-1$
            new String[] { "SELECT e1 FROM pm1.g1" }); //$NON-NLS-1$
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    @Test public void testStoredQuery5() {
        ProcessorPlan plan = TestOptimizer.helpPlan("EXEC pm1.sp1()", new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore()), //$NON-NLS-1$
            new String[] { "EXEC pm1.sp1()" }); //$NON-NLS-1$
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

    @Test public void testStoredQuery6() {
        ProcessorPlan plan = TestOptimizer.helpPlan("select x.e1 from (EXEC pm1.sp1()) as x", new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore()), //$NON-NLS-1$
            new String[] { "EXEC pm1.sp1()" }); //$NON-NLS-1$
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
            2,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    @Ignore("stored procedure wrapper removal logic has been removed")
    @Test public void testStoredQuery7() {
        ProcessorPlan plan = TestOptimizer.helpPlan("EXEC pm1.sqsp1()", new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore()), //$NON-NLS-1$
            new String[] { "EXEC pm1.sp1()" }); //$NON-NLS-1$
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
            2,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    /**
     * Test planning stored queries. GeminiStoredQueryTestPlan - 1c
     */
    @Ignore("stored procedure wrapper removal logic has been removed")
    @Test public void testStoredQuery8() {
        ProcessorPlan plan = TestOptimizer.helpPlan("EXEC pm1.sq3('1', 1)", new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore()), //$NON-NLS-1$
            new String[] { "SELECT e1, e2 FROM pm1.g1 WHERE e1 = '1'", "SELECT e1, e2 FROM pm1.g1 WHERE e2 = 1" }); //$NON-NLS-1$ //$NON-NLS-2$
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
            0,      // Project
            0,      // Select
            0,      // Sort
            1       // UnionAll
        });
    }

    /**
     * Test planning stored queries. GeminiStoredQueryTestPlan - 5a
     */
    @Ignore("stored procedure wrapper removal logic has been removed")
    @Test public void testStoredQuery9() {
        ProcessorPlan plan = TestOptimizer.helpPlan("EXEC pm1.sq4()", new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore()), //$NON-NLS-1$
            new String[] {"SELECT e1, e2 FROM pm1.g1" }); //$NON-NLS-1$
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    /**
     * Test planning stored queries. GeminiStoredQueryTestPlan - 5b
     */
    @Ignore("stored procedure wrapper removal logic has been removed")
    @Test public void testStoredQuery10() {
        ProcessorPlan plan = TestOptimizer.helpPlan("EXEC pm1.sq5('1')", new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore()), //$NON-NLS-1$
            new String[] { "SELECT e1, e2 FROM pm1.g1 WHERE e1 = '1'"}); //$NON-NLS-1$
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

     /**
     * Test planning stored queries. GeminiStoredQueryTestPlan - 5c
     */
    @Ignore("stored procedure wrapper removal logic has been removed")
    @Test public void testStoredQuery11() {
        ProcessorPlan plan = TestOptimizer.helpPlan("EXEC pm1.sq6()", new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore()), //$NON-NLS-1$
            new String[] {"SELECT e1, e2 FROM pm1.g1 WHERE e1 = '1'" }); //$NON-NLS-1$
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

     /**
     * Test planning stored queries. GeminiStoredQueryTestPlan - 6a
     */
    @Ignore("stored procedure wrapper removal logic has been removed")
    @Test public void testStoredQuery12() {
        ProcessorPlan plan = TestOptimizer.helpPlan("EXEC pm1.sq7()", new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore()), //$NON-NLS-1$
            new String[] { "SELECT e1 FROM pm1.g1" }); //$NON-NLS-1$
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    /**
     * Test planning stored queries. GeminiStoredQueryTestPlan - 6c
     */
    @Ignore("stored procedure wrapper removal logic has been removed")
    @Test public void testStoredQuery13() {
        ProcessorPlan plan = TestOptimizer.helpPlan("EXEC pm1.sq8('1')", new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore()), //$NON-NLS-1$
            new String[] { "SELECT e1 FROM pm1.g1 WHERE e1 = '1'" }); //$NON-NLS-1$
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    /**
     * Test planning stored queries. GeminiStoredQueryTestPlan - 6b
     */
    @Ignore("stored procedure wrapper removal logic has been removed")
    @Test public void testStoredQuery14() {
        ProcessorPlan plan = TestOptimizer.helpPlan("EXEC pm1.sq9('1')", new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore()), //$NON-NLS-1$
            new String[] { "SELECT e1 FROM pm1.g1 WHERE e1 = '1'" }); //$NON-NLS-1$
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    /**
     * Test planning stored queries. GeminiStoredQueryTestPlan - 6d
     */
    @Ignore("stored procedure wrapper removal logic has been removed")
    @Test public void testStoredQuery15() {
        ProcessorPlan plan = TestOptimizer.helpPlan("EXEC pm1.sq10('1', 2)", new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore()), //$NON-NLS-1$
            new String[] { "SELECT e1 FROM pm1.g1 WHERE (e1 = '1') AND (e2 = 2)" }); //$NON-NLS-1$
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    /**
     * Test planning stored queries.
     */
    @Test public void testStoredQuery16() {
        ProcessorPlan plan = TestOptimizer.helpPlan("EXEC pm1.sp2(1)", new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore()), //$NON-NLS-1$
            new String[] { "EXEC pm1.sp2(1)" }); //$NON-NLS-1$
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
     * Test planning stored queries. GeminiStoredQueryTestPlan - 6d
     */
    @Ignore("stored procedure wrapper removal logic has been removed")
    @Test public void testStoredQuery17() {
        ProcessorPlan plan = TestOptimizer.helpPlan("EXEC pm1.sq11(1, 2)", new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore()), //$NON-NLS-1$
            new String[] { "EXEC pm1.sp2(?)" }); //$NON-NLS-1$

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
            2,      // Project
            1,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    //GeminiStoredQueryTestPlan - 2a, 2b
    @Ignore("stored procedure wrapper removal logic has been removed")
    @Test public void testStoredQuery18() {
        ProcessorPlan plan = TestOptimizer.helpPlan("EXEC pm1.sq12('1', 1)", new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore()), //$NON-NLS-1$
            new String[] { "INSERT INTO pm1.g1 (e1, e2) VALUES ('1', 1)" }); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    //GeminiStoredQueryTestPlan - 2c
    @Ignore("stored procedure wrapper removal logic has been removed")
    @Test public void testStoredQuery19() {
        ProcessorPlan plan = TestOptimizer.helpPlan("EXEC pm1.sq13('1')", new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore()), //$NON-NLS-1$
            new String[] { "INSERT INTO pm1.g1 (e1, e2) VALUES ('1', 2)" }); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    //GeminiStoredQueryTestPlan - 3c
    @Ignore("stored procedure wrapper removal logic has been removed")
    @Test public void testStoredQuery20() {
        ProcessorPlan plan = TestOptimizer.helpPlan("EXEC pm1.sq14('1', 2)", new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore()), //$NON-NLS-1$
            new String[] { "UPDATE pm1.g1 SET e1 = '1' WHERE e2 = 2" }); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    //GeminiStoredQueryTestPlan - 4b
    @Ignore("stored procedure wrapper removal logic has been removed")
    @Test public void testStoredQuery21() {
        ProcessorPlan plan = TestOptimizer.helpPlan("EXEC pm1.sq15('1', 2)", new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore()), //$NON-NLS-1$
            new String[] { "DELETE FROM pm1.g1 WHERE (e1 = '1') AND (e2 = 2)" }); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    @Ignore("stored procedure wrapper removal logic has been removed")
    @Test public void testStoredQuery22() {
        ProcessorPlan plan = TestOptimizer.helpPlan("select e1 from (EXEC pm1.sq1()) as x where e1='a' union (select e1 from vm1.g2 where e1='b')", new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore()), //$NON-NLS-1$
            new String[] { "SELECT g_0.e1 FROM pm1.g1 AS g_0 WHERE g_0.e1 = 'a'", "SELECT g_0.e1 FROM pm1.g1 AS g_0, pm1.g2 AS g_1 WHERE (g_0.e1 = g_1.e1) AND (g_0.e1 = 'b') AND (g_1.e1 = 'b')" }); //$NON-NLS-1$ //$NON-NLS-2$

        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            1,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            0,      // Project
            0,      // Select
            0,      // Sort
            1       // UnionAll
        });
    }

    @Ignore("stored procedure wrapper removal logic has been removed")
    @Test public void testStoredQuery23() {
        ProcessorPlan plan = TestOptimizer.helpPlan("EXEC pm1.sq16()", new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore()), //$NON-NLS-1$
            new String[] { "INSERT INTO pm1.g1 (e1, e2) VALUES ('1', 2)" }); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    @Test public void testStoredQuery24() {
        ProcessorPlan plan = TestOptimizer.helpPlan("EXEC pm1.sp3()", new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore()), //$NON-NLS-1$
            new String[] { "EXEC pm1.sp3()" }); //$NON-NLS-1$

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

    // test implicit type conversion of argument
    @Ignore("stored procedure wrapper removal logic has been removed")
    @Test public void testStoredQuery25() {
        ProcessorPlan plan = TestOptimizer.helpPlan("EXEC pm1.sq15(1, 2)", new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore()), //$NON-NLS-1$
            new String[] { "DELETE FROM pm1.g1 WHERE (e1 = '1') AND (e2 = 2)" }); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    /**
     * union of two stored procs - case #1466
     */
    @Test public void testStoredProc1() {
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT * FROM (EXEC pm1.sp2(1)) AS x UNION ALL SELECT * FROM (EXEC pm1.sp2(2)) AS y", new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore()), //$NON-NLS-1$
            new String[] { "EXEC pm1.sp2(1)", "EXEC pm1.sp2(2)" }); //$NON-NLS-1$ //$NON-NLS-2$
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
            4,      // Project
            0,      // Select
            0,      // Sort
            1       // UnionAll
        });
    }

    /**
     * union of stored proc and query - case #1466
     */
    @Test public void testStoredProc2() {
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT * FROM (EXEC pm1.sp2(1)) AS x UNION ALL SELECT e1, e2 FROM pm1.g1", new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore()), //$NON-NLS-1$
            new String[] { "EXEC pm1.sp2(1)", "SELECT e1, e2 FROM pm1.g1" }); //$NON-NLS-1$ //$NON-NLS-2$
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
            2,      // Project
            0,      // Select
            0,      // Sort
            1       // UnionAll
        });
    }

    @Test(expected=ExpressionEvaluationException.class) public void testProcedureParamSourceFunction() throws Exception {
        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();

        helpTestProcedureParameterExpression(bsc);
    }

    @Test public void testProcedureParamSourceFunctionPasses() throws Exception {
        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        bsc.setCapabilitySupport(Capability.PROCEDURE_PARAMETER_EXPRESSION, true);

        helpTestProcedureParameterExpression(bsc);
    }

    private void helpTestProcedureParameterExpression(
            BasicSourceCapabilities bsc) throws Exception {
        String sql = "exec proc(foo('1'))"; //$NON-NLS-1$
        TransformationMetadata tm = RealMetadataFactory.fromDDL(
                "create foreign procedure proc (param object); " +
                "create foreign function foo (param string) returns object;", "x", "y");

        ProcessorPlan plan = helpGetPlan(sql, tm, new DefaultCapabilitiesFinder(bsc));

        HardcodedDataManager dataManager = new HardcodedDataManager(tm);
        dataManager.addData("EXEC proc(foo('1'))", new List<?>[] {});
        CommandContext context = createCommandContext();
        context.setMetadata(tm);
        helpProcess(plan, context, dataManager, new List<?>[] {});
    }

}
