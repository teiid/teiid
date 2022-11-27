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

package org.teiid.query.processor;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.teiid.api.exception.query.QueryValidatorException;
import org.teiid.client.metadata.ParameterInfo;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.dqp.internal.process.TestPreparedStatement;
import org.teiid.metadata.ColumnSet;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.Schema;
import org.teiid.query.mapping.relational.QueryNode;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.metadata.TempMetadataStore;
import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.processor.proc.CreateCursorResultSetInstruction;
import org.teiid.query.processor.proc.ProcedurePlan;
import org.teiid.query.processor.proc.TestProcedureProcessor;
import org.teiid.query.processor.relational.DependentProcedureExecutionNode;
import org.teiid.query.processor.relational.RelationalNode;
import org.teiid.query.processor.relational.RelationalPlan;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.SPParameter;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.util.CommandContext;

@SuppressWarnings("nls")
public class TestProcedureRelational {

    @Test public void testProcInExistsSubquery() throws Exception {
        String sql = "select pm1.g1.e1 from pm1.g1 where exists (select * from (EXEC pm1.vsp9(pm1.g1.e2 + 1)) x where x.e1 = pm1.g1.e1)"; //$NON-NLS-1$

        List<?>[] expected = new List[] {
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
        };

        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);

        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached());

        TestProcessor.helpProcess(plan, dataManager, expected);
    }

    @Test public void testProcTableFunction() throws Exception {
        String sql = "select param1, param2, e1, e2 from pm1.vsp26 as x, texttable('abc' columns a string) as y where param1=1 and param2='a'"; //$NON-NLS-1$

        List<?>[] expected = new List[] {
                Arrays.asList(new Object[] { new Integer(1), "a", "a", new Integer(3)}), //$NON-NLS-1$  //$NON-NLS-2$
        };

        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);

        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached());

        TestProcessor.helpProcess(plan, dataManager, expected);
    }

    @Test public void testProcInSelectScalarSubquery() throws Exception {
        String sql = "select (EXEC pm1.vsp36(pm1.g1.e2)) from pm1.g1 where pm1.g1.e1 = 'a'"; //$NON-NLS-1$

        List<?>[] expected = new List[] {
            Arrays.asList(new Object[] { new Integer(0) }),
            Arrays.asList(new Object[] { new Integer(6) }),
            Arrays.asList(new Object[] { new Integer(0) }),
        };

        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);

        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached());

        TestProcessor.helpProcess(plan, dataManager, expected);
    }

    @Test public void testProcAsTable(){
        String sql = "select param1, param2, e1, e2 from pm1.vsp26 where param1=1 and param2='a'"; //$NON-NLS-1$

        // Create expected results
        List<?>[] expected = new List[] {
            Arrays.asList(new Object[] { new Integer(1), "a", "a", new Integer(3)}), //$NON-NLS-1$  //$NON-NLS-2$
        };
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);
        // Plan query
        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached());

        assertFalse(plan.requiresTransaction(false));
        assertTrue(plan.requiresTransaction(true)); //TODO: this should be false as there is only a single execution possible
        // Run query
        TestProcessor.helpProcess(plan, dataManager, expected);
    }

    //virtual group with procedure in transformation
    @Test public void testAliasedProcAsTable(){
        String sql = "select param1, param2, e1, e2 from pm1.vsp26 as x where param1=1 and param2='a'"; //$NON-NLS-1$

        // Create expected results
        List<?>[] expected = new List[] {
            Arrays.asList(new Object[] { new Integer(1), "a", "a", new Integer(3)}), //$NON-NLS-1$  //$NON-NLS-2$
        };
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);
        // Plan query
        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached());
        // Run query
        TestProcessor.helpProcess(plan, dataManager, expected);
    }

    @Test public void testAliasedJoin(){
        String sql = "select x.param1, x.param2, y.param1, y.param2, x.e1 from pm1.vsp26 as x, pm1.vsp26 as y where x.param1=1 and x.param2='a' and y.param1 = 2 and y.param2 = 'b'"; //$NON-NLS-1$

        // Create expected results
        List<?>[] expected = new List[] {
            Arrays.asList(new Object[] { new Integer(1), "a", new Integer(2), "b", "a"}), //$NON-NLS-1$  //$NON-NLS-2$ //$NON-NLS-3$
        };
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);
        // Plan query
        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached());
        // Run query
        TestProcessor.helpProcess(plan, dataManager, expected);
    }


    @Test public void testAliasedJoin1(){
        String sql = "select x.param1, x.param2, y.param1, y.param2, x.e1 from pm1.vsp26 as x, pm1.vsp26 as y where x.param1=1 and x.param2='a' and y.param1 = x.param1 and y.param2 = x.param2"; //$NON-NLS-1$

        // Create expected results
        List<?>[] expected = new List[] {
            Arrays.asList(new Object[] { new Integer(1), "a", new Integer(1), "a", "a"}), //$NON-NLS-1$  //$NON-NLS-2$ //$NON-NLS-3$
        };
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);
        // Plan query
        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached());
        // Run query
        TestProcessor.helpProcess(plan, dataManager, expected);
    }

    /**
     * Will fail due to access pattern validation (missing param2 assignment)
     */
    @Test public void testProcAsTable1(){
        String sql = "select param1, param2, e1, e2 from pm1.vsp26 where param1=1"; //$NON-NLS-1$

        TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), null, TestOptimizer.getGenericFinder(), null, false);
    }

    /**
     * Will fail since less than does not constitue an input
     */
    @Test public void testProcAsTable2(){
        String sql = "select param1, param2, e1, e2 from pm1.vsp26 where param1<1 and param2='a'"; //$NON-NLS-1$

        TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), null, TestOptimizer.getGenericFinder(), null, false);
    }

    @Test public void testProcAsTable3(){
        String sql = "select param1, param2, e1, e2 from pm1.vsp26 where param1 in (1,2,3) and param2 in ('a', 'b') order by param1, param2"; //$NON-NLS-1$

        // Create expected results
        List<?>[] expected = new List[] {
            Arrays.asList(new Object[] { new Integer(1), "a", "a", new Integer(3)}), //$NON-NLS-1$  //$NON-NLS-2$
            Arrays.asList(new Object[] { new Integer(1), "b", "b", new Integer(2)}), //$NON-NLS-1$  //$NON-NLS-2$
            Arrays.asList(new Object[] { new Integer(2), "a", "a", new Integer(3)}), //$NON-NLS-1$  //$NON-NLS-2$
            Arrays.asList(new Object[] { new Integer(2), "b", "b", new Integer(2)}), //$NON-NLS-1$  //$NON-NLS-2$
            Arrays.asList(new Object[] { new Integer(3), "a", "a", new Integer(3)}), //$NON-NLS-1$  //$NON-NLS-2$
        };
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);
        // Plan query
        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached());
        // Run query
        TestProcessor.helpProcess(plan, dataManager, expected);
    }

    /**
     * Will fail missing param2 assignment
     */
    @Test public void testProcAsTable4(){
        String sql = "select param1, param2, e1, e2 from pm1.vsp26 where param1=1 and not(param2 = 'a')"; //$NON-NLS-1$

        TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), null, TestOptimizer.getGenericFinder(), null, false);
    }

    /**
     * Will fail missing param2 assignment
     */
    @Test public void testProcAsTable5(){
        String sql = "select param1, param2, e1, e2 from pm1.vsp26 where param1=e2 and param2 = 'a'"; //$NON-NLS-1$

        TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), null, TestOptimizer.getGenericFinder(), null, false);
    }

    @Test public void testProcAsTableInJoin(){
        String sql = "select param1, param2, pm1.vsp26.e2 from pm1.vsp26, pm1.g1 where param1 = pm1.g1.e2 and param2 = pm1.g1.e1 order by param1, param2, e2"; //$NON-NLS-1$

        // Create expected results
        List<?>[] expected = new List[] {
            Arrays.asList(new Object[] { new Integer(0), "a", new Integer(0)}), //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(0), "a", new Integer(0)}), //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(0), "a", new Integer(0)}), //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(0), "a", new Integer(0)}), //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(0), "a", new Integer(3)}), //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(0), "a", new Integer(3)}), //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(1), "c", new Integer(1)}), //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(2), "b", new Integer(2)}), //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(3), "a", new Integer(3)}), //$NON-NLS-1$
        };
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);
        // Plan query
        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached());
        // Run query
        TestProcessor.helpProcess(plan, dataManager, expected);
    }

    @Test public void testProcAsTableInJoinWithOutJoinPredicate(){
        String sql = "select param1, param2, pm1.vsp26.e2, pm1.g1.e2 from pm1.vsp26, pm1.g1 where pm1.vsp26.e2 = pm1.g1.e2 and param1 = pm1.g1.e2 and param2 = pm1.g1.e1 order by param1, param2, pm1.vsp26.e2"; //$NON-NLS-1$

        // Create expected results
        List<?>[] expected = new List[] {
            Arrays.asList(new Object[] { new Integer(0), "a", new Integer(0), 0}), //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(0), "a", new Integer(0), 0}), //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(0), "a", new Integer(0), 0}), //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(0), "a", new Integer(0), 0}), //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(1), "c", new Integer(1), 1}), //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(2), "b", new Integer(2), 2}), //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(3), "a", new Integer(3), 3}), //$NON-NLS-1$
        };
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);
        // Plan query
        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached());
        // Run query
        TestProcessor.helpProcess(plan, dataManager, expected);
    }

    @Test public void testProcAsTableInSubquery(){
        String sql = "select param1, param2, pm1.vsp26.e2, (select count(e1) from pm1.vsp26 where param1 = 1 and param2 = 'a') x from pm1.vsp26, pm1.g1 where param1 = pm1.g1.e2 and param2 = pm1.g1.e1 order by param1, param2, e2"; //$NON-NLS-1$

        // Create expected results
        List<?>[] expected = new List[] {
            Arrays.asList(new Object[] { new Integer(0), "a", new Integer(0), new Integer(1)}), //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(0), "a", new Integer(0), new Integer(1)}), //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(0), "a", new Integer(0), new Integer(1)}), //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(0), "a", new Integer(0), new Integer(1)}), //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(0), "a", new Integer(3), new Integer(1)}), //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(0), "a", new Integer(3), new Integer(1)}), //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(1), "c", new Integer(1), new Integer(1)}), //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(2), "b", new Integer(2), new Integer(1)}), //$NON-NLS-1$
            Arrays.asList(new Object[] { new Integer(3), "a", new Integer(3), new Integer(1)}), //$NON-NLS-1$
        };
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);
        // Plan query
        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached());
        // Run query
        TestProcessor.helpProcess(plan, dataManager, expected);
    }

    private void helpTestProcRelational(String userQuery,
                                        String inputCriteria,
                                        String atomicQuery) {
        ProcessorPlan plan = TestOptimizer.helpPlan(userQuery, RealMetadataFactory.example1Cached(),
            new String[] {} );

        RelationalPlan rplan = (RelationalPlan)plan;

        RelationalNode root = rplan.getRootNode();

        while (root.getChildren() != null) {
            root = root.getChildren()[0];

            if (root instanceof DependentProcedureExecutionNode) {
                break;
            }
        }

        DependentProcedureExecutionNode dep = (DependentProcedureExecutionNode)root;

        assertEquals(inputCriteria, dep.getInputCriteria().toString());

        ProcedurePlan pp = (ProcedurePlan)dep.getProcessorPlan();

        CreateCursorResultSetInstruction ccrsi = (CreateCursorResultSetInstruction)pp.getOriginalProgram().getInstructionAt(0);

        plan = ccrsi.getCommand();

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);

        TestOptimizer.checkAtomicQueries(new String[] {atomicQuery}, plan);
    }

    //virtual group with procedure in transaformation
    @Test public void testProcInVirtualGroup1() {

        String userQuery = "select e1 from pm1.vsp26 where param1=1 and param2='a'"; //$NON-NLS-1$
        String inputCriteria = "(pm1.vsp26.param1 = 1) AND (pm1.vsp26.param2 = 'a')"; //$NON-NLS-1$
        String atomicQuery = "SELECT g_0.e1, g_0.e2 FROM pm1.g1 AS g_0 WHERE (g_0.e2 >= pm1.vsp26.param1) AND (g_0.e1 = pm1.vsp26.param2)"; //$NON-NLS-1$

        helpTestProcRelational(userQuery, inputCriteria, atomicQuery);
    }

    //virtual group with procedure in transformation
    @Test public void testCase3403() {
        String userQuery = "select e1 from pm1.vsp26 where param1=2 and param2='a' and 'x'='x'"; //$NON-NLS-1$
        String inputCriteria = "(pm1.vsp26.param1 = 2) AND (pm1.vsp26.param2 = 'a')"; //$NON-NLS-1$
        String atomicQuery = "SELECT g_0.e1, g_0.e2 FROM pm1.g1 AS g_0 WHERE (g_0.e2 >= pm1.vsp26.param1) AND (g_0.e1 = pm1.vsp26.param2)"; //$NON-NLS-1$

        helpTestProcRelational(userQuery, inputCriteria, atomicQuery);
    }

    @Test public void testCase3448() {
        String userQuery = "select e1 from pm1.vsp26 where (param1=1 and e2=2) and param2='a'"; //$NON-NLS-1$
        String inputCriteria = "(pm1.vsp26.param1 = 1) AND (pm1.vsp26.param2 = 'a')"; //$NON-NLS-1$
        String atomicQuery = "SELECT g_0.e1, g_0.e2 FROM pm1.g1 AS g_0 WHERE (g_0.e2 >= pm1.vsp26.param1) AND (g_0.e1 = pm1.vsp26.param2)"; //$NON-NLS-1$

        helpTestProcRelational(userQuery, inputCriteria, atomicQuery);
    }

    @Test public void testProcAsVirtualGroup2(){
        String sql = "select e1 from (SELECT * FROM pm1.vsp26 as P where P.e1='a') x where param1=1 and param2='a'"; //$NON-NLS-1$

        // Create expected results
        List<?>[] expected = new List[] {
            Arrays.asList(new Object[] { "a"}), //$NON-NLS-1$
        };
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);
        // Plan query
        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached());
        // Run query
        TestProcessor.helpProcess(plan, dataManager, expected);
    }

    @Test public void testProcAsVirtualGroup3(){
        String sql = "select e1 from (SELECT * FROM pm1.vsp26 as P where P.e1='a') x where param1=1 and param2='a'"; //$NON-NLS-1$

        // Create expected results
        List<?>[] expected = new List[] {
            Arrays.asList(new Object[] { "a"}), //$NON-NLS-1$
        };
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);
        // Plan query
        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached());
        // Run query
        TestProcessor.helpProcess(plan, dataManager, expected);
    }

    @Test public void testProcAsVirtualGroup4(){
        String sql = "SELECT P.e1 as ve3 FROM pm1.vsp26 as P, pm1.g2 where P.e1=g2.e1 and param1=1 and param2='a'"; //$NON-NLS-1$

        // Create expected results
        List<?>[] expected = new List[] {
            Arrays.asList(new Object[] { "a"}), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a"}), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a"}) //$NON-NLS-1$
        };
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);
        // Plan query
        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached());
        // Run query
        TestProcessor.helpProcess(plan, dataManager, expected);
    }

    @Test public void testProcAsVirtualGroup5(){
        String sql = "select e1 from (SELECT * FROM pm1.vsp26 as P where P.e1='a') x where param1=1 and param2='a' and e1='a'"; //$NON-NLS-1$

        // Create expected results
        List<?>[] expected = new List[] {
            Arrays.asList(new Object[] { "a"}), //$NON-NLS-1$
        };
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);
        // Plan query
        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached());
        // Run query
        TestProcessor.helpProcess(plan, dataManager, expected);
    }

    @Test public void testProcAsVirtualGroup6(){
        String sql = "SELECT P.e1 as ve3 FROM pm1.vsp26 as P, vm1.g1 where P.e1=g1.e1 and param1=1 and param2='a'"; //$NON-NLS-1$

        // Create expected results
        List<?>[] expected = new List[] {
                Arrays.asList(new Object[] { "a"}), //$NON-NLS-1$
                Arrays.asList(new Object[] { "a"}), //$NON-NLS-1$
                Arrays.asList(new Object[] { "a"}) //$NON-NLS-1$
        };
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);
        // Plan query
        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached());
        // Run query
        TestProcessor.helpProcess(plan, dataManager, expected);
    }

    @Test public void testProcAsVirtualGroup7(){
        String sql = "SELECT e1 FROM (SELECT p.e1, param1, param2 FROM pm1.vsp26 as P, vm1.g1 where P.e1=g1.e1) x where param1=1 and param2='a'"; //$NON-NLS-1$

        // Create expected results
        List<?>[] expected = new List[] {
                Arrays.asList(new Object[] { "a"}), //$NON-NLS-1$
                Arrays.asList(new Object[] { "a"}), //$NON-NLS-1$
                Arrays.asList(new Object[] { "a"}) //$NON-NLS-1$
        };
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);
        // Plan query
        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached());
        // Run query
        TestProcessor.helpProcess(plan, dataManager, expected);
    }

    @Test public void testProcAsVirtualGroup10_Defect20164(){
        String sql = "select e1 from (SELECT * FROM pm1.vsp26 as P where P.e1='a') x where (param1=1 and param2='a') and e1='c'"; //$NON-NLS-1$

        // Create expected results
        List<?>[] expected = new List[0];
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);
        // Plan query
        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached());
        // Run query
        TestProcessor.helpProcess(plan, dataManager, expected);
    }

    @Test public void testProcAsVirtualGroup8(){
        String sql = "SELECT P.e1 as ve3, P.e2 as ve4 FROM pm1.vsp26 as P where param1=1 and param2='a' and e2=3"; //$NON-NLS-1$

        // Create expected results
        List<?>[] expected = new List[] {
            Arrays.asList(new Object[] { "a", new Integer(3)}), //$NON-NLS-1$
        };
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);
        // Plan query
        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached());
        // Run query
        TestProcessor.helpProcess(plan, dataManager, expected);
    }

    //virtual group with procedure in transformation
    @Test public void testProcAsVirtualGroup9(){
        String sql = "SELECT P.e2 as ve3, P.e1 as ve4 FROM pm1.vsp47 as P where param1=1 and param2='a'"; //$NON-NLS-1$

        // Create expected results
        List<?>[] expected = new List[] {
            Arrays.asList(new Object[] { new Integer(1), "FOO" }), //$NON-NLS-1$
        };
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);
        // Plan query
        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached());
        // Run query
        TestProcessor.helpProcess(plan, dataManager, expected);
    }

    /**
     * Relies upon a default value of null for param1
     *
     * This is marked as defered since it is not desirable to support this behavior for a single default value
     */
    public void defer_testProcAsVirtualGroup9a(){
        String sql = "SELECT P.e2 as ve3, P.e1 as ve4 FROM pm1.vsp47 as P where param2='a'"; //$NON-NLS-1$

        // Create expected results
        List<?>[] expected = new List[] {
            Arrays.asList(new Object[] { new Integer(2112), "a" }), //$NON-NLS-1$
        };
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);
        // Plan query
        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached());
        // Run query
        TestProcessor.helpProcess(plan, dataManager, expected);
    }

    /**
     * Relies upon a default value of null for both parameters
     *
     * This is marked as defered since it is not desirable to support this behavior for a single default value
     */
    public void defer_testProcAsVirtualGroup9b(){
        String sql = "SELECT P.e2 as ve3, P.e1 as ve4 FROM pm1.vsp47 as P"; //$NON-NLS-1$

        // Create expected results
        List<?>[] expected = new List[] {
            Arrays.asList(new Object[] { new Integer(2112), null })
        };
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);
        // Plan query
        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached());
        // Run query
        TestProcessor.helpProcess(plan, dataManager, expected);
    }

    /**
     *  test for defect 22376
     */
    @Test public void testParameterPassing() throws Exception {
        MetadataStore metadataStore = new MetadataStore();
        Schema v1 = RealMetadataFactory.createVirtualModel("v1", metadataStore); //$NON-NLS-1$

        ColumnSet<Procedure> rs1 = RealMetadataFactory.createResultSet("v1.rs1", new String[] {"e1"}, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$

        QueryNode n1 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN declare string VARIABLES.x = '1'; SELECT e1 FROM v1.vp2 where v1.vp2.in = VARIABLES.x; END"); //$NON-NLS-1$
        Procedure vt1 = RealMetadataFactory.createVirtualProcedure("vp1", v1, null, n1); //$NON-NLS-1$
        vt1.setResultSet(rs1);

        ProcedureParameter p1 = RealMetadataFactory.createParameter("in", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING);  //$NON-NLS-1$
        QueryNode n2 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN declare string VARIABLES.x; declare string VARIABLES.y; VARIABLES.x = '2'; VARIABLES.y = v1.vp2.in; select VARIABLES.y; end"); //$NON-NLS-1$
        Procedure vt2 = RealMetadataFactory.createVirtualProcedure("vp2", v1, Arrays.asList(p1), n2); //$NON-NLS-1$
        vt2.setResultSet(RealMetadataFactory.createResultSet("v1.rs1", new String[] {"e1"}, new String[] { DataTypeManager.DefaultDataTypes.STRING }));

        String sql = "select * from (exec v1.vp1()) foo"; //$NON-NLS-1$

        List<?>[] expected = new List[] {
            Arrays.asList(new Object[] { "1" }), //$NON-NLS-1$
        };

        QueryMetadataInterface metadata = RealMetadataFactory.createTransformationMetadata(metadataStore, "foo");

        // Construct data manager with data
        // Plan query
        ProcessorPlan plan = TestProcedureProcessor.getProcedurePlan(sql, metadata);
        // Run query
        TestProcedureProcessor.helpTestProcess(plan, expected, new FakeDataManager(), metadata);

    }

    //virtual group with procedure in transformation
    @Test public void testCase6395ProcAsVirtualGroup9(){
        String sql = "SELECT P.e2 as ve3, P.e1 as ve4 FROM pm1.vsp47 as P where param1=1 and param2='a'"; //$NON-NLS-1$

        // Create expected results
        List<?>[] expected = new List[] {
            Arrays.asList(new Object[] { new Integer(1), "FOO" }), //$NON-NLS-1$
        };
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);
        // Plan query
        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached());
        // Run query
        TestProcessor.helpProcess(plan, dataManager, expected);
    }

    /**
     *  Case 6395 - This test case will now raise a QueryPlannerException.  param2 is required
     *  and not nullable.  This case is expected to fail because of 'param2 is null'
     */
    @Test public void testProcAsVirtualGroup2WithNull() throws Exception {
        String sql = "select e1 from (SELECT * FROM pm1.vsp26 as P where P.e1='a') x where param1=1 and param2 is null"; //$NON-NLS-1$

        // Create expected results
        List<?>[] expected = new List[] {
        };
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);
        // Plan query
        try {
            ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached());
            // Run query
            TestProcessor.doProcess(plan, dataManager, expected, TestProcessor.createCommandContext());
            fail("QueryPlannerException was expected.");  //$NON-NLS-1$
        } catch (QueryValidatorException e) {
            assertEquals("TEIID30164 The procedure parameter pm1.vsp26.param2 is not nullable, but is set to null.",e.getMessage());  //$NON-NLS-1$
        }
    }

    /**
     *  Case 6395 - This case is expected to succeed.  param1 and param2 are both required, but nulls
     *  are acceptable for both.
     */
    @Test public void testProcAsVirtualGroup2WithNull2() throws Exception {
        String sql = "select * from pm1.vsp47 where param1 is null and param2 is null"; //$NON-NLS-1$

        // Create expected results
        List<?>[] expected = new List[] {
                Arrays.asList(new Object[] { null, new Integer(2112), null, null })
        };
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);
        // Plan query
        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached());
        // Run query
        TestProcessor.helpProcess(plan, dataManager, expected);
    }

    /**
     *  Case 6395 - This will not throw an exception and the proc will not be invoked.
     */
    @Test public void testProcAsVirtualGroup2WithNull3() throws Exception {
        String sql = "select e1 from (SELECT * FROM pm1.vsp26 as P where P.e1='a') x where param1=1 and param2 = commandpayload()"; //$NON-NLS-1$

        // Create expected results
        List<?>[] expected = new List[] {
        };
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);
        // Plan query
        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached());
        // Run query
        CommandContext context = TestProcessor.createCommandContext();
        context.setMetadata(RealMetadataFactory.example1Cached());
        TestProcessor.helpProcess(plan, context, dataManager, expected);
    }

    /*
     * The following are tests that were removed from the validator.  We are no longer trying to validate a priori whether
     * procedure input criteria is valid.  This can be addressed later more generally when we do up front validation of
     * access patterns and access patterns have a wider range of semantics.
     *
    @Test public void testProcInVirtualGroupDefect14609_1() throws Exception{
        helpValidate("select ve3 from vm1.vgvp1 where ve1=1.1 and ve2='a'", new String[] {"ve1 = 1.1"}, RealMetadataFactory.example1Cached()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testProcInVirtualGroupDefect14609_2() throws Exception{
        helpValidate("select ve3 from vm1.vgvp1 where convert(ve1, integer)=1 and ve2='a'", new String[] {"convert(ve1, integer) = 1" }, RealMetadataFactory.example1Cached()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testProcInVirtualGroupDefect14609_3() throws Exception{
        helpValidate("select ve3 from vm1.vgvp1 where 1.1=ve1 and ve2='a'", new String[] {"1.1 = ve1" }, RealMetadataFactory.example1Cached()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testProcInVirtualGroupDefect14609_4() throws Exception{
        helpValidate("select ve3 from vm1.vgvp1 where 1=convert(ve1, integer) and ve2='a'", new String[] {"1 = convert(ve1, integer)" }, RealMetadataFactory.example1Cached()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testDefect15861() throws Exception{
        helpValidate("select ve3 from vm1.vgvp1 where (ve1=1 or ve1=2) and ve2='a'", new String[] {"(ve1 = 1) OR (ve1 = 2)", "ve1 = 2"}, RealMetadataFactory.example1Cached()); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testProcInVirtualGroup1_Defect20164() {
        helpFailProcedure("select ve3 from vm1.vgvp2 where (ve1=1 and ve2='a') or ve3='c'", RealMetadataFactory.example1Cached()); //$NON-NLS-1$
    }

    @Test public void testProcInVirtualGroup2_Defect20164() {
        helpFailProcedure("select ve3 from vm1.vgvp2 where ve1=1 or ve2='a'", RealMetadataFactory.example1Cached()); //$NON-NLS-1$
    }

    @Test public void testProcInVirtualGroup3_Defect20164() {
        helpFailProcedure("select ve3 from vm1.vgvp2, pm1.g1 where ve1=pm1.g1.e2 and ve2='a'", RealMetadataFactory.example1Cached()); //$NON-NLS-1$
    }

    @Test public void testProcInVirtualGroup4_Defect20164() {
        helpValidate("select ve3 from vm1.vgvp2 where (ve1=1 and ve2='a') and (ve3='a' OR ve3='c')", new String[0], RealMetadataFactory.example1Cached()); //$NON-NLS-1$
    }

    @Test public void testProcInVirtualGroup5_Defect20164() {
        helpFailProcedure("select ve3 from vm1.vgvp2 where ve1=1 and NOT(ve2='a')", RealMetadataFactory.example1Cached()); //$NON-NLS-1$
    }

    @Test public void testProcInVirtualGroup6_Defect20164() {
        helpValidate("select ve3 from vm1.vgvp2 where ve1=1 and ve2 is null", new String[0], RealMetadataFactory.example1Cached()); //$NON-NLS-1$
    }

    @Test public void testProcInVirtualGroup7_Defect20164() {
        helpFailProcedure("select ve3 from vm1.vgvp2 where ve1=1 and ve2 is not null", RealMetadataFactory.example1Cached()); //$NON-NLS-1$
    }*/

    /**
     * Ensures that dependent procedures are processed 1 at a time so that projected input values
     * are set correctly.
     */
    @Test public void testIssue119() throws Exception {
        MetadataStore metadataStore = new MetadataStore();
        Schema v1 = RealMetadataFactory.createVirtualModel("v1", metadataStore); //$NON-NLS-1$
        Schema pm1 = RealMetadataFactory.createPhysicalModel("pm1", metadataStore); //$NON-NLS-1$

        ProcedureParameter in = RealMetadataFactory.createParameter("in1", SPParameter.IN, DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$
        ColumnSet<Procedure> rs1 = RealMetadataFactory.createResultSet("v1.vp1.rs1", new String[] {"e1", "e2", "e3", "e4", "e5"}, new String[] { DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$

        QueryNode n1 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN SELECT vp1.in1 e1, x.in1 e2, x.e1 e3, y.in1 e4, y.e1 e5 FROM pm1.sp119 x, pm1.sp119 y where x.in1 = vp1.in1 and y.in1 = x.e1; END"); //$NON-NLS-1$
        Procedure vt1 = RealMetadataFactory.createVirtualProcedure("vp1", v1, Arrays.asList(in), n1); //$NON-NLS-1$
        vt1.setResultSet(rs1);

        ProcedureParameter in1 = RealMetadataFactory.createParameter("in1", SPParameter.IN, DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$
        ColumnSet<Procedure> rs3 = RealMetadataFactory.createResultSet("pm1.sp119.rs1", new String[] { "e1" }, new String[] { DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure sp1 = RealMetadataFactory.createStoredProcedure("sp119", pm1, Arrays.asList(in1));  //$NON-NLS-1$
        sp1.setResultSet(rs3);

        String sql = "select * from (exec v1.vp1(1)) foo order by e4, e5"; //$NON-NLS-1$

        List<?>[] expected = new List[] {
            Arrays.asList(1, 1, 3, 3, 5),
            Arrays.asList(1, 1, 3, 3, 8),
            Arrays.asList(1, 1, 6, 6, 8),
            Arrays.asList(1, 1, 6, 6, 11),
        };

        QueryMetadataInterface metadata = RealMetadataFactory.createTransformationMetadata(metadataStore, "foo");

        // Construct data manager with data
        // Plan query
        ProcessorPlan plan = TestProcedureProcessor.getProcedurePlan(sql, metadata);
        // Run query
        HardcodedDataManager dataManager = new HardcodedDataManager() {
            @Override
            public TupleSource registerRequest(CommandContext context,
                    Command command, String modelName,
                    RegisterRequestParameter parameterObject)
                    throws TeiidComponentException {
                if (command instanceof StoredProcedure) {
                    StoredProcedure proc = (StoredProcedure)command;
                    List<SPParameter> params = proc.getInputParameters();
                    assertEquals(1, params.size());
                    int value = (Integer)((Constant)params.get(0).getExpression()).getValue();
                    return new FakeTupleSource(command.getProjectedSymbols(), new List[] {
                        Arrays.asList(value+2), Arrays.asList(value+5)
                    });
                }
                return super.registerRequest(context, command, modelName,
                        parameterObject);
            }
        };

        TestProcedureProcessor.helpTestProcess(plan, expected, dataManager, metadata);

    }

    @Test public void testProcRelationalWithNoInputs() {
        String sql = "select e1 from pm1.vsp2 order by e1 desc limit 1"; //$NON-NLS-1$

        // Create expected results
        List<?>[] expected = new List[] {
                Arrays.asList("c") //$NON-NLS-1$
        };
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);
        // Plan query
        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached());
        // Run query
        TestProcessor.helpProcess(plan, dataManager, expected);
    }

    @Test public void testProcRelationalWithNoInputs1() {
        String sql = "select e1 from pm1.sp1"; //$NON-NLS-1$

        List<?>[] expected = new List[] {
                Arrays.asList("c") //$NON-NLS-1$
        };
        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("EXEC pm1.sp1()", expected);
        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.createTransformationMetadata(RealMetadataFactory.example1Store(), "e1"));
        TestProcessor.helpProcess(plan, dataManager, expected);
    }

    @Test public void testNestedJoin(){
        String sql = "select y.e1 from pm1.g1 join pm1.vsp26 as x on x.param1=1 and x.param2=pm1.g1.e1 join pm1.vsp26 as y on y.param1 = pm1.g1.e2 and y.param2 = x.e1"; //$NON-NLS-1$

        List<?>[] expected = new List[] {
            Arrays.asList("a"),Arrays.asList("a"),Arrays.asList("a"),Arrays.asList("a"),Arrays.asList("a"),Arrays.asList("a"),Arrays.asList("c"),Arrays.asList("b"),Arrays.asList("a"), //$NON-NLS-1$  //$NON-NLS-2$ //$NON-NLS-3$
        };
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);
        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached());
        TestProcessor.helpProcess(plan, dataManager, expected);
    }

    @Test public void testProcAsTable3Prepared() throws Exception{
        String sql = "select param1, param2, e1, e2 from pm1.vsp26 where param1 in (?,?,?) and param2 in ('a', 'b') order by param1, param2"; //$NON-NLS-1$

        // Create expected results
        List<?>[] expected = new List[] {
            Arrays.asList(new Object[] { new Integer(1), "a", "a", new Integer(3)}), //$NON-NLS-1$  //$NON-NLS-2$
            Arrays.asList(new Object[] { new Integer(1), "b", "b", new Integer(2)}), //$NON-NLS-1$  //$NON-NLS-2$
            Arrays.asList(new Object[] { new Integer(2), "a", "a", new Integer(3)}), //$NON-NLS-1$  //$NON-NLS-2$
            Arrays.asList(new Object[] { new Integer(2), "b", "b", new Integer(2)}), //$NON-NLS-1$  //$NON-NLS-2$
            Arrays.asList(new Object[] { new Integer(3), "a", "a", new Integer(3)}), //$NON-NLS-1$  //$NON-NLS-2$
        };
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);

        List<?> values = Arrays.asList(1, 2, 3);
        //values = Collections.EMPTY_LIST;
        TempMetadataAdapter tma = new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore());
        TestPreparedStatement.helpTestProcessing(sql, values, expected, dataManager, tma, false, RealMetadataFactory.example1VDB());
    }

}
