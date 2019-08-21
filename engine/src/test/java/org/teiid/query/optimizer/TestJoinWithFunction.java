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

import java.util.List;

import org.teiid.api.exception.query.QueryParserException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.api.exception.query.QueryValidatorException;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.optimizer.TestOptimizer.ComparisonMode;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.relational.RelationalPlan;
import org.teiid.query.sql.symbol.AliasSymbol;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.util.SymbolMap;

import junit.framework.TestCase;


/**
 * <p><code>TestCase</code> to cover planning and optimization of JOINs which
 * use a scalar function as a symbol or as part of the JOIN criteria.
 *
 * <p>All tests should verify and validate that the scalar function is being
 * pushed/merged with the correct layer of the plan.  For example, if a
 * non-deterministic function is being merged with a parent node the resulting
 * query may alter the final result set.  Most specifically, if the function is
 * executed too late during the processing of a command, the results may be
 * different than if it were executed earlier during processing.
 * @since 6.0
 */
public class TestJoinWithFunction extends TestCase {

    /**
     * <p>Test the use of a non-deterministic function on a user command that
     * performs a JOIN of two sources.
     *
     * <p>The function should be executed on the result returned from the JOIN and
     * is expected to be executed for each row of the final result set.
     * @throws TeiidComponentException
     * @throws QueryValidatorException
     * @throws QueryResolverException
     * @throws QueryParserException
     */
    public void testNonDeterministicPostJoin() throws Exception {
        // source query for one side of a JOIN
        String leftQuery = "SELECT pm1.g1.e1 as ID, pm1.g1.e2, pm1.g1.e3, pm1.g1.e4 " //$NON-NLS-1$
                + "FROM pm1.g1"; //$NON-NLS-1$
        // source query for other side of a JOIN
        String rightQuery = "SELECT pm2.g2.e1 as ID, pm2.g2.e2, pm2.g2.e3, pm2.g2.e4 " //$NON-NLS-1$
                + "FROM pm2.g2"; //$NON-NLS-1$

        // User Command
        /*
         * Return everything from the JOIN. RandomTop is the use of RAND() on
         * the user command and should result in unique random numbers for each
         * row in the JOINed output.
         */
        String sql = "SELECT l.ID, l.e2, l.e3, l.e4, r.ID, r.e2, r.e3, r.e4, RAND() AS RandomTop " + //$NON-NLS-1$
                "FROM (" + leftQuery + ") AS l, " + //$NON-NLS-1$ //$NON-NLS-2$
                "("	+ rightQuery + ") AS r " + //$NON-NLS-1$ //$NON-NLS-2$
                "WHERE l.ID = r.ID"; //$NON-NLS-1$

        // The user command should result in two atomic commands
        String[] expected = new String[] {
                "SELECT g_0.e1 AS c_0, g_0.e2 AS c_1, g_0.e3 AS c_2, g_0.e4 AS c_3 FROM pm1.g1 AS g_0 ORDER BY c_0", //$NON-NLS-1$
                "SELECT g_0.e1 AS c_0, g_0.e2 AS c_1, g_0.e3 AS c_2, g_0.e4 AS c_3 FROM pm2.g2 AS g_0 ORDER BY c_0", //$NON-NLS-1$
                };

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, TestOptimizer.example1(), expected,
                ComparisonMode.EXACT_COMMAND_STRING);
        TestOptimizer.checkNodeTypes(plan, new int[] { 2, // Access
                0, // DependentAccess
                0, // DependentSelect
                0, // DependentProject
                0, // DupRemove
                0, // Grouping
                0, // NestedLoopJoinStrategy
                1, // MergeJoinStrategy
                0, // Null
                0, // PlanExecution
                1, // Project
                0, // Select
                0, // Sort
                0  // UnionAll
        });
    }

    /**
     * <p>Test the use of a non-deterministic function on the source command of a JOIN
     * defined by a user command which performs a JOIN of two sources.
     *
     * <p>The function should be executed on the result that will be used for one side
     * of the JOIN.  The function should only be executed for each row of the the result
     * set returned from the left-side of the JOIN which should result in the same return
     * value for multiple rows of the final result set after the JOIN is completed.  For
     * example, if the left-side query is expected to return one row and the right-side
     * query will return three rows which match the JOIN criteria for the one row on the
     * left-side then the expected result should be that the function be executed once to
     * represent the one row from the left-side and the function's return value will be
     * repeated for each of the three post-JOINed results.
     * @throws TeiidComponentException
     * @throws QueryValidatorException
     * @throws QueryResolverException
     * @throws QueryParserException
     */
    public void testNonDeterministicPreJoin() throws Exception {
        // source query for one side of a JOIN
        String leftQuery = "SELECT pm1.g1.e1 as ID, pm1.g1.e2, pm1.g1.e3, pm1.g1.e4, RAND() AS RandomLeft " //$NON-NLS-1$
                + "FROM pm1.g1"; //$NON-NLS-1$
        // source query for other side of a JOIN
        String rightQuery = "SELECT pm1.g2.e1 as ID, pm1.g2.e2, pm1.g2.e3, pm1.g2.e4 " //$NON-NLS-1$
                + "FROM pm1.g2"; //$NON-NLS-1$

        // User Command
        /*
         * Return everything from the JOIN. TopRandom is the use of RAND() on
         * the user command while RandomLeft is the use of RAND() within a
         * source node.
         */
        String sql = "SELECT l.ID, l.e2, l.e3, l.e4, r.ID, r.e2, r.e3, r.e4, l.RandomLeft " + //$NON-NLS-1$
                "FROM (" + leftQuery + ") AS l, " + //$NON-NLS-1$ //$NON-NLS-2$
                "(" + rightQuery + ") AS r " + //$NON-NLS-1$ //$NON-NLS-2$
                "WHERE l.ID = r.ID"; //$NON-NLS-1$

        // The user command should result in two atomic commands
        String[] expected = new String[] {
                "SELECT g_0.e1, g_0.e2, g_0.e3, g_0.e4 FROM pm1.g1 AS g_0",  //$NON-NLS-1$
                "SELECT g_0.e1 AS c_0, g_0.e2 AS c_1, g_0.e3 AS c_2, g_0.e4 AS c_3 FROM pm1.g2 AS g_0 ORDER BY c_0",  //$NON-NLS-1$
        };

        // create a plan and assert our atomic queries
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, TestOptimizer.example1(), expected,
                ComparisonMode.EXACT_COMMAND_STRING);
        TestOptimizer.checkNodeTypes(plan, new int[] { 2, // Access
                0, // DependentAccess
                0, // DependentSelect
                0, // DependentProject
                0, // DupRemove
                0, // Grouping
                0, // NestedLoopJoinStrategy
                1, // MergeJoinStrategy
                0, // Null
                0, // PlanExecution
                2, // Project
                0, // Select
                0, // Sort
                0 // UnionAll
        });
    }

    /**
     * Note that we detect the lower rand is not used
     */
    public void testNonDeterministicPreJoin1() throws Exception {
        // source query for one side of a JOIN
        String leftQuery = "SELECT pm1.g1.e1 as ID, pm1.g1.e2, pm1.g1.e3, pm1.g1.e4, RAND() AS RandomLeft " //$NON-NLS-1$
                + "FROM pm1.g1"; //$NON-NLS-1$
        // source query for other side of a JOIN
        String rightQuery = "SELECT pm1.g2.e1 as ID, pm1.g2.e2, pm1.g2.e3, pm1.g2.e4 " //$NON-NLS-1$
                + "FROM pm1.g2"; //$NON-NLS-1$

        // User Command
        /*
         * Return everything from the JOIN. TopRandom is the use of RAND() on
         * the user command while RandomLeft is the use of RAND() within a
         * source node.
         */
        String sql = "SELECT l.ID, l.e2, l.e3, l.e4, r.ID, r.e2, r.e3, r.e4 " + //$NON-NLS-1$
                "FROM (" + leftQuery + ") AS l, " + //$NON-NLS-1$ //$NON-NLS-2$
                "(" + rightQuery + ") AS r " + //$NON-NLS-1$ //$NON-NLS-2$
                "WHERE l.ID = r.ID"; //$NON-NLS-1$

        // The user command should result in two atomic commands
        String[] expected = new String[] {
                "SELECT g_0.e1, g_0.e2, g_0.e3, g_0.e4, g_1.e1, g_1.e2, g_1.e3, g_1.e4 FROM pm1.g1 AS g_0, pm1.g2 AS g_1 WHERE g_0.e1 = g_1.e1",  //$NON-NLS-1$
        };

        // create a plan and assert our atomic queries
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, TestOptimizer.example1(), expected,
                ComparisonMode.EXACT_COMMAND_STRING);
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    /**
     * <p>Test the use of a non-deterministic function on the sub-command of a user
     * command and the user command itself, which performs a JOIN of two sources.
     *
     * <p>This test combines the PostJoin and PreJoin test cases.
     * @throws TeiidComponentException
     * @throws QueryValidatorException
     * @throws QueryResolverException
     * @throws QueryParserException
     * @see #testNonDeterministicPostJoin
     * @see #testNonDeterministicPreJoin
     */
    public void testNonDeterministicPrePostJoin() throws Exception {
        // source query for one side of a JOIN
        String leftQuery = "SELECT pm1.g1.e1 as ID, pm1.g1.e2, pm1.g1.e3, pm1.g1.e4, RAND() AS RandomLeft " //$NON-NLS-1$
                + "FROM pm1.g1"; //$NON-NLS-1$
        // source query for other side of a JOIN
        String rightQuery = "SELECT pm2.g2.e1 as ID, pm2.g2.e2, pm2.g2.e3, pm2.g2.e4 " //$NON-NLS-1$
                + "FROM pm2.g2"; //$NON-NLS-1$

        // User Command
        /*
         * Return everything from the JOIN. TopRandom is the use of RAND() on
         * the user command while RandomLeft is the use of RAND() within a
         * source node.
         */
        String sql = "SELECT l.ID, l.e2, l.e3, l.e4, r.ID, r.e2, r.e3, r.e4, l.RandomLeft, RAND() AS RandomTop " + //$NON-NLS-1$
                "FROM (" + leftQuery + ") AS l, " + //$NON-NLS-1$ //$NON-NLS-2$
                "(" + rightQuery + ") AS r " + //$NON-NLS-1$ //$NON-NLS-2$
                "WHERE l.ID = r.ID"; //$NON-NLS-1$

        // The user command should result in two atomic commands
        String[] expected = new String[] {
                "SELECT g_0.e1, g_0.e2, g_0.e3, g_0.e4 FROM pm1.g1 AS g_0",  //$NON-NLS-1$
                "SELECT g_0.e1 AS c_0, g_0.e2 AS c_1, g_0.e3 AS c_2, g_0.e4 AS c_3 FROM pm2.g2 AS g_0 ORDER BY c_0",  //$NON-NLS-1$
        };

        // create a plan and assert our atomic queries
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, TestOptimizer.example1(), expected,
                ComparisonMode.EXACT_COMMAND_STRING);
        TestOptimizer.checkNodeTypes(plan, new int[] { 2, // Access
                0, // DependentAccess
                0, // DependentSelect
                0, // DependentProject
                0, // DupRemove
                0, // Grouping
                0, // NestedLoopJoinStrategy
                1, // MergeJoinStrategy
                0, // Null
                0, // PlanExecution
                2, // Project
                0, // Select
                0, // Sort
                0 // UnionAll
        });
    }

    /**
     * <p>Test the use of a deterministic function on the user command which
     * performs a JOIN of two sources.
     *
     * <p>The function should be executed prior to the JOIN being executed and should
     * result in the projected symbol becoming a constant.
     * @throws TeiidComponentException
     * @throws QueryValidatorException
     * @throws QueryResolverException
     * @throws QueryParserException
     */
    public void testDeterministicPostJoin()  throws Exception {
        // source query for one side of a JOIN
        String leftQuery = "SELECT pm1.g1.e1 as ID, pm1.g1.e2, pm1.g1.e3, pm1.g1.e4 " //$NON-NLS-1$
                + "FROM pm1.g1"; //$NON-NLS-1$
        // source query for other side of a JOIN
        String rightQuery = "SELECT pm2.g2.e1 as ID, pm2.g2.e2, pm2.g2.e3, pm2.g2.e4 " //$NON-NLS-1$
                + "FROM pm2.g2"; //$NON-NLS-1$

        // User Command
        /*
         * Return everything from the JOIN. SqrtTop is the use of SQRT(100) on
         * the user command and should result in 10 for each row in the JOINed
         * output.
         */
        String sql = "SELECT l.ID, l.e2, l.e3, l.e4, r.ID, r.e2, r.e3, r.e4, SQRT(100) AS SqrtTop " + //$NON-NLS-1$
                "FROM (" + leftQuery + ") AS l, " + //$NON-NLS-1$ //$NON-NLS-2$
                "(" + rightQuery + ") AS r " + //$NON-NLS-1$ //$NON-NLS-2$
                "WHERE l.ID = r.ID"; //$NON-NLS-1$

        // The user command should result in two atomic commands
        String[] expected = new String[] {
                "SELECT g_0.e1 AS c_0, g_0.e2 AS c_1, g_0.e3 AS c_2, g_0.e4 AS c_3 FROM pm1.g1 AS g_0 ORDER BY c_0",  //$NON-NLS-1$
                "SELECT g_0.e1 AS c_0, g_0.e2 AS c_1, g_0.e3 AS c_2, g_0.e4 AS c_3 FROM pm2.g2 AS g_0 ORDER BY c_0",  //$NON-NLS-1$
        };

        // create a plan and assert our atomic queries
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, TestOptimizer.example1(), expected,
                ComparisonMode.EXACT_COMMAND_STRING);
        TestOptimizer.checkNodeTypes(plan, new int[] { 2, // Access
                0, // DependentAccess
                0, // DependentSelect
                0, // DependentProject
                0, // DupRemove
                0, // Grouping
                0, // NestedLoopJoinStrategy
                1, // MergeJoinStrategy
                0, // Null
                0, // PlanExecution
                1, // Project
                0, // Select
                0, // Sort
                0 // UnionAll
        });

        /*
         * Retrieve root nodes elements to assert that a constant has
         * has replaced the SQRT() function.
         */
        List<?> elem = ((RelationalPlan) plan).getRootNode().getElements();
        Constant expectedConst = new Constant(new Double(10.0));
        assertEquals("Did not get expected constant value for SqrtTop in root node of plan: ",  //$NON-NLS-1$
                expectedConst,
                SymbolMap.getExpression((Expression)elem.get(8))  // should be a AliasSymbol containing an expression
            );
    }

    /**
     * <p>The function should be executed prior to the JOIN being executed and should
     * result in the projected symbol becoming a constant.

     * <p>Test the use of a deterministic function on the source command of a JOIN
     * defined by a user command which performs a JOIN of two sources.
     *
     * <p>The function should be executed prior to the commands from either side of the
     * JOIN being executed and merged into user command prior to the JOIN actually being
     * executed.
     * @throws TeiidComponentException
     * @throws QueryValidatorException
     * @throws QueryResolverException
     * @throws QueryParserException
     */
    public void testDeterministicPreJoin() throws Exception {
        // source query for one side of a JOIN
        String leftQuery = "SELECT pm1.g1.e1 as ID, pm1.g1.e2, pm1.g1.e3, pm1.g1.e4, SQRT(100) AS SqrtLeft " //$NON-NLS-1$
                + "FROM pm1.g1"; //$NON-NLS-1$
        // source query for other side of a JOIN
        String rightQuery = "SELECT pm2.g2.e1 as ID, pm2.g2.e2, pm2.g2.e3, pm2.g2.e4 " //$NON-NLS-1$
                + "FROM pm2.g2"; //$NON-NLS-1$

        // User Command
        /*
         * Return everything from the JOIN. SqrtLeft is the use of SQRT()
         * within a source node.
         */
        String sql = "SELECT l.ID, l.e2, l.e3, l.e4, r.ID, r.e2, r.e3, r.e4, l.SqrtLeft " + //$NON-NLS-1$
                "FROM (" + leftQuery + ") AS l, " + //$NON-NLS-1$ //$NON-NLS-2$
                "(" + rightQuery + ") AS r " + //$NON-NLS-1$ //$NON-NLS-2$
                "WHERE l.ID = r.ID"; //$NON-NLS-1$

        // The user command should result in two atomic commands
        String[] expected = new String[] {
                "SELECT g_0.e1 AS c_0, g_0.e2 AS c_1, g_0.e3 AS c_2, g_0.e4 AS c_3 FROM pm1.g1 AS g_0 ORDER BY c_0",  //$NON-NLS-1$
                "SELECT g_0.e1 AS c_0, g_0.e2 AS c_1, g_0.e3 AS c_2, g_0.e4 AS c_3 FROM pm2.g2 AS g_0 ORDER BY c_0",  //$NON-NLS-1$
        };

        // create a plan and assert our atomic queries
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, TestOptimizer.example1(), expected,
                ComparisonMode.EXACT_COMMAND_STRING);
        TestOptimizer.checkNodeTypes(plan, new int[] { 2, // Access
                0, // DependentAccess
                0, // DependentSelect
                0, // DependentProject
                0, // DupRemove
                0, // Grouping
                0, // NestedLoopJoinStrategy
                1, // MergeJoinStrategy
                0, // Null
                0, // PlanExecution
                1, // Project
                0, // Select
                0, // Sort
                0 // UnionAll
                });

        /*
         * Retrieve root nodes elements to assert that a constant has
         * has replaced the SQRT() function.
         */
        List<?> elem = ((RelationalPlan) plan).getRootNode().getElements();
        Constant expectedConst = new Constant(new Double(10.0));
        assertEquals("Did not get expected constant value for SqrtLeft in root node of plan: ",  //$NON-NLS-1$
                expectedConst,
                ((AliasSymbol)elem.get(8)).getSymbol()  // should be a AliasSymbol containing an expression
            );
    }

    /**
     * <p>Test the use of a deterministic function on the sub-command of a user
     * command and the user command itself, which performs a JOIN of two sources.
     *
     * <p>This test combines the PostJoin and PreJoin test cases.
     * @throws TeiidComponentException
     * @throws QueryValidatorException
     * @throws QueryResolverException
     * @throws QueryParserException
     * @see #testDeterministicPostJoin
     * @see #testDeterministicPreJoin
     */
    public void testDeterministicPrePostJoin() throws Exception {
        // sub-query for one side of a JOIN
        String leftQuery = "SELECT pm1.g1.e1 as ID, pm1.g1.e2, pm1.g1.e3, pm1.g1.e4, SQRT(100) AS SqrtLeft " //$NON-NLS-1$
                + "FROM pm1.g1"; //$NON-NLS-1$
        // sub-query for other side of a JOIN
        String rightQuery = "SELECT pm2.g2.e1 as ID, pm2.g2.e2, pm2.g2.e3, pm2.g2.e4 " //$NON-NLS-1$
                + "FROM pm2.g2"; //$NON-NLS-1$

        // User Command
        /*
         * Return everything from the JOIN. SqrtTop is the use of SQRT(100) on
         * the user command while SqrtLeft is the use of SQRT() within a
         * source node.
         */
        String sql = "SELECT l.ID, l.e2, l.e3, l.e4, r.ID, r.e2, r.e3, r.e4, l.SqrtLeft, SQRT(100) AS SqrtTop " + //$NON-NLS-1$
                "FROM (" + leftQuery + ") AS l, " + //$NON-NLS-1$ //$NON-NLS-2$
                "(" + rightQuery + ") AS r " + //$NON-NLS-1$ //$NON-NLS-2$
                "WHERE l.ID = r.ID"; //$NON-NLS-1$

        // The user command should result in two atomic commands
        String[] expected = new String[] {
                "SELECT g_0.e1 AS c_0, g_0.e2 AS c_1, g_0.e3 AS c_2, g_0.e4 AS c_3 FROM pm1.g1 AS g_0 ORDER BY c_0",  //$NON-NLS-1$
                "SELECT g_0.e1 AS c_0, g_0.e2 AS c_1, g_0.e3 AS c_2, g_0.e4 AS c_3 FROM pm2.g2 AS g_0 ORDER BY c_0",  //$NON-NLS-1$
        };

        // create a plan and assert our atomic queries
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, TestOptimizer.example1(), expected,
                ComparisonMode.EXACT_COMMAND_STRING);
        TestOptimizer.checkNodeTypes(plan, new int[] { 2, // Access
                0, // DependentAccess
                0, // DependentSelect
                0, // DependentProject
                0, // DupRemove
                0, // Grouping
                0, // NestedLoopJoinStrategy
                1, // MergeJoinStrategy
                0, // Null
                0, // PlanExecution
                1, // Project
                0, // Select
                0, // Sort
                0 // UnionAll
                });

        /*
         * Retrieve root nodes elements to assert that a constant has
         * replaced the SQRT() function for both SqrtTop and SqrtLeft.
         */
        List<?> elem = ((RelationalPlan) plan).getRootNode().getElements();
        Constant expectedConst = new Constant(new Double(10.0));
        assertEquals("Did not get expected constant value for SqrtLeft in root node of plan: ",  //$NON-NLS-1$
                expectedConst,
                SymbolMap.getExpression((Expression)elem.get(8))  // should be a AliasSymbol containing an expression
            );
        assertEquals("Did not get expected constant value for SqrtTop in root node of plan: ",  //$NON-NLS-1$
                expectedConst,
                SymbolMap.getExpression((Expression)elem.get(9))  // should be a AliasSymbol containing an expression
            );
    }
}
