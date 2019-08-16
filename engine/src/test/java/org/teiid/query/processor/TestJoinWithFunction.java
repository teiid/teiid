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

import java.util.Arrays;
import java.util.List;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.FakeCapabilitiesFinder;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.unittest.RealMetadataFactory;

import junit.framework.TestCase;


/**
 * <p><code>TestCase</code> to cover processing of JOINs which use a scalar
 * function as a symbol or as part of the JOIN criteria.
 *
 * <p>All tests should verify and validate that the scalar function's result
 * is being used appropriately from the pre-JOIN and post-JOIN aspect.  Most
 * specifically, the results returned from the JOIN should match the expected
 * results defined in each test method.
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
     * @throws QueryMetadataException
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

        /*
         * Populate a List with our expected results. We can predict the return value
         * for RAND() because the TestProcessor.helpProcess() method seeds the random
         * number generator.
         */
        List<?>[] expected = new List[] {
                Arrays.asList(new Object[] { "a", new Integer(0), //$NON-NLS-1$
                        new Boolean(false), new Double(2), "a", new Integer(1), //$NON-NLS-1$
                        new Boolean(true), new Double(2.0), new Double(0.24053641567148587) }),
                Arrays.asList(new Object[] { "b", new Integer(1), //$NON-NLS-1$
                        new Boolean(true), null, "b", new Integer(0), //$NON-NLS-1$
                        new Boolean(false), new Double(0.0), new Double(0.6374174253501083) }),
                Arrays.asList(new Object[] { "b", new Integer(1), //$NON-NLS-1$
                        new Boolean(true), null, "b", new Integer(5), //$NON-NLS-1$
                        new Boolean(true), new Double(2.0), new Double(0.5504370051176339) }),
                Arrays.asList(new Object[] { "b", new Integer(1), //$NON-NLS-1$
                        new Boolean(true), null, "b", new Integer(2), //$NON-NLS-1$
                        new Boolean(false), null, new Double(0.5975452777972018) }), };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        FakeDataStore.sampleData2(dataManager);

        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$
        Command command = TestProcessor.helpParse(sql);
        ProcessorPlan plan = TestProcessor.helpGetPlan(command, RealMetadataFactory.example1Cached(), capFinder);

        // Run query
        TestProcessor.helpProcess(plan, dataManager, expected);
    }

    /**
     * <p>Test the use of a non-deterministic function on a source command of a JOIN
     * defined by a user command that performs a JOIN of two sources.
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
     * @throws QueryMetadataException
     */
    public void testNonDeterministicPreJoin() throws Exception {
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
        String sql = "SELECT l.ID, l.e2, l.e3, l.e4, r.ID, r.e2, r.e3, r.e4, l.RandomLeft " + //$NON-NLS-1$
                "FROM (" + leftQuery + ") AS l, " + //$NON-NLS-1$ //$NON-NLS-2$
                "(" + rightQuery + ") AS r " + //$NON-NLS-1$ //$NON-NLS-2$
                "WHERE l.ID = r.ID"; //$NON-NLS-1$

        /*
         * Populate a List with our expected results. We can predict the return value
         * for RAND() because the TestProcessor.helpProcess() method seeds the random
         * number generator.
         */
        List<?>[] expected = new List[] {
                Arrays.asList(new Object[] { "a", new Integer(0), //$NON-NLS-1$
                        new Boolean(false), new Double(2), "a", new Integer(1), //$NON-NLS-1$
                        new Boolean(true), new Double(2.0), new Double(0.24053641567148587) }),
                Arrays.asList(new Object[] { "b", new Integer(1), //$NON-NLS-1$
                        new Boolean(true), null, "b", new Integer(0), //$NON-NLS-1$
                        new Boolean(false), new Double(0.0), new Double(0.6374174253501083) }),
                Arrays.asList(new Object[] { "b", new Integer(1), //$NON-NLS-1$
                        new Boolean(true), null, "b", new Integer(5), //$NON-NLS-1$
                        new Boolean(true), new Double(2.0), new Double(0.6374174253501083) }),
                Arrays.asList(new Object[] { "b", new Integer(1), //$NON-NLS-1$
                        new Boolean(true), null, "b", new Integer(2), //$NON-NLS-1$
                        new Boolean(false), null, new Double(0.6374174253501083) }),
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        FakeDataStore.sampleData2(dataManager);

        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$
        Command command = TestProcessor.helpParse(sql);
        ProcessorPlan plan = TestProcessor.helpGetPlan(command, RealMetadataFactory.example1Cached(), capFinder);

        // Run query
        TestProcessor.helpProcess(plan, dataManager, expected);
    }

    /**
     * <p>Test the use of a non-deterministic function on the sub-command of a user
     * command and the user command itself, which performs a JOIN of two sources.
     *
     * <p>This test combines the PostJoin and PreJoin test cases.
     * @see #testNonDeterministicPostJoin
     * @see #testNonDeterministicPreJoin
     * @throws TeiidComponentException
     * @throws QueryMetadataException
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

        /*
         * Populate a List with our expected results. We can predict the return value
         * for RAND() because the TestProcessor.helpProcess() method seeds the random
         * number generator.
         */
        List<?>[] expected = new List[] {
            Arrays.asList(new Object[] { "a", new Integer(0), //$NON-NLS-1$
                    new Boolean(false), new Double(2), "a", new Integer(1), //$NON-NLS-1$
                    new Boolean(true), new Double(2.0), new Double(0.24053641567148587), new Double(0.5975452777972018) }),
            Arrays.asList(new Object[] { "b", new Integer(1), //$NON-NLS-1$
                    new Boolean(true), null, "b", new Integer(0), //$NON-NLS-1$
                    new Boolean(false), new Double(0.0), new Double(0.6374174253501083), new Double(0.3332183994766498) }),
            Arrays.asList(new Object[] { "b", new Integer(1), //$NON-NLS-1$
                    new Boolean(true), null, "b", new Integer(5), //$NON-NLS-1$
                    new Boolean(true), new Double(2.0), new Double(0.6374174253501083), new Double(0.3851891847407185) }),
            Arrays.asList(new Object[] { "b", new Integer(1), //$NON-NLS-1$
                    new Boolean(true), null, "b", new Integer(2), //$NON-NLS-1$
                    new Boolean(false), null, new Double(0.6374174253501083), new Double(0.984841540199809) })
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        FakeDataStore.sampleData2(dataManager);

        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$
        Command command = TestProcessor.helpParse(sql);
        ProcessorPlan plan = TestProcessor.helpGetPlan(command, RealMetadataFactory.example1Cached(), capFinder);

        // Run query
        TestProcessor.helpProcess(plan, dataManager, expected);
    }

    /**
     * <p>Test the use of a deterministic function on the user command which
     * performs a JOIN of two sources.
     *
     * <p>The function should be executed prior to the JOIN being executed and should
     * result in the projected symbol becoming a constant.
     * @throws TeiidComponentException
     * @throws QueryMetadataException
     */
    public void testDeterministicPostJoin() throws Exception {
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

        /*
         * Populate a List with our expected results.
         */
        List<?>[] expected = new List[] {
                Arrays.asList(new Object[] { "a", new Integer(0), //$NON-NLS-1$
                        new Boolean(false), new Double(2), "a", new Integer(1), //$NON-NLS-1$
                        new Boolean(true), new Double(2.0), new Double(10) }),
                Arrays.asList(new Object[] { "b", new Integer(1), //$NON-NLS-1$
                        new Boolean(true), null, "b", new Integer(0), //$NON-NLS-1$
                        new Boolean(false), new Double(0.0), new Double(10) }),
                Arrays.asList(new Object[] { "b", new Integer(1), //$NON-NLS-1$
                        new Boolean(true), null, "b", new Integer(5), //$NON-NLS-1$
                        new Boolean(true), new Double(2.0), new Double(10) }),
                Arrays.asList(new Object[] { "b", new Integer(1), //$NON-NLS-1$
                        new Boolean(true), null, "b", new Integer(2), //$NON-NLS-1$
                        new Boolean(false), null, new Double(10) }),
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        FakeDataStore.sampleData2(dataManager);

        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$
        Command command = TestProcessor.helpParse(sql);
        ProcessorPlan plan = TestProcessor.helpGetPlan(command, RealMetadataFactory.example1Cached(), capFinder);

        // Run query
        TestProcessor.helpProcess(plan, dataManager, expected);
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
     * @throws QueryMetadataException
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

        /*
         * Populate a List with our expected results.
         */
        List<?>[] expected = new List[] {
                Arrays.asList(new Object[] { "a", new Integer(0), //$NON-NLS-1$
                        new Boolean(false), new Double(2), "a", new Integer(1), //$NON-NLS-1$
                        new Boolean(true), new Double(2.0), new Double(10) }),
                Arrays.asList(new Object[] { "b", new Integer(1), //$NON-NLS-1$
                        new Boolean(true), null, "b", new Integer(0), //$NON-NLS-1$
                        new Boolean(false), new Double(0.0), new Double(10) }),
                Arrays.asList(new Object[] { "b", new Integer(1), //$NON-NLS-1$
                        new Boolean(true), null, "b", new Integer(5), //$NON-NLS-1$
                        new Boolean(true), new Double(2.0), new Double(10) }),
                Arrays.asList(new Object[] { "b", new Integer(1), //$NON-NLS-1$
                        new Boolean(true), null, "b", new Integer(2), //$NON-NLS-1$
                        new Boolean(false), null, new Double(10) }),
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        FakeDataStore.sampleData2(dataManager);

        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$
        Command command = TestProcessor.helpParse(sql);
        ProcessorPlan plan = TestProcessor.helpGetPlan(command, RealMetadataFactory.example1Cached(), capFinder);

        // Run query
        TestProcessor.helpProcess(plan, dataManager, expected);
    }

    /**
     * <p>Test the use of a deterministic function on the sub-command of a user
     * command and the user command itself, which performs a JOIN of two sources.
     *
     * <p>This test combines the PostJoin and PreJoin test cases.
     * @throws TeiidComponentException
     * @throws QueryMetadataException
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

        /*
         * Populate a List with our expected results.
         */
        List<?>[] expected = new List[] {
                Arrays.asList(new Object[] { "a", new Integer(0), //$NON-NLS-1$
                        new Boolean(false), new Double(2), "a", new Integer(1), //$NON-NLS-1$
                        new Boolean(true), new Double(2.0), new Double(10.0), new Double(10.0) }),
                Arrays.asList(new Object[] { "b", new Integer(1), //$NON-NLS-1$
                        new Boolean(true), null, "b", new Integer(0), //$NON-NLS-1$
                        new Boolean(false), new Double(0.0), new Double(10.0), new Double(10.0) }),
                Arrays.asList(new Object[] { "b", new Integer(1), //$NON-NLS-1$
                        new Boolean(true), null, "b", new Integer(5), //$NON-NLS-1$
                        new Boolean(true), new Double(2.0), new Double(10.0), new Double(10.0) }),
                Arrays.asList(new Object[] { "b", new Integer(1), //$NON-NLS-1$
                        new Boolean(true), null, "b", new Integer(2), //$NON-NLS-1$
                        new Boolean(false), null, new Double(10.0), new Double(10.0) }),
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        FakeDataStore.sampleData2(dataManager);

        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$
        Command command = TestProcessor.helpParse(sql);
        ProcessorPlan plan = TestProcessor.helpGetPlan(command, RealMetadataFactory.example1Cached(), capFinder);

        // Run query
        TestProcessor.helpProcess(plan, dataManager, expected);
    }
}
