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

package org.teiid.dqp.internal.process;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.teiid.cache.DefaultCacheFactory;
import org.teiid.core.types.BinaryType;
import org.teiid.language.Parameter;
import org.teiid.language.visitor.CollectorVisitor;
import org.teiid.metadata.Table;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.FakeCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.processor.FakeDataManager;
import org.teiid.query.processor.HardcodedDataManager;
import org.teiid.query.processor.TestProcessor;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.lang.Update;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.validator.TestUpdateValidator;
import org.teiid.translator.SourceSystemFunctions;


/**
 * JUnit TestCase to test planning and caching of <code>PreparedStatement</code>
 * plans that contain batched updates.
 *
 */
@SuppressWarnings("nls")
public class TestPreparedStatementBatchedUpdate {

    @Test public void testBatchedUpdatePushdown() throws Exception {
        // Create query
        String preparedSql = "UPDATE pm1.g1 SET pm1.g1.e1=?, pm1.g1.e3=? WHERE pm1.g1.e2=?"; //$NON-NLS-1$

        // Create a testable prepared plan cache
        SessionAwareCache<PreparedPlan> prepPlanCache = new SessionAwareCache<PreparedPlan>("preparedplan", DefaultCacheFactory.INSTANCE, SessionAwareCache.Type.PREPAREDPLAN, 0);

        // Construct data manager with data
        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("UPDATE pm1.g1 SET e1 = ?, e3 = ? WHERE pm1.g1.e2 = ?", new List[] {Arrays.asList(4)}); //$NON-NLS-1$
        // Source capabilities must support batched updates
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.BULK_UPDATE, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        // batch with two commands
        ArrayList<ArrayList<Object>> values = new ArrayList<ArrayList<Object>>(2);
        values.add(new ArrayList<Object>(Arrays.asList(new Object[] { "a",  Boolean.FALSE, new Integer(0) })));  //$NON-NLS-1$
        values.add(new ArrayList<Object>(Arrays.asList(new Object[] { null, Boolean.FALSE, new Integer(1) })));

        List<?>[] expected = new List[] {
                Arrays.asList(4)
        };

        // Create the plan and process the query
        TestPreparedStatement.helpTestProcessing(preparedSql, values, expected, dataManager, capFinder, RealMetadataFactory.example1Cached(), prepPlanCache, false, false, false,RealMetadataFactory.example1VDB());
        Update update = (Update)dataManager.getCommandHistory().iterator().next();
        assertTrue(((Constant)update.getChangeList().getClauses().get(0).getValue()).isMultiValued());
    }

    @Test public void testBatchedUpdatePushdown1() throws Exception {
        //TODO: just use straight ddl
        TransformationMetadata metadata = TestUpdateValidator.example1();
        TestUpdateValidator.createView("select 1 as x, 2 as y", metadata, "GX");
        Table t = metadata.getMetadataStore().getSchemas().get("VM1").getTables().get("GX");
        t.setDeletePlan("");
        t.setUpdatePlan("");
        t.setInsertPlan("FOR EACH ROW BEGIN insert into pm1.g1 (e1) values (new.x); END");

        String preparedSql = "insert into gx (x, y) values (?,?)"; //$NON-NLS-1$

        // Create a testable prepared plan cache
        SessionAwareCache<PreparedPlan> prepPlanCache = new SessionAwareCache<PreparedPlan>("preparedplan", DefaultCacheFactory.INSTANCE, SessionAwareCache.Type.PREPAREDPLAN, 0);

        // Construct data manager with data
        HardcodedDataManager dataManager = new HardcodedDataManager(metadata);
        dataManager.addData("INSERT INTO g1 (e1) VALUES (convert(?, string))", new List[] {Arrays.asList(2)}); //$NON-NLS-1$
        // Source capabilities must support batched updates
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setFunctionSupport(SourceSystemFunctions.CONVERT, true);
        caps.setCapabilitySupport(Capability.BULK_UPDATE, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        // batch with two commands
        ArrayList<ArrayList<Object>> values = new ArrayList<ArrayList<Object>>(2);
        values.add(new ArrayList<Object>(Arrays.asList(3, 4)));  //$NON-NLS-1$
        values.add(new ArrayList<Object>(Arrays.asList(5, 6)));

        List<?>[] expected = new List[] {
                Arrays.asList(2)
        };

        // Create the plan and process the query
        TestPreparedStatement.helpTestProcessing(preparedSql, values, expected, dataManager, capFinder, metadata, prepPlanCache, false, false, false,RealMetadataFactory.example1VDB());
        org.teiid.language.Insert insert = (org.teiid.language.Insert)dataManager.getPushdownCommands().iterator().next();
        Parameter p = CollectorVisitor.collectObjects(Parameter.class, insert).iterator().next();
        assertEquals(0, p.getValueIndex());
        assertEquals(Arrays.asList(3), insert.getParameterValues().next());
        assertTrue(insert.getParameterValues().hasNext());
    }

    /**
     * Test batch handling when a function cannot be pushed
     */
    @Test public void testBatchedUpdatePushdown2() throws Exception {
        //TODO: just use straight ddl
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();

        String preparedSql = "insert into pm1.g1 (e1) values (? + 1)"; //$NON-NLS-1$

        // Create a testable prepared plan cache
        SessionAwareCache<PreparedPlan> prepPlanCache = new SessionAwareCache<PreparedPlan>("preparedplan", DefaultCacheFactory.INSTANCE, SessionAwareCache.Type.PREPAREDPLAN, 0);

        // Construct data manager with data
        HardcodedDataManager dataManager = new HardcodedDataManager(metadata);
        dataManager.addData("INSERT INTO g1 (e1) VALUES ('4')", Arrays.asList(1));
        dataManager.addData("INSERT INTO g1 (e1) VALUES ('6')", Arrays.asList(1));
        // Source capabilities must support batched updates
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setFunctionSupport(SourceSystemFunctions.CONVERT, false);
        caps.setCapabilitySupport(Capability.BULK_UPDATE, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        // batch with two commands
        ArrayList<ArrayList<Object>> values = new ArrayList<ArrayList<Object>>(2);
        values.add(new ArrayList<Object>(Arrays.asList(3)));  //$NON-NLS-1$
        values.add(new ArrayList<Object>(Arrays.asList(5)));

        List<?>[] expected = new List[] {
                Arrays.asList(1), Arrays.asList(1)
        };

        // Create the plan and process the query
        TestPreparedStatement.helpTestProcessing(preparedSql, values, expected, dataManager, capFinder, metadata, prepPlanCache, false, false, false,RealMetadataFactory.example1VDB());
    }

    @Test public void testBatchedUpdateNotPushdown() throws Exception {
        // Create query
        String preparedSql = "UPDATE pm1.g1 SET pm1.g1.e1=?, pm1.g1.e3=? WHERE pm1.g1.e2=?"; //$NON-NLS-1$

        // Create a testable prepared plan cache
        SessionAwareCache<PreparedPlan> prepPlanCache = new SessionAwareCache<PreparedPlan>("preparedplan", DefaultCacheFactory.INSTANCE, SessionAwareCache.Type.PREPAREDPLAN, 0);

        // Construct data manager with data
        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("UPDATE pm1.g1 SET e1 = 'a', e3 = FALSE WHERE pm1.g1.e2 = 0", new List[] {Arrays.asList(2)}); //$NON-NLS-1$
        dataManager.addData("UPDATE pm1.g1 SET e1 = null, e3 = FALSE WHERE pm1.g1.e2 = 1", new List[] {Arrays.asList(2)}); //$NON-NLS-1$
        // Source capabilities must support batched updates
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.BULK_UPDATE, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        // batch with two commands
        ArrayList<ArrayList<Object>> values = new ArrayList<ArrayList<Object>>(2);
        values.add(new ArrayList<Object>(Arrays.asList(new Object[] { "a",  Boolean.FALSE, new Integer(0) })));  //$NON-NLS-1$
        values.add(new ArrayList<Object>(Arrays.asList(new Object[] { null, Boolean.FALSE, new Integer(1) })));

        List<?>[] expected = new List[] {
                Arrays.asList(2),
                Arrays.asList(2)
        };

        // Create the plan and process the query
        TestPreparedStatement.helpTestProcessing(preparedSql, values, expected, dataManager, capFinder, RealMetadataFactory.example1Cached(), prepPlanCache, false, false, false,RealMetadataFactory.example1VDB());
    }

    @Test public void testBatchedMerge() throws Exception {
        String ddl = "CREATE foreign table x (y string primary key, z integer) options (updatable true)";

        QueryMetadataInterface metadata = RealMetadataFactory.fromDDL(ddl, "x", "phy");

        // Create query
        String preparedSql = "merge into x (y, z) values (?, ?)"; //$NON-NLS-1$

        // Create a testable prepared plan cache
        SessionAwareCache<PreparedPlan> prepPlanCache = new SessionAwareCache<PreparedPlan>("preparedplan", DefaultCacheFactory.INSTANCE, SessionAwareCache.Type.PREPAREDPLAN, 0);

        // Construct data manager with data
        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT 1 FROM phy.x AS g_0 WHERE g_0.y = 'a'", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        dataManager.addData("UPDATE x SET z = 0 WHERE y = 'a'", new List[] {Arrays.asList(1)}); //$NON-NLS-1$
        dataManager.addData("INSERT INTO x (y, z) VALUES (null, 1)", new List[] {Arrays.asList(1)}); //$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        capFinder.addCapabilities("phy", caps); //$NON-NLS-1$

        // batch with two commands
        ArrayList<ArrayList<Object>> values = new ArrayList<ArrayList<Object>>(2);
        values.add(new ArrayList<Object>(Arrays.asList(new Object[] { "a",  new Integer(0) })));  //$NON-NLS-1$
        values.add(new ArrayList<Object>(Arrays.asList(new Object[] { null, new Integer(1) })));

        List<?>[] expected = new List[] {
                Arrays.asList(1),
                Arrays.asList(1)
        };

        //no upsert nor bulk/batch support, will use a compensating procedure

        // Create the plan and process the query
        TestPreparedStatement.helpTestProcessing(preparedSql, values, expected, dataManager, capFinder, metadata, prepPlanCache, false, false, false,RealMetadataFactory.example1VDB());

        //without upsert support it should function the same regardless of batching/bulk support
        prepPlanCache.clearAll();
        caps.setCapabilitySupport(Capability.BATCHED_UPDATES, true);
        TestPreparedStatement.helpTestProcessing(preparedSql, values, expected, dataManager, capFinder, metadata, prepPlanCache, false, false, false,RealMetadataFactory.example1VDB());

        prepPlanCache.clearAll();
        caps.setCapabilitySupport(Capability.BATCHED_UPDATES, false);
        caps.setCapabilitySupport(Capability.BULK_UPDATE, true);
        TestPreparedStatement.helpTestProcessing(preparedSql, values, expected, dataManager, capFinder, metadata, prepPlanCache, false, false, false,RealMetadataFactory.example1VDB());

        //with upsert full pushdown is expected
        prepPlanCache.clearAll();
        caps.setCapabilitySupport(Capability.UPSERT, true);
        caps.setCapabilitySupport(Capability.BULK_UPDATE, true);
        dataManager.addData("UPSERT INTO x (y, z) VALUES (?, ?)", Arrays.asList(1), Arrays.asList(1));
        TestPreparedStatement.helpTestProcessing(preparedSql, values, expected, dataManager, capFinder, metadata, prepPlanCache, false, false, false,RealMetadataFactory.example1VDB());
    }

    /**
     * Test prepared statements that use batched updates using the same prepared
     * command with same number of commands in the batch.
     * <p>
     * The test verifies that no errors occur when planning and executing the
     * same batched command SQL with the same number of batched command parameter
     * value sets.  For example, if the first executeBatch() call were to occur
     * with two batched commands a repeated call with two batched commands
     * should not result in an error during planning or execution and the value
     * used in the second batched command should be used instead of any values
     * from the first batched command.
     * <p>
     * The test also verifies that the correct SQL is pushed to the data manager
     * to verify that the parameter substitution occurred and is correct and the
     * correct number of statements made it to the data manager for the respective
     * batch command.
     * <p>
     * The batched command "UPDATE pm1.g1 SET pm1.g1.e1=?, pm1.g1.e3=? WHERE pm1.g1.e2=?"
     * will appear as:
     * <p>
     * UPDATE pm1.g1 SET pm1.g1.e1='a', pm1.g1.e3=false WHERE pm1.g1.e2=0
     * UPDATE pm1.g1 SET pm1.g1.e1=null, pm1.g1.e3=false WHERE pm1.g1.e2=1
     * <p>
     * UPDATE pm1.g1 SET pm1.g1.e1='a', pm1.g1.e3=false WHERE pm1.g1.e2=0
     * UPDATE pm1.g1 SET pm1.g1.e1='b', pm1.g1.e3=true WHERE pm1.g1.e2=5
     * <p>
     * The result should be that one command is in the plan cache and
     * no plan creation, validation, or execution errors will occur and
     * a predetermined set of queries were executed in the data manager.
     *
     * @throws Exception
     */
    @Test public void testUpdateSameNumCmds() throws Exception {
        // Create query
        String preparedSql = "UPDATE pm1.g1 SET pm1.g1.e1=?, pm1.g1.e3=? WHERE pm1.g1.e2=?"; //$NON-NLS-1$
        // Create a testable prepared plan cache
        SessionAwareCache<PreparedPlan> prepPlanCache = new SessionAwareCache<PreparedPlan>("preparedplan", DefaultCacheFactory.INSTANCE, SessionAwareCache.Type.PREPAREDPLAN, 0);

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);

        // Source capabilities must support batched updates
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.BATCHED_UPDATES, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        // Something to hold our final query list
        List<String> finalQueryList = new ArrayList<String>(13);

        // Create expected results
        // first command should result in 2 rows affected
        // second command should result in 2 rows affected
        List<?>[] expected = new List[] {
            Arrays.asList(new Object[] { new Integer(2) }),
            Arrays.asList(new Object[] { new Integer(2) })
        };

        // batch with two commands
        List<List<Object>> values = new ArrayList<List<Object>>(2);
        values.add(new ArrayList<Object>(Arrays.asList(new Object[] { "a",  Boolean.FALSE, new Integer(0) })));  //$NON-NLS-1$
        values.add(new ArrayList<Object>(Arrays.asList(new Object[] { null, Boolean.FALSE, new Integer(1) })));

        // Add our expected queries to the final query list
           finalQueryList.add(new String("UPDATE pm1.g1 SET e1 = 'a', e3 = FALSE WHERE pm1.g1.e2 = 0")); //$NON-NLS-1$
           finalQueryList.add(new String("UPDATE pm1.g1 SET e1 = null, e3 = FALSE WHERE pm1.g1.e2 = 1")); //$NON-NLS-1$

        // Create the plan and process the query
        TestPreparedStatement.helpTestProcessing(preparedSql, values, expected, dataManager, capFinder, RealMetadataFactory.example1Cached(), prepPlanCache, false, false, false,RealMetadataFactory.example1VDB());

        // Repeat with different number of commands in batch
        // Create expected results
        // first command should result in 2 rows affected
        expected = new List[] {
                Arrays.asList(new Object[] { new Integer(2) }),
                Arrays.asList(new Object[] { new Integer(0) })
        };

        // batch with two commands
        values = new ArrayList<List<Object>>(1);
        values.add(new ArrayList<Object>(Arrays.asList(new Object[] { "a",  Boolean.FALSE, new Integer(0) })));  //$NON-NLS-1$
        values.add(new ArrayList<Object>(Arrays.asList(new Object[] { "b",  Boolean.TRUE, new Integer(5) })));  //$NON-NLS-1$

        // Add our expected queries to the final query list
           finalQueryList.add(new String("UPDATE pm1.g1 SET e1 = 'a', e3 = FALSE WHERE pm1.g1.e2 = 0")); //$NON-NLS-1$
           finalQueryList.add(new String("UPDATE pm1.g1 SET e1 = 'b', e3 = TRUE WHERE pm1.g1.e2 = 5")); //$NON-NLS-1$

        // Use the cached plan and process the query
        TestPreparedStatement.helpTestProcessing(preparedSql, values, expected, dataManager, capFinder, RealMetadataFactory.example1Cached(), prepPlanCache, false, false, true,RealMetadataFactory.example1VDB());

        // Verify all the queries that were run
        assertEquals("Unexpected queries executed -", finalQueryList, dataManager.getQueries()); //$NON-NLS-1$
    }

    /**
     * Test prepared statements that use batched updates using the same prepared
     * command with same number of commands in the batch.  Update is performed
     * against a view model instead of a source model.
     * <p>
     * The test verifies that no errors occur when planning and executing the
     * same batched command SQL with the same number of batched command parameter
     * value sets.  For example, if the first executeBatch() call were to occur
     * with two batched commands a repeated call with two batched commands
     * should not result in an error during planning or execution and the value
     * used in the second batched command should be used instead of any values
     * from the first batched command.
     * <p>
     * The test also verifies that the correct SQL is pushed to the data manager
     * to verify that the parameter substitution occurred and is correct and the
     * correct number of statements made it to the data manager for the respective
     * batch command.
     * <p>
     * The batched command "UPDATE vm1.g1 SET vm1.g1.e2=? WHERE vm1.g1.e1=?"
     * will appear as:
     * <p>
     * UPDATE pm1.g1 SET e2=0 WHERE pm1.g1.e1='a'
     * UPDATE pm1.g1 SET e2=1 WHERE pm1.g1.e1='b'
     * <p>
     * UPDATE pm1.g1 SET e2=2 WHERE pm1.g1.e1='c'
     * UPDATE pm1.g1 SET e2=3 WHERE pm1.g1.e1='d'
     * <p>
     * The result should be that one command is in the plan cache and
     * no plan creation, validation, or execution errors will occur and
     * a predetermined set of queries were executed in the data manager.
     *
     * @throws Exception
     */
    @Test public void testUpdateSameNumCmds_Virtual() throws Exception {
        // Create query
        String preparedSql = "UPDATE vm1.g1 SET vm1.g1.e2=? WHERE vm1.g1.e1=?"; //$NON-NLS-1$
        // Create a testable prepared plan cache
        SessionAwareCache<PreparedPlan> prepPlanCache = new SessionAwareCache<PreparedPlan>("preparedplan", DefaultCacheFactory.INSTANCE, SessionAwareCache.Type.PREPAREDPLAN, 0);

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);

        // Source capabilities must support batched updates
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.BATCHED_UPDATES, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        // Something to hold our final query list
        List<String> finalQueryList = new ArrayList<String>();

        // Create expected results
        List<?>[] expected = new List[] {
            Arrays.asList(3),
            Arrays.asList(1)
        };

        // batch with two commands
        ArrayList<ArrayList<Object>> values = new ArrayList<ArrayList<Object>>(2);
        values.add(new ArrayList<Object>(Arrays.asList(new Object[] { new Integer(0), "a" })));  //$NON-NLS-1$
        values.add(new ArrayList<Object>(Arrays.asList(new Object[] { new Integer(1), "b" })));  //$NON-NLS-1$

        // Add our expected queries to the final query list
           finalQueryList.add(new String("UPDATE pm1.g1 SET e2 = 0 WHERE pm1.g1.e1 = 'a'")); //$NON-NLS-1$
           finalQueryList.add(new String("UPDATE pm1.g1 SET e2 = 1 WHERE pm1.g1.e1 = 'b'")); //$NON-NLS-1$

        // Create the plan and process the query
        TestPreparedStatement.helpTestProcessing(preparedSql, values, expected, dataManager, capFinder, RealMetadataFactory.example1Cached(), prepPlanCache, false, false, false, RealMetadataFactory.example1VDB());

        // Repeat
        expected = new List[] {
            Arrays.asList(1),
            Arrays.asList(0)
        };

        // batch with two commands
        values = new ArrayList<ArrayList<Object>>(1);
        values.add(new ArrayList<Object>(Arrays.asList(new Object[] { new Integer(2), "c" })));  //$NON-NLS-1$
        values.add(new ArrayList<Object>(Arrays.asList(new Object[] { new Integer(3), "d" })));  //$NON-NLS-1$

        // Add our expected queries to the final query list
           finalQueryList.add(new String("UPDATE pm1.g1 SET e2 = 2 WHERE pm1.g1.e1 = 'c'")); //$NON-NLS-1$
           finalQueryList.add(new String("UPDATE pm1.g1 SET e2 = 3 WHERE pm1.g1.e1 = 'd'")); //$NON-NLS-1$

        // Use the cached plan and process the query
        TestPreparedStatement.helpTestProcessing(preparedSql, values, expected, dataManager, capFinder, RealMetadataFactory.example1Cached(), prepPlanCache, false, false, true,RealMetadataFactory.example1VDB());

        // Verify all the queries that were run
        assertEquals("Unexpected queries executed -", finalQueryList, dataManager.getQueries()); //$NON-NLS-1$
    }

    /**
     * Test prepared statements that use batched updates using the same prepared
     * command with varying number of commands in the batch.
     * <p>
     * The test verifies that no errors occur when planning and executing the
     * same batched command SQL with varying number of batched command parameter
     * value sets.  For example, if the first executeBatch() call were to occur
     * with two batched commands a repeated call with only one batched command
     * should not result in an error during planning or execution.
     * <p>
     * The test also verifies that the correct SQL is pushed to the data manager
     * to verify that the parameter substitution occurred and is correct and the
     * correct number of statements made it to the data manager for the respective
     * batch command.
     * <p>
     * The batched command "UPDATE pm1.g1 SET pm1.g1.e1=?, pm1.g1.e3=? WHERE pm1.g1.e2=?"
     * will appear as:
     * <p>
     * UPDATE pm1.g1 SET pm1.g1.e1='a', pm1.g1.e3=false WHERE pm1.g1.e2=0
     * UPDATE pm1.g1 SET pm1.g1.e1=null, pm1.g1.e3=false WHERE pm1.g1.e2=1
     * <p>
     * UPDATE pm1.g1 SET pm1.g1.e1='a', pm1.g1.e3=false WHERE pm1.g1.e2=0
     * <p>
     * UPDATE pm1.g1 SET pm1.g1.e1='a', pm1.g1.e3=false WHERE pm1.g1.e2=0
     * UPDATE pm1.g1 SET pm1.g1.e1=null, pm1.g1.e3=false WHERE pm1.g1.e2=1
     * UPDATE pm1.g1 SET pm1.g1.e1='c', pm1.g1.e3=true WHERE pm1.g1.e2=4
     * UPDATE pm1.g1 SET pm1.g1.e1='b', pm1.g1.e3=true WHERE pm1.g1.e2=5
     * <p>
     * The result should be that three commands are in the plan cache and
     * no plan creation, validation, or execution errors will occur and
     * a predetermined set of queries were executed in the data manager.
     *
     * @throws Exception
     */
    @Test public void testUpdateVarNumCmds() throws Exception {
        // Create query
        String preparedSql = "UPDATE pm1.g1 SET pm1.g1.e1=?, pm1.g1.e3=? WHERE pm1.g1.e2=?"; //$NON-NLS-1$
        // Create a testable prepared plan cache
        SessionAwareCache<PreparedPlan> prepPlanCache = new SessionAwareCache<PreparedPlan>("preparedplan", DefaultCacheFactory.INSTANCE, SessionAwareCache.Type.PREPAREDPLAN, 0);

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);

        // Source capabilities must support batched updates
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.BATCHED_UPDATES, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        // Something to hold our final query list
        List<String> finalQueryList = new ArrayList<String>(13);

        // Create expected results
        // first command should result in 2 rows affected
        // second command should result in 2 rows affected
        List<?>[] expected = new List[] {
            Arrays.asList(new Object[] { new Integer(2) }),
            Arrays.asList(new Object[] { new Integer(2) })
        };

        // batch with two commands
        ArrayList<ArrayList<Object>> values = new ArrayList<ArrayList<Object>>(2);
        values.add(new ArrayList<Object>(Arrays.asList(new Object[] { "a",  Boolean.FALSE, new Integer(0) })));  //$NON-NLS-1$
        values.add(new ArrayList<Object>(Arrays.asList(new Object[] { null, Boolean.FALSE, new Integer(1) })));

        // Add our expected queries to the final query list
           finalQueryList.add(new String("UPDATE pm1.g1 SET e1 = 'a', e3 = FALSE WHERE pm1.g1.e2 = 0")); //$NON-NLS-1$
           finalQueryList.add(new String("UPDATE pm1.g1 SET e1 = null, e3 = FALSE WHERE pm1.g1.e2 = 1")); //$NON-NLS-1$

        // Create the plan and process the query
        TestPreparedStatement.helpTestProcessing(preparedSql, values, expected, dataManager, capFinder, RealMetadataFactory.example1Cached(), prepPlanCache, false, false, false, RealMetadataFactory.example1VDB());

        // Repeat with different number of commands in batch
        // Create expected results
        // first command should result in 2 rows affected
        expected = new List[] {
            Arrays.asList(new Object[] { new Integer(2) })
        };

        // batch with one command
        values = new ArrayList<ArrayList<Object>>(1);
        values.add(new ArrayList<Object>(Arrays.asList(new Object[] { "a",  Boolean.FALSE, new Integer(0) })));  //$NON-NLS-1$

        // Add our expected queries to the final query list
           finalQueryList.add(new String("UPDATE pm1.g1 SET e1 = 'a', e3 = FALSE WHERE pm1.g1.e2 = 0")); //$NON-NLS-1$

        // Use the cached plan and process the query
        TestPreparedStatement.helpTestProcessing(preparedSql, values, expected, dataManager, capFinder, RealMetadataFactory.example1Cached(), prepPlanCache, false, false, true, RealMetadataFactory.example1VDB());

        // Repeat with different number of commands in batch
        // Create expected results
        // first command should result in 2 rows affected
        // second command should result in 2 rows affected
        // third command should result in 0 rows affected
        // fourth command should result in 0 rows affected
        expected = new List[] {
                Arrays.asList(new Object[] { new Integer(2) }),
                Arrays.asList(new Object[] { new Integer(2) }),
                Arrays.asList(new Object[] { new Integer(0) }),
                Arrays.asList(new Object[] { new Integer(0) })
        };

        // batch with four commands
        values = new ArrayList<ArrayList<Object>>(4);
        values.add(new ArrayList<Object>(Arrays.asList(new Object[] { "a",  Boolean.FALSE, new Integer(0)} )));  //$NON-NLS-1$
        values.add(new ArrayList<Object>(Arrays.asList(new Object[] { null, Boolean.FALSE, new Integer(1)} )));
        values.add(new ArrayList<Object>(Arrays.asList(new Object[] { "c",  Boolean.TRUE, new Integer(4)} )));  //$NON-NLS-1$
        values.add(new ArrayList<Object>(Arrays.asList(new Object[] { "b",  Boolean.TRUE, new Integer(5)} )));  //$NON-NLS-1$

        // Add our expected queries to the final query list
        finalQueryList.add(new String("UPDATE pm1.g1 SET e1 = 'a', e3 = FALSE WHERE pm1.g1.e2 = 0")); //$NON-NLS-1$
        finalQueryList.add(new String("UPDATE pm1.g1 SET e1 = null, e3 = FALSE WHERE pm1.g1.e2 = 1")); //$NON-NLS-1$
        finalQueryList.add(new String("UPDATE pm1.g1 SET e1 = 'c', e3 = TRUE WHERE pm1.g1.e2 = 4")); //$NON-NLS-1$
        finalQueryList.add(new String("UPDATE pm1.g1 SET e1 = 'b', e3 = TRUE WHERE pm1.g1.e2 = 5")); //$NON-NLS-1$

        TestPreparedStatement.helpTestProcessing(preparedSql, values, expected, dataManager, capFinder, RealMetadataFactory.example1Cached(), prepPlanCache, false, false, true, RealMetadataFactory.example1VDB());

        // Verify all the queries that were run
        assertEquals("Unexpected queries executed -", finalQueryList, dataManager.getQueries()); //$NON-NLS-1$
    }

    /**
     * Test prepared statements that use batched updates using the same prepared
     * command with varying number of commands in the batch.  Update is
     * performed against a view model instead of a source model.
     * <p>
     * The test verifies that no errors occur when planning and executing the
     * same batched command SQL with varying number of batched command parameter
     * value sets.  For example, if the first executeBatch() call were to occur
     * with two batched commands a repeated call with only one batched command
     * should not result in an error during planning or execution.
     * <p>
     * The test also verifies that the correct SQL is pushed to the data manager
     * to verify that the parameter substitution occurred and is correct and the
     * correct number of statements made it to the data manager for the respective
     * batch command.
     * <p>
     * The batched command "UPDATE vm1.g1 SET vm1.g1.e1=?, vm1.g1.e3=? WHERE vm1.g1.e2=?"
     * will appear as:
     * <p>
     * UPDATE pm1.g1 SET e1='a', e3=false WHERE pm1.g1.e2=0
     * UPDATE pm1.g1 SET e1='b', e3=true WHERE pm1.g1.e2=1
     * <p>
     * UPDATE pm1.g1 SET e1='c', e3=false WHERE pm1.g1.e2=1
     * <p>
     * UPDATE pm1.g1 SET e1='d', e3=false WHERE pm1.g1.e2=1
     * UPDATE pm1.g1 SET e1='e', e3=false WHERE pm1.g1.e2=0
     * UPDATE pm1.g1 SET e1='f', e3=true WHERE pm1.g1.e2=2
     * UPDATE pm1.g1 SET e1='g', e3=true WHERE pm1.g1.e2=3
     * <p>
     * The result should be that three commands are in the plan cache and
     * no plan creation, validation, or execution errors will occur and
     * a predetermined set of queries were executed in the data manager.
     *
     * @throws Exception
     */
    @Test public void testUpdateVarNumCmds_Virtual() throws Exception {
        // Create query
        String preparedSql = "UPDATE vm1.g1 SET vm1.g1.e1=?, vm1.g1.e3=? WHERE vm1.g1.e2=?"; //$NON-NLS-1$
        // Create a testable prepared plan cache
        SessionAwareCache<PreparedPlan> prepPlanCache = new SessionAwareCache<PreparedPlan>("preparedplan", DefaultCacheFactory.INSTANCE, SessionAwareCache.Type.PREPAREDPLAN, 0);

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);

        // Source capabilities must support batched updates
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.BATCHED_UPDATES, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        // Something to hold our final query list
        List<String> finalQueryList = new ArrayList<String>(13);

        // Create expected results
        List<?>[] expected = new List[] {
                Arrays.asList(2),
                Arrays.asList(2)
        };

        // batch with two commands
        ArrayList<ArrayList<Object>> values = new ArrayList<ArrayList<Object>>(2);
        values.add(new ArrayList<Object>(Arrays.asList(new Object[] { "a",  Boolean.FALSE, new Integer(0) })));  //$NON-NLS-1$
        values.add(new ArrayList<Object>(Arrays.asList(new Object[] { "b", Boolean.TRUE, new Integer(1) })));  //$NON-NLS-1$

        // Add our expected queries to the final query list
           finalQueryList.add(new String("UPDATE pm1.g1 SET e1 = 'a', e3 = FALSE WHERE pm1.g1.e2 = 0")); //$NON-NLS-1$
           finalQueryList.add(new String("UPDATE pm1.g1 SET e1 = 'b', e3 = TRUE WHERE pm1.g1.e2 = 1")); //$NON-NLS-1$

        // Create the plan and process the query
        TestPreparedStatement.helpTestProcessing(preparedSql, values, expected, dataManager, capFinder, RealMetadataFactory.example1Cached(), prepPlanCache, false, false, false, RealMetadataFactory.example1VDB());

        // Repeat with different number of commands in batch
        expected = new List[] {
            Arrays.asList(new Object[] { new Integer(2) })
        };

        // batch with one command
        values = new ArrayList<ArrayList<Object>>(1);
        values.add(new ArrayList<Object>(Arrays.asList(new Object[] { "c",  Boolean.FALSE, new Integer(1) })));  //$NON-NLS-1$

        // Add our expected queries to the final query list
           finalQueryList.add(new String("UPDATE pm1.g1 SET e1 = 'c', e3 = FALSE WHERE pm1.g1.e2 = 1")); //$NON-NLS-1$

        // Use the cached plan and process the query
        TestPreparedStatement.helpTestProcessing(preparedSql, values, expected, dataManager, capFinder, RealMetadataFactory.example1Cached(), prepPlanCache, false, false, true, RealMetadataFactory.example1VDB());

        // Repeat with different number of commands in batch
        expected = new List[] {
                Arrays.asList(2),
                Arrays.asList(2),
                Arrays.asList(1),
                Arrays.asList(1)
        };

        // batch with four commands
        values = new ArrayList<ArrayList<Object>>(4);
        values.add(new ArrayList<Object>(Arrays.asList(new Object[] { "d",  Boolean.FALSE, new Integer(1)} )));  //$NON-NLS-1$
        values.add(new ArrayList<Object>(Arrays.asList(new Object[] { "e", Boolean.FALSE, new Integer(0)} )));  //$NON-NLS-1$
        values.add(new ArrayList<Object>(Arrays.asList(new Object[] { "f",  Boolean.TRUE, new Integer(2)} )));  //$NON-NLS-1$
        values.add(new ArrayList<Object>(Arrays.asList(new Object[] { "g",  Boolean.TRUE, new Integer(3)} )));  //$NON-NLS-1$

        // Add our expected queries to the final query list
        finalQueryList.add(new String("UPDATE pm1.g1 SET e1 = 'd', e3 = FALSE WHERE pm1.g1.e2 = 1")); //$NON-NLS-1$
        finalQueryList.add(new String("UPDATE pm1.g1 SET e1 = 'e', e3 = FALSE WHERE pm1.g1.e2 = 0")); //$NON-NLS-1$
        finalQueryList.add(new String("UPDATE pm1.g1 SET e1 = 'f', e3 = TRUE WHERE pm1.g1.e2 = 2")); //$NON-NLS-1$
        finalQueryList.add(new String("UPDATE pm1.g1 SET e1 = 'g', e3 = TRUE WHERE pm1.g1.e2 = 3")); //$NON-NLS-1$

        TestPreparedStatement.helpTestProcessing(preparedSql, values, expected, dataManager, capFinder, RealMetadataFactory.example1Cached(), prepPlanCache, false, false, true,RealMetadataFactory.example1VDB());

        // Verify all the queries that were run
        assertEquals("Unexpected queries executed -", finalQueryList, dataManager.getQueries()); //$NON-NLS-1$
    }

    @Test public void testBulkBytePushdown() throws Exception {
        String preparedSql = "insert into g1 (e1) values (?)"; //$NON-NLS-1$
        TransformationMetadata tm = RealMetadataFactory.fromDDL("create foreign table g1 (e1 varbinary) options (updatable true)", "y", "z");

        // Create a testable prepared plan cache
        SessionAwareCache<PreparedPlan> prepPlanCache = new SessionAwareCache<PreparedPlan>("preparedplan", DefaultCacheFactory.INSTANCE, SessionAwareCache.Type.PREPAREDPLAN, 0);

        // Construct data manager with data
        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("INSERT INTO g1 (e1) VALUES (?)", new List[] {Arrays.asList(1), Arrays.asList(1)}); //$NON-NLS-1$
        // Source capabilities must support batched updates
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.BULK_UPDATE, true);
        capFinder.addCapabilities("z", caps); //$NON-NLS-1$

        ArrayList<List<?>> values = new ArrayList<List<?>>(2);
        values.add(Arrays.asList(new byte[1]));  //$NON-NLS-1$
        values.add(Arrays.asList(new byte[1]));  //$NON-NLS-1$

        List<?>[] expected = new List[] {
                Arrays.asList(1), Arrays.asList(1)
        };

        // Create the plan and process the query
        TestPreparedStatement.helpTestProcessing(preparedSql, values, expected, dataManager, capFinder, tm, prepPlanCache, false, false, false, tm.getVdbMetaData());
        Insert insert = (Insert)dataManager.getCommandHistory().iterator().next();
        Constant c = (Constant)insert.getValues().get(0);
        assertTrue(c.isMultiValued());
        assertTrue(((List<?>)c.getValue()).get(0) instanceof BinaryType);
    }

}
