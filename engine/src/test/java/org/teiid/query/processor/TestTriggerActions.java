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
import static org.teiid.query.processor.TestProcessor.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.teiid.api.exception.query.QueryProcessingException;
import org.teiid.dqp.service.TransactionContext;
import org.teiid.dqp.service.TransactionContext.Scope;
import org.teiid.dqp.service.TransactionService;
import org.teiid.metadata.Table;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.resolver.TestResolver;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.util.CommandContext;
import org.teiid.query.validator.TestUpdateValidator;
import org.teiid.translator.ExecutionFactory.TransactionSupport;
import org.teiid.translator.SourceSystemFunctions;

@SuppressWarnings("nls")
public class TestTriggerActions {

    private static final String GX = "GX";
    private static final String VM1 = "VM1";

    @Test public void testInsert() throws Exception {
        TransformationMetadata metadata = TestUpdateValidator.example1();
        TestUpdateValidator.createView("select 1 as x, 2 as y", metadata, GX);
        Table t = metadata.getMetadataStore().getSchemas().get(VM1).getTables().get(GX);
        t.setDeletePlan("");
        t.setUpdatePlan("");
        t.setInsertPlan("FOR EACH ROW BEGIN insert into pm1.g1 (e1) values (new.x); END");

        String sql = "insert into gx (x, y) values (1, 2)";

        FakeDataManager dm = new FakeDataManager();
        FakeDataStore.addTable("pm1.g1", dm, metadata);

        CommandContext context = createCommandContext();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        ProcessorPlan plan = TestProcessor.helpGetPlan(TestResolver.helpResolve(sql, metadata), metadata, new DefaultCapabilitiesFinder(caps), context);
        List<?>[] expected = new List[] {Arrays.asList(1)};
        helpProcess(plan, context, dm, expected);
    }

    @Test public void testInsertWithDefault() throws Exception {
        TransformationMetadata metadata = TestUpdateValidator.example1();
        TestUpdateValidator.createView("select 1 as x, 2 as y", metadata, GX);
        Table t = metadata.getMetadataStore().getSchemas().get(VM1).getTables().get(GX);
        t.setDeletePlan("");
        t.setUpdatePlan("");
        t.setInsertPlan("FOR EACH ROW BEGIN insert into pm1.g1 (e1) values (new.x); END");

        String sql = "insert into gx (x) values (1)";

        FakeDataManager dm = new FakeDataManager();
        FakeDataStore.addTable("pm1.g1", dm, metadata);

        CommandContext context = createCommandContext();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        ProcessorPlan plan = TestProcessor.helpGetPlan(TestResolver.helpResolve(sql, metadata), metadata, new DefaultCapabilitiesFinder(caps), context);
        List<?>[] expected = new List[] {Arrays.asList(1)};
        helpProcess(plan, context, dm, expected);
    }

    @Test public void testInsertWithQueryExpression() throws Exception {
        TransformationMetadata metadata = TestUpdateValidator.example1();
        TestUpdateValidator.createView("select '1' as x, 2 as y", metadata, GX);
        Table t = metadata.getMetadataStore().getSchemas().get(VM1).getTables().get(GX);
        t.setDeletePlan("");
        t.setUpdatePlan("");
        t.setInsertPlan("FOR EACH ROW BEGIN insert into pm1.g1 (e1) values (new.x); END");

        String sql = "insert into gx (x, y) select e1, e2 from pm1.g1";

        FakeDataManager dm = new FakeDataManager();
        FakeDataStore.addTable("pm1.g1", dm, metadata);
        CommandContext context = createCommandContext();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        ProcessorPlan plan = TestProcessor.helpGetPlan(TestResolver.helpResolve(sql, metadata), metadata, new DefaultCapabilitiesFinder(caps), context);
        List<?>[] expected = new List[] {Arrays.asList(6)};
        helpProcess(plan, context, dm, expected);
    }

    @Test public void testDynamic() throws Exception {
        TransformationMetadata metadata = TestUpdateValidator.example1();
        TestUpdateValidator.createView("select '1' as x, 2 as y", metadata, GX);
        Table t = metadata.getMetadataStore().getSchemas().get(VM1).getTables().get(GX);
        t.setDeletePlan("FOR EACH ROW BEGIN ATOMIC END");
        t.setUpdatePlan("");
        t.setInsertPlan("FOR EACH ROW BEGIN execute immediate 'delete from gx where gx.x = new.x'; END");

        String sql = "insert into gx (x, y) select e1, e2 from pm1.g1";

        FakeDataManager dm = new FakeDataManager();
        FakeDataStore.addTable("pm1.g1", dm, metadata);
        CommandContext context = createCommandContext();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        ProcessorPlan plan = TestProcessor.helpGetPlan(TestResolver.helpResolve(sql, metadata), metadata, new DefaultCapabilitiesFinder(caps), context);
        List<?>[] expected = new List[] {Arrays.asList(6)};
        helpProcess(plan, context, dm, expected);
    }

    @Test public void testDynamicUpdate() throws Exception {
        TransformationMetadata metadata = TestUpdateValidator.example1();
        TestUpdateValidator.createView("select '1' as x, 2 as y", metadata, GX);
        Table t = metadata.getMetadataStore().getSchemas().get(VM1).getTables().get(GX);
        t.setDeletePlan("");
        t.setUpdatePlan("FOR EACH ROW BEGIN execute immediate 'update pm1.g1 set e1 = new.x where e2 = new.y'; END");
        t.setInsertPlan("");

        String sql = "update gx set x = 1 where y = 2";

        HardcodedDataManager dm = new HardcodedDataManager();
        dm.addData("UPDATE pm1.g1 SET e1 = '1' WHERE e2 = 2", new List[] {Arrays.asList(1)});
        CommandContext context = createCommandContext();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        ProcessorPlan plan = TestProcessor.helpGetPlan(TestResolver.helpResolve(sql, metadata), metadata, new DefaultCapabilitiesFinder(caps), context);
        List<?>[] expected = new List[] {Arrays.asList(1)};
        helpProcess(plan, context, dm, expected);
    }

    @Test public void testDynamicRecursion() throws Exception {
        TransformationMetadata metadata = TestUpdateValidator.example1();
        TestUpdateValidator.createView("select 'a' as x, 2 as y", metadata, GX);
        Table t = metadata.getMetadataStore().getSchemas().get(VM1).getTables().get(GX);
        t.setDeletePlan("FOR EACH ROW BEGIN ATOMIC insert into gx (x, y) values (old.x, old.y); END");
        t.setUpdatePlan("");
        t.setInsertPlan("FOR EACH ROW BEGIN execute immediate 'delete from gx where gx.x = new.x'; END");

        String sql = "insert into gx (x, y) select e1, e2 from pm1.g1";

        FakeDataManager dm = new FakeDataManager();
        FakeDataStore.addTable("pm1.g1", dm, metadata);
        CommandContext context = createCommandContext();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        ProcessorPlan plan = TestProcessor.helpGetPlan(TestResolver.helpResolve(sql, metadata), metadata, new DefaultCapabilitiesFinder(caps), context);
        try {
            helpProcess(plan, context, dm, null);
            fail();
        } catch (QueryProcessingException e) {
            assertEquals("TEIID30168 Couldn't execute the dynamic SQL command \"EXECUTE IMMEDIATE 'delete from gx where gx.x = new.x'\" with the SQL statement \"delete from gx where gx.x = new.x\" due to: TEIID30347 There is a recursive invocation of group 'I gx'. Please correct the SQL.", e.getMessage());
        }
    }

    @Test public void testDelete() throws Exception {
        TransformationMetadata metadata = TestUpdateValidator.example1();
        TestUpdateValidator.createView("select 1 as x, 2 as y", metadata, GX);
        Table t = metadata.getMetadataStore().getSchemas().get(VM1).getTables().get(GX);
        t.setDeletePlan("FOR EACH ROW BEGIN delete from pm1.g1 where e2 = old.x; END");
        t.setUpdatePlan("");
        t.setInsertPlan("");

        String sql = "delete from gx where y = 2";

        FakeDataManager dm = new FakeDataManager();
        FakeDataStore.addTable("pm1.g1", dm, metadata);

        CommandContext context = createCommandContext();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        ProcessorPlan plan = TestProcessor.helpGetPlan(TestResolver.helpResolve(sql, metadata), metadata, new DefaultCapabilitiesFinder(caps), context);
        List<?>[] expected = new List[] {Arrays.asList(1)};
        helpProcess(plan, context, dm, expected);
    }

    @Test public void testUpdate() throws Exception {
        TransformationMetadata metadata = TestUpdateValidator.example1();
        TestUpdateValidator.createView("select 1 as x, 2 as y", metadata, GX);
        Table t = metadata.getMetadataStore().getSchemas().get(VM1).getTables().get(GX);
        t.setDeletePlan("");
        t.setUpdatePlan("FOR EACH ROW BEGIN update pm1.g1 set e2 = new.y where e2 = old.y; END");
        t.setInsertPlan("");

        String sql = "update gx set y = 5";

        FakeDataManager dm = new FakeDataManager();
        FakeDataStore.addTable("pm1.g1", dm, metadata);

        CommandContext context = createCommandContext();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        ProcessorPlan plan = TestProcessor.helpGetPlan(TestResolver.helpResolve(sql, metadata), metadata, new DefaultCapabilitiesFinder(caps), context);
        List<?>[] expected = new List[] {Arrays.asList(1)};
        helpProcess(plan, context, dm, expected);
        assertEquals("UPDATE pm1.g1 SET e2 = 5 WHERE e2 = 2", dm.getQueries().get(0));
    }

    @Test public void testUpdateWithChanging() throws Exception {
        TransformationMetadata metadata = TestUpdateValidator.example1();
        TestUpdateValidator.createView("select 1 as x, 2 as y", metadata, GX);
        Table t = metadata.getMetadataStore().getSchemas().get(VM1).getTables().get(GX);
        t.setDeletePlan("");
        t.setUpdatePlan("FOR EACH ROW BEGIN update pm1.g1 set e2 = case when changing.y then new.y end where e2 = old.y; END");
        t.setInsertPlan("");

        String sql = "update gx set y = 5";

        FakeDataManager dm = new FakeDataManager();
        FakeDataStore.addTable("pm1.g1", dm, metadata);

        CommandContext context = createCommandContext();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        ProcessorPlan plan = TestProcessor.helpGetPlan(TestResolver.helpResolve(sql, metadata), metadata, new DefaultCapabilitiesFinder(caps), context);
        List<?>[] expected = new List[] {Arrays.asList(1)};
        helpProcess(plan, context, dm, expected);
        assertEquals("UPDATE pm1.g1 SET e2 = 5 WHERE e2 = 2", dm.getQueries().get(0));
    }

    @Test public void testUpdateWithNonConstant() throws Exception {
        TransformationMetadata metadata = TestUpdateValidator.example1();
        TestUpdateValidator.createView("select 1 as x, 2 as y", metadata, GX);
        Table t = metadata.getMetadataStore().getSchemas().get(VM1).getTables().get(GX);
        t.setDeletePlan("");
        t.setUpdatePlan("FOR EACH ROW BEGIN update pm1.g1 set e2 = new.y where e2 = old.y; END");
        t.setInsertPlan("");

        String sql = "update gx set y = x";

        FakeDataManager dm = new FakeDataManager();
        FakeDataStore.addTable("pm1.g1", dm, metadata);

        CommandContext context = createCommandContext();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        ProcessorPlan plan = TestProcessor.helpGetPlan(TestResolver.helpResolve(sql, metadata), metadata, new DefaultCapabilitiesFinder(caps), context);
        List<?>[] expected = new List[] {Arrays.asList(1)};
        helpProcess(plan, context, dm, expected);
        assertEquals("UPDATE pm1.g1 SET e2 = 1 WHERE e2 = 2", dm.getQueries().get(0));
    }

    @Test public void testUpdateSetExpression() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.fromDDL("create foreign table g1 (e1 string, e2 integer) options (updatable true);"
                + " create view GX options (updatable true) as select '1' as x, 2 as y;"
                + " create trigger on GX instead of update as for each row begin update g1 set e1 = new.x, e2 = new.y where e2 = old.y; END", "x", "y");

        String sql = "update gx set x = x || 'a' where y = 2";

        HardcodedDataManager dm = new HardcodedDataManager();
        dm.addData("UPDATE g1 SET e1 = '1a', e2 = 2 WHERE e2 = 2", new List[] {Arrays.asList(1)});
        CommandContext context = createCommandContext();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        ProcessorPlan plan = TestProcessor.helpGetPlan(TestResolver.helpResolve(sql, metadata), metadata, new DefaultCapabilitiesFinder(caps), context);
        List<?>[] expected = new List[] {Arrays.asList(1)};
        helpProcess(plan, context, dm, expected);
    }

    @Test public void testUpdateIfDistinct() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.fromDDL("create foreign table g1 (e1 string, e2 integer) options (updatable true);"
                + " create view GX options (updatable true) as select '1' as x, 2 as y union all select '2' as x, 2 as y;"
                + " create trigger on GX instead of update as for each row begin if (\"new\" is distinct from \"old\") update g1 set e1 = new.x, e2 = new.y where e2 = old.y; END", "x", "y");

        String sql = "update gx set x = x || 'a' where y = 2";

        HardcodedDataManager dm = new HardcodedDataManager();
        dm.addData("UPDATE g1 SET e1 = '1a', e2 = 2 WHERE e2 = 2", new List[] {Arrays.asList(1)});
        dm.addData("UPDATE g1 SET e1 = '2a', e2 = 2 WHERE e2 = 2", new List[] {Arrays.asList(1)});
        CommandContext context = createCommandContext();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        ProcessorPlan plan = TestProcessor.helpGetPlan(TestResolver.helpResolve(sql, metadata), metadata, new DefaultCapabilitiesFinder(caps), context);
        List<?>[] expected = new List[] {Arrays.asList(2)};
        helpProcess(plan, context, dm, expected);

        metadata = RealMetadataFactory.fromDDL("create foreign table g1 (e1 string, e2 integer) options (updatable true);"
                + " create view GX options (updatable true) as select '1' as x, 2 as y union all select '2' as x, 2 as y;"
                + " create trigger on GX instead of update as for each row begin if (\"new\" is not distinct from \"old\") update g1 set e1 = new.x, e2 = new.y where e2 = old.y; END", "x", "y");

        //no updates expected
        dm.clearData();
        context = createCommandContext();
        plan = TestProcessor.helpGetPlan(TestResolver.helpResolve(sql, metadata), metadata, new DefaultCapabilitiesFinder(caps), context);
        expected = new List[] {Arrays.asList(2)};
        helpProcess(plan, context, dm, expected);
    }

    @Test public void testUpdateIfDistinctVariables() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.fromDDL("create foreign table g1 (e1 string, e2 integer) options (updatable true);"
                + " create view GX options (updatable true) as select '1' as x, 2 as y union all select '2' as x, 2 as y;"
                + " create trigger on GX instead of update as for each row begin if (\"new\" is distinct from variables) update g1 set e1 = new.x, e2 = new.y where e2 = old.y; END", "x", "y");

        String sql = "update gx set x = x || 'a' where y = 2";

        HardcodedDataManager dm = new HardcodedDataManager();
        dm.addData("UPDATE g1 SET e1 = '1a', e2 = 2 WHERE e2 = 2", new List[] {Arrays.asList(1)});
        dm.addData("UPDATE g1 SET e1 = '2a', e2 = 2 WHERE e2 = 2", new List[] {Arrays.asList(1)});
        CommandContext context = createCommandContext();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        ProcessorPlan plan = TestProcessor.helpGetPlan(TestResolver.helpResolve(sql, metadata), metadata, new DefaultCapabilitiesFinder(caps), context);
        List<?>[] expected = new List[] {Arrays.asList(2)};
        helpProcess(plan, context, dm, expected);
    }

    @Test public void testInsertWithQueryExpressionAndAlias() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.fromDDL(
                "create foreign table tablea (TEST_ID integer, TEST_NBR bigdecimal) options (updatable true);\n" +
                "create foreign table tableb (TEST_ID integer, TEST_NBR bigdecimal);\n" +
                "create view viewa options (updatable true) as SELECT TEST_ID, TEST_NBR FROM tablea;\n" +
                "create trigger on viewa instead of insert as for each row begin atomic "
                + "INSERT INTO tablea (tablea.TEST_ID, tablea.TEST_NBR) VALUES (\"NEW\".TEST_ID, \"NEW\".TEST_NBR); END;"
                , "x", "y");

        String sql = "insert into viewa (TEST_ID, TEST_NBR) SELECT TEST_ID AS X, TEST_NBR FROM tableb";

        HardcodedDataManager dm = new HardcodedDataManager();
        dm.addData("INSERT INTO tablea (TEST_ID, TEST_NBR) VALUES (1, 2.0)", Arrays.asList(1));
        dm.addData("SELECT g_0.TEST_ID, g_0.TEST_NBR FROM y.tableb AS g_0", Arrays.asList(1, 2.0));

        CommandContext context = createCommandContext();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        ProcessorPlan plan = TestProcessor.helpGetPlan(TestResolver.helpResolve(sql, metadata), metadata, new DefaultCapabilitiesFinder(caps), context);
        List<?>[] expected = new List[] {Arrays.asList(1)};
        helpProcess(plan, context, dm, expected);
    }

    @Test public void testTransactions() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.fromDDL("x",
                new RealMetadataFactory.DDLHolder("virt", "CREATE VIEW users options (updatable true) as select id, dummy_data, updated_at from db1.user1 union all select id, dummy_data, updated_at from db2.user2; "
                        + "CREATE TRIGGER ON users INSTEAD OF DELETE AS FOR EACH ROW IF (OLD.id < (SELECT MAX(id) FROM db1.user1)) BEGIN ATOMIC DELETE FROM db1.user1 WHERE id = OLD.id; END ELSE IF (OLD.id = (SELECT MAX(id) FROM db1.user1)) BEGIN ATOMIC UPDATE db1.user1 SET dummy_data = '', updated_at = NOW() WHERE id = OLD.id; END ELSE IF (OLD.id > (SELECT MAX(id) FROM db1.user1)) BEGIN ATOMIC DELETE FROM db2.user2 WHERE id = OLD.id; END; "
                        + "CREATE TRIGGER ON users INSTEAD OF INSERT AS FOR EACH ROW begin atomic insert into db1.user1 (id) values (new.id); insert into db2.user2 (id) values (new.id); end; "
                        + "CREATE TRIGGER ON users INSTEAD OF update AS FOR EACH ROW begin atomic end; "),
                new RealMetadataFactory.DDLHolder("db1", "create foreign table user1 (id integer, dummy_data string, updated_at timestamp) options (updatable true);"),
                new RealMetadataFactory.DDLHolder("db2", "create foreign table user2 (id integer, dummy_data string, updated_at timestamp) options (updatable true);"));

        String sql = "delete from users where id = 203";
        HardcodedDataManager dm = new HardcodedDataManager();
        dm.addData("SELECT g_0.id, g_0.dummy_data, g_0.updated_at FROM db1.user1 AS g_0 WHERE g_0.id = 203", Arrays.asList(203, "", null));
        dm.addData("SELECT g_0.id, g_0.dummy_data, g_0.updated_at FROM db2.user2 AS g_0 WHERE g_0.id = 203");
        dm.addData("SELECT g_0.id FROM db1.user1 AS g_0", Arrays.asList(203), Arrays.asList(204));
        dm.addData("DELETE FROM db1.user1 WHERE id = 203", Arrays.asList(1));
        dm.addData("INSERT INTO db1.user1 (id) VALUES (205)", Arrays.asList(1));
        dm.addData("INSERT INTO db2.user2 (id) VALUES (205)", Arrays.asList(1));

        CommandContext context = createCommandContext();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setSourceProperty(Capability.TRANSACTION_SUPPORT, TransactionSupport.XA);
        ProcessorPlan plan = TestProcessor.helpGetPlan(TestResolver.helpResolve(sql, metadata), metadata, new DefaultCapabilitiesFinder(caps), context);
        //assumed required, but won't start a txn
        assertTrue(plan.requiresTransaction(false));

        TransactionContext tc = new TransactionContext();
        TransactionService ts = Mockito.mock(TransactionService.class);
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                ((TransactionContext)invocation.getArguments()[0]).setTransactionType(Scope.REQUEST);
                return null;
            }
        }).when(ts).begin(tc);

        context.setTransactionService(ts);
        context.setTransactionContext(tc);

        List<?>[] expected = new List[] {Arrays.asList(1)};
        helpProcess(plan, context, dm, expected);
        Mockito.verify(ts, Mockito.never()).begin(tc);

        //required, and will start a txn
        sql = "insert into users (id) values (205)";
        plan = TestProcessor.helpGetPlan(TestResolver.helpResolve(sql, metadata), metadata, new DefaultCapabilitiesFinder(caps), context);
        assertTrue(plan.requiresTransaction(false));

        helpProcess(plan, context, dm, expected);
        Mockito.verify(ts, Mockito.times(1)).begin(tc);

        //does nothing
        sql = "update users set id = 205";
        plan = TestProcessor.helpGetPlan(TestResolver.helpResolve(sql, metadata), metadata, new DefaultCapabilitiesFinder(caps), context);
        assertFalse(plan.requiresTransaction(false));
    }

    /**
     * Ensure that we simplify expressions
     */
    @Test public void testInsertRewrite() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.fromDDL(
                "create foreign table tablea (s string primary key) options (updatable true);\n" +
                "create view viewa (i integer) options (updatable true) as SELECT cast(s as integer) from tablea;\n" +
                "create trigger on viewa instead of insert as for each row begin atomic "
                + "INSERT INTO tablea (tablea.s) VALUES (\"NEW\".i); END;"
                + "create trigger on viewa instead of update as for each row begin atomic "
                + "update tablea set s = \"NEW\".i; END;"
                , "x", "y");

        String sql = "insert into viewa (i) values (1)";

        HardcodedDataManager dm = new HardcodedDataManager();
        dm.addData("INSERT INTO tablea (s) VALUES ('1')", Arrays.asList(1));

        CommandContext context = createCommandContext();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setFunctionSupport(SourceSystemFunctions.CONVERT, true);
        ProcessorPlan plan = TestProcessor.helpGetPlan(TestResolver.helpResolve(sql, metadata), metadata, new DefaultCapabilitiesFinder(caps), context);
        List<?>[] expected = new List[] {Arrays.asList(1)};
        helpProcess(plan, context, dm, expected);
    }

}
