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
import org.teiid.api.exception.query.QueryValidatorException;
import org.teiid.cache.DefaultCacheFactory;
import org.teiid.dqp.internal.process.PreparedPlan;
import org.teiid.dqp.internal.process.SessionAwareCache;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.rewriter.TestQueryRewriter;
import org.teiid.query.sql.lang.BatchedUpdateCommand;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.util.CommandContext;
import org.teiid.query.validator.TestUpdateValidator;
import org.teiid.translator.SourceSystemFunctions;

@SuppressWarnings("nls")
public class TestInherintlyUpdatableViews {

    @Test public void testUpdatePassThrough() throws Exception {
        String userSql = "update vm1.gx set e1 = e2"; //$NON-NLS-1$
        String viewSql = "select * from pm1.g1 where e3 < 5";
        String expectedSql = "UPDATE pm1.g1 SET e1 = convert(pm1.g1.e2, string) WHERE convert(e3, integer) < 5";
        helpTest(userSql, viewSql, expectedSql, null);
    }

    private Command helpTest(String userSql, String viewSql, String expectedSql, ProcessorDataManager dm)
            throws Exception {
        TransformationMetadata metadata = TestUpdateValidator.example1();
        TestUpdateValidator.createView(viewSql, metadata, "gx");
        Command command = TestQueryRewriter.helpTestRewriteCommand(userSql, expectedSql, metadata);

        if (dm != null) {
            CommandContext context = createCommandContext();
            SessionAwareCache<PreparedPlan> planCache = new SessionAwareCache<PreparedPlan>("preparedplan", DefaultCacheFactory.INSTANCE, SessionAwareCache.Type.PREPAREDPLAN, 0);
            context.setPreparedPlanCache(planCache); //$NON-NLS-1$
            BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
            caps.setFunctionSupport(SourceSystemFunctions.CONVERT, true);
            ProcessorPlan plan = helpGetPlan(helpParse(userSql), metadata, new DefaultCapabilitiesFinder(caps), context);
            List<?>[] expected = new List[] {Arrays.asList(1)};
            helpProcess(plan, context, dm, expected);
            assertEquals(0, planCache.getTotalCacheEntries());
        }

        return command;
    }

    @Test public void testUpdatePassThroughWithAlias() throws Exception {
        String userSql = "update vm1.gx set e1 = e2"; //$NON-NLS-1$
        String viewSql = "select * from pm1.g1 as x where e3 < 5";
        String expectedSql = "UPDATE pm1.g1 SET e1 = convert(pm1.g1.e2, string) WHERE convert(e3, integer) < 5";
        helpTest(userSql, viewSql, expectedSql, null);
    }

    @Test public void testDeletePassThrough() throws Exception {
        String userSql = "delete from vm1.gx where e1 = e2"; //$NON-NLS-1$
        String viewSql = "select * from pm1.g1 where e3 < 5";
        String expectedSql = "DELETE FROM pm1.g1 WHERE (pm1.g1.e1 = convert(pm1.g1.e2, string)) AND (convert(e3, integer) < 5)";
        helpTest(userSql, viewSql, expectedSql, null);
    }

    @Test public void testInsertPassThrough() throws Exception {
        String userSql = "insert into vm1.gx (e1) values (1)"; //$NON-NLS-1$
        String viewSql = "select * from pm1.g1 where e3 < 5";
        String expectedSql = "INSERT INTO pm1.g1 (e1) VALUES ('1')";
        helpTest(userSql, viewSql, expectedSql, null);
    }

    @Test public void testDeleteUnion() throws Exception {
        String userSql = "delete from vm1.gx where e4 is null"; //$NON-NLS-1$
        String viewSql = "select * from pm1.g1 where e3 < 5 union all select * from pm1.g2 where e1 > 1";
        String expectedSql = "BatchedUpdate{D,D}";
        BatchedUpdateCommand buc = (BatchedUpdateCommand)helpTest(userSql, viewSql, expectedSql, null);
        assertEquals("DELETE FROM pm1.g2 WHERE (pm1.g2.e4 IS NULL) AND (e1 > '1')", buc.getUpdateCommands().get(1).toString());
    }

    /**
     * Here we should be able to figure out that we can pass through the join
     * @throws Exception
     */
    @Test public void testInsertPassThrough1() throws Exception {
        String userSql = "insert into vm1.gx (e1) values (1)"; //$NON-NLS-1$
        String viewSql = "select g2.* from pm1.g1 inner join pm1.g2 on g1.e1 = g2.e1";
        String expectedSql = "INSERT INTO pm1.g2 (e1) VALUES ('1')";
        helpTest(userSql, viewSql, expectedSql, null);
    }

    @Test public void testUpdateComplex() throws Exception {
        String userSql = "update vm1.gx set e1 = e2 where e3 is null"; //$NON-NLS-1$
        String viewSql = "select g2.e1, g1.e2, g2.e3, g2.e4 from pm1.g1 inner join pm1.g2 on g1.e1 = g2.e1";

        HardcodedDataManager dm = new HardcodedDataManager();
        dm.addData("SELECT convert(g_0.e2, string), g_1.e2 FROM pm1.g1 AS g_0, pm1.g2 AS g_1 WHERE (g_0.e1 = g_1.e1) AND (g_1.e3 IS NULL)", new List[] {Arrays.asList("1", 1)});
        dm.addData("UPDATE pm1.g2 SET e1 = '1' WHERE pm1.g2.e2 = 1", new List[] {Arrays.asList(1)});

        helpTest(userSql, viewSql, "BEGIN ATOMIC\n" +
                "DECLARE integer VARIABLES.ROWS_UPDATED = 0;\n" +
                "INSERT INTO #changes (s_0, s_1) SELECT convert(g1.e2, string) AS s_0, pm1.g2.e2 AS s_1 FROM pm1.g1 INNER JOIN pm1.g2 ON g1.e1 = g2.e1 WHERE g2.e3 IS NULL;\n" +
                "LOOP ON (SELECT #changes.s_0, #changes.s_1 FROM #changes) AS X\n" +
                "BEGIN\n" +
                "UPDATE pm1.g2 SET e1 = X.s_0 WHERE pm1.g2.e2 = X.s_1;\n" +
                "VARIABLES.ROWS_UPDATED = (VARIABLES.ROWS_UPDATED + 1);\n" +
                "END\n" +
                "SELECT VARIABLES.ROWS_UPDATED AS ROWS_UPDATED;\n" +
                "END",
                dm);
    }

    @Test public void testDeleteComplex() throws Exception {
        String userSql = "delete from vm1.gx where e2 < 10"; //$NON-NLS-1$
        String viewSql = "select g2.e1, g1.e2, g2.e3, g2.e4 from pm1.g1 inner join pm1.g2 on g1.e1 = g2.e1";

        HardcodedDataManager dm = new HardcodedDataManager();
        dm.addData("SELECT g_1.e2 FROM pm1.g1 AS g_0, pm1.g2 AS g_1 WHERE (g_0.e1 = g_1.e1) AND (g_0.e2 < 10)", new List[] {Arrays.asList(2)});
        dm.addData("DELETE FROM pm1.g2 WHERE pm1.g2.e2 = 2", new List[] {Arrays.asList(1)});

        helpTest(userSql, viewSql, "BEGIN ATOMIC\n" +
                "DECLARE integer VARIABLES.ROWS_UPDATED = 0;\n" +
                "INSERT INTO #changes (s_0) SELECT pm1.g2.e2 AS s_0 FROM pm1.g1 INNER JOIN pm1.g2 ON g1.e1 = g2.e1 WHERE g1.e2 < 10;\n" +
                "LOOP ON (SELECT #changes.s_0 FROM #changes) AS X\n" +
                "BEGIN\n" +
                "DELETE FROM pm1.g2 WHERE pm1.g2.e2 = X.s_0;\n" +
                "VARIABLES.ROWS_UPDATED = (VARIABLES.ROWS_UPDATED + 1);\n" +
                "END\n" +
                "SELECT VARIABLES.ROWS_UPDATED AS ROWS_UPDATED;\n" +
                "END",
                dm);
    }

    /**
     * Here we should use the partitioning
     * @throws Exception
     */
    @Test public void testInsertPartitionedUnion() throws Exception {
        String userSql = "insert into vm1.gx (e1, e2) values (1, 2)"; //$NON-NLS-1$
        String viewSql = "select 1 as e1, e2 from pm1.g1 union all select 2 as e1, e2 from pm1.g2";
        String expectedSql = "INSERT INTO pm1.g1 (e2) VALUES (2)";
        helpTest(userSql, viewSql, expectedSql, null);
    }

    @Test public void testWherePartitioningUpdates() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.fromDDL("create foreign table b (custid integer, field1 varchar) options (updatable true); " +
                "create view finnish_customers options (updatable true) as select custid, field1 as name from b where custid = 1; " +
                "create view other_customers options (updatable true) as select custid, field1 as name from b where custid = 2; " +
                "create view customers options (updatable true) as select * from finnish_customers where custid = 1 union all select * from other_customers where custid = 2;", "x", "y");

        ProcessorPlan plan = TestProcessor.helpGetPlan("insert into customers (custid, name) values (1, 'a')", metadata);
        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("INSERT INTO b (custid, field1) VALUES (1, 'a')", new List<?>[] {Arrays.asList(1)});
        TestProcessor.helpProcess(plan, dataManager, new List<?>[] {Arrays.asList(1)});

        //ensure that update works as expected - TODO: eventually we should support a check option to not allow updates such as this
        plan = TestProcessor.helpGetPlan("update customers set custid = 3, name = 'a'", metadata, TestOptimizer.getGenericFinder());
        dataManager = new HardcodedDataManager();
        dataManager.addData("UPDATE b SET custid = 3, field1 = 'a' WHERE custid = 1", new List<?>[] {Arrays.asList(1)});
        dataManager.addData("UPDATE b SET custid = 3, field1 = 'a' WHERE custid = 2", new List<?>[] {Arrays.asList(1)});
        TestProcessor.helpProcess(plan, dataManager, new List<?>[] {Arrays.asList(2)});
    }

    @Test public void testUpdatesWithProjectedFunction() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.fromDDL("CREATE FOREIGN TABLE SmallA (IntValue string, StringKey string PRIMARY KEY) OPTIONS(UPDATABLE true);"
                + "CREATE VIEW ViewA(x integer, y string PRIMARY KEY) OPTIONS (UPDATABLE true) AS\n" +
                "           SELECT CONVERT(source.IntValue,integer) as x, source.StringKey as y FROM SmallA as source;", "x", "y");

        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setFunctionSupport(SourceSystemFunctions.CONVERT, true);

        ProcessorPlan plan = TestProcessor.helpGetPlan("DELETE FROM ViewA WHERE x=13", metadata, new DefaultCapabilitiesFinder(caps));
        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT y.SmallA.StringKey FROM y.SmallA WHERE CONVERT(y.SmallA.IntValue, integer) = 13", new List<?>[] {Arrays.asList("a")});
        dataManager.addData("DELETE FROM y.SmallA WHERE y.SmallA.StringKey = 'a'", new List<?>[] {Arrays.asList(1)});
        TestProcessor.helpProcess(plan, dataManager, new List<?>[] {Arrays.asList(1)});

        plan = TestProcessor.helpGetPlan("Update ViewA Set y='b' WHERE x=12", metadata, new DefaultCapabilitiesFinder(caps));
        dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT y.SmallA.StringKey FROM y.SmallA WHERE CONVERT(y.SmallA.IntValue, integer) = 12", new List<?>[] {Arrays.asList("a")});
        dataManager.addData("UPDATE y.SmallA SET StringKey = 'b' WHERE y.SmallA.StringKey = 'a'", new List<?>[] {Arrays.asList(1)});
        TestProcessor.helpProcess(plan, dataManager, new List<?>[] {Arrays.asList(1)});
    }

    @Test(expected=QueryValidatorException.class) public void testDeleteWithoutPrimaryKey() throws Exception {
        String ddl = "CREATE FOREIGN TABLE smalla_source(\n" +
                "        charvalue string OPTIONS (NATIVE_TYPE 'STRING'),\n" +
                "        intkey integer OPTIONS (NATIVE_TYPE 'NUMBER')\n" +
                "        ) OPTIONS (UPDATABLE 'TRUE', NAMEINSOURCE 'smalla_${label}');"
                + ""
                + "CREATE VIEW SmallA (IntKey integer,\n" +
                "            CharValue char\n" +
                "            )\n" +
                " \n" +
                "            OPTIONS (UPDATABLE 'TRUE')\n" +
                "        AS\n" +
                "        SELECT\n" +
                "          intkey, convert(charvalue, char)\n" +
                "        FROM\n" +
                "          smalla_source;";

        TransformationMetadata metadata = RealMetadataFactory.fromDDL(ddl, "x", "y");

        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);

        TestProcessor.helpGetPlan(helpParse("DELETE FROM smalla WHERE CharValue IN ('2', '3')"), metadata, new DefaultCapabilitiesFinder(caps), createCommandContext());
    }

}
