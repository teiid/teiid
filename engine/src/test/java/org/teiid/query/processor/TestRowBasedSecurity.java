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

import static org.teiid.query.processor.TestProcessor.createCommandContext;
import static org.teiid.query.processor.TestProcessor.helpGetPlan;
import static org.teiid.query.processor.TestProcessor.helpParse;
import static org.teiid.query.processor.TestProcessor.helpProcess;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.teiid.adminapi.DataPolicy;
import org.teiid.adminapi.DataPolicy.ResourceType;
import org.teiid.adminapi.impl.DataPolicyMetadata;
import org.teiid.adminapi.impl.DataPolicyMetadata.PermissionMetaData;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.api.exception.query.QueryProcessingException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.util.CommandContext;

@SuppressWarnings({"nls", "unchecked"})
public class TestRowBasedSecurity {

    CommandContext context;
    private static PermissionMetaData pmd;

    @Before public void setup() {
        context = createContext();
    }

    private static CommandContext createContext() {
        CommandContext context = createCommandContext();
        DQPWorkContext workContext = new DQPWorkContext();
        HashMap<String, DataPolicy> policies = new HashMap<String, DataPolicy>();
        DataPolicyMetadata policy = new DataPolicyMetadata();
        pmd = new PermissionMetaData();
        pmd.setResourceName("pm1.g1");
        pmd.setCondition("e1 = user()");

        PermissionMetaData pmd1 = new PermissionMetaData();
        pmd1.setResourceName("pm1.g2");
        pmd1.setCondition("foo = bar");

        PermissionMetaData pmd2 = new PermissionMetaData();
        pmd2.setResourceName("pm1.g4");
        pmd2.setCondition("e1 = max(e2)");

        PermissionMetaData pmd3 = new PermissionMetaData();
        pmd3.setResourceName("pm1.g3");
        pmd3.setAllowDelete(true);

        PermissionMetaData pmd4 = new PermissionMetaData();
        pmd4.setResourceType(ResourceType.PROCEDURE);
        pmd4.setResourceName("pm1.sp1");
        pmd4.setCondition("e1 = 'a'");

        policy.addPermission(pmd, pmd1, pmd2, pmd3, pmd4);
        policy.setName("some-role");
        policies.put("some-role", policy);

        workContext.setPolicies(policies);
        context.setDQPWorkContext(workContext);
        return context;
    }

    @Test public void testSelectFilter() throws Exception {
        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1", new List<?>[] {Arrays.asList("a", 1), Arrays.asList("b", 2)});
        ProcessorPlan plan = helpGetPlan(helpParse("select e2 from pm1.g1"), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(), context);
        List<?>[] expectedResults = new List<?>[0];
        helpProcess(plan, context, dataManager, expectedResults);
    }

    /**
     * Note that we create an inline view to keep the proper position of the filter
     */
    @Test public void testSelectFilterOuterJoin() throws Exception {
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER_FULL, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_INLINE_VIEWS, true);
        ProcessorPlan plan = helpGetPlan(helpParse("SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1 full outer join pm1.g3 on (pm1.g1.e1=pm1.g3.e1)"), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(caps), context);
        TestOptimizer.checkAtomicQueries(new String[] {"SELECT v_0.c_0, v_0.c_1 FROM (SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm1.g1 AS g_0 WHERE g_0.e1 = 'user') AS v_0 FULL OUTER JOIN pm1.g3 AS g_1 ON v_0.c_0 = g_1.e1"}, plan);
    }

    @Test public void testSelectFilterOuterJoin1() throws Exception {
        TransformationMetadata tm = RealMetadataFactory.fromDDL("create foreign table t (x string, y integer); create foreign table t1 (x string, y integer); create view v as select t.x, t1.y from t left outer join t1 on t.y = t1.y", "x", "y");
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, false);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, false);
        caps.setCapabilitySupport(Capability.QUERY_FROM_INLINE_VIEWS, false);

        CommandContext context = createCommandContext();
        DQPWorkContext workContext = new DQPWorkContext();
        HashMap<String, DataPolicy> policies = new HashMap<String, DataPolicy>();
        DataPolicyMetadata policy = new DataPolicyMetadata();
        pmd = new PermissionMetaData();
        pmd.setResourceName("y.v");
        pmd.setCondition("x = user()");

        policy.addPermission(pmd);
        policy.setName("some-role");
        policies.put("some-role", policy);

        workContext.setPolicies(policies);
        context.setDQPWorkContext(workContext);

        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT g_0.y AS c_0, g_0.x AS c_1 FROM y.t AS g_0 ORDER BY c_0", new List<?>[] {Arrays.asList(1, "a"), Arrays.asList(2, "b")});
        dataManager.addData("SELECT g_0.y AS c_0 FROM y.t1 AS g_0 ORDER BY c_0", new List<?>[] {Arrays.asList(1)});

        ProcessorPlan plan = helpGetPlan(helpParse("select count(1) from v"), tm, new DefaultCapabilitiesFinder(caps), context);
        List<?>[] expectedResults = new List<?>[] {Arrays.asList(0)};
        helpProcess(plan, context, dataManager, expectedResults);

        plan = helpGetPlan(helpParse("select count(1) from v where y is not null"), tm, new DefaultCapabilitiesFinder(caps), context);
        dataManager.addData("SELECT g_0.y FROM y.t AS g_0 WHERE (g_0.x = 'user') AND (g_0.y = 1)", new List<?>[] {Arrays.asList(1)});
        dataManager.addData("SELECT g_0.y AS c_0 FROM y.t1 AS g_0 WHERE g_0.y IS NOT NULL ORDER BY c_0", Arrays.asList(1));
        expectedResults = new List<?>[] {Arrays.asList(1)};
        helpProcess(plan, context, dataManager, expectedResults);
    }

    /**
     * Same as above, but ensures it's still in effect under a proceudre
     */
    @Test public void testTransitiveFilter() throws Exception {
        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1", new List<?>[] {Arrays.asList("a", 1), Arrays.asList("b", 2)});
        dataManager.addData("exec pm1.sq1()", new List<?>[] {Arrays.asList("a", 1), Arrays.asList("b", 2)});
        ProcessorPlan plan = helpGetPlan(helpParse("exec pm1.sq1()"), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(), context);
        List<?>[] expectedResults = new List<?>[0];
        helpProcess(plan, context, dataManager, expectedResults);
    }

    /**
     * restricted to e1 = a
     */
    @Test public void testProcedureFilter() throws Exception {
        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("EXEC pm1.sp1()", new List<?>[] {Arrays.asList("a", 1), Arrays.asList("b", 2)});
        ProcessorPlan plan = helpGetPlan(helpParse("exec pm1.sp1()"), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(), context);
        List<?>[] expectedResults = new List<?>[] {Arrays.asList("a", 1)};
        helpProcess(plan, context, dataManager, expectedResults);
    }

    @Test public void testProcedureRelationalFilter() throws Exception {
        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("EXEC pm1.sp1()", new List<?>[] {Arrays.asList("a", 1), Arrays.asList("b", 2)});
        ProcessorPlan plan = helpGetPlan(helpParse("select * from pm1.sp1"), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(), context);
        List<?>[] expectedResults = new List<?>[] {Arrays.asList("a", 1)};
        helpProcess(plan, context, dataManager, expectedResults);
    }

    /**
     * Shouldn't even execute as 'user' <> 'a'
     */
    @Test public void testDeleteFilter() throws Exception {
        HardcodedDataManager dataManager = new HardcodedDataManager();
        ProcessorPlan plan = helpGetPlan(helpParse("delete from pm1.g1 where e1 = 'a'"), RealMetadataFactory.example1Cached(), TestOptimizer.getGenericFinder(), context);
        List<?>[] expectedResults = new List<?>[] {Arrays.asList(0)};
        helpProcess(plan, context, dataManager, expectedResults);
    }

    @Test public void testInsertDisabledConstraint() throws Exception {
        pmd.setConstraint(false);
        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("INSERT INTO pm1.g1 (e1) VALUES ('a')", new List<?>[] {Arrays.asList(1)});
        ProcessorPlan plan = helpGetPlan(helpParse("insert into pm1.g1 (e1) values ('a')"), RealMetadataFactory.example1Cached(), TestOptimizer.getGenericFinder(), context);
        List<?>[] expectedResults = new List<?>[] {Arrays.asList(1)};
        helpProcess(plan, context, dataManager, expectedResults);
    }

    /**
     * invalid insert value
     */
    @Test(expected=QueryPlannerException.class) public void testInsertConstraint() throws Exception {
        HardcodedDataManager dataManager = new HardcodedDataManager();
        ProcessorPlan plan = helpGetPlan(helpParse("insert into pm1.g1 (e1) values ('a')"), RealMetadataFactory.example1Cached(), TestOptimizer.getGenericFinder(), context);
        helpProcess(plan, context, dataManager, null);
    }

    /**
     * Assumes the null value for e1, which results in a violation
     */
    @Test(expected=QueryPlannerException.class) public void testInsertConstraint1() throws Exception {
        HardcodedDataManager dataManager = new HardcodedDataManager();
        ProcessorPlan plan = helpGetPlan(helpParse("insert into pm1.g1 (e2) values (1)"), RealMetadataFactory.example1Cached(), TestOptimizer.getGenericFinder(), context);
        helpProcess(plan, context, dataManager, null);
    }

    @Test(expected=TeiidProcessingException.class) public void testInsertConstraintCorrelatedSubquery() throws Exception {
        DataPolicyMetadata policy1 = new DataPolicyMetadata();
        PermissionMetaData pmd3 = new PermissionMetaData();
        pmd3.setResourceName("pm1.g1");
        pmd3.setCondition("e1 = (select min(e1) from pm1.g3 where pm1.g1.e2 = e2)");
        policy1.addPermission(pmd3);
        policy1.setName("some-other-role");
        context.getAllowedDataPolicies().put("some-other-role", policy1);

        HardcodedDataManager dataManager = new HardcodedDataManager();
        ProcessorPlan plan = helpGetPlan(helpParse("insert into pm1.g1 (e1, e2) values ('a', 1)"), RealMetadataFactory.example1Cached(), TestOptimizer.getGenericFinder(), context);
        List<?>[] expectedResults = new List<?>[] {Arrays.asList(0)};
        helpProcess(plan, context, dataManager, expectedResults);
    }

    @Test public void testInsertConstraintSubquery() throws Exception {
        DataPolicyMetadata policy1 = new DataPolicyMetadata();
        PermissionMetaData pmd3 = new PermissionMetaData();
        pmd3.setResourceName("pm1.g1");
        pmd3.setCondition("e1 = (select min(e1) from pm1.g3)");
        policy1.addPermission(pmd3);
        policy1.setName("some-other-role");
        context.getAllowedDataPolicies().put("some-other-role", policy1);

        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT g_0.e1 FROM pm1.g3 AS g_0", new List<?>[] {Arrays.asList("a"), Arrays.asList("b")});
        dataManager.addData("INSERT INTO pm1.g1 (e1, e2) VALUES ('a', 1)", new List<?>[] {Arrays.asList(1)});
        ProcessorPlan plan = helpGetPlan(helpParse("insert into pm1.g1 (e1, e2) values ('a', 1)"), RealMetadataFactory.example1Cached(), TestOptimizer.getGenericFinder(), context);
        List<?>[] expectedResults = new List<?>[] {Arrays.asList(1)};
        helpProcess(plan, context, dataManager, expectedResults);
    }

    /**
     * should fail since it doesn't match the condition
     */
    @Test(expected=QueryProcessingException.class) public void testInsertConstraintWithQueryExpression() throws Exception {
        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT pm1.g3.e1 FROM pm1.g3", new List<?>[] {Arrays.asList("a"), Arrays.asList("b")});
        BasicSourceCapabilities bsc = new BasicSourceCapabilities();
        bsc.setCapabilitySupport(Capability.INSERT_WITH_QUERYEXPRESSION, true);
        DefaultCapabilitiesFinder capFinder = new DefaultCapabilitiesFinder(bsc);
        ProcessorPlan plan = helpGetPlan(helpParse("insert into pm1.g1 (e1) select e1 from pm1.g3"), RealMetadataFactory.example1Cached(), capFinder, context);
        List<?>[] expectedResults = new List<?>[] {Arrays.asList(0)};
        helpProcess(plan, context, dataManager, expectedResults);
    }

    @Test public void testInsertConstraintAutoIncrement() throws Exception {
        HardcodedDataManager dataManager = new HardcodedDataManager();
        TransformationMetadata tm = RealMetadataFactory.fromDDL("create foreign table g1 (e1 string, e2 integer not null auto_increment) options (updatable true)", "x", "pm1");
        ProcessorPlan plan = helpGetPlan(helpParse("insert into pm1.g1 (e1) values ('user')"), tm, TestOptimizer.getGenericFinder(), context);
        dataManager.addData("INSERT INTO pm1.g1 (e1) VALUES ('user')", new List<?>[] {Arrays.asList(1)});
        helpProcess(plan, context, dataManager, null);
    }

    @Test(expected=QueryPlannerException.class) public void testInsertConstraintAutoIncrement1() throws Exception {
        HardcodedDataManager dataManager = new HardcodedDataManager();
        TransformationMetadata tm = RealMetadataFactory.fromDDL("create foreign table g1 (e1 string not null auto_increment, e2 integer) options (updatable true)", "x", "pm1");
        ProcessorPlan plan = helpGetPlan(helpParse("insert into pm1.g1 (e2) values (1)"), tm, TestOptimizer.getGenericFinder(), context);
        helpProcess(plan, context, dataManager, null);
    }

    /**
     * should succeed since it matches the condition
     */
    @Test public void testInsertConstraintWithQueryExpression1() throws Exception {
        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT pm1.g3.e1 FROM pm1.g3", new List<?>[] {Arrays.asList("a")});
        dataManager.addData("INSERT INTO pm1.g1 (e1) VALUES ('user')", new List<?>[] {Arrays.asList(1)});
        BasicSourceCapabilities bsc = new BasicSourceCapabilities();
        bsc.setCapabilitySupport(Capability.INSERT_WITH_QUERYEXPRESSION, true);
        DefaultCapabilitiesFinder capFinder = new DefaultCapabilitiesFinder(bsc);
        ProcessorPlan plan = helpGetPlan(helpParse("insert into pm1.g1 (e1) select user() from pm1.g3"), RealMetadataFactory.example1Cached(), capFinder, context);
        List<?>[] expectedResults = new List<?>[] {Arrays.asList(1)};
        helpProcess(plan, context, dataManager, expectedResults);
    }

    @Test public void testInsertFilter1() throws Exception {
        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("INSERT INTO pm1.g1 (e1) VALUES ('user')", new List<?>[] {Arrays.asList(1)});
        ProcessorPlan plan = helpGetPlan(helpParse("insert into pm1.g1 (e1) values ('user')"), RealMetadataFactory.example1Cached(), TestOptimizer.getGenericFinder(), context);
        List<?>[] expectedResults = new List<?>[] {Arrays.asList(1)};
        helpProcess(plan, context, dataManager, expectedResults);
    }

    /**
     * not a valid value for e1
     */
    @Test(expected=QueryPlannerException.class) public void testUpdateFilter() throws Exception {
        HardcodedDataManager dataManager = new HardcodedDataManager();
        ProcessorPlan plan = helpGetPlan(helpParse("update pm1.g1 set e1 = 'a' where e2 = 5"), RealMetadataFactory.example1Cached(), TestOptimizer.getGenericFinder(), context);
        List<?>[] expectedResults = new List<?>[] {Arrays.asList(0)};
        helpProcess(plan, context, dataManager, expectedResults);
    }

    /**
     * no primary key for compensation
     */
    @Test(expected=QueryPlannerException.class) public void testUpdateFilter1() throws Exception {
        HardcodedDataManager dataManager = new HardcodedDataManager();
        ProcessorPlan plan = helpGetPlan(helpParse("update pm1.g1 set e1 = e3 where e2 = 5"), RealMetadataFactory.example1Cached(), TestOptimizer.getGenericFinder(), context);
        List<?>[] expectedResults = new List<?>[] {Arrays.asList(0)};
        helpProcess(plan, context, dataManager, expectedResults);
    }

    @Test(expected=QueryProcessingException.class) public void testUpdateFilter2() throws Exception {
        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT g_0.e3, g_0.e1 FROM pm1.g1 AS g_0 WHERE (g_0.e1 = 'user') AND (g_0.e2 = 5)", new List<?>[] {Arrays.asList(Boolean.TRUE, "user")});
        ProcessorPlan plan = helpGetPlan(helpParse("update pm1.g1 set e1 = e3 || 'r' where e2 = 5"), RealMetadataFactory.example4(), TestOptimizer.getGenericFinder(), context);
        List<?>[] expectedResults = new List<?>[] {Arrays.asList(0)};
        helpProcess(plan, context, dataManager, expectedResults);
    }

    /**
     * Ensures that the filter still gets applied to the insert
     */
    @Test public void testUpdateFilter3() throws Exception {
        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("UPDATE pm1.g1 SET e2 = 1 WHERE (e2 = 5) AND (e1 = 'user')", new List<?>[] {Arrays.asList(1)});
        ProcessorPlan plan = helpGetPlan(helpParse("update pm1.g1 set e2 = 1 where e2 = 5"), RealMetadataFactory.example4(), TestOptimizer.getGenericFinder(), context);
        List<?>[] expectedResults = new List<?>[] {Arrays.asList(1)};
        helpProcess(plan, context, dataManager, expectedResults);
    }

    /**
     * Tests an outside column in the constraint
     */
    @Test public void testUpdateFilter4() throws Exception {
        DataPolicyMetadata policy1 = new DataPolicyMetadata();
        PermissionMetaData pmd3 = new PermissionMetaData();
        pmd3.setResourceName("pm1.g1");
        pmd3.setCondition("e2 = 1 and e3");
        policy1.addPermission(pmd3);
        policy1.setName("some-role");
        context.getAllowedDataPolicies().put("some-role", policy1);

        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT g_0.e4, g_0.e3, g_0.e1 FROM pm1.g1 AS g_0 WHERE (g_0.e3 = TRUE) AND (g_0.e2 = 1) AND (g_0.e1 IN ('a', 'b'))", new List<?>[] {Arrays.asList(Double.valueOf(1), Boolean.TRUE, "a"), Arrays.asList(Double.valueOf(1), Boolean.TRUE, "b")});
        dataManager.addData("UPDATE pm1.g1 SET e2 = 1 WHERE pm1.g1.e1 = 'a'", new List<?>[] {Arrays.asList(1)});
        dataManager.addData("UPDATE pm1.g1 SET e2 = 1 WHERE pm1.g1.e1 = 'b'", new List<?>[] {Arrays.asList(1)});
        ProcessorPlan plan = helpGetPlan(helpParse("update pm1.g1 set e2 = case when e4 = 1 then 1 else 2 end where e1 in ('a', 'b')"), RealMetadataFactory.example4(), TestOptimizer.getGenericFinder(), context);
        List<?>[] expectedResults = new List<?>[] {Arrays.asList(2)};
        helpProcess(plan, context, dataManager, expectedResults);
    }

    //TODO: should add validation prior to queries being run
    @Test(expected=QueryMetadataException.class) public void testBadFilter() throws Exception {
        HardcodedDataManager dataManager = new HardcodedDataManager();
        ProcessorPlan plan = helpGetPlan(helpParse("select e2 from pm1.g2"), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(), context);
        List<?>[] expectedResults = new List<?>[0];
        helpProcess(plan, context, dataManager, expectedResults);
    }

    @Test(expected=QueryMetadataException.class) public void testBadFilter1() throws Exception {
        HardcodedDataManager dataManager = new HardcodedDataManager();
        ProcessorPlan plan = helpGetPlan(helpParse("select * from pm1.g4"), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(), context);
        List<?>[] expectedResults = new List<?>[0];
        helpProcess(plan, context, dataManager, expectedResults);
    }

    /**
     * Here the other role makes the g1 rows visible again
     */
    @Test public void testMultipleRoles() throws Exception {
        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1", new List<?>[] {Arrays.asList("a", 1), Arrays.asList("b", 2)});
        ProcessorPlan plan = helpGetPlan(helpParse("select e2 from pm1.g1"), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(), context);
        helpProcess(plan, context, dataManager, new List<?>[0]);

        DataPolicyMetadata policy1 = new DataPolicyMetadata();
        PermissionMetaData pmd3 = new PermissionMetaData();
        pmd3.setResourceName("pm1.g1");
        pmd3.setCondition("true");
        policy1.addPermission(pmd3);
        policy1.setName("some-other-role");
        context.getAllowedDataPolicies().put("some-other-role", policy1);

        dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT pm1.g1.e2 FROM pm1.g1", new List<?>[] {Arrays.asList(1), Arrays.asList(2)});
        plan = helpGetPlan(helpParse("select e2 from pm1.g1"), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(), context);
        List<?>[] expectedResults = new List<?>[] {Arrays.asList(1), Arrays.asList(2)};
        helpProcess(plan, context, dataManager, expectedResults);
    }

    @Test public void testSubqueryHint() throws Exception {
        DataPolicyMetadata policy1 = new DataPolicyMetadata();
        PermissionMetaData pmd3 = new PermissionMetaData();
        pmd3.setResourceName("pm1.g1");
        pmd3.setCondition("e1 in /*+ DJ */ (select e1 from pm1.g3)");
        policy1.addPermission(pmd3);
        policy1.setName("some-other-role");
        context.getAllowedDataPolicies().clear();
        context.getAllowedDataPolicies().put("some-other-role", policy1);

        HardcodedDataManager dataManager = new HardcodedDataManager();

        dataManager.addData("SELECT pm1.g3.e1 FROM pm1.g3", new List<?>[] {Arrays.asList("b"), Arrays.asList("a")});
        dataManager.addData("SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1", new List<?>[] {Arrays.asList("b", 1), Arrays.asList("a", 2)});

        ProcessorPlan plan = helpGetPlan(helpParse("select e1, e2 from pm1.g1"), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(), context);
        List<?>[] expectedResults = new List<?>[] {Arrays.asList("a", 2), Arrays.asList("b", 1)};
        helpProcess(plan, context, dataManager, expectedResults);

        dataManager.addData("SELECT g_0.e1 AS c_0 FROM pm1.g3 AS g_0 ORDER BY c_0", new List<?>[] {Arrays.asList("a"), Arrays.asList("b")});
        dataManager.addData("SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm1.g1 AS g_0 WHERE g_0.e1 IN ('a', 'b') ORDER BY c_0", new List<?>[] {Arrays.asList("a", 2), Arrays.asList("b", 1)});

        plan = helpGetPlan(helpParse("select e1, e2 from pm1.g1"), RealMetadataFactory.example1Cached(), TestOptimizer.getGenericFinder(), context);
        expectedResults = new List<?>[] {Arrays.asList("a", 2), Arrays.asList("b", 1)};
        helpProcess(plan, context, dataManager, expectedResults);
    }

}
