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

import static org.teiid.query.processor.TestProcessor.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.teiid.adminapi.DataPolicy;
import org.teiid.adminapi.impl.DataPolicyMetadata;
import org.teiid.adminapi.impl.DataPolicyMetadata.PermissionMetaData;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.util.CommandContext;

@SuppressWarnings({"nls", "unchecked"})
public class TestColumnMasking {

    CommandContext context;

    @Before public void setup() {
        context = createContext();
    }

    private static CommandContext createContext() {
        CommandContext context = createCommandContext();
        DQPWorkContext workContext = new DQPWorkContext();
        HashMap<String, DataPolicy> policies = new HashMap<String, DataPolicy>();
        DataPolicyMetadata policy = new DataPolicyMetadata();
        PermissionMetaData pmd = new PermissionMetaData();
        pmd.setResourceName("pm1.sp1.e1");
        pmd.setMask("case when e2 > 1 then null else e1 end");

        PermissionMetaData pmd1 = new PermissionMetaData();
        pmd1.setResourceName("pm1.g1.e2");
        pmd1.setMask("case when e1 = 'a' then null else e2 end");

        policy.addPermission(pmd, pmd1);
        policy.setName("some-role");
        policies.put("some-role", policy);

        workContext.setPolicies(policies);
        context.setDQPWorkContext(workContext);
        return context;
    }

    @Test public void testProcedureMask() throws Exception {
        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("EXEC pm1.sp1()", new List<?>[] {Arrays.asList("a", 1), Arrays.asList("b", 2)});
        ProcessorPlan plan = helpGetPlan(helpParse("exec pm1.sp1()"), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(), context);
        List<?>[] expectedResults = new List<?>[] {Arrays.asList("a", 1), Arrays.asList(null, 2)};
        helpProcess(plan, context, dataManager, expectedResults);
    }

    @Test public void testProcedureMask1() throws Exception {
        DataPolicyMetadata policy1 = new DataPolicyMetadata();
        PermissionMetaData pmd11 = new PermissionMetaData();
        pmd11.setResourceName("pm1.sp1.e1");
        pmd11.setOrder(1); //takes presedence
        pmd11.setMask("null");

        policy1.addPermission(pmd11);
        policy1.setName("other-role");
        context.getAllowedDataPolicies().put("other-role", policy1);

        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("EXEC pm1.sp1()", new List<?>[] {Arrays.asList("a", 1), Arrays.asList("b", 2)});
        ProcessorPlan plan = helpGetPlan(helpParse("exec pm1.sp1()"), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(), context);
        List<?>[] expectedResults = new List<?>[] {Arrays.asList(null, 1), Arrays.asList(null, 2)};
        helpProcess(plan, context, dataManager, expectedResults);
    }

    @Test public void testTableMask() throws Exception {
        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1", new List<?>[] {Arrays.asList("a", 1), Arrays.asList("b", 2)});
        ProcessorPlan plan = helpGetPlan(helpParse("select e2 from pm1.g1"), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(), context);
        List<?>[] expectedResults = new List<?>[] {Collections.singletonList(null), Arrays.asList(2)};
        helpProcess(plan, context, dataManager, expectedResults);
    }

    @Test(expected=QueryMetadataException.class) public void testInvalidTableMask() throws Exception {
        DataPolicyMetadata policy1 = new DataPolicyMetadata();
        PermissionMetaData pmd11 = new PermissionMetaData();
        pmd11.setResourceName("pm1.g1.e2");
        pmd11.setOrder(1); //takes presedence
        pmd11.setMask("'a'");

        policy1.addPermission(pmd11);
        policy1.setName("other-role");
        context.getAllowedDataPolicies().put("other-role", policy1);

        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1", new List<?>[] {Arrays.asList("a", 1), Arrays.asList("b", 2)});
        ProcessorPlan plan = helpGetPlan(helpParse("select e2 from pm1.g1"), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(), context);
        List<?>[] expectedResults = new List<?>[] {Collections.singletonList(null), Arrays.asList(2)};
        helpProcess(plan, context, dataManager, expectedResults);
    }

    @Test public void testTableAliasMask() throws Exception {
        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1", new List<?>[] {Arrays.asList("a", 1), Arrays.asList("b", 2)});
        ProcessorPlan plan = helpGetPlan(helpParse("select g2.e2 from pm1.g1 as g2"), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(), context);
        List<?>[] expectedResults = new List<?>[] {Collections.singletonList(null), Arrays.asList(2)};
        helpProcess(plan, context, dataManager, expectedResults);
    }

    @Test(expected=QueryPlannerException.class) public void testSubqueryTableMaskRecursive() throws Exception {
        DataPolicyMetadata policy1 = new DataPolicyMetadata();
        PermissionMetaData pmd11 = new PermissionMetaData();
        pmd11.setResourceName("pm1.g1.e2");
        pmd11.setOrder(1); //takes presedence
        pmd11.setMask("(select min(e2) from pm1.g1)");

        policy1.addPermission(pmd11);
        policy1.setName("other-role");
        context.getAllowedDataPolicies().put("other-role", policy1);

        HardcodedDataManager dataManager = new HardcodedDataManager();
        ProcessorPlan plan = helpGetPlan(helpParse("select g2.e2 from pm1.g1 as g2"), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(), context);
        helpProcess(plan, context, dataManager, null);
    }

    @Test public void testSubqueryTableMask() throws Exception {
        DataPolicyMetadata policy1 = new DataPolicyMetadata();
        PermissionMetaData pmd11 = new PermissionMetaData();
        pmd11.setResourceName("pm1.g1.e2");
        pmd11.setOrder(1); //takes presedence
        pmd11.setMask("(select min(e2) from pm1.g3)");

        policy1.addPermission(pmd11);
        policy1.setName("other-role");
        context.getAllowedDataPolicies().put("other-role", policy1);

        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT pm1.g1.e1 FROM pm1.g1", new List<?>[] {Arrays.asList("a"), Arrays.asList("b")});
        dataManager.addData("SELECT pm1.g3.e2 FROM pm1.g3", new List<?>[] {Arrays.asList(1), Arrays.asList(2)});
        ProcessorPlan plan = helpGetPlan(helpParse("select e1, g2.e2 from pm1.g1 as g2"), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(), context);
        List<?>[] expectedResults = new List<?>[] {Arrays.asList("a", 1), Arrays.asList("b", 1)};
        helpProcess(plan, context, dataManager, expectedResults);
    }

    @Test public void testMultipleTableMaskWithPreservedProjection() throws Exception {
        DataPolicyMetadata policy1 = new DataPolicyMetadata();
        PermissionMetaData pmd11 = new PermissionMetaData();
        pmd11.setResourceName("pm1.g1.e3");
        pmd11.setMask("rand() > .5");

        policy1.addPermission(pmd11);
        policy1.setName("other-role");
        context.getAllowedDataPolicies().put("other-role", policy1);

        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1", new List<?>[] {Arrays.asList("a", 1), Arrays.asList("b", 2)});
        ProcessorPlan plan = helpGetPlan(helpParse("select e2 from pm1.g1"), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(), context);
        List<?>[] expectedResults = new List<?>[] {Collections.singletonList(null), Arrays.asList(2)};
        helpProcess(plan, context, dataManager, expectedResults);
    }

    @Test public void testColumnSubstitution() throws Exception {
        DataPolicyMetadata policy1 = new DataPolicyMetadata();
        PermissionMetaData pmd11 = new PermissionMetaData();
        pmd11.setResourceName("vm1.g15.x");
        pmd11.setMask("e1");

        policy1.addPermission(pmd11);
        policy1.setName("other-role");
        context.getAllowedDataPolicies().put("other-role", policy1);

        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT pm3.g1.e1 FROM pm3.g1", new List<?>[] {Arrays.asList("a"), Arrays.asList("b")});
        ProcessorPlan plan = helpGetPlan(helpParse("select * from vm1.g15"), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(), context);
        List<?>[] expectedResults = new List<?>[] {Arrays.asList("a", "a"), Arrays.asList("b", "b")};
        helpProcess(plan, context, dataManager, expectedResults);
    }

    @Test public void testSubqueryProcedureMask() throws Exception {
        DataPolicyMetadata policy1 = new DataPolicyMetadata();
        PermissionMetaData pmd11 = new PermissionMetaData();
        pmd11.setResourceName("pm1.sp1.e2");
        pmd11.setOrder(1); //takes presedence
        pmd11.setMask("(select min(e2) from pm1.g3 where e1 = pm1.sp1.e2)");

        policy1.addPermission(pmd11);
        policy1.setName("other-role");
        context.getAllowedDataPolicies().put("other-role", policy1);

        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("EXEC pm1.sp1()", new List<?>[] {Arrays.asList("a", 1), Arrays.asList("b", 2)});
        dataManager.addData("SELECT pm1.g3.e1, pm1.g3.e2 FROM pm1.g3", new List<?>[] {Arrays.asList("1", 0), Arrays.asList("2", -1)});
        ProcessorPlan plan = helpGetPlan(helpParse("exec pm1.sp1()"), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(), context);
        List<?>[] expectedResults = new List<?>[] {Arrays.asList("a", 0), Arrays.asList(null, -1)};
        helpProcess(plan, context, dataManager, expectedResults);
    }

    @Test public void testViewMask() throws Exception {
        DataPolicyMetadata policy1 = new DataPolicyMetadata();
        PermissionMetaData pmd11 = new PermissionMetaData();
        pmd11.setResourceName("vm1.g1.e2");
        pmd11.setMask("null");

        policy1.addPermission(pmd11);
        policy1.setName("other-role");
        context.getAllowedDataPolicies().put("other-role", policy1);

        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT pm1.g1.e1 FROM pm1.g1", new List<?>[] {Arrays.asList("a", 1), Arrays.asList("b", 2)});
        ProcessorPlan plan = helpGetPlan(helpParse("select g2.e2 from vm1.g1 as g2"), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(), context);
        List<?>[] expectedResults = new List<?>[] {Collections.singletonList(null), Collections.singletonList(null)};
        helpProcess(plan, context, dataManager, expectedResults);
    }

    @Test(expected=QueryMetadataException.class) public void testWindowFunctionViewMask() throws Exception {
        DataPolicyMetadata policy1 = new DataPolicyMetadata();
        PermissionMetaData pmd11 = new PermissionMetaData();
        pmd11.setResourceName("vm1.g1.e2");
        pmd11.setMask("min(e2) over ()");

        policy1.addPermission(pmd11);
        policy1.setName("other-role");
        context.getAllowedDataPolicies().put("other-role", policy1);

        HardcodedDataManager dataManager = new HardcodedDataManager();
        ProcessorPlan plan = helpGetPlan(helpParse("select g2.e2 from vm1.g1 as g2"), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(), context);
        helpProcess(plan, context, dataManager, null);
    }

    @Test public void testViewMaskWithRowFilter() throws Exception {
        DataPolicyMetadata policy1 = new DataPolicyMetadata();
        PermissionMetaData pmd11 = new PermissionMetaData();
        pmd11.setResourceName("vm1.g1.e2");
        pmd11.setMask("null");

        PermissionMetaData pmd12 = new PermissionMetaData();
        pmd12.setResourceName("vm1.g1");
        pmd12.setCondition("e2 = 1"); //should be applied before the mask affect, otherwise we'd get no rows

        policy1.addPermission(pmd11, pmd12);
        policy1.setName("other-role");
        context.getAllowedDataPolicies().put("other-role", policy1);

        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1", new List<?>[] {Arrays.asList("a", 1), Arrays.asList("b", 1)});
        ProcessorPlan plan = helpGetPlan(helpParse("select g2.e2 from vm1.g1 as g2"), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(), context);
        List<?>[] expectedResults = new List<?>[] {Collections.singletonList(null)};
        helpProcess(plan, context, dataManager, expectedResults);
    }

   @Test public void testAliasedMask() throws Exception {
        DataPolicyMetadata policy1 = new DataPolicyMetadata();
        PermissionMetaData pmd11 = new PermissionMetaData();
        pmd11.setResourceName("vm1.g1.e2");
        pmd11.setCondition("vm1.g1.e1 > 1");
        pmd11.setMask("null");

        policy1.addPermission(pmd11);
        policy1.setName("other-role");
        context.getAllowedDataPolicies().put("other-role", policy1);

        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1", new List<?>[] {Arrays.asList("a", 1), Arrays.asList("b", 1)});

        TransformationMetadata tm = RealMetadataFactory.fromDDL("create view g2 as select 1 as e1, 2 as e2 union select 2 as e1, 3 as e2; create view g1 as select e1, e2 from g2 order by e1 limit 5;", "x", "vm1");

        ProcessorPlan plan = helpGetPlan(helpParse("select e1, g2.e2 from vm1.g1 as g2 order by e1"), tm, new DefaultCapabilitiesFinder(), context);
        List<?>[] expectedResults = new List<?>[] {Arrays.asList(1,2), Arrays.asList(2, null)};
        helpProcess(plan, context, dataManager, expectedResults);
    }

   @Test public void testAliasedMask1() throws Exception {
       DataPolicyMetadata policy1 = new DataPolicyMetadata();
       PermissionMetaData pmd11 = new PermissionMetaData();
       pmd11.setResourceName("vm1.g1.e2");
       pmd11.setCondition("vm1.g1.e1 > 1");
       pmd11.setMask("null");

       policy1.addPermission(pmd11);
       policy1.setName("other-role");
       context.getAllowedDataPolicies().put("other-role", policy1);

       HardcodedDataManager dataManager = new HardcodedDataManager();
       dataManager.addData("SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1", new List<?>[] {Arrays.asList("a", 1), Arrays.asList("b", 1)});

       TransformationMetadata tm = RealMetadataFactory.fromDDL("create view g1 as select 1 as e1, 2 as e2 union select 2 as e1, 3 as e2;", "x", "vm1");

       ProcessorPlan plan = helpGetPlan(helpParse("select e1, g2.e2 from vm1.g1 as g2 order by e1"), tm, new DefaultCapabilitiesFinder(), context);
       List<?>[] expectedResults = new List<?>[] {Arrays.asList(1,2), Arrays.asList(2, null)};
       helpProcess(plan, context, dataManager, expectedResults);
   }

   /**
    * Similar test to the above, because the output name is not reset
    * it initially appeared as if we were not recontexting the expression,
    * but this works as expected.
    * @throws Exception
    */
   @Test public void testAliasedFilter() throws Exception {
       DataPolicyMetadata policy1 = new DataPolicyMetadata();
       PermissionMetaData pmd11 = new PermissionMetaData();
       pmd11.setResourceName("vm1.g1");
       pmd11.setCondition("vm1.g1.e1 > 1");

       policy1.addPermission(pmd11);
       policy1.setName("other-role");
       context.getAllowedDataPolicies().put("other-role", policy1);

       HardcodedDataManager dataManager = new HardcodedDataManager();
       dataManager.addData("SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1", new List<?>[] {Arrays.asList("a", 1), Arrays.asList("b", 1)});

       TransformationMetadata tm = RealMetadataFactory.fromDDL("create view g1 as select 1 as e1, 2 as e2 union select 2 as e1, 3 as e2;", "x", "vm1");

       ProcessorPlan plan = helpGetPlan(helpParse("select e1, g2.e2 from vm1.g1 as g2 order by e1"), tm, new DefaultCapabilitiesFinder(), context);
       List<?>[] expectedResults = new List<?>[] {Arrays.asList(2, 3)};
       helpProcess(plan, context, dataManager, expectedResults);
   }

    @Test public void testConditionalMask() throws Exception {
        DataPolicyMetadata policy1 = new DataPolicyMetadata();
        PermissionMetaData pmd11 = new PermissionMetaData();
        pmd11.setResourceName("pm1.g1.e2");
        pmd11.setOrder(1); //takes presedence
        pmd11.setCondition("e1 = 'c'");
        pmd11.setMask("0");

        policy1.addPermission(pmd11);
        policy1.setName("other-role");
        context.getAllowedDataPolicies().put("other-role", policy1);

        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1", new List<?>[] {Arrays.asList("a", 1), Arrays.asList("b", 2), Arrays.asList("c", 0)});
        ProcessorPlan plan = helpGetPlan(helpParse("select g2.e2 from pm1.g1 as g2"), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(), context);
        List<?>[] expectedResults = new List<?>[] {Collections.singletonList(null), Arrays.asList(2), Arrays.asList(0)};
        helpProcess(plan, context, dataManager, expectedResults);
    }

}
