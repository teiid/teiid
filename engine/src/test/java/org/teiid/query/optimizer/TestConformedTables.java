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
import static org.teiid.query.optimizer.TestOptimizer.*;

import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.metadata.Table;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.TestOptimizer.ComparisonMode;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.optimizer.relational.rules.RulePlaceAccess;
import org.teiid.query.processor.relational.AccessNode;
import org.teiid.query.processor.relational.RelationalPlan;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings("nls")
public class TestConformedTables {

    private static TransformationMetadata tm;

    @BeforeClass public static void oneTimeSetup() throws Exception {
        tm = RealMetadataFactory.example1();
        Table t = tm.getGroupID("pm1.g1");
        t.setProperty(RulePlaceAccess.CONFORMED_SOURCES, "pm2");
        t = tm.getGroupID("pm2.g3");
        t.setProperty(RulePlaceAccess.CONFORMED_SOURCES, "pm1");
        t = tm.getGroupID("pm2.g1");
        t.setProperty(RulePlaceAccess.CONFORMED_SOURCES, "pm3");
    }

    @Test public void testConformedJoin() throws Exception {
        String sql = "select pm1.g1.e1 from pm1.g1, pm2.g2 where g1.e1=g2.e1";

        RelationalPlan plan = (RelationalPlan)helpPlan(sql, tm, new String[] {"SELECT g_0.e1 FROM pm1.g1 AS g_0, pm2.g2 AS g_1 WHERE g_0.e1 = g_1.e1"}, ComparisonMode.EXACT_COMMAND_STRING);
        AccessNode anode = (AccessNode) plan.getRootNode();
        assertEquals("pm2", anode.getModelName());

        //it should work either way
        sql = "select pm1.g1.e1 from pm2.g2, pm1.g1 where g1.e1=g2.e1";

        plan = (RelationalPlan)helpPlan(sql, tm, new String[] {"SELECT g_1.e1 FROM pm2.g2 AS g_0, pm1.g1 AS g_1 WHERE g_1.e1 = g_0.e1"}, ComparisonMode.EXACT_COMMAND_STRING);
        anode = (AccessNode) plan.getRootNode();
        assertEquals("pm2", anode.getModelName());
    }

    @Test public void testConformedJoin1() throws Exception {
        String sql = "select pm1.g1.e1 from pm1.g1, pm2.g1 where pm1.g1.e1=pm2.g1.e1";

        helpPlan(sql, tm, new String[] {"SELECT g_0.e1 FROM pm1.g1 AS g_0, pm2.g1 AS g_1 WHERE g_0.e1 = g_1.e1"}, ComparisonMode.EXACT_COMMAND_STRING);
    }

    @Test public void testConformedSubquery() throws Exception {
        String sql = "select pm2.g2.e1 from pm2.g2 where e1 in /*+ no_unnest */ (select e1 from pm1.g1)";

        BasicSourceCapabilities bsc = getTypicalCapabilities();
        bsc.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);

        helpPlan(sql, tm, new String[] {"SELECT g_0.e1 FROM pm2.g2 AS g_0 WHERE g_0.e1 IN /*+ NO_UNNEST */ (SELECT g_1.e1 FROM pm1.g1 AS g_1)"}, new DefaultCapabilitiesFinder(bsc), ComparisonMode.EXACT_COMMAND_STRING);

        //TODO: it should work either way, but for now we expect the subquery to conform to the parent
        sql = "select pm1.g1.e1 from pm1.g1 where e2 in (select e2 from pm2.g2)";
    }

    @Test public void testConformedSubquery1() throws Exception {
        String sql = "select pm2.g3.e1 from pm2.g3 where e1 in /*+ no_unnest */ (select e1 from pm1.g1)";

        BasicSourceCapabilities bsc = getTypicalCapabilities();
        bsc.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);

        helpPlan(sql, tm, new String[] {"SELECT g_0.e1 FROM pm2.g3 AS g_0 WHERE g_0.e1 IN /*+ NO_UNNEST */ (SELECT g_1.e1 FROM pm1.g1 AS g_1)"}, new DefaultCapabilitiesFinder(bsc), ComparisonMode.EXACT_COMMAND_STRING);
    }

}
