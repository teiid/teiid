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

import org.junit.Test;
import org.teiid.metadata.ForeignKey;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.TestOptimizer.ComparisonMode;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.FakeCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.translator.ExecutionFactory.SupportedJoinCriteria;

@SuppressWarnings("nls")
public class TestJoinPushdownRestrictions {

    @Test public void testThetaRestriction() throws Exception {
        String sql = "select pm1.g1.e2, pm1.g2.e2 from pm1.g1 inner join pm1.g2 on (pm1.g1.e2 + pm1.g2.e2 = 5)"; //$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setSourceProperty(Capability.JOIN_CRITERIA_ALLOWED, SupportedJoinCriteria.THETA);
        caps.setFunctionSupport("+", true); //$NON-NLS-1$
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(),
                new String[] {"SELECT g_0.e2 FROM pm1.g2 AS g_0", "SELECT g_0.e2 FROM pm1.g1 AS g_0"}, capFinder, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$

        caps.setSourceProperty(Capability.JOIN_CRITERIA_ALLOWED, SupportedJoinCriteria.ANY);

        TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(),
                new String[] {"SELECT g_0.e2, g_1.e2 FROM pm1.g1 AS g_0, pm1.g2 AS g_1 WHERE (g_0.e2 + g_1.e2) = 5"}, capFinder, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    }

    @Test public void testEquiRestriction() throws Exception {
        String sql = "select pm1.g1.e2, pm1.g2.e2 from pm1.g1 inner join pm1.g2 on (pm1.g1.e2 < pm1.g2.e2)"; //$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setSourceProperty(Capability.JOIN_CRITERIA_ALLOWED, SupportedJoinCriteria.EQUI);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(),
                new String[] {"SELECT g_0.e2 FROM pm1.g2 AS g_0", "SELECT g_0.e2 FROM pm1.g1 AS g_0"}, capFinder, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$

        caps.setSourceProperty(Capability.JOIN_CRITERIA_ALLOWED, SupportedJoinCriteria.THETA);

        TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(),
                new String[] {"SELECT g_0.e2, g_1.e2 FROM pm1.g1 AS g_0, pm1.g2 AS g_1 WHERE g_0.e2 < g_1.e2"}, capFinder, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    }

    @Test public void testKeyRestriction() throws Exception {
        String sql = "select pm1.g1.e2, pm1.g2.e2 from pm1.g1 inner join pm1.g2 on (pm1.g1.e2 = pm1.g2.e2)"; //$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setSourceProperty(Capability.JOIN_CRITERIA_ALLOWED, SupportedJoinCriteria.KEY);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(),
                new String[] {"SELECT g_0.e2 AS c_0 FROM pm1.g1 AS g_0 ORDER BY c_0", "SELECT g_0.e2 AS c_0 FROM pm1.g2 AS g_0 ORDER BY c_0"}, capFinder, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$

        caps.setSourceProperty(Capability.JOIN_CRITERIA_ALLOWED, SupportedJoinCriteria.EQUI);

        TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(),
                new String[] {"SELECT g_0.e2, g_1.e2 FROM pm1.g1 AS g_0, pm1.g2 AS g_1 WHERE g_0.e2 = g_1.e2"}, capFinder, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    }

    @Test public void testKeyRestriction1() throws Exception {
        String sql = "select a.e1, b.e1, c.e2 from pm4.g1 a inner join pm4.g2 b on (a.e1 = b.e1 and a.e2 = b.e2) left outer join pm4.g1 c on (c.e1 = b.e1 and c.e2 = b.e2)"; //$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
        caps.setSourceProperty(Capability.JOIN_CRITERIA_ALLOWED, SupportedJoinCriteria.KEY);
        capFinder.addCapabilities("pm4", caps); //$NON-NLS-1$

        TestOptimizer.helpPlan(sql, RealMetadataFactory.example4(),
                new String[] {"SELECT g_0.e1, g_1.e1, g_2.e2 FROM (pm4.g1 AS g_0 INNER JOIN pm4.g2 AS g_1 ON g_0.e1 = g_1.e1 AND g_0.e2 = g_1.e2) LEFT OUTER JOIN pm4.g1 AS g_2 ON g_2.e1 = g_1.e1 AND g_2.e2 = g_1.e2"}, capFinder, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$

        //equivalent form, should be same result
        sql = "select a.e1, b.e1, c.e2 from pm4.g1 a inner join pm4.g2 b left outer join pm4.g1 c on (c.e1 = b.e1 and c.e2 = b.e2) on (a.e1 = b.e1 and a.e2 = b.e2)"; //$NON-NLS-1$

        TestOptimizer.helpPlan(sql, RealMetadataFactory.example4(),
                new String[] {"SELECT g_2.e1, g_0.e1, g_1.e2 FROM (pm4.g2 AS g_0 LEFT OUTER JOIN pm4.g1 AS g_1 ON g_1.e1 = g_0.e1 AND g_1.e2 = g_0.e2) INNER JOIN pm4.g1 AS g_2 ON g_2.e1 = g_0.e1 AND g_2.e2 = g_0.e2"}, capFinder, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testKeyRestriction2() throws Exception {
        String sql = "select a.e1, b.e1, c.e2 from pm4.g1 a left outer join pm4.g2 b on (a.e1 = b.e1 and a.e2 = b.e2) inner join pm4.g2 c on (c.e1 = a.e1 and c.e2 = a.e2)"; //$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
        caps.setSourceProperty(Capability.JOIN_CRITERIA_ALLOWED, SupportedJoinCriteria.KEY);
        capFinder.addCapabilities("pm4", caps); //$NON-NLS-1$

        TestOptimizer.helpPlan(sql, RealMetadataFactory.example4(),
                new String[] {"SELECT g_0.e1, g_1.e1, g_2.e2 FROM (pm4.g1 AS g_0 LEFT OUTER JOIN pm4.g2 AS g_1 ON g_0.e1 = g_1.e1 AND g_0.e2 = g_1.e2) INNER JOIN pm4.g2 AS g_2 ON g_2.e1 = g_0.e1 AND g_2.e2 = g_0.e2"}, capFinder, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testKeyPasses() throws Exception {
        String sql = "select a.e1, b.e1 from pm4.g1 a, pm4.g2 b where a.e1 = b.e1 and a.e2 = b.e2"; //$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setSourceProperty(Capability.JOIN_CRITERIA_ALLOWED, SupportedJoinCriteria.KEY);
        capFinder.addCapabilities("pm4", caps); //$NON-NLS-1$

        TestOptimizer.helpPlan(sql, RealMetadataFactory.example4(),
                new String[] {"SELECT g_0.e1, g_1.e1 FROM pm4.g1 AS g_0, pm4.g2 AS g_1 WHERE (g_0.e1 = g_1.e1) AND (g_0.e2 = g_1.e2)"}, capFinder, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    }

    @Test public void testAllowJoinFalse() throws Exception {
        String sql = "select a.e1, b.e1 from pm4.g1 a, pm4.g2 b where a.e1 = b.e1 and a.e2 = b.e2"; //$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setSourceProperty(Capability.JOIN_CRITERIA_ALLOWED, SupportedJoinCriteria.KEY);
        capFinder.addCapabilities("pm4", caps); //$NON-NLS-1$

        TransformationMetadata example4 = RealMetadataFactory.example4();
        example4.getMetadataStore().getSchema("pm4").getTable("g2").getForeignKeys().get(0).setProperty(ForeignKey.ALLOW_JOIN, Boolean.FALSE.toString());
        TestOptimizer.helpPlan(sql, example4,
                new String[] {"SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm4.g1 AS g_0 ORDER BY c_0, c_1", "SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm4.g2 AS g_0 ORDER BY c_0, c_1"}, capFinder, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    }

    @Test public void testAllowJoinInner() throws Exception {
        String sql = "select a.e1, b.e1 from pm4.g1 a, pm4.g2 b where a.e1 = b.e1 and a.e2 = b.e2"; //$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setSourceProperty(Capability.JOIN_CRITERIA_ALLOWED, SupportedJoinCriteria.KEY);
        capFinder.addCapabilities("pm4", caps); //$NON-NLS-1$

        TransformationMetadata example4 = RealMetadataFactory.example4();
        example4.getMetadataStore().getSchema("pm4").getTable("g2").getForeignKeys().get(0).setProperty(ForeignKey.ALLOW_JOIN, "INNER");

        //should not inhibit
        TestOptimizer.helpPlan(sql, example4,
                new String[] {"SELECT g_0.e1, g_1.e1 FROM pm4.g1 AS g_0, pm4.g2 AS g_1 WHERE (g_0.e1 = g_1.e1) AND (g_0.e2 = g_1.e2)"}, capFinder, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        //should inhibit
        sql = "select a.e1, b.e1 from pm4.g1 a left outer join pm4.g2 b on a.e1 = b.e1 and a.e2 = b.e2"; //$NON-NLS-1$
        TestOptimizer.helpPlan(sql, example4,
                new String[] {"SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm4.g1 AS g_0 ORDER BY c_0, c_1", "SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm4.g2 AS g_0 ORDER BY c_0, c_1"}, capFinder, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        //should not inhibit
        sql = "select a.e1, b.e1 from pm4.g1 a right outer join pm4.g2 b on a.e1 = b.e1 and a.e2 = b.e2"; //$NON-NLS-1$
        TestOptimizer.helpPlan(sql, example4,
                new String[] {"SELECT g_1.e1, g_0.e1 FROM pm4.g2 AS g_0 LEFT OUTER JOIN pm4.g1 AS g_1 ON g_1.e1 = g_0.e1 AND g_1.e2 = g_0.e2"}, capFinder, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    }

    @Test public void testCrossJoinWithRestriction() throws Exception {
        String sql = "select pm1.g1.e2, pm1.g2.e2 from pm1.g1, pm1.g2"; //$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setSourceProperty(Capability.JOIN_CRITERIA_ALLOWED, SupportedJoinCriteria.THETA);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(),
                new String[] {"SELECT g_0.e2 FROM pm1.g2 AS g_0", "SELECT g_0.e2 FROM pm1.g1 AS g_0"}, capFinder, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testOuterRestriction() throws Exception {
        String sql = "select pm1.g1.e2, pm1.g2.e2 from pm1.g1 inner join pm1.g2 on (pm1.g1.e2 + pm1.g2.e2 = 5)"; //$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, false);
        caps.setCapabilitySupport(Capability.CRITERIA_ISNULL, false);
        caps.setSourceProperty(Capability.JOIN_CRITERIA_ALLOWED, SupportedJoinCriteria.ANY);
        caps.setFunctionSupport("+", true); //$NON-NLS-1$
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(),
                new String[] {"SELECT g_0.e2 FROM pm1.g2 AS g_0", "SELECT g_0.e2 FROM pm1.g1 AS g_0"}, capFinder, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testOuterRestrictionMultiLevel() throws Exception {
        String ddl = "create foreign table g1 (e1 integer primary key, e2 integer); " +
                "create foreign table g2 (e1 integer primary key, e2 integer, FOREIGN KEY (e2) REFERENCES g1 (e1));" +
                "create foreign table g3 (e1 integer primary key, e2 integer, FOREIGN KEY (e2) REFERENCES g2 (e1));";

        String sql = "select g1.e2, g2.e2 from g1, g2, g3 where g1.e1 = g2.e2 and g2.e1 = g3.e2"; //$NON-NLS-1$

        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, false);
        caps.setSourceProperty(Capability.JOIN_CRITERIA_ALLOWED, SupportedJoinCriteria.KEY);

        TestOptimizer.helpPlan(sql, RealMetadataFactory.fromDDL(ddl, "x", "y"),
                new String[] {"SELECT g_0.e2, g_1.e2 FROM (y.g1 AS g_0 LEFT OUTER JOIN y.g2 AS g_1 ON g_0.e1 = g_1.e2) LEFT OUTER JOIN y.g3 AS g_2 ON g_1.e1 = g_2.e2 WHERE (g_1.e2 IS NOT NULL) AND (g_2.e2 IS NOT NULL)"}, new DefaultCapabilitiesFinder(caps), TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testOuterPreservation() throws Exception {
        String sql = "select pm1.g1.e2, pm1.g2.e2 from pm1.g1 left outer join pm1.g2 on (pm1.g1.e2 = pm1.g2.e2) where pm1.g2.e1 = 'a'"; //$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, false);
        caps.setSourceProperty(Capability.JOIN_CRITERIA_ALLOWED, SupportedJoinCriteria.ANY);
        caps.setFunctionSupport("+", true); //$NON-NLS-1$
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(),
                new String[] {"SELECT g_0.e2, g_1.e2 FROM pm1.g1 AS g_0 LEFT OUTER JOIN pm1.g2 AS g_1 ON g_0.e2 = g_1.e2 WHERE g_1.e1 = 'a'"}, capFinder, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testOuterPreservation1() throws Exception {
        String sql = "select pm1.g1.e2, pm1.g2.e2 from pm1.g1 inner join pm1.g2 on (pm1.g1.e2 = pm1.g2.e2)"; //$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, false);
        caps.setSourceProperty(Capability.JOIN_CRITERIA_ALLOWED, SupportedJoinCriteria.ANY);
        caps.setFunctionSupport("+", true); //$NON-NLS-1$
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(),
                new String[] {"SELECT g_0.e2, g_1.e2 FROM pm1.g1 AS g_0 LEFT OUTER JOIN pm1.g2 AS g_1 ON g_0.e2 = g_1.e2 WHERE g_1.e2 IS NOT NULL"}, capFinder, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testCriteriaRestrictionWithNonJoinCriteria() throws Exception {
        String sql = "select pm1.g1.e2, pm1.g2.e2 from pm1.g1 left outer join pm1.g2 on (pm1.g1.e2 = pm1.g2.e2 and pm1.g2.e1 = 'hello')"; //$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, true);
        caps.setSourceProperty(Capability.JOIN_CRITERIA_ALLOWED, SupportedJoinCriteria.THETA);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(),
                new String[] {"SELECT g_0.e2 AS c_0 FROM pm1.g1 AS g_0 ORDER BY c_0", "SELECT g_0.e2 AS c_0 FROM pm1.g2 AS g_0 WHERE g_0.e1 = 'hello' ORDER BY c_0"}, capFinder, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRelationshipJoin() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setSourceProperty(Capability.JOIN_CRITERIA_ALLOWED, SupportedJoinCriteria.KEY);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
        caps.setCapabilitySupport(Capability.QUERY_ONLY_FROM_RELATIONSHIP_JOIN, true);
        capFinder.addCapabilities("y", caps); //$NON-NLS-1$

        String ddl = "create foreign table parent (id integer primary key, name string);"
                + "create foreign table child (id integer primary key, parentid integer, name string, foreign key (parentid) references parent (id));"
                + "create foreign table grandchild (id integer primary key, childid integer, name string, foreign key (childid) references child (id));";

        TestOptimizer.helpPlan("select parent.name, child.name, grandchild.name from grandchild left outer join child on grandchild.childid=child.id left outer join parent on child.parentid = parent.id", RealMetadataFactory.fromDDL(ddl, "x", "y"), null, capFinder, //$NON-NLS-1$
                new String[] { "SELECT g_2.name, g_1.name, g_0.name FROM (y.grandchild AS g_0 LEFT OUTER JOIN y.child AS g_1 ON g_0.childid = g_1.id) LEFT OUTER JOIN y.parent AS g_2 ON g_1.parentid = g_2.id"}, ComparisonMode.EXACT_COMMAND_STRING ); //$NON-NLS-1$ //$NON-NLS-2$

        TestOptimizer.helpPlan("select g1.name, child.name, grandchild.name from grandchild inner join child on grandchild.childid=child.id inner join grandchild g1 on g1.childid = child.id", RealMetadataFactory.fromDDL(ddl, "x", "y"), null, capFinder, //$NON-NLS-1$
                new String[] { "SELECT g_0.childid AS c_0, g_0.name AS c_1 FROM y.grandchild AS g_0 ORDER BY c_0", "SELECT g_1.id AS c_0, g_1.name AS c_1, g_0.name AS c_2 FROM y.grandchild AS g_0, y.child AS g_1 WHERE g_0.childid = g_1.id ORDER BY c_0"}, ComparisonMode.EXACT_COMMAND_STRING ); //$NON-NLS-1$ //$NON-NLS-2$
    }

}
