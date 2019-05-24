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

package org.teiid.query.optimizer.relational.rules;

import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.optimizer.TestOptimizer.ComparisonMode;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.FakeCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.optimizer.relational.rules.RuleValidateWhereAll;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.unittest.RealMetadataFactory;

import junit.framework.TestCase;


public class TestRuleValidateWhereAll extends TestCase {

    public TestRuleValidateWhereAll(String name) {
        super(name);
    }

    public void testHasNoCriteria1() {
        assertEquals("Got incorrect answer checking for no criteria", false, RuleValidateWhereAll.hasNoCriteria(new Insert())); //$NON-NLS-1$
    }

    public void testHasNoCriteria2() {
        Query query = new Query();
        CompareCriteria crit = new CompareCriteria(new Constant("a"), CompareCriteria.EQ, new Constant("b")); //$NON-NLS-1$ //$NON-NLS-2$
        query.setCriteria(crit);
        assertEquals("Got incorrect answer checking for no criteria", false, RuleValidateWhereAll.hasNoCriteria(query)); //$NON-NLS-1$
    }

    public void testHasNoCriteria3() {
        assertEquals("Got incorrect answer checking for no criteria", true, RuleValidateWhereAll.hasNoCriteria(new Query())); //$NON-NLS-1$
    }

    private FakeCapabilitiesFinder getWhereAllCapabilities() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.REQUIRES_CRITERIA, true);
        capFinder.addCapabilities("pm1", TestOptimizer.getTypicalCapabilities()); //$NON-NLS-1$
        capFinder.addCapabilities("pm6", caps); //$NON-NLS-1$
        return capFinder;
    }

    public void testDefect21982_3() {
        TestOptimizer.helpPlan(
                 "SELECT * FROM vm1.g38",   //$NON-NLS-1$
                 RealMetadataFactory.example1Cached(),
                 null, getWhereAllCapabilities(),
                 new String[0],
                 false);
    }

    public void testWhereAll1() {
        TestOptimizer.helpPlan(
            "SELECT * FROM pm6.g1",   //$NON-NLS-1$
            RealMetadataFactory.example1Cached(),
            null, getWhereAllCapabilities(),
            new String[0],
            false);
    }

    public void testWhereAll2() throws Exception {
        TestOptimizer.helpPlan(
            "SELECT pm1.g1.e1 FROM pm1.g1, pm6.g1 WHERE pm1.g1.e1=pm6.g1.e1 OPTION MAKEDEP pm6.g1",   //$NON-NLS-1$
            RealMetadataFactory.example1Cached(),
            null, getWhereAllCapabilities(),
            new String[] {
                "SELECT g_0.e1 AS c_0 FROM pm6.g1 AS g_0 WHERE g_0.e1 IN (<dependent values>) ORDER BY c_0", "SELECT g_0.e1 AS c_0 FROM pm1.g1 AS g_0 ORDER BY c_0" //$NON-NLS-1$ //$NON-NLS-2$
            },
            ComparisonMode.EXACT_COMMAND_STRING);
    }

}
