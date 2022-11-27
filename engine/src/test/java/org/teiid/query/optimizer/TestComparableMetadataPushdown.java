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
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.FakeCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.validator.TestValidator;


public class TestComparableMetadataPushdown {

    @Test public void testCantPushSort() throws Exception {
        String sql = "select e3, e2 from test.group order by e3, e2"; //$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        capFinder.addCapabilities("test", TestOptimizer.getTypicalCapabilities()); //$NON-NLS-1$

        TestOptimizer.helpPlan(sql, TestValidator.exampleMetadata(),
                new String[] {"SELECT g_0.e3, g_0.e2 FROM test.\"group\" AS g_0"}, capFinder, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    }

    @Test public void testCantPushGroupBy() throws Exception {
        String sql = "select e3, e2 from test.group group by e3, e2"; //$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_GROUP_BY, true);
        capFinder.addCapabilities("test", caps); //$NON-NLS-1$

        TestOptimizer.helpPlan(sql, TestValidator.exampleMetadata(),
                new String[] {"SELECT g_0.e3, g_0.e2 FROM test.\"group\" AS g_0"}, capFinder, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    }

    @Test public void testCantPushDup() throws Exception {
        String sql = "select distinct e3, e2 from test.group"; //$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        capFinder.addCapabilities("test", TestOptimizer.getTypicalCapabilities()); //$NON-NLS-1$

        TestOptimizer.helpPlan(sql, TestValidator.exampleMetadata(),
                new String[] {"SELECT g_0.e3, g_0.e2 FROM test.\"group\" AS g_0"}, capFinder, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    }

    @Test public void testCantPushSetOp() throws Exception {
        String sql = "select e3, e2 from test.group union select e0, e1 from test.group2"; //$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_UNION, true);
        capFinder.addCapabilities("test", caps); //$NON-NLS-1$

        TestOptimizer.helpPlan(sql, TestValidator.exampleMetadata(),
                new String[] {"SELECT test.\"group\".e3, test.\"group\".e2 FROM test.\"group\"", "SELECT test.group2.e0, test.group2.e1 FROM test.group2"}, capFinder, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$
    }

}
