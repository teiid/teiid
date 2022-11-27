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
import org.teiid.query.optimizer.TestOptimizer.ComparisonMode;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.FakeCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.unittest.RealMetadataFactory;


public class TestAnsiJoinPushdown {

    /**
     * Notice that the non-join criteria is still in the on clause.
     */
    @Test public void testAnsiInnerJoin() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_ANSI_JOIN, true);
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan(
                "select pm2.g1.e1 from pm2.g1, pm2.g2 where pm2.g1.e1 = pm2.g2.e1 and (pm2.g1.e2 = 1 OR pm2.g2.e2 = 2) and pm2.g2.e3 = 1", //$NON-NLS-1$
                RealMetadataFactory.example1Cached(),
                null,
                capFinder,
                new String[] { "SELECT g_0.e1 FROM pm2.g1 AS g_0 INNER JOIN pm2.g2 AS g_1 ON g_0.e1 = g_1.e1 AND ((g_0.e2 = 1) OR (g_1.e2 = 2)) WHERE g_1.e3 = TRUE" }, //$NON-NLS-1$
                ComparisonMode.EXACT_COMMAND_STRING);
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

}
