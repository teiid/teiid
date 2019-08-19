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
import static org.teiid.query.optimizer.TestOptimizer.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.optimizer.TestOptimizer.ComparisonMode;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings("nls")
public class TestIsDistinctProcessing {

    @Test public void testFullPushdown() throws Exception {
        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        bsc.setCapabilitySupport(Capability.CRITERIA_IS_DISTINCT, true);
        DefaultCapabilitiesFinder dcf = new DefaultCapabilitiesFinder(bsc);
        ProcessorPlan plan = helpPlan("SELECT pm1.g1.e2 is not distinct from 1 FROM pm1.g1", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
                new String[] {"SELECT g_0.e2 IS NOT DISTINCT FROM 1 FROM pm1.g1 AS g_0"}, dcf, ComparisonMode.EXACT_COMMAND_STRING ); //$NON-NLS-1$
        assertNull(plan.requiresTransaction(true));
        checkNodeTypes(plan, FULL_PUSHDOWN);

        HardcodedDataManager hdm = new HardcodedDataManager(RealMetadataFactory.example1Cached());
        hdm.addData("SELECT g_0.e2 IS NOT DISTINCT FROM 1 FROM g1 AS g_0", Arrays.asList(true));
        TestProcessor.helpProcess(plan, hdm, new List<?>[] {Arrays.asList(true)});
    }

    @Test public void testJoin() throws Exception {
        //TODO: this currently plans as a nested loop
        String sql = "select pm1.g1.e1, pm2.g1.e2 from pm1.g1, pm2.g1 where pm1.g1.e1 is not distinct from pm2.g1.e1";
        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached());
        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT pm2.g1.e1, pm2.g1.e2 FROM pm2.g1", Arrays.asList("a", 1), Arrays.asList(null, 2), Arrays.asList("b", null));
        dataManager.addData("SELECT pm1.g1.e1 FROM pm1.g1", Arrays.asList("a"), Collections.singletonList(null));
        TestProcessor.helpProcess(plan, dataManager, new List<?>[] {Arrays.asList("a", 1), Arrays.asList(null, 2)});
    }

    @Test public void testJoinRewrite() throws Exception {
        //adding the extra reference to g1 causes an alias to be added, which the expression mapping was missing for is distinct
        String sql = "begin select e1 from pm1.g1 without return; select pm1.g1.e1, pm2.g1.e2 from pm1.g1, pm2.g1 where pm1.g1.e1 is not distinct from pm2.g1.e1; end";
        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached());
        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT pm2.g1.e1, pm2.g1.e2 FROM pm2.g1", Arrays.asList("a", 1), Arrays.asList(null, 2), Arrays.asList("b", null));
        dataManager.addData("SELECT pm1.g1.e1 FROM pm1.g1", Arrays.asList("a"), Collections.singletonList(null));
        TestProcessor.helpProcess(plan, dataManager, new List<?>[] {Arrays.asList("a", 1), Arrays.asList(null, 2)});
    }

}
