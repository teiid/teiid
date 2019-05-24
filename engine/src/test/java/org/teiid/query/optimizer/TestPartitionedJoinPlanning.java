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

import static org.teiid.query.optimizer.TestOptimizer.*;

import org.junit.Test;
import org.teiid.common.buffer.BufferManager;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.FakeCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.relational.EnhancedSortMergeJoinStrategy;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings("nls")
public class TestPartitionedJoinPlanning {

    @Test public void testUsePartitionedMergeJoin() throws Exception {
        // Create query
        String sql = "SELECT pm1.g1.e1 FROM pm1.g1, pm1.g2 WHERE pm1.g1.e1 = pm1.g2.e1";//$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        caps.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, 10);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1();
        RealMetadataFactory.setCardinality("pm1.g1", BufferManager.DEFAULT_PROCESSOR_BATCH_SIZE * 2, metadata);
        RealMetadataFactory.setCardinality("pm1.g2", BufferManager.DEFAULT_PROCESSOR_BATCH_SIZE * 16, metadata);

        ProcessorPlan plan = helpPlan(sql, metadata,
            null, capFinder,
            new String[] { "SELECT pm1.g1.e1 FROM pm1.g1 ORDER BY pm1.g1.e1", "SELECT pm1.g2.e1 FROM pm1.g2" }, SHOULD_SUCCEED); //$NON-NLS-1$ //$NON-NLS-2$
        checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            1,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
        checkNodeTypes(plan, new int[] {1}, new Class[] {EnhancedSortMergeJoinStrategy.class});
    }


}
