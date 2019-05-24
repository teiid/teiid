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
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.unittest.RealMetadataFactory;


public class TestRuleRemoveSorts {

    /** Tests an order by in a query transformation */
    @Test public void testRemovedOrderByFromQueryTransform() {
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT e1, e2 FROM vm1.g14", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "SELECT pm3.g1.e1, pm3.g1.e2 FROM pm3.g1"}); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    /**
     * Tests an order by in a query transformation, where the
     * physical model does not support pushing order bys
     */
    @Test public void testRemovedOrderByFromQueryTransform2() {
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT e, e2 FROM vm1.g8", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "SELECT e1, e2 FROM pm1.g1"}); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    /**
     * Tests an order by in a query transformation, where the
     * query transformation contains a function
     */
    @Test public void testRemovedOrderByFromQueryTransform3() {
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT e, e2 FROM vm1.g16", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "SELECT e1, e2 FROM pm3.g1"}); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    /** Tests an order by in a query transformation */
    @Test public void testRemovedOrderByFromQueryTransform4() {
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT e1, e2 FROM vm1.g13", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "SELECT DISTINCT pm3.g1.e1, pm3.g1.e2, pm3.g1.e3, pm3.g1.e4 FROM pm3.g1"}); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    /** Order by is not removed */
    @Test public void testOrderByWithLimit() throws Exception {
        ProcessorPlan plan = TestOptimizer.helpPlan("select * from (SELECT e1, e2 FROM pm1.g1 order by e1 limit 10) x", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm1.g1 AS g_0 ORDER BY c_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

}
