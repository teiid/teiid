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
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.TestOptimizer.ComparisonMode;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings("nls")
public class TestOptionalJoins {

    @Test public void testOptionalJoinNode1() {
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT pm1.g1.e1 FROM pm1.g1, /* optional */ pm1.g2", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] {"SELECT pm1.g1.e1 FROM pm1.g1"} ); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    @Test public void testOptionalJoinNode1WithPredicate() {
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT pm1.g1.e1 FROM pm1.g1, /* optional */ pm1.g2 WHERE pm1.g1.e1 = pm1.g2.e1", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] {"SELECT pm1.g1.e1 FROM pm1.g1"} ); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    @Test public void testOptionalJoinNode1WithJoinCriteria() {
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT pm1.g3.e1 FROM (pm1.g1 CROSS JOIN /* optional */ pm1.g2) INNER JOIN pm1.g3 ON pm1.g1.e1 = pm1.g3.e1 AND pm1.g2.e1 = pm1.g3.e1", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] {"SELECT g_1.e1 FROM pm1.g1 AS g_0, pm1.g3 AS g_1 WHERE g_0.e1 = g_1.e1"} ); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    @Test public void testOptionalJoinNodeNonAnsiWithHaving() {
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT e1 FROM (SELECT pm1.g1.e1, max(pm1.g2.e2) FROM pm1.g1, /* optional */ pm1.g2 WHERE pm1.g1.e1 = pm1.g2.e1 GROUP BY pm1.g1.e1) x", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] {"SELECT pm1.g1.e1 FROM pm1.g1"} ); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, new int[] {
                1,      // Access
                0,      // DependentAccess
                0,      // DependentSelect
                0,      // DependentProject
                0,      // DupRemove
                1,      // Grouping
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

    @Test public void testOptionalJoinNode1_1() {
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT pm1.g1.e1,pm2.g2.e1  FROM pm1.g1, /* optional */ pm2.g2", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] {"SELECT pm1.g1.e1 FROM pm1.g1", "SELECT pm2.g2.e1 FROM pm2.g2"} ); //$NON-NLS-1$//$NON-NLS-2$

        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            1,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    @Test public void testOptionalJoinNode2() {
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT pm1.g1.e1 FROM pm1.g1, /* optional */ pm1.g2, pm1.g3", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] {"SELECT g_0.e1 FROM pm1.g1 AS g_0, pm1.g3 AS g_1"} ); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    @Test public void testOptionalJoinNode3() {
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT pm1.g1.e1 FROM pm1.g1 LEFT OUTER JOIN /* optional */ pm1.g2 on pm1.g1.e1 = pm1.g2.e1", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] {"SELECT pm1.g1.e1 FROM pm1.g1"} ); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    @Test public void testOptionalJoinNode3_1() {
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT pm1.g1.e1, pm2.g2.e1 FROM pm1.g1 LEFT OUTER JOIN /* optional */ pm2.g2 on pm1.g1.e1 = pm2.g2.e1", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] {"SELECT g_0.e1 AS c_0 FROM pm2.g2 AS g_0 ORDER BY c_0", "SELECT g_0.e1 AS c_0 FROM pm1.g1 AS g_0 ORDER BY c_0"} ); //$NON-NLS-1$ //$NON-NLS-2$

        TestOptimizer.checkNodeTypes(plan, new int[] {
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
    }

    @Test public void testOptionalJoinNode4() {
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT pm1.g1.e1 FROM (pm1.g1 LEFT OUTER JOIN /* optional */ pm1.g2 on pm1.g1.e1 = pm1.g2.e1) LEFT OUTER JOIN /* optional */ pm1.g3 on pm1.g1.e1 = pm1.g3.e1", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] {"SELECT pm1.g1.e1 FROM pm1.g1"} ); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    @Test public void testOptionalJoinNode5() {
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT pm1.g1.e1 FROM (pm1.g1 LEFT OUTER JOIN pm1.g2 on pm1.g1.e1 = pm1.g2.e1) LEFT OUTER JOIN /* optional */ pm1.g3 on pm1.g1.e1 = pm1.g3.e1", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] {"SELECT g_0.e1 FROM pm1.g1 AS g_0 LEFT OUTER JOIN pm1.g2 AS g_1 ON g_0.e1 = g_1.e1"} ); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    @Test public void testOptionalJoinNode6() {
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT pm1.g1.e1 FROM (pm1.g1 LEFT OUTER JOIN /* optional */ pm1.g2 on pm1.g1.e1 = pm1.g2.e1) LEFT OUTER JOIN pm1.g3 on pm1.g1.e1 = pm1.g3.e1", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] {"SELECT g_0.e1 FROM pm1.g1 AS g_0 LEFT OUTER JOIN pm1.g3 AS g_1 ON g_0.e1 = g_1.e1"} ); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    @Test public void testOptionalJoinNode7() {
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT pm1.g3.e1 FROM /* optional */ (pm1.g1 LEFT OUTER JOIN pm1.g2 on pm1.g1.e1 = pm1.g2.e1) LEFT OUTER JOIN pm1.g3 on pm1.g1.e1 = pm1.g3.e1", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] {"SELECT pm1.g3.e1 FROM pm1.g3"} ); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    @Test public void testOptionalJoinNode8() {
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT pm1.g1.e1 FROM pm1.g1 LEFT OUTER JOIN /* optional */ (select * from pm1.g2) as X on pm1.g1.e1 = x.e1", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] {"SELECT pm1.g1.e1 FROM pm1.g1"} ); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    @Test public void testOptionalJoinNode9() {
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT pm1.g2.e1 FROM pm1.g2, /* optional */ vm1.g1", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] {"SELECT pm1.g2.e1 FROM pm1.g2"} ); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    @Test public void testOptionalJoinNode10() {
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT pm1.g1.e1 FROM /* optional */ vm1.g1, pm1.g1", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] {"SELECT pm1.g1.e1 FROM pm1.g1"} ); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    @Test public void testOptionalJoinNode11() {
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT pm1.g1.e1 FROM pm1.g1 LEFT OUTER JOIN /* optional */ vm1.g2 on pm1.g1.e1 = vm1.g2.e1", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] {"SELECT pm1.g1.e1 FROM pm1.g1"} ); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    @Test public void testOptionalJoinNode12() {
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT pm1.g3.e1 FROM /* optional */ (pm1.g1 LEFT OUTER JOIN vm1.g1 on pm1.g1.e1 = vm1.g1.e1) LEFT OUTER JOIN pm1.g3 on pm1.g1.e1 = pm1.g3.e1", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] {"SELECT pm1.g3.e1 FROM pm1.g3"} ); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    @Test public void testOptionalJoinNode13() {
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT count(pm1.g1.e1) FROM pm1.g1 LEFT OUTER JOIN /* optional */ pm1.g2 on pm1.g1.e1 = pm1.g2.e1", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] {"SELECT pm1.g1.e1 FROM pm1.g1"} ); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            1,      // Grouping
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
     * The distinct prevents the removal of the optional join
     */
    @Test public void testOptionalJoinNode14() throws Exception {
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT ve1 FROM vm1.g4", RealMetadataFactory.example4(), //$NON-NLS-1$
            new String[] {"SELECT g_0.e1 AS c_0 FROM pm1.g1 AS g_0 WHERE g_0.e1 IN (<dependent values>) ORDER BY c_0", "SELECT DISTINCT g_0.e1 AS c_0 FROM pm1.g2 AS g_0 ORDER BY c_0"}, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING ); //$NON-NLS-1$ //$NON-NLS-2$

        TestOptimizer.checkNodeTypes(plan, new int[] {
            1,      // Access
            1,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            1,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            1,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            2,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    @Test public void testOptionalJoinNode15() {
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT x.e1 FROM (select vm1.g1.e1, vm1.g2.e2 from vm1.g1 LEFT OUTER JOIN /* optional */vm1.g2 on vm1.g1.e2 = vm1.g2.e2) AS x", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] {"SELECT pm1.g1.e1 FROM pm1.g1"} ); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    @Test public void testOptionalJoinNode16() {
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT length(z) FROM /* optional */ pm1.g1, (select distinct e2 as y, e3 || 'x' as z from pm1.g1 ORDER BY y, z) AS x", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] {"SELECT e2, e3 FROM pm1.g1"} ); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            1,      // DupRemove
            0,      // Grouping
            0,      // Join
            0,      // MergeJoin
            0,      // Null
            0,      // PlanExecution
            2,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    @Test public void testOptionalJoinNode17() {
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT length(z) FROM /* optional */ pm1.g1 inner join (select e2 as y, e3 || 'x' as z from pm1.g1 ORDER BY z) AS x on pm1.g1.e2=x.y", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] {"SELECT e3 FROM pm1.g1"} ); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // Join
            0,      // MergeJoin
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    @Test public void testOptionalJoinWithIntersection() throws Exception {
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT pm1.g3.e1 FROM pm1.g3 inner join (select pm1.g1.e2 as y from /* optional */ pm1.g1 inner join pm1.g2 on pm1.g1.e1 = pm1.g2.e1) AS x on pm1.g3.e2=x.y", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] {"SELECT g_0.e1 FROM pm1.g3 AS g_0, pm1.g1 AS g_1, pm1.g2 AS g_2 WHERE (g_1.e1 = g_2.e1) AND (g_0.e2 = g_1.e2)"}, ComparisonMode.EXACT_COMMAND_STRING ); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    @Test public void testOptionalJoinWithNestedOrderBy() {
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT pm1.g3.e1 FROM pm1.g3 inner join (select pm1.g2.e1, pm1.g1.e2 as y from /* optional */ pm1.g1 inner join pm1.g2 on pm1.g1.e1 = pm1.g2.e1 order by pm1.g2.e1 limit 10000) AS x on pm1.g3.e2=x.y", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] {"SELECT g_0.e2 AS c_0, g_0.e1 AS c_1 FROM pm1.g3 AS g_0 ORDER BY c_0", "SELECT g_0.e2 AS c_0 FROM pm1.g1 AS g_0, pm1.g2 AS g_1 WHERE g_0.e1 = g_1.e1 ORDER BY g_1.e1"} ); //$NON-NLS-1$ //$NON-NLS-2$

        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // Join
            1,      // MergeJoin
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    /**
     * Grouping will prevent the removal from happening
     */
    @Test public void testOptionalJoinWithGroupingOverAllColumns() {
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT pm1.g3.e1 FROM pm1.g3, (select max(pm1.g1.e4) y from /* optional */ pm1.g1, pm1.g2 where pm1.g1.e1 = pm1.g2.e1) AS x where pm1.g3.e2=x.y", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] {"SELECT g_0.e2, g_0.e1 FROM pm1.g3 AS g_0", "SELECT g_0.e4 FROM pm1.g1 AS g_0, pm1.g2 AS g_1 WHERE g_0.e1 = g_1.e1"} ); //$NON-NLS-1$ //$NON-NLS-2$

        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            1,      // Grouping
            0,      // Join
            1,      // MergeJoin
            0,      // Null
            0,      // PlanExecution
            3,      // Project
            1,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    /**
     * Union all should not prevent the removal from happening
     */
    @Test public void testOptionalJoinWithUnion() {
        ProcessorPlan plan = TestOptimizer.helpPlan("select pm1.g2.e4 from /* optional */ pm1.g1 inner join pm1.g2 on pm1.g1.e1 = pm1.g2.e1 union all select convert(pm1.g2.e2, double) from /* optional */ pm1.g1 inner join pm1.g2 on pm1.g1.e1 = pm1.g2.e1", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] {"SELECT pm1.g2.e4 FROM pm1.g2", "SELECT pm1.g2.e2 FROM pm1.g2"} ); //$NON-NLS-1$ //$NON-NLS-2$

        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // Join
            0,      // MergeJoin
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            1       // UnionAll
        });
    }

    /**
     * The first branch should have the join removed, but not the second branch
     */
    @Test public void testOptionalJoinWithUnion1() {
        ProcessorPlan plan = TestOptimizer.helpPlan("select e4 from (select e4, e2 from (select pm1.g2.e4, pm1.g1.e2 from /* optional */ pm1.g1 inner join pm1.g2 on pm1.g1.e1 = pm1.g2.e1) as x union all select e4, e2 from (select convert(pm2.g1.e2, double) as e4, pm2.g2.e2 from /* optional */ pm2.g1 inner join pm2.g2 on pm2.g1.e1 = pm2.g2.e1) as x) as y", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] {"SELECT pm1.g2.e4 FROM pm1.g2", "SELECT g_0.e2 FROM pm2.g1 AS g_0, pm2.g2 AS g_1 WHERE g_0.e1 = g_1.e1"} ); //$NON-NLS-1$ //$NON-NLS-2$

        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // Join
            0,      // MergeJoin
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            1       // UnionAll
        });
    }

    @Test public void testOptionalJoinWithCompoundCriteria() {
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT length(z) FROM /* optional */ pm1.g1 inner join (select e2 as y, e3 || 'x' as z from pm1.g1 ORDER BY z) AS x on pm1.g1.e2=x.y and concat(x.y, x.z) = '1'", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] {"SELECT e3 FROM pm1.g1"} ); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // Join
            0,      // MergeJoin
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    @Test public void testOptionalJoinWithDupRemoval() {
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT a.e1 from (SELECT distinct pm1.g1.e1, x.y FROM pm1.g1, /* optional */ (select e2 as y, e3 || 'x' as z from pm1.g1 ORDER BY z) AS x where pm1.g1.e2=x.y) as a", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] {"SELECT DISTINCT g_0.e2 AS c_0 FROM pm1.g1 AS g_0 ORDER BY c_0", "SELECT DISTINCT g_0.e2 AS c_0, g_0.e1 AS c_1 FROM pm1.g1 AS g_0 ORDER BY c_0"} ); //$NON-NLS-1$ //$NON-NLS-2$

        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            1,      // DupRemove
            0,      // Grouping
            0,      // Join
            1,      // MergeJoin
            0,      // Null
            0,      // PlanExecution
            2,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    /**
     * Cross Joins do not allow for join removal
     * This could be optimized though as an exists predicate
     */
    @Test public void testOptionalJoinWithoutHint_crossJoin() {
        ProcessorPlan plan = TestOptimizer
                .helpPlan(
                        "SELECT distinct pm1.g1.e1 from pm1.g1, pm1.g2", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
                        new String[] { "SELECT DISTINCT g_0.e1 FROM pm1.g1 AS g_0, pm1.g2 AS g_1" }); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    @Test public void testOptionalJoinWithoutHint_outerJoin() {
        ProcessorPlan plan = TestOptimizer
                .helpPlan(
                        "SELECT distinct pm1.g1.e2 from pm1.g1 left outer join pm1.g2 on (pm1.g1.e1 = pm1.g2.e1)", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
                        new String[] { "SELECT DISTINCT g_0.e2 FROM pm1.g1 AS g_0" }); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    @Test public void testOptionalJoinWithoutHint_aggregate() {
        ProcessorPlan plan = TestOptimizer
                .helpPlan(
                        "SELECT pm1.g1.e3, max(pm1.g1.e2) from pm1.g1 left outer join pm1.g2 on (pm1.g1.e1 = pm1.g2.e1) group by pm1.g1.e3", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
                        new String[] { "SELECT g_0.e3, g_0.e2 FROM pm1.g1 AS g_0" }); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, new int[] {
                1, // Access
                0, // DependentAccess
                0, // DependentSelect
                0, // DependentProject
                0, // DupRemove
                1, // Grouping
                0, // Join
                0, // MergeJoin
                0, // Null
                0, // PlanExecution
                1, // Project
                0, // Select
                0, // Sort
                0 // UnionAll
                });
    }

    /**
     * The average agg will prevent the join removal
     */
    @Test public void testOptionalJoinWithoutHint_aggregate1() {
        ProcessorPlan plan = TestOptimizer
                .helpPlan(
                        "SELECT pm1.g1.e3, avg(pm1.g1.e2) from pm1.g1 left outer join pm1.g2 on (pm1.g1.e1 = pm1.g2.e1) group by pm1.g1.e3", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
                        new String[] { "SELECT g_0.e3, g_0.e2 FROM pm1.g1 AS g_0 LEFT OUTER JOIN pm1.g2 AS g_1 ON g_0.e1 = g_1.e1" }); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, new int[] {
                1, // Access
                0, // DependentAccess
                0, // DependentSelect
                0, // DependentProject
                0, // DupRemove
                1, // Grouping
                0, // Join
                0, // MergeJoin
                0, // Null
                0, // PlanExecution
                1, // Project
                0, // Select
                0, // Sort
                0 // UnionAll
                });
    }

    @Test public void testOptionalJoinWithoutHint_union() {
        ProcessorPlan plan = TestOptimizer
                .helpPlan(
                        "SELECT pm1.g1.e3 from pm1.g1 left outer join pm1.g2 on (pm1.g1.e1 = pm1.g2.e1) union select 1", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
                        new String[] { "SELECT g_0.e3 FROM pm1.g1 AS g_0" }); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, new int[] {
                1, // Access
                0, // DependentAccess
                0, // DependentSelect
                0, // DependentProject
                1, // DupRemove
                0, // Grouping
                0, // Join
                0, // MergeJoin
                0, // Null
                0, // PlanExecution
                2, // Project
                0, // Select
                0, // Sort
                1 // UnionAll
                });
    }

    @Test public void testOptionalJoinWithOrderedLimit() {
        ProcessorPlan plan = TestOptimizer
                .helpPlan(
                        "select distinct * from (SELECT pm1.g1.e3 from pm1.g1 left outer join pm1.g2 on (pm1.g1.e1 = pm1.g2.e1) order by e3 limit 10) x", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
                        new String[] { "SELECT g_0.e3 AS c_0 FROM pm1.g1 AS g_0 LEFT OUTER JOIN pm1.g2 AS g_1 ON g_0.e1 = g_1.e1 ORDER BY c_0" }); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, new int[] {
                1, // Access
                0, // DependentAccess
                0, // DependentSelect
                0, // DependentProject
                1, // DupRemove
                0, // Grouping
                0, // Join
                0, // MergeJoin
                0, // Null
                0, // PlanExecution
                1, // Project
                0, // Select
                0, // Sort
                0  // UnionAll
                });
    }

    @Test public void testOptionalJoinNodeStar() throws Exception {
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT g2.e1 FROM /* optional */ ( /* optional */ pm1.g1 as g1 makedep INNER JOIN /* optional */ pm2.g2 ON g1.e1 = pm2.g2.e1) makedep INNER JOIN /* optional */ pm2.g3 ON g1.e1 = pm2.g3.e1", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] {"SELECT g_0.e1 FROM pm2.g2 AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING ); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    @Test public void testOptionalJoinNodeStarTransitiveAnsi() throws Exception {
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT g3.e1 FROM ( /* optional */ pm1.g1 as g1 makedep INNER JOIN /* optional */ pm2.g2 ON g1.e1 = pm2.g2.e1) makedep INNER JOIN /* optional */ pm2.g3 ON g1.e1 = pm2.g3.e1", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] {"SELECT g_0.e1 FROM pm2.g3 AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING ); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    @Test public void testOptionalJoinNodeStarNonAnsi() throws Exception {
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT g3.e1 FROM /* optional */ pm1.g1 as g1, /* optional */ pm2.g2, /* optional */ pm2.g3 WHERE g1.e1 = pm2.g2.e1 AND g1.e1 = pm2.g3.e1", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] {"SELECT g_0.e1 FROM pm2.g3 AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING ); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    @Test public void testOptionalJoinNodeBridgeNonAnsi() throws Exception {
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT g3.e1 FROM /* optional */ pm1.g1 as g1 makedep, pm2.g2, /* optional */ pm2.g3 WHERE g1.e1 = pm2.g2.e1 AND g1.e1 = pm2.g3.e1", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] {"SELECT g_1.e1 AS c_0, g_0.e1 AS c_1 FROM pm2.g2 AS g_0, pm2.g3 AS g_1 WHERE g_1.e1 = g_0.e1 ORDER BY c_0",
            "SELECT g_0.e1 AS c_0 FROM pm1.g1 AS g_0 WHERE g_0.e1 IN (<dependent values>) ORDER BY c_0"}, ComparisonMode.EXACT_COMMAND_STRING ); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, new int[] {
                1, // Access
                1, // DependentAccess
                0, // DependentSelect
                0, // DependentProject
                0, // DupRemove
                0, // Grouping
                0, // Join
                1, // MergeJoin
                0, // Null
                0, // PlanExecution
                1, // Project
                0, // Select
                0, // Sort
                0  // UnionAll
                });
    }

    @Test public void testOptionalUsingFk() throws Exception {
        TransformationMetadata tm = RealMetadataFactory.fromDDL("create foreign table t1 (col1 integer primary key, col2 varchar);"
                + " create foreign table t2 (col1 integer primary key, col2 integer, FOREIGN KEY (col2) REFERENCES t1 (col1), FOREIGN KEY (col1) REFERENCES t3 (col1));"
                + " create foreign table t3 (col1 integer primary key, col2 integer);", "x", "y");
        ProcessorPlan plan = TestOptimizer.helpPlan("select t2.* from t1 inner join t2 on t1.col1 = t2.col2", //$NON-NLS-1$
                tm, new String[] {"SELECT g_0.col1, g_0.col2 FROM y.t2 AS g_0 WHERE g_0.col2 IS NOT NULL"}, ComparisonMode.EXACT_COMMAND_STRING ); //$NON-NLS-1$ //$NON-NLS-2$

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);

        plan = TestOptimizer.helpPlan("select t1.*, t2.* from t1 inner join t2 on t1.col1 = t2.col2 inner join t3 on t2.col1 = t3.col1", //$NON-NLS-1$
                tm, new String[] {"SELECT g_0.col1, g_0.col2, g_1.col1, g_1.col2 FROM y.t1 AS g_0, y.t2 AS g_1 WHERE (g_0.col1 = g_1.col2) AND (g_1.col1 IS NOT NULL)"}, ComparisonMode.EXACT_COMMAND_STRING ); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testNotOptionalUsingFk() throws Exception {
        TransformationMetadata tm = RealMetadataFactory.fromDDL("create foreign table t1 (col1 integer primary key, col2 varchar);"
                + " create foreign table t2 (col1 integer primary key, col2 integer, FOREIGN KEY (col2) REFERENCES t1 (col1));", "x", "y");

        ProcessorPlan plan = TestOptimizer.helpPlan("select t2.* from t1 inner join t2 on t1.col1 = t2.col2 and t1.col1 > 100", //$NON-NLS-1$
                tm, new String[] {"SELECT g_1.col1, g_1.col2 FROM y.t1 AS g_0, y.t2 AS g_1 WHERE (g_0.col1 = g_1.col2) AND (g_0.col1 > 100)"}, ComparisonMode.EXACT_COMMAND_STRING ); //$NON-NLS-1$ //$NON-NLS-2$

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);

        plan = TestOptimizer.helpPlan("select t2.* from t1 inner join t2 on t1.col1 = t2.col2 and t1.col2 = t2.col1", //$NON-NLS-1$
                tm, new String[] {"SELECT g_0.col2, g_1.col1, g_1.col2 FROM y.t1 AS g_0, y.t2 AS g_1 WHERE g_0.col1 = g_1.col2"}, ComparisonMode.EXACT_COMMAND_STRING ); //$NON-NLS-1$ //$NON-NLS-2$


        plan = TestOptimizer.helpPlan("select t2.* from t1 inner join t2 on t1.col1 + 1 = t2.col2", //$NON-NLS-1$
                tm, new String[] {"SELECT g_0.col2 AS c_0, g_0.col1 AS c_1 FROM y.t2 AS g_0 ORDER BY c_0", "SELECT g_0.col1 FROM y.t1 AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING ); //$NON-NLS-1$ //$NON-NLS-2$

    }

    @Test public void testSelfJoin() throws Exception {
        TransformationMetadata tm = RealMetadataFactory.fromDDL("create foreign table t1 (id integer primary key, parentid integer, FOREIGN KEY (parentid) REFERENCES t1 (id));", "x", "y");
        ProcessorPlan plan = TestOptimizer.helpPlan("select a1.id from t1 a1 inner join t1 a2 on a1.parentid = a2.id", //$NON-NLS-1$
                tm, new String[] {"SELECT g_0.id FROM y.t1 AS g_0 WHERE g_0.parentid IS NOT NULL"}, ComparisonMode.EXACT_COMMAND_STRING ); //$NON-NLS-1$ //$NON-NLS-2$

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

}
