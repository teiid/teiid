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

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.TestOptimizer.ComparisonMode;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.FakeCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.processor.FakeDataManager;
import org.teiid.query.processor.FakeDataStore;
import org.teiid.query.processor.HardcodedDataManager;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.TestProcessor;
import org.teiid.query.processor.relational.LimitNode;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings("nls")
public class TestUnionPlanning {

    @Test public void testUnionPushDown() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_UNION, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("BQT2", caps); //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT IntKey FROM BQT1.SmallA UNION ALL SELECT IntNum FROM BQT2.SmallA UNION ALL SELECT IntNum FROM BQT1.SmallA", RealMetadataFactory.exampleBQTCached(), null, capFinder,//$NON-NLS-1$
            new String[] { "SELECT IntNum FROM BQT2.SmallA", "SELECT IntKey FROM BQT1.SmallA UNION ALL SELECT IntNum FROM BQT1.SmallA" }, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$ //$NON-NLS-2$

        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            0,      // Project
            0,      // Select
            0,      // Sort
            1       // UnionAll
        });
    }

    /**
     * Here the change in the all causes us not to pushdown
     */
    @Test public void testUnionPushDown1() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_UNION, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("BQT2", caps); //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT IntKey FROM BQT1.SmallA UNION SELECT IntNum FROM BQT2.SmallA UNION ALL SELECT IntNum FROM BQT1.SmallA", RealMetadataFactory.exampleBQTCached(), null, capFinder,//$NON-NLS-1$
            new String[] { "SELECT IntNum FROM BQT2.SmallA", "SELECT IntKey FROM BQT1.SmallA", "SELECT IntNum FROM BQT1.SmallA" }, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

        TestOptimizer.checkNodeTypes(plan, new int[] {
            3,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            1,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            0,      // Project
            0,      // Select
            0,      // Sort
            2       // UnionAll
        });
    }

    @Test public void testUnionPushDown2() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_UNION, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("BQT2", caps); //$NON-NLS-1$
        BasicSourceCapabilities caps1 = TestOptimizer.getTypicalCapabilities();
        capFinder.addCapabilities("BQT3", caps1); //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT IntKey FROM BQT1.SmallA UNION ALL SELECT IntNum FROM BQT2.SmallA UNION ALL SELECT IntNum FROM BQT1.SmallA UNION ALL SELECT IntNum FROM BQT3.SmallA UNION ALL SELECT IntNum FROM BQT2.SmallA", RealMetadataFactory.exampleBQTCached(), null, capFinder,//$NON-NLS-1$
            new String[] { "SELECT IntNum FROM BQT2.SmallA UNION ALL SELECT IntNum FROM BQT2.SmallA", "SELECT IntNum FROM BQT3.SmallA", "SELECT IntKey FROM BQT1.SmallA UNION ALL SELECT IntNum FROM BQT1.SmallA" }, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

        TestOptimizer.checkNodeTypes(plan, new int[] {
            3,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            0,      // Project
            0,      // Select
            0,      // Sort
            1       // UnionAll
        });
    }

    public void testUnionPushDown3() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_UNION, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("BQT2", caps); //$NON-NLS-1$
        BasicSourceCapabilities caps1 = TestOptimizer.getTypicalCapabilities();
        capFinder.addCapabilities("BQT3", caps1); //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT IntKey FROM BQT1.SmallA UNION ALL SELECT IntNum FROM BQT2.SmallA UNION ALL SELECT IntNum FROM BQT1.SmallA UNION ALL SELECT IntNum FROM BQT3.SmallA UNION ALL (SELECT IntNum FROM BQT2.SmallA UNION ALL SELECT IntNum FROM BQT2.SmallA)", RealMetadataFactory.exampleBQTCached(), null, capFinder,//$NON-NLS-1$
            new String[] { "SELECT IntNum FROM BQT3.SmallA", "SELECT IntNum FROM BQT2.SmallA UNION ALL (SELECT IntNum FROM BQT2.SmallA UNION ALL SELECT IntNum FROM BQT2.SmallA)", "SELECT IntKey FROM BQT1.SmallA UNION ALL SELECT IntNum FROM BQT1.SmallA" }, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

        TestOptimizer.checkNodeTypes(plan, new int[] {
            3,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            0,      // Project
            0,      // Select
            0,      // Sort
            2       // UnionAll
        });
    }

    @Test public void testUnionPushDownWithJoin() {
        ProcessorPlan plan = TestOptimizer.helpPlan("select b.*,a.* from (SELECT IntKey FROM BQT1.SmallA where intkey in (1, 2) UNION ALL SELECT intkey FROM BQT2.SmallA where intkey in (3, 4)) A inner join (SELECT intkey FROM BQT1.SmallB where intkey in (1, 2) UNION ALL SELECT intkey FROM BQT2.SmallB where intkey in (3, 4)) B on a.intkey = b.intkey", RealMetadataFactory.exampleBQTCached(), null, TestOptimizer.getGenericFinder(),//$NON-NLS-1$
            new String[] { "SELECT g_1.intkey, g_0.intkey FROM BQT2.SmallA AS g_0, BQT2.SmallB AS g_1 WHERE (g_0.intkey = g_1.intkey) AND (g_0.intkey IN (3, 4)) AND (g_1.intkey IN (3, 4))",
            "SELECT g_1.intkey, g_0.IntKey FROM BQT1.SmallA AS g_0, BQT1.SmallB AS g_1 WHERE (g_0.IntKey = g_1.intkey) AND (g_0.intkey IN (1, 2)) AND (g_1.intkey IN (1, 2))" }, TestOptimizer.SHOULD_SUCCEED);

        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            0,      // Project
            0,      // Select
            0,      // Sort
            1       // UnionAll
        });
    }

    @Test public void testUnionPushDownWithJoinNonAnsi() {
        ProcessorPlan plan = TestOptimizer.helpPlan("select * from (SELECT IntKey FROM BQT1.SmallA where intkey in (1, 2) UNION ALL SELECT intkey FROM BQT2.SmallA where intkey in (3, 4)) A, (SELECT intkey FROM BQT1.SmallB where intkey in (1, 2) UNION ALL SELECT intkey FROM BQT2.SmallB where intkey in (3, 4)) B where a.intkey = b.intkey", RealMetadataFactory.exampleBQTCached(), null, TestOptimizer.getGenericFinder(),//$NON-NLS-1$
            new String[] { "SELECT g_0.intkey, g_1.intkey FROM BQT2.SmallA AS g_0, BQT2.SmallB AS g_1 WHERE (g_0.intkey = g_1.intkey) AND (g_0.intkey IN (3, 4)) AND (g_1.intkey IN (3, 4))",
            "SELECT g_0.intkey, g_1.IntKey FROM BQT1.SmallA AS g_0, BQT1.SmallB AS g_1 WHERE (g_0.IntKey = g_1.intkey) AND (g_0.intkey IN (1, 2)) AND (g_1.intkey IN (1, 2))" }, TestOptimizer.SHOULD_SUCCEED);

        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            0,      // Project
            0,      // Select
            0,      // Sort
            1       // UnionAll
        });
    }

    @Test public void testUnionPushDownWithViewAliases() throws Exception {
        TransformationMetadata tm = RealMetadataFactory.fromDDL("create foreign table smalla (intkey integer); "
                + " create foreign table smallb (intkey integer);"
                + " create view v as SELECT 1 as part, IntKey FROM SmallA as t1 UNION ALL "
                + "  SELECT 2 as part, intkey FROM SmallB as t2", "x", "y");

        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);

        ProcessorPlan plan = TestOptimizer.helpPlan(
                "select t2.*,t1.* from v as t1 inner join v as t2 on t1.part = t2.part and t1.intkey = t2.intkey", tm,
                new String[] {"SELECT g_1.intkey, g_0.intkey FROM y.smalla AS g_0, y.smalla AS g_1 WHERE g_0.intkey = g_1.intkey",
                        "SELECT g_1.intkey, g_0.intkey FROM y.smallb AS g_0, y.smallb AS g_1 WHERE g_0.intkey = g_1.intkey"}, new DefaultCapabilitiesFinder(caps), ComparisonMode.EXACT_COMMAND_STRING);

        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            0,      // Project
            0,      // Select
            0,      // Sort
            1       // UnionAll
        });
    }

    @Test public void testUnionPushDownWithJoinNoMatches() {
        TestOptimizer.helpPlan("select * from (SELECT IntKey FROM BQT1.SmallA where intkey in (1, 2) UNION ALL SELECT intkey FROM BQT2.SmallA where intkey in (3, 4)) A inner join (SELECT intkey FROM BQT1.SmallB where intkey in (5, 6) UNION ALL SELECT intkey FROM BQT2.SmallB where intkey in (7, 8)) B on a.intkey = b.intkey", RealMetadataFactory.exampleBQTCached(), null, TestOptimizer.getGenericFinder(),//$NON-NLS-1$
            new String[] {}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$
    }

    @Test public void testUnionPushDownWithJoin1() throws Exception {
        ProcessorPlan plan = TestOptimizer.helpPlan("select b.*,a.* from (SELECT IntKey FROM BQT1.SmallA where intkey in (1, 2) UNION ALL SELECT intkey FROM BQT2.SmallA where intkey in (3, 4)) A inner join (SELECT intkey FROM BQT1.SmallB where intkey in (1, 2) UNION ALL SELECT intkey FROM BQT2.SmallB where intkey in (3, 4)) B on a.intkey = b.intkey where a.intkey in (1, 4)", RealMetadataFactory.exampleBQTCached(), null, TestOptimizer.getGenericFinder(),//$NON-NLS-1$
            new String[] { "SELECT g_1.IntKey, g_0.IntKey FROM BQT1.SmallA AS g_0, BQT1.SmallB AS g_1 WHERE (g_0.IntKey = g_1.IntKey) AND (g_0.IntKey IN (1)) AND (g_0.IntKey = 1) AND (g_1.IntKey = 1)"
            , "SELECT g_1.IntKey, g_0.IntKey FROM BQT2.SmallA AS g_0, BQT2.SmallB AS g_1 WHERE (g_0.IntKey = g_1.IntKey) AND (g_0.IntKey IN (4)) AND (g_0.IntKey = 4) AND (g_1.IntKey = 4)" }, ComparisonMode.EXACT_COMMAND_STRING);

        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            0,      // Project
            0,      // Select
            0,      // Sort
            1       // UnionAll
        });
    }

    @Test public void testUnionWithPartitionedAggregate() throws Exception {
        ProcessorPlan plan = TestOptimizer.helpPlan("select max(intnum) from (SELECT IntKey, intnum FROM BQT1.SmallA where intkey in (1, 2) UNION ALL SELECT intkey, intnum FROM BQT2.SmallA where intkey in (3, 4)) A group by intkey", RealMetadataFactory.exampleBQTCached(), null, TestInlineView.getInliveViewCapabilitiesFinder(),//$NON-NLS-1$
            new String[] { "SELECT MAX(g_0.IntNum) FROM BQT1.SmallA AS g_0 WHERE g_0.IntKey IN (1, 2) GROUP BY g_0.IntKey", "SELECT MAX(g_0.IntNum) FROM BQT2.SmallA AS g_0 WHERE g_0.IntKey IN (3, 4) GROUP BY g_0.IntKey" }, ComparisonMode.EXACT_COMMAND_STRING);

        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
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
            1       // UnionAll
        });
    }

    @Test public void testUnionWithUnnecessaryGroupBy() throws Exception {
        ProcessorPlan plan = TestOptimizer.helpPlan("select intkey from (SELECT IntKey, intnum FROM BQT1.SmallA UNION ALL SELECT intkey, intnum FROM BQT2.SmallA) A group by intkey", RealMetadataFactory.exampleBQTCached(), null, TestInlineView.getInliveViewCapabilitiesFinder(),//$NON-NLS-1$
            new String[] { "SELECT g_0.IntKey FROM BQT1.SmallA AS g_0", "SELECT g_0.IntKey FROM BQT2.SmallA AS g_0" }, ComparisonMode.EXACT_COMMAND_STRING);

        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            1,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            1       // UnionAll
        });
    }

    @Test public void testUnionWithUnnecessaryGroupByPartitionedConstant() throws Exception {
        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        bsc.setCapabilitySupport(Capability.ROW_LIMIT, true);
        DefaultCapabilitiesFinder capFinder = new DefaultCapabilitiesFinder(bsc);
        ProcessorPlan plan = TestOptimizer.helpPlan("select intkey from (SELECT 1 as IntKey, intnum FROM BQT1.SmallA UNION ALL SELECT 2 as intkey, intnum FROM BQT2.SmallA) A group by intkey", RealMetadataFactory.exampleBQTCached(), null, capFinder,//$NON-NLS-1$
            new String[] { "SELECT 1 AS c_0 FROM BQT2.SmallA AS g_0 LIMIT 1", "SELECT 1 AS c_0 FROM BQT1.SmallA AS g_0 LIMIT 1" }, ComparisonMode.EXACT_COMMAND_STRING);

        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            3,      // Project
            0,      // Select
            0,      // Sort
            1       // UnionAll
        });
    }

    @Test public void testUnionPartitionedWithMerge() throws Exception {
        //"select max(intnum) from (select * from (SELECT IntKey, intnum FROM BQT1.SmallA where intkey in (1, 2) UNION ALL SELECT intkey, intnum FROM BQT2.SmallA where intkey in (3, 4)) A where intkey in (1, 2, 3, 4) UNION ALL select intkey, intnum from bqt2.smallb where intkey in 6) B group by intkey"
        ProcessorPlan plan = TestOptimizer.helpPlan("select c.*,b.* from (select * from (SELECT IntKey, intnum FROM BQT1.SmallA UNION ALL SELECT intkey, intnum FROM BQT2.SmallA) A where intkey in (1, 2, 3, 4) UNION ALL select intkey, intnum from bqt2.smallb where intkey in (6)) B inner join (SELECT IntKey, intnum FROM BQT1.SmallA where intkey in (1, 2) UNION ALL SELECT intkey, intnum FROM BQT2.SmallA where intkey in (5, 6)) C on b.intkey = c.intkey", RealMetadataFactory.exampleBQTCached(), null, TestInlineView.getInliveViewCapabilitiesFinder(),//$NON-NLS-1$
            new String[] { "SELECT g_0.IntKey AS c_0, g_0.IntNum AS c_1 FROM BQT1.SmallA AS g_0 WHERE g_0.IntKey IN (1, 2) ORDER BY c_0",
            "SELECT g_0.IntKey, g_0.IntNum FROM BQT1.SmallA AS g_0 WHERE g_0.IntKey IN (1, 2)",
            "SELECT g_0.IntKey, g_0.IntNum FROM BQT2.SmallA AS g_0 WHERE g_0.IntKey IN (1, 2)",
            "SELECT g_1.IntKey, g_1.IntNum, g_0.IntKey, g_0.IntNum FROM BQT2.SmallB AS g_0, BQT2.SmallA AS g_1 WHERE (g_0.IntKey = g_1.IntKey) AND (g_0.IntKey = 6) AND (g_1.IntKey = 6)" }, ComparisonMode.EXACT_COMMAND_STRING);

        TestOptimizer.checkNodeTypes(plan, new int[] {
            4,      // Access
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
            2       // UnionAll
        });
    }

    @Test public void testUnionCosting() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1();
        RealMetadataFactory.setCardinality("pm1.g1", 100, metadata);
        RealMetadataFactory.setCardinality("pm1.g2", 100, metadata);
        RealMetadataFactory.setCardinality("pm1.g3", 100, metadata);
        RealMetadataFactory.setCardinality("pm1.g4", 100, metadata);
        BasicSourceCapabilities bac = new BasicSourceCapabilities();
        bac.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        bac.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT T.e1 AS e1, T.e2 AS e2, T.e3 AS e3 FROM (SELECT e1, 'a' AS e2, e3 FROM pm1.g1 UNION SELECT e1, 'b' AS e2, e3 FROM pm1.g2 UNION SELECT e1, 'c' AS e2, e3 FROM pm1.g3) AS T, vm1.g1 AS L WHERE (T.e1 = L.e1) AND (T.e3 = TRUE)", metadata, null, new DefaultCapabilitiesFinder(bac),//$NON-NLS-1$
            new String[] { "SELECT pm1.g1.e1 FROM pm1.g1", "SELECT pm1.g1.e1, pm1.g1.e3 FROM pm1.g1 WHERE pm1.g1.e3 = TRUE", "SELECT pm1.g3.e1, pm1.g3.e3 FROM pm1.g3 WHERE pm1.g3.e3 = TRUE", "SELECT pm1.g2.e1, pm1.g2.e3 FROM pm1.g2 WHERE pm1.g2.e3 = TRUE" }, ComparisonMode.EXACT_COMMAND_STRING);

        TestOptimizer.checkNodeTypes(plan, new int[] {
            4,      // Access
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
            2       // UnionAll
        });
    }

    @Test public void testUnionPartitionedDistinct() throws Exception {
        ProcessorPlan plan = TestOptimizer.helpPlan("select distinct * from (SELECT 1 as IntKey, intnum FROM BQT1.SmallA UNION ALL SELECT 2 as intkey, intnum FROM BQT2.SmallA) A", RealMetadataFactory.exampleBQTCached(), null, TestInlineView.getInliveViewCapabilitiesFinder(),//$NON-NLS-1$
            new String[] { "SELECT DISTINCT g_0.IntNum FROM BQT2.SmallA AS g_0", "SELECT DISTINCT g_0.IntNum FROM BQT1.SmallA AS g_0" }, ComparisonMode.EXACT_COMMAND_STRING);

        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            0,      // Project
            0,      // Select
            0,      // Sort
            1       // UnionAll
        });
    }

    @Test public void testUnionPartitioningWithOrderedLimits() throws Exception {
        String sql = "select * from ((select e1, e2, 'a' source from pm1.g1 order by e2 desc limit 5000)"
                + " union all (select e1, e2, 'b' source from pm2.g2 order by e2 desc limit 5000)) x"
                + " where source in ('b') order by e2 desc limit 0, 500";

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), null, TestOptimizer.getGenericFinder(),//$NON-NLS-1$
                new String[] { "SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm2.g2 AS g_0 ORDER BY c_1 DESC" }, ComparisonMode.EXACT_COMMAND_STRING);

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
                0,      // Project
                0,      // Select
                1,      // Sort
                0       // UnionAll
            });

    }

    @Test public void testUnionWithOrderedLimits1() throws Exception {
        String sql = "select * from ((select e1, e2, 'a' source from pm1.g1 order by e2 desc limit 5000)"
                + " union all (select e1, e2, 'b' source from pm2.g2 order by e2 desc limit 5000)"
                + " union all (select e1, e2, 'c' source from pm1.g3 order by e2 desc limit 5000)) x"
                + " order by e2 desc limit 0, 500";

        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), null, new DefaultCapabilitiesFinder(caps),//$NON-NLS-1$
                new String[] { "SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm1.g1 AS g_0 ORDER BY c_1 DESC LIMIT 500",
                         "SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm2.g2 AS g_0 ORDER BY c_1 DESC LIMIT 500",
                         "SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm1.g3 AS g_0 ORDER BY c_1 DESC LIMIT 500" }, ComparisonMode.EXACT_COMMAND_STRING);

        TestOptimizer.checkNodeTypes(plan, new int[] {
                3,      // Access
                0,      // DependentAccess
                0,      // DependentSelect
                0,      // DependentProject
                0,      // DupRemove
                0,      // Grouping
                0,      // NestedLoopJoinStrategy
                0,      // MergeJoinStrategy
                0,      // Null
                0,      // PlanExecution
                0,      // Project
                0,      // Select
                1,      // Sort
                1       // UnionAll
            });


        TestOptimizer.checkNodeTypes(plan, new int[] {1}, new Class<?>[] {LimitNode.class});
    }

    //here the second branch does not match the top level ordering, so the limit is not combined
    @Test public void testUnionWithOrderedLimits2() throws Exception {
        String sql = "select * from ((select e1, e2, 'a' source from pm1.g1 order by e2 desc limit 5000)"
                + " union all (select e1, e2, 'b' source from pm2.g2 order by e1 desc limit 5000)) x"
                + " order by e2 desc limit 0, 500";

        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), null, new DefaultCapabilitiesFinder(caps),//$NON-NLS-1$
                new String[] { "SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm1.g1 AS g_0 ORDER BY c_1 DESC LIMIT 500"
            , "SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm2.g2 AS g_0 ORDER BY c_0 DESC LIMIT 5000" }, ComparisonMode.EXACT_COMMAND_STRING);

        TestOptimizer.checkNodeTypes(plan, new int[] {
                2,      // Access
                0,      // DependentAccess
                0,      // DependentSelect
                0,      // DependentProject
                0,      // DupRemove
                0,      // Grouping
                0,      // NestedLoopJoinStrategy
                0,      // MergeJoinStrategy
                0,      // Null
                0,      // PlanExecution
                0,      // Project
                0,      // Select
                1,      // Sort
                1       // UnionAll
            });

        TestOptimizer.checkNodeTypes(plan, new int[] {1}, new Class<?>[] {LimitNode.class});
    }

    @Test public void testUnionWithOrderedLimits3() throws Exception {
        String sql = "select * from ((select e1, e2, 'a' source from pm1.g1)"
                + " union all (select e1, e2, 'b' source from pm2.g2 limit 5000)) x"
                + " order by e2 desc limit 0, 500";

        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), null, new DefaultCapabilitiesFinder(caps),//$NON-NLS-1$
                new String[] { "SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm1.g1 AS g_0 ORDER BY c_1 DESC LIMIT 500", "SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm2.g2 AS g_0 ORDER BY c_1 DESC LIMIT 500" }, ComparisonMode.EXACT_COMMAND_STRING);

        TestOptimizer.checkNodeTypes(plan, new int[] {
                2,      // Access
                0,      // DependentAccess
                0,      // DependentSelect
                0,      // DependentProject
                0,      // DupRemove
                0,      // Grouping
                0,      // NestedLoopJoinStrategy
                0,      // MergeJoinStrategy
                0,      // Null
                0,      // PlanExecution
                0,      // Project
                0,      // Select
                1,      // Sort
                1       // UnionAll
            });

        TestOptimizer.checkNodeTypes(plan, new int[] {1}, new Class<?>[] {LimitNode.class});
    }

    @Test public void testUnionWithOrderedLimits4() throws Exception {
        String sql = "select * from ((select e1, e2, 'a' source from pm1.g1)"
                + " union all (select e1, e2, 'b' source from pm2.g2 limit 5000)) x"
                + " order by e2 desc limit 0, 500";

        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, false);

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), null, new DefaultCapabilitiesFinder(caps),//$NON-NLS-1$
                new String[] { "SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm2.g2 AS g_0 LIMIT 5000", "SELECT g_0.e1, g_0.e2 FROM pm1.g1 AS g_0" }, ComparisonMode.EXACT_COMMAND_STRING);

        TestOptimizer.checkNodeTypes(plan, new int[] {
                2,      // Access
                0,      // DependentAccess
                0,      // DependentSelect
                0,      // DependentProject
                0,      // DupRemove
                0,      // Grouping
                0,      // NestedLoopJoinStrategy
                0,      // MergeJoinStrategy
                0,      // Null
                0,      // PlanExecution
                0,      // Project
                0,      // Select
                2,      // Sort
                1       // UnionAll
            });

    }

    @Test public void testCriteriaRewrite() throws Exception {
        String sql = "select * from (select e1, e2 from pm1.g1 union all select convert(e2, string), e2 from pm1.g2) x where e1 in ('1', '2')";

        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, false);

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), null, new DefaultCapabilitiesFinder(caps),//$NON-NLS-1$
                new String[] { "SELECT g_0.e2 FROM pm1.g2 AS g_0 WHERE g_0.e2 IN (1, 2)", "SELECT g_0.e1, g_0.e2 FROM pm1.g1 AS g_0 WHERE g_0.e1 IN ('1', '2')" }, ComparisonMode.EXACT_COMMAND_STRING);

        TestOptimizer.checkNodeTypes(plan, new int[] {
                2,      // Access
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
                1       // UnionAll
            });

    }

    @Test public void testCostingWithGroupingAndOrder() throws Exception {
        String sql = "select e1 as admissionid,e2 as patgroup,e3 as ward,e4 as admtime, 'wh' as origin from pm1.g1 gd "
                + " group by e1,e2,e3,e4 UNION ALL select e1,e2,e3,e4, 'prod' from pm1.g2 gd"
                + " group by e1,e2,e3,e4 order by admtime limit 1";

        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, false);

        TransformationMetadata tm = RealMetadataFactory.example1();
        RealMetadataFactory.setCardinality("pm1.g1", 1000, tm);
        RealMetadataFactory.setCardinality("pm1.g2", 1000, tm);

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, tm, null, new DefaultCapabilitiesFinder(caps),//$NON-NLS-1$
                new String[] { "SELECT g_0.e1, g_0.e2, g_0.e3, g_0.e4 FROM pm1.g2 AS g_0", "SELECT g_0.e1, g_0.e2, g_0.e3, g_0.e4 FROM pm1.g1 AS g_0" }, ComparisonMode.EXACT_COMMAND_STRING);

        FakeDataManager dataManager = new FakeDataManager();
        FakeDataStore.sampleData2(dataManager);
        TestProcessor.helpProcess(plan, dataManager, new List[] {Arrays.asList("b",1,true,null,"wh")} );
    }

    //TODO: enhancement for ordering over a partition

    @Test public void testPreserveGroupingOverUnion() throws TeiidComponentException, TeiidProcessingException {
        String sql = "select y.col2 from ( select x.col2, min(x.col1) as col1 from ( select 1 as col2, col1 from "
                + "(select 'a' as col1 UNION SELECT '' as col1) v1 union select 1 as col2, col1 from (select 'b' as col1 UNION SELECT '' as col1) v2) x group by x.col2 ) y";
        TransformationMetadata tm = RealMetadataFactory.example1Cached();
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, tm, null, new DefaultCapabilitiesFinder(),//$NON-NLS-1$
                new String[] {}, ComparisonMode.EXACT_COMMAND_STRING);

        TestProcessor.helpProcess(plan, new HardcodedDataManager(), new List[] {Arrays.asList(1)} );
    }

    @Test public void testImplictPartitionwiseStarJoin() throws Exception {
        TransformationMetadata tm = partitionedStartSchema();

        String sql = "select * from combined_fact, combined_dim1, combined_dim2 where combined_fact.dim_id1 = combined_dim1.id and combined_fact.dim_id2 = combined_dim2.id";

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, tm, null, TestOptimizer.getGenericFinder(),//$NON-NLS-1$
            new String[] { "SELECT g_0.id, g_0.dim_id1, g_0.dim_id2, g_1.id, g_1.val, g_2.id, g_2.val FROM source1.fact AS g_0, source1.dim1 AS g_1, source1.dim2 AS g_2 WHERE (g_0.dim_id1 = g_1.id) AND (g_0.dim_id2 = g_2.id)",
                    "SELECT g_0.id, g_0.dim_id1, g_0.dim_id2, g_1.id, g_1.val, g_2.id, g_2.val FROM source2.fact AS g_0, source2.dim1 AS g_1, source2.dim2 AS g_2 WHERE (g_0.dim_id1 = g_1.id) AND (g_0.dim_id2 = g_2.id)" }, ComparisonMode.EXACT_COMMAND_STRING);

        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            0,      // Project
            0,      // Select
            0,      // Sort
            1       // UnionAll
        });

        HardcodedDataManager dataManager = new HardcodedDataManager();

        dataManager.addData("SELECT g_0.id, g_0.dim_id1, g_0.dim_id2, g_1.id, g_1.val, g_2.id, g_2.val FROM source1.fact AS g_0, source1.dim1 AS g_1, source1.dim2 AS g_2 WHERE (g_0.dim_id1 = g_1.id) AND (g_0.dim_id2 = g_2.id)",
                Arrays.asList(1, 1, 1, 1, "abc", 1, "def"));
        dataManager.addData("SELECT g_0.id, g_0.dim_id1, g_0.dim_id2, g_1.id, g_1.val, g_2.id, g_2.val FROM source2.fact AS g_0, source2.dim1 AS g_1, source2.dim2 AS g_2 WHERE (g_0.dim_id1 = g_1.id) AND (g_0.dim_id2 = g_2.id)",
                Arrays.asList(10, 10, 10, 10, "2abc", 10, "2def"));

        TestProcessor.helpProcess(plan, dataManager, new List[] {
                Arrays.asList(1, 1, 1, 1, 1, "abc", 1, 1, "def", 1), Arrays.asList(10, 10, 10, 2, 10, "2abc", 2, 10, "2def", 2)} );
    }

    private TransformationMetadata partitionedStartSchema()
            throws Exception, TeiidComponentException, QueryMetadataException {
        TransformationMetadata tm = RealMetadataFactory.fromDDL("x", new RealMetadataFactory.DDLHolder("source1",
                "create foreign table fact (id integer, dim_id1 integer, dim_id2 integer);"
                + "create foreign table dim1 (id integer, val string);"
                + "create foreign table dim2 (id integer, val string);"),
                new RealMetadataFactory.DDLHolder("source2",
                        "create foreign table fact (id integer, dim_id1 integer, dim_id2 integer);"
                        + "create foreign table dim1 (id integer, val string);"
                        + "create foreign table dim2 (id integer, val string);"),
                new RealMetadataFactory.DDLHolder("v",
                        "create view combined_fact as select id, dim_id1, dim_id2, 1 as part from source1.fact union all select id, dim_id1, dim_id2, 2 as part from source2.fact;"
                        + "create view combined_dim1 as select id, val, 1 as part from source1.dim1 union all select id, val, 2 as part from source2.dim1;"
                        + "create view combined_dim2 as select id, val, 1 as part from source1.dim2 union all select id, val, 2 as part from source2.dim2;")
                );

        tm.getModelID("v").setProperty("implicit_partition.columnName", "part");
        return tm;
    }

    @Test public void testImplicitPartionwiseStarJoinMinimalColumns() throws Exception {
        TransformationMetadata tm = partitionedStartSchema();

        String sql = "select combined_fact.id, combined_dim1.val, combined_dim2.val from combined_fact, combined_dim1, combined_dim2 where combined_fact.dim_id1 = combined_dim1.id and combined_fact.dim_id2 = combined_dim2.id";

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, tm, null, TestOptimizer.getGenericFinder(),//$NON-NLS-1$
                new String[] { "SELECT g_0.id, g_1.val, g_2.val FROM source2.fact AS g_0, source2.dim1 AS g_1, source2.dim2 AS g_2 WHERE (g_0.dim_id1 = g_1.id) AND (g_0.dim_id2 = g_2.id)",
                        "SELECT g_0.id, g_1.val, g_2.val FROM source1.fact AS g_0, source1.dim1 AS g_1, source1.dim2 AS g_2 WHERE (g_0.dim_id1 = g_1.id) AND (g_0.dim_id2 = g_2.id)" }, ComparisonMode.EXACT_COMMAND_STRING);

        HardcodedDataManager dataManager = new HardcodedDataManager();

        dataManager.addData("SELECT g_0.id, g_1.val, g_2.val FROM source1.fact AS g_0, source1.dim1 AS g_1, source1.dim2 AS g_2 WHERE (g_0.dim_id1 = g_1.id) AND (g_0.dim_id2 = g_2.id)",
                Arrays.asList(1, "abc", "def"));
        dataManager.addData("SELECT g_0.id, g_1.val, g_2.val FROM source2.fact AS g_0, source2.dim1 AS g_1, source2.dim2 AS g_2 WHERE (g_0.dim_id1 = g_1.id) AND (g_0.dim_id2 = g_2.id)",
                Arrays.asList(10, "2abc", "2def"));

        TestProcessor.helpProcess(plan, dataManager, new List[] {
                Arrays.asList(1, "abc", "def"), Arrays.asList(10, "2abc", "2def")} );
    }

}
