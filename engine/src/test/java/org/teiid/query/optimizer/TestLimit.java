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
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.metadata.Column;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.query.mapping.relational.QueryNode;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.TestOptimizer.ComparisonMode;
import org.teiid.query.optimizer.TestOptimizer.DependentProjectNode;
import org.teiid.query.optimizer.TestOptimizer.DependentSelectNode;
import org.teiid.query.optimizer.TestOptimizer.DupRemoveSortNode;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.FakeCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.processor.FakeDataManager;
import org.teiid.query.processor.HardcodedDataManager;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.TestProcessor;
import org.teiid.query.processor.relational.*;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings("nls")
public class TestLimit {

    private static final int[] FULL_PUSHDOWN = new int[] {
                1,      // Access
                0,      // DependentAccess
                0,      // DependentSelect
                0,      // DependentProject
                0,      // DupRemove
                0,      // Grouping
                0,      // Limit
                0,      // NestedLoopJoinStrategy
                0,      // MergeJoinStrategy
                0,      // Null
                0,      // PlanExecution
                0,      // Project
                0,      // Select
                0,      // Sort
                0       // UnionAll
            };

    public static final Class<?>[] NODE_TYPES = new Class[] {
        AccessNode.class,
        DependentAccessNode.class,
        DependentSelectNode.class,
        DependentProjectNode.class,
        DupRemoveNode.class,
        GroupingNode.class,
        LimitNode.class,
        NestedLoopJoinStrategy.class,
        MergeJoinStrategy.class,
        NullNode.class,
        PlanExecutionNode.class,
        ProjectNode.class,
        SelectNode.class,
        SortNode.class,
        UnionAllNode.class
    };

    private static TransformationMetadata exampleMetadata() {
        MetadataStore metadataStore = new MetadataStore();
        // Create models
        Schema pm1 = RealMetadataFactory.createPhysicalModel("pm1", metadataStore); //$NON-NLS-1$
        Schema vm1 = RealMetadataFactory.createVirtualModel("vm1", metadataStore);  //$NON-NLS-1$

        // Create physical groups
        Table pm1g1 = RealMetadataFactory.createPhysicalGroup("g1", pm1); //$NON-NLS-1$
        Table pm1g2 = RealMetadataFactory.createPhysicalGroup("g2", pm1); //$NON-NLS-1$
        Table pm1g3 = RealMetadataFactory.createPhysicalGroup("g3", pm1); //$NON-NLS-1$
        Table pm1g4 = RealMetadataFactory.createPhysicalGroup("g4", pm1); //$NON-NLS-1$
        Table pm1g5 = RealMetadataFactory.createPhysicalGroup("g5", pm1); //$NON-NLS-1$
        Table pm1g6 = RealMetadataFactory.createPhysicalGroup("g6", pm1); //$NON-NLS-1$

        // Create physical elements
        RealMetadataFactory.createElements(pm1g1,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        RealMetadataFactory.createElements(pm1g2,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        RealMetadataFactory.createElements(pm1g3,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        List<Column> pm1g4e = RealMetadataFactory.createElements(pm1g4,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        pm1g4e.get(1).setSelectable(false);
        pm1g4e.get(3).setSelectable(false);
        List<Column> pm1g5e = RealMetadataFactory.createElements(pm1g5,
            new String[] { "e1" }, //$NON-NLS-1$
            new String[] { DataTypeManager.DefaultDataTypes.STRING });
        pm1g5e.get(0).setSelectable(false);
        RealMetadataFactory.createElements(pm1g6,
            new String[] { "in", "in3" }, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING });

        // Create virtual groups
        QueryNode vm1g1n1 = new QueryNode("SELECT * FROM pm1.g1 LIMIT 100"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g1 = RealMetadataFactory.createVirtualGroup("g1", vm1, vm1g1n1); //$NON-NLS-1$

        // Create virtual elements
        RealMetadataFactory.createElements(vm1g1,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });

        QueryNode vm1g2n1 = new QueryNode("SELECT * FROM vm1.g1 ORDER BY e1"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g2 = RealMetadataFactory.createVirtualGroup("g2", vm1, vm1g2n1); //$NON-NLS-1$

        // Create virtual elements
        RealMetadataFactory.createElements(vm1g2,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });

        return RealMetadataFactory.createTransformationMetadata(metadataStore, "example");
    }
    @Test public void testLimit() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        String sql = "SELECT * FROM pm1.g1 limit 100";//$NON-NLS-1$
        String[] expectedSql = new String[] {
            "SELECT pm1.g1.e1, pm1.g1.e2, pm1.g1.e3, pm1.g1.e4 FROM pm1.g1" //$NON-NLS-1$
            };
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(),
                                                    null, capFinder, expectedSql, true);

        TestOptimizer.checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            1,      // Limit
            0,      // NestedLoopJoinStrategy

            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            0,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        }, NODE_TYPES);
    }

    @Test public void testLimitPushdown() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        String sql = "SELECT * FROM pm1.g1 limit 100";//$NON-NLS-1$
        String[] expectedSql = new String[] {
            "SELECT pm1.g1.e1, pm1.g1.e2, pm1.g1.e3, pm1.g1.e4 FROM pm1.g1 LIMIT 100" //$NON-NLS-1$
            };
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(),
                                                    null, capFinder, expectedSql, true);

        TestOptimizer.checkNodeTypes(plan, FULL_PUSHDOWN, NODE_TYPES);
    }

    @Test public void testLimitWithOffset() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        String sql = "SELECT * FROM pm1.g1 limit 50, 100";//$NON-NLS-1$
        String[] expectedSql = new String[] {
            "SELECT pm1.g1.e1, pm1.g1.e2, pm1.g1.e3, pm1.g1.e4 FROM pm1.g1" //$NON-NLS-1$
            };
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(),
                                                    null, capFinder, expectedSql, true);

        TestOptimizer.checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            1,      // Limit
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            0,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        }, NODE_TYPES);
    }

    @Test public void testPushedLimitWithOffset() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        String sql = "SELECT * FROM pm1.g1 limit 50, 100";//$NON-NLS-1$
        String[] expectedSql = new String[] {
            "SELECT pm1.g1.e1, pm1.g1.e2, pm1.g1.e3, pm1.g1.e4 FROM pm1.g1 LIMIT 150" //$NON-NLS-1$
            };
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(),
                                                    null, capFinder, expectedSql, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING);

        TestOptimizer.checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            1,      // Limit
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            0,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        }, NODE_TYPES);
    }

    @Test public void testLimitWithOffsetFullyPushed() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
        caps.setCapabilitySupport(Capability.ROW_OFFSET, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        String sql = "SELECT * FROM pm1.g1 limit 50, 100";//$NON-NLS-1$
        String[] expectedSql = new String[] {
            "SELECT pm1.g1.e1, pm1.g1.e2, pm1.g1.e3, pm1.g1.e4 FROM pm1.g1 LIMIT 50, 100" //$NON-NLS-1$
            };
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(),
                                                    null, capFinder, expectedSql, true);

        TestOptimizer.checkNodeTypes(plan, FULL_PUSHDOWN, NODE_TYPES);
    }

    @Test public void testSort() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        String sql = "SELECT * FROM pm1.g1 order by e1 limit 100";//$NON-NLS-1$
        String[] expectedSql = new String[] {
            "SELECT pm1.g1.e1, pm1.g1.e2, pm1.g1.e3, pm1.g1.e4 FROM pm1.g1" //$NON-NLS-1$
            };
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(),
                                                    null, capFinder, expectedSql, true);

        TestOptimizer.checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            1,      // Limit
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            0,      // Project
            0,      // Select
            1,      // Sort
            0       // UnionAll
        }, NODE_TYPES);
    }

    @Test public void testSortPushed() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        // pm3 model supports order by
        capFinder.addCapabilities("pm3", caps); //$NON-NLS-1$

        String sql = "SELECT * FROM pm3.g1 order by e1 limit 100";//$NON-NLS-1$
        String[] expectedSql = new String[] {
            "SELECT pm3.g1.e1, pm3.g1.e2, pm3.g1.e3, pm3.g1.e4 FROM pm3.g1 ORDER BY pm3.g1.e1" //$NON-NLS-1$
            };
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(),
                                                    null, capFinder, expectedSql, true);

        TestOptimizer.checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            1,      // Limit
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            0,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        }, NODE_TYPES);
    }

    @Test public void testSortPushedWithLimit() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
        // pm3 model supports order by
        capFinder.addCapabilities("pm3", caps); //$NON-NLS-1$

        String sql = "SELECT * FROM pm3.g1 order by e1 limit 100";//$NON-NLS-1$
        String[] expectedSql = new String[] {
            "SELECT pm3.g1.e1, pm3.g1.e2, pm3.g1.e3, pm3.g1.e4 FROM pm3.g1 ORDER BY pm3.g1.e1 LIMIT 100" //$NON-NLS-1$
            };
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(),
                                                    null, capFinder, expectedSql, true);

        TestOptimizer.checkNodeTypes(plan, FULL_PUSHDOWN, NODE_TYPES);
    }

    @Test public void testSortUnderLimitNotRemoved() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        // pm3 model supports order by
        capFinder.addCapabilities("pm3", caps); //$NON-NLS-1$

        String sql = "SELECT * FROM (SELECT * FROM pm3.g1 order by e1 limit 100) AS V1 ORDER BY v1.e2";//$NON-NLS-1$
        String[] expectedSql = new String[] {
            "SELECT pm3.g1.e1, pm3.g1.e2, pm3.g1.e3, pm3.g1.e4 FROM pm3.g1" //$NON-NLS-1$
            };
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(),
                                                    null, capFinder, expectedSql, true);

        TestOptimizer.checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            1,      // Limit
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            2,      // Sort
            0       // UnionAll
        }, NODE_TYPES);
    }

    //TODO: there is a redundent project node here
    @Test public void testSortAboveLimitNotPushed() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        String sql = "SELECT * FROM vm1.g2 order by e1";//$NON-NLS-1$
        String[] expectedSql = new String[] {
            "SELECT pm1.g1.e1, pm1.g1.e2, pm1.g1.e3, pm1.g1.e4 FROM pm1.g1 LIMIT 100" //$NON-NLS-1$
            };
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, exampleMetadata(),
                                                    null, capFinder, expectedSql, true);

        TestOptimizer.checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // Limit
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            1,      // Sort
            0       // UnionAll
        }, NODE_TYPES);
    }

    @Test public void testLimitNotPushedWithUnion() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
        // pm1 model supports order by
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        String sql = "SELECT * FROM pm1.g1 UNION SELECT * FROM PM1.g2 LIMIT 100";//$NON-NLS-1$
        String[] expectedSql = new String[] {
            "SELECT pm1.g1.e1, pm1.g1.e2, pm1.g1.e3, pm1.g1.e4 FROM pm1.g1", "SELECT pm1.g2.e1, pm1.g2.e2, pm1.g2.e3, pm1.g2.e4 FROM PM1.g2" //$NON-NLS-1$
            };
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(),
                                                    null, capFinder, expectedSql, true);

        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            1,      // DupRemove
            0,      // Grouping
            1,      // Limit
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            0,      // Project
            0,      // Select
            0,      // Sort
            1       // UnionAll
        }, NODE_TYPES);
    }

    @Test public void testStrictLimitWithUnion() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
        // pm1 model supports order by
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        String sql = "SELECT * FROM (select e2 from pm1.g1 where e1 = 'a') x UNION ALL SELECT e2 FROM PM1.g2 LIMIT 100";//$NON-NLS-1$
        String[] expectedSql = new String[] {
            "SELECT pm1.g2.e2 FROM pm1.g2 LIMIT 100", "SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1" //$NON-NLS-1$
            };
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(),
                                                    null, capFinder, expectedSql, true);

        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            2,      // Limit
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            1,      // Select
            0,      // Sort
            1       // UnionAll
        }, NODE_TYPES);
    }

    @Test public void testPushedLimit() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
        caps.setCapabilitySupport(Capability.QUERY_UNION, true);
        caps.setCapabilitySupport(Capability.QUERY_SET_LIMIT_OFFSET, true);
        // pm1 model supports order by
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        String sql = "SELECT e2 from pm1.g2 UNION ALL SELECT e2 FROM PM1.g2 LIMIT 100";//$NON-NLS-1$
        String[] expectedSql = new String[] {
            "SELECT pm1.g2.e2 AS c_0 FROM pm1.g2 UNION ALL SELECT pm1.g2.e2 AS c_0 FROM pm1.g2 LIMIT 100" //$NON-NLS-1$
            };
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(),
                                                    null, capFinder, expectedSql, true);

        TestOptimizer.checkNodeTypes(plan, FULL_PUSHDOWN, NODE_TYPES);

        caps.setCapabilitySupport(Capability.QUERY_SET_LIMIT_OFFSET, false);
        expectedSql = new String[] {
                "SELECT pm1.g2.e2 AS c_0 FROM pm1.g2 UNION ALL SELECT pm1.g2.e2 AS c_0 FROM pm1.g2" //$NON-NLS-1$
                };
        TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(),
                                                        null, capFinder, expectedSql, true);
    }

    @Test public void testLimitNotPushedWithDupRemove() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
        // pm1 model supports order by
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        String sql = "SELECT distinct * FROM pm1.g1 LIMIT 100";//$NON-NLS-1$
        String[] expectedSql = new String[] {
            "SELECT pm1.g1.e1, pm1.g1.e2, pm1.g1.e3, pm1.g1.e4 FROM pm1.g1" //$NON-NLS-1$
            };
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(),
                                                    null, capFinder, expectedSql, true);

        TestOptimizer.checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            1,      // DupRemove
            0,      // Grouping
            1,      // Limit
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            0,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        }, NODE_TYPES);
    }

    @Test public void testLimitPushedWithUnionAll() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
        // pm1 model supports order by
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        String sql = "SELECT * FROM pm1.g1 UNION ALL SELECT * FROM PM1.g2 LIMIT 100";//$NON-NLS-1$
        String[] expectedSql = new String[] {
            "SELECT pm1.g2.e1, pm1.g2.e2, pm1.g2.e3, pm1.g2.e4 FROM PM1.g2 LIMIT 100", "SELECT pm1.g1.e1, pm1.g1.e2, pm1.g1.e3, pm1.g1.e4 FROM pm1.g1 LIMIT 100" //$NON-NLS-1$ //$NON-NLS-2$
            };
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(),
                                                    null, capFinder, expectedSql, true);

        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            1,      // Limit
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            0,      // Project
            0,      // Select
            0,      // Sort
            1       // UnionAll
        }, NODE_TYPES);
    }

    @Test public void testLimitWithOffsetPushedWithUnion() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
        caps.setCapabilitySupport(Capability.ROW_OFFSET, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        // pm1 model supports order by
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        String sql = "SELECT * FROM pm1.g1 UNION ALL SELECT * FROM PM1.g2 LIMIT 50, 100";//$NON-NLS-1$
        String[] expectedSql = new String[] {
            "SELECT pm1.g1.e1 AS c_0, pm1.g1.e2 AS c_1, pm1.g1.e3 AS c_2, pm1.g1.e4 AS c_3 FROM pm1.g1 LIMIT 150", "SELECT pm1.g2.e1 AS c_0, pm1.g2.e2 AS c_1, pm1.g2.e3 AS c_2, pm1.g2.e4 AS c_3 FROM pm1.g2 LIMIT 150" //$NON-NLS-1$ //$NON-NLS-2$
            };
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(),
                                                    null, capFinder, expectedSql, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING);

        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            1,      // Limit
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            0,      // Project
            0,      // Select
            0,      // Sort
            1       // UnionAll
        }, NODE_TYPES);
    }

    @Test public void testLimitNotPushedWithUnionOrderBy() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
        // pm1 model supports order by
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        String sql = "SELECT * FROM pm1.g1 UNION SELECT * FROM PM1.g2 ORDER BY e1 LIMIT 100";//$NON-NLS-1$
        String[] expectedSql = new String[] {
            "SELECT pm1.g2.e1, pm1.g2.e2, pm1.g2.e3, pm1.g2.e4 FROM PM1.g2", "SELECT pm1.g1.e1, pm1.g1.e2, pm1.g1.e3, pm1.g1.e4 FROM pm1.g1" //$NON-NLS-1$ //$NON-NLS-2$
            };
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(),
                                                    null, capFinder, expectedSql, true);

        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            1,      // Limit
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            0,      // Project
            0,      // Select
            0,      // Sort
            1       // UnionAll
        }, NODE_TYPES);
        TestOptimizer.checkNodeTypes(plan, new int[] {1}, new Class[]{DupRemoveSortNode.class});
    }

    @Test public void testCombinedLimits() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        // pm1 model supports order by
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        String sql = "SELECT * from (SELECT pm1.g1.e1 FROM pm1.g1 LIMIT 10, 100) x LIMIT 20, 75";//$NON-NLS-1$
        String[] expectedSql = new String[] {
            "SELECT pm1.g1.e1 AS c_0 FROM pm1.g1 LIMIT 105" //$NON-NLS-1$
            };
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(),
                                                    null, capFinder, expectedSql, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING);

        TestOptimizer.checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            1,      // Limit
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            0,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        }, NODE_TYPES);
    }

    @Test public void testCombinedLimitsWithOffset() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
        caps.setCapabilitySupport(Capability.ROW_OFFSET, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        // pm1 model supports order by
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        String sql = "SELECT * from (SELECT pm1.g1.e1 FROM pm1.g1 LIMIT 10, 100) x LIMIT 40, 75";//$NON-NLS-1$
        String[] expectedSql = new String[] {
            "SELECT pm1.g1.e1 AS c_0 FROM pm1.g1 LIMIT 10, 60" //$NON-NLS-1$
            };
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(),
                                                    null, capFinder, expectedSql, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING);

        TestOptimizer.checkNodeTypes(plan, FULL_PUSHDOWN, NODE_TYPES);
    }

    @Test public void testInlineView() throws TeiidComponentException, TeiidProcessingException {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        //caps.setCapabilitySupport(SourceCapabilities.QUERY_ORDERBY, true);
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_INLINE_VIEWS, true);
        // pm3 model supports order by
        capFinder.addCapabilities("pm3", caps); //$NON-NLS-1$

        String sql = "SELECT * FROM (SELECT * FROM pm3.g1) as v1 limit 100";//$NON-NLS-1$
        String[] expectedSql = new String[] {
            "SELECT g_0.e1 AS c_0, g_0.e2 AS c_1, g_0.e3 AS c_2, g_0.e4 AS c_3 FROM pm3.g1 AS g_0 LIMIT 100" //$NON-NLS-1$
            };
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(),
                                                    null, capFinder, expectedSql, ComparisonMode.EXACT_COMMAND_STRING);

        TestOptimizer.checkNodeTypes(plan, FULL_PUSHDOWN, NODE_TYPES);
    }

    /**
     * This turns out to be an important test for LIMIT: there are several nodes
     * (e.g. grouping, inline views, aggregates, sorting, joins, etc) that should not be pushed
     * down (because they change row order or row count) if there is already a limit node in that plan branch,
     * which can only be placed above LIMIT with an inline view. This test acts as a gatekeeper for avoiding
     * several of those pushdowns.
     * @throws TeiidProcessingException
     * @throws TeiidComponentException
     *
     * @since 4.3
     */
    @Test public void testInlineViewAboveLimitNotMerged() throws TeiidComponentException, TeiidProcessingException {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_INLINE_VIEWS, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        // pm3 model supports order by
        capFinder.addCapabilities("pm3", caps); //$NON-NLS-1$

        String sql = "SELECT * FROM (SELECT * FROM pm3.g1 limit 100) as v1 order by e1";//$NON-NLS-1$
        String[] expectedSql = new String[] {
            "SELECT v_0.c_0, v_0.c_1, v_0.c_2, v_0.c_3 FROM (SELECT g_0.e1 AS c_0, g_0.e2 AS c_1, g_0.e3 AS c_2, g_0.e4 AS c_3 FROM pm3.g1 AS g_0 LIMIT 100) AS v_0 ORDER BY c_0" //$NON-NLS-1$
            };
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(),
                                                    null, capFinder, expectedSql, ComparisonMode.EXACT_COMMAND_STRING);

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    /**
     * since there is no order by with the nested limit, the criteria can be pushed through
     *
     */
    @Test public void testCriteriaPushedUnderLimit() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        // pm3 model supports order by
        capFinder.addCapabilities("pm3", caps); //$NON-NLS-1$

        String sql = "SELECT * FROM (SELECT * FROM pm3.g1 /*+ non_strict */ limit 100) as v1 where v1.e1 = 1";//$NON-NLS-1$
        String[] expectedSql = new String[] {
            "SELECT pm3.g1.e1, pm3.g1.e2, pm3.g1.e3, pm3.g1.e4 FROM pm3.g1 WHERE pm3.g1.e1 = '1' LIMIT 100" //$NON-NLS-1$
            };
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(),
                                                    null, capFinder, expectedSql, true);

        TestOptimizer.checkNodeTypes(plan, FULL_PUSHDOWN, NODE_TYPES);
    }

    @Test public void testInlineViewJoin() throws TeiidComponentException, TeiidProcessingException {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_UNION, true);
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_INLINE_VIEWS, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        String sql = "SELECT x FROM ((SELECT e1 as x FROM pm1.g1 LIMIT 700) c INNER JOIN (SELECT e1 FROM pm1.g2) d ON d.e1 = c.x) order by x LIMIT 5";//$NON-NLS-1$
        String[] expectedSql = new String[] {"SELECT g_0.e1 AS c_0 FROM pm1.g1 AS g_0 LIMIT 700", "SELECT g_0.e1 FROM pm1.g2 AS g_0"};//$NON-NLS-1$ //$NON-NLS-2$

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(),
                                      null, capFinder, expectedSql, ComparisonMode.EXACT_COMMAND_STRING);

        TestOptimizer.checkNodeTypes(plan, new int[] {
                                        2,      // Access
                                        0,      // DependentAccess
                                        0,      // DependentSelect
                                        0,      // DependentProject
                                        0,      // DupRemove
                                        0,      // Grouping
                                        1,      // Limit
                                        0,      // NestedLoopJoinStrategy
                                        1,      // MergeJoinStrategy
                                        0,      // Null
                                        0,      // PlanExecution
                                        1,      // Project
                                        0,      // Select
                                        1,      // Sort
                                        0       // UnionAll
        }, NODE_TYPES);

        //test to ensure that the unnecessary inline view removal is done properly
        FakeDataManager fdm = new FakeDataManager();
        TestProcessor.sampleData1(fdm);
        TestProcessor.helpProcess(plan, fdm, new List[] {
                Arrays.asList("a"), //$NON-NLS-1$
                Arrays.asList("a"), //$NON-NLS-1$
                Arrays.asList("a"), //$NON-NLS-1$
                Arrays.asList("a"), //$NON-NLS-1$
                Arrays.asList("a"), //$NON-NLS-1$
        });
    }

    @Test public void testDontPushSelectWithOrderedLimit() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        String sql = "select * from (SELECT e1 as x FROM pm1.g1 order by x LIMIT 700) y where x = 1";//$NON-NLS-1$
        String[] expectedSql = new String[] {"SELECT g_0.e1 AS c_0 FROM pm1.g1 AS g_0 ORDER BY c_0"};//$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(),
                                      null, capFinder, expectedSql, true);

        TestOptimizer.checkNodeTypes(plan, new int[] {
                                        1,      // Access
                                        0,      // DependentAccess
                                        0,      // DependentSelect
                                        0,      // DependentProject
                                        0,      // DupRemove
                                        0,      // Grouping
                                        1,      // Limit
                                        0,      // NestedLoopJoinStrategy
                                        0,      // MergeJoinStrategy
                                        0,      // Null
                                        0,      // PlanExecution
                                        1,      // Project
                                        1,      // Select
                                        0,      // Sort
                                        0       // UnionAll
        }, NODE_TYPES);
    }

    @Test public void testDontPushSelectWithOrderedLimit1() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        String sql = "select * from (SELECT e1 as x FROM pm1.g1 order by x LIMIT 10, 700) y where x = 1";//$NON-NLS-1$
        String[] expectedSql = new String[] {"SELECT g_0.e1 AS c_0 FROM pm1.g1 AS g_0 ORDER BY c_0"};//$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(),
                                      null, capFinder, expectedSql, true);

        TestOptimizer.checkNodeTypes(plan, new int[] {
                                        1,      // Access
                                        0,      // DependentAccess
                                        0,      // DependentSelect
                                        0,      // DependentProject
                                        0,      // DupRemove
                                        0,      // Grouping
                                        1,      // Limit
                                        0,      // NestedLoopJoinStrategy
                                        0,      // MergeJoinStrategy
                                        0,      // Null
                                        0,      // PlanExecution
                                        1,      // Project
                                        1,      // Select
                                        0,      // Sort
                                        0       // UnionAll
        }, NODE_TYPES);
    }

    @Test public void testLimitWithNoAccessNode() {
        String sql = "select 1 limit 1";//$NON-NLS-1$
        String[] expectedSql = new String[] {};

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), expectedSql);

        TestOptimizer.checkNodeTypes(plan, new int[] {
                                        0,      // Access
                                        0,      // DependentAccess
                                        0,      // DependentSelect
                                        0,      // DependentProject
                                        0,      // DupRemove
                                        0,      // Grouping
                                        1,      // Limit
                                        0,      // NestedLoopJoinStrategy
                                        0,      // MergeJoinStrategy
                                        0,      // Null
                                        0,      // PlanExecution
                                        1,      // Project
                                        0,      // Select
                                        0,      // Sort
                                        0       // UnionAll
        }, NODE_TYPES);
    }

    /**
     * Note here that the criteria made it to the having clause
     */
    @Test public void testAggregateCriteriaOverUnSortedLimit() {
        String sql = "select a from (SELECT MAX(e2) as a FROM pm1.g1 GROUP BY e2 /*+ non_strict */ LIMIT 1) x where a = 0"; //$NON-NLS-1$
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
        caps.setCapabilitySupport(Capability.QUERY_GROUP_BY, true);
        caps.setCapabilitySupport(Capability.QUERY_HAVING, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        String[] expectedSql = new String[] {"SELECT MAX(e2) FROM pm1.g1 GROUP BY e2 HAVING MAX(e2) = 0 LIMIT 1"};//$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), null, capFinder, expectedSql, true);

        TestOptimizer.checkNodeTypes(plan, FULL_PUSHDOWN, NODE_TYPES);
    }

    @Test public void testSortWithLimitInlineView() {
        String sql = "select e1 from (select pm1.g1.e1, pm1.g1.e2 from pm1.g1 order by pm1.g1.e1, pm1.g1.e2 limit 1) x"; //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), new String[] {"SELECT g_0.e1 AS c_0 FROM pm1.g1 AS g_0 ORDER BY c_0, g_0.e2"}); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, new int[] {
                1,      // Access
                0,      // DependentAccess
                0,      // DependentSelect
                0,      // DependentProject
                0,      // DupRemove
                0,      // Grouping
                1,      // Limit
                0,      // NestedLoopJoinStrategy
                0,      // MergeJoinStrategy
                0,      // Null
                0,      // PlanExecution
                1,      // Project
                0,      // Select
                0,      // Sort
                0       // UnionAll
        }, NODE_TYPES);
    }

    @Test public void testCrossJoinLimit() throws Exception {
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
        DefaultCapabilitiesFinder capFinder = new DefaultCapabilitiesFinder(caps);

        String sql = "select pm1.g1.e1, pm1.g1.e2 from pm1.g1, pm2.g1 limit 5, 5"; //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), new String[] {"SELECT pm2.g1.e1 FROM pm2.g1 LIMIT 10", "SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1 LIMIT 10"}, capFinder, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, new int[] {
                2,      // Access
                0,      // DependentAccess
                0,      // DependentSelect
                0,      // DependentProject
                0,      // DupRemove
                0,      // Grouping
                1,      // Limit
                1,      // NestedLoopJoinStrategy
                0,      // MergeJoinStrategy
                0,      // Null
                0,      // PlanExecution
                1,      // Project
                0,      // Select
                0,      // Sort
                0       // UnionAll
        }, NODE_TYPES);
    }

    @Test public void testCrossJoinLimitNestedTable() throws Exception {
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
        DefaultCapabilitiesFinder capFinder = new DefaultCapabilitiesFinder(caps);

        String sql = "select pm1.g1.e1, pm1.g1.e2 from pm1.g1, TABLE(select pm2.g1.e1 FROM pm2.g1 WHERE pm2.g1.e1 = pm1.g1.e1) as x limit 5, 5"; //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), new String[] {"SELECT pm2.g1.e1 FROM pm2.g1 WHERE pm2.g1.e1 = pm1.g1.e1 LIMIT 10", "SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1"}, capFinder, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, new int[] {
                2,      // Access
                0,      // DependentAccess
                0,      // DependentSelect
                0,      // DependentProject
                0,      // DupRemove
                0,      // Grouping
                1,      // Limit
                0,      // NestedLoopJoinStrategy
                0,      // MergeJoinStrategy
                0,      // Null
                0,      // PlanExecution
                1,      // Project
                0,      // Select
                0,      // Sort
                0       // UnionAll
        }, NODE_TYPES);
    }

    /**
     * Note that the limit is not pushed below the select nodes under the join
     */
    @Test public void testEffectivelyCrossJoinLimit() throws Exception {
         BasicSourceCapabilities caps = new BasicSourceCapabilities();
         caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
         DefaultCapabilitiesFinder capFinder = new DefaultCapabilitiesFinder(caps);

         String sql = "select pm1.g1.e1, pm1.g1.e2 from pm1.g1, pm2.g1 where pm1.g1.e1 = pm2.g1.e1 and pm1.g1.e1 = 2 limit 5"; //$NON-NLS-1$

         ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), new String[] {"SELECT pm2.g1.e1 FROM pm2.g1", "SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1"}, capFinder, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

         TestOptimizer.checkNodeTypes(plan, new int[] {
                 2,      // Access
                 0,      // DependentAccess
                 0,      // DependentSelect
                 0,      // DependentProject
                 0,      // DupRemove
                 0,      // Grouping
                 3,      // Limit
                 1,      // NestedLoopJoinStrategy
                 0,      // MergeJoinStrategy
                 0,      // Null
                 0,      // PlanExecution
                 1,      // Project
                 2,      // Select
                 0,      // Sort
                 0       // UnionAll
         }, NODE_TYPES);
    }

    @Test public void testOuterJoinLimit() throws Exception {
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
        DefaultCapabilitiesFinder capFinder = new DefaultCapabilitiesFinder(caps);

        String sql = "select pm1.g1.e1, pm1.g1.e2 from pm1.g1 left outer join pm2.g1 on pm1.g1.e1 = pm2.g1.e1 limit 20000"; //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), new String[] {"SELECT pm2.g1.e1 FROM pm2.g1", "SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1 LIMIT 20000"}, capFinder, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, new int[] {
                2,      // Access
                0,      // DependentAccess
                0,      // DependentSelect
                0,      // DependentProject
                0,      // DupRemove
                0,      // Grouping
                1,      // Limit
                0,      // NestedLoopJoinStrategy
                1,      // MergeJoinStrategy
                0,      // Null
                0,      // PlanExecution
                1,      // Project
                0,      // Select
                0,      // Sort
                0       // UnionAll
        }, NODE_TYPES);
    }

    @Test public void testOrderedOuterJoinLimit() throws Exception {
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        DefaultCapabilitiesFinder capFinder = new DefaultCapabilitiesFinder(caps);

        String sql = "select pm1.g1.e1, pm1.g1.e2 from pm1.g1 left outer join pm2.g1 on pm1.g1.e1 = pm2.g1.e1 order by pm1.g1.e1 limit 3"; //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), new String[] {"SELECT pm2.g1.e1 FROM pm2.g1 ORDER BY pm2.g1.e1", "SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1 ORDER BY pm1.g1.e1 LIMIT 3"}, capFinder, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, new int[] {
                2,      // Access
                0,      // DependentAccess
                0,      // DependentSelect
                0,      // DependentProject
                0,      // DupRemove
                0,      // Grouping
                1,      // Limit
                0,      // NestedLoopJoinStrategy
                1,      // MergeJoinStrategy
                0,      // Null
                0,      // PlanExecution
                1,      // Project
                1,      // Select
                1,      // Sort
                0       // UnionAll
        }, NODE_TYPES);

        HardcodedDataManager hdm = new HardcodedDataManager();
        hdm.addData("SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1 ORDER BY pm1.g1.e1 LIMIT 3", Arrays.asList("a", 1), Arrays.asList("a", 2), Arrays.asList("c", 3));
        hdm.addData("SELECT pm2.g1.e1 FROM pm2.g1 ORDER BY pm2.g1.e1", Arrays.asList("a"), Arrays.asList("a"), Arrays.asList("c"));
        TestProcessor.helpProcess(plan, hdm, new List[] {
                Arrays.asList("a", 1), //$NON-NLS-1$
                Arrays.asList("a", 1), //$NON-NLS-1$
                Arrays.asList("a", 2), //$NON-NLS-1$
        });
    }

    @Test public void testOrderedOuterJoinLimit1() throws Exception {
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        DefaultCapabilitiesFinder capFinder = new DefaultCapabilitiesFinder(caps);

        String sql = "select pm1.g1.e1, pm1.g1.e2 from pm1.g1 full outer join pm2.g1 on pm1.g1.e1 = pm2.g1.e1 order by pm1.g1.e1 limit 3"; //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), new String[] {"SELECT pm2.g1.e1 FROM pm2.g1 ORDER BY pm2.g1.e1", "SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1 ORDER BY pm1.g1.e1 LIMIT 3"}, capFinder, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, new int[] {
                2,      // Access
                0,      // DependentAccess
                0,      // DependentSelect
                0,      // DependentProject
                0,      // DupRemove
                0,      // Grouping
                1,      // Limit
                0,      // NestedLoopJoinStrategy
                1,      // MergeJoinStrategy
                0,      // Null
                0,      // PlanExecution
                1,      // Project
                0,      // Select
                1,      // Sort
                0       // UnionAll
        }, NODE_TYPES);

        HardcodedDataManager hdm = new HardcodedDataManager();
        hdm.addData("SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1 ORDER BY pm1.g1.e1 LIMIT 3", Arrays.asList(null, 1), Arrays.asList("a", 2), Arrays.asList("c", 3));
        hdm.addData("SELECT pm2.g1.e1 FROM pm2.g1 ORDER BY pm2.g1.e1", Arrays.asList((String)null), Arrays.asList("a"), Arrays.asList("c"));
        TestProcessor.helpProcess(plan, hdm, new List[] {
                Arrays.asList(null, null), //$NON-NLS-1$
                Arrays.asList(null, 1), //$NON-NLS-1$
                Arrays.asList("a", 2), //$NON-NLS-1$
        });
    }

    /**
     * limit won't be pushed as the ordering is over both sides
     * @throws Exception
     */
    @Test public void testOrderedOuterJoinLimit2() throws Exception {
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        DefaultCapabilitiesFinder capFinder = new DefaultCapabilitiesFinder(caps);

        String sql = "select pm1.g1.e1, pm1.g1.e2 from pm1.g1 left outer join pm2.g1 on pm1.g1.e1 = pm2.g1.e1 order by pm1.g1.e1, pm2.g1.e1 limit 3"; //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), new String[] {"SELECT pm2.g1.e1 FROM pm2.g1 ORDER BY pm2.g1.e1", "SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1 ORDER BY pm1.g1.e1"}, capFinder, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, new int[] {
                2,      // Access
                0,      // DependentAccess
                0,      // DependentSelect
                0,      // DependentProject
                0,      // DupRemove
                0,      // Grouping
                1,      // Limit
                0,      // NestedLoopJoinStrategy
                1,      // MergeJoinStrategy
                0,      // Null
                0,      // PlanExecution
                1,      // Project
                0,      // Select
                1,      // Sort
                0       // UnionAll
        }, NODE_TYPES);
    }

    /**
     * ensure we can push through multiple joins and handle an offset
     * @throws Exception
     */
    @Test public void testOrderedOuterJoinLimit3() throws Exception {
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        DefaultCapabilitiesFinder capFinder = new DefaultCapabilitiesFinder(caps);

        String sql = "select pm1.g1.e1, pm1.g1.e2, pm1.g2.e3 from pm1.g1 left outer join pm2.g1 on pm1.g1.e1 = pm2.g1.e1 left outer join pm1.g2 on pm1.g1.e2 = pm1.g2.e2 order by pm1.g1.e4 limit 3,3000"; //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), new String[] {"SELECT pm2.g1.e1 FROM pm2.g1 ORDER BY pm2.g1.e1", "SELECT pm1.g1.e1, pm1.g1.e2, pm1.g1.e4 FROM pm1.g1 ORDER BY pm1.g1.e4 LIMIT 3003", "SELECT pm1.g2.e2, pm1.g2.e3 FROM pm1.g2 ORDER BY pm1.g2.e2"}, capFinder, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, new int[] {
                3,      // Access
                0,      // DependentAccess
                0,      // DependentSelect
                0,      // DependentProject
                0,      // DupRemove
                0,      // Grouping
                1,      // Limit
                0,      // NestedLoopJoinStrategy
                2,      // MergeJoinStrategy
                0,      // Null
                0,      // PlanExecution
                1,      // Project
                0,      // Select
                1,      // Sort
                0       // UnionAll
        }, NODE_TYPES);

        HardcodedDataManager hdm = new HardcodedDataManager();
        hdm.addData("SELECT pm1.g1.e1, pm1.g1.e2, pm1.g1.e4 FROM pm1.g1 ORDER BY pm1.g1.e4 LIMIT 3003", Arrays.asList(null, 4, null), Arrays.asList("a", 5, false), Arrays.asList("c", 6, false), Arrays.asList(null, 1, true), Arrays.asList("a", 2, true), Arrays.asList("c", 3, true));
        hdm.addData("SELECT pm2.g1.e1 FROM pm2.g1 ORDER BY pm2.g1.e1", Arrays.asList((String)null), Arrays.asList("a"), Arrays.asList("c"));
        hdm.addData("SELECT pm1.g2.e2, pm1.g2.e3 FROM pm1.g2 ORDER BY pm1.g2.e2", Arrays.asList(1, 1.0), Arrays.asList(2, 2.0));
        TestProcessor.helpProcess(plan, hdm, new List[] {
                Arrays.asList(null, 1, 1.0), //$NON-NLS-1$
                Arrays.asList("a", 2, 2.0), //$NON-NLS-1$
                Arrays.asList("c", 3, null), //$NON-NLS-1$
        });
    }

    @Test public void testOrderedOuterJoinLimitInlineView() throws Exception {
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        DefaultCapabilitiesFinder capFinder = new DefaultCapabilitiesFinder(caps);

        String sql = "select u.e1, u.e2 from (select pm1.g1.e1, pm1.g1.e2 from pm1.g1 union all select pm3.g1.e1, pm3.g1.e2 from pm3.g1 )as u left outer join pm2.g1 on u.e1 = pm2.g1.e1 order by u.e1 limit 3"; //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), new String[] {"SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1 ORDER BY pm1.g1.e1 LIMIT 3", "SELECT pm3.g1.e1, pm3.g1.e2 FROM pm3.g1 ORDER BY pm3.g1.e1 LIMIT 3", "SELECT pm2.g1.e1 FROM pm2.g1 ORDER BY pm2.g1.e1"}, capFinder, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, new int[] {
                3,      // Access
                0,      // DependentAccess
                0,      // DependentSelect
                0,      // DependentProject
                0,      // DupRemove
                0,      // Grouping
                2,      // Limit
                0,      // NestedLoopJoinStrategy
                1,      // MergeJoinStrategy
                0,      // Null
                0,      // PlanExecution
                3,      // Project
                1,      // Select
                2,      // Sort
                1       // UnionAll
        }, NODE_TYPES);

        HardcodedDataManager hdm = new HardcodedDataManager();
        hdm.addData("SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1 ORDER BY pm1.g1.e1 LIMIT 3", Arrays.asList(null, 4), Arrays.asList("a", 5), Arrays.asList("c", 6));
        hdm.addData("SELECT pm3.g1.e1, pm3.g1.e2 FROM pm3.g1 ORDER BY pm3.g1.e1 LIMIT 3", Arrays.asList(null, 7), Arrays.asList("a", 8), Arrays.asList("c", 9));
        hdm.addData("SELECT pm2.g1.e1 FROM pm2.g1 ORDER BY pm2.g1.e1", Arrays.asList("a"), Arrays.asList("b"), Arrays.asList("c"));
        TestProcessor.helpProcess(plan, hdm, new List[] {
                Arrays.asList(null, 4), //$NON-NLS-1$
                Arrays.asList(null, 7), //$NON-NLS-1$
                Arrays.asList("a", 5), //$NON-NLS-1$
        });

    }

    @Test public void testOrderedOuterJoinLimitUnionPushdown() throws Exception {
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_INLINE_VIEWS, false);
        caps.setCapabilitySupport(Capability.QUERY_SET_ORDER_BY, true);
        caps.setCapabilitySupport(Capability.QUERY_SET_LIMIT_OFFSET, true);
        caps.setCapabilitySupport(Capability.QUERY_UNION, true);
        caps.setCapabilitySupport(Capability.CRITERIA_ISNULL, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        DefaultCapabilitiesFinder capFinder = new DefaultCapabilitiesFinder(caps);

        String sql = "select u.e1, u.e2, pm2.g1.e2 from (select pm1.g1.e1, pm1.g1.e2 from pm1.g1 union all select pm1.g2.e1, pm1.g2.e2 from pm1.g2 )as u left outer join pm2.g1 on u.e1 = pm2.g1.e1 where u.e2 is null order by u.e1 limit 3"; //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), new String[] {"SELECT pm1.g1.e1 AS c_0, pm1.g1.e2 AS c_1 FROM pm1.g1 WHERE pm1.g1.e2 IS NULL UNION ALL SELECT pm1.g2.e1 AS c_0, pm1.g2.e2 AS c_1 FROM pm1.g2 WHERE pm1.g2.e2 IS NULL ORDER BY c_0 LIMIT 3",
            "SELECT pm2.g1.e1 AS c_0, pm2.g1.e2 AS c_1 FROM pm2.g1 ORDER BY c_0"}, capFinder, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, new int[] {
                2,      // Access
                0,      // DependentAccess
                0,      // DependentSelect
                0,      // DependentProject
                0,      // DupRemove
                0,      // Grouping
                1,      // Limit
                0,      // NestedLoopJoinStrategy
                1,      // MergeJoinStrategy
                0,      // Null
                0,      // PlanExecution
                1,      // Project
                1,      // Select
                1,      // Sort
                0       // UnionAll
        }, NODE_TYPES);

        HardcodedDataManager hdm = new HardcodedDataManager();
        hdm.addData("SELECT pm1.g1.e1 AS c_0, pm1.g1.e2 AS c_1 FROM pm1.g1 WHERE pm1.g1.e2 IS NULL UNION ALL SELECT pm1.g2.e1 AS c_0, pm1.g2.e2 AS c_1 FROM pm1.g2 WHERE pm1.g2.e2 IS NULL ORDER BY c_0 LIMIT 3", Arrays.asList(null, null), Arrays.asList("a", null), Arrays.asList("c", null));
        hdm.addData("SELECT pm2.g1.e1 AS c_0, pm2.g1.e2 AS c_1 FROM pm2.g1 ORDER BY c_0", Arrays.asList(null, 0), Arrays.asList("a", 1), Arrays.asList("e", 2));
        TestProcessor.helpProcess(plan, hdm, new List[] {
                Arrays.asList(null, null, null), //$NON-NLS-1$
                Arrays.asList("a", null, 1), //$NON-NLS-1$
                Arrays.asList("c", null, null), //$NON-NLS-1$
        });

    }

    @Test public void testPushSortOverAliases() throws Exception {
        String sql = "select column_a, column_b from (select sum(column_a) over (partition by key_column) as column_a, key_column from a ) a left outer join ( "
                + " select sum(column_b) over (partition by key_column) as column_b, key_column from b) b on a.key_column = b.key_column order by column_a desc";


        TransformationMetadata tm = RealMetadataFactory.fromDDL("create foreign table a (column_a integer, key_column string primary key);"
                + " create foreign table b (column_b integer, key_column string primary key);", "x", "y");

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, tm, new String[] {"SELECT g_0.key_column, g_0.column_b FROM y.b AS g_0", "SELECT g_0.key_column, g_0.column_a FROM y.a AS g_0"}, TestOptimizer.getGenericFinder(false), ComparisonMode.EXACT_COMMAND_STRING);

        HardcodedDataManager hdm = new HardcodedDataManager();
        hdm.addData("SELECT g_0.key_column, g_0.column_a FROM y.a AS g_0", Arrays.asList("a", 1));
        hdm.addData("SELECT g_0.key_column, g_0.column_b FROM y.b AS g_0", Arrays.asList("a", 1));
        TestProcessor.helpProcess(plan, hdm, new List[] {Arrays.asList(1L, 1L)});
    }

    @Test public void testPushSortOverAliasesWithLimit() throws Exception {
        String sql = "select column_a, column_b from (select sum(column_a) over (partition by key_column) as column_a, key_column from a ) a left outer join ( "
                + " select sum(column_b) over (partition by key_column) as column_b, key_column from b) b on a.key_column = b.key_column order by column_a desc limit 10";


        TransformationMetadata tm = RealMetadataFactory.fromDDL("create foreign table a (column_a integer, key_column string primary key);"
                + " create foreign table b (column_b integer, key_column string primary key);", "x", "y");

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, tm, new String[] {"SELECT g_0.key_column, g_0.column_b FROM y.b AS g_0 WHERE g_0.key_column IN (<dependent values>)", "SELECT g_0.key_column, g_0.column_a FROM y.a AS g_0"}, TestOptimizer.getGenericFinder(false), ComparisonMode.EXACT_COMMAND_STRING);

        HardcodedDataManager hdm = new HardcodedDataManager();
        hdm.addData("SELECT g_0.key_column, g_0.column_a FROM y.a AS g_0", Arrays.asList("a", 1));
        hdm.addData("SELECT g_0.key_column, g_0.column_b FROM y.b AS g_0 WHERE g_0.key_column = 'a'", Arrays.asList("a", 1));
        TestProcessor.helpProcess(plan, hdm, new List[] {Arrays.asList(1L, 1L)});
    }

    @Test public void testSortedLimitOverLOJ() throws Exception {
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, false);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY_UNRELATED, true);
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
        TransformationMetadata tm = RealMetadataFactory.fromDDL("create foreign table test_ep_ds (str varchar, i integer);"
                + "create foreign table test_ep_dwh (str varchar, i integer);", "x", "y");
        String sql = "select * from (\n" +
                "    select b.i as col1, 'def' as col2 from test_ep_ds a\n" +
                "    left join  test_ep_dwh b on b.str=a.str    \n" +
                "    union all\n" +
                "    select 2 as col1, 'abc' as col2\n" +
                ")x order by col1 limit 100";
        TestOptimizer.helpPlan(sql, //$NON-NLS-1$
                tm,
                new String[] {
                    "SELECT y.test_ep_ds.str AS c_0 FROM y.test_ep_ds ORDER BY c_0",
                    "SELECT y.test_ep_dwh.str AS c_0, y.test_ep_dwh.i AS c_1 FROM y.test_ep_dwh ORDER BY c_0"}, new DefaultCapabilitiesFinder(caps), ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    }

}
