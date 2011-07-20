/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

package org.teiid.query.optimizer;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.teiid.core.types.DataTypeManager;
import org.teiid.metadata.Column;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.query.mapping.relational.QueryNode;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.TestOptimizer.DependentProjectNode;
import org.teiid.query.optimizer.TestOptimizer.DependentSelectNode;
import org.teiid.query.optimizer.TestOptimizer.DupRemoveNode;
import org.teiid.query.optimizer.TestOptimizer.DupRemoveSortNode;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.FakeCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.processor.FakeDataManager;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.TestProcessor;
import org.teiid.query.processor.relational.AccessNode;
import org.teiid.query.processor.relational.DependentAccessNode;
import org.teiid.query.processor.relational.GroupingNode;
import org.teiid.query.processor.relational.LimitNode;
import org.teiid.query.processor.relational.MergeJoinStrategy;
import org.teiid.query.processor.relational.NestedLoopJoinStrategy;
import org.teiid.query.processor.relational.NullNode;
import org.teiid.query.processor.relational.PlanExecutionNode;
import org.teiid.query.processor.relational.ProjectNode;
import org.teiid.query.processor.relational.SelectNode;
import org.teiid.query.processor.relational.SortNode;
import org.teiid.query.processor.relational.UnionAllNode;
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
            "SELECT PM1.g2.e1 AS c_0, PM1.g2.e2 AS c_1, PM1.g2.e3 AS c_2, PM1.g2.e4 AS c_3 FROM PM1.g2 LIMIT 150", "SELECT pm1.g1.e1 AS c_0, pm1.g1.e2 AS c_1, pm1.g1.e3 AS c_2, pm1.g1.e4 AS c_3 FROM pm1.g1 LIMIT 150" //$NON-NLS-1$ //$NON-NLS-2$
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

    @Test public void testInlineView() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        //caps.setCapabilitySupport(SourceCapabilities.QUERY_ORDERBY, true);
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_INLINE_VIEWS, true);
        // pm3 model supports order by
        capFinder.addCapabilities("pm3", caps); //$NON-NLS-1$

        String sql = "SELECT * FROM (SELECT * FROM pm3.g1) as v1 limit 100";//$NON-NLS-1$
        String[] expectedSql = new String[] {
            "SELECT pm3.g1.e1, pm3.g1.e2, pm3.g1.e3, pm3.g1.e4 FROM pm3.g1 LIMIT 100" //$NON-NLS-1$
            };
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), 
                                                    null, capFinder, expectedSql, true);  

        TestOptimizer.checkNodeTypes(plan, FULL_PUSHDOWN, NODE_TYPES);
    }
    
    /**
     * This turns out to be an important test for LIMIT: there are several nodes
     * (e.g. grouping, inline views, aggregates, sorting, joins, etc) that should not be pushed
     * down (because they change row order or row count) if there is already a limit node in that plan branch,
     * which can only be placed above LIMIT with an inline view. This test acts as a gatekeeper for avoiding
     * several of those pushdowns.
     * 
     * @since 4.3
     */
    @Test public void testInlineViewAboveLimitNotMerged() {
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
            "SELECT v_0.c_0, v_0.c_1, v_0.c_2, v_0.c_3 FROM (SELECT pm3.g1.e1 AS c_0, pm3.g1.e2 AS c_1, pm3.g1.e3 AS c_2, pm3.g1.e4 AS c_3 FROM pm3.g1 LIMIT 100) AS v_0 ORDER BY c_0" //$NON-NLS-1$
            };
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), 
                                                    null, capFinder, expectedSql, true);  

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

        String sql = "SELECT * FROM (SELECT * FROM pm3.g1 limit 100) as v1 where v1.e1 = 1";//$NON-NLS-1$
        String[] expectedSql = new String[] {
            "SELECT pm3.g1.e1, pm3.g1.e2, pm3.g1.e3, pm3.g1.e4 FROM pm3.g1 WHERE pm3.g1.e1 = '1' LIMIT 100" //$NON-NLS-1$
            };
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), 
                                                    null, capFinder, expectedSql, true);  

        TestOptimizer.checkNodeTypes(plan, FULL_PUSHDOWN, NODE_TYPES);
    }
    
    @Test public void testInlineViewJoin() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_UNION, true);
        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_INLINE_VIEWS, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        String sql = "SELECT x FROM ((SELECT e1 as x FROM pm1.g1 LIMIT 700) c INNER JOIN (SELECT e1 FROM pm1.g2) d ON d.e1 = c.x) order by x LIMIT 5";//$NON-NLS-1$
        String[] expectedSql = new String[] {"SELECT e1 FROM pm1.g1 LIMIT 700", "SELECT e1 FROM pm1.g2"};//$NON-NLS-1$ //$NON-NLS-2$
        
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
        String sql = "select a from (SELECT MAX(e2) as a FROM pm1.g1 GROUP BY e2 LIMIT 1) x where a = 0"; //$NON-NLS-1$
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

}
