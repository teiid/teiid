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

import org.junit.Test;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.TestOptimizer.ComparisonMode;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.FakeCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.processor.ProcessorPlan;
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
        ProcessorPlan plan = TestOptimizer.helpPlan("select * from (SELECT IntKey FROM BQT1.SmallA where intkey in (1, 2) UNION ALL SELECT intkey FROM BQT2.SmallA where intkey in (3, 4)) A inner join (SELECT intkey FROM BQT1.SmallB where intkey in (1, 2) UNION ALL SELECT intkey FROM BQT2.SmallB where intkey in (3, 4)) B on a.intkey = b.intkey", RealMetadataFactory.exampleBQTCached(), null, TestOptimizer.getGenericFinder(),//$NON-NLS-1$
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
    
    @Test public void testUnionPushDownWithJoinNoMatches() {
        TestOptimizer.helpPlan("select * from (SELECT IntKey FROM BQT1.SmallA where intkey in (1, 2) UNION ALL SELECT intkey FROM BQT2.SmallA where intkey in (3, 4)) A inner join (SELECT intkey FROM BQT1.SmallB where intkey in (5, 6) UNION ALL SELECT intkey FROM BQT2.SmallB where intkey in (7, 8)) B on a.intkey = b.intkey", RealMetadataFactory.exampleBQTCached(), null, TestOptimizer.getGenericFinder(),//$NON-NLS-1$
            new String[] {}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$  
    }
    
    @Test public void testUnionPushDownWithJoin1() throws Exception {
        ProcessorPlan plan = TestOptimizer.helpPlan("select * from (SELECT IntKey FROM BQT1.SmallA where intkey in (1, 2) UNION ALL SELECT intkey FROM BQT2.SmallA where intkey in (3, 4)) A inner join (SELECT intkey FROM BQT1.SmallB where intkey in (1, 2) UNION ALL SELECT intkey FROM BQT2.SmallB where intkey in (3, 4)) B on a.intkey = b.intkey where a.intkey in (1, 4)", RealMetadataFactory.exampleBQTCached(), null, TestOptimizer.getGenericFinder(),//$NON-NLS-1$
            new String[] { "SELECT g_1.intkey, g_0.IntKey FROM BQT1.SmallA AS g_0, BQT1.SmallB AS g_1 WHERE (g_0.IntKey = g_1.intkey) AND (g_0.intkey IN (1)) AND (g_0.IntKey = 1) AND (g_1.intkey = 1)",
            		"SELECT g_1.intkey, g_0.intkey FROM BQT2.SmallA AS g_0, BQT2.SmallB AS g_1 WHERE (g_0.intkey = g_1.intkey) AND (g_0.intkey IN (4)) AND (g_0.intkey = 4) AND (g_1.intkey = 4)" }, ComparisonMode.EXACT_COMMAND_STRING); 

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
            new String[] { "SELECT MAX(v_0.c_1) FROM (SELECT g_0.IntKey AS c_0, g_0.intnum AS c_1 FROM BQT1.SmallA AS g_0 WHERE g_0.intkey IN (1, 2)) AS v_0 GROUP BY v_0.c_0", 
        			"SELECT MAX(v_0.c_1) FROM (SELECT g_0.intkey AS c_0, g_0.intnum AS c_1 FROM BQT2.SmallA AS g_0 WHERE g_0.intkey IN (3, 4)) AS v_0 GROUP BY v_0.c_0" }, ComparisonMode.EXACT_COMMAND_STRING); 

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
    
    @Test public void testUnionPartitionedWithMerge() throws Exception {
    	//"select max(intnum) from (select * from (SELECT IntKey, intnum FROM BQT1.SmallA where intkey in (1, 2) UNION ALL SELECT intkey, intnum FROM BQT2.SmallA where intkey in (3, 4)) A where intkey in (1, 2, 3, 4) UNION ALL select intkey, intnum from bqt2.smallb where intkey in 6) B group by intkey"
        ProcessorPlan plan = TestOptimizer.helpPlan("select * from (select * from (SELECT IntKey, intnum FROM BQT1.SmallA UNION ALL SELECT intkey, intnum FROM BQT2.SmallA) A where intkey in (1, 2, 3, 4) UNION ALL select intkey, intnum from bqt2.smallb where intkey in (6)) B inner join (SELECT IntKey, intnum FROM BQT1.SmallA where intkey in (1, 2) UNION ALL SELECT intkey, intnum FROM BQT2.SmallA where intkey in (5, 6)) C on b.intkey = c.intkey", RealMetadataFactory.exampleBQTCached(), null, TestInlineView.getInliveViewCapabilitiesFinder(),//$NON-NLS-1$
            new String[] { "SELECT g_0.intkey, g_0.intnum FROM BQT2.SmallA AS g_0 WHERE g_0.intkey IN (1, 2)",
        	"SELECT g_0.IntKey, g_0.intnum FROM BQT1.SmallA AS g_0 WHERE g_0.IntKey IN (1, 2)",
        	"SELECT g_1.IntKey, g_1.IntNum, g_0.intkey, g_0.intnum FROM bqt2.smallb AS g_0, BQT2.SmallA AS g_1 WHERE (g_0.intkey = g_1.IntKey) AND (g_0.intkey = 6) AND (g_1.IntKey = 6)",
        	"SELECT g_0.IntKey AS c_0, g_0.IntNum AS c_1 FROM BQT1.SmallA AS g_0 WHERE g_0.IntKey IN (1, 2) ORDER BY c_0" }, ComparisonMode.EXACT_COMMAND_STRING); 

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

}
