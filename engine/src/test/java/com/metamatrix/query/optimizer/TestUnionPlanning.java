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

package com.metamatrix.query.optimizer;

import com.metamatrix.query.optimizer.capabilities.BasicSourceCapabilities;
import com.metamatrix.query.optimizer.capabilities.FakeCapabilitiesFinder;
import com.metamatrix.query.optimizer.capabilities.SourceCapabilities.Capability;
import com.metamatrix.query.processor.ProcessorPlan;
import com.metamatrix.query.unittest.FakeMetadataFactory;

import junit.framework.TestCase;

public class TestUnionPlanning extends TestCase {

    public void testUnionPushDown() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_UNION, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("BQT2", caps); //$NON-NLS-1$
        
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT IntKey FROM BQT1.SmallA UNION ALL SELECT IntNum FROM BQT2.SmallA UNION ALL SELECT IntNum FROM BQT1.SmallA", FakeMetadataFactory.exampleBQTCached(), null, capFinder,//$NON-NLS-1$
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
    public void testUnionPushDown1() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_UNION, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("BQT2", caps); //$NON-NLS-1$
        
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT IntKey FROM BQT1.SmallA UNION SELECT IntNum FROM BQT2.SmallA UNION ALL SELECT IntNum FROM BQT1.SmallA", FakeMetadataFactory.exampleBQTCached(), null, capFinder,//$NON-NLS-1$
            new String[] { "SELECT DISTINCT IntNum FROM BQT2.SmallA", "SELECT DISTINCT IntKey FROM BQT1.SmallA", "SELECT IntNum FROM BQT1.SmallA" }, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 

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

    public void testUnionPushDown2() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_UNION, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("BQT2", caps); //$NON-NLS-1$
        BasicSourceCapabilities caps1 = TestOptimizer.getTypicalCapabilities();
        capFinder.addCapabilities("BQT3", caps1); //$NON-NLS-1$
        
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT IntKey FROM BQT1.SmallA UNION ALL SELECT IntNum FROM BQT2.SmallA UNION ALL SELECT IntNum FROM BQT1.SmallA UNION ALL SELECT IntNum FROM BQT3.SmallA UNION ALL SELECT IntNum FROM BQT2.SmallA", FakeMetadataFactory.exampleBQTCached(), null, capFinder,//$NON-NLS-1$
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
            2       // UnionAll
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
        
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT IntKey FROM BQT1.SmallA UNION ALL SELECT IntNum FROM BQT2.SmallA UNION ALL SELECT IntNum FROM BQT1.SmallA UNION ALL SELECT IntNum FROM BQT3.SmallA UNION ALL (SELECT IntNum FROM BQT2.SmallA UNION ALL SELECT IntNum FROM BQT2.SmallA)", FakeMetadataFactory.exampleBQTCached(), null, capFinder,//$NON-NLS-1$
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
    
}
