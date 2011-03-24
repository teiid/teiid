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

import static org.junit.Assert.*;
import static org.teiid.query.optimizer.TestOptimizer.*;

import org.junit.Test;
import org.teiid.query.optimizer.TestOptimizer.DupRemoveSortNode;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.FakeCapabilitiesFinder;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.relational.LimitNode;
import org.teiid.query.processor.relational.ProjectNode;
import org.teiid.query.processor.relational.RelationalPlan;
import org.teiid.query.unittest.FakeMetadataFactory;


public class TestSortOptimization {

    @Test public void testSortDupCombination() { 
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        // Create query 
        String sql = "select distinct e1, e2 from pm1.g1 order by e2"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql, FakeMetadataFactory.example1Cached(), null, capFinder, 
                                      new String[] {"SELECT e1, e2 FROM pm1.g1"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$
        
        checkNodeTypes(plan, FULL_PUSHDOWN); 
        checkNodeTypes(plan, new int[] {1}, new Class[] {DupRemoveSortNode.class});
    }
    
    @Test public void testSortDupCombination1() { 
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        // Create query 
        String sql = "select e1, e2 from pm1.g1 union select e1, e2 from pm1.g2 order by e2"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql, FakeMetadataFactory.example1Cached(), null, capFinder, 
                                      new String[] {"SELECT e1, e2 FROM pm1.g1", "SELECT e1, e2 FROM pm1.g2"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$ //$NON-NLS-2$
        
        checkNodeTypes(plan, new int[] {
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
        checkNodeTypes(plan, new int[] {1}, new Class[] {DupRemoveSortNode.class});
    }
	
    @Test public void testSortDupCombination2() { 
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        // Create query 
        String sql = "select x.*, y.* from (select distinct e1, e2 from pm1.g1) x, (select distinct e1, e2 from pm1.g2) y where x.e1 = y.e1"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql, FakeMetadataFactory.example1Cached(), null, capFinder, 
                                      new String[] {"SELECT e1, e2 FROM pm1.g1", "SELECT e1, e2 FROM pm1.g2"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$ //$NON-NLS-2$
        
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
        checkNodeTypes(plan, new int[] {0}, new Class[] {DupRemoveSortNode.class});
    }
    
    @Test public void testGroupDupCombination() { 
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        // Create query 
        String sql = "select max(e1), e2 from (select distinct e1, e2 from pm1.g1) x group by e2"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql, FakeMetadataFactory.example1Cached(), null, capFinder, 
                                      new String[] {"SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$ 
        
        checkNodeTypes(plan, new int[] {
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
        checkNodeTypes(plan, new int[] {0}, new Class[] {DupRemoveSortNode.class});
    }

    @Test public void testSortGroupCombination() { 
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        // Create query 
        String sql = "select max(e1), e2 from pm1.g1 x group by e2 order by e2"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sql, FakeMetadataFactory.example1Cached(), null, capFinder, 
                                      new String[] {"SELECT pm1.g1.e2, pm1.g1.e1 FROM pm1.g1"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$ 
        
        checkNodeTypes(plan, new int[] {
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
        checkNodeTypes(plan, new int[] {0}, new Class[] {DupRemoveSortNode.class});
    }
    
    @Test public void testProjectionRaisingWithLimit() { 
        // Create query 
        String sql = "select e1, (select e1 from pm2.g1 where e2 = x.e2) from pm1.g1 as x order by e2 limit 2"; //$NON-NLS-1$

        RelationalPlan plan = (RelationalPlan)helpPlan(sql, FakeMetadataFactory.example1Cached(), null, new DefaultCapabilitiesFinder(), 
                                      new String[] {"SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$
        
        assertTrue(plan.getRootNode() instanceof ProjectNode);
    }
    
    @Test public void testProjectionRaisingWithLimit1() { 
        // Create query 
        String sql = "select (select e1 from pm2.g1 where e2 = x.e2) as z from pm1.g1 as x order by z limit 2"; //$NON-NLS-1$

        RelationalPlan plan = (RelationalPlan)helpPlan(sql, FakeMetadataFactory.example1Cached(), null, new DefaultCapabilitiesFinder(), 
                                      new String[] {"SELECT pm1.g1.e2 FROM pm1.g1"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$
        
        assertTrue(plan.getRootNode() instanceof LimitNode);
    }

}
