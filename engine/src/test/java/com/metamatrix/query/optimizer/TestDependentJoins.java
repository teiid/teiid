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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import junit.framework.TestCase;

import com.metamatrix.query.optimizer.TestOptimizer.ComparisonMode;
import com.metamatrix.query.optimizer.capabilities.BasicSourceCapabilities;
import com.metamatrix.query.optimizer.capabilities.FakeCapabilitiesFinder;
import com.metamatrix.query.optimizer.capabilities.SourceCapabilities.Capability;
import com.metamatrix.query.optimizer.relational.rules.NewCalculateCostUtil;
import com.metamatrix.query.processor.ProcessorPlan;
import com.metamatrix.query.processor.relational.AccessNode;
import com.metamatrix.query.processor.relational.DependentAccessNode;
import com.metamatrix.query.processor.relational.RelationalNode;
import com.metamatrix.query.processor.relational.RelationalPlan;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.Option;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.sql.visitor.GroupCollectorVisitor;
import com.metamatrix.query.unittest.FakeMetadataFacade;
import com.metamatrix.query.unittest.FakeMetadataFactory;
import com.metamatrix.query.unittest.FakeMetadataObject;


public class TestDependentJoins extends TestCase {
    
    static void checkDependentGroups(ProcessorPlan plan, String[] groups) {
        if(! (plan instanceof RelationalPlan)) {
            return;                
        }
                                
        // Collect all the group names (uppercase) for all the dependent groups in the plan
        Set depGroups = new HashSet();
        getDependentGroups(((RelationalPlan)plan).getRootNode(), depGroups);

        // Check that all the expected groups exist in depGroups
        Set expectedGroups = new HashSet();
        for(int i=0; i<groups.length; i++) {
            expectedGroups.add(groups[i].toUpperCase());    
        }        
        
        assertEquals("Expected groups were not made dependent", expectedGroups, depGroups);         //$NON-NLS-1$
    }
    
    static void getDependentGroups(RelationalNode node, Set depGroups) {
        if(node instanceof DependentAccessNode) {
            DependentAccessNode accessNode = (DependentAccessNode)node;
            Command depCommand = accessNode.getCommand();
            Collection groupSymbols = GroupCollectorVisitor.getGroups(depCommand, true);
            Iterator groupIter = groupSymbols.iterator();
            while(groupIter.hasNext()) {
                GroupSymbol group = (GroupSymbol) groupIter.next();
                depGroups.add(group.getName().toUpperCase());    
            }
        }
        
        // Recurse through children
        RelationalNode[] children = node.getChildren();
        for(int i=0; i<children.length; i++) {
            if(children[i] != null) {
                getDependentGroups(node.getChildren()[i], depGroups);                
            }
        }
    }
            
    private void checkNotDependentGroups(ProcessorPlan plan, String[] groups) {
        if(! (plan instanceof RelationalPlan)) {
            return;                
        }
                                
        // Collect all the group names (uppercase) for all the dependent groups in the plan
        Set notDepGroups = new HashSet();
        getNotDependentGroups(((RelationalPlan)plan).getRootNode(), notDepGroups);

        // Check that all the expected groups exist in depGroups
        Set expectedGroups = new HashSet();
        for(int i=0; i<groups.length; i++) {
            expectedGroups.add(groups[i].toUpperCase());    
        }        
        
        assertEquals("Expected groups were made dependent", expectedGroups, notDepGroups);         //$NON-NLS-1$
    }
    
    private void getNotDependentGroups(RelationalNode node, Set notDepGroups) {
        if(node instanceof AccessNode && !(node instanceof DependentAccessNode)) {
            AccessNode accessNode = (AccessNode)node;
            Command depCommand = accessNode.getCommand();
            Collection groupSymbols = GroupCollectorVisitor.getGroups(depCommand, true);
            Iterator groupIter = groupSymbols.iterator();
            while(groupIter.hasNext()) {
                GroupSymbol group = (GroupSymbol) groupIter.next();
                notDepGroups.add(group.getName().toUpperCase());    
            }
        }
        
        // Recurse through children
        RelationalNode[] children = node.getChildren();
        for(int i=0; i<children.length; i++) {
            if(children[i] != null) {
                getNotDependentGroups(node.getChildren()[i], notDepGroups);                
            }
        }
    }
    
    public void testOptionMakeDep1() throws Exception {

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$
        
        Command command = TestOptimizer.helpGetCommand("select pm1.g1.e1 from pm1.g1, pm2.g1 where pm1.g1.e1 = pm2.g1.e1", FakeMetadataFactory.example1Cached(), Collections.EMPTY_LIST); //$NON-NLS-1$

        Option option = new Option();
        option.addDependentGroup("pm2.g1"); //$NON-NLS-1$
        command.setOption(option);
        
        ProcessorPlan plan = TestOptimizer.helpPlanCommand(command, FakeMetadataFactory.example1Cached(), capFinder,
            new String[] { "SELECT g_0.e1 FROM pm2.g1 AS g_0 WHERE g_0.e1 IN (<dependent values>)", "SELECT g_0.e1 FROM pm1.g1 AS g_0" }, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$

        checkDependentGroups(plan, new String[] {"pm2.g1"}); //$NON-NLS-1$
        checkNotDependentGroups(plan, new String[] {"pm1.g1"}); //$NON-NLS-1$
        TestOptimizer.checkNodeTypes(plan, new int[] {
            1,      // Access
            1,      // DependentAccess
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
    
    public void testOptionMakeDep2() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$
        
        Command command = TestOptimizer.helpGetCommand("select pm1.g1.e1, pm2.g1.e1 from pm1.g1 MAKEDEP INNER JOIN pm2.g1 MAKENOTDEP ON pm1.g1.e1 = pm2.g1.e1", FakeMetadataFactory.example1Cached(), Collections.EMPTY_LIST); //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlanCommand(command, FakeMetadataFactory.example1Cached(), capFinder,
            new String[] { "SELECT g_0.e1 FROM pm1.g1 AS g_0 WHERE g_0.e1 IN (<dependent values>)", "SELECT g_0.e1 FROM pm2.g1 AS g_0" }, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$

        checkDependentGroups(plan, new String[] {"pm1.g1"}); //$NON-NLS-1$
        checkNotDependentGroups(plan, new String[] {"pm2.g1"}); //$NON-NLS-1$
        TestOptimizer.checkNodeTypes(plan, new int[] {
            1,      // Access
            1,      // DependentAccess
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
    
    public void testDepJoinHintForceLeft() throws Exception {
    	ProcessorPlan plan = TestOptimizer.helpPlan("select * FROM vm1.g4 option makedep pm1.g1", TestOptimizer.example1(), //$NON-NLS-1$
            new String[] { "SELECT g_0.e1 FROM pm1.g1 AS g_0 WHERE g_0.e1 IN (<dependent values>)", "SELECT g_0.e1 FROM pm1.g2 AS g_0" }, TestOptimizer.getGenericFinder(false), TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING ); //$NON-NLS-1$ //$NON-NLS-2$
        TestOptimizer.checkNodeTypes(plan, new int[] {
            1,      // Access
            1,      // DependentAccess
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
        checkDependentGroups(plan, new String[] {"pm1.g1"});                             //$NON-NLS-1$
    }

    public void testDepJoinHintForceRight() throws Exception {
        
        ProcessorPlan plan = TestOptimizer.helpPlan("select * FROM vm1.g4 option makedep pm1.g2", TestOptimizer.example1(), //$NON-NLS-1$
            new String[] { "SELECT g_0.e1 FROM pm1.g2 AS g_0 WHERE g_0.e1 IN (<dependent values>)", "SELECT g_0.e1 FROM pm1.g1 AS g_0" }, TestOptimizer.getGenericFinder(false), TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING ); //$NON-NLS-1$ //$NON-NLS-2$ 
        TestOptimizer.checkNodeTypes(plan, new int[] {
            1,      // Access
            1,      // DependentAccess
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
        checkDependentGroups(plan, new String[] {"pm1.g2"});                             //$NON-NLS-1$
    }

	public void testDepJoinMultiGroupBaseline() throws Exception {
        ProcessorPlan plan = TestOptimizer.helpPlan("select vm1.g4.*, pm1.g3.e1 FROM vm1.g4, pm1.g3 where pm1.g3.e1=vm1.g4.e1", TestOptimizer.example1(), //$NON-NLS-1$
            new String[] { "SELECT pm1.g1.e1 FROM pm1.g1", //$NON-NLS-1$
                            "SELECT pm1.g2.e1 FROM pm1.g2", //$NON-NLS-1$
                            "SELECT pm1.g3.e1 FROM pm1.g3" }, TestOptimizer.getGenericFinder(false), ComparisonMode.CORRECTED_COMMAND_STRING ); //$NON-NLS-1$
        TestOptimizer.checkNodeTypes(plan, new int[] {
            3,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            2,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        }); 
        checkDependentGroups(plan, new String[0]);                             
    }

    public void testDepJoinMultiGroupForceOther() throws Exception {
        ProcessorPlan plan = TestOptimizer.helpPlan("select vm1.g4.*, pm1.g3.e1 FROM vm1.g4, pm1.g3 where pm1.g3.e1=vm1.g4.e1 option makedep pm1.g2", TestOptimizer.example1(), //$NON-NLS-1$
            new String[] { "SELECT g_0.e1 FROM pm1.g3 AS g_0", "SELECT g_0.e1 FROM pm1.g2 AS g_0 WHERE g_0.e1 IN (<dependent values>)", "SELECT g_0.e1 FROM pm1.g1 AS g_0" }, TestOptimizer.getGenericFinder(false), TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            1,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            2,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        }); 
        checkDependentGroups(plan, new String[] {"pm1.g2"});                             //$NON-NLS-1$ 
    }

    public void testDepJoinHintForceLeft_NotDep() throws Exception {
        ProcessorPlan plan = TestOptimizer.helpPlan("select * FROM vm1.g4 option makedep pm1.g1 makenotdep pm1.g2", TestOptimizer.example1(), //$NON-NLS-1$
            new String[] { "SELECT g_0.e1 FROM pm1.g1 AS g_0 WHERE g_0.e1 IN (<dependent values>)", "SELECT g_0.e1 FROM pm1.g2 AS g_0" }, TestOptimizer.getGenericFinder(false), TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING ); //$NON-NLS-1$ //$NON-NLS-2$
        TestOptimizer.checkNodeTypes(plan, new int[] {
            1,      // Access
            1,      // DependentAccess
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
        checkDependentGroups(plan, new String[] {"pm1.g1"});                             //$NON-NLS-1$
        checkNotDependentGroups(plan, new String[] {"pm1.g2"});                             //$NON-NLS-1$
    }

    public void testDepJoinHintForceRight_NotDep() throws Exception {
        ProcessorPlan plan = TestOptimizer.helpPlan("select * FROM vm1.g4 option makedep pm1.g2 makenotdep pm1.g1", TestOptimizer.example1(), //$NON-NLS-1$
            new String[] { "SELECT g_0.e1 FROM pm1.g2 AS g_0 WHERE g_0.e1 IN (<dependent values>)", "SELECT g_0.e1 FROM pm1.g1 AS g_0" }, TestOptimizer.getGenericFinder(false), TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING ); //$NON-NLS-1$ //$NON-NLS-2$
        TestOptimizer.checkNodeTypes(plan, new int[] {
            1,      // Access
            1,      // DependentAccess
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
        checkDependentGroups(plan, new String[] {"pm1.g2"});                             //$NON-NLS-1$
        checkNotDependentGroups(plan, new String[] {"pm1.g1"});                             //$NON-NLS-1$
    }

    public void testDepJoinMultiGroupForceOther_NotDep() throws Exception {
        ProcessorPlan plan = TestOptimizer.helpPlan("select vm1.g4.*, pm1.g3.e1 FROM vm1.g4, pm1.g3 where pm1.g3.e1=vm1.g4.e1 option makedep pm1.g2 makenotdep pm1.g1, pm1.g3", TestOptimizer.example1(), //$NON-NLS-1$
            new String[] { "SELECT g_0.e1 FROM pm1.g3 AS g_0", "SELECT g_0.e1 FROM pm1.g2 AS g_0 WHERE g_0.e1 IN (<dependent values>)", "SELECT g_0.e1 FROM pm1.g1 AS g_0" }, TestOptimizer.getGenericFinder(false), TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            1,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            2,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        }); 
        checkDependentGroups(plan, new String[] {"pm1.g2"}); //$NON-NLS-1$ 
        checkNotDependentGroups(plan, new String[] {"pm1.g1", "pm1.g3"}); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    /**
     * Test that access node with unsatisfied access pattern is made dependent
     */
    public void testMakeDependentAccessPattern1() throws Exception {
        ProcessorPlan plan = TestOptimizer.helpPlan("select pm1.g1.e1 from pm1.g1, pm4.g1 where pm1.g1.e1 = pm4.g1.e1", FakeMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "SELECT g_0.e1 FROM pm4.g1 AS g_0 WHERE g_0.e1 IN (<dependent values>)", "SELECT g_0.e1 FROM pm1.g1 AS g_0" }, TestOptimizer.getGenericFinder(false), TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$
        checkDependentGroups(plan, new String[] {"pm4.g1"}); //$NON-NLS-1$
    }

    /**
     * Test that access node with unsatisfied access pattern is made dependent
     * (Same query written slightly different way)
     */
    public void testMakeDependentAccessPattern1a() throws Exception {
        ProcessorPlan plan = TestOptimizer.helpPlan("select pm1.g1.e1 from pm4.g1, pm1.g1 where pm4.g1.e1 = pm1.g1.e1", FakeMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "SELECT g_0.e1 FROM pm4.g1 AS g_0 WHERE g_0.e1 IN (<dependent values>)", "SELECT g_0.e1 FROM pm1.g1 AS g_0" }, TestOptimizer.getGenericFinder(false), TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$
        checkDependentGroups(plan, new String[] {"pm4.g1"}); //$NON-NLS-1$
    }
    
    /**
     * Test that access node with unsatisfied access pattern is made dependent
     */
    public void testMakeDependentAccessPattern2() throws Exception {
        ProcessorPlan plan = TestOptimizer.helpPlan("select pm1.g1.e1 from pm1.g1, pm4.g1 where pm4.g1.e2 = 1 and pm1.g1.e1 = pm4.g1.e1", FakeMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "SELECT g_0.e1 FROM pm4.g1 AS g_0 WHERE (g_0.e2 = 1) AND (g_0.e1 IN (<dependent values>))", "SELECT g_0.e1 FROM pm1.g1 AS g_0" }, TestOptimizer.getGenericFinder(false), TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$
        checkDependentGroups(plan, new String[] {"pm4.g1"}); //$NON-NLS-1$
    }

    /**
     * Test that second access pattern of access node is chosen to make
     * dependent with
     */
    public void testMakeDependentAccessPattern3() throws Exception {
        ProcessorPlan plan = TestOptimizer.helpPlan("select pm1.g1.e1 from pm1.g1, pm4.g2 where pm1.g1.e1 = pm4.g2.e5", FakeMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "SELECT g_0.e5 FROM pm4.g2 AS g_0 WHERE g_0.e5 IN (<dependent values>)", "SELECT g_0.e1 FROM pm1.g1 AS g_0" }, TestOptimizer.getGenericFinder(false), TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$
        checkDependentGroups(plan, new String[] {"pm4.g2"}); //$NON-NLS-1$
    }

    /**
     * This case actually tests the dead-tie case - either access node could
     * be made dependent, but merge join is used since no access pattern 
     * needs to be fulfilled and there is no cost info available for either source
     */
    public void testPushSelectAndMakeDependentAccessPattern1() throws Exception {
        ProcessorPlan plan = TestOptimizer.helpPlan("select pm1.g1.e1 from pm1.g1, pm4.g1 where pm4.g1.e1 = 'abc' and pm1.g1.e1 = 'abc' and pm1.g1.e2 = pm4.g1.e2", FakeMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "SELECT pm1.g1.e2, pm1.g1.e1 FROM pm1.g1 WHERE pm1.g1.e1 = 'abc'", "SELECT pm4.g1.e2 FROM pm4.g1 WHERE pm4.g1.e1 = 'abc'" }, TestOptimizer.getGenericFinder(false), ComparisonMode.CORRECTED_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$
        checkDependentGroups(plan, new String[0]); 

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

    /**
     * This case actually tests the dead-tie case - either access node could
     * be made dependent, but merge join is used since no access pattern 
     * needs to be fulfilled and there is no cost info available for either source
     * (Same query written slightly different)
     */
    public void testPushSelectAndMakeDependentAccessPattern1a() throws Exception {
        ProcessorPlan plan = TestOptimizer.helpPlan("select pm1.g1.e1 from pm4.g1, pm1.g1 where pm4.g1.e2 = pm1.g1.e2 and pm4.g1.e1 = 'abc' and pm1.g1.e1 = 'abc'", FakeMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "SELECT pm1.g1.e2, pm1.g1.e1 FROM pm1.g1 WHERE pm1.g1.e1 = 'abc'", "SELECT pm4.g1.e2 FROM pm4.g1 WHERE pm4.g1.e1 = 'abc'" }, TestOptimizer.getGenericFinder(false), ComparisonMode.CORRECTED_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$
        checkDependentGroups(plan, new String[0]); 
    
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

    /**
     * Tests that it is enforced if an access node can't be made dependent
     * because of it's (already-satisfied) access pattern - merge join is used
     */
    public void testPushSelectAndMakeDependentAccessPattern2() throws Exception {
        ProcessorPlan plan = TestOptimizer.helpPlan("select pm1.g1.e1 from pm1.g1, pm4.g1 where pm4.g1.e1 = 'abc' and pm1.g1.e2 = pm4.g1.e2", FakeMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "SELECT pm1.g1.e2, pm1.g1.e1 FROM pm1.g1", "SELECT pm4.g1.e2 FROM pm4.g1 WHERE pm4.g1.e1 = 'abc'" }, TestOptimizer.getGenericFinder(false), ComparisonMode.CORRECTED_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$
        checkDependentGroups(plan, new String[0] ); 

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
    
    /** Should use dependent join since one access node is "strong" */
    public void testUseMergeJoin1() throws Exception {
        // Create query
        String sql = "SELECT pm1.g1.e1 FROM pm1.g1, pm1.g2 WHERE pm1.g1.e1 = pm1.g2.e1";//$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.CRITERIA_QUANTIFIED_SOME, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, new Integer(10));
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        FakeMetadataObject obj = metadata.getStore().findObject("pm1.g1", FakeMetadataObject.GROUP); //$NON-NLS-1$
        obj.putProperty(FakeMetadataObject.Props.CARDINALITY, new Integer(NewCalculateCostUtil.DEFAULT_STRONG_COST - 1));
    
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, metadata,  
            null, capFinder,
            new String[] { "SELECT g_0.e1 FROM pm1.g2 AS g_0 WHERE g_0.e1 IN (<dependent values>)", "SELECT g_0.e1 FROM pm1.g1 AS g_0" }, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$
        TestOptimizer.checkNodeTypes(plan, new int[] {
            1,      // Access
            1,      // DependentAccess
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

    /** Should not use a dependent join since neither access node is "strong" */
    public void testUseMergeJoin2() throws Exception {
        // Create query
        String sql = "SELECT pm1.g1.e1 FROM pm1.g1, pm1.g2 WHERE pm1.g1.e1 = pm1.g2.e1";//$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        FakeMetadataObject obj = metadata.getStore().findObject("pm1.g1", FakeMetadataObject.GROUP); //$NON-NLS-1$
        obj.putProperty(FakeMetadataObject.Props.CARDINALITY, new Integer(NewCalculateCostUtil.DEFAULT_STRONG_COST + 1));
    
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, metadata,  
            null, capFinder,
            new String[] { "SELECT pm1.g1.e1 FROM pm1.g1", "SELECT pm1.g2.e1 FROM pm1.g2" }, TestOptimizer.ComparisonMode.CORRECTED_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$
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
    
    /** should have one dependent joins */
    public void testMultiMergeJoin3() throws Exception {
        // Create query
        String sql = "SELECT pm1.g2.e1 FROM pm1.g3, pm1.g2, pm1.g1 WHERE pm1.g2.e1 = pm1.g3.e1 AND pm1.g1.e1 = pm1.g2.e1";//$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT, true);
        caps.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, new Integer(1000));
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        FakeMetadataObject g1 = metadata.getStore().findObject("pm1.g1", FakeMetadataObject.GROUP); //$NON-NLS-1$
        g1.putProperty(FakeMetadataObject.Props.CARDINALITY, new Integer(NewCalculateCostUtil.DEFAULT_STRONG_COST + 1));
        FakeMetadataObject g2 = metadata.getStore().findObject("pm1.g2", FakeMetadataObject.GROUP); //$NON-NLS-1$
        g2.putProperty(FakeMetadataObject.Props.CARDINALITY, new Integer(NewCalculateCostUtil.DEFAULT_STRONG_COST - 1 ));
        FakeMetadataObject g3 = metadata.getStore().findObject("pm1.g3", FakeMetadataObject.GROUP); //$NON-NLS-1$
        g3.putProperty(FakeMetadataObject.Props.CARDINALITY, new Integer(NewCalculateCostUtil.DEFAULT_STRONG_COST + 1000));
    
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, metadata,  
            null, capFinder,
            new String[] { "SELECT g_0.e1 FROM pm1.g3 AS g_0 WHERE g_0.e1 IN (<dependent values>)", "SELECT g_0.e1 FROM pm1.g2 AS g_0", "SELECT g_0.e1 FROM pm1.g1 AS g_0" }, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            1,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            2,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });         
    } 
    
    public void testMultiMergeJoin2() throws Exception {
        String sql = "SELECT pm1.g1.e1 FROM pm1.g1, pm1.g2, pm1.g3 WHERE pm1.g1.e1 = pm1.g2.e1 AND pm1.g2.e1 = pm1.g3.e1";//$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT, true);
        caps.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, new Integer(1000));
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        FakeMetadataObject g1 = metadata.getStore().findObject("pm1.g1", FakeMetadataObject.GROUP); //$NON-NLS-1$
        g1.putProperty(FakeMetadataObject.Props.CARDINALITY, new Integer(NewCalculateCostUtil.DEFAULT_STRONG_COST - 1));
        FakeMetadataObject g2 = metadata.getStore().findObject("pm1.g2", FakeMetadataObject.GROUP); //$NON-NLS-1$
        g2.putProperty(FakeMetadataObject.Props.CARDINALITY, new Integer(NewCalculateCostUtil.DEFAULT_STRONG_COST + 1000));
        FakeMetadataObject g3 = metadata.getStore().findObject("pm1.g3", FakeMetadataObject.GROUP); //$NON-NLS-1$
        g3.putProperty(FakeMetadataObject.Props.CARDINALITY, new Integer(NewCalculateCostUtil.DEFAULT_STRONG_COST + 1000));
    
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, metadata,  
            null, capFinder,
            new String[] { "SELECT g_0.e1 FROM pm1.g3 AS g_0", "SELECT g_0.e1 FROM pm1.g2 AS g_0 WHERE g_0.e1 IN (<dependent values>)", "SELECT g_0.e1 FROM pm1.g1 AS g_0" }, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            1,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            2,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });         
    } 

    /**
     * Defect 13448 
     * should be one merge join and one dependent join
     * Unlike the above tests, here the model pm1 supports ORDER BY.
     */
    public void testMultiMergeJoin5_defect13448() throws Exception {
        // Create query
        String sql = "SELECT pm1.g1.e1 FROM pm1.g1, pm1.g2, pm1.g3 WHERE pm1.g1.e1 = pm1.g2.e1 AND pm1.g1.e1 = pm1.g3.e1";//$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT, true);
        caps.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, new Integer(1000));
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        FakeMetadataObject g1 = metadata.getStore().findObject("pm1.g1", FakeMetadataObject.GROUP); //$NON-NLS-1$
        g1.putProperty(FakeMetadataObject.Props.CARDINALITY, new Integer(NewCalculateCostUtil.DEFAULT_STRONG_COST - 1));
        FakeMetadataObject g2 = metadata.getStore().findObject("pm1.g2", FakeMetadataObject.GROUP); //$NON-NLS-1$
        g2.putProperty(FakeMetadataObject.Props.CARDINALITY, new Integer(NewCalculateCostUtil.DEFAULT_STRONG_COST + 1000));
        FakeMetadataObject g3 = metadata.getStore().findObject("pm1.g3", FakeMetadataObject.GROUP); //$NON-NLS-1$
        g3.putProperty(FakeMetadataObject.Props.CARDINALITY, new Integer(NewCalculateCostUtil.DEFAULT_STRONG_COST + 1000));
    
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, metadata,  
            null, capFinder,
            new String[] { "SELECT g_0.e1 AS c_0 FROM pm1.g1 AS g_0 ORDER BY c_0", "SELECT g_0.e1 AS c_0 FROM pm1.g3 AS g_0 ORDER BY c_0", "SELECT g_0.e1 AS c_0 FROM pm1.g2 AS g_0 WHERE g_0.e1 IN (<dependent values>) ORDER BY c_0" }, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            1,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            2,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });         
    }
    
    public void testMergeJoinVirtualGroups() throws Exception {
        String sql = "SELECT vm1.g1.e1 FROM vm1.g1, vm1.g2a WHERE vm1.g1.e1 = vm1.g2a.e1";//$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT, true);
        caps.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, new Integer(1000));
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        FakeMetadataObject g1 = metadata.getStore().findObject("pm1.g1", FakeMetadataObject.GROUP); //$NON-NLS-1$
        g1.putProperty(FakeMetadataObject.Props.CARDINALITY, new Integer(NewCalculateCostUtil.DEFAULT_STRONG_COST - 1));
        FakeMetadataObject g2 = metadata.getStore().findObject("pm1.g2", FakeMetadataObject.GROUP); //$NON-NLS-1$
        g2.putProperty(FakeMetadataObject.Props.CARDINALITY, new Integer(NewCalculateCostUtil.DEFAULT_STRONG_COST + 1000));
    
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, metadata,  
            null, capFinder,
            new String[] { "SELECT g_0.e1 AS c_0 FROM pm1.g1 AS g_0 ORDER BY c_0", "SELECT g_0.e1 AS c_0 FROM pm1.g2 AS g_0 WHERE g_0.e1 IN (<dependent values>) ORDER BY c_0" }, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$ 
        TestOptimizer.checkNodeTypes(plan, new int[] {
            1,      // Access
            1,      // DependentAccess
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
    
    public void testRLMCase2077() throws Exception {
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.QUERY_SEARCHED_CASE, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, new Integer(1000));
        caps.setFunctionSupport("||", true); //$NON-NLS-1$
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("BQT2", caps); //$NON-NLS-1$

        FakeMetadataFacade metadata = FakeMetadataFactory.exampleBQT();
        FakeMetadataObject g1 = metadata.getStore().findObject("BQT1.SmallA", FakeMetadataObject.GROUP); //$NON-NLS-1$
        g1.putProperty(FakeMetadataObject.Props.CARDINALITY, new Integer(NewCalculateCostUtil.DEFAULT_STRONG_COST + 1000));
        FakeMetadataObject g2 = metadata.getStore().findObject("BQT2.SmallA", FakeMetadataObject.GROUP); //$NON-NLS-1$
        g2.putProperty(FakeMetadataObject.Props.CARDINALITY, new Integer(NewCalculateCostUtil.DEFAULT_STRONG_COST - 1));
         
        ProcessorPlan plan = TestOptimizer.helpPlan(
            "SELECT table1comp.IntKey, table1comp.key1, BQT1.SmallA.StringKey FROM (SELECT t1.*, (STRINGKEY || STRINGNUM) AS key1 FROM BQT2.SmallA AS t1) AS table1comp, BQT1.SmallA WHERE table1comp.key1 = BQT1.SmallA.StringKey",  //$NON-NLS-1$
            metadata,
            null, capFinder,
            new String[] {"SELECT g_0.StringKey FROM BQT1.SmallA AS g_0 WHERE g_0.StringKey IN (<dependent values>)", "SELECT g_0.STRINGKEY, g_0.STRINGNUM, g_0.IntKey FROM BQT2.SmallA AS g_0"}, //$NON-NLS-1$ //$NON-NLS-2$
            TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING );

        TestOptimizer.checkNodeTypes(plan, new int[] {
            1,      // Access
            1,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
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

    public void testRLMCase2077_2() throws Exception {
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.QUERY_SEARCHED_CASE, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, new Integer(1000));
        caps.setFunctionSupport("||", true); //$NON-NLS-1$
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("BQT2", caps); //$NON-NLS-1$

        FakeMetadataFacade metadata = FakeMetadataFactory.exampleBQT();
        FakeMetadataObject g1 = metadata.getStore().findObject("BQT1.SmallA", FakeMetadataObject.GROUP); //$NON-NLS-1$
        g1.putProperty(FakeMetadataObject.Props.CARDINALITY, new Integer(NewCalculateCostUtil.DEFAULT_STRONG_COST + 1000));
        FakeMetadataObject g2 = metadata.getStore().findObject("BQT2.SmallA", FakeMetadataObject.GROUP); //$NON-NLS-1$
        g2.putProperty(FakeMetadataObject.Props.CARDINALITY, new Integer(NewCalculateCostUtil.DEFAULT_STRONG_COST - 1));
         
        ProcessorPlan plan = TestOptimizer.helpPlan(
            "SELECT table1comp.IntKey, table1comp.key1, BQT1.SmallA.StringKey FROM (SELECT t1.*, (STRINGKEY || STRINGNUM) AS key1 FROM BQT2.SmallA AS t1) AS table1comp, BQT1.SmallA WHERE table1comp.key1 = BQT1.SmallA.StringKey AND table1comp.key1 = BQT1.SmallA.StringNum",  //$NON-NLS-1$
            metadata,
            null, capFinder,
            new String[] {"SELECT g_0.STRINGKEY, g_0.STRINGNUM, g_0.IntKey FROM BQT2.SmallA AS g_0", "SELECT g_0.StringKey, g_0.StringNum FROM BQT1.SmallA AS g_0 WHERE (g_0.StringKey IN (<dependent values>)) AND (g_0.StringNum IN (<dependent values>))"}, //$NON-NLS-1$ //$NON-NLS-2$
            TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING );

        TestOptimizer.checkNodeTypes(plan, new int[] {
            1,      // Access
            1,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
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

}
