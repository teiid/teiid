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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.metadata.Column;
import org.teiid.query.metadata.CompositeMetadataStore;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.TestOptimizer.ComparisonMode;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.FakeCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.optimizer.relational.rules.RuleChooseDependent;
import org.teiid.query.processor.HardcodedDataManager;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.TestProcessor;
import org.teiid.query.processor.relational.AccessNode;
import org.teiid.query.processor.relational.DependentAccessNode;
import org.teiid.query.processor.relational.JoinNode;
import org.teiid.query.processor.relational.RelationalNode;
import org.teiid.query.processor.relational.RelationalPlan;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.visitor.GroupCollectorVisitor;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings("nls")
public class TestDependentJoins {

    static void checkDependentGroups(ProcessorPlan plan, String[] groups) {
        if(! (plan instanceof RelationalPlan)) {
            return;
        }

        // Collect all the group names (uppercase) for all the dependent groups in the plan
        Set<String> depGroups = new HashSet<String>();
        getDependentGroups(((RelationalPlan)plan).getRootNode(), depGroups, true);

        // Check that all the expected groups exist in depGroups
        Set<String> expectedGroups = new HashSet<String>();
        for(int i=0; i<groups.length; i++) {
            expectedGroups.add(groups[i].toUpperCase());
        }

        assertEquals("Expected groups were not made dependent", expectedGroups, depGroups);         //$NON-NLS-1$
    }

    static void getDependentGroups(RelationalNode node, Set<String> depGroups, boolean depdenent) {
        if(node instanceof AccessNode) {
            if (node instanceof DependentAccessNode) {
                if (!depdenent) {
                    return;
                }
            } else if (depdenent) {
                return;
            }
            AccessNode accessNode = (AccessNode)node;
            Command depCommand = accessNode.getCommand();
            Collection<GroupSymbol> groupSymbols = GroupCollectorVisitor.getGroups(depCommand, true);
            for (GroupSymbol groupSymbol : groupSymbols) {
                depGroups.add(groupSymbol.getNonCorrelationName().toUpperCase());
            }
        }

        // Recurse through children
        RelationalNode[] children = node.getChildren();
        for(int i=0; i<children.length; i++) {
            if(children[i] != null) {
                getDependentGroups(node.getChildren()[i], depGroups, depdenent);
            }
        }
    }

    private void checkNotDependentGroups(ProcessorPlan plan, String[] groups) {
        if(! (plan instanceof RelationalPlan)) {
            return;
        }

        // Collect all the group names (uppercase) for all the dependent groups in the plan
        Set<String> notDepGroups = new HashSet<String>();
        getDependentGroups(((RelationalPlan)plan).getRootNode(), notDepGroups, false);

        // Check that all the expected groups exist in depGroups
        Set<String> expectedGroups = new HashSet<String>();
        for(int i=0; i<groups.length; i++) {
            expectedGroups.add(groups[i].toUpperCase());
        }

        assertEquals("Expected groups were made dependent", expectedGroups, notDepGroups);         //$NON-NLS-1$
    }

    @Test public void testOptionMakeDep1() throws Exception {

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan("select pm1.g1.e1 from pm1.g1, pm2.g1 where pm1.g1.e1 = pm2.g1.e1 option makedep pm2.g1", RealMetadataFactory.example1Cached(), null, capFinder, //$NON-NLS-1$
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

    @Test public void testOptionMakeDep2() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan("select pm1.g1.e1, pm2.g1.e1 from pm1.g1 MAKEDEP INNER JOIN pm2.g1 MAKENOTDEP ON pm1.g1.e1 = pm2.g1.e1", RealMetadataFactory.example1Cached(), null, capFinder, //$NON-NLS-1$
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

    @Test public void testDepJoinHintForceLeft() throws Exception {
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

    @Test public void testDepJoinHintForceRight() throws Exception {

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

    @Test public void testGlobalHint() throws Exception {
        ProcessorPlan plan = TestOptimizer.helpPlan("select * FROM vm1.g4 option makedep @g4.g2", TestOptimizer.example1(), //$NON-NLS-1$
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

    @Test public void testGlobalHint1() throws Exception {
        ProcessorPlan plan = TestOptimizer.helpPlan("select * FROM vm1.g4 as x option makedep @x.g2", TestOptimizer.example1(), //$NON-NLS-1$
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

    @Test public void testGlobalHintSetQuery() throws Exception {
        ProcessorPlan plan = TestOptimizer.helpPlan("(Select e1 from pm1.g1 Intersect select abc.e1 from (select pm1.g2.e1 from pm1.g2, pm1.g3 where pm1.g2.e2 = pm1.g3.e2) abc) option makedep @abc.g2", TestOptimizer.example1(), //$NON-NLS-1$
            new String[] { "SELECT g_0.e1 FROM pm1.g1 AS g_0", "SELECT g_0.e2, g_0.e1 FROM pm1.g2 AS g_0 WHERE g_0.e2 IN (<dependent values>)", "SELECT g_0.e2 FROM pm1.g3 AS g_0" }, TestOptimizer.getGenericFinder(false), TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING ); //$NON-NLS-1$ //$NON-NLS-2$
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

    @Test public void testDepJoinMultiGroupBaseline() throws Exception {
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

    @Test public void testDepJoinMultiGroupForceOther() throws Exception {
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

    @Test public void testDepJoinHintForceLeft_NotDep() throws Exception {
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

    @Test public void testDepJoinHintForceRight_NotDep() throws Exception {
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

    @Test public void testDepJoinMultiGroupForceOther_NotDep() throws Exception {
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
    @Test public void testMakeDependentAccessPattern1() throws Exception {
        ProcessorPlan plan = TestOptimizer.helpPlan("select pm1.g1.e1 from pm1.g1, pm4.g1 where pm1.g1.e1 = pm4.g1.e1", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "SELECT g_0.e1 FROM pm4.g1 AS g_0 WHERE g_0.e1 IN (<dependent values>)", "SELECT g_0.e1 FROM pm1.g1 AS g_0" }, TestOptimizer.getGenericFinder(false), TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$
        checkDependentGroups(plan, new String[] {"pm4.g1"}); //$NON-NLS-1$
    }

    /**
     * Test that access node with unsatisfied access pattern is made dependent
     * (Same query written slightly different way)
     */
    @Test public void testMakeDependentAccessPattern1a() throws Exception {
        ProcessorPlan plan = TestOptimizer.helpPlan("select pm1.g1.e1 from pm4.g1, pm1.g1 where pm4.g1.e1 = pm1.g1.e1", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "SELECT g_0.e1 FROM pm4.g1 AS g_0 WHERE g_0.e1 IN (<dependent values>)", "SELECT g_0.e1 FROM pm1.g1 AS g_0" }, TestOptimizer.getGenericFinder(false), TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$
        checkDependentGroups(plan, new String[] {"pm4.g1"}); //$NON-NLS-1$
    }

    /**
     * Test that access node with unsatisfied access pattern is made dependent
     */
    @Test public void testMakeDependentAccessPattern2() throws Exception {
        ProcessorPlan plan = TestOptimizer.helpPlan("select pm1.g1.e1 from pm1.g1, pm4.g1 where pm4.g1.e2 = 1 and pm1.g1.e1 = pm4.g1.e1", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "SELECT g_0.e1 FROM pm4.g1 AS g_0 WHERE (g_0.e2 = 1) AND (g_0.e1 IN (<dependent values>))", "SELECT g_0.e1 FROM pm1.g1 AS g_0" }, TestOptimizer.getGenericFinder(false), TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$
        checkDependentGroups(plan, new String[] {"pm4.g1"}); //$NON-NLS-1$
    }

    /**
     * Test that second access pattern of access node is chosen to make
     * dependent with
     */
    @Test public void testMakeDependentAccessPattern3() throws Exception {
        ProcessorPlan plan = TestOptimizer.helpPlan("select pm1.g1.e1 from pm1.g1, pm4.g2 where pm1.g1.e1 = pm4.g2.e5", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "SELECT g_0.e5 FROM pm4.g2 AS g_0 WHERE g_0.e5 IN (<dependent values>)", "SELECT g_0.e1 FROM pm1.g1 AS g_0" }, TestOptimizer.getGenericFinder(false), TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$
        checkDependentGroups(plan, new String[] {"pm4.g2"}); //$NON-NLS-1$
    }

    /**
     * This case actually tests the dead-tie case - either access node could
     * be made dependent, but merge join is used since no access pattern
     * needs to be fulfilled and there is no cost info available for either source
     */
    @Test public void testPushSelectAndMakeDependentAccessPattern1() throws Exception {
        ProcessorPlan plan = TestOptimizer.helpPlan("select pm1.g1.e1 from pm1.g1, pm4.g1 where pm4.g1.e1 = 'abc' and pm1.g1.e1 = 'abc' and pm1.g1.e2 = pm4.g1.e2", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
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
    @Test public void testPushSelectAndMakeDependentAccessPattern1a() throws Exception {
        ProcessorPlan plan = TestOptimizer.helpPlan("select pm1.g1.e1 from pm4.g1, pm1.g1 where pm4.g1.e2 = pm1.g1.e2 and pm4.g1.e1 = 'abc' and pm1.g1.e1 = 'abc'", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
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
    @Test public void testPushSelectAndMakeDependentAccessPattern2() throws Exception {
        ProcessorPlan plan = TestOptimizer.helpPlan("select pm1.g1.e1 from pm1.g1, pm4.g1 where pm4.g1.e1 = 'abc' and pm1.g1.e2 = pm4.g1.e2", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
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
    @Test public void testUseMergeJoin1() throws Exception {
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

        QueryMetadataInterface metadata = RealMetadataFactory.example1();
        RealMetadataFactory.setCardinality("pm1.g1", RuleChooseDependent.DEFAULT_INDEPENDENT_CARDINALITY - 1, metadata);

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
    @Test public void testUseMergeJoin2() throws Exception {
        // Create query
        String sql = "SELECT pm1.g1.e1 FROM pm1.g1, pm1.g2 WHERE pm1.g1.e1 = pm1.g2.e1";//$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1();
        RealMetadataFactory.setCardinality("pm1.g1", RuleChooseDependent.DEFAULT_INDEPENDENT_CARDINALITY + 1, metadata);
        RealMetadataFactory.setCardinality("pm1.g2", RuleChooseDependent.DEFAULT_INDEPENDENT_CARDINALITY + 1, metadata);

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
    @Test public void testMultiMergeJoin3() throws Exception {
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

        QueryMetadataInterface metadata = RealMetadataFactory.example1();
        RealMetadataFactory.setCardinality("pm1.g1", RuleChooseDependent.DEFAULT_INDEPENDENT_CARDINALITY + 1, metadata);
        RealMetadataFactory.setCardinality("pm1.g2", RuleChooseDependent.DEFAULT_INDEPENDENT_CARDINALITY - 1, metadata);
        RealMetadataFactory.setCardinality("pm1.g3", RuleChooseDependent.DEFAULT_INDEPENDENT_CARDINALITY + 1000, metadata);

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

    @Test public void testMultiMergeJoin2() throws Exception {
        String sql = "SELECT pm1.g1.e1 FROM pm1.g1, pm1.g2, pm1.g3 WHERE pm1.g1.e1 = pm1.g2.e1 AND pm1.g2.e1 = pm1.g3.e1";//$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT, true);
        caps.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, new Integer(1000));
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1();
        RealMetadataFactory.setCardinality("pm1.g1", RuleChooseDependent.DEFAULT_INDEPENDENT_CARDINALITY - 1, metadata);
        RealMetadataFactory.setCardinality("pm1.g2", RuleChooseDependent.DEFAULT_INDEPENDENT_CARDINALITY + 1000, metadata);
        RealMetadataFactory.setCardinality("pm1.g3", RuleChooseDependent.DEFAULT_INDEPENDENT_CARDINALITY + 1000, metadata);

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, metadata,
            null, capFinder,
            new String[] { "SELECT g_0.e1 FROM pm1.g1 AS g_0", "SELECT g_0.e1 FROM pm1.g2 AS g_0 WHERE g_0.e1 IN (<dependent values>)", "SELECT g_0.e1 FROM pm1.g3 AS g_0 WHERE g_0.e1 IN (<dependent values>)" }, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        TestOptimizer.checkNodeTypes(plan, new int[] {
            1,      // Access
            2,      // DependentAccess
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
     * should be one merge join and two dependent join
     * Unlike the above tests, here the model pm1 supports ORDER BY.
     */
    @Test public void testMultiMergeJoin5_defect13448() throws Exception {
        // Create query
        String sql = "SELECT pm1.g1.e1 FROM pm1.g1, pm1.g2, pm1.g3 WHERE pm1.g1.e1 = pm1.g2.e1 AND pm1.g1.e1 = pm1.g3.e1";//$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        caps.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, new Integer(1000));
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1();
        RealMetadataFactory.setCardinality("pm1.g1", RuleChooseDependent.DEFAULT_INDEPENDENT_CARDINALITY - 1, metadata);
        RealMetadataFactory.setCardinality("pm1.g2", RuleChooseDependent.DEFAULT_INDEPENDENT_CARDINALITY + 1000, metadata);
        RealMetadataFactory.setCardinality("pm1.g3", RuleChooseDependent.DEFAULT_INDEPENDENT_CARDINALITY + 1000, metadata);

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, metadata,
            null, capFinder,
            new String[] { "SELECT g_0.e1 AS c_0 FROM pm1.g2 AS g_0 WHERE g_0.e1 IN (<dependent values>) ORDER BY c_0", "SELECT g_0.e1 AS c_0 FROM pm1.g3 AS g_0 WHERE g_0.e1 IN (<dependent values>) ORDER BY c_0", "SELECT g_0.e1 AS c_0 FROM pm1.g1 AS g_0 ORDER BY c_0" }, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        TestOptimizer.checkNodeTypes(plan, new int[] {
            1,      // Access
            2,      // DependentAccess
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

    @Test public void testMergeJoinVirtualGroups() throws Exception {
        String sql = "SELECT vm1.g1.e1 FROM vm1.g1, vm1.g2a WHERE vm1.g1.e1 = vm1.g2a.e1";//$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        caps.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, new Integer(1000));
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1();
        RealMetadataFactory.setCardinality("pm1.g1", RuleChooseDependent.DEFAULT_INDEPENDENT_CARDINALITY - 1, metadata);
        RealMetadataFactory.setCardinality("pm1.g2", RuleChooseDependent.DEFAULT_INDEPENDENT_CARDINALITY + 1000, metadata);

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

    @Test public void testRLMCase2077() throws Exception {

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

        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQT();
        RealMetadataFactory.setCardinality("bqt1.smalla", RuleChooseDependent.DEFAULT_INDEPENDENT_CARDINALITY + 1000, metadata); //$NON-NLS-1$
        RealMetadataFactory.setCardinality("bqt2.smalla", RuleChooseDependent.DEFAULT_INDEPENDENT_CARDINALITY - 1, metadata); //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan(
            "SELECT table1comp.IntKey, table1comp.key1, BQT1.SmallA.StringKey FROM (SELECT t1.*, (STRINGKEY || STRINGNUM) AS key1 FROM BQT2.SmallA AS t1) AS table1comp, BQT1.SmallA WHERE table1comp.key1 = BQT1.SmallA.StringKey",  //$NON-NLS-1$
            metadata,
            null, capFinder,
            new String[] {"SELECT g_0.StringKey, g_0.StringNum, g_0.IntKey FROM BQT2.SmallA AS g_0", "SELECT g_0.StringKey FROM BQT1.SmallA AS g_0 WHERE g_0.StringKey IN (<dependent values>)"}, //$NON-NLS-1$ //$NON-NLS-2$
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

    @Test public void testRLMCase2077_2() throws Exception {

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.QUERY_SEARCHED_CASE, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.ARRAY_TYPE, true);
        caps.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, new Integer(1000));
        caps.setFunctionSupport("||", true); //$NON-NLS-1$
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("BQT2", caps); //$NON-NLS-1$

        TransformationMetadata metadata = RealMetadataFactory.exampleBQT();
        RealMetadataFactory.setCardinality("bqt1.smalla", RuleChooseDependent.DEFAULT_INDEPENDENT_CARDINALITY + 1000, metadata); //$NON-NLS-1$
        RealMetadataFactory.setCardinality("bqt2.smalla", RuleChooseDependent.DEFAULT_INDEPENDENT_CARDINALITY - 1, metadata); //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan(
            "SELECT table1comp.IntKey, table1comp.key1, BQT1.SmallA.StringKey FROM (SELECT t1.*, (STRINGKEY || STRINGNUM) AS key1 FROM BQT2.SmallA AS t1) AS table1comp, BQT1.SmallA WHERE table1comp.key1 = BQT1.SmallA.StringKey AND table1comp.key1 = BQT1.SmallA.StringNum",  //$NON-NLS-1$
            metadata,
            null, capFinder,
            new String[] {"SELECT g_0.StringKey, g_0.StringNum, g_0.IntKey FROM BQT2.SmallA AS g_0", "SELECT g_0.StringKey, g_0.StringNum FROM BQT1.SmallA AS g_0 WHERE (g_0.StringNum = g_0.StringKey) AND (g_0.StringKey IN (<dependent values>))"}, //$NON-NLS-1$ //$NON-NLS-2$
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

    @Test public void testCostingCleanup() throws Exception {

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        caps.setCapabilitySupport(Capability.QUERY_GROUP_BY, true);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("BQT2", caps); //$NON-NLS-1$

        TransformationMetadata metadata = RealMetadataFactory.exampleBQT();
        RealMetadataFactory.setCardinality("bqt1.smalla", 1000, metadata); //$NON-NLS-1$
        RealMetadataFactory.setCardinality("bqt2.smalla", 10000, metadata); //$NON-NLS-1$
        Column fmo = metadata.getElementID("bqt1.smalla.intnum");
        fmo.setDistinctValues(1000);
        Column floatnum = metadata.getElementID("bqt1.smalla.floatnum");
        floatnum.setDistinctValues(800);

        ProcessorPlan plan = TestOptimizer.helpPlan(
            "SELECT max(a.stringkey) from bqt1.smalla a, bqt2.smalla a2, bqt1.smalla a1 where a.intnum = a2.intnum and a1.stringnum = a2.stringnum and a.floatnum = a1.floatnum",  //$NON-NLS-1$
            metadata,
            null, capFinder,
            new String[] {"SELECT g_0.StringNum, g_0.IntNum FROM BQT2.SmallA AS g_0 WHERE (g_0.StringNum IN (<dependent values>)) AND (g_0.IntNum IN (<dependent values>))", "SELECT DISTINCT g_1.StringNum AS c_0, g_0.IntNum AS c_1, g_0.StringKey AS c_2 FROM BQT1.SmallA AS g_0, BQT1.SmallA AS g_1 WHERE g_0.FloatNum = g_1.FloatNum ORDER BY c_0, c_1"},
            TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING );

        TestOptimizer.checkNodeTypes(plan, new int[] {
            1,      // Access
            1,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            1,      // Grouping
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

    @Test public void testSystemDependent() throws TeiidComponentException, TeiidProcessingException {
        // Create query
        String sql = "SELECT pm1.g1.e1 FROM pm1.g1, sys.columns makedep where pm1.g1.e1 = sys.columns.name"; //$NON-NLS-1$

        CompositeMetadataStore cms = new CompositeMetadataStore(Arrays.asList(RealMetadataFactory.example1Store(), SystemMetadata.getInstance().getSystemStore()));
        TransformationMetadata tm = new TransformationMetadata(null, cms, null, null, null);
        BasicSourceCapabilities bsc = new BasicSourceCapabilities();
        bsc.setCapabilitySupport(Capability.CRITERIA_IN, true);
        ProcessorPlan plan = TestOptimizer.helpPlan(
                sql,  //$NON-NLS-1$
                tm,
                null, new DefaultCapabilitiesFinder(bsc),
                new String[] {"SELECT pm1.g1.e1 FROM pm1.g1", "SELECT SYS.Columns.Name FROM SYS.Columns WHERE SYS.Columns.Name IN (<dependent values>)"},
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
                2,      // Project - we expect a project over the system query
                0,      // Select
                0,      // Sort
                0       // UnionAll
            });
    }

    @Test public void testPlanningOverJoin() throws Exception {
        // Create query
        String sql = "SELECT pm1.g1.e1 FROM pm1.g1, (select pm2.g2.e1, pm2.g3.e2 from pm2.g2 left outer join pm2.g3 on pm2.g2.e1 = pm2.g3.e1) as x where pm1.g1.e1 = x.e1";//$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", TestOptimizer.getTypicalCapabilities()); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1();
        RealMetadataFactory.setCardinality("pm1.g1", RuleChooseDependent.DEFAULT_INDEPENDENT_CARDINALITY, metadata);
        RealMetadataFactory.setCardinality("pm2.g2", RuleChooseDependent.DEFAULT_INDEPENDENT_CARDINALITY + 1000, metadata);
        RealMetadataFactory.setCardinality("pm2.g3", RuleChooseDependent.DEFAULT_INDEPENDENT_CARDINALITY + 1, metadata);

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, metadata,
            null, capFinder,
            new String[] { "SELECT g_0.e1 AS c_0 FROM pm2.g2 AS g_0 LEFT OUTER JOIN pm2.g3 AS g_1 ON g_0.e1 = g_1.e1 WHERE g_0.e1 IN (<dependent values>) ORDER BY c_0", "SELECT g_0.e1 FROM pm1.g1 AS g_0" }, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$
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

    /**
     * Makes sure that hints are considered after rule push aggregates
     * @throws Exception
     */
    @Test public void testMakedepWithAggregatePushdown() throws Exception {
        String sql = "select count(x.e2), pm2.g2.e1 from pm1.g1 x makedep, pm2.g2 where x.e4 = pm2.g2.e4 group by pm2.g2.e1";//$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, metadata,
            null, new DefaultCapabilitiesFinder(TestOptimizer.getTypicalCapabilities()),
            new String[] { "SELECT g_0.e4, g_0.e2 FROM pm1.g1 AS g_0 WHERE g_0.e4 IN (<dependent values>)", "SELECT g_0.e4 AS c_0, g_0.e1 AS c_1 FROM pm2.g2 AS g_0 ORDER BY c_0" }, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$
        TestOptimizer.checkNodeTypes(plan, new int[] {
            1,      // Access
            1,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            2,      // Grouping
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

    @Test public void testKeepTransitiveWithDependentJoin() throws Exception {

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan("select pm1.g1.e1 from pm1.g1, pm2.g1, pm2.g2 where pm1.g1.e1 = pm2.g1.e1 and pm1.g1.e1 = pm2.g2.e1 and pm2.g1.e2 = pm2.g2.e2 option makedep pm2.g1", RealMetadataFactory.example1Cached(), null, capFinder, //$NON-NLS-1$
            new String[] { "SELECT g_1.e1, g_0.e1 FROM pm2.g1 AS g_0, pm2.g2 AS g_1 WHERE (g_0.e2 = g_1.e2) AND (g_1.e1 = g_0.e1) AND (g_1.e1 IN (<dependent values>))", "SELECT g_0.e1 FROM pm1.g1 AS g_0" }, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$

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

    @Test public void testNestedLeftOuterJoin() throws TeiidComponentException, TeiidProcessingException {
        String sql = "select pm1.g1.e2, 'a', trim(pm1.g3.e1) from (pm1.g1 left outer join pm1.g2 on pm1.g1.e2 = pm1.g2.e2) left outer join pm1.g3 on pm1.g3.e3 = pm1.g2.e3 and pm1.g3.e4 = pm1.g1.e4"; //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1();
        RealMetadataFactory.setCardinality("pm1.g2", 6, metadata); //$NON-NLS-1$
        RealMetadataFactory.setCardinality("pm1.g1", 0, metadata); //$NON-NLS-1$
        RealMetadataFactory.setCardinality("pm1.g3", 0, metadata); //$NON-NLS-1$

        CapabilitiesFinder finder = TestOptimizer.getGenericFinder(false);

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, metadata, new String[] {
                "SELECT g_0.e3, g_0.e4, g_0.e1 FROM pm1.g3 AS g_0",
                "SELECT g_0.e2, g_0.e4 FROM pm1.g1 AS g_0",
                "SELECT g_0.e2, g_0.e3 FROM pm1.g2 AS g_0 WHERE g_0.e2 IN (<dependent values>)"}, finder, ComparisonMode.EXACT_COMMAND_STRING);

        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            1,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // Join
            2,      // MergeJoin
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });

        RelationalPlan rPlan = (RelationalPlan)plan;
        RelationalNode node = rPlan.getRootNode().getChildren()[0];
        assertTrue(node instanceof JoinNode);
        node = node.getChildren()[0];
        assertTrue(node instanceof JoinNode);
        assertEquals(JoinType.JOIN_LEFT_OUTER, ((JoinNode)node).getJoinType());
    }

    @Test public void testMakeDependentOverUnionProjectingLiterals() throws Exception {
        ProcessorPlan plan = TestOptimizer.helpPlan("select pm1.g2.e1 from pm1.g2 inner join /*+ makedep */ "
                + "(select 1 as x, 'a' as y, e1 from pm1.g1 union all select 2, 'b', e1 from pm2.g1) v on y = pm1.g2.e1 and x = pm1.g2.e2", RealMetadataFactory.example1Cached(), //$NON-NLS-1$
            new String[] { "SELECT 2 FROM pm2.g1 AS g_0", "SELECT g_0.e1, g_0.e2 FROM pm1.g2 AS g_0", "SELECT 1 FROM pm1.g1 AS g_0" }, TestOptimizer.getGenericFinder(false), TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$
        checkDependentGroups(plan, new String[] {}); //$NON-NLS-1$
    }

    @Test public void testMakeDependentWithLimit() throws Exception {
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();

        ProcessorPlan plan = TestOptimizer.helpPlan(
                "select pm1.g1.e1, pm2.g2.e2 from pm1.g1 left outer join pm2.g2 on pm1.g1.e1 = pm2.g2.e1 order by pm1.g1.e1 limit 5", //$NON-NLS-1$
                RealMetadataFactory.example1Cached(), new String[] {
                        "SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm2.g2 AS g_0 WHERE g_0.e1 IN (<dependent values>) ORDER BY c_0",
                        "SELECT g_0.e1 AS c_0 FROM pm1.g1 AS g_0 ORDER BY c_0" }, //$NON-NLS-1$
                new DefaultCapabilitiesFinder(caps),
                TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING);
        checkDependentGroups(plan, new String[] { "PM2.G2" }); //$NON-NLS-1$

        caps.setCapabilitySupport(Capability.ROW_LIMIT, true);

        plan = TestOptimizer.helpPlan(
                "select pm1.g1.e1, pm2.g2.e2 from pm1.g1 left outer join pm2.g2 on pm1.g1.e1 = pm2.g2.e1 order by pm1.g1.e1 limit 5", //$NON-NLS-1$
                RealMetadataFactory.example1Cached(), new String[] {
                        "SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm2.g2 AS g_0 WHERE g_0.e1 IN (<dependent values>) ORDER BY c_0",
                        "SELECT g_0.e1 AS c_0 FROM pm1.g1 AS g_0 ORDER BY c_0 LIMIT 5" }, //$NON-NLS-1$
                new DefaultCapabilitiesFinder(caps),
                TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING);
        checkDependentGroups(plan, new String[] { "PM2.G2" }); //$NON-NLS-1$
    }

    @Test public void testMultiAttributeWithoutRight() throws Exception {
        String sql = "select \"a.shop_key\" -- it doesn't matter what is selected here\n" +
                "from \n" +
                "    (\n" +
                "        select \"shop_key\", \"channel_key\" \n" +
                "        from \"test_dwh_pg.table_a\"\n" +
                "    ) a\n" +
                "    join \n" +
                "        (\n" +
                "            select\n" +
                "                table_b.\"shop_key\",  \n" +
                "                table_b.medium || '_' || p.procedure_result as \"channel_key\" -- mandatory: coalesce between one field from dwhtable and a result field from proc (with proc consuming adwh table field)\n" +
                "            from test_dwh_pg.\"table_b\" \n" +
                "            , table( exec p1(dummy)) p -- any field from dwh table may be consumed, one field has to be consumed\n" +
                "        ) b \n" +
                "        on \n" +
                "            \"b.shop_key\" = \"a.shop_key\" \n" +
                "            and \"b.channel_key\" = \"a.channel_key\" -- mandatory: 2 fields in on clause";

        TransformationMetadata metadata = RealMetadataFactory.fromDDL(
                "CREATE foreign TABLE table_a\n" +
                "(\n" +
                "  shop_key varchar(4000),\n" +
                "  channel_key varchar(4000)\n" +
                ") options (cardinality 1);\n" +
                "CREATE foreign TABLE table_b\n" +
                "(\n" +
                "  shop_key varchar(4000),\n" +
                "  dummy varchar(4000),\n" +
                "  medium varchar(4000)\n" +
                ");" +
                "create virtual procedure p1(in dummy string) returns (procedure_result string)\n" +
                "as begin\n" +
                "    select 'key' procedure_result;\n" +
                "end", "x", "test_dwh_pg");
        HardcodedDataManager hdm = new HardcodedDataManager();
        hdm.addData("SELECT g_0.shop_key AS c_0, g_0.channel_key AS c_1 FROM test_dwh_pg.table_a AS g_0 ORDER BY c_0, c_1", Arrays.asList("shop_key", "channel_key"));
        hdm.addData("SELECT g_0.medium, g_0.dummy, g_0.shop_key FROM test_dwh_pg.table_b AS g_0 WHERE g_0.shop_key = 'shop_key'", Arrays.asList("channel", "dummy", "shop_key"));

        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, metadata, TestOptimizer.getGenericFinder());

        TestProcessor.helpProcess(plan, TestProcessor.createCommandContext(), hdm, new List<?>[] { Arrays.asList("shop_key") });
    }

    @Test public void testMultipleMakeIndAgainstLargeCardinalities() throws Exception {
        String sql = "select bet.e1 from (pm1.g1 AS bet LEFT OUTER JOIN pm1.g2 AS bre ON bet.e1 = bre.e1) LEFT OUTER JOIN pm1.g3 AS zak ON bet.e2 = zak.e2 inner join /*+ MAKEIND */ (select e1 from pm2.g1) test on (zak.e1 = test.e1) inner join /*+ MAKEIND */ (select e2 from pm3.g1) test1 on (zak.e1 = test1.e2)";
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();

        //causes the plan structure to loose the hints
        TransformationMetadata metadata = RealMetadataFactory.example1();
        RealMetadataFactory.setCardinality("pm1.g2", 100000000, metadata);
        RealMetadataFactory.setCardinality("pm1.g1", 100000000, metadata);
        RealMetadataFactory.setCardinality("pm1.g3", 100000000, metadata);

        TestOptimizer.helpPlan(sql, metadata, null, new DefaultCapabilitiesFinder(caps), new String[] {
                "SELECT g_0.e1 AS c_0 FROM pm2.g1 AS g_0 ORDER BY c_0", "SELECT g_2.e1, g_0.e1 FROM (pm1.g1 AS g_0 LEFT OUTER JOIN pm1.g2 AS g_1 ON g_0.e1 = g_1.e1) INNER JOIN pm1.g3 AS g_2 ON g_0.e2 = g_2.e2 WHERE (g_2.e1 IN (<dependent values>)) AND (g_2.e1 IN (<dependent values>))", "SELECT g_0.e2 FROM pm3.g1 AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING);
    }

}
