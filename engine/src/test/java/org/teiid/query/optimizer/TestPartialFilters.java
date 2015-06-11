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
import org.teiid.query.optimizer.TestOptimizer.ComparisonMode;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.processor.HardcodedDataManager;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.TestProcessor;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings("nls")
public class TestPartialFilters {

	@Test public void testFilterPlanning() throws Exception {
		String ddl = "create foreign table People_Groups (user_dn string options (nameinsource 'dn'), group_dn string options (nameinsource 'memberOf', \"teiid_rel:partial_filter\" true)) options (nameinsource 'ou=people,dc=metamatrix,dc=com')";
		BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
		caps.setCapabilitySupport(Capability.PARTIAL_FILTERS, true);
		ProcessorPlan plan = TestOptimizer.helpPlan("select * from people_groups where group_dn = 'a'", RealMetadataFactory.fromDDL(ddl, "x", "y"), new String[] {"SELECT g_0.user_dn, g_0.group_dn FROM y.People_Groups AS g_0 WHERE g_0.group_dn = 'a'"}, new DefaultCapabilitiesFinder(caps), ComparisonMode.EXACT_COMMAND_STRING);
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
                1,      // Select
                0,      // Sort
                0       // UnionAll
            });
		HardcodedDataManager dataManager = new HardcodedDataManager();
		dataManager.addData("SELECT g_0.user_dn, g_0.group_dn FROM y.People_Groups AS g_0 WHERE g_0.group_dn = 'a'", Arrays.asList("b", "a"), Arrays.asList("b", "c"));
		TestProcessor.helpProcess(plan, dataManager, new List<?>[] {Arrays.asList("b", "a")});
	}
	
	@Test public void testFilterPlanning1() throws Exception {
		String ddl = "create foreign table People_Groups (user_dn string options (nameinsource 'dn'), group_dn string options (nameinsource 'memberOf', \"teiid_rel:partial_filter\" true)) options (nameinsource 'ou=people,dc=metamatrix,dc=com')";
		BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
		caps.setCapabilitySupport(Capability.PARTIAL_FILTERS, true);
		ProcessorPlan plan = TestOptimizer.helpPlan("select * from people_groups where group_dn > 'abc' and group_dn like 'a_'", RealMetadataFactory.fromDDL(ddl, "x", "y"), new String[] {"SELECT g_0.user_dn, g_0.group_dn FROM y.People_Groups AS g_0 WHERE (g_0.group_dn > 'abc') AND (g_0.group_dn LIKE 'a_')"}, new DefaultCapabilitiesFinder(caps), ComparisonMode.EXACT_COMMAND_STRING);
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
                1,      // Select
                0,      // Sort
                0       // UnionAll
            });
		HardcodedDataManager dataManager = new HardcodedDataManager();
		dataManager.addData("SELECT g_0.user_dn, g_0.group_dn FROM y.People_Groups AS g_0 WHERE (g_0.group_dn > 'abc') AND (g_0.group_dn LIKE 'a_')", Arrays.asList("a", "aa"), Arrays.asList("a", "ac"), Arrays.asList("a", "e"));
		TestProcessor.helpProcess(plan, dataManager, new List<?>[] {Arrays.asList("a", "ac")});
	}
	
	@Test public void testFilterPlanning2() throws Exception {
		String ddl = "create foreign table People_Groups (user_dn string options (nameinsource 'dn'), group_dn string options (nameinsource 'memberOf', \"teiid_rel:partial_filter\" true)) options (nameinsource 'ou=people,dc=metamatrix,dc=com')";
		BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
		caps.setCapabilitySupport(Capability.PARTIAL_FILTERS, true);
		ProcessorPlan plan = TestOptimizer.helpPlan("select group_dn from people_groups where group_dn > 'abc' or user_dn < 'def'", RealMetadataFactory.fromDDL(ddl, "x", "y"), new String[] {"SELECT g_0.group_dn, g_0.user_dn FROM y.People_Groups AS g_0 WHERE (g_0.group_dn > 'abc') OR (g_0.user_dn < 'def')"}, new DefaultCapabilitiesFinder(caps), ComparisonMode.EXACT_COMMAND_STRING);
		HardcodedDataManager dataManager = new HardcodedDataManager();
		dataManager.addData("SELECT g_0.group_dn, g_0.user_dn FROM y.People_Groups AS g_0 WHERE (g_0.group_dn > 'abc') OR (g_0.user_dn < 'def')", Arrays.asList("a", "b"), Arrays.asList("d", "e"), Arrays.asList("a", "e"));
		TestProcessor.helpProcess(plan, dataManager, new List<?>[] {Arrays.asList("a"), Arrays.asList("d")});
	}
	
	@Test public void testFilterPlanningFullyPushed() throws Exception {
		String ddl = "create foreign table People_Groups (user_dn string options (nameinsource 'dn'), group_dn string options (nameinsource 'memberOf', \"teiid_rel:partial_filter\" true)) options (nameinsource 'ou=people,dc=metamatrix,dc=com')";
		BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
		caps.setCapabilitySupport(Capability.PARTIAL_FILTERS, true);
		ProcessorPlan plan = TestOptimizer.helpPlan("select user_dn from people_groups where group_dn = 'a'", RealMetadataFactory.fromDDL(ddl, "x", "y"), new String[] {"SELECT g_0.user_dn FROM y.People_Groups AS g_0 WHERE g_0.group_dn = 'a'"}, new DefaultCapabilitiesFinder(caps), ComparisonMode.EXACT_COMMAND_STRING);
		TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
	}
	
	@Test public void testFilterPlanningJoin() throws Exception {
		String ddl = "create foreign table People (UserID string options (nameinsource 'uid'), Name string options (nameinsource 'cn'), dn string, vals string[]) options (nameinsource 'ou=people,dc=metamatrix,dc=com')"
				+ "create foreign table People_Groups (user_dn string options (nameinsource 'dn'), group_dn string options (nameinsource 'memberOf', \"teiid_rel:partial_filter\" true)) options (nameinsource 'ou=people,dc=metamatrix,dc=com')";
		BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
		caps.setCapabilitySupport(Capability.PARTIAL_FILTERS, true);
		ProcessorPlan plan = TestOptimizer.helpPlan("select user_dn from people, people_groups where user_dn = dn", RealMetadataFactory.fromDDL(ddl, "x", "y"), new String[] {"SELECT g_1.user_dn FROM y.People AS g_0, y.People_Groups AS g_1 WHERE g_1.user_dn = g_0.dn"}, new DefaultCapabilitiesFinder(caps), ComparisonMode.EXACT_COMMAND_STRING);
		TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
	}
	
}
