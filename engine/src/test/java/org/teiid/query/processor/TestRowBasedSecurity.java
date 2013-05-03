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

package org.teiid.query.processor;

import static org.teiid.query.processor.TestProcessor.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.teiid.adminapi.DataPolicy;
import org.teiid.adminapi.impl.DataPolicyMetadata;
import org.teiid.adminapi.impl.DataPolicyMetadata.PermissionMetaData;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.api.exception.query.QueryProcessingException;
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.util.CommandContext;

@SuppressWarnings("nls")
public class TestRowBasedSecurity {

	CommandContext context;
	
	@Before public void setup() {
		context = createContext();
	}

	private static CommandContext createContext() {
		CommandContext context = createCommandContext();
		DQPWorkContext workContext = new DQPWorkContext();
		HashMap<String, DataPolicy> policies = new HashMap<String, DataPolicy>();
		DataPolicyMetadata policy = new DataPolicyMetadata();
		PermissionMetaData pmd = new PermissionMetaData();
		pmd.setResourceName("pm1.g1");
		pmd.setCondition("e1 = user()");

		PermissionMetaData pmd1 = new PermissionMetaData();
		pmd1.setResourceName("pm1.g2");
		pmd1.setCondition("foo = bar");
		
		PermissionMetaData pmd2 = new PermissionMetaData();
		pmd2.setResourceName("pm1.g4");
		pmd2.setCondition("e1 = max(e2)");
		
		PermissionMetaData pmd3 = new PermissionMetaData();
		pmd3.setResourceName("pm1.g3");
		pmd3.setAllowDelete(true);
		
		PermissionMetaData pmd4 = new PermissionMetaData();
		pmd4.setResourceName("pm1.sp1");
		pmd4.setCondition("e1 = 'a'");
		
		policy.addPermission(pmd, pmd1, pmd2, pmd3, pmd4);
		policy.setName("some-role");
		policies.put("some-role", policy);
		
		workContext.setPolicies(policies);
		context.setDQPWorkContext(workContext);
		return context;
	}

	@Test public void testSelectFilter() throws Exception {
		HardcodedDataManager dataManager = new HardcodedDataManager();
		dataManager.addData("SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1", new List<?>[] {Arrays.asList("a", 1), Arrays.asList("b", 2)});
		ProcessorPlan plan = helpGetPlan(helpParse("select e2 from pm1.g1"), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(), context);
		List<?>[] expectedResults = new List<?>[0]; 
		helpProcess(plan, context, dataManager, expectedResults);
	}
	
	/**
	 * Same as above, but ensures it's still in effect under a proceudre
	 */
	@Test public void testTransitiveFilter() throws Exception {
		HardcodedDataManager dataManager = new HardcodedDataManager();
		dataManager.addData("SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1", new List<?>[] {Arrays.asList("a", 1), Arrays.asList("b", 2)});
		dataManager.addData("exec pm1.sq1()", new List<?>[] {Arrays.asList("a", 1), Arrays.asList("b", 2)});
		ProcessorPlan plan = helpGetPlan(helpParse("exec pm1.sq1()"), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(), context);
		List<?>[] expectedResults = new List<?>[0]; 
		helpProcess(plan, context, dataManager, expectedResults);
	}

	/**
	 * restricted to e1 = a
	 */
	@Test public void testProcedureFilter() throws Exception {
		HardcodedDataManager dataManager = new HardcodedDataManager();
		dataManager.addData("EXEC pm1.sp1()", new List<?>[] {Arrays.asList("a", 1), Arrays.asList("b", 2)});
		ProcessorPlan plan = helpGetPlan(helpParse("exec pm1.sp1()"), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(), context);
		List<?>[] expectedResults = new List<?>[] {Arrays.asList("a", 1)}; 
		helpProcess(plan, context, dataManager, expectedResults);
	}
	
	@Test public void testProcedureRelationalFilter() throws Exception {
		HardcodedDataManager dataManager = new HardcodedDataManager();
		dataManager.addData("EXEC pm1.sp1()", new List<?>[] {Arrays.asList("a", 1), Arrays.asList("b", 2)});
		ProcessorPlan plan = helpGetPlan(helpParse("select * from pm1.sp1"), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(), context);
		List<?>[] expectedResults = new List<?>[] {Arrays.asList("a", 1)}; 
		helpProcess(plan, context, dataManager, expectedResults);
	}
	
	/**
	 * Shouldn't even execute as 'user' <> 'a'
	 */
	@Test public void testDeleteFilter() throws Exception {
		HardcodedDataManager dataManager = new HardcodedDataManager();
		ProcessorPlan plan = helpGetPlan(helpParse("delete from pm1.g1 where e1 = 'a'"), RealMetadataFactory.example1Cached(), TestOptimizer.getGenericFinder(), context);
		List<?>[] expectedResults = new List<?>[] {Arrays.asList(0)}; 
		helpProcess(plan, context, dataManager, expectedResults);
	}
	
	/**
	 * invalid insert value
	 */
	@Test(expected=QueryPlannerException.class) public void testInsertConstraint() throws Exception {
		HardcodedDataManager dataManager = new HardcodedDataManager();
		ProcessorPlan plan = helpGetPlan(helpParse("insert into pm1.g1 (e1) values ('a')"), RealMetadataFactory.example1Cached(), TestOptimizer.getGenericFinder(), context);
		List<?>[] expectedResults = new List<?>[] {Arrays.asList(0)}; 
		helpProcess(plan, context, dataManager, expectedResults);
	}
	
	/**
	 * should fail since it doesn't match the condition
	 */
	@Test(expected=QueryProcessingException.class) public void testInsertConstraintWithQueryExpression() throws Exception {
		HardcodedDataManager dataManager = new HardcodedDataManager();
		dataManager.addData("SELECT pm1.g3.e1 FROM pm1.g3", new List<?>[] {Arrays.asList("a"), Arrays.asList("b")});
		BasicSourceCapabilities bsc = new BasicSourceCapabilities();
		bsc.setCapabilitySupport(Capability.INSERT_WITH_QUERYEXPRESSION, true);
		DefaultCapabilitiesFinder capFinder = new DefaultCapabilitiesFinder(bsc);
		ProcessorPlan plan = helpGetPlan(helpParse("insert into pm1.g1 (e1) select e1 from pm1.g3"), RealMetadataFactory.example1Cached(), capFinder, context);
		List<?>[] expectedResults = new List<?>[] {Arrays.asList(0)}; 
		helpProcess(plan, context, dataManager, expectedResults);
	}
	
	/**
	 * should succeed since it matches the condition
	 */
	@Test public void testInsertConstraintWithQueryExpression1() throws Exception {
		HardcodedDataManager dataManager = new HardcodedDataManager();
		dataManager.addData("SELECT pm1.g3.e1 FROM pm1.g3", new List<?>[] {Arrays.asList("a")});
		dataManager.addData("INSERT INTO pm1.g1 (e1) VALUES ('user')", new List<?>[] {Arrays.asList(1)});
		BasicSourceCapabilities bsc = new BasicSourceCapabilities();
		bsc.setCapabilitySupport(Capability.INSERT_WITH_QUERYEXPRESSION, true);
		DefaultCapabilitiesFinder capFinder = new DefaultCapabilitiesFinder(bsc);
		ProcessorPlan plan = helpGetPlan(helpParse("insert into pm1.g1 (e1) select user() from pm1.g3"), RealMetadataFactory.example1Cached(), capFinder, context);
		List<?>[] expectedResults = new List<?>[] {Arrays.asList(1)}; 
		helpProcess(plan, context, dataManager, expectedResults);
	}
	
	@Test public void testInsertFilter1() throws Exception {
		HardcodedDataManager dataManager = new HardcodedDataManager();
		dataManager.addData("INSERT INTO pm1.g1 (e1) VALUES ('user')", new List<?>[] {Arrays.asList(1)});
		ProcessorPlan plan = helpGetPlan(helpParse("insert into pm1.g1 (e1) values ('user')"), RealMetadataFactory.example1Cached(), TestOptimizer.getGenericFinder(), context);
		List<?>[] expectedResults = new List<?>[] {Arrays.asList(1)}; 
		helpProcess(plan, context, dataManager, expectedResults);
	}
	
	/**
	 * not a valid value for e1
	 */
	@Test(expected=QueryPlannerException.class) public void testUpdateFilter() throws Exception {
		HardcodedDataManager dataManager = new HardcodedDataManager();
		ProcessorPlan plan = helpGetPlan(helpParse("update pm1.g1 set e1 = 'a' where e2 = 5"), RealMetadataFactory.example1Cached(), TestOptimizer.getGenericFinder(), context);
		List<?>[] expectedResults = new List<?>[] {Arrays.asList(0)}; 
		helpProcess(plan, context, dataManager, expectedResults);
	}
	
	/**
	 * no primary key for compensation
	 */
	@Test(expected=QueryPlannerException.class) public void testUpdateFilter1() throws Exception {
		HardcodedDataManager dataManager = new HardcodedDataManager();
		ProcessorPlan plan = helpGetPlan(helpParse("update pm1.g1 set e1 = e3 where e2 = 5"), RealMetadataFactory.example1Cached(), TestOptimizer.getGenericFinder(), context);
		List<?>[] expectedResults = new List<?>[] {Arrays.asList(0)}; 
		helpProcess(plan, context, dataManager, expectedResults);
	}
	
	@Test(expected=QueryProcessingException.class) public void testUpdateFilter2() throws Exception {
		HardcodedDataManager dataManager = new HardcodedDataManager();
		dataManager.addData("SELECT g_0.e3, g_0.e1 FROM pm1.g1 AS g_0 WHERE (g_0.e1 = 'user') AND (g_0.e2 = 5)", new List<?>[] {Arrays.asList(Boolean.TRUE, "user")});
		ProcessorPlan plan = helpGetPlan(helpParse("update pm1.g1 set e1 = e3 || 'r' where e2 = 5"), RealMetadataFactory.example4(), TestOptimizer.getGenericFinder(), context);
		List<?>[] expectedResults = new List<?>[] {Arrays.asList(0)}; 
		helpProcess(plan, context, dataManager, expectedResults);
	}
	
	//TODO: should add validation prior to queries being run
	@Test(expected=QueryMetadataException.class) public void testBadFilter() throws Exception {
		HardcodedDataManager dataManager = new HardcodedDataManager();
		ProcessorPlan plan = helpGetPlan(helpParse("select e2 from pm1.g2"), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(), context);
		List<?>[] expectedResults = new List<?>[0]; 
		helpProcess(plan, context, dataManager, expectedResults);
	}

	@Test(expected=QueryMetadataException.class) public void testBadFilter1() throws Exception {
		HardcodedDataManager dataManager = new HardcodedDataManager();
		ProcessorPlan plan = helpGetPlan(helpParse("select * from pm1.g4"), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(), context);
		List<?>[] expectedResults = new List<?>[0]; 
		helpProcess(plan, context, dataManager, expectedResults);
	}
	
	/**
	 * Here the other role makes the g1 rows visible again
	 */
	@Test public void testMultipleRoles() throws Exception {
		HardcodedDataManager dataManager = new HardcodedDataManager();
		dataManager.addData("SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1", new List<?>[] {Arrays.asList("a", 1), Arrays.asList("b", 2)});
		ProcessorPlan plan = helpGetPlan(helpParse("select e2 from pm1.g1"), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(), context);
		helpProcess(plan, context, dataManager, new List<?>[0]);
		
		DataPolicyMetadata policy1 = new DataPolicyMetadata();
		PermissionMetaData pmd3 = new PermissionMetaData();
		pmd3.setResourceName("pm1.g1");
		pmd3.setCondition("true");
		policy1.addPermission(pmd3);
		policy1.setName("some-other-role");
		context.getAllowedDataPolicies().put("some-other-role", policy1);

		dataManager = new HardcodedDataManager();
		dataManager.addData("SELECT pm1.g1.e2 FROM pm1.g1", new List<?>[] {Arrays.asList(1), Arrays.asList(2)});
		plan = helpGetPlan(helpParse("select e2 from pm1.g1"), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(), context);
		List<?>[] expectedResults = new List<?>[] {Arrays.asList(1), Arrays.asList(2)}; 
		helpProcess(plan, context, dataManager, expectedResults);
	}

}
