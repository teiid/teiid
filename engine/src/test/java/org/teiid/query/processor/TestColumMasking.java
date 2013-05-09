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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.teiid.adminapi.DataPolicy;
import org.teiid.adminapi.impl.DataPolicyMetadata;
import org.teiid.adminapi.impl.DataPolicyMetadata.PermissionMetaData;
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.util.CommandContext;

@SuppressWarnings("nls")
public class TestColumMasking {

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
		pmd.setResourceName("pm1.sp1.rs3.e1");
		pmd.setMask("case when e2 > 1 then null else e1 end");

		PermissionMetaData pmd1 = new PermissionMetaData();
		pmd1.setResourceName("pm1.g1.e2");
		pmd1.setMask("case when e1 = 'a' then null else e2 end");
		
		policy.addPermission(pmd, pmd1);
		policy.setName("some-role");
		policies.put("some-role", policy);

		DataPolicyMetadata policy1 = new DataPolicyMetadata();
		PermissionMetaData pmd11 = new PermissionMetaData();
		pmd11.setResourceName("pm1.sp1.rs3.e1");
		pmd11.setOrder(1); //ensure that pmd above still applies
		pmd11.setMask("null");

		policy1.addPermission(pmd, pmd1);
		policy1.setName("other-role");
		policies.put("other-role", policy);
		
		workContext.setPolicies(policies);
		context.setDQPWorkContext(workContext);
		return context;
	}

	@Test public void testProcedureMask() throws Exception {
		HardcodedDataManager dataManager = new HardcodedDataManager();
		dataManager.addData("EXEC pm1.sp1()", new List<?>[] {Arrays.asList("a", 1), Arrays.asList("b", 2)});
		ProcessorPlan plan = helpGetPlan(helpParse("exec pm1.sp1()"), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(), context);
		List<?>[] expectedResults = new List<?>[] {Arrays.asList("a", 1), Arrays.asList(null, 2)}; 
		helpProcess(plan, context, dataManager, expectedResults);
	}
	
	@Test public void testTableMask() throws Exception {
		HardcodedDataManager dataManager = new HardcodedDataManager();
		dataManager.addData("SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1", new List<?>[] {Arrays.asList("a", 1), Arrays.asList("b", 2)});
		ProcessorPlan plan = helpGetPlan(helpParse("select e2 from pm1.g1"), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(), context);
		List<?>[] expectedResults = new List<?>[] {Collections.singletonList(null), Arrays.asList(2)};
		helpProcess(plan, context, dataManager, expectedResults);
	}
	
	@Test public void testTableAliasMask() throws Exception {
		HardcodedDataManager dataManager = new HardcodedDataManager();
		dataManager.addData("SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1", new List<?>[] {Arrays.asList("a", 1), Arrays.asList("b", 2)});
		ProcessorPlan plan = helpGetPlan(helpParse("select g2.e2 from pm1.g1 as g2"), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(), context);
		List<?>[] expectedResults = new List<?>[] {Collections.singletonList(null), Arrays.asList(2)};
		helpProcess(plan, context, dataManager, expectedResults);
	}

}
