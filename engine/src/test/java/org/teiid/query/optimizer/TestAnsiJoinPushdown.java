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
import org.teiid.query.optimizer.TestOptimizer.ComparisonMode;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.FakeCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.unittest.RealMetadataFactory;


public class TestAnsiJoinPushdown {

	/**
	 * See {@link TestOptimizer.testPushMultiGroupCriteria}
	 * 
	 * Notice that the non-join criteria is still in the on clause.
	 */
    @Test public void testAnsiInnerJoin() throws Exception { 
    	FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_ANSI_JOIN, true);
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$
    	
        ProcessorPlan plan = TestOptimizer.helpPlan(
        		"select pm2.g1.e1 from pm2.g1, pm2.g2 where pm2.g1.e1 = pm2.g2.e1 and (pm2.g1.e2 = 1 OR pm2.g2.e2 = 2) and pm2.g2.e3 = 1", //$NON-NLS-1$ 
        		RealMetadataFactory.example1Cached(), 
        		null,
        		capFinder,
        		new String[] { "SELECT g_0.e1 FROM pm2.g1 AS g_0 INNER JOIN pm2.g2 AS g_1 ON g_0.e1 = g_1.e1 AND ((g_0.e2 = 1) OR (g_1.e2 = 2)) WHERE g_1.e3 = TRUE" }, //$NON-NLS-1$
        		ComparisonMode.EXACT_COMMAND_STRING); 
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN); 
    }  
	
}
