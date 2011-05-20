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
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.FakeCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.translator.ExecutionFactory.SupportedJoinCriteria;


public class TestJoinPushdownRestrictions {

	@Test public void testThetaRestriction() throws Exception {
		String sql = "select pm1.g1.e2, pm1.g2.e2 from pm1.g1 inner join pm1.g2 on (pm1.g1.e2 + pm1.g2.e2 = 5)"; //$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setSourceProperty(Capability.JOIN_CRITERIA_ALLOWED, SupportedJoinCriteria.THETA);
        caps.setFunctionSupport("+", true); //$NON-NLS-1$
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(),  
        		new String[] {"SELECT g_0.e2 FROM pm1.g2 AS g_0", "SELECT g_0.e2 FROM pm1.g1 AS g_0"}, capFinder, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$   
        
        caps.setSourceProperty(Capability.JOIN_CRITERIA_ALLOWED, SupportedJoinCriteria.ANY);
        
        TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(),  
        		new String[] {"SELECT g_0.e2, g_1.e2 FROM pm1.g1 AS g_0, pm1.g2 AS g_1 WHERE (g_0.e2 + g_1.e2) = 5"}, capFinder, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$  
	}
	
	@Test public void testEquiRestriction() throws Exception {
		String sql = "select pm1.g1.e2, pm1.g2.e2 from pm1.g1 inner join pm1.g2 on (pm1.g1.e2 < pm1.g2.e2)"; //$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setSourceProperty(Capability.JOIN_CRITERIA_ALLOWED, SupportedJoinCriteria.EQUI);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(),  
        		new String[] {"SELECT g_0.e2 FROM pm1.g2 AS g_0", "SELECT g_0.e2 FROM pm1.g1 AS g_0"}, capFinder, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$   
        
        caps.setSourceProperty(Capability.JOIN_CRITERIA_ALLOWED, SupportedJoinCriteria.THETA);
        
        TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(),  
        		new String[] {"SELECT g_0.e2, g_1.e2 FROM pm1.g1 AS g_0, pm1.g2 AS g_1 WHERE g_0.e2 < g_1.e2"}, capFinder, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$  
	}
	
	@Test public void testKeyRestriction() throws Exception {
		String sql = "select pm1.g1.e2, pm1.g2.e2 from pm1.g1 inner join pm1.g2 on (pm1.g1.e2 = pm1.g2.e2)"; //$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setSourceProperty(Capability.JOIN_CRITERIA_ALLOWED, SupportedJoinCriteria.KEY);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(),  
        		new String[] {"SELECT g_0.e2 AS c_0 FROM pm1.g1 AS g_0 ORDER BY c_0", "SELECT g_0.e2 AS c_0 FROM pm1.g2 AS g_0 ORDER BY c_0"}, capFinder, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$   
        
        caps.setSourceProperty(Capability.JOIN_CRITERIA_ALLOWED, SupportedJoinCriteria.EQUI);
        
        TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(),  
        		new String[] {"SELECT g_0.e2, g_1.e2 FROM pm1.g1 AS g_0, pm1.g2 AS g_1 WHERE g_0.e2 = g_1.e2"}, capFinder, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$  
	}
	
	@Test public void testKeyRestriction1() throws Exception {
		String sql = "select a.e1, b.e1 from pm4.g1 a inner join pm4.g2 b on (a.e1 = b.e1 and a.e2 = b.e2) left outer join pm4.g1 c on (c.e1 = b.e1 and c.e2 = b.e2)"; //$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
        caps.setSourceProperty(Capability.JOIN_CRITERIA_ALLOWED, SupportedJoinCriteria.KEY);
        capFinder.addCapabilities("pm4", caps); //$NON-NLS-1$

        TestOptimizer.helpPlan(sql, RealMetadataFactory.example4(),  
        		new String[] {"SELECT g_1.e1 AS c_0, g_1.e2 AS c_1, g_0.e1 AS c_2 FROM pm4.g1 AS g_0, pm4.g2 AS g_1 WHERE (g_0.e1 = g_1.e1) AND (g_0.e2 = g_1.e2) ORDER BY c_0, c_1", "SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm4.g1 AS g_0 ORDER BY c_0, c_1"}, capFinder, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$   
	}
	
	@Test public void testKeyPasses() throws Exception {
		String sql = "select a.e1, b.e1 from pm4.g1 a, pm4.g2 b where a.e1 = b.e1 and a.e2 = b.e2"; //$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setSourceProperty(Capability.JOIN_CRITERIA_ALLOWED, SupportedJoinCriteria.KEY);
        capFinder.addCapabilities("pm4", caps); //$NON-NLS-1$

        TestOptimizer.helpPlan(sql, RealMetadataFactory.example4(),  
        		new String[] {"SELECT g_0.e1, g_1.e1 FROM pm4.g1 AS g_0, pm4.g2 AS g_1 WHERE (g_0.e1 = g_1.e1) AND (g_0.e2 = g_1.e2)"}, capFinder, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$   
	}
	
	@Test public void testCrossJoinWithRestriction() throws Exception {
		String sql = "select pm1.g1.e2, pm1.g2.e2 from pm1.g1, pm1.g2"; //$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setSourceProperty(Capability.JOIN_CRITERIA_ALLOWED, SupportedJoinCriteria.THETA);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(),  
        		new String[] {"SELECT g_0.e2 FROM pm1.g2 AS g_0", "SELECT g_0.e2 FROM pm1.g1 AS g_0"}, capFinder, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$   
	}
	
	@Test public void testOuterRestriction() throws Exception {
		String sql = "select pm1.g1.e2, pm1.g2.e2 from pm1.g1 inner join pm1.g2 on (pm1.g1.e2 + pm1.g2.e2 = 5)"; //$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, false);
        caps.setSourceProperty(Capability.JOIN_CRITERIA_ALLOWED, SupportedJoinCriteria.ANY);
        caps.setFunctionSupport("+", true); //$NON-NLS-1$
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(),  
        		new String[] {"SELECT g_0.e2 FROM pm1.g2 AS g_0", "SELECT g_0.e2 FROM pm1.g1 AS g_0"}, capFinder, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$   
	}
	
	@Test public void testCriteriaRestrictionWithNonJoinCriteria() throws Exception {
		String sql = "select pm1.g1.e2, pm1.g2.e2 from pm1.g1 left outer join pm1.g2 on (pm1.g1.e2 = pm1.g2.e2 and pm1.g2.e1 = 'hello')"; //$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, true);
        caps.setSourceProperty(Capability.JOIN_CRITERIA_ALLOWED, SupportedJoinCriteria.THETA);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(),  
        		new String[] {"SELECT g_0.e2 AS c_0 FROM pm1.g1 AS g_0 ORDER BY c_0", "SELECT g_0.e2 AS c_0 FROM pm1.g2 AS g_0 WHERE g_0.e1 = 'hello' ORDER BY c_0"}, capFinder, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$   
	}
	
}
