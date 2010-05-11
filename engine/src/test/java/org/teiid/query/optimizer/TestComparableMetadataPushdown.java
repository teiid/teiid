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
import org.teiid.query.validator.TestValidator;


public class TestComparableMetadataPushdown {
	
	@Test public void testCantPushSort() throws Exception {
		String sql = "select e3, e2 from test.group order by e3, e2"; //$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        capFinder.addCapabilities("test", TestOptimizer.getTypicalCapabilities()); //$NON-NLS-1$

        TestOptimizer.helpPlan(sql, TestValidator.exampleMetadata(),  
        		new String[] {"SELECT g_0.e3, g_0.e2 FROM test.\"group\" AS g_0"}, capFinder, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    }
	
	@Test public void testCantPushGroupBy() throws Exception {
		String sql = "select e3, e2 from test.group group by e3, e2"; //$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_GROUP_BY, true);
        capFinder.addCapabilities("test", caps); //$NON-NLS-1$

        TestOptimizer.helpPlan(sql, TestValidator.exampleMetadata(),  
        		new String[] {"SELECT g_0.e3, g_0.e2 FROM test.\"group\" AS g_0"}, capFinder, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$  
	}
	
	@Test public void testCantPushDup() throws Exception {
		String sql = "select distinct e3, e2 from test.group"; //$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        capFinder.addCapabilities("test", TestOptimizer.getTypicalCapabilities()); //$NON-NLS-1$

        TestOptimizer.helpPlan(sql, TestValidator.exampleMetadata(),  
        		new String[] {"SELECT g_0.e3, g_0.e2 FROM test.\"group\" AS g_0"}, capFinder, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$  
	}
	
	@Test public void testCantPushSetOp() throws Exception {
		String sql = "select e3, e2 from test.group union select e0, e1 from test.group2"; //$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_UNION, true);
        capFinder.addCapabilities("test", caps); //$NON-NLS-1$

        TestOptimizer.helpPlan(sql, TestValidator.exampleMetadata(),  
        		new String[] {"SELECT test.\"group\".e3, test.\"group\".e2 FROM test.\"group\"", "SELECT test.group2.e0, test.group2.e1 FROM test.group2"}, capFinder, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$  
	}

}
