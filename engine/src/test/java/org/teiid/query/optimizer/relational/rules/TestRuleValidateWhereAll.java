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

package org.teiid.query.optimizer.relational.rules;

import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.optimizer.TestOptimizer.ComparisonMode;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.FakeCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.optimizer.relational.rules.RuleValidateWhereAll;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.unittest.RealMetadataFactory;

import junit.framework.TestCase;


public class TestRuleValidateWhereAll extends TestCase {
    
    public TestRuleValidateWhereAll(String name) {
        super(name);
    }

    public void testHasNoCriteria1() {
        assertEquals("Got incorrect answer checking for no criteria", false, RuleValidateWhereAll.hasNoCriteria(new Insert())); //$NON-NLS-1$
    }

    public void testHasNoCriteria2() {
        Query query = new Query();
        CompareCriteria crit = new CompareCriteria(new Constant("a"), CompareCriteria.EQ, new Constant("b")); //$NON-NLS-1$ //$NON-NLS-2$
        query.setCriteria(crit);        
        assertEquals("Got incorrect answer checking for no criteria", false, RuleValidateWhereAll.hasNoCriteria(query)); //$NON-NLS-1$
    }

    public void testHasNoCriteria3() {
        assertEquals("Got incorrect answer checking for no criteria", true, RuleValidateWhereAll.hasNoCriteria(new Query())); //$NON-NLS-1$
    }
    
	private FakeCapabilitiesFinder getWhereAllCapabilities() {
		FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.REQUIRES_CRITERIA, true);
        capFinder.addCapabilities("pm1", TestOptimizer.getTypicalCapabilities()); //$NON-NLS-1$
        capFinder.addCapabilities("pm6", caps); //$NON-NLS-1$
		return capFinder;
	}    
	
    public void testDefect21982_3() {
        TestOptimizer.helpPlan(
                 "SELECT * FROM vm1.g38",   //$NON-NLS-1$
                 RealMetadataFactory.example1Cached(),
                 null, getWhereAllCapabilities(),
                 new String[0],
                 false);       
    }
    
    public void testWhereAll1() {
    	TestOptimizer.helpPlan(
            "SELECT * FROM pm6.g1",   //$NON-NLS-1$
            RealMetadataFactory.example1Cached(),
            null, getWhereAllCapabilities(),
            new String[0],
            false);
    }    

    public void testWhereAll2() throws Exception {
    	TestOptimizer.helpPlan(
            "SELECT pm1.g1.e1 FROM pm1.g1, pm6.g1 WHERE pm1.g1.e1=pm6.g1.e1 OPTION MAKEDEP pm6.g1",   //$NON-NLS-1$
            RealMetadataFactory.example1Cached(),
            null, getWhereAllCapabilities(),
            new String[] {
                "SELECT g_0.e1 AS c_0 FROM pm6.g1 AS g_0 WHERE g_0.e1 IN (<dependent values>) ORDER BY c_0", "SELECT g_0.e1 AS c_0 FROM pm1.g1 AS g_0 ORDER BY c_0" //$NON-NLS-1$ //$NON-NLS-2$
            },
            ComparisonMode.EXACT_COMMAND_STRING);
    }    

}
