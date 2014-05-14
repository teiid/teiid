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

import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.metadata.Table;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.TestOptimizer.ComparisonMode;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.optimizer.relational.rules.RulePlaceAccess;
import org.teiid.query.processor.relational.AccessNode;
import org.teiid.query.processor.relational.RelationalPlan;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings("nls")
public class TestConformedTables {
	
	private static TransformationMetadata tm;
	
	@BeforeClass public static void oneTimeSetup() throws Exception {
		tm = RealMetadataFactory.example1();
		Table t = tm.getGroupID("pm1.g1");
		t.setProperty(RulePlaceAccess.CONFORMED_SOURCES, "pm2");
		t = tm.getGroupID("pm2.g3");
		t.setProperty(RulePlaceAccess.CONFORMED_SOURCES, "pm1");
		t = tm.getGroupID("pm2.g1");
		t.setProperty(RulePlaceAccess.CONFORMED_SOURCES, "pm3");
	}
	
	@Test public void testConformedJoin() throws Exception {
		String sql = "select pm1.g1.e1 from pm1.g1, pm2.g2 where g1.e1=g2.e1";
		
		RelationalPlan plan = (RelationalPlan)helpPlan(sql, tm, new String[] {"SELECT g_0.e1 FROM pm1.g1 AS g_0, pm2.g2 AS g_1 WHERE g_0.e1 = g_1.e1"}, ComparisonMode.EXACT_COMMAND_STRING);
		AccessNode anode = (AccessNode) plan.getRootNode();
		assertEquals("pm2", anode.getModelName());
		
		//it should work either way
		sql = "select pm1.g1.e1 from pm2.g2, pm1.g1 where g1.e1=g2.e1";
		
		plan = (RelationalPlan)helpPlan(sql, tm, new String[] {"SELECT g_1.e1 FROM pm2.g2 AS g_0, pm1.g1 AS g_1 WHERE g_1.e1 = g_0.e1"}, ComparisonMode.EXACT_COMMAND_STRING);
		anode = (AccessNode) plan.getRootNode();
		assertEquals("pm2", anode.getModelName());
	}
	
	@Test public void testConformedJoin1() throws Exception {
		String sql = "select pm1.g1.e1 from pm1.g1, pm2.g1 where pm1.g1.e1=pm2.g1.e1";
		
		helpPlan(sql, tm, new String[] {"SELECT g_0.e1 FROM pm1.g1 AS g_0, pm2.g1 AS g_1 WHERE g_0.e1 = g_1.e1"}, ComparisonMode.EXACT_COMMAND_STRING);
	}
	
	@Test public void testConformedSubquery() throws Exception {
		String sql = "select pm2.g2.e1 from pm2.g2 where e1 in (select e1 from pm1.g1)";
		
		BasicSourceCapabilities bsc = getTypicalCapabilities();
		bsc.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);
		
		helpPlan(sql, tm, new String[] {"SELECT g_0.e1 FROM pm2.g2 AS g_0 WHERE g_0.e1 IN (SELECT g_1.e1 FROM pm1.g1 AS g_1)"}, new DefaultCapabilitiesFinder(bsc), ComparisonMode.EXACT_COMMAND_STRING);
		
		//TODO: it should work either way, but for now we expect the subquery to conform to the parent
		sql = "select pm1.g1.e1 from pm1.g1 where e2 in (select e2 from pm2.g2)";
	}
	
	@Test public void testConformedSubquery1() throws Exception {
		String sql = "select pm2.g3.e1 from pm2.g3 where e1 in (select e1 from pm1.g1)";
		
		BasicSourceCapabilities bsc = getTypicalCapabilities();
		bsc.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);
		
		helpPlan(sql, tm, new String[] {"SELECT g_0.e1 FROM pm2.g3 AS g_0 WHERE g_0.e1 IN (SELECT g_1.e1 FROM pm1.g1 AS g_1)"}, new DefaultCapabilitiesFinder(bsc), ComparisonMode.EXACT_COMMAND_STRING);
	}
	
}
