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

import static org.teiid.query.optimizer.TestOptimizer.*;

import org.junit.Test;
import org.teiid.metadata.Table;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.TestOptimizer.ComparisonMode;
import org.teiid.query.optimizer.relational.rules.RulePlaceAccess;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings("nls")
public class TestConformedTables {
	
	@Test public void testConformedJoin() throws Exception {
		TransformationMetadata tm = RealMetadataFactory.example1();
		Table t = tm.getGroupID("pm1.g1");
		t.setProperty(RulePlaceAccess.CONFORMED_SOURCES, "pm2");
		
		String sql = "select pm1.g1.e1 from pm1.g1, pm2.g2 where g1.e1=g2.e1";
		
		helpPlan(sql, tm, new String[] {"SELECT g_0.e1 FROM pm1.g1 AS g_0, pm2.g2 AS g_1 WHERE g_0.e1 = g_1.e1"}, ComparisonMode.EXACT_COMMAND_STRING);
		
		//it should work either way
		sql = "select pm1.g1.e1 from pm1.g2, pm1.g1 where g1.e1=g2.e1";
		
		helpPlan(sql, tm, new String[] {"SELECT g_1.e1 FROM pm1.g2 AS g_0, pm1.g1 AS g_1 WHERE g_1.e1 = g_0.e1"}, ComparisonMode.EXACT_COMMAND_STRING);
	}
	
	
}
