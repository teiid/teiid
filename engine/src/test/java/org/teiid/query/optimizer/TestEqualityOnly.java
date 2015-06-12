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
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings("nls")
public class TestEqualityOnly {

	@Test public void testPushdown() throws Exception {
		TransformationMetadata tm = RealMetadataFactory.fromDDL("create foreign table t (x string options (searchable 'equality_only'))", "x", "y");
		//should push
		TestOptimizer.helpPlan("select x from t where x = 'a'", tm, new String[] {"SELECT g_0.x FROM y.t AS g_0 WHERE g_0.x = 'a'"});
		//should not push
		TestOptimizer.helpPlan("select x from t where x > 'b'", tm, new String[] {"SELECT g_0.x FROM y.t AS g_0"});
		TestOptimizer.helpPlan("select x from t where x like 'c%'", tm, new String[] {"SELECT g_0.x FROM y.t AS g_0"});
	}
	
}
