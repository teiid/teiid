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
import org.teiid.query.optimizer.TestOptimizer.ComparisonMode;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.unittest.RealMetadataFactory;

public class TestWindowFunctions {

    @Test public void testViewNotRemoved() throws Exception {
    	BasicSourceCapabilities caps = getTypicalCapabilities();
    	caps.setCapabilitySupport(Capability.ELEMENTARY_OLAP, true);
    	caps.setCapabilitySupport(Capability.QUERY_FROM_INLINE_VIEWS, true);
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT y FROM (select row_number() over (order by e1) as y from pm1.g1) as x where x.y = 10", //$NON-NLS-1$
                                      RealMetadataFactory.example1Cached(), null, new DefaultCapabilitiesFinder(caps),
                                      new String[] {
                                          "SELECT v_0.c_0 FROM (SELECT ROW_NUMBER() OVER (ORDER BY g_0.e1) AS c_0 FROM pm1.g1 AS g_0) AS v_0 WHERE v_0.c_0 = 10"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    
        checkNodeTypes(plan, FULL_PUSHDOWN);                                    
    }
	
}
