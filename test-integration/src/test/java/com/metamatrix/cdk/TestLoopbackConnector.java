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

package com.metamatrix.cdk;

import org.junit.Test;

import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.jdbc.api.AbstractMMQueryTestCase;

public class TestLoopbackConnector extends AbstractMMQueryTestCase {
	
	private static final String DQP_PROP_FILE = UnitTestUtil.getTestDataPath() + "/partssupplier/dqp.properties;user=test"; //$NON-NLS-1$
    private static final String VDB = "PartsSupplier"; //$NON-NLS-1$
        
    @Test public void test() {
    	getConnection(VDB, DQP_PROP_FILE);
    	
    	executeAndAssertResults("select * from parts", new String[] { //$NON-NLS-1$
    			"PART_ID[string]    PART_NAME[string]    PART_COLOR[string]    PART_WEIGHT[string]", //$NON-NLS-1$
    			"ABCDEFGHIJ    ABCDEFGHIJ    ABCDEFGHIJ    ABCDEFGHIJ"//$NON-NLS-1$
    	});
    	closeConnection();
    }
}
