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

package com.metamatrix.server.integration;

import java.sql.Connection;
import java.util.Collection;

import org.junit.Test;
import org.teiid.adminapi.Admin;
import org.teiid.adminapi.ProcessObject;

import static org.junit.Assert.*;

import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.jdbc.MMConnection;
import com.metamatrix.jdbc.api.AbstractMMQueryTestCase;

public class TestAdminApi extends AbstractMMQueryTestCase {
	
	private static final String DQP_PROP_FILE = UnitTestUtil.getTestDataPath() + "/authcheck/bqt.properties;"; //$NON-NLS-1$
    private static final String VDB = "bqt"; //$NON-NLS-1$

    @Test public void testGetProcess() throws Exception {
		Connection conn = getConnection(VDB, DQP_PROP_FILE, "user=admin;password=teiid;"); //$NON-NLS-1$
		Admin admin = ((MMConnection)conn).getAdminAPI();
		Collection<ProcessObject> processes = admin.getProcesses("*"); //$NON-NLS-1$
		assertEquals(1, processes.size()); 
		assertNotNull(processes.iterator().next().getInetAddress());
    }

}
