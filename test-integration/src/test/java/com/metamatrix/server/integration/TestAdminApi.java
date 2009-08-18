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
