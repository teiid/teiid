package com.metamatrix.server.integration;

import java.sql.Connection;

import junit.framework.TestCase;

import org.junit.Test;

import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.jdbc.api.AbstractMMQueryTestCase;


public class TestEmbeddedShutdown extends AbstractMMQueryTestCase {

	private static final String DQP_PROP_FILE = UnitTestUtil.getTestDataPath() + "/authcheck/bqt.properties;"; //$NON-NLS-1$
    private static final String VDB = "bqt"; //$NON-NLS-1$


    @Test public void testShutdown() {
    	try {
			Connection conn = getConnection(VDB, DQP_PROP_FILE, "user=admin;password=teiid;"); //$NON-NLS-1$
			execute("select intkey, stringkey from BQT1.smalla"); //$NON-NLS-1$
			walkResults();
			
			try {
				getConnection(VDB, DQP_PROP_FILE, "user=admin;password=teiid;shutdown=true"); //$NON-NLS-1$
				TestCase.fail("should have failed to connect, as this is request for shutdown"); //$NON-NLS-1$ 
			}catch(Exception e) {
				//pass
			}
			if (!conn.isClosed()) {
				TestCase.fail("should have closed"); //$NON-NLS-1$
			}
		} catch (Exception e) {
		}
    }    
	
}
