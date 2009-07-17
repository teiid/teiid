package com.metamatrix.server.integration;

import junit.framework.TestCase;

import org.junit.Test;

import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.jdbc.api.AbstractMMQueryTestCase;

public class TestDataEntitlements extends AbstractMMQueryTestCase {

    private static final String DQP_PROP_FILE = UnitTestUtil.getTestDataPath() + "/authcheck/bqt.properties;"; //$NON-NLS-1$
    private static final String VDB = "bqt"; //$NON-NLS-1$

    @Test public void testNoUserAuth() {
    	try {
			getConnection(VDB, DQP_PROP_FILE, "user=test"); //$NON-NLS-1$
			TestCase.fail("Should have failed authenticate user test"); //$NON-NLS-1$
			closeConnection();
		} catch (Exception e) {
		}
    }
    
    @Test public void testAdminAuth() {
		try {
			getConnection(VDB, DQP_PROP_FILE, "user=admin;password=mm"); //$NON-NLS-1$
			TestCase.fail("Should have failed authenticate user test"); //$NON-NLS-1$			
		} catch (Exception e) {
		}
		
		getConnection(VDB, DQP_PROP_FILE, "user=admin;password=teiid"); //$NON-NLS-1$
		execute("select * from BQT1.smalla"); //$NON-NLS-1$
		walkResults();
		closeConnection();
    }

    @Test public void testEntitlements() {
		try {
			getConnection(VDB, DQP_PROP_FILE, "user=john;password=foo"); //$NON-NLS-1$
			TestCase.fail("Should have failed authenticate user test"); //$NON-NLS-1$
		} catch (Exception e) {
		}

		getConnection(VDB, DQP_PROP_FILE, "user=john;password=mm"); //$NON-NLS-1$
		try {
			execute("select intkey, stringkey from BQT1.smalla"); //$NON-NLS-1$
			walkResults();
		} catch(Exception e) {
			TestCase.assertTrue(e.getMessage().endsWith("is not entitled to action <Read> for 1 or more of the groups/elements/procedures.")); //$NON-NLS-1$
		}
		closeConnection();
		
		getConnection(VDB, DQP_PROP_FILE, "user=paul;password=mm"); //$NON-NLS-1$
		execute("select intkey, stringkey from BQT1.smalla"); //$NON-NLS-1$
		assertRowCount(50);
		closeConnection();

		getConnection(VDB, DQP_PROP_FILE, "user=paul;password=mm"); //$NON-NLS-1$
		try {
			execute("select * from BQT1.smalla"); //$NON-NLS-1$
			walkResults();
		} catch(Exception e) {
			TestCase.assertTrue(e.getMessage().endsWith("is not entitled to action <Read> for 1 or more of the groups/elements/procedures.")); //$NON-NLS-1$
		}
		closeConnection();
    }
    
}
