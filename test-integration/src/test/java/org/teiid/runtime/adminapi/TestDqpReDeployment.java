package org.teiid.runtime.adminapi;


import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Iterator;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import org.teiid.adminapi.Admin;
import org.teiid.adminapi.AdminOptions;
import org.teiid.adminapi.Session;
import org.teiid.adminapi.VDB;

import com.metamatrix.core.util.FileUtils;
import com.metamatrix.core.util.UnitTestUtil;

public class TestDqpReDeployment {

    String DEPLOY_FILE = UnitTestUtil.getTestDataPath()+"/admin/dqp.properties;user=admin;password=teiid"; //$NON-NLS-1$
    String ADMIN_URL_PREFIX = "jdbc:teiid:admin@"; //$NON-NLS-1$
    
	@Before
	public void setUp() throws Exception {
		FileUtils.copy(UnitTestUtil.getTestDataPath()+"/admin/Admin.vdb", UnitTestUtil.getTestScratchPath()+"/adminapi/deploy/Admin.vdb"); //$NON-NLS-1$ //$NON-NLS-2$
		FileUtils.copy(UnitTestUtil.getTestDataPath()+"/ServerConfig.xml", UnitTestUtil.getTestScratchPath()+"/adminapi/deploy/configuration.xml"); //$NON-NLS-1$ //$NON-NLS-2$
	}
    
	@After
    public void tearDown() throws Exception {
        Connection conn = null;
        
        conn = Util.getConnection(ADMIN_URL_PREFIX + DEPLOY_FILE);
        com.metamatrix.jdbc.api.Connection mmconn = (com.metamatrix.jdbc.api.Connection) conn;
        Admin admin = mmconn.getAdminAPI();
        
        // Remove any existing VDBs from the previous tests.
        Iterator iter = admin.getVDBs("*").iterator(); //$NON-NLS-1$
        while (iter.hasNext()) {
            VDB vdb = (VDB)iter.next();
            if (!vdb.getName().equalsIgnoreCase("admin")){ //$NON-NLS-1$
                admin.changeVDBStatus(vdb.getName(), vdb.getVDBVersion(), VDB.DELETED);                
            }
        }
    }
    
    
    /**
     * Opens connection to the VDB and tries to deploy when conn is open.
     * The redeployment should not affect the current connection.
     * @throws Exception
     */
    @Test public void testKeepConnectionReplaceVdb () throws Exception {
        Connection conn = null;
        Connection testConn = null;
        String VDB_NAME = "ReplaceActive"; //$NON-NLS-1$
        try {
            conn = Util.getConnection(ADMIN_URL_PREFIX + DEPLOY_FILE);
            com.metamatrix.jdbc.api.Connection mmconn = (com.metamatrix.jdbc.api.Connection) conn;
            Admin admin = mmconn.getAdminAPI();

            VDB vdb = deployVdbClean(admin, VDB_NAME, UnitTestUtil.getTestDataPath()+"/admin/TestORS.vdb"); //$NON-NLS-1$
            String currentVersion = vdb.getVDBVersion();
            assertEquals("1", currentVersion); //$NON-NLS-1$
            
            // Connect and run test query
            testConn = Util.getConnection("jdbc:teiid:" + VDB_NAME + "@" + DEPLOY_FILE); //$NON-NLS-1$ //$NON-NLS-2$
            testConnection(testConn, "SELECT * FROM C_REPOS_DB_RELEASE", true, "Initial Test"); //$NON-NLS-1$ //$NON-NLS-2$

            // Redeploy the VDB while the connection is open.  New VDB has A_CCOUNT table
            vdb = deployVdbClean(admin, VDB_NAME, UnitTestUtil.getTestDataPath()+"/admin/TestORS1.vdb"); //$NON-NLS-1$

            testConnection(testConn, "select * from C_ACCOUNT", false, "Redeploy V2 Test"); //$NON-NLS-1$ //$NON-NLS-2$
        } catch (Exception x) {
            fail("Failed with exception " + Util.getStackTraceAsString(x)); //$NON-NLS-1$
        } finally {
            Util.closeQuietly(conn);
        }
    }

    /**
     * Redeploy while there is an opened connection.  Restart the DQP to have the changes take effect
     * Validate that the open connection becomes invalid and a new connection gets the latest VDB
     * @throws Exception
     */
    @Test public void testReplaceVdbWithDqpRestart() throws Exception {
        Connection conn = null;
        Connection testConn = null;
        Connection newTestConn = null;

        String VDB_NAME = "ReplaceRestart"; //$NON-NLS-1$
        try {
            conn = Util.getConnection(ADMIN_URL_PREFIX + DEPLOY_FILE);
            com.metamatrix.jdbc.api.Connection mmconn = (com.metamatrix.jdbc.api.Connection) conn;
            Admin admin = mmconn.getAdminAPI();
            VDB vdb = deployVdbClean(admin, VDB_NAME, UnitTestUtil.getTestDataPath()+"/admin/TestORS.vdb"); //$NON-NLS-1$
            String currentVersion = vdb.getVDBVersion();
            assertEquals("1", currentVersion); //$NON-NLS-1$
            
            // Connect and run test query
            testConn = Util.getConnection("jdbc:teiid:" + VDB_NAME + "@" + DEPLOY_FILE); //$NON-NLS-1$ //$NON-NLS-2$
            testConnection(testConn, "SELECT * FROM C_REPOS_DB_RELEASE", true, "Initial V1 Test"); //$NON-NLS-1$ //$NON-NLS-2$
            testConnection(testConn, "select * from C_ACCOUNT", false, "Initial V2 Test"); //$NON-NLS-1$ //$NON-NLS-2$

            // Redeploy the VDB while the connection is open.  New VDB has A_CCOUNT table
            vdb = deployVdbClean(admin, VDB_NAME, UnitTestUtil.getTestDataPath()+"/admin/TestORS1.vdb"); //$NON-NLS-1$

            admin.restart();
            Thread.sleep(2000);

            newTestConn = Util.getConnection("jdbc:teiid:" + VDB_NAME + "@" + DEPLOY_FILE); //$NON-NLS-1$ //$NON-NLS-2$
            testConnection(newTestConn, "select * from C_ACCOUNT", true, "Redeploy V2 Test"); //$NON-NLS-1$ //$NON-NLS-2$
            testConnection(testConn, "SELECT 1", false, "Stale Conn test"); //$NON-NLS-1$ //$NON-NLS-2$

        } catch (Exception x) {
            fail("Failed with exception " + Util.getStackTraceAsString(x)); //$NON-NLS-1$
        } finally {
            Util.closeQuietly(conn);
            Util.closeQuietly(testConn);
            Util.closeQuietly(newTestConn);
        }
    }

    /**
     * SIP53 Scenario 1
     * Redeployment when there are no active connections.  The new connection should get the latest version
     * @throws Exception
     */
    @Test public void testReplaceNonActiveVdb () throws Exception {
        Connection conn = null;
        Connection testConn = null;
        String VDB_NAME = "ReplaceNonActive"; //$NON-NLS-1$
        try {
            conn = Util.getConnection(ADMIN_URL_PREFIX + DEPLOY_FILE);
            com.metamatrix.jdbc.api.Connection mmconn = (com.metamatrix.jdbc.api.Connection) conn;
            Admin admin = mmconn.getAdminAPI();

            VDB vdb = deployVdbClean(admin, VDB_NAME, UnitTestUtil.getTestDataPath()+"/admin/TestORS.vdb"); //$NON-NLS-1$
            String currentVersion = vdb.getVDBVersion();
            assertEquals("1", currentVersion); //$NON-NLS-1$
            
            // Connect and run test query
            testConn = Util.getConnection("jdbc:teiid:" + VDB_NAME + "@" + DEPLOY_FILE); //$NON-NLS-1$ //$NON-NLS-2$
            testConnection(testConn, "SELECT * FROM C_REPOS_DB_RELEASE", true, "Initial V1 Test"); //$NON-NLS-1$ //$NON-NLS-2$
            testConnection(testConn, "select * from C_ACCOUNT", false, "Initial V2 Test"); //$NON-NLS-1$ //$NON-NLS-2$
            Util.closeQuietly(testConn);

            // Verify that there are no sessions left reset all sessions to the VDB
            Iterator sessionIter = admin.getSessions("*").iterator(); //$NON-NLS-1$
            while(sessionIter.hasNext()) {
                Session session = (Session)sessionIter.next();
                if (VDB_NAME.equals(session.getVDBName()) && currentVersion.equals(session.getVDBVersion())) {
                    fail("There should not be any sessions open against the VDB"); //$NON-NLS-1$
                }
            }

            // Redeploy the VDB while the connection is open.  New VDB has A_CCOUNT table
            vdb = deployVdbClean(admin, VDB_NAME, UnitTestUtil.getTestDataPath()+"/admin/TestORS1.vdb"); //$NON-NLS-1$

            testConn = Util.getConnection("jdbc:teiid:" + VDB_NAME + "@" + DEPLOY_FILE); //$NON-NLS-1$ //$NON-NLS-2$
            testConnection(testConn, "select * from C_ACCOUNT", true, "Redeploy V2 Test"); //$NON-NLS-1$ //$NON-NLS-2$
        } catch (Exception x) {
            fail("Failed with exception " + Util.getStackTraceAsString(x)); //$NON-NLS-1$
        } finally {
            Util.closeQuietly(conn);
            Util.closeQuietly(testConn);
        }
    }

    /**
     * SIP53 Scenario 2
     * Similar to the <code>testReplaceNonActiveVdb</code>.  Uses the session shutdown instead of
     * expecting connections to be closed
     * @throws Exception
     */
    @Test public void testReplaceVdbWithSessionClose() throws Exception {
        Connection conn = null;
        Connection testConn = null;
        Connection newTestConn = null;
        String VDB_NAME = "ReplaceSessionClose"; //$NON-NLS-1$
        try {
            conn = Util.getConnection(ADMIN_URL_PREFIX + DEPLOY_FILE);
            com.metamatrix.jdbc.api.Connection mmconn = (com.metamatrix.jdbc.api.Connection) conn;
            Admin admin = mmconn.getAdminAPI();
            VDB vdb = deployVdbClean(admin, VDB_NAME, UnitTestUtil.getTestDataPath()+"/admin/TestORS.vdb"); //$NON-NLS-1$
            String currentVersion = vdb.getVDBVersion();

            // Connect and run test query
            testConn = Util.getConnection("jdbc:teiid:" + VDB_NAME + "@" + DEPLOY_FILE); //$NON-NLS-1$ //$NON-NLS-2$
            testConnection(testConn, "SELECT * FROM C_REPOS_DB_RELEASE", true, "Initial V1 Test"); //$NON-NLS-1$ //$NON-NLS-2$
            testConnection(testConn, "select * from C_ACCOUNT", false, "Initial V2 Test"); //$NON-NLS-1$ //$NON-NLS-2$

            // reset all sessions to the VDB
            Iterator sessionIter = admin.getSessions("*").iterator(); //$NON-NLS-1$
            boolean sessionFound = false;
            while(sessionIter.hasNext()) {
                Session session = (Session)sessionIter.next();
                if (VDB_NAME.equals(session.getVDBName()) && currentVersion.equals(session.getVDBVersion())) {
                    admin.terminateSession(session.getSessionID());
                    sessionFound = true;
                }
            }
            if ( ! sessionFound ) {
                fail("Did not find the expected connection to terminate"); //$NON-NLS-1$
            }

            // Redeploy the VDB while the connection is open.  New VDB has A_CCOUNT table
            vdb = deployVdbClean(admin, VDB_NAME, UnitTestUtil.getTestDataPath()+"/admin/TestORS1.vdb"); //$NON-NLS-1$

            newTestConn = Util.getConnection("jdbc:teiid:" + VDB_NAME + "@" + DEPLOY_FILE); //$NON-NLS-1$ //$NON-NLS-2$
            testConnection(newTestConn, "select * from C_ACCOUNT", true, "Redeploy V2 Test"); //$NON-NLS-1$ //$NON-NLS-2$
            testConnection(testConn, "SELECT 1", false, "Stale Conn test"); //$NON-NLS-1$ //$NON-NLS-2$

        } catch (Exception x) {
            fail("Failed with exception " + Util.getStackTraceAsString(x)); //$NON-NLS-1$
        } finally {
            Util.closeQuietly(conn);
            Util.closeQuietly(testConn);
            Util.closeQuietly(newTestConn);
        }
    }

    /**
     * SIP51, SIP52
     * @throws Exception
     */
    @Test public void testDeleteInvalidVdb() throws Exception {
        Connection conn = null;
        String VDB_NAME = "DeleteInvalid"; //$NON-NLS-1$
        try {
            conn = Util.getConnection(ADMIN_URL_PREFIX + DEPLOY_FILE);
            com.metamatrix.jdbc.api.Connection mmconn = (com.metamatrix.jdbc.api.Connection) conn;
            Admin admin = mmconn.getAdminAPI();
            // Try clean deployment twice
            try {
				deployVdbClean(admin, VDB_NAME, UnitTestUtil.getTestDataPath()+"/admin/TestORSInvalid.vdb"); //$NON-NLS-1$
				fail("Failed with exception "); //$NON-NLS-1$
			} catch (Exception e) {
				//pass
			}
        } catch (Exception x) {
        	fail("Failed with exception " + Util.getStackTraceAsString(x)); //$NON-NLS-1$
        } finally {
            Util.closeQuietly(conn);
        }
    }


    private VDB deployVdbClean(Admin admin, String vdbName, String vdbFile) throws Exception{

        VDB vdb;
        // Remove any existing VDBs from the previous tests.
        Iterator iter = admin.getVDBs(vdbName).iterator();
        while (iter.hasNext()) {
            vdb = (VDB)iter.next();
            admin.changeVDBStatus(vdb.getName(), vdb.getVDBVersion(), VDB.DELETED);
        }

        vdb = admin.addVDB(vdbName,Util.getBinaryFile(vdbFile), new AdminOptions(AdminOptions.OnConflict.OVERWRITE));
        return vdb;
    }

    void testConnection(Connection conn, String sql, boolean expectSuccess, String testName) {
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);
            if ( ! rs.next() ) {
                fail(sql + "executed with 0 rows returned"); //$NON-NLS-1$
            }
        } catch (Exception e) {
            if (expectSuccess) {
                fail(testName + " failed for [" + sql + "] with " + e.getMessage()); //$NON-NLS-1$ //$NON-NLS-2$
            }
        } finally {
            Util.closeQuietly(null, stmt, rs);
        }
    }




}
