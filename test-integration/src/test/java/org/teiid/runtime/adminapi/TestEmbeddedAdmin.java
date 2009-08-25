/*
 * Copyright (c) 2000-2005 MetaMatrix, Inc.
 * All rights reserved.
 */
package org.teiid.runtime.adminapi;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.teiid.adminapi.Admin;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.AdminOptions;
import org.teiid.adminapi.ConnectorBinding;
import org.teiid.adminapi.Session;
import org.teiid.adminapi.VDB;
import org.teiid.jdbc.TeiidDriver;

import com.metamatrix.core.util.FileUtils;
import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.jdbc.MMConnection;
import com.metamatrix.jdbc.api.Connection;

/** 
 * @since 4.3
 */
public class TestEmbeddedAdmin {
    
    private static final String VDB_FILE = UnitTestUtil.getTestDataPath()+"/admin/TestEmpty.vdb"; //$NON-NLS-1$
    private static final String VDB_NAME = "TestEmpty"; //$NON-NLS-1$

    String configFile = UnitTestUtil.getTestDataPath()+"/admin/dqp.properties"; //$NON-NLS-1$
    Connection conn = null;
    Connection adminConn = null;
    Admin admin = null;
    Statement stmt = null;
    ResultSet result = null;
    
    /* Utility methods */
    private Connection getConnection(String vdb, String configFile) throws SQLException {
        String url = "jdbc:teiid:"+vdb+"@"+configFile+";user=admin;password=teiid"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$            
        new TeiidDriver();
        Connection conn = (Connection)DriverManager.getConnection(url);            
        return conn;
    }
    
    Admin getAdmin() throws Exception{        
        adminConn = getConnection("admin", configFile); //$NON-NLS-1$
        return adminConn.getAdminAPI();
    }

    @Before
	public void setUp() throws Exception {
		FileUtils.copy(UnitTestUtil.getTestDataPath()+"/admin/Admin.vdb", UnitTestUtil.getTestScratchPath()+"/Admin.vdb"); //$NON-NLS-1$ //$NON-NLS-2$
		FileUtils.copy(UnitTestUtil.getTestDataPath()+"/ServerConfig.xml", UnitTestUtil.getTestScratchPath()+"/configuration.xml"); //$NON-NLS-1$ //$NON-NLS-2$	
	}
    

    @After
    public void tearDown() throws Exception {
        if (result != null) {
            result.close();
        }
        if (stmt != null) {
            stmt.close();
        }
        if (adminConn != null) {
            adminConn.close();
        }
        if (conn != null) {
            conn.close();
        }
    }
    
    void cleanupVDB(Admin admin, String name, String version) {
        try {
            // make sure we delete any
            admin.changeVDBStatus(name, version, VDB.DELETED);
        }catch(Exception e) {
            //ignore it might say not found
        }        
    }
    
    /* Test methods */
    @Test public void testGetConnectionToAdmin() throws Exception {        
        conn = getConnection("admin", configFile); //$NON-NLS-1$
        assertFalse("Found a Closed Connection to Admin", conn.isClosed());             //$NON-NLS-1$
    }
    
    @Test public void testAddTwoVDBsWithSameNameandVersion() throws Exception {
        admin = getAdmin();
        cleanupVDB(admin, VDB_NAME, "1"); //$NON-NLS-1$
        
        admin.addVDB(VDB_NAME, Util.getBinaryFile(VDB_FILE), new AdminOptions(AdminOptions.OnConflict.IGNORE));
        admin.changeVDBStatus(VDB_NAME, "1", VDB.ACTIVE); //$NON-NLS-1$ 
        
        boolean found = false;
        Collection vdbs = admin.getVDBs(VDB_NAME);
        for (Iterator i = vdbs.iterator(); i.hasNext();) {
            VDB vdb = (VDB)i.next();
            if (vdb.getName().equals(VDB_NAME) && vdb.getVDBVersion().equals("1")) { //$NON-NLS-1$
                found = true;
            }
        }            
        assertTrue("Deployed VDB not found in the configuration", found); //$NON-NLS-1$

        admin.addVDB(VDB_NAME, Util.getBinaryFile(VDB_FILE), new AdminOptions(AdminOptions.OnConflict.IGNORE)); 
        admin.changeVDBStatus(VDB_NAME, "1", VDB.ACTIVE); //$NON-NLS-1$ 

        found = false;
        vdbs = admin.getVDBs(VDB_NAME);
        for (Iterator i = vdbs.iterator(); i.hasNext();) {
            VDB vdb = (VDB)i.next();
            if (vdb.getName().equals(VDB_NAME) && vdb.getVDBVersion().equals("2")) { //$NON-NLS-1$
                found = true;
            }
        }            
        assertTrue("Deployed VDB not found in the configuration", found); //$NON-NLS-1$
        
        cleanupVDB(admin, VDB_NAME, "1"); //$NON-NLS-1$
        cleanupVDB(admin, VDB_NAME, "2"); //$NON-NLS-1$
    }
    
    @Test public void testDeployVdbImbeddedDef() throws Exception {
        admin = getAdmin();
        cleanupVDB(admin, VDB_NAME, "1"); //$NON-NLS-1$
        
        admin.addVDB(VDB_NAME, Util.getBinaryFile(VDB_FILE), new AdminOptions(AdminOptions.OnConflict.IGNORE)); 
        admin.changeVDBStatus(VDB_NAME, "1", VDB.ACTIVE); //$NON-NLS-1$ 
        
        boolean found = false;
        Collection vdbs = admin.getVDBs(VDB_NAME);
        for (Iterator i = vdbs.iterator(); i.hasNext();) {
            VDB vdb = (VDB)i.next();
            if (vdb.getName().equals(VDB_NAME)) {
                found = true;
            }
        }
        assertTrue("Deployed VDB not found in the configuration", found); //$NON-NLS-1$
        cleanupVDB(admin, VDB_NAME, "1"); //$NON-NLS-1$
    }    
    
    @Test public void testUndeployVdb() throws Exception {
        admin = getAdmin();
        cleanupVDB(admin, VDB_NAME, "1"); //$NON-NLS-1$
        
        admin.addVDB(VDB_NAME, Util.getBinaryFile(VDB_FILE), new AdminOptions(AdminOptions.OnConflict.IGNORE));
        admin.changeVDBStatus(VDB_NAME, "1", VDB.ACTIVE); //$NON-NLS-1$ 
        
        boolean found = false;
        Collection vdbs = admin.getVDBs(VDB_NAME);
        for (Iterator i = vdbs.iterator(); i.hasNext();) {
            VDB vdb = (VDB)i.next();
            if (vdb.getName().equals(VDB_NAME)) {
                found = true;
            }
        }
        assertTrue("Deployed VDB not found in the configuration", found); //$NON-NLS-1$
        
        admin.changeVDBStatus(VDB_NAME, "1", VDB.DELETED); //$NON-NLS-1$ 

        found = false;
        vdbs = admin.getVDBs(VDB_NAME);
        for (Iterator i = vdbs.iterator(); i.hasNext();) {
            VDB vdb = (VDB)i.next();
            if (vdb.getName().equals(VDB_NAME)) {
                found = true;
            }
        }
        assertFalse("Deployed VDB found in the configuration after delete", found); //$NON-NLS-1$
        cleanupVDB(admin, VDB_NAME, "1"); //$NON-NLS-1$
    }

    @Test public void testUndeployNonExistantVdb() throws Exception {
        admin = getAdmin();
        try {
            admin.changeVDBStatus("DoesNotExist", "1", VDB.DELETED); //$NON-NLS-1$ //$NON-NLS-2$
            fail("Must have failed to delete a non existing VDB"); //$NON-NLS-1$
        } catch (AdminException err) {
            assertEquals("VDB \"DoesNotExist\" version \"1\" does not exist or not in valid state.", err.getMessage()); //$NON-NLS-1$
        } 
    }

    @Test public void testGetConnectionToExistingVdb() throws Exception {
        admin = getAdmin();
        cleanupVDB(admin, VDB_NAME, "1"); //$NON-NLS-1$
        
        admin.addVDB(VDB_NAME, Util.getBinaryFile(VDB_FILE), new AdminOptions(AdminOptions.OnConflict.IGNORE)); 
        admin.changeVDBStatus(VDB_NAME, "1", VDB.ACTIVE); //$NON-NLS-1$
        
        conn = getConnection(VDB_NAME, configFile);
        assertFalse("found a closed connection", conn.isClosed()); //$NON-NLS-1$
        cleanupVDB(admin, VDB_NAME, "1"); //$NON-NLS-1$
    }
    
    @Test public void testGetConnectionToNonExistingVdb() throws Exception {
        try {
            getConnection("DoesNotExist", configFile); //$NON-NLS-1$
            fail("found a Connection to a non avtive VDB"); //$NON-NLS-1$
        } catch (SQLException err) {
            assertEquals("VDB \"DoesNotExist\" version \"latest\" does not exist or not in valid state.", err.getMessage()); //$NON-NLS-1$
        } 
    }
    
    @Test public void testGetConnectionToNotActiveVdb() throws Exception {
        admin = getAdmin();
        cleanupVDB(admin, VDB_NAME, "1"); //$NON-NLS-1$
        
        admin.addVDB(VDB_NAME, Util.getBinaryFile(VDB_FILE), new AdminOptions(AdminOptions.OnConflict.IGNORE));
        admin.changeVDBStatus(VDB_NAME, "1", VDB.INACTIVE); //$NON-NLS-1$ 
        
        try {
            conn = getConnection(VDB_NAME, configFile);
            fail("found a Connection to a non avtive VDB");                 //$NON-NLS-1$
        } catch (SQLException err) {
            assertEquals("Unexpected error finding latest version of Virtual Database TestEmpty", err.getMessage()); //$NON-NLS-1$
        } finally {
            cleanupVDB(admin, VDB_NAME, "1"); //$NON-NLS-1$
        }
    }

    
    @Test public void testSelectNonPrepared() throws Exception {
        admin = getAdmin();
        cleanupVDB(admin, VDB_NAME, "1"); //$NON-NLS-1$
        
        admin.addVDB(VDB_NAME, Util.getBinaryFile(VDB_FILE), new AdminOptions(AdminOptions.OnConflict.IGNORE)); 
        admin.changeVDBStatus(VDB_NAME, "1", VDB.ACTIVE); //$NON-NLS-1$ 

        conn = getConnection(VDB_NAME, configFile);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM smalla"); //$NON-NLS-1$
        if (!rs.next()) {
            fail("SELECT * FROM smalla failed"); //$NON-NLS-1$
        }
        cleanupVDB(admin, VDB_NAME, "1"); //$NON-NLS-1$
    }

    @Test public void testSelectPrepared() throws Exception {
        admin = getAdmin();
        cleanupVDB(admin, VDB_NAME, "1"); //$NON-NLS-1$
        admin.addVDB(VDB_NAME, Util.getBinaryFile(VDB_FILE), new AdminOptions(AdminOptions.OnConflict.IGNORE)); 
        admin.changeVDBStatus(VDB_NAME, "1", VDB.ACTIVE); //$NON-NLS-1$ 

        conn = getConnection(VDB_NAME, configFile);
        executePreparedStatement("SELECT * FROM smalla"); //$NON-NLS-1$
        cleanupVDB(admin, VDB_NAME, "1"); //$NON-NLS-1$
    }
    
    @Test public void testStopConnectorBinding() throws Exception {
        addVDB();
        
        admin.stopConnectorBinding("Loopback", true); //$NON-NLS-1$
        
        try {
            executeStatement("SELECT * FROM smalla"); //$NON-NLS-1$
            fail("Ran statement on closed connector binding! Wrong!"); //$NON-NLS-1$
        }catch(Exception e) {
            // pass good
        }
        
        admin.startConnectorBinding("Loopback");             //$NON-NLS-1$
        executeStatement("SELECT * FROM smalla"); //$NON-NLS-1$
        
        cleanupVDB(admin, VDB_NAME, "1"); //$NON-NLS-1$
    }    
    
    
    @Test public void testStopNonExistingConnectorBinding() throws Exception {
        addVDB();
        
        try {
            admin.stopConnectorBinding("NO_CONNECTORS", true); //$NON-NLS-1$
            fail("stopped a unknown connector Wow!"); //$NON-NLS-1$
        } catch(AdminException e) {
            assertEquals("Connector Binding with name \"NO_CONNECTORS\" does not exist in the configuration", e.getMessage()); //$NON-NLS-1$
        }
        
        executeStatement("SELECT * FROM smalla"); //$NON-NLS-1$
        
        admin.startConnectorBinding("Loopback");             //$NON-NLS-1$
        executeStatement("SELECT * FROM smalla"); //$NON-NLS-1$
        
        cleanupVDB(admin, VDB_NAME, "1"); //$NON-NLS-1$
    } 

    @Test public void testAddConnectorBinding() throws Exception {
        addVDB();            
        String LOOPBACK = "loopy";             //$NON-NLS-1$
        admin.addConnectorBinding(LOOPBACK, Util.getCharacterFile(UnitTestUtil.getTestDataPath()+"/admin/loopback.cdk"), new AdminOptions(AdminOptions.OnConflict.OVERWRITE)); //$NON-NLS-1$
        admin.startConnectorBinding(LOOPBACK);
        
        boolean found = true;
        Collection c = admin.getConnectorBindings(LOOPBACK);
        for (Iterator i = c.iterator(); i.hasNext();) {
            ConnectorBinding binding = (ConnectorBinding)i.next();
            if (binding.getName().equals(LOOPBACK)) {
                found = true;
            }
        }
        assertTrue("Connector LOOPBACK not found after adding", found); //$NON-NLS-1$
        
        admin.assignBindingToModel(LOOPBACK, VDB_NAME, "1", "Oracle");             //$NON-NLS-1$ //$NON-NLS-2$
        stmt = conn.createStatement(); 
        result =  stmt.executeQuery("Select * from smalla"); //$NON-NLS-1$
        int count = 0;
        while (result.next()) {
            count++;                
        }        
        assertEquals("Expected one row", 1, count); //$NON-NLS-1$
        
        
        Collection cbindings = admin.getConnectorBindings("*"); //$NON-NLS-1$
        ConnectorBinding binding = null;
        for (Iterator i = cbindings.iterator(); i.hasNext();) {
            ConnectorBinding current = (ConnectorBinding)i.next();
            if (current.getName().equals(LOOPBACK)) {
                binding = current;
            }
        }            
        admin.setProperty(binding.getIdentifier(), ConnectorBinding.class.getName(), "RowCount", "10"); //$NON-NLS-1$ //$NON-NLS-2$
        admin.stopConnectorBinding(LOOPBACK, true);
        admin.startConnectorBinding(LOOPBACK);            

        stmt = conn.createStatement(); 
        result =  stmt.executeQuery("Select * from smalla"); //$NON-NLS-1$

        count = 0;
        while (result.next()) {
            count++;                
        }        
        assertEquals("Expected ten rows", 10, count); //$NON-NLS-1$
        
        cleanupVDB(admin, VDB_NAME, "1"); //$NON-NLS-1$
    }

    @Test public void testAddUnknownConnectorBinding() throws Exception {
        addVDB();            
        try {
            admin.startConnectorBinding("UNKNOWN"); //$NON-NLS-1$
            fail("Started a unknown connector, BAD"); //$NON-NLS-1$
        } catch(AdminException e) {
            assertEquals("Connector Binding with name \"UNKNOWN\" does not exist in the configuration", e.getMessage()); //$NON-NLS-1$
        }            
        cleanupVDB(admin, VDB_NAME, "1"); //$NON-NLS-1$
    }
   

    @Test public void testExtensionModules() throws Exception {
        addVDB();            
                    
        Collection c = admin.getExtensionModules("*"); //$NON-NLS-1$
        assertEquals(0, c.size());

        admin.addExtensionModule("jar", "loopbackconn.jar", Util.getBinaryFile(UnitTestUtil.getTestDataPath()+"/admin/loopbackconn.jar"), "Loopback Jar"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$

        c = admin.getExtensionModules("*"); //$NON-NLS-1$
        assertEquals(1, c.size());
        
        admin.deleteExtensionModule("loopbackconn.jar");             //$NON-NLS-1$

        c = admin.getExtensionModules("*"); //$NON-NLS-1$
        assertEquals(0, c.size());
        
        cleanupVDB(admin, VDB_NAME, "1"); //$NON-NLS-1$
    }
    
    @Test public void testGetSessions() throws Exception {
        adminConn = getConnection("admin", configFile); //$NON-NLS-1$
        admin = adminConn.getAdminAPI();           
        admin.restart();    
        
        addVDB();
        Collection<Session> c = admin.getSessions("*"); //$NON-NLS-1$
        assertEquals(2, c.size());
        
        MMConnection myconn = (MMConnection)this.conn;
        
        admin.terminateSession(myconn.getConnectionId());
        this.result = null;
        this.stmt = null;
        this.conn = null;
        
        c = admin.getSessions("*"); //$NON-NLS-1$
        assertEquals(1, c.size());            
        
        cleanupVDB(admin, VDB_NAME, "1"); //$NON-NLS-1$
    }    
           
  
    @Test public void testExportVDB() throws Exception {
        addVDB();
        
        Util.writeToFile("Test.VDB", admin.exportVDB(VDB_NAME, "1")); //$NON-NLS-1$ //$NON-NLS-2$
        File f = new File("Test.VDB"); //$NON-NLS-1$
        assertTrue("failed to export vdb", f.exists()); //$NON-NLS-1$
        
        f.delete();
        
        cleanupVDB(admin, VDB_NAME, "1"); //$NON-NLS-1$
    }

    @Test public void testExportConnectorBinging() throws Exception {
        addVDB();
        
        Util.writeToFile("Oracle.cdk", admin.exportConnectorBinding("Loopback")); //$NON-NLS-1$ //$NON-NLS-2$
        File f = new File("Oracle.cdk"); //$NON-NLS-1$
        assertTrue("failed to export connector binding", f.exists()); //$NON-NLS-1$
        
        f.delete();
        
        cleanupVDB(admin, VDB_NAME, "1"); //$NON-NLS-1$
    }

    @Test public void testExportConnectorType() throws Exception {
        addVDB();
        
        Util.writeToFile(UnitTestUtil.getTestScratchPath()+"/loopy.cdk", admin.exportConnectorType("Loopback Connector")); //$NON-NLS-1$ //$NON-NLS-2$
        File f = new File(UnitTestUtil.getTestScratchPath()+"/loopy.cdk"); //$NON-NLS-1$
        assertTrue("failed to export connector binding", f.exists()); //$NON-NLS-1$
        
        f.delete();
        
        cleanupVDB(admin, VDB_NAME, "1"); //$NON-NLS-1$
    }
    
    @Test public void testExportServerConfig() throws Exception {
        addVDB();
        
        Util.writeToFile(UnitTestUtil.getTestScratchPath()+"/serverconfig.xml", admin.exportConfiguration()); //$NON-NLS-1$
        File f = new File(UnitTestUtil.getTestScratchPath()+"/serverconfig.xml"); //$NON-NLS-1$
        assertTrue("failed to export connector binding", f.exists()); //$NON-NLS-1$
        
        f.delete();
        
        cleanupVDB(admin, VDB_NAME, "1"); //$NON-NLS-1$
    }    
    
    @Test public void testExportExtensionModule() throws Exception {
        addVDB();
        admin.addExtensionModule("jar", "loopback.jar", Util.getBinaryFile(UnitTestUtil.getTestDataPath()+"/admin/loopbackconn.jar"), ""); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        Util.writeToFile(UnitTestUtil.getTestScratchPath()+"/loop.jar", admin.exportExtensionModule("loopback.jar")); //$NON-NLS-1$ //$NON-NLS-2$
        File f = new File(UnitTestUtil.getTestScratchPath()+"/loop.jar"); //$NON-NLS-1$
        assertTrue("failed to export connector binding", f.exists()); //$NON-NLS-1$
        
        f.delete();
        
        admin.deleteExtensionModule("loopback.jar"); //$NON-NLS-1$
        
        cleanupVDB(admin, VDB_NAME, "1"); //$NON-NLS-1$
    } 
     
    
    @Test public void testUDF() throws Exception {
        addVDB();
        stmt = conn.createStatement();
        result = stmt.executeQuery("SELECT GetSystemProperty('path.separator')"); //$NON-NLS-1$
        
        assertTrue(result.next());
        assertEquals(System.getProperty("path.separator"), result.getObject(1)); //$NON-NLS-1$
        assertTrue(!result.next());
        
        result.close();
        stmt.close();
        
        stmt = conn.createStatement();
        result = stmt.executeQuery("SELECT getxyz()"); //$NON-NLS-1$
        
        assertTrue(result.next());
        assertEquals("xyz", result.getObject(1)); //$NON-NLS-1$
        assertTrue(!result.next());        
    }    
    
    /** 
     * @throws Exception
     * @throws AdminException
     * @since 4.3
     */
    private void addVDB() throws Exception{
        admin = getAdmin();
        cleanupVDB(admin, VDB_NAME, "1"); //$NON-NLS-1$

        admin.addVDB(VDB_NAME, Util.getBinaryFile(VDB_FILE), new AdminOptions(AdminOptions.OnConflict.IGNORE)); 
        admin.changeVDBStatus(VDB_NAME, "1", VDB.ACTIVE); //$NON-NLS-1$ 

        conn = getConnection(VDB_NAME, configFile);
        executeStatement("SELECT * FROM smalla"); //$NON-NLS-1$
    } 
    
 
    private void executePreparedStatement(String query)  throws Exception{
        PreparedStatement pstmt = conn.prepareStatement(query); 
        pstmt.execute();
        result =  pstmt.getResultSet();
        if (!result.next()) {
            fail("SELECT * FROM smalla"); //$NON-NLS-1$
        }        
        stmt = pstmt;
    }

    private void executeStatement(String query) throws Exception{
        stmt = conn.createStatement();        
        result =  stmt.executeQuery(query);
        if (!result.next()) {
            fail("SELECT * FROM smalla"); //$NON-NLS-1$
        }        
    }    
}
