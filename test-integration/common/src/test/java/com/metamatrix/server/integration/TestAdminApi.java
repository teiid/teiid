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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.AdminObject;
import org.teiid.adminapi.AdminOptions;
import org.teiid.adminapi.ConnectorBinding;
import org.teiid.adminapi.ConnectorType;
import org.teiid.adminapi.EmbeddedLogger;
import org.teiid.adminapi.ExtensionModule;
import org.teiid.adminapi.Group;
import org.teiid.adminapi.LogConfiguration;
import org.teiid.adminapi.ProcessObject;
import org.teiid.adminapi.PropertyDefinition;
import org.teiid.adminapi.Role;
import org.teiid.adminapi.VDB;
import org.teiid.runtime.adminapi.Util;

import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.util.FileUtils;
import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.jdbc.MMConnection;
import com.metamatrix.jdbc.api.AbstractMMQueryTestCase;

public class TestAdminApi extends AbstractMMQueryTestCase {
	
	private static final String STAR = "*"; //$NON-NLS-1$
	private static final String PROPS_FILE = UnitTestUtil.getTestDataPath()+"/admin/dqp.properties;user=admin;password=teiid"; //$NON-NLS-1$
	private static final String MEMBERSHIP_PROPS_FILE = UnitTestUtil.getTestDataPath()+"/admin/dqp-membership.properties"; //$NON-NLS-1$
	private static final String BQT = "BQT"; //$NON-NLS-1$
	private static final String ADMIN = "admin"; //$NON-NLS-1$
	private static final String VDB_NAME = "TestEmpty"; //$NON-NLS-1$
    private static final String VDB_FILE = UnitTestUtil.getTestDataPath()+"/admin/TestEmpty.vdb"; //$NON-NLS-1$
 
	
    @Before
	public void setUp() throws Exception {
		FileUtils.copy(UnitTestUtil.getTestDataPath()+"/admin/Admin.vdb", UnitTestUtil.getTestScratchPath()+"/adminapi/deploy/Admin.vdb"); //$NON-NLS-1$ //$NON-NLS-2$
		FileUtils.copy(UnitTestUtil.getTestDataPath()+"/ServerConfig.xml", UnitTestUtil.getTestScratchPath()+"/adminapi/deploy/configuration.xml"); //$NON-NLS-1$ //$NON-NLS-2$
	}
	
    private void cleanDeploy() throws Exception {
    	Collection<VDB> vdbs = getAdmin().getVDBs(STAR);
    	for(VDB vdb:vdbs) {
    		if (!vdb.getName().equalsIgnoreCase(ADMIN)) {
    			deleteVDB(vdb.getName(), vdb.getVDBVersion());
    		}
    	}
    	
    	Collection<ConnectorBinding> bindings = getAdmin().getConnectorBindings(STAR);
    	for(ConnectorBinding b:bindings) {
    		getAdmin().stopConnectorBinding(b.getIdentifier(), true);
    		getAdmin().deleteConnectorBinding(b.getIdentifier());
    	}
    }
    
    @Test public void testGetProcess() throws Exception {
		getConnection(ADMIN, PROPS_FILE);
		Collection<ProcessObject> processes = getAdmin().getProcesses(STAR); 
		assertEquals(1, processes.size()); 
		assertNotNull(processes.iterator().next().getInetAddress());
    }    
    
	// Setting AutoCommit to "false" was failing in the DQP. This was happening because
	// when auto commit is false, it loads a user transaction and that needs a Txn 
	// manager which currently not available in the DQP. Currently this has been stubbed
	// not to use the txn messaging in the embedded connection.
	@Test public void testDefect19748_setAutoCommitFalse() throws Exception {
		getConnection(ADMIN, PROPS_FILE);
		cleanDeploy();
		
        getAdmin().addVDB(VDB_NAME, Util.getBinaryFile(VDB_FILE), new AdminOptions(AdminOptions.OnConflict.IGNORE)); 
        getAdmin().changeVDBStatus(VDB_NAME, "1", VDB.ACTIVE); //$NON-NLS-1$ 		
		
        pushConnection();
	    Connection c = getConnection(VDB_NAME, PROPS_FILE);
	    try {
	        c.setAutoCommit(false);
	        execute("select * from smalla"); //$NON-NLS-1$
	    }catch(SQLException e) {
	        fail("Embedded driver should have let the user set the autocommit=false"); //$NON-NLS-1$
	    }
	    closeConnection();
	    popConnection();
	    
	    closeConnection();
	}

	@Test public void testDefect19748_setAutoCommitFalse_txnoff() throws Exception {
		getConnection(ADMIN, PROPS_FILE);
		cleanDeploy();
		
        getAdmin().addVDB(BQT, Util.getBinaryFile(VDB_FILE), new AdminOptions(AdminOptions.OnConflict.IGNORE)); 
        getAdmin().changeVDBStatus(BQT, "1", VDB.ACTIVE); //$NON-NLS-1$
        
        pushConnection();
	    Connection c = getConnection(BQT, PROPS_FILE+";txnAutoWrap=OFF"); //$NON-NLS-1$
	    try {
	        c.setAutoCommit(false);
	        execute("select * from smalla"); //$NON-NLS-1$
	        assertRowCount(10);
	    }catch(SQLException e) {
	        fail("Embedded driver should have let the user set the autocommit=false"); //$NON-NLS-1$
	        //pass
	    }
	    closeConnection();
	    popConnection();
	    closeConnection();
	}

	@Test public void testDefect19748_commit_txnoff() throws Exception {
	    Connection c = getConnection(ADMIN, PROPS_FILE+";txnAutoWrap=OFF"); //$NON-NLS-1$
	    cleanDeploy();
	    try {
	        c.commit();    
	    }catch(SQLException e) {
	        fail("Embedded driver should have have let the user commit; because of txn off"); //$NON-NLS-1$
	    }
	    closeConnection();
	}
	
	@Test public void testBindingNames() throws Exception {    
	    getConnection(ADMIN, PROPS_FILE);
	    cleanDeploy();
	    
	    Collection bindings = getAdmin().getConnectorBindings(STAR);
	    
	    int size = bindings.size();
	    
	    assertFalse("VDB does not exist", hasVDB(BQT)); //$NON-NLS-1$
	    
	    addVDB(BQT, UnitTestUtil.getTestDataPath()+"/admin/QT_Ora9DSwDEF.vdb"); //$NON-NLS-1$
	    assertTrue("VDB does not exist", hasVDB(BQT)); //$NON-NLS-1$ 
	    
	    bindings = getAdmin().getConnectorBindings(STAR);
	    assertEquals("Two bindings should exist", 2+size, bindings.size()); //$NON-NLS-1$
	    
	    bindings = getAdmin().getConnectorBindingsInVDB("BQT", "1"); //$NON-NLS-1$
	    assertEquals("Two bindings should exist", 2+size, bindings.size()); //$NON-NLS-1$
	    
	    assertTrue("Binding must be available", hasBinding("BQT_1.BQT1 Oracle 9i Simple Cap"));  //$NON-NLS-1$ //$NON-NLS-2$
	    assertTrue("Binding must be available", hasBinding("BQT_1.BQT2 Oracle 9i Simple Cap")); //$NON-NLS-1$ //$NON-NLS-2$
	    

	    // make sure when the VDB is gone all the enclosed bindings are gone too
	    deleteVDB(BQT, "1"); //$NON-NLS-1$

	    assertFalse("Binding must be available", hasBinding("BQT_1.BQT1 Oracle 9i Simple Cap"));  //$NON-NLS-1$ //$NON-NLS-2$
	    assertFalse("Binding must be available", hasBinding("BQT_1.BQT2 Oracle 9i Simple Cap")); //$NON-NLS-1$ //$NON-NLS-2$

	    closeConnection();    
	}

	@Test public void testDeleteBindings() throws Exception {    
	    getConnection(ADMIN, PROPS_FILE);
	    cleanDeploy();
	    
	    assertFalse("VDB does not exist", hasVDB(BQT)); //$NON-NLS-1$
	    
	    addVDB(BQT, UnitTestUtil.getTestDataPath()+"/admin/QT_Ora9DSwDEF.vdb"); //$NON-NLS-1$
	    
	    try {
	    	getAdmin().deleteConnectorBinding("BQT_1.BQT2 Oracle 9i Simple Cap"); //$NON-NLS-1$
	    	fail("must have failed to delete due to active assosiation to VDB"); //$NON-NLS-1$
	    } catch(Exception e) {
	    }
	    
	    getAdmin().assignBindingToModel("BQT_1.BQT1 Oracle 9i Simple Cap", "BQT", "1", "BQT2"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
	    getAdmin().assignBindingToModel("BQT_1.BQT1 Oracle 9i Simple Cap", "BQT", "1", "SP"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
	    
	    assertFalse("Binding not found", hasBinding("BQT_1.BQT2 Oracle 9i Simple Cap")); //$NON-NLS-1$ //$NON-NLS-2$
        
	    // Check that config state has changed
	    assertTrue("Binding must be found", hasBinding("BQT_1.BQT1 Oracle 9i Simple Cap"));	     //$NON-NLS-1$ //$NON-NLS-2$
	    
	    // make sure when the VDB is gone all the enclosed bindings are gone too
	    deleteVDB(BQT, "1"); //$NON-NLS-1$

	    assertFalse("Binding must be available", hasBinding("BQT_1.BQT1 Oracle 9i Simple Cap"));  //$NON-NLS-1$ //$NON-NLS-2$

	    closeConnection();    
	}	
	
	@Test public void testBindingNames_sharedBinding() throws Exception {    
	    getConnection(ADMIN, PROPS_FILE);
	    cleanDeploy();
	    
	    getAdmin().addConnectorBinding("BQT1 Oracle 9i Simple Cap", Util.getCharacterFile(UnitTestUtil.getTestDataPath()+"/admin/BQT1Binding.cdk"), new AdminOptions(AdminOptions.OnConflict.IGNORE)); //$NON-NLS-1$ //$NON-NLS-2$
	    
	    assertTrue("Binding must be available", hasBinding("BQT1 Oracle 9i Simple Cap")); //$NON-NLS-1$ //$NON-NLS-2$
	    
    	addVDB(BQT, UnitTestUtil.getTestDataPath()+"/admin/QT_Ora9DSwDEF.vdb"); //$NON-NLS-1$
	    
	    assertTrue("VDB does exist", hasVDB(BQT, "1")); //$NON-NLS-1$ //$NON-NLS-2$
	    
	    Collection bindings = getAdmin().getConnectorBindings(STAR);
	    int initialSize = bindings.size();
	    
	    assertTrue("Binding must be available", hasBinding("BQT1 Oracle 9i Simple Cap")); //$NON-NLS-1$ //$NON-NLS-2$
	    assertTrue("Binding must be available", hasBinding("BQT_1.BQT1 Oracle 9i Simple Cap"));  //$NON-NLS-1$ //$NON-NLS-2$
	    assertTrue("Binding must be available", hasBinding("BQT_1.BQT2 Oracle 9i Simple Cap")); //$NON-NLS-1$ //$NON-NLS-2$
	    
	    addVDB(BQT, UnitTestUtil.getTestDataPath()+"/admin/QT_Ora9DSwDEF.vdb"); //$NON-NLS-1$
	    
	    assertTrue("VDB does exist", hasVDB(BQT, "2")); //$NON-NLS-1$ //$NON-NLS-2$
	    
	    bindings = getAdmin().getConnectorBindings(STAR);
	    
	    assertTrue("Addtion of the same VDB should", bindings.size()==initialSize); //$NON-NLS-1$
	    
	    // the bindings are shared
	    assertTrue("Binding must be available", hasBinding("BQT1 Oracle 9i Simple Cap")); //$NON-NLS-1$ //$NON-NLS-2$
	    assertTrue("Binding must be available", hasBinding("BQT_1.BQT1 Oracle 9i Simple Cap"));  //$NON-NLS-1$ //$NON-NLS-2$
	    assertTrue("Binding must be available", hasBinding("BQT_1.BQT2 Oracle 9i Simple Cap")); //$NON-NLS-1$ //$NON-NLS-2$
	    
	    deleteVDB(BQT, "1"); //$NON-NLS-1$

	    //still bindings are there
	    assertTrue("Binding must be available", hasBinding("BQT1 Oracle 9i Simple Cap")); //$NON-NLS-1$ //$NON-NLS-2$
	    assertTrue("Binding must be available", hasBinding("BQT_1.BQT1 Oracle 9i Simple Cap"));  //$NON-NLS-1$ //$NON-NLS-2$
	    assertTrue("Binding must be available", hasBinding("BQT_1.BQT2 Oracle 9i Simple Cap")); //$NON-NLS-1$ //$NON-NLS-2$
	    
	    deleteVDB(BQT, "2"); //$NON-NLS-1$
	    
	    // deletion of VDB should remove the bindings specific to VDB? 
	    assertTrue("Binding must be available", hasBinding("BQT1 Oracle 9i Simple Cap")); //$NON-NLS-1$ //$NON-NLS-2$
	    assertFalse("Binding must be available", hasBinding("BQT_1.BQT1 Oracle 9i Simple Cap"));  //$NON-NLS-1$ //$NON-NLS-2$
	    assertFalse("Binding must be available", hasBinding("BQT_1.BQT2 Oracle 9i Simple Cap")); //$NON-NLS-1$ //$NON-NLS-2$

	    getAdmin().deleteConnectorBinding("BQT1 Oracle 9i Simple Cap"); //$NON-NLS-1$
	    assertFalse("Binding must not available", hasBinding("BQT1 Oracle 9i Simple Cap"));	     //$NON-NLS-1$ //$NON-NLS-2$
	    
	    closeConnection();    
	}



	@Test public void testStartStopConnectorBindings() throws Exception {
	    getConnection(ADMIN, PROPS_FILE);
	    cleanDeploy();
	    
    	addVDB(BQT, UnitTestUtil.getTestDataPath()+"/admin/QT_Ora9DSwDEF.vdb"); //$NON-NLS-1$
	    assertTrue("VDB does not exist", hasVDB(BQT, "1")); //$NON-NLS-1$ //$NON-NLS-2$

	    assertTrue("Binding must be available", hasBinding("BQT_1.BQT1 Oracle 9i Simple Cap"));  //$NON-NLS-1$ //$NON-NLS-2$
	    assertTrue("Binding must be available", hasBinding("BQT_1.BQT2 Oracle 9i Simple Cap")); //$NON-NLS-1$ //$NON-NLS-2$

	    pushConnection();
	    	getConnection(BQT, PROPS_FILE);
	        execute("SELECT * FROM BQT1.SmallA"); //$NON-NLS-1$
	        assertRowCount(50);        
	        execute("SELECT * FROM System.schemas"); //$NON-NLS-1$
	        assertRowCount(9);        	        
	        closeConnection();    
	    popConnection(); 

	    getAdmin().stopConnectorBinding("BQT_1.BQT1 Oracle 9i Simple Cap", true); //$NON-NLS-1$
	    //calling second time has no effect
	    getAdmin().stopConnectorBinding("BQT_1.BQT1 Oracle 9i Simple Cap", true); //$NON-NLS-1$
	    
	    try {
	    	pushConnection();
	    	getConnection(BQT, PROPS_FILE);
	        execute("SELECT * FROM BQT1.SmallA"); // this should fail //$NON-NLS-1$
	        fail("maust have failed, since the connector is not started."); //$NON-NLS-1$
	    }catch(Exception e) {
	        //pass
	    }finally {
	        closeConnection();    
	        popConnection();         
	    }
	    
	    // start the binding using un-qualified, should not start    
	    try {
	    	 getAdmin().startConnectorBinding("BQT1 Oracle 9i Simple Cap"); //$NON-NLS-1$
	        fail("must have failed to start"); //$NON-NLS-1$
	    }catch(Exception e) {
	        //pass
	    }
	    
	    // start correctly 
	    getAdmin().startConnectorBinding("BQT_1.BQT1 Oracle 9i Simple Cap"); //$NON-NLS-1$
	    // calling second time should have no effect
	    getAdmin().startConnectorBinding("BQT_1.BQT1 Oracle 9i Simple Cap"); //$NON-NLS-1$
	    
	    // test again
	    pushConnection();
	    	getConnection(BQT, PROPS_FILE);
	        execute("SELECT * FROM BQT1.SmallA"); //$NON-NLS-1$
	        assertRowCount(50);        
	        closeConnection();    
	    popConnection();  
	    	    
	    closeConnection();    
	}

	@Test public void testStartStopNonExistentConnectorBindings() throws Exception {
	    getConnection(ADMIN, PROPS_FILE);
	    cleanDeploy();
	    
	    addVDB(BQT, UnitTestUtil.getTestDataPath()+"/admin/QT_Ora9DSwDEF.vdb"); //$NON-NLS-1$
	    
	    try {
		    getAdmin().startConnectorBinding("fakeConnector"); //$NON-NLS-1$
		    fail("must have failed to start");//$NON-NLS-1$
	    }catch(Exception e) {
	    }
	    
	    try {
		    getAdmin().stopConnectorBinding("fakeConnector", true);//$NON-NLS-1$
		    fail("must have failed to stop");//$NON-NLS-1$
	    }catch(Exception e) {
	    }
	    
	    try {
	    	getAdmin().deleteConnectorBinding("fakeConnector"); //$NON-NLS-1$
	        fail("Must have failed delete a unknown connector"); //$NON-NLS-1$
	    }catch(AdminException e) {
	        //pass
	    }
	    
	    try {
	    	getAdmin().exportConnectorBinding("fakeConnector"); //$NON-NLS-1$
	        fail("Must have failed exporting a unknown connector"); //$NON-NLS-1$
	    }catch(AdminException e) {
	        //pass
	    }	    
	    
	    try {
	        getAdmin().assignBindingToModel("fakeConnector", BQT, "1", "BQT1"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	        fail("Must have failed assigning non-existing binding to the model"); //$NON-NLS-1$
	    }catch(AdminException e) {
	        //pass
	    }	    
	    
	    closeConnection();
	}
	
	@Test public void testConnectorTypes() throws Exception {
	    getConnection(ADMIN, PROPS_FILE);
	    cleanDeploy();
	    
	    addVDB(BQT, UnitTestUtil.getTestDataPath()+"/admin/QT_Ora9DSwDEF.vdb"); //$NON-NLS-1$
	    
	    Collection<ConnectorType> types = getAdmin().getConnectorTypes(STAR);
	    assertEquals("31 types expected", 31, types.size()); //$NON-NLS-1$
	    
	    
	    assertTrue("Should be available", hasConnectorType("Loopback Connector")); //$NON-NLS-1$ //$NON-NLS-2$
	    
	    getAdmin().deleteConnectorType("Loopback Connector"); //$NON-NLS-1$
	    
	    assertFalse("must have been deleted", hasConnectorType("Loopback Connector")); //$NON-NLS-1$ //$NON-NLS-2$
	    
	    try {
	    	getAdmin().deleteConnectorType("Oracle ANSI JDBC Connector"); //$NON-NLS-1$
	    	fail("must have failed as this type in use by BQT "); //$NON-NLS-1$
	    } catch(Exception e) {
	    	//pass
	    }
	    
	    try {
	    	getAdmin().deleteConnectorType("FakeConnector"); //$NON-NLS-1$
	    	fail("must have failed as this type as this is unknown"); //$NON-NLS-1$
	    } catch(Exception e) {
	    	//pass
	    }
	    
	    getAdmin().addConnectorType("Loopback Connector", Util.getCharacterFile(UnitTestUtil.getTestDataPath()+"/admin/loopback.cdk")); //$NON-NLS-1$ //$NON-NLS-2$
	    assertTrue("Should be available", hasConnectorType("Loopback Connector")); //$NON-NLS-1$ //$NON-NLS-2$
	    
		try {
		    getAdmin().addConnectorType("Loopback Connector", Util.getCharacterFile(UnitTestUtil.getTestScratchPath()+"/loopback.cdk")); //$NON-NLS-1$ //$NON-NLS-2$
		    fail("must have fail to add existing type"); //$NON-NLS-1$
		} catch(Exception e) {
			//pass
		}
	    
		
		// try exporting
	    Util.writeToFile(UnitTestUtil.getTestScratchPath()+"/textfileexport.cdk",getAdmin().exportConnectorType("Text File Connector")); //$NON-NLS-1$ //$NON-NLS-2$
	    File cdkFile = new File(UnitTestUtil.getTestScratchPath()+"/textfileexport.cdk"); //$NON-NLS-1$
	    assertTrue("Connector type Export should exist", cdkFile.exists()); //$NON-NLS-1$
	    cdkFile.delete();		
		
	    closeConnection();
	}
	
	@Test public void testUpdateConnectorBindingProperty() throws Exception {
	    getConnection(ADMIN, PROPS_FILE);
	    cleanDeploy();
	    
    	addVDB(BQT, UnitTestUtil.getTestDataPath()+"/admin/QT_Ora9DSwDEF.vdb"); //$NON-NLS-1$
	    assertTrue("VDB does not exist", hasVDB(BQT, "1")); //$NON-NLS-1$ //$NON-NLS-2$
	    
	    pushConnection();
	    getConnection(BQT, PROPS_FILE);
	    execute("select * from BQT1.smalla"); //$NON-NLS-1$
	    assertRowCount(50);
	    closeConnection();
	    popConnection();	
	    
	    Collection<ConnectorBinding> bindings = getAdmin().getConnectorBindings("BQT_1.BQT1 Oracle 9i Simple Cap"); //$NON-NLS-1$
	    for (ConnectorBinding binding:bindings) {
	        getAdmin().setConnectorBindingProperty(binding.getIdentifier(), "RowCount", "10"); //$NON-NLS-1$ //$NON-NLS-2$
	        getAdmin().stopConnectorBinding(binding.getIdentifier(), true);
	        getAdmin().startConnectorBinding(binding.getIdentifier());
	    }	  
	    
	    pushConnection();
	    getConnection(BQT, PROPS_FILE);
	    execute("select * from BQT1.smalla"); //$NON-NLS-1$
	    assertRowCount(10);
	    closeConnection();
	    popConnection();
	    
	    // changes are persistent after the restart too.
	    getAdmin().restart();
	    
	    closeConnection();
	    
	    getConnection(BQT, PROPS_FILE);
	    execute("select * from BQT1.smalla"); //$NON-NLS-1$
	    assertRowCount(10);
	    closeConnection();	    
	    
	}

	@Test public void testVDBAddsConnectorTypes() throws Exception {
	    getConnection(ADMIN, PROPS_FILE);
	    cleanDeploy();
	    
	    assertFalse(hasVDB(BQT));
	    
	    if (hasConnectorType("Oracle ANSI JDBC Connector")) { //$NON-NLS-1$
	    	getAdmin().deleteConnectorType("Oracle ANSI JDBC Connector"); //$NON-NLS-1$
	    }
	    assertFalse("binding must exist", hasConnectorType("Oracle ANSI JDBC Connector")); //$NON-NLS-1$ //$NON-NLS-2$
	    
	    addVDB(BQT, UnitTestUtil.getTestDataPath()+"/admin/QT_Ora9DSwDEF.vdb"); //$NON-NLS-1$
	    assertTrue("VDB does exist", hasVDB(BQT, "1")); //$NON-NLS-1$ //$NON-NLS-2$
	    
	    assertTrue("connector type must exist", hasConnectorType("Oracle ANSI JDBC Connector")); //$NON-NLS-1$ //$NON-NLS-2$
	    deleteVDB(BQT, "1"); //$NON-NLS-1$
	    closeConnection();
	}

	@Test public void testAdminOptions_addvdb_conflict_ignore() throws Exception {    
		getConnection(ADMIN, PROPS_FILE);
		cleanDeploy();
		
	    // we start out with no bindings
	    assertFalse("VDB does not exist", hasVDB("Empty"));         //$NON-NLS-1$ //$NON-NLS-2$
	    
    	getAdmin().addConnectorBinding("Loopback", Util.getCharacterFile(UnitTestUtil.getTestDataPath()+"/admin/loopback.cdk"), new AdminOptions(AdminOptions.OnConflict.IGNORE)); //$NON-NLS-1$ //$NON-NLS-2$
	    assertTrue("Binding must be available", hasBinding("Loopback")); //$NON-NLS-1$ //$NON-NLS-2$
	    
	    // we add a vdb, loop back which returns 10 rows
	    addVDB("Empty", UnitTestUtil.getTestDataPath()+"/admin/TestEmpty.vdb"); //$NON-NLS-1$ //$NON-NLS-2$
	    assertTrue("VDB does not exist", hasVDB("Empty", "1")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	    
	    pushConnection();
	        getConnection("Empty", PROPS_FILE); //$NON-NLS-1$
	        execute("SELECT * FROM Oracle.SmallA"); //$NON-NLS-1$
	        assertRowCount(1);        
	        closeConnection();    
	    popConnection();  
	        
	    getAdmin().restart();

	    closeConnection();
	    
        getConnection("Empty", PROPS_FILE); //$NON-NLS-1$
        execute("SELECT * FROM Oracle.SmallA"); //$NON-NLS-1$
        assertRowCount(1);        
        closeConnection();    
	    
	}


	@Test public void testAdminOptions_addvdb_conflict_overwrite() throws Exception {    
		getConnection(ADMIN, PROPS_FILE);
		cleanDeploy();
		
	    // we start out with no bindings
	    assertFalse("VDB does not exist", hasVDB("Empty"));         //$NON-NLS-1$ //$NON-NLS-2$
	    
    	getAdmin().addConnectorBinding("Loopback", Util.getCharacterFile(UnitTestUtil.getTestDataPath()+"/admin/loopback.cdk"), new AdminOptions(AdminOptions.OnConflict.IGNORE)); //$NON-NLS-1$ //$NON-NLS-2$
	    assertTrue("Binding must be available", hasBinding("Loopback")); //$NON-NLS-1$ //$NON-NLS-2$
	    
	    // we add a vdb, loop back which returns 10 rows
	    getAdmin().addVDB("Empty", Util.getBinaryFile(UnitTestUtil.getTestDataPath()+"/admin/TestEmpty.vdb"), new AdminOptions(AdminOptions.OnConflict.OVERWRITE)); //$NON-NLS-1$ //$NON-NLS-2$
	    assertTrue("VDB does not exist", hasVDB("Empty", "1")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	    	    
	    pushConnection();
        getConnection("Empty", PROPS_FILE); //$NON-NLS-1$
        execute("SELECT * FROM Oracle.SmallA"); //$NON-NLS-1$
        assertRowCount(10);        
        closeConnection();    
	    popConnection();  
	      
	    getAdmin().restart();

	    pushConnection();
        getConnection("Empty", PROPS_FILE); //$NON-NLS-1$
        execute("SELECT * FROM Oracle.SmallA"); //$NON-NLS-1$
        assertRowCount(10);        
        closeConnection();    
        popConnection();  
	}


	@Test public void testAdminOptions_addvdb_conflict_Exception() throws Exception {
		getConnection(ADMIN, PROPS_FILE);
		cleanDeploy();
		
	    // we start out with no bindings
	    assertFalse("VDB does not exist", hasVDB("Empty"));         //$NON-NLS-1$ //$NON-NLS-2$
	    
    	getAdmin().addConnectorBinding("Loopback", Util.getCharacterFile(UnitTestUtil.getTestDataPath()+"/admin/loopback.cdk"), new AdminOptions(AdminOptions.OnConflict.IGNORE)); //$NON-NLS-1$ //$NON-NLS-2$
	    assertTrue("Binding must be available", hasBinding("Loopback")); //$NON-NLS-1$ //$NON-NLS-2$
	    
	    assertFalse("VDB does not exist", hasVDB("Empty", "1")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	    
	    // we add a vdb, loop back which returns 10 rows
	    try {
		    // we add a vdb, loop back which returns 10 rows
		    getAdmin().addVDB("Empty", Util.getBinaryFile(UnitTestUtil.getTestDataPath()+"/admin/TestEmpty.vdb"), new AdminOptions(AdminOptions.OnConflict.EXCEPTION));	    	 //$NON-NLS-1$ //$NON-NLS-2$
	        fail("Must have failed to Add a vdb, as loopback already exists"); //$NON-NLS-1$
	    }catch(AdminException e) {
	        // pass        
	    }
	    
	    assertFalse("VDB does not exist", hasVDB("Empty", "1"));     //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	    closeConnection();
	}
	
	@Test public void testGetSessions() throws Exception {
		getConnection(ADMIN, PROPS_FILE);
		cleanDeploy();
		addVDB(BQT, UnitTestUtil.getTestDataPath()+"/admin/QT_Ora9DSwDEF.vdb"); //$NON-NLS-1$
		
		assertEquals(1, getAdmin().getSessions(STAR).size());
		
		for (int i = 0; i < 10; i++) {
			pushConnection();
			getConnection(BQT, PROPS_FILE);			
		}
		
		assertEquals(11, getAdmin().getSessions(STAR).size());
		
		for (int i = 0; i < 10; i++) {			
			closeConnection();
			popConnection();
		}
		
		assertEquals(1, getAdmin().getSessions(STAR).size());

		closeConnection();
	}
	
	@Test public void testSessionTermination() throws Exception {
		com.metamatrix.jdbc.api.Connection c = getConnection(ADMIN, PROPS_FILE);
		cleanDeploy();
		addVDB(BQT, UnitTestUtil.getTestDataPath()+"/admin/QT_Ora9DSwDEF.vdb"); //$NON-NLS-1$
		
		assertEquals(1, getAdmin().getSessions(STAR).size());
		
		pushConnection();
		MMConnection c2 = (MMConnection)getConnection(BQT, PROPS_FILE);
        execute("SELECT * FROM BQT1.SmallA"); //$NON-NLS-1$
        assertRowCount(50);        
        
        // terminate the session
        c.getAdminAPI().terminateSession(c2.getConnectionId());
        
        // make sure it is gone
        assertEquals(1, c.getAdminAPI().getSessions(STAR).size());
        
        try {
	        execute("SELECT * FROM BQT1.SmallA"); //$NON-NLS-1$
	        fail("must have failed to execute"); //$NON-NLS-1$
        }catch(Exception e) {
        	// success
        	try {
				closeConnection();
			} catch (Exception e1) {
			}
        }
        
		popConnection();
		closeConnection();
	}
	
	@Test public void testAddExtensionModules() throws Exception {
		getConnection(ADMIN, PROPS_FILE);
		cleanDeploy();
		
		assertFalse(hasExtensionModule("Loopy.jar")); //$NON-NLS-1$
		
	    getAdmin().addExtensionModule("JAR File", "Loopy.jar", Util.getBinaryFile(UnitTestUtil.getTestDataPath()+"/admin/loopbackconn.jar"), "JAR File"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
	    
	    assertTrue("Expected extension module", hasExtensionModule("Loopy.jar")); //$NON-NLS-1$ //$NON-NLS-2$

	    // try to add a duplicate
	    try {
	    	getAdmin().addExtensionModule("JAR File", "Loopy.jar", Util.getBinaryFile(UnitTestUtil.getTestDataPath()+"/admin/loopbackconn.jar"), "JAR File"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
	    	fail("should have failed to add the extension module as it is duplicate"); //$NON-NLS-1$
	    } catch(Exception e) {
	    	//pass
	    }
	    
	    //export the extension
	    Util.writeToFile(UnitTestUtil.getTestScratchPath()+"/ext.jar", getAdmin().exportExtensionModule("Loopy.jar")); //$NON-NLS-1$ //$NON-NLS-2$
	    File f = new File(UnitTestUtil.getTestScratchPath()+"/ext.jar"); //$NON-NLS-1$
	    assertTrue(f.exists());
	    f.delete();
	    
	    // delete the extension
	    getAdmin().deleteExtensionModule("Loopy.jar"); //$NON-NLS-1$
	    
	    // Assert that no vdb with this name exists
	    assertFalse("Expected no prior extension module", hasExtensionModule("Loopy.jar")); //$NON-NLS-1$ //$NON-NLS-2$
		
		closeConnection();
	}	

	@Test public void testExportConfiguration() throws Exception {
		getConnection(ADMIN, PROPS_FILE);
		cleanDeploy();

		Util.writeToFile(UnitTestUtil.getTestScratchPath()+"/serverconfigexport.properties", getAdmin().exportConfiguration()); //$NON-NLS-1$
	    File f = new File(UnitTestUtil.getTestScratchPath()+"/serverconfigexport.properties"); //$NON-NLS-1$
	    assertTrue("Exported configuration must exist", f.exists()); //$NON-NLS-1$
	    //f.delete();
	    closeConnection();
	}
	
	// Test exporting VDB and adding the same VDB and make sure it exported correctly
	@Test public void testExportVDB() throws Exception {
		getConnection(ADMIN, PROPS_FILE);
		cleanDeploy();
		
	    // Assert that no vdb with this name exists
	    assertFalse("Expected no prior VDB", hasVDB(BQT)); //$NON-NLS-1$
	    
	    // Add .VDB with included .DEF
	    addVDB(BQT, UnitTestUtil.getTestDataPath()+"/admin/QT_Ora9DSwDEF.vdb"); //$NON-NLS-1$
	    
	    // Check that config state has changed
	    assertTrue("Expected new VDB", hasVDB(BQT));     //$NON-NLS-1$
	    
	    // Export the VDB
	    Util.writeToFile(UnitTestUtil.getTestScratchPath()+"/bqtexport.vdb", getAdmin().exportVDB(BQT, "1")); //$NON-NLS-1$ //$NON-NLS-2$
	    
	    closeConnection();
	           
	    // Assert bqt.vdb exists
	    File f = new File(UnitTestUtil.getTestScratchPath()+"/bqtexport.vdb"); //$NON-NLS-1$
	    assertTrue(f.exists());
	    f.delete();
	}	
	
	@Test public void testVDBModified() throws Exception {
		getConnection(ADMIN, PROPS_FILE);
		cleanDeploy();
	              
		addVDB(BQT, UnitTestUtil.getTestDataPath()+"/admin/QT_Ora9DSwDEF.vdb"); //$NON-NLS-1$
		
	    // make susre it exists as file
	    File vdb = new File(UnitTestUtil.getTestScratchPath()+"/adminapi/deploy/BQT_1.vdb"); //$NON-NLS-1$
	    assertTrue("Persisted VDB file does not exists", vdb.exists()); //$NON-NLS-1$
	    
	    // Check when it last modified
	    long modified = vdb.lastModified();
	    Thread.sleep(1000);
	    getAdmin().assignBindingToModel("BQT_1.BQT1 Oracle 9i Simple Cap", BQT, "1", "BQT2"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	     
	    // the deployed VDB is modified not the original vdb
	    assertTrue(vdb.lastModified() == modified);

	    closeConnection();
	}
	
	@Test public void testIncompleteVDB() throws Exception {  
		getConnection(ADMIN, PROPS_FILE);
		cleanDeploy();
	              
		VDB vdb = getAdmin().addVDB(BQT, Util.getBinaryFile(UnitTestUtil.getTestDataPath()+"/admin/Empty.vdb"), new AdminOptions (AdminOptions.OnConflict.IGNORE)); //$NON-NLS-1$
		
		assertTrue("Status must have been INCOMPLTE", !(vdb.getState() == VDB.ACTIVE)); //$NON-NLS-1$
		
		try {
			getAdmin().changeVDBStatus(BQT, "1", VDB.ACTIVE); //$NON-NLS-1$
			fail("must have failed to set status to active"); //$NON-NLS-1$
		} catch(Exception e) {
			
		}
		
		pushConnection();
		try {
			getConnection(BQT, PROPS_FILE);
			fail("must have failed to get connection"); //$NON-NLS-1$
		}catch(Exception e) {
			
		}
		popConnection();
		
	    getAdmin().addConnectorBinding("Loopback", Util.getCharacterFile(UnitTestUtil.getTestDataPath()+"/admin/loopback.cdk"), new AdminOptions(AdminOptions.OnConflict.OVERWRITE)); //$NON-NLS-1$ //$NON-NLS-2$
	    getAdmin().startConnectorBinding("Loopback");     //$NON-NLS-1$
	    getAdmin().assignBindingToModel("Loopback", BQT, "1", "Oracle"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	    
	    getAdmin().changeVDBStatus(BQT, "1", VDB.ACTIVE);  	 //$NON-NLS-1$
	    
	    pushConnection();
	    getConnection(BQT, PROPS_FILE);
	    closeConnection();
	    popConnection();
	    
		closeConnection();
	}
	
	@Test public void testUseLatestVersion() throws Exception {
		getConnection(ADMIN, PROPS_FILE);
		cleanDeploy();
	              
		// this returns 50 rows
		addVDB(BQT, UnitTestUtil.getTestDataPath()+"/admin/QT_Ora9DSwDEF.vdb"); //$NON-NLS-1$

		// this returns 10 row
		addVDB(BQT, UnitTestUtil.getTestDataPath()+"/admin/TestEmpty.vdb"); //$NON-NLS-1$

	    pushConnection();
	    getConnection(BQT, PROPS_FILE);
	    execute("select * from Oracle.smalla"); //$NON-NLS-1$
	    assertRowCount(10);
	    closeConnection();
	    popConnection();	    
	    closeConnection();
	}	
	
	@Test public void testDeleteVDBHavingActiveConnection() throws Exception {
		helpDeleteVDBHavingActiveConnection(false);
		helpDeleteVDBHavingActiveConnection(true);
	}

	// Test to delete a VDB with active connection to it
	// 1) we should be able to delete the vdb; hasVDB should return false 
	// 2) should be able to continue the old connection, as it is 
	// 3) No new connections are allowed
	// 4) if you add another VDB with same name, it can be given different version
	// 5) The old one can not switch over to the new connection.
	 void helpDeleteVDBHavingActiveConnection(boolean terminate) throws Exception {
		getConnection(ADMIN, PROPS_FILE);
		cleanDeploy();
	              
		// this returns 50 rows
		addVDB(BQT, UnitTestUtil.getTestDataPath()+"/admin/QT_Ora9DSwDEF.vdb"); //$NON-NLS-1$
	    
	    assertTrue("VDB does not exist", hasVDB(BQT)); //$NON-NLS-1$
	    
	    // make another context and create a 2nd connection
	    pushConnection();
	    	MMConnection vdbConn = (MMConnection)getConnection("bqt", PROPS_FILE);
	        execute("SELECT * FROM BQT1.SmallA"); //$NON-NLS-1$
	        assertRowCount(50);        
	        // we have not closed the connection here, kept active connection
	    popConnection();
	    
	    // testcase (1) (here the file should still exist; but report as non existent from API)
	    deleteVDB(BQT, "1"); //$NON-NLS-1$
	    assertFalse("VDB should exist because we still have an active user", hasVDB(BQT, "1")); //$NON-NLS-1$ //$NON-NLS-2$
	    File f = new File(UnitTestUtil.getTestScratchPath()+"/adminapi/deploy/BQT_1.vdb"); //$NON-NLS-1$
	    assertTrue("since the connection is still open this file should exist", f.exists()); //$NON-NLS-1$
	        
	    // testcase (2)
	    // switch to 2nd connection and try to execute, since we did not close it should work fine
	    pushConnection();
	    setConnection(vdbConn);
	    execute("SELECT * FROM BQT1.SmallA"); //$NON-NLS-1$
	    assertRowCount(50);        
	    popConnection();
	    
	    // testcase (3)
	    // try to make third connection and should fail, because we do not allow connections
	    // to non-active vdbs
	    pushConnection();
	        try {
	        	getConnection(BQT, PROPS_FILE);
	            fail("Must have failed to connect to the VDB as it is deleted"); //$NON-NLS-1$
	        }catch(Exception e) {
	            // yes failed to connect
	        }
	    popConnection();    
	    
	    // testcase (4)
	    // Add the VDB again to the configuration it should give new version
	    getAdmin().addVDB(BQT, Util.getBinaryFile(UnitTestUtil.getTestDataPath()+"/admin/QT_Ora9DSwDEF.vdb"), new AdminOptions(AdminOptions.OnConflict.IGNORE));     //$NON-NLS-1$
	    assertTrue("Empty VDB must exist", hasVDB(BQT, "2")); //$NON-NLS-1$ //$NON-NLS-2$
	    
	    // testcase (5)
	    // try to execute the 2nd connection, it should still go after the deleted vdb
	    // as we keep until this connections goes dead.
	    pushConnection();
	    setConnection(vdbConn);
	    execute("SELECT * FROM BQT1.SmallA"); //$NON-NLS-1$
	    assertRowCount(50);        
	    popConnection();

	    // now make a 4th new connection this should work
	    pushConnection();
	    	getConnection(BQT, PROPS_FILE+";version=2"); //$NON-NLS-1$
	        execute("SELECT * FROM BQT1.SmallA"); //$NON-NLS-1$
	        assertRowCount(50);        
	        closeConnection();
	    popConnection();	    
	    
	    if (terminate) {
	    	getAdmin().terminateSession(vdbConn.getConnectionId());
	    }
	    else {
		    // clean up 2nd connection
		    pushConnection();
		    setConnection(vdbConn);
		        closeConnection();
		        assertFalse("The vdb must have deleted as the connection closed", f.exists()); //$NON-NLS-1$
		    popConnection();
	    }
	    
	    // now make a 5th new connection this should work
	    pushConnection();
	    	getConnection(BQT, PROPS_FILE);
	        execute("SELECT * FROM BQT1.SmallA"); //$NON-NLS-1$
	        assertRowCount(50);        
	        closeConnection();
	    popConnection();

	    // here it should be gone we call delete
	    deleteVDB(BQT, "2"); //$NON-NLS-1$
	    assertFalse("VDB should exist because we still have an active user", hasVDB(BQT, "2")); //$NON-NLS-1$ //$NON-NLS-2$
	    f = new File(UnitTestUtil.getTestScratchPath()+"/adminapi/deploy/BQT_2.vdb"); //$NON-NLS-1$
	    assertFalse("since the connection is still open this file should exist", f.exists()); //$NON-NLS-1$
	    
	    // close the 1st connection
	    closeConnection();
	}	

	private void helpConnectorBindingAddTest(int option, int rows) throws Exception {
		getConnection(ADMIN, PROPS_FILE);
		cleanDeploy();
	              
		// this returns 50 rows
		addVDB(BQT, UnitTestUtil.getTestDataPath()+"/admin/TestEmpty.vdb"); //$NON-NLS-1$
		
		getAdmin().addConnectorBinding("Loopback", Util.getCharacterFile(UnitTestUtil.getTestDataPath()+"/admin/loopback.cdk"), new AdminOptions(option)); //$NON-NLS-1$ //$NON-NLS-2$
		
	    pushConnection();
    	getConnection(BQT, PROPS_FILE);
        execute("SELECT * FROM Oracle.SmallA"); //$NON-NLS-1$
        assertRowCount(rows);        
        closeConnection();
        popConnection();        
	}
	
	@Test public void testConnectorBindingAdd_optionIgnore() throws Exception {
		helpConnectorBindingAddTest(AdminOptions.OnConflict.IGNORE, 10);
	}

	@Test public void testConnectorBindingAdd_optionException() throws Exception {
		try {
			helpConnectorBindingAddTest(AdminOptions.OnConflict.EXCEPTION, 10);
			fail("Must have failed to add connector"); //$NON-NLS-1$
		} catch(Exception e) {
			
		}
	}
	
	@Test public void testConnectorBindingAdd_optionOverwrite() throws Exception {
		helpConnectorBindingAddTest(AdminOptions.OnConflict.OVERWRITE, 1);
	}
	
	@Test public void testAddConnectorBindingWithProperties() throws Exception {
		getConnection(ADMIN, PROPS_FILE);
		cleanDeploy();
	              
		// this returns 50 rows
		addVDB(BQT, UnitTestUtil.getTestDataPath()+"/admin/TestEmpty.vdb"); //$NON-NLS-1$
	    
	    Properties props = new Properties();
	        
	    props.setProperty("MaxResultRows","10000"); //$NON-NLS-1$ //$NON-NLS-2$
	    props.setProperty("ConnectorThreadTTL", "120000"); //$NON-NLS-1$ //$NON-NLS-2$
	    props.setProperty("ConnectorMaxThreads","5"); //$NON-NLS-1$ //$NON-NLS-2$
	    props.setProperty("metamatrix.service.essentialservice", "false"); //$NON-NLS-1$ //$NON-NLS-2$
	    props.setProperty("ConnectorClassPath","extensionjar:loopbackconn.jar;extensionjar:jdbcconn.jar"); //$NON-NLS-1$ //$NON-NLS-2$
	    props.setProperty("RowCount","12"); //$NON-NLS-1$ //$NON-NLS-2$
	    props.setProperty("ServiceClassName","com.metamatrix.server.connector.service.ConnectorService"); //$NON-NLS-1$ //$NON-NLS-2$
	    props.setProperty("ConnectorClass","com.metamatrix.connector.loopback.LoopbackConnector"); //$NON-NLS-1$ //$NON-NLS-2$
	    props.setProperty("WaitTime", "0");     //$NON-NLS-1$ //$NON-NLS-2$

	    // Add the connector binding
	    getAdmin().addConnectorBinding("Loopy", "Loopback Connector", props, new AdminOptions(AdminOptions.OnConflict.OVERWRITE)); //$NON-NLS-1$ //$NON-NLS-2$
	    
	    assertTrue("Connector binding must exist", hasBinding("Loopy")); //$NON-NLS-1$ //$NON-NLS-2$
	        
	    getAdmin().assignBindingToModel("Loopy", "BQT", "1", "Oracle"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
	    getAdmin().startConnectorBinding("Loopy"); //$NON-NLS-1$
	    
	    pushConnection();
    	getConnection(BQT, PROPS_FILE);
        execute("SELECT * FROM Oracle.SmallA"); //$NON-NLS-1$
        assertRowCount(12);        
        closeConnection();
        popConnection();        
	    
	    closeConnection();    
	}
	
	@Test public void testConnectorArchive() throws Exception {
		getConnection(ADMIN, PROPS_FILE);
		cleanDeploy();
		
		// this returns 50 rows
		addVDB(BQT, UnitTestUtil.getTestDataPath()+"/admin/QT_Ora9DSwDEF.vdb"); //$NON-NLS-1$

		assertFalse(hasConnectorType("Loopback")); //$NON-NLS-1$
		assertFalse(hasExtensionModule("jdbcconn.jar")); //$NON-NLS-1$
		assertFalse(hasExtensionModule("loopbackconn.jar")); //$NON-NLS-1$

		getAdmin().addConnectorArchive(Util.getBinaryFile(UnitTestUtil.getTestDataPath()+"/admin/loopback_archive.caf"), new AdminOptions(AdminOptions.OnConflict.IGNORE)); //$NON-NLS-1$

		assertTrue(hasConnectorType("Loopback")); //$NON-NLS-1$
		assertTrue(hasExtensionModule("jdbcconn.jar")); //$NON-NLS-1$
		assertTrue(hasExtensionModule("loopbackconn.jar")); //$NON-NLS-1$
		
		File f = new File(UnitTestUtil.getTestScratchPath()+"/loopback_archive.caf"); //$NON-NLS-1$
		Util.writeToFile(f.getCanonicalPath(), getAdmin().exportConnectorArchive("Loopback")); //$NON-NLS-1$
		assertTrue(f.exists());
		f.delete();
		
		getAdmin().deleteExtensionModule("jdbcconn.jar"); //$NON-NLS-1$
		getAdmin().deleteExtensionModule("loopbackconn.jar"); //$NON-NLS-1$
		getAdmin().deleteConnectorType("Loopback"); //$NON-NLS-1$
		
		assertFalse(hasConnectorType("Loopback")); //$NON-NLS-1$
		assertFalse(hasExtensionModule("jdbcconn.jar")); //$NON-NLS-1$
		assertFalse(hasExtensionModule("loopbackconn.jar")); //$NON-NLS-1$
		
	    closeConnection();    
	}
	
	public class MyLogger implements EmbeddedLogger{
	    StringBuffer sb = new StringBuffer();
	    public void log(int logLevel, long timestamp, String componentName, String threadName, String message, Throwable throwable) {
	        sb.append("logLevel=").append(logLevel); //$NON-NLS-1$
	        sb.append("message=").append(message); //$NON-NLS-1$
	    }
	    
	    public String getLog() {
	        return sb.toString();
	    }
	}

	@Test public void testLogListener() throws Exception {
		getConnection(ADMIN, PROPS_FILE);
		cleanDeploy();
		

		MyLogger log = new MyLogger();
	    
		getAdmin().setLogListener(log);
		//force a critical log
		LogConfiguration c = getAdmin().getLogConfiguration();
		for(String context:c.getContexts()) {
			c.setLogLevel(context, LogConfiguration.DETAIL);
		}
		getAdmin().setLogConfiguration(c);

		// this returns 50 rows
		addVDB(BQT, UnitTestUtil.getTestDataPath()+"/admin/QT_Ora9DSwDEF.vdb"); //$NON-NLS-1$
			            
	    assertTrue("Log should not be empty", log.getLog().length() > 0); //$NON-NLS-1$
	    closeConnection();
	}	
	
	@Test public void testPropertyDefinitions() throws Exception{
		getConnection(ADMIN, PROPS_FILE);
		cleanDeploy();

		Collection<ConnectorType> c = getAdmin().getConnectorTypes("Oracle Connector"); //$NON-NLS-1$
		Collection<PropertyDefinition> defs = getAdmin().getConnectorTypePropertyDefinitions(c.iterator().next().getIdentifier());
		
		for (PropertyDefinition d:defs) {
			if (d.getName().equalsIgnoreCase("URL")) { //$NON-NLS-1$ 
				assertTrue(d.isRequired());
				assertTrue(d.getDefaultValue() != null);
				assertEquals("JDBC URL", d.getDisplayName()); //$NON-NLS-1$
				assertEquals("String", d.getPropertyType()); //$NON-NLS-1$
				assertEquals("java.lang.String", d.getPropertyTypeClassName()); //$NON-NLS-1$
				assertTrue(d.getAllowedValues().isEmpty());
				assertTrue(!d.isMasked());
			}			
			
			if (d.getName().equalsIgnoreCase("Password")) { //$NON-NLS-1$
				assertTrue(d.isMasked());
			}
		}
		
	    closeConnection();
	}
	
	@Test public void testGetRolesForGroup() throws Exception {
		getConnection(ADMIN, MEMBERSHIP_PROPS_FILE, ";user=admin;password=teiid"); //$NON-NLS-1$
		cleanDeploy();

		Collection<Role> r = getAdmin().getRolesForGroup("group1@file");//$NON-NLS-1$
		assertTrue(!r.isEmpty());
		assertEquals(Role.ADMIN_SYSTEM, r.iterator().next().getIdentifier());

		try {
			r = getAdmin().getRolesForGroup("unknown");//$NON-NLS-1$
			fail("must have failed to fetch the group"); //$NON-NLS-1$
		}catch(Exception e) {
			
		}
		assertTrue(!r.isEmpty());
		
		closeConnection();	    		
	}

	@Test public void testGetGroupsForUser() throws Exception {
		getConnection(ADMIN, MEMBERSHIP_PROPS_FILE, ";user=admin;password=teiid"); //$NON-NLS-1$
		cleanDeploy();

		Collection<Group> g = getAdmin().getGroupsForUser("paul@file"); //$NON-NLS-1$
		assertEquals(2, g.size());
		Iterator<Group> i = g.iterator();
		assertEquals("group4@file", i.next().getIdentifier()); //$NON-NLS-1$
		assertEquals("group1@file", i.next().getIdentifier()); //$NON-NLS-1$

		// with out the @file 
		g = getAdmin().getGroupsForUser("paul"); //$NON-NLS-1$
		assertEquals(2, g.size());

		try {
			g = getAdmin().getGroupsForUser("unknown");//$NON-NLS-1$
			fail("should failed to resolve the unknown user"); //$NON-NLS-1$
		}catch(Exception e) {
		}
		
		closeConnection();	    		
	}	
	
	@Test public void testGetGroups() throws Exception {
		getConnection(ADMIN, MEMBERSHIP_PROPS_FILE, ";user=admin;password=teiid"); //$NON-NLS-1$
		cleanDeploy();

		Collection<Group> g = getAdmin().getGroups("*"); //$NON-NLS-1$
		assertEquals(4, g.size());
		
		closeConnection();	    		
	}	
	
	@Test public void testGetDomainNames() throws Exception {
		getConnection(ADMIN, MEMBERSHIP_PROPS_FILE, ";user=admin;password=teiid"); //$NON-NLS-1$
		cleanDeploy();

		List<String> g = getAdmin().getDomainNames();
		assertEquals(1, g.size());
		assertEquals("file", g.get(0)); //$NON-NLS-1$
		closeConnection();	    		
	}		

	@Test public void testGetGroupsForDomain() throws Exception {
		getConnection(ADMIN, MEMBERSHIP_PROPS_FILE, ";user=admin;password=teiid"); //$NON-NLS-1$
		cleanDeploy();

		Collection<Group> g = getAdmin().getGroupsForDomain("file"); //$NON-NLS-1$
		assertEquals(4, g.size());

		g = getAdmin().getGroupsForDomain("unknown"); //$NON-NLS-1$
		assertEquals(0, g.size());
		
		closeConnection();	    		
	}		

	
	
	VDB addVDB(String name, String vdbFile) {
	    try {
			return getAdmin().addVDB(name, Util.getBinaryFile(vdbFile), new AdminOptions(AdminOptions.OnConflict.IGNORE));
		} catch (Exception e) {
			e.printStackTrace();
			throw new MetaMatrixRuntimeException();
		}
	}

	/**
	 * Checks to make sure the given VDB exists in the system
	 * @param vdbName - name of the VDB
	 * @return boolean - true if exists; false otherwise
	 */
	boolean hasVDB(String vdbName) {
	    try {
			Collection<VDB> vdbs = getAdmin().getVDBs(vdbName);
			for (VDB vdb:vdbs) {
			    if (vdb.getName().equals(vdbName)) {
			        return true;
			    }
			}
		} catch (AdminException e) {
		}
	    return false;
	}

	/**
	 * Checks to make sure the given VDB with version exists in the system
	 * @param vdbName - name of the VDB
	 * @param version - version of the VDB
	 * @return boolean - true if exists; false otherwise
	 */
	boolean hasVDB(String vdbName, String version) {
	    try {
			Collection<VDB> vdbs = getAdmin().getVDBs(vdbName);
			for (VDB vdb:vdbs) {
			    if (vdb.getName().equals(vdbName) && vdb.getVDBVersion().equals(version)) {
			        return true;
			    }
			}
		} catch (AdminException e) {
		}
	    return false;
	}

	/**
	 * Checks to make sure the given binging exists.
	 * @param bindingName - Name of the Binding.
	 * @return boolean - true if exists; false otherwise
	 */
	boolean hasBinding(String bindingName) {
	    try {
			Collection<ConnectorBinding> bindings = getAdmin().getConnectorBindings(AdminObject.WILDCARD + AdminObject.DELIMITER + bindingName);
			for (ConnectorBinding binding:bindings) {
			    if (binding.getName().equals(bindingName)) {
			        return true;
			    }        
			}
		} catch (AdminException e) {
			e.printStackTrace();
		}            
	    return false;
	}

	/**
	 * Checks if given Connector Type exists in system
	 * @param typeName - Binding type name
	 * @return boolean - true if exists; false otherwise
	 */
	boolean hasConnectorType(String typeName) {
		try {
			Collection<ConnectorType> types = getAdmin().getConnectorTypes(typeName);
			for (ConnectorType type:types) {
			    if (type.getName().equals(typeName)) {
			        return true;
			    }
			}
		} catch (AdminException e) {
		}
	    return false;
	}

	/**
	 * Checks if given Extension Module exists in system
	 * @param name - Extension Module name
	 * @return boolean - true if exists; false otherwise
	 */
	boolean hasExtensionModule(String name) {
	    try {
	        Collection<ExtensionModule> modules = getAdmin().getExtensionModules(name);
	        for(ExtensionModule module:modules) {
	            if (module.getName().equals(name)) {
	                return true;
	            }
	        }
	    }catch(Exception e) {}
	    return false;
	}
	
	void deleteVDB(String name, String version) {
	    try {
			getAdmin().changeVDBStatus(name, version, VDB.DELETED);
		} catch (AdminException e) {
			throw new MetaMatrixRuntimeException();
		}
	}	
}
