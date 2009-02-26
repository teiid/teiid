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

package com.metamatrix.admin.server;
import java.io.File;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.admin.api.exception.AdminException;
import com.metamatrix.admin.api.objects.Host;
import com.metamatrix.core.util.ObjectConverterUtil;
import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.metadata.runtime.api.Model;
import com.metamatrix.metadata.runtime.api.VirtualDatabase;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseID;
import com.metamatrix.platform.registry.ClusteredRegistryState;
import com.metamatrix.platform.registry.FakeRegistryUtil;


/** 
 * Unit tests of ServerMonitoringAdminImpl
 * @since 4.3
 */
public class TestServerConfigAdminImpl extends TestCase implements IdentifierConstants {
    
    private static final String BOGUS_HOST = "slwxp120"; //$NON-NLS-1$
    private static final String BOGUS_HOST_IP = "192.168.10.157"; //$NON-NLS-1$
    private static final String BOGUS_HOST_FULLY_QUALIFIED = BOGUS_HOST + "quadrian.com"; //$NON-NLS-1$

    private static String VDB_NAME1 = "myVdb1"; //$NON-NLS-1$
    private static String VDB_NAME2 = "myVdb2"; //$NON-NLS-1$
    private static String VERSION1 = "1"; //$NON-NLS-1$
    private static String PHYSICAL_MODEL_NAME1 = "PhysicalModel1"; //$NON-NLS-1$
    private static String PHYSICAL_MODEL_NAME2 = "PhysicalModel2"; //$NON-NLS-1$
    
    private ServerAdminImpl parent;
    private FakeServerConfigAdminImpl admin;
    
    
    public void setUp() throws Exception {
        System.setProperty("metamatrix.config.none", "true"); //$NON-NLS-1$ //$NON-NLS-2$
        System.setProperty("metamatrix.message.bus.type", "noop.message.bus"); //$NON-NLS-1$ //$NON-NLS-2$
        
        ClusteredRegistryState registry = FakeRegistryUtil.getFakeRegistry();
        parent = new FakeServerAdminImpl(registry);
        admin = new FakeServerConfigAdminImpl(parent, registry);        
    }
   
    
    
    private void helpCheckBindings(Model model, Collection expectedBindingNames) throws Exception {
    	Collection modelBindings = model.getConnectorBindingNames();
    	// Check sizes first
    	if(modelBindings.size()!=expectedBindingNames.size()) {
    		fail("The number of actual bindings does not match the expected count "+ //$NON-NLS-1$
    			 "\n  actual: " + modelBindings.size() + //$NON-NLS-1$
    			 "\n  expected: " + expectedBindingNames.size()); //$NON-NLS-1$
    	}
    	
    	// Check whether names match
    	Iterator expectedIter = expectedBindingNames.iterator();
    	while(expectedIter.hasNext()) {
    		String expectedName = (String)expectedIter.next();
    		boolean found = false;
    		Iterator actualIter = modelBindings.iterator();
    		while(actualIter.hasNext()) {
    			String actualName = (String)actualIter.next();
    			if(actualName.equals(expectedName)) {
    				found = true;
    				break;
    			}
    		}
    		if(!found) {
    			fail("Binding ["+expectedName+"] was not found in the list of actual model bindings"); //$NON-NLS-1$ //$NON-NLS-2$
    		}
    	}
    }
    
    private Model helpGetModel(String vdbName, String vdbVersion, String modelName) throws Exception {
    	Model resultModel = null;
        Collection vdbs = FakeRuntimeMetadataCatalog.getVirtualDatabases();
        Iterator iter = vdbs.iterator();
        while(iter.hasNext()) {
        	VirtualDatabase vdb = (VirtualDatabase)iter.next();
        	VirtualDatabaseID vdbID = vdb.getVirtualDatabaseID();
        	if(vdbID.getName().equals(vdbName)) {
        		Collection models = FakeRuntimeMetadataCatalog.getModels(vdbID);
        		Iterator modelIter = models.iterator();
        		while(modelIter.hasNext()) {
        			Model model = (Model)modelIter.next();
        			if(model.getName().equals(modelName)) {
        				resultModel = model;
        				break;
        			}
        		}
        	}
        	
        }
        return resultModel;
    }
    
     /**
     * Tests <code>ServerConfigAdminImpl.testExportConfiguration()</code> 
     * @since 4.3
     */
    public void testExportConfiguration() throws AdminException {
        char[] results = admin.exportConfiguration();
        assertNotNull(results);
    }
    
    public void testAddConnectorType() throws Exception {
        String name = "MS Access Connector New"; //$NON-NLS-1$
        String cdkFileName = "MS Access Connector.cdk"; //$NON-NLS-1$
        
        final String datapath = UnitTestUtil.getTestDataPath(); 
        final String fullpathName = datapath + File.separator + cdkFileName; 
        
        File file = new File(fullpathName);
        
        char[] data = ObjectConverterUtil.convertFileToCharArray(file, null);
        admin.addConnectorType(name, data);
    }
    
    public void testAddHost() throws Exception {
        String hostIdentifier = BOGUS_HOST; 
        Properties hostProperties = new Properties();
        hostProperties.setProperty(Host.INSTALL_DIR, "D:\\MetaMatrix\\s43401\\"); //$NON-NLS-1$
        hostProperties.setProperty(Host.HOST_DIRECTORY, "D:\\MetaMatrix\\s43401\\host"); //$NON-NLS-1$
        hostProperties.setProperty(Host.LOG_DIRECTORY, "D:\\MetaMatrix\\s43401\\log"); //$NON-NLS-1$
        hostProperties.setProperty(Host.HOST_ENABLED, "true"); //$NON-NLS-1$
        admin.addHost(hostIdentifier, hostProperties);
    }
    
    public void testAddHostIP() throws Exception {
        String hostIdentifier = BOGUS_HOST_IP; 
        Properties hostProperties = new Properties();
        hostProperties.setProperty(Host.INSTALL_DIR, "D:\\MetaMatrix\\s43401\\"); //$NON-NLS-1$
        hostProperties.setProperty(Host.HOST_DIRECTORY, "D:\\MetaMatrix\\s43401\\host"); //$NON-NLS-1$
        hostProperties.setProperty(Host.LOG_DIRECTORY, "D:\\MetaMatrix\\s43401\\log"); //$NON-NLS-1$
        hostProperties.setProperty(Host.HOST_ENABLED, "true"); //$NON-NLS-1$
        admin.addHost(hostIdentifier, hostProperties);
    }
    
    public void testAddHostFullyQualifiedName() throws Exception {
        String hostIdentifier = BOGUS_HOST_FULLY_QUALIFIED; 
        Properties hostProperties = new Properties();
        hostProperties.setProperty(Host.INSTALL_DIR, "D:\\MetaMatrix\\s43401\\"); //$NON-NLS-1$
        hostProperties.setProperty(Host.HOST_DIRECTORY, "D:\\MetaMatrix\\s43401\\host"); //$NON-NLS-1$
        hostProperties.setProperty(Host.LOG_DIRECTORY, "D:\\MetaMatrix\\s43401\\log"); //$NON-NLS-1$
        hostProperties.setProperty(Host.HOST_ENABLED, "true"); //$NON-NLS-1$
        admin.addHost(hostIdentifier, hostProperties);
    }
    
    public void testAssignBindingToModel() throws Exception {
    	// The FakeConfiguration has 3 connectors available, connectorBinding1, 2 and 3.
    	
    	// Assign a single connector binding, connectorBinding2
    	admin.assignBindingToModel("connectorBinding2",VDB_NAME1,VERSION1,PHYSICAL_MODEL_NAME1); //$NON-NLS-1$ 
    	
    	// Check results
    	Collection expectedBindingNames = new HashSet();
    	expectedBindingNames.add("connectorBinding2uuid");  //$NON-NLS-1$
    	
    	Model model = helpGetModel(VDB_NAME1,VERSION1,PHYSICAL_MODEL_NAME1);
    	
    	helpCheckBindings(model,expectedBindingNames);
    }
    
    public void testAssignBindingsToNonMultiEnabledModel() throws Exception {
    	// The FakeConfiguration has 3 connectors available, connectorBinding1, 2 and 3.
    	
    	// Assign multiple connector bindings, connectorBinding1 and 2
    	String[] bindings = new String[] {"connectorBinding1", "connectorBinding2"}; //$NON-NLS-1$ //$NON-NLS-2$
    	
    	admin.assignBindingsToModel(bindings,VDB_NAME1,VERSION1,PHYSICAL_MODEL_NAME1);  
    	
    	// Check results - Since the model is not multi-source binding enabled, 
    	// only the first binding was actually assigned
    	Collection expectedBindingNames = new HashSet();
    	expectedBindingNames.add("connectorBinding1uuid");  //$NON-NLS-1$
    	
    	Model model = helpGetModel(VDB_NAME1,VERSION1,PHYSICAL_MODEL_NAME1);
    	
    	helpCheckBindings(model,expectedBindingNames);
    }

    public void testAssignBindingsToMultiEnabledModel() throws Exception {
    	// The FakeConfiguration has 3 connectors available, connectorBinding1, 2 and 3.
    	
    	// Assign multiple connector bindings, connectorBinding1 and 2
    	String[] bindings = new String[] {"connectorBinding1", "connectorBinding2"}; //$NON-NLS-1$ //$NON-NLS-2$
    	
    	admin.assignBindingsToModel(bindings,VDB_NAME2,VERSION1,PHYSICAL_MODEL_NAME2);  
    	
    	// Check results - Since the model is multi-source binding enabled, 
    	// both bindings should be assinged
    	Collection expectedBindingNames = new HashSet();
    	expectedBindingNames.add("connectorBinding1uuid");  //$NON-NLS-1$
    	expectedBindingNames.add("connectorBinding2uuid");  //$NON-NLS-1$
    	
    	Model model = helpGetModel(VDB_NAME2,VERSION1,PHYSICAL_MODEL_NAME2);
    	
    	helpCheckBindings(model,expectedBindingNames);
    }

    public void testDeassignBindingFromModel() throws Exception {
    	// The FakeConfiguration has 3 connectors available, connectorBinding1, 2 and 3.
    	
    	// Assign a single connector binding, connectorBinding2
    	admin.assignBindingToModel("connectorBinding2",VDB_NAME1,VERSION1,PHYSICAL_MODEL_NAME1); //$NON-NLS-1$ 
    	
    	// Check results
    	Collection expectedBindingNames = new HashSet();
    	expectedBindingNames.add("connectorBinding2uuid");  //$NON-NLS-1$
    	
    	Model model = helpGetModel(VDB_NAME1,VERSION1,PHYSICAL_MODEL_NAME1);
    	
    	helpCheckBindings(model,expectedBindingNames);
    	
    	// Now Deassign it
    	admin.deassignBindingFromModel("connectorBinding2",VDB_NAME1,VERSION1,PHYSICAL_MODEL_NAME1); //$NON-NLS-1$ 
    	
    	// Check results - expect to be empty
    	expectedBindingNames = new HashSet();
    	
    	helpCheckBindings(model,expectedBindingNames);
    	
    }
    
    public void testDeassignMultiBindingsFromMultiModel() throws Exception {
    	// The FakeConfiguration has 3 connectors available, connectorBinding1, 2 and 3.
    	
    	// Assign multiple connector bindings, connectorBinding1 , 2 and 3
    	String[] bindings = new String[] {"connectorBinding1", "connectorBinding2", "connectorBinding3"}; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    	
    	admin.assignBindingsToModel(bindings,VDB_NAME2,VERSION1,PHYSICAL_MODEL_NAME2);  
    	
    	// Check results - Since the model is multi-source binding enabled, 
    	// all bindings should be assinged
    	Collection expectedBindingNames = new HashSet();
    	expectedBindingNames.add("connectorBinding1uuid");  //$NON-NLS-1$
    	expectedBindingNames.add("connectorBinding2uuid");  //$NON-NLS-1$
    	expectedBindingNames.add("connectorBinding3uuid");  //$NON-NLS-1$
    	
    	Model model = helpGetModel(VDB_NAME2,VERSION1,PHYSICAL_MODEL_NAME2);
    	
    	helpCheckBindings(model,expectedBindingNames);

    	//-------------------------------
    	// Now Deassign two bindings
    	//-------------------------------
    	String[] debindings = new String[] {"connectorBinding1", "connectorBinding3"}; //$NON-NLS-1$ //$NON-NLS-2$ 
    	admin.deassignBindingsFromModel(debindings,VDB_NAME2,VERSION1,PHYSICAL_MODEL_NAME2);  
    	
    	// Check results - expect to have one binding remaining
    	expectedBindingNames = new HashSet();
    	expectedBindingNames.add("connectorBinding2uuid");  //$NON-NLS-1$
    	
    	helpCheckBindings(model,expectedBindingNames);

    	//--------------------------------------------------
    	// Now Deassign them again - should get same result
    	//--------------------------------------------------
    	admin.deassignBindingsFromModel(debindings,VDB_NAME2,VERSION1,PHYSICAL_MODEL_NAME2);  
    	
    	helpCheckBindings(model,expectedBindingNames);

    	//--------------------------------------------------
    	// Now Deassign the last one 
    	//--------------------------------------------------
    	debindings = new String[] {"connectorBinding2"}; //$NON-NLS-1$ 
    	admin.deassignBindingsFromModel(debindings,VDB_NAME2,VERSION1,PHYSICAL_MODEL_NAME2);  
    	
    	// Check results - expect to have no bindings
    	expectedBindingNames = new HashSet();
    	helpCheckBindings(model,expectedBindingNames);
    }
    
    public void testAddAuthenticationProvider() throws Exception {
        String name = "My Test Provider"; //$NON-NLS-1$
        String providertype = "File Membership Domain Provider"; //$NON-NLS-1$
        

        Properties props = new Properties();
        props.setProperty("usersFile", "usersFile.txt"); //$NON-NLS-1$ //$NON-NLS-2$
        props.setProperty("groupsFile", "groupsFile.txt"); //$NON-NLS-1$ //$NON-NLS-2$
        admin.addAuthorizationProvider(name, providertype, props);
        
        if(admin.getConfigurationModel().getConfiguration().getAuthenticationProvider(name) == null) {
            fail("AuthenticationProvider ["+name+"] was not found to be added"); //$NON-NLS-1$ //$NON-NLS-2$
        }
    }    
    
}
    
    
