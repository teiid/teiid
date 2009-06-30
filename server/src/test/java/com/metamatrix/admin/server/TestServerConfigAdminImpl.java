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
import java.util.List;
import java.util.Properties;

import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.AdminObject;
import org.teiid.adminapi.AdminOptions;
import org.teiid.adminapi.AdminProcessingException;
import org.teiid.adminapi.ConnectorBinding;
import org.teiid.adminapi.Host;
import org.teiid.adminapi.ProcessObject;
import org.teiid.adminapi.Service;

import junit.framework.TestCase;

import com.metamatrix.admin.objects.MMConnectorBinding;
import com.metamatrix.admin.objects.MMProcess;
import com.metamatrix.common.application.DQPConfigSource;
import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.DeployedComponent;
import com.metamatrix.common.config.api.ServiceComponentDefn;
import com.metamatrix.core.util.ObjectConverterUtil;
import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.metadata.runtime.api.Model;
import com.metamatrix.metadata.runtime.api.VirtualDatabase;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseID;
import com.metamatrix.platform.config.ConfigUpdateMgr;
import com.metamatrix.platform.config.util.CurrentConfigHelper;
import com.metamatrix.platform.registry.ClusteredRegistryState;
import com.metamatrix.platform.registry.FakeRegistryUtil;
import com.metamatrix.platform.security.api.service.MembershipServiceInterface;


/** 
 * Unit tests of ServerMonitoringAdminImpl
 * @since 4.3
 */
public class TestServerConfigAdminImpl extends TestCase implements IdentifierConstants {
    
    private static final String BOGUS_HOST = "slwxp120"; //$NON-NLS-1$
    private static final String BOGUS_HOST_IP = "192.168.10.157"; //$NON-NLS-1$
    private static final String BOGUS_HOST_FULLY_QUALIFIED = BOGUS_HOST + "quadrian.com"; //$NON-NLS-1$
    
    private static final String BOGUS_PROCESS = "MMProcess"; //$NON-NLS-1$
    private static final String BOGUS_SERVICE = "MyService"; //$NON-NLS-1$

    private static String VDB_NAME1 = "myVdb1"; //$NON-NLS-1$
    private static String VDB_NAME2 = "myVdb2"; //$NON-NLS-1$
    private static String VERSION1 = "1"; //$NON-NLS-1$
    private static String PHYSICAL_MODEL_NAME1 = "PhysicalModel1"; //$NON-NLS-1$
    private static String PHYSICAL_MODEL_NAME2 = "PhysicalModel2"; //$NON-NLS-1$
    
    private static FakeConfigurationService configService = new FakeConfigurationService();
    private static ConfigurationModelContainer configModelContainer = null;
    
    private ServerAdminImpl parent;
    private FakeServerConfigAdminImpl admin;
    
    
    public void setUp() throws Exception {
        System.setProperty("metamatrix.config.none", "true"); //$NON-NLS-1$ //$NON-NLS-2$
        System.setProperty("metamatrix.message.bus.type", "noop.message.bus"); //$NON-NLS-1$ //$NON-NLS-2$
        
        ConfigUpdateMgr.createSystemProperties("config_multihost.xml");        
        
        ClusteredRegistryState registry = FakeRegistryUtil.getFakeRegistry();
        parent = new FakeServerAdminImpl(registry);
        admin = new FakeServerConfigAdminImpl(parent, registry);        
    }
   
    
    
    private void helpCheckBindings(Model model, Collection<String> expectedBindingNames) throws Exception {
    	Collection modelBindings = model.getConnectorBindingNames();
    	// Check sizes first
    	if(modelBindings.size()!=expectedBindingNames.size()) {
    		fail("The number of actual bindings does not match the expected count "+ //$NON-NLS-1$
    			 "\n  actual: " + modelBindings.size() + //$NON-NLS-1$
    			 "\n  expected: " + expectedBindingNames.size()); //$NON-NLS-1$
    	}
    	
    	// Check whether names match
    	Iterator<String> expectedIter = expectedBindingNames.iterator();
    	while(expectedIter.hasNext()) {
    		String expectedName = expectedIter.next();
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
    
    public void testAddConnectorBindingUsingNameInCDK() throws Exception {
         String cdkFileName = "GateaConnector.cdk"; //$NON-NLS-1$
        
        final String datapath = UnitTestUtil.getTestDataPath(); 
        final String fullpathName = datapath + File.separator + "config" + File.separator + cdkFileName; 
        
        File file = new File(fullpathName);
        
        char[] data = ObjectConverterUtil.convertFileToCharArray(file, null);
        ConnectorBinding cb = admin.addConnectorBinding("", data, new AdminOptions(AdminOptions.OnConflict.OVERWRITE));
        assertNotNull(cb);

    }  
    
    public void testAddHost() throws Exception {
        String hostIdentifier = BOGUS_HOST; 
        Properties hostProperties = new Properties();
//        hostProperties.setProperty(Host.INSTALL_DIR, "D:\\MetaMatrix\\s43401\\"); //$NON-NLS-1$
//        hostProperties.setProperty(Host.HOST_DIRECTORY, "D:\\MetaMatrix\\s43401\\host"); //$NON-NLS-1$
//        hostProperties.setProperty(Host.LOG_DIRECTORY, "D:\\MetaMatrix\\s43401\\log"); //$NON-NLS-1$
//        hostProperties.setProperty(Host.HOST_ENABLED, "true"); //$NON-NLS-1$
        admin.addHost(hostIdentifier, hostProperties);
    }
    
    public void testAddHostIP() throws Exception {
        String hostIdentifier = BOGUS_HOST_IP; 
        Properties hostProperties = new Properties();
//        hostProperties.setProperty(Host.INSTALL_DIR, "D:\\MetaMatrix\\s43401\\"); //$NON-NLS-1$
//        hostProperties.setProperty(Host.HOST_DIRECTORY, "D:\\MetaMatrix\\s43401\\host"); //$NON-NLS-1$
//        hostProperties.setProperty(Host.LOG_DIRECTORY, "D:\\MetaMatrix\\s43401\\log"); //$NON-NLS-1$
//        hostProperties.setProperty(Host.HOST_ENABLED, "true"); //$NON-NLS-1$
        admin.addHost(hostIdentifier, hostProperties);
    }
    
    public void testAddHostFullyQualifiedName() throws Exception {
        String hostIdentifier = BOGUS_HOST_FULLY_QUALIFIED; 
        Properties hostProperties = new Properties();
//        hostProperties.setProperty(Host.INSTALL_DIR, "D:\\MetaMatrix\\s43401\\"); //$NON-NLS-1$
//        hostProperties.setProperty(Host.HOST_DIRECTORY, "D:\\MetaMatrix\\s43401\\host"); //$NON-NLS-1$
//        hostProperties.setProperty(Host.LOG_DIRECTORY, "D:\\MetaMatrix\\s43401\\log"); //$NON-NLS-1$
//        hostProperties.setProperty(Host.HOST_ENABLED, "true"); //$NON-NLS-1$
        admin.addHost(hostIdentifier, hostProperties);
    }
    
    public void testAssignBindingToModel() throws Exception {
    	// The FakeConfiguration has 3 connectors available, connectorBinding1, 2 and 3.
    	
    	// Assign a single connector binding, connectorBinding2
    	admin.assignBindingToModel("connectorBinding2",VDB_NAME1,VERSION1,PHYSICAL_MODEL_NAME1); //$NON-NLS-1$ 
    	
    	// Check results
    	Collection<String> expectedBindingNames = new HashSet<String>();
    	expectedBindingNames.add("connectorBinding2");  //$NON-NLS-1$
    	
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
    	Collection<String> expectedBindingNames = new HashSet<String>();
    	expectedBindingNames.add("connectorBinding1");  //$NON-NLS-1$
    	
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
    	Collection<String> expectedBindingNames = new HashSet<String>();
    	expectedBindingNames.add("connectorBinding1");  //$NON-NLS-1$
    	expectedBindingNames.add("connectorBinding2");  //$NON-NLS-1$
    	
    	Model model = helpGetModel(VDB_NAME2,VERSION1,PHYSICAL_MODEL_NAME2);
    	
    	helpCheckBindings(model,expectedBindingNames);
    }

    public void testDeassignBindingFromModel() throws Exception {
    	// The FakeConfiguration has 3 connectors available, connectorBinding1, 2 and 3.
    	
    	// Assign a single connector binding, connectorBinding2
    	admin.assignBindingToModel("connectorBinding2",VDB_NAME1,VERSION1,PHYSICAL_MODEL_NAME1); //$NON-NLS-1$ 
    	
    	// Check results
    	Collection<String> expectedBindingNames = new HashSet<String>();
    	expectedBindingNames.add("connectorBinding2");  //$NON-NLS-1$
    	
    	Model model = helpGetModel(VDB_NAME1,VERSION1,PHYSICAL_MODEL_NAME1);
    	
    	helpCheckBindings(model,expectedBindingNames);
    	
    	// Now Deassign it
    	admin.deassignBindingFromModel("connectorBinding2",VDB_NAME1,VERSION1,PHYSICAL_MODEL_NAME1); //$NON-NLS-1$ 
    	
    	// Check results - expect to be empty
    	expectedBindingNames = new HashSet<String>();
    	
    	helpCheckBindings(model,expectedBindingNames);
    	
    }
    
    public void testDeassignMultiBindingsFromMultiModel() throws Exception {
    	// The FakeConfiguration has 3 connectors available, connectorBinding1, 2 and 3.
    	
    	// Assign multiple connector bindings, connectorBinding1 , 2 and 3
    	String[] bindings = new String[] {"connectorBinding1", "connectorBinding2", "connectorBinding3"}; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    	
    	admin.assignBindingsToModel(bindings,VDB_NAME2,VERSION1,PHYSICAL_MODEL_NAME2);  
    	
    	// Check results - Since the model is multi-source binding enabled, 
    	// all bindings should be assinged
    	Collection<String> expectedBindingNames = new HashSet<String>();
    	expectedBindingNames.add("connectorBinding1");  //$NON-NLS-1$
    	expectedBindingNames.add("connectorBinding2");  //$NON-NLS-1$
    	expectedBindingNames.add("connectorBinding3");  //$NON-NLS-1$
    	
    	Model model = helpGetModel(VDB_NAME2,VERSION1,PHYSICAL_MODEL_NAME2);
    	
    	helpCheckBindings(model,expectedBindingNames);

    	//-------------------------------
    	// Now Deassign two bindings
    	//-------------------------------
    	String[] debindings = new String[] {"connectorBinding1", "connectorBinding3"}; //$NON-NLS-1$ //$NON-NLS-2$ 
    	admin.deassignBindingsFromModel(debindings,VDB_NAME2,VERSION1,PHYSICAL_MODEL_NAME2);  
    	
    	// Check results - expect to have one binding remaining
    	expectedBindingNames = new HashSet<String>();
    	expectedBindingNames.add("connectorBinding2");  //$NON-NLS-1$
    	
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
    	expectedBindingNames = new HashSet<String>();
    	helpCheckBindings(model,expectedBindingNames);
    }
    
    public void testDeassignNonexistantBinding() throws Exception {
    	// The FakeConfiguration has 3 connectors available, connectorBinding1, 2 and 3.
    	
    	// Assign multiple connector bindings, connectorBinding1 , 2 and 3
    	String[] bindings = new String[] {"connectorBinding1", "connectorBinding2", "connectorBinding3"}; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    	
    	admin.assignBindingsToModel(bindings,VDB_NAME2,VERSION1,PHYSICAL_MODEL_NAME2);  
    	
    	String[] debindings = new String[] {"connectorBindingx"}; //$NON-NLS-1$ 
    	try {
    		admin.deassignBindingsFromModel(debindings,VDB_NAME2,VERSION1,PHYSICAL_MODEL_NAME2);  
    	} catch (AdminProcessingException e) {
    		assertEquals("Connector Binding connectorBindingx not found in Configuration", e.getMessage()); //$NON-NLS-1$ 
    	}
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
    
 
    
    public void testgetNodeCount() throws Exception {
      
    	assertEquals(1, admin.getNodeCount(BOGUS_HOST_FULLY_QUALIFIED));
    	assertEquals(2, admin.getNodeCount(BOGUS_HOST_FULLY_QUALIFIED + AdminObject.DELIMITER + BOGUS_PROCESS));
    	assertEquals(3, admin.getNodeCount(BOGUS_HOST_FULLY_QUALIFIED + AdminObject.DELIMITER + BOGUS_PROCESS + AdminObject.DELIMITER + BOGUS_SERVICE));

    }
    
//    public void testUpdateProperties() throws Exception {
//        
//    	//Test update properties for deployed connector
//    	Properties properties = new Properties();
//    	properties.put(DQPConfigSource.PROCESS_POOL_MAX_THREADS, "11");
//    	admin.updateProperties(HOST_2_2_2_2_PROCESS2_CONNECTOR_BINDING2,com.metamatrix.admin.api.objects.ConnectorBinding.class.getName(), properties);
//    	//Verify results
//    	DeployedComponent dc = admin.getDeployedComponent(HOST_2_2_2_2_PROCESS2_CONNECTOR_BINDING2);
//    	String actualPropValue = dc.getProperty(DQPConfigSource.PROCESS_POOL_MAX_THREADS);
//    	assertEquals("11", actualPropValue);
//    	
//    	//Test update properties for a connector binding from configuration
//    	properties = new Properties();
//    	properties.put(DQPConfigSource.PROCESS_POOL_MAX_THREADS, "22");
//    	admin.updateProperties("connectorbinding2",com.metamatrix.admin.api.objects.ConnectorBinding.class.getName(), properties);
//    	//Verify results
//    	List<com.metamatrix.common.config.api.ConnectorBinding> objs1 =  admin.getConnectorBindingsByName(new String[] {"connectorBinding2"});     		
//    	MMConnectorBinding binding = objs1.iterator().next();
//       	actualPropValue = binding.getPropertyValue(DQPConfigSource.PROCESS_POOL_MAX_THREADS);
//    	assertEquals("22", actualPropValue);    	    	
//    	    	
//    	//Test update properties for deployed service
//    	properties = new Properties();
//    	properties.put("ProcessPoolThreadTTL", "9");
//    	admin.updateProperties(HOST_2_2_2_2_PROCESS2_DQP2,com.metamatrix.admin.api.objects.Service.class.getName(), properties);
//    	//Verify results
//    	dc = admin.getDeployedComponent(HOST_2_2_2_2_PROCESS2_DQP2);
//    	actualPropValue = dc.getProperty("ProcessPoolThreadTTL");
//    	assertEquals("9", actualPropValue);
//    	
//    	//Test update properties for a service from configuration
//    	properties = new Properties();
//    	properties.put(MembershipServiceInterface.SECURITY_ENABLED, "false");
//    	admin.updateProperties(MembershipServiceInterface.NAME,com.metamatrix.admin.api.objects.Service.class.getName(), properties);
//    	//Verify results
//    	ServiceComponentDefn service =  admin.getServiceByName(MembershipServiceInterface.NAME);     		
//      	actualPropValue = service.getProperty(MembershipServiceInterface.SECURITY_ENABLED);
//    	assertEquals("false", actualPropValue);    
//    	
//    	//Test update properties for process
//    	properties = new Properties();
//    	properties.put(ProcessObject.TIMETOLIVE, "99");
//    	admin.updateProperties(HOST_2_2_2_2_PROCESS2,com.metamatrix.admin.api.objects.ProcessObject.class.getName(), properties);
//    	//Verify results
//    	Collection<MMProcess> processObjs = admin.getAdminObjects(HOST_2_2_2_2_PROCESS2,com.metamatrix.admin.api.objects.ProcessObject.class.getName());
//    	MMProcess process = processObjs.iterator().next();
//    	actualPropValue = process.getPropertyValue(ProcessObject.TIMETOLIVE);
//    	assertEquals("99", actualPropValue);
//    	
//    }
//    
    
}
    
    
