/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.platform.config.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.api.ComponentDefn;
import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.ConfigurationObjectEditor;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.config.api.ConnectorBindingType;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.common.config.api.HostID;
import com.metamatrix.common.config.api.HostType;
import com.metamatrix.common.config.api.ProductServiceConfig;
import com.metamatrix.common.config.api.ProductServiceConfigID;
import com.metamatrix.common.config.api.ProductType;
import com.metamatrix.common.config.api.ProductTypeID;
import com.metamatrix.common.config.api.ResourceDescriptor;
import com.metamatrix.common.config.api.ServiceComponentDefn;
import com.metamatrix.common.config.api.ServiceComponentDefnID;
import com.metamatrix.common.config.api.SharedResource;
import com.metamatrix.common.config.api.VMComponentDefn;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.config.model.BasicConfigurationObjectEditor;
import com.metamatrix.common.object.Multiplicity;
import com.metamatrix.common.object.PropertyDefinitionImpl;
import com.metamatrix.common.object.PropertyType;
import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.core.util.DateUtil;
import com.metamatrix.core.util.MetaMatrixProductVersion;




/**
*  
*/
public class TstConfigActionHelper {  
	
	private String principal;
	private boolean printMessages = false;
	        
    public TstConfigActionHelper(String thisPrincipal, boolean log) {
    	System.out.println("PRIN1: " + thisPrincipal); //$NON-NLS-1$
    	this.principal = thisPrincipal;
    	this.printMessages = log;
    }

    protected void printMsg(String msg) {
    	if (printMessages) {
    			System.out.println(msg);
    	}	
    }

//    private static final String NEW_CONNECTOR_TYPE_NAME = "TestConnectorType";
//    private static final String NEW_HOST_NAME = "TESTHOST";
//    private static final String NEW_VM_NAME = "TestVM";
//    private static final String NEW_CONNECTOR_NAME = "TestConnectorSVC";
//    private static final String NEW_PSC_NAME = "TestPSC";
    
    
    
   
    
    public Host addHost(String name, String port, BasicConfigurationObjectEditor editor) throws Exception {
			Host newHost = editor.createHost(name);
			
		
			editor.addProperty(newHost, HostType.PORT_NUMBER, port);
			
			return newHost;
    	    	
    }
    
    public ResourceDescriptor addPool(String name, Collection types, BasicConfigurationObjectEditor editor) throws Exception {
		ComponentType connType = findType(SharedResource.JDBC_COMPONENT_TYPE_NAME, types);
 		if (connType == null) {
 			throw new ConfigurationException("AddPool Error - unable to fine type " + SharedResource.JDBC_COMPONENT_TYPE_NAME ); //$NON-NLS-1$
 		}
 	    	
 	    	
 	    	
   			ResourceDescriptor descr = editor.createResourceDescriptor(Configuration.NEXT_STARTUP_ID, 
    				(ComponentTypeID) connType.getID(), name);
//    		coe.modifyProperties(pool2, pool.getProperties(), 
//    				ConfigurationObjectEditor.ADD);
		return descr;
	
    }

    
    public ProductServiceConfig addPSC(String name, ServiceComponentDefn svc, Configuration config, Collection types, BasicConfigurationObjectEditor editor) throws Exception {
		ProductType connType = findProductType(MetaMatrixProductVersion.CONNECTOR_PRODUCT_TYPE_NAME, types);
 		if (connType == null) {
 			throw new ConfigurationException("AddPSC Error - unable to fine component type " + MetaMatrixProductVersion.CONNECTOR_PRODUCT_TYPE_NAME); //$NON-NLS-1$
 		}

			ProductServiceConfig newPSC = editor.createProductServiceConfig((ConfigurationID) config.getID(), (ProductTypeID) connType.getID(), name);
           
			
			if (newPSC == null) {
 				throw new ConfigurationException("AddPSC Error - editor is unable to create new PSC " + name + " of type " + connType.getFullName()); //$NON-NLS-1$ //$NON-NLS-2$
			}
			
			newPSC = (ProductServiceConfig) editor.setLastChangedHistory(newPSC, principal, DateUtil.getCurrentDateAsString());
			newPSC = (ProductServiceConfig) editor.setCreationChangedHistory(newPSC, principal, DateUtil.getCurrentDateAsString());
							
    		printMsg("addPSC created new PSC " + newPSC.getFullName()); //$NON-NLS-1$

//			ProductServiceConfigID pscID = (ProductServiceConfigID) newPSC.getID();
			
			editor.addServiceComponentDefn(config, newPSC,  (ServiceComponentDefnID) svc.getID());
	//		editor.deployServiceDefn(config, svc, pscID);
			
			
//			addServiceComponentDefn(newPSC, svcID);    	
    		printMsg("addPSC add service to PSC "); //$NON-NLS-1$
    	    	
    	    return newPSC;
    }
    
    
 	private static final String TEST_PROPERTY1 = "TestProperty1"; //$NON-NLS-1$
	private static final String TEST_PROPERTY_VALUE_1 = "TestValue1"; //$NON-NLS-1$
//	private static final String TEST_PROPERTY2 = "TestProperty2";
//	private static final String TEST_PROPERTY_VALUE_2 = "TestValue2";
//	private static final String TEST_PROPERTY3 = "TestProperty3";
//	private static final String TEST_PROPERTY_VALUE_3 = "TestValue3";
   
    public ComponentDefn addVM(String name, ConfigurationID configID, ComponentType type, BasicConfigurationObjectEditor editor) throws Exception {

			VMComponentDefn defn = editor.createVMComponentDefn(configID, new HostID("TestHost"), (ComponentTypeID) type.getID(), name);  //$NON-NLS-1$

			defn = (VMComponentDefn) editor.setLastChangedHistory(defn, principal, DateUtil.getCurrentDateAsString());
			defn = (VMComponentDefn) editor.setCreationChangedHistory(defn, principal, DateUtil.getCurrentDateAsString());
			
		
			editor.addProperty(defn, TEST_PROPERTY1, TEST_PROPERTY_VALUE_1);
			
			return defn;
    	    	
    }
    
    
//    private ComponentDefn addService(String name, ConfigurationID configID, ComponentType type, BasicConfigurationObjectEditor editor) throws Exception {
//
//			ComponentDefn defn = (ComponentDefn) editor.createServiceComponentDefn(configID, (ComponentTypeID) type.getID(), name);
//		
//			defn = (ComponentDefn) editor.setLastChangedHistory(defn, principal, DateUtil.getCurrentDateAsString());
//			defn = (ComponentDefn) editor.setCreationChangedHistory(defn, principal, DateUtil.getCurrentDateAsString());
//					
//			editor.addProperty(defn, TEST_PROPERTY1, TEST_PROPERTY_VALUE_1);
//			
//			return defn;
//    	    	
//    }
    
    public ComponentDefn addConnector(String name, ComponentType type, ConfigurationObjectEditor editor) throws Exception {

			ConnectorBinding defn = editor.createConnectorComponent(Configuration.NEXT_STARTUP_ID, (ComponentTypeID) type.getID(), name, null);

    		ArgCheck.isNotNull(defn, "Unable to create connector " + name); //$NON-NLS-1$

			printMsg("Principal " + principal); //$NON-NLS-1$

			defn = (ConnectorBinding) editor.setLastChangedHistory(defn, principal, DateUtil.getCurrentDateAsString());
			defn = (ConnectorBinding) editor.setCreationChangedHistory(defn, principal, DateUtil.getCurrentDateAsString());
					
			editor.addProperty(defn, TEST_PROPERTY1, TEST_PROPERTY_VALUE_1);
			
			return defn;
    	    	
    }    
    
    public ComponentDefn addConnector(Configuration config, String name, ComponentType type, ProductServiceConfigID pscID, ConfigurationObjectEditor editor) throws Exception {

			ConnectorBinding defn = editor.createConnectorComponent(config,
							 (ComponentTypeID) type.getID(), 
							 name, 
							 pscID);
							 
    		ArgCheck.isNotNull(defn, "Unable to create connector " + name); //$NON-NLS-1$

			defn = (ConnectorBinding) editor.setLastChangedHistory(defn, principal, DateUtil.getCurrentDateAsString());
			defn = (ConnectorBinding) editor.setCreationChangedHistory(defn, principal, DateUtil.getCurrentDateAsString());
					
			editor.addProperty(defn, TEST_PROPERTY1, TEST_PROPERTY_VALUE_1);
			
			return defn;
    	    	
    }    
    
    
    
    public ComponentType addConnectorComponentType(String name, ConfigurationObjectEditor editor, Collection types) throws Exception {
		ComponentType connType = findType(ConnectorBindingType.COMPONENT_TYPE_NAME, types);
 		if (connType == null) {
 			throw new ConfigurationException("AddConnectorComponentType Error - unable to fine type " + ConnectorBindingType.COMPONENT_TYPE_NAME ); //$NON-NLS-1$
 		}
 		
 		
 		ProductType pct = CurrentConfiguration.getConfigurationModel().getProductType(MetaMatrixProductVersion.CONNECTOR_PRODUCT_TYPE_NAME);
 		
 		
		ComponentType testSvcType = editor.createComponentType(ComponentType.CONNECTOR_COMPONENT_TYPE_CODE, name, (ProductTypeID) pct.getID(), (ComponentTypeID) connType.getID(), true, false);


        PropertyType type = PropertyType.STRING;
        
        Multiplicity mult = null;
        mult = Multiplicity.getInstance("1"); //$NON-NLS-1$
        

      	PropertyDefinitionImpl defn = new PropertyDefinitionImpl("TestPropDefn", "TestPropDefn", type, //$NON-NLS-1$ //$NON-NLS-2$
                        mult);
         
        ArrayList compTypeDefns = new ArrayList(1);        
        compTypeDefns.add(editor.createComponentTypeDefn(testSvcType, defn, false));

        editor.setComponentTypeDefinitions(testSvcType, compTypeDefns);
	
								  

    	return testSvcType;
     }
     
     
     
     public ComponentType findType(String name, Collection types) {
		ComponentType connType = null;
		for (Iterator it=types.iterator(); it.hasNext(); ) {
			connType = (ComponentType) it.next();
			if (connType.getFullName().equals(name)) {
				return connType;
			}

			connType = null;	
		}

		return connType;
		 		
   	
    }
     
     public ProductType findProductType(String name, Collection types) {
         ProductType connType = null;
        for (Iterator it=types.iterator(); it.hasNext(); ) {
            connType = (ProductType) it.next();
            if (connType.getFullName().equals(name)) {
                return connType;
            }

            connType = null;    
        }

        return connType;
                
        
     }     
    
//    private ComponentObject findMatch(String name, Collection types) {
//		ComponentObject connType = null;
//		for (Iterator it=types.iterator(); it.hasNext(); ) {
//			connType = (ComponentObject) it.next();
//			if (connType.getFullName().endsWith(name)) {
//				return connType;
//			}
//
//			connType = null;	
//		}
//
//		return connType;
//		 		
//   	
//    }    
    
    
//    private boolean compare(String fileName1, ConfigurationID id1, String fileName2, ConfigurationID id2, String logFile) throws Exception {
//    	
//    	ConfigurationModelContainer model1 = TestFilePersistence.readModel(fileName1, id1);
//    	
//    	ConfigurationModelContainer model2 = TestFilePersistence.readModel(fileName2, id2);
//    	
//   	
//    	boolean isEqual = TestFilePersistence.compareModels(model1, model2, logFile);
//    	
//    	return isEqual;
//    		
//    }
        
    
//     private InputStream readFile(String fileName) throws ConfigurationException {
//        InputStream inputStream = null;
//        
//        File configFile = new File(fileName);
//        if (!configFile.exists()) {
//            throw new ConfigurationException("Unable to read configuration file " + fileName + ", it does not exist.");
//        }
//        
//        try {          
//                inputStream = new FileInputStream(configFile);
//                return inputStream;
//
//        } catch (IOException io) {
//            throw new ConfigurationException(io, "Unable to read configuration file " + fileName + ", error in reading file");
//        }                               
//    }
    
//    private void designateNextStartupConfiguration(Collection configObjects,
//                                                    ConfigurationObjectEditor editor) throws Exception{
//        Iterator iterator = configObjects.iterator();
//        while (iterator.hasNext()) {
//            Object obj = iterator.next();
//            if (obj instanceof Configuration) {
//                Configuration config = (Configuration)obj;
//                ConfigurationID configID = (ConfigurationID)config.getID();
//                editor.setNextStartupConfiguration(configID);
//            }
//        }
//    }
    

}



