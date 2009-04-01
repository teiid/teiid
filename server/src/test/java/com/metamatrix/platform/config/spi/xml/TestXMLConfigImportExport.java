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

package com.metamatrix.platform.config.spi.xml;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Properties;

import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.api.ComponentDefnID;
import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.ConfigurationObjectEditor;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.config.api.ProductServiceConfig;
import com.metamatrix.common.config.api.ProductServiceConfigID;
import com.metamatrix.common.config.api.ProductType;
import com.metamatrix.common.config.api.ProductTypeID;
import com.metamatrix.common.config.api.ServiceComponentDefn;
import com.metamatrix.common.config.api.ServiceComponentDefnID;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.config.model.BasicConfigurationObjectEditor;
import com.metamatrix.common.config.xml.XMLConfigurationImportExportUtility;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.core.util.DateUtil;
import com.metamatrix.core.util.FileUtils;
import com.metamatrix.core.util.MetaMatrixProductVersion;
import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.platform.PlatformPlugin;
import com.metamatrix.platform.config.BaseTest;
import com.metamatrix.platform.config.persistence.api.PersistentConnection;
import com.metamatrix.platform.config.persistence.api.PersistentConnectionFactory;
import com.metamatrix.platform.config.persistence.impl.file.FilePersistentConnection;
import com.metamatrix.platform.config.persistence.impl.file.FilePersistentUtil;
import com.metamatrix.platform.config.util.CurrentConfigHelper;
import com.metamatrix.platform.util.ErrorMessageKeys;


public class TestXMLConfigImportExport extends BaseTest {  
	
	private static String PRINCIPAL = "TestXMLConfigImportExport";       //$NON-NLS-1$
    
	
   	/**
	 * This test the import operation that the console will be doing
	 * when importing a new configuration
	 */
   	
    private static final String CDK_FILE = "GateaConnector.cdk"; //$NON-NLS-1$
	
    public TestXMLConfigImportExport(String name) {
        super(name);
    }
    
	private void initializeConfig(String fileName) throws Exception {
		printMsg("Perform initializeConfig using " + fileName); //$NON-NLS-1$
	
		CurrentConfigHelper.initXMLConfig(fileName, this.getPath(), PRINCIPAL);
        
        this.initTransactions(new Properties());
        
		printMsg("Completed initializeConfig"); //$NON-NLS-1$
	}
		
	/**
	 * This test the import operation that the console will be doing
	 * when importing a new configuration
	 */
	public void testImportNewStartupConfiguration() throws Exception {
    	printMsg("Starting testImportNewStartupConfiguration");    	 //$NON-NLS-1$
                    
  		initializeConfig(CONFIG_FILE);

  		FileUtils.copy(UnitTestUtil.getTestDataPath()+"/config/config.xml", UnitTestUtil.getTestScratchPath()+"/config_future.xml");
  		
        Properties props = new Properties();
        props.setProperty(FilePersistentConnection.CONFIG_FILE_PATH_PROPERTY, UnitTestUtil.getTestScratchPath());
        props.setProperty(FilePersistentConnection.CONFIG_NS_FILE_NAME_PROPERTY, FilePersistentConnection.NEXT_STARTUP_FILE_NAME);
        props.setProperty(PersistentConnectionFactory.PERSISTENT_FACTORY_NAME, PersistentConnectionFactory.FILE_FACTORY_NAME);
        
        importConfiguration("config_future.xml", props, PRINCIPAL);
	    	
		
    	printMsg("Completed testImportNewStartupConfiguration"); //$NON-NLS-1$
    }
	
    /**
     * Imports into next startup configuration
     */
    public static void importConfiguration(String fileName, Properties properties, String principal) throws ConfigurationException {
		ConfigurationModelContainer nsModel = null;

 		try {
        	 nsModel = FilePersistentUtil.readModel(fileName, properties.getProperty(FilePersistentConnection.CONFIG_FILE_PATH_PROPERTY), Configuration.NEXT_STARTUP_ID);
 		} catch (Exception e) {
 			throw new ConfigurationException(e, ErrorMessageKeys.CONFIG_0186, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0186, fileName));
 		}

		Properties props = PropertiesUtils.clone(properties, false);

        PersistentConnectionFactory pf = new PersistentConnectionFactory(props);

        PersistentConnection pc = pf.createPersistentConnection(false);

        // write the models out
        pc.delete(Configuration.NEXT_STARTUP_ID, principal);
      	pc.write(nsModel, principal);
      	pc.commit();
      	pc.close();
    }

    	
    
    /**
     * This test the Console process for importing a new binding
    
	public void testImportConnector() throws Exception {
    	printMsg("Starting testImportConnector"); //$NON-NLS-1$

      	// file should not have the example connector in it
//      		initializeConfig(CONFIG_WITH_CONNECTOR_FILE);
      		initializeConfig(CONFIG_FILE);
            this.initTransactions(new Properties());
            
    	
    		helperImportConnectorType();
    		helperImportConnectorBinding(CDK_FILE);
    	
    	printMsg("Completed testImportConnector"); //$NON-NLS-1$
    	
	}
	 */
//	public void testImportJDBCConnector() {
//    	printMsg("Starting testImportJDBCConnector"); //$NON-NLS-1$
//
//      try {
//      	// file should not have the example connector in it
//       		initializeConfig(CONFIG_FILE);
//    	
//    		helperImportConnectorBinding(JDBC_FILE);
//      } catch (Exception e) {
// //     		e.printStackTrace();
//      	
//    		fail(e.getMessage());
//   	  }
//    	
//    	printMsg("Completed testImportJDBCConnector"); //$NON-NLS-1$
//    	
//	}
	
	
	private void helperImportConnectorType() throws Exception {    	
    	printMsg("Starting helperImportConnectorType"); //$NON-NLS-1$
    	
//    	   	initializeConfig(CONFIG_FILE);

                    
 //     		XMLConfigurationConnector writer = (XMLConfigurationConnector) factory.createTransaction(conn, false);

	     	File cdkFile = new File(getPath(), CDK_FILE);
	      		          
	    	
	    	InputStream inputStream = readFile(cdkFile.getPath());
	    	
	   	
	    	XMLConfigurationImportExportUtility io = new XMLConfigurationImportExportUtility();
	    
	        
	     //   ConfigurationObjectEditor editor = new BasicConfigurationObjectEditor(true);
	                     
	        ComponentType type = io.importComponentType(inputStream, getEditor(), null);
	         
				
//			editor.getDestination().popActions();						
	
//	    	editor.createComponentType(type);
            
            commit();
	    	  
			
//			List actions = editor.getDestination().popActions();
//				
//			printMsg("Executing # of Actions: " + actions.size()); //$NON-NLS-1$
//			
//			try {
//				writer.executeActions(actions, PRINCIPAL);
//			
//
//				writer.commit();
//			} catch (Exception e) {
//				writer.rollback();
//				throw e;	
//			}
			
			ComponentType newType = getWriter().getComponentType((ComponentTypeID) type.getID());
			if (newType == null) {
				fail("Import of connector type " + type.getID() + " was not successfull."); //$NON-NLS-1$ //$NON-NLS-2$
			}
			HelperTestConfiguration.validateComponentType(newType);

    	printMsg("Completed helperImportConnectorType"); //$NON-NLS-1$
 
    }
    
   	/**
	 * This test the import operation that the console will be doing
	 * when importing a new configuration
	 */
	private void helperImportConnectorBinding(String file) throws Exception {    	
    	printMsg("Starting helperImportConnectorBinding");    	 //$NON-NLS-1$
                    
//      		XMLConfigurationConnector writer = (XMLConfigurationConnector) factory.createTransaction(conn, false);

	     	File connFile = new File(getPath(), file);
	      		          
	    	
	    	InputStream inputStream = readFile(connFile.getPath());
	    	
	   	
	    	XMLConfigurationImportExportUtility io = new XMLConfigurationImportExportUtility();
	    
	        
	        ConfigurationObjectEditor editor =  new BasicConfigurationObjectEditor(false);
	                     
	        ConnectorBinding defn = io.importConnectorBinding(inputStream, editor, null);
	        
            if (defn == null) {
                fail("No connector binding was imported"); //$NON-NLS-1$
            }
		        
//		   	ConfigurationObjectEditor reditor = new BasicConfigurationObjectEditor(true);

	        ConfigurationModelContainer config = getWriter().getConfigurationModel(Configuration.NEXT_STARTUP);

	        
            
	        
	        
	        ConnectorBinding newdefn = getEditor().createConnectorComponent(Configuration.NEXT_STARTUP_ID, defn, defn.getFullName(), defn.getRoutingUUID());
//	        reditor.setEnabled(newdefn, true);
            
            commit();
            
            ProductServiceConfig psc = com.metamatrix.common.config.util.ConfigUtil.getFirstDeployedConnectorProductTypePSC(config);
            
            ProductServiceConfigID pscID = null;

//            ComponentDefn defn = null;
            if (psc != null) {
                pscID = (ProductServiceConfigID)psc.getID();
                
            } else {
                psc = helperTestAddPSC("TestPSC_" + String.valueOf( (new Date()).getTime()), (ServiceComponentDefnID) newdefn.getID(),  Configuration.NEXT_STARTUP_ID ); //$NON-NLS-1$
                pscID = (ProductServiceConfigID)psc.getID();
                
            }
	        // add to the psc, but does not deploy
            psc = getEditor().addServiceComponentDefn(psc, (ServiceComponentDefnID) newdefn.getID());

			// deploys the binding if the psc is deployed
            getEditor().deployServiceDefn(config.getConfiguration(), newdefn, pscID);
	         				
            commit();
//			List actions = reditor.getDestination().popActions();						
//	
//				
//			printMsg("Executing # of Actions: " + actions.size() + " for # of Connector: " + newdefn.getFullName()); //$NON-NLS-1$ //$NON-NLS-2$
//
//			try {
//				writer.executeActions(actions, PRINCIPAL);
//			
//				writer.commit();
//			} catch (Exception e) {
//				writer.rollback();
//				throw e;	
//			}
			
			ConnectorBinding newDefn = (ConnectorBinding) getWriter().getComponentDefinition((ComponentDefnID) defn.getID(), Configuration.NEXT_STARTUP_ID);	
			if (newDefn == null) {
				fail("Import of connector binding was not successfull, no obect found when reading."); //$NON-NLS-1$
			}
			
			HelperTestConfiguration.validateConnectorBinding(newDefn);
 		
    	printMsg("Completed helperImportConnectorBinding"); //$NON-NLS-1$
 
    }
    
    
    
    private ProductServiceConfig helperTestAddPSC(String pscName, ServiceComponentDefnID id, ConfigurationID configID) {
        printMsg("Starting helperTestAddPSC");       //$NON-NLS-1$
        ProductServiceConfig newDefn = null;            
      try {
        
            
            Configuration config = this.getConfigModel().getConfiguration();
            
            Collection types = this.getConfigModel().getProductTypes();
                    
            ConnectorBinding defn =  config.getConnectorBinding(id)  ; 
            
            newDefn = addPSC(pscName, defn, config, types, getEditor());
                        

            printMsg("helperTestAddPSC actions committed"); //$NON-NLS-1$
        
            
            HelperTestConfiguration.validateComponentDefn(newDefn);
            
//          ConfigurationPrinter.printComponentObject(h, false, System.out);
                    
          
 
      } catch (Exception e) {
        e.printStackTrace();
            fail(e.getMessage());
        }
        printMsg("Completed helperTestAddPSC"); //$NON-NLS-1$
        return newDefn;
    }
    
    
    private ProductServiceConfig addPSC(String name, ServiceComponentDefn svc, Configuration config, Collection types, BasicConfigurationObjectEditor editor) throws Exception {

        ProductType connType = findProductType(MetaMatrixProductVersion.CONNECTOR_PRODUCT_TYPE_NAME, types);
        if (connType == null) {
            throw new ConfigurationException("AddPSC Error - unable to fine component type " + MetaMatrixProductVersion.CONNECTOR_PRODUCT_TYPE_NAME); //$NON-NLS-1$
        }

    

            printMsg("addPSC found product type " + connType.getFullName()); //$NON-NLS-1$

            ProductServiceConfig newPSC = getEditor().createProductServiceConfig((ConfigurationID) config.getID(), (ProductTypeID) connType.getID(), name);
            
            if (newPSC == null) {
                throw new ConfigurationException("AddPSC Error - editor is unable to create new PSC " + name + " of type " + connType.getFullName()); //$NON-NLS-1$ //$NON-NLS-2$
            }
            
            newPSC = (ProductServiceConfig) getEditor().setLastChangedHistory(newPSC, PRINCIPAL, DateUtil.getCurrentDateAsString());
            newPSC = (ProductServiceConfig) getEditor().setCreationChangedHistory(newPSC, PRINCIPAL, DateUtil.getCurrentDateAsString());
                            
            printMsg("addPSC created new PSC " + newPSC.getFullName()); //$NON-NLS-1$

            this.getEditor().addServiceComponentDefn(config, newPSC,  (ServiceComponentDefnID) svc.getID());
            printMsg("addPSC add service to PSC "); //$NON-NLS-1$
            
            commit();
            
            ProductServiceConfig psc = CurrentConfiguration.getInstance().getConfiguration().getPSC((ProductServiceConfigID) newPSC.getID());
            assertNotNull(psc);
                
            return newPSC;
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
    
     private InputStream readFile(String fileName) throws ConfigurationException {
        InputStream inputStream = null;
        
        File configFile = new File(fileName);
        if (!configFile.exists()) {
            throw new ConfigurationException("Unable to read configuration file " + fileName + ", it does not exist."); //$NON-NLS-1$ //$NON-NLS-2$
        }
        
        try {          
                inputStream = new FileInputStream(configFile);
                return inputStream;

        } catch (IOException io) {
            throw new ConfigurationException(io, "Unable to read configuration file " + fileName + ", error in reading file"); //$NON-NLS-1$ //$NON-NLS-2$
        }                               
    }
    
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



