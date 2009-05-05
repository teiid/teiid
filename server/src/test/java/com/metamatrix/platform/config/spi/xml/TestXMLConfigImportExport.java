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
import java.util.Properties;

import com.metamatrix.common.config.api.ComponentDefnID;
import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.ConfigurationObjectEditor;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.config.api.VMComponentDefn;
import com.metamatrix.common.config.api.VMComponentDefnID;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.config.model.BasicConfigurationObjectEditor;
import com.metamatrix.common.config.xml.XMLConfigurationImportExportUtility;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.core.util.FileUtils;
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
	 * This test the import operation that the console will be doing
	 * when importing a new configuration
	 */
	private void helperImportConnectorBinding(String file) throws Exception {    	
    	printMsg("Starting helperImportConnectorBinding");    	 //$NON-NLS-1$

	     	File connFile = new File(getPath(), file);
	      		          
	    	
	    	InputStream inputStream = readFile(connFile.getPath());
	    	
	   	
	    	XMLConfigurationImportExportUtility io = new XMLConfigurationImportExportUtility();
	    
	        
	        ConfigurationObjectEditor editor =  new BasicConfigurationObjectEditor(false);
	                     
	        ConnectorBinding defn = io.importConnectorBinding(inputStream, editor, null);
	        
            if (defn == null) {
                fail("No connector binding was imported"); //$NON-NLS-1$
            }

	        ConfigurationModelContainer config = getWriter().getConfigurationModel();

	        ConnectorBinding newdefn = getEditor().createConnectorComponent(Configuration.NEXT_STARTUP_ID, defn, defn.getFullName(), defn.getRoutingUUID());
            
            commit();
            
            VMComponentDefn vmdefn = (VMComponentDefn) config.getConfiguration().getVMComponentDefns().iterator().next();
			// deploys the binding if the psc is deployed
            getEditor().deployServiceDefn(config.getConfiguration(), newdefn, (VMComponentDefnID) vmdefn.getID());
	         				
            commit();
			
			ConnectorBinding newDefn = (ConnectorBinding) getWriter().getConfigurationModel().getConfiguration().getComponentDefn((ComponentDefnID)defn.getID());	
			if (newDefn == null) {
				fail("Import of connector binding was not successfull, no obect found when reading."); //$NON-NLS-1$
			}
			
			HelperTestConfiguration.validateConnectorBinding(newDefn);
 		
    	printMsg("Completed helperImportConnectorBinding"); //$NON-NLS-1$
 
    }
    
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
    
}



