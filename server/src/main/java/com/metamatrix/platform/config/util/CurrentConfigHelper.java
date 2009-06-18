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

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.platform.config.persistence.api.PersistentConnection;
import com.metamatrix.platform.config.persistence.api.PersistentConnectionFactory;
import com.metamatrix.platform.config.persistence.impl.file.FilePersistentConnection;
import com.metamatrix.platform.config.spi.xml.XMLConfigurationMgr;
import com.metamatrix.platform.config.spi.xml.XMLCurrentConfigurationReader;

/**
 * The CurrentConfigHelper is used to load a configuration into memory when a repository isn't available.
 * @author vanhalbert
 *
 */
public class CurrentConfigHelper {

	public void loadMetaMatrixPropertiesIntoSystem() throws Exception {
		loadMetaMatrixPropertiesIntoSystem(CurrentConfiguration.BOOTSTRAP_FILE_NAME);
	}
	
	public void loadMetaMatrixPropertiesIntoSystem(String filename) throws Exception {
		Properties bootstrapProps = new Properties();
        InputStream bootstrapPropStream =  this.getClass().getClassLoader().getResourceAsStream(filename);

        if (bootstrapPropStream == null) {
            bootstrapPropStream = new FileInputStream(new File(filename));

        }
		bootstrapProps.load(bootstrapPropStream);
       	bootstrapProps.remove(CurrentConfiguration.CONFIGURATION_READER_CLASS_PROPERTY_NAME);
       	
        Properties sys = System.getProperties();
        sys.putAll(bootstrapProps);
        System.setProperties(sys);
	}


	/**
	 * init will do the following:
	 * <li>remove existing config_ns.xml file</li>
	 * <li>set required system properties for CurrentConfiguration<li>
	 * <li>reload CurrentConfiguration with new information from <code<fileName</code>
	 * @param fileName is the configuration file to use; if contains full path, set path to null
	 * @param path can optionally specify the path seperate from the fileName
	 * @param principal is the user initializing configuration

	 */
	public static void initConfig(String fileName, String path, String principal) throws Exception {
		Properties sysProps = new Properties();
		initConfig(fileName, path, sysProps, principal);
	}
	
	public static void initXMLConfig(String fileName, String path, String principal) throws Exception {
		Properties sysProps = new Properties();
 		sysProps.put(CurrentConfiguration.CONFIGURATION_READER_CLASS_PROPERTY_NAME, XMLCurrentConfigurationReader.class.getName());
		initConfig(fileName, path, sysProps, principal);
	}
	
	/**
	 * init will do the following:
	 * <li>remove existing config_ns.xml file</li>
	 * <li>set required system properties for CurrentConfiguration<li>
	 * <li>reload CurrentConfiguration with new information from <code<fileName</code>
	 * @param fileName is the configuration file to use; if contains full path, set path to null
	 * @param path can optionally specify the path seperate from the fileName
	 * @param properties will be set as the System properties
	 * @param principal is the user initializing configuration
	 */		
	static void initConfig(String fileName, String path, Properties properties, String principal) throws Exception {
		File f = new File(path, fileName);
 		if (!f.exists()) {
 			throw new Exception("Configuration file " + f.getAbsolutePath() + " does not exist"); //$NON-NLS-1$ //$NON-NLS-2$
 		}
		Properties sysProps = PropertiesUtils.clone(System.getProperties(), false);

		sysProps.putAll(properties);		
		System.setProperties(sysProps);	
		
		cleanModelFile(principal, fileName, path);		
		
		createSystemProperties(fileName, path);
		
		CurrentConfiguration.reset();
		
		XMLConfigurationMgr.getInstance().reset();

		
		CurrentConfiguration.getInstance().getConfiguration();
	}
	
	
	
   protected static void cleanModelFile(String principal, String fileName, String path) throws Exception {
			Properties props = createSystemProperties(fileName, path);

      	    deleteModel(Configuration.NEXT_STARTUP_ID, props, principal);
      	    
    }
    
    protected static Properties createSystemProperties(String fileName, String path) {
        Properties props = new Properties();
        
        props.setProperty(CurrentConfiguration.CONFIGURATION_READER_CLASS_PROPERTY_NAME, XMLCurrentConfigurationReader.class.getName());
        
        if (fileName != null) {
        	props.setProperty(FilePersistentConnection.CONFIG_NS_FILE_NAME_PROPERTY,  fileName );
        	props.setProperty(PersistentConnectionFactory.PERSISTENT_FACTORY_NAME, PersistentConnectionFactory.FILE_FACTORY_NAME);
        }
        
        if (path != null) {
            props.setProperty(FilePersistentConnection.CONFIG_FILE_PATH_PROPERTY, path  );
        }
    		    		
		// these system props need to be set for the CurrentConfiguration call

 		Properties sysProps = System.getProperties();
 		sysProps.putAll(props);
 		System.setProperties(sysProps);
 		
 		return props;
    }

    public static void deleteModel(ConfigurationID configID, Properties props,
			String principal) throws Exception {

		PersistentConnectionFactory pf = new PersistentConnectionFactory(props);

		PersistentConnection readin = pf.createPersistentConnection(false);

		readin.delete(configID, principal);
		readin.commit();
		readin.close();

	}
    
	
}
