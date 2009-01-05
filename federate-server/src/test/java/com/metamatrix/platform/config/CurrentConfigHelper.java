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

package com.metamatrix.platform.config;

import java.util.Properties;

import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.bootstrap.CurrentConfigBootstrap;
import com.metamatrix.common.config.bootstrap.SystemCurrentConfigBootstrap;
import com.metamatrix.common.messaging.MessageBusConstants;
import com.metamatrix.platform.config.persistence.api.PersistentConnection;
import com.metamatrix.platform.config.persistence.api.PersistentConnectionFactory;
import com.metamatrix.platform.config.persistence.impl.file.FilePersistentConnection;

public class CurrentConfigHelper {

	/**
	 * Constructor for CurrentConfigHelper.
	 */
	protected CurrentConfigHelper() {
		super();
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
		sysProps.put(SystemCurrentConfigBootstrap.NO_CONFIGURATION, "none");  //$NON-NLS-1$
 		sysProps.put(MessageBusConstants.MESSAGE_BUS_TYPE, MessageBusConstants.TYPE_NOOP);
		sysProps.put(CurrentConfigBootstrap.CONFIGURATION_READER_CLASS_PROPERTY_NAME, "com.metamatrix.platform.config.spi.xml.XMLCurrentConfigurationReader"); //$NON-NLS-1$
		sysProps.put("metamatrix.security.password.PasswordKeyStore", "c3B1dG5pazEz"); //$NON-NLS-1$ //$NON-NLS-2$
	
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
		Properties sysProps = System.getProperties();

		sysProps.putAll(properties);		
		System.setProperties(sysProps);	
		
		cleanModelFile(principal, fileName, path);		
		CurrentConfiguration.reset();
		
		createSystemProperties(fileName, path);
		
        CurrentConfiguration.performSystemInitialization(true);
		CurrentConfiguration.getConfiguration();

	}
	
	
	
   protected static void cleanModelFile(String principal, String fileName, String path) throws Exception {
			Properties props = createSystemProperties("config.xml", path);

      	    deleteModel(Configuration.NEXT_STARTUP_ID, props, principal);
      	    
       	    deleteModel(Configuration.STARTUP_ID, props, principal);
  	
    }
    
    protected static Properties createSystemProperties(String fileName, String path) {
    		Properties cfg_props = createProperties(fileName, path);
    		    		
    		// these system props need to be set for the CurrentConfiguration call

     		Properties sysProps = System.getProperties();
     		sysProps.putAll(cfg_props);
     		System.setProperties(sysProps);
     		
     		return cfg_props;
     		     		
    }
    
    
	protected static Properties createProperties(String fileName, String path) {
		
     		          
            Properties props = new Properties();
             
            if (fileName != null) {
            	props.setProperty(FilePersistentConnection.CONFIG_NS_FILE_NAME_PROPERTY,  fileName );
            	props.setProperty(PersistentConnectionFactory.PERSISTENT_FACTORY_NAME, PersistentConnectionFactory.FILE_FACTORY_NAME);
            }
            
            if (path != null) {
	            props.setProperty(FilePersistentConnection.CONFIG_FILE_PATH_PROPERTY, path  );
            }
            

			return props;
			
			
			
	}
	


    public static void deleteModel(ConfigurationID configID, Properties props,
			String principal) throws Exception {

		PersistentConnectionFactory pf = PersistentConnectionFactory
				.createPersistentConnectionFactory(props);

		PersistentConnection readin = pf.createPersistentConnection();

		readin.delete(configID, principal);

	}
    
	
}
