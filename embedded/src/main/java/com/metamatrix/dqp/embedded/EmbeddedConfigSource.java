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

package com.metamatrix.dqp.embedded;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.google.inject.Binder;
import com.metamatrix.common.application.ApplicationService;
import com.metamatrix.common.application.DQPConfigSource;
import com.metamatrix.common.application.exception.ApplicationInitializationException;
import com.metamatrix.common.protocol.URLHelper;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.dqp.embedded.services.EmbeddedBufferService;
import com.metamatrix.dqp.embedded.services.EmbeddedConfigurationService;
import com.metamatrix.dqp.embedded.services.EmbeddedDataService;
import com.metamatrix.dqp.embedded.services.EmbeddedMetadataService;
import com.metamatrix.dqp.embedded.services.EmbeddedTrackingService;
import com.metamatrix.dqp.embedded.services.EmbeddedTransactionService;
import com.metamatrix.dqp.embedded.services.EmbeddedVDBService;
import com.metamatrix.dqp.service.DQPServiceNames;
import com.metamatrix.jdbc.EmbeddedDataSource;
import com.metamatrix.jdbc.JDBCPlugin;

/**
 * This class is main hook point for the Embedded DQP configuration. This classe's
 * responsibility is to encapsulate the knowledge of creating of the various application
 * services used the DQP.
 * 
 */
public class EmbeddedConfigSource implements DQPConfigSource {
	private static final String SERVER_CONFIG_FILE_EXTENSION = ".properties"; //$NON-NLS-1$
    
	private Properties props;
    private boolean useTxn;
    
    /**  
    * Based the configuration file load the DQP services
    * @param configFile
    * @throws ApplicationInitializationException
    */    
    public EmbeddedConfigSource(Properties connectionProperties) {
            
    	URL dqpURL = (URL)connectionProperties.get(EmbeddedDataSource.DQP_BOOTSTRAP_FILE);            
        if(dqpURL == null) {
            throw new MetaMatrixRuntimeException(JDBCPlugin.Util.getString("LocalTransportHandler.No_configuration_file_set_in_property", DQPEmbeddedProperties.DQP_BOOTSTRAP_PROPERTIES_FILE)); //$NON-NLS-1$
        }

        String dqpFileName = dqpURL.toString().toLowerCase(); 
        if (!dqpFileName.endsWith(SERVER_CONFIG_FILE_EXTENSION)) {
            throw new MetaMatrixRuntimeException(JDBCPlugin.Util.getString("LocalTransportHandler.Invalid_config_file_extension", dqpFileName) ); //$NON-NLS-1$                    
        }    	
    	
        String dqpURLString = dqpURL.toString(); 
        try {
            dqpURL = URLHelper.buildURL(dqpURLString);
            InputStream in = dqpURL.openStream();
            if (in == null) {
            	throw new MetaMatrixRuntimeException(DQPEmbeddedPlugin.Util.getString("EmbeddedConfigSource.Can_not_load_config_file_2", dqpURL)); //$NON-NLS-1$
            }

            // Load the "dqp.properties" file.
            props = new Properties();
            props.load(in);
            in.close();

            // Merge any user properties with the mm.properties
            if (connectionProperties != null) {
                props.putAll(connectionProperties);
            }
            
            // this will resolve any nested properties in the properties
            // file; this created for testing purpose
            props = PropertiesUtils.resolveNestedProperties(props);
            
            // create a unique identity number for this DQP
            props.setProperty(DQPEmbeddedProperties.DQP_IDENTITY, getDQPIdentity());
            
            // create a workspace directory for the DQP
            props.setProperty(DQPEmbeddedProperties.DQP_TMPDIR, getWorkspaceDirectory());
            
            // This is context of where the dqp.properties loaded, VDB are defined relative to
            // this path.
            props.put(DQPEmbeddedProperties.DQP_BOOTSTRAP_PROPERTIES_FILE, dqpURL);
            
            useTxn = PropertiesUtils.getBooleanProperty(props, EmbeddedTransactionService.TRANSACTIONS_ENABLED, true);
        } catch (IOException e) {
            throw new MetaMatrixRuntimeException(e);
        }        
    }  

    /**
     * create an identity for the DQP instance in this JVM 
     * @return int a unique number for this JVM
     */
    String getDQPIdentity() {
        String id = System.getProperty(DQPEmbeddedProperties.DQP_IDENTITY, "0"); //$NON-NLS-1$
        return id;
    }
    
    String getWorkspaceDirectory() {
        return System.getProperty(DQPEmbeddedProperties.DQP_TMPDIR, System.getProperty("java.io.tmpdir")); //$NON-NLS-1$ 
    }

    /** 
     * @see com.metamatrix.common.application.DQPConfigSource#getProperties()
     */
    public Properties getProperties() {
        return this.props;
    }
    
	@Override
	public void updateBindings(Binder binder) {
		
	}

	@Override
	public Map<String, Class<? extends ApplicationService>> getDefaultServiceClasses() {
		Map<String, Class<? extends ApplicationService>> result = new HashMap<String, Class<? extends ApplicationService>>();
		result.put(DQPServiceNames.CONFIGURATION_SERVICE, EmbeddedConfigurationService.class);
		result.put(DQPServiceNames.TRACKING_SERVICE, EmbeddedTrackingService.class);
		result.put(DQPServiceNames.BUFFER_SERVICE, EmbeddedBufferService.class);
		result.put(DQPServiceNames.VDB_SERVICE, EmbeddedVDBService.class);
		result.put(DQPServiceNames.METADATA_SERVICE, EmbeddedMetadataService.class);
		result.put(DQPServiceNames.DATA_SERVICE, EmbeddedDataService.class);
		if (useTxn) {
			result.put(DQPServiceNames.TRANSACTION_SERVICE, EmbeddedTransactionService.class);
		}
		return result;
	}

}
