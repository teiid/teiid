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

package com.metamatrix.dqp.embedded.configuration;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.config.model.ConfigurationModelContainerAdapter;

/**
 * This class loades the server configuration file <code>ServerConfig.xml</code>
 */
public class ServerConfigFileReader {

    private static final String QUERY_SERVICE= "QueryService"; //$NON-NLS-1$
    private Properties properties = new Properties();
    private Map connectorBindings = new HashMap();
    private Map connectorTypes = new HashMap();
    private ConfigurationModelContainer configuration = null;
    
    /**
     * Load the server configuration
     * @param configFile Server Config File
     */
    public ServerConfigFileReader(URL configFile) throws IOException {                       
        loadConfigFile(configFile);
    }       

    private void loadConfigFile(URL configFile) throws IOException {

        InputStream in = null;
        try {
            in = configFile.openStream();
            ConfigurationModelContainerAdapter configAdapter = new ConfigurationModelContainerAdapter();

            configuration = configAdapter.readConfigurationModel(in, Configuration.NEXT_STARTUP_ID);
           
            // Get DQP properties
            Properties globalProps = configuration.getConfiguration().getProperties();
            
            Properties qs = configuration.getConfiguration().getServiceComponentDefn(QUERY_SERVICE).getProperties();            
            this.properties.putAll(globalProps);
            this.properties.putAll(qs);
            
            // get connector types
            Collection componentTypes = configuration.getComponentTypes().values();
            for (Iterator i = componentTypes.iterator(); i.hasNext();) {
                ComponentType type = (ComponentType)i.next();
                if (type.getComponentTypeCode() == ComponentType.CONNECTOR_COMPONENT_TYPE_CODE) {
                    connectorTypes.put(type.getName(), type);
                }
            }

            // Load connector bindings, do we ever need connector types?
            Collection bindings = configuration.getConfiguration().getConnectorBindings();
            Iterator it = bindings.iterator();
            while(it.hasNext()) {
                ConnectorBinding binding = (ConnectorBinding)it.next();
                connectorBindings.put(binding.getFullName(), binding);
            }            
        } catch (ConfigurationException e) {
            throw new IOException(e.getMessage());
        } finally {
            if (in != null) {
                in.close();
            }
        }
    }
    
    /**
     * Get properties defined for the System/Query Service 
     * @return properties of the system
     * @since 4.3
     */
    public Properties getSystemProperties() {
        return this.properties;
    }
    
    /**
     * Will return a map of connector binding properties if the
     * configuration file contained any,
     * key->binding name    value->Map of binding properties
     * @return map of the connector bindings.
     */
    public Map getConnectorBindings() {
        return connectorBindings;
    }       
    
    /**
     * Get Server Configuration 
     * @return
     * @since 4.3
     */
    public ConfigurationModelContainer getConfiguration() {
        return configuration;
    }
    
    /**
     * Return the Connector types registered with the server configuration 
     * @return
     * @since 4.3
     */
    public Map getConnectorTypes() {
        return connectorTypes;
    }
}
