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

package com.metamatrix.dqp.embedded.configuration;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.util.Properties;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.application.exception.ApplicationInitializationException;
import com.metamatrix.common.config.api.ComponentDefnID;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.config.api.ConnectorBindingType;
import com.metamatrix.common.config.model.BasicConfiguration;
import com.metamatrix.common.config.model.BasicConfigurationObjectEditor;
import com.metamatrix.common.config.model.BasicServiceComponentDefn;
import com.metamatrix.common.config.model.ConfigurationModelContainerAdapter;
import com.metamatrix.common.config.model.ConfigurationModelContainerImpl;
import com.metamatrix.common.protocol.URLHelper;


/** 
 * A utility to write/convert the ServerConfig file from its object form. 
 * @since 4.3
 */
public class ServerConfigFileWriter {
    private static final String QUERY_SERVICE= "QueryService"; //$NON-NLS-1$
    
    /**
     * Write the Server Configuration into supplied file, in the known Server 
     * Configuration format as in config.xml (This is clearly a hack without Mr.Halbert's help) 
     * @param properties
     * @return
     * @throws ApplicationInitializationException
     * @since 4.3
     */
    public static char[] writeToCharArray(ConfigurationModelContainer model) 
        throws MetaMatrixComponentException{
    
        try {
            ConfigurationModelContainerAdapter configAdapter = new ConfigurationModelContainerAdapter();            
            ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
            configAdapter.writeConfigurationModel(bos, model, "DQP"); //$NON-NLS-1$            
            char[] contents = bos.toString().toCharArray();
            bos.close();
            return contents;
        } catch (Exception e) {
            throw new MetaMatrixComponentException(e);
        }
    }
    
    /**
     * Write the Server Configuration into supplied file, in the known Server 
     * Configuration format as in config.xml 
     * @param properties
     * @param configFileURL
     * @throws ApplicationInitializationException
     * @since 4.3
     */
    public static void write(ConfigurationModelContainer model, URL configFileURL) 
        throws MetaMatrixComponentException{    
        ConfigurationModelContainerAdapter configAdapter = new ConfigurationModelContainerAdapter();
        OutputStream out = null;
        try {
            String configFile = configFileURL.toString()+"?action=write"; //$NON-NLS-1$
            configFileURL = URLHelper.buildURL(configFile);
            out = configFileURL.openConnection().getOutputStream();
            configAdapter.writeConfigurationModel(out, model, "DQP"); //$NON-NLS-1$            
        } catch (Exception e) {
            throw new MetaMatrixComponentException(e);
        } finally {
            if (out != null) {                
                try {out.close();} catch (IOException e) {}
            }
        }
    }    
    
    /**
     * Add the Connector Bindings to the Configuration object and return the modified
     * object  
     * @param model - Configuration to be changed.
     * @param binding - Connector binding to add/replace
     * @return modified object
     * @since 4.3
     */
    public static ConfigurationModelContainer addConnectorBinding(ConfigurationModelContainer model, ConnectorBinding binding) 
        throws MetaMatrixComponentException{
        ConfigurationModelContainerImpl config = (ConfigurationModelContainerImpl)model;
        ((BasicConfiguration)config.getConfiguration()).addComponentDefn(binding);
        return config;
    }
    
    /**
     * Remove the Connector Bindings to the Configuration object and return the modified
     * object  
     * @param model - Configuration to be changed.
     * @param binding - Connector binding to add/replace
     * @return modified object
     * @since 4.3
     */
    public static ConfigurationModelContainer deleteConnectorBinding(ConfigurationModelContainer model, ConnectorBinding binding)
        throws MetaMatrixComponentException{
        ConfigurationModelContainerImpl config = (ConfigurationModelContainerImpl)model;
        ((BasicConfiguration)config.getConfiguration()).removeComponentObject((ComponentDefnID)binding.getID());
        return config;        
    }
    
    /**
     * Add the connector Type to the configuration object supplied and return the modified
     * object 
     * @param model - current configuration to be modified
     * @param type - connector type to be added/replaced
     * @return - modified configuration
     * @since 4.3
     */
    public static ConfigurationModelContainer addConnectorType(ConfigurationModelContainer model, ConnectorBindingType type)
        throws MetaMatrixComponentException{
        ConfigurationModelContainerImpl config = (ConfigurationModelContainerImpl)model;
        config.addComponentType(type);
        return config;
    }
    
    
    /**
     * Delete the connector Type to the configuration object supplied and return the modified
     * object 
     * @param model - current configuration to be modified
     * @param type - connector type to be added/replaced
     * @return - modified configuration
     * @since 4.3
     */
    public static ConfigurationModelContainer deleteConnectorType(ConfigurationModelContainer model, ConnectorBindingType type)
        throws MetaMatrixComponentException{
        ConfigurationModelContainerImpl config = (ConfigurationModelContainerImpl)model;
        config.remove(type.getID());
        return config;
    }
    
    /**
     * Add the given property to the configuration object and return the modified object 
     * @param model - model to be modified
     * @param propertyName - Property Name
     * @param propertyValue - value to be added/replaced.
     * @return - modified configuration object
     * @since 4.3
     */
    public static ConfigurationModelContainer addProperty(ConfigurationModelContainer model, String propertyName, String propertyValue)
        throws MetaMatrixComponentException {
        BasicServiceComponentDefn component = (BasicServiceComponentDefn)model.getConfiguration().getServiceComponentDefn(QUERY_SERVICE);        
        BasicConfigurationObjectEditor editor = new BasicConfigurationObjectEditor();
        
        Properties currentProperties = component.getProperties();
        currentProperties.put(propertyName, propertyValue);
        editor.modifyProperties(component, currentProperties, BasicConfigurationObjectEditor.SET);
        
        return model;
    }
    
    
    /**
     * Add the specified properties to the configuration object and return the modified object 
     * @param model - model to be modified
     * @param properties - properties to add/replace.
     * @return - modified configuration object
     * @since 4.3
     */
    public static ConfigurationModelContainer addProperties(ConfigurationModelContainer model, Properties properties)
        throws MetaMatrixComponentException {
        BasicServiceComponentDefn component = (BasicServiceComponentDefn)model.getConfiguration().getServiceComponentDefn(QUERY_SERVICE);        
        BasicConfigurationObjectEditor editor = new BasicConfigurationObjectEditor();
        
        Properties currentProperties = component.getProperties();
        currentProperties.putAll(properties);
        
        editor.modifyProperties(component, currentProperties, BasicConfigurationObjectEditor.SET);
        
        return model;
    }
}
