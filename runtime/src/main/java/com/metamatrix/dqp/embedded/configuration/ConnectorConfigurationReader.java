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

import java.io.InputStream;
import java.util.Properties;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationObjectEditor;
import com.metamatrix.common.config.api.ConnectorArchive;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.config.api.ConnectorBindingType;
import com.metamatrix.common.config.model.BasicConfigurationObjectEditor;
import com.metamatrix.common.config.xml.XMLConfigurationImportExportUtility;
import com.metamatrix.core.util.ObjectConverterUtil;


/** 
 * This reader object is used to read the connector types and connector bindings.
 * This is light weight reader than other implementations with out much baggage, which
 * will be used in the DQP.
 * 
 * @since 4.3
 */
public class ConnectorConfigurationReader {
    
    private static final XMLConfigurationImportExportUtility UTIL = new XMLConfigurationImportExportUtility();

    /**
     *  Load the connector bindings from the given char array contents.
     * @param contents
     * @return
     * @throws MetaMatrixComponentException
     * @since 4.3
     */
    public static ConnectorBinding loadConnectorBinding(String name, char[] contents)  
        throws MetaMatrixComponentException {
        try {
            InputStream in = ObjectConverterUtil.convertToInputStream(new String(contents));
            return UTIL.importConnectorBinding(in, new BasicConfigurationObjectEditor(), name);
        } catch (Exception e) {
            throw new MetaMatrixComponentException(e);
        } 
    }
    
    /**
     * Load/build connector binding from the given properties 
     * @param bindingName - binding Name
     * @param props - Properties for the Connector Binding
     * @param type - Connector Binding Type
     * @return ConnectorBinding
     * @throws MetaMatrixComponentException
     */
    public static ConnectorBinding loadConnectorBinding(String bindingName, Properties props, ConnectorBindingType type)  
        throws MetaMatrixComponentException {
        ConfigurationObjectEditor editor = new BasicConfigurationObjectEditor(true);
        ConnectorBinding binding = editor.createConnectorComponent(Configuration.NEXT_STARTUP_ID, (ComponentTypeID)type.getID(), bindingName, null);
        return (ConnectorBinding)editor.modifyProperties(binding, props, ConfigurationObjectEditor.SET);        
    }    
    
    /**
     *  Load the connector type from the given char array contents.
     * @param contents
     * @return
     * @throws MetaMatrixComponentException
     * @since 4.3
     */
    public static ConnectorBindingType loadConnectorType(char[] contents) 
        throws MetaMatrixComponentException {
        InputStream in = ObjectConverterUtil.convertToInputStream(new String(contents));
        try {
            return (ConnectorBindingType)UTIL.importComponentType(in, new BasicConfigurationObjectEditor(), null);
        } catch (Exception e) {
            throw new MetaMatrixComponentException(e);
        }
    }

    /**
     * Load the Connector Archive from the given byte arrary contents 
     * @param contents
     * @return Connector Archive
     * @throws MetaMatrixComponentException
     * @since 4.3.2
     */
    public static ConnectorArchive loadConnectorArchive(byte[] contents) 
        throws MetaMatrixComponentException {
        InputStream in = ObjectConverterUtil.convertToInputStream(contents);
        try {
            return UTIL.importConnectorArchive(in, new BasicConfigurationObjectEditor());
        } catch (Exception e) {
            throw new MetaMatrixComponentException(e);
        }        
    }
        
    /**
     * Add or modify property for the given cnnector binding and return the modified 
     * connector binding. It should be the same object. 
     * @param binding - Connector binding
     * @param propName - Property Name
     * @param value - Property Value 
     * @return modified connector binding; same original reference
     */
    public static ConnectorBinding addConnectorBindingProperty(ConnectorBinding binding, String propName, Object value) {
        BasicConfigurationObjectEditor editor = new BasicConfigurationObjectEditor();
        Properties props = binding.getProperties();
        props.put(propName, value);        
        editor.modifyProperties(binding, props, BasicConfigurationObjectEditor.SET);
        return binding;
    }
    
    /**
     * Add or modify properties for the given cnnector binding and return the modified 
     * connector binding. It should be the same object. 
     * @param binding - Connector binding
     * @param properties - Properties to set
     * @return modified connector binding; same original reference
     */
    public static ConnectorBinding addConnectorBindingProperties(ConnectorBinding binding, Properties properties) {
        BasicConfigurationObjectEditor editor = new BasicConfigurationObjectEditor();
        Properties props = binding.getProperties();
        props.putAll(properties);        
        editor.modifyProperties(binding, props, BasicConfigurationObjectEditor.SET);
        return binding;
    }

}

