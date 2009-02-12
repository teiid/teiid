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
import java.util.Properties;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.config.api.ConnectorArchive;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.config.api.ConnectorBindingType;
import com.metamatrix.common.config.util.ConfigurationPropertyNames;
import com.metamatrix.common.config.xml.XMLConfigurationImportExportUtility;


/** 
 * Writer utility for dealing with Connector Bindings and Connector Types
 * @since 4.3
 */
public class ConnectorConfigurationWriter {
    private static final XMLConfigurationImportExportUtility UTIL = new XMLConfigurationImportExportUtility();
    
    /**
     * Convert the Connector binding supplied into known CDK (XML) format 
     * @param binding - Connector Binding to be converted.
     * @return char[] - converted data object
     * @since 4.3
     */
    public static char[] writeToCharArray(ConnectorBinding[] bindings, ConnectorBindingType[] types) 
        throws MetaMatrixComponentException{           
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
            UTIL.exportConnectorBindings(bos, bindings, types, getPropertiesForExporting());        
            char[] contents = bos.toString().toCharArray();
            bos.close();
            return contents;
        } catch (Exception e) {
            throw new MetaMatrixComponentException(e);
        }
    }

  
    /**
     * Convert the Connector type supplied into known CDK (XML) format 
     * @param binding - Connector Binding to be converted.
     * @return char[] - converted data object
     * @since 4.3
     */    
    public static char[] writeToCharArray(ConnectorBindingType[] bindingTypes) 
        throws MetaMatrixComponentException {        
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);        
            UTIL.exportComponentTypes(bos, bindingTypes, getPropertiesForExporting());     
            char[] contents = bos.toString().toCharArray();
            bos.close();
            return contents;
        } catch (Exception e) {
            throw new MetaMatrixComponentException(e);
        }
    }

    /**
     * Convert the Connector type supplied into known CDK (XML) format 
     * @param binding - Connector Binding to be converted.
     * @return char[] - converted data object
     * @since 4.3
     */    
    public static byte[] writeToByteArray(ConnectorArchive connectorArchive) 
        throws MetaMatrixComponentException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
        try {            
            UTIL.exportConnectorArchive(bos, connectorArchive, getPropertiesForExporting());            
            byte[] contents = bos.toByteArray();
            return contents;
        } catch (Exception e) {
            throw new MetaMatrixComponentException(e);
        } finally {
            try{bos.close();}catch(Exception e) {}
        }
    }    
    
    /** 
     * @return
     * @since 4.3
     */
    static Properties getPropertiesForExporting() {
        Properties properties = new Properties();
        properties.put(ConfigurationPropertyNames.APPLICATION_CREATED_BY, "EmbeddedAdmin"); //$NON-NLS-1$
        properties.put(ConfigurationPropertyNames.APPLICATION_VERSION_CREATED_BY, "4.3"); //$NON-NLS-1$
        properties.put(ConfigurationPropertyNames.USER_CREATED_BY, "dqpadmin"); //$NON-NLS-1$
        return properties;
    }    
}
