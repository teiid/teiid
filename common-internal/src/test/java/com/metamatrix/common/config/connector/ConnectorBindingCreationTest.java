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

package com.metamatrix.common.config.connector;

import java.io.File;
import java.io.FileInputStream;
import java.util.Collection;
import java.util.Properties;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase;

import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.ConfigurationObjectEditor;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.config.model.BasicConfigurationObjectEditor;
import com.metamatrix.common.config.model.ConfigurationModelContainerImpl;
import com.metamatrix.common.config.xml.XMLConfigurationImportExportUtility;
import com.metamatrix.core.util.UnitTestUtil;


/** 
 * Added to test VDB administration Use Cases.
 * 
 * @since 4.3
 */
public class ConnectorBindingCreationTest extends TestCase {

    // Connector binding name constants - Must match name in config.xml
    private static final String ORACLE_ANSI = "Oracle ANSI JDBC Connector"; //$NON-NLS-1$
    private static final String ORACLE_8 = "Oracle 8 JDBC Connector"; //$NON-NLS-1$
    private static final String DB2 = "DB2 JDBC Connector"; //$NON-NLS-1$
    private static final String SYBASE_ANSI = "Sybase ANSI JDBC Connector"; //$NON-NLS-1$
    private static final String SYBASE_11= "Sybase 11 JDBC Connector"; //$NON-NLS-1$
    private static final String SQL_SERVER = "SQL Server JDBC Connector"; //$NON-NLS-1$
    private static final String JDBC_GENERIC = "JDBC Connector"; //$NON-NLS-1$
    private static final String MS_ACCESS = "MS Access Connector"; //$NON-NLS-1$
    private static final String JDBC_ODBC = "JDBC ODBC Connector"; //$NON-NLS-1$
    private static final String TEXT_FILE = "Text File Connector"; //$NON-NLS-1$
    private static final String MYSQL = "MySQL JDBC Connector"; //$NON-NLS-1$
    private static final String POSTGRESQL = "PostgreSQL JDBC Connector"; //$NON-NLS-1$
    private static final String APACHE_DERBY_EMBEDDED = "Apache Derby Embedded Connector"; //$NON-NLS-1$
    private static final String APACHE_DERBY_NETWORK = "Apache Derby Network Connector"; //$NON-NLS-1$
    private static final String SECURE_DATA_SOURCE = "Secure Data Source Connector"; //$NON-NLS-1$
    private static final String SOAP = "SOAP Connector"; //$NON-NLS-1$
    private static final String XML_FILE = "XML File Connector"; //$NON-NLS-1$
    private static final String XML_HTTP = "XML HTTP Connector"; //$NON-NLS-1$
    
    private String CONFIG_FILE_PATH = null;
    private ConfigurationModelContainer config;
    
    /** 
     * 
     * @since 5.0
     */
    public ConnectorBindingCreationTest() {
        super();
    }

    /** 
     * @param name
     * @since 5.0
     */
    public ConnectorBindingCreationTest(String name) {
        super(name);
    }

    public void setUp() throws Exception {
        CONFIG_FILE_PATH = UnitTestUtil.getTestDataPath() + "/" + "config.xml"; //$NON-NLS-1$ //$NON-NLS-2$
        File configFile = new File(CONFIG_FILE_PATH);
        config = importConfigurationModel(configFile, Configuration.NEXT_STARTUP_ID);
    }
   
    /**
     * Test that setting properties on an Oracle ANSI connector binding does something.
     *  
     * @throws Exception
     * @since 5.0
     */
    public void testCreateORACLE_ANSIConnectorBinding() throws Exception {
        
        ComponentType ct = config.getComponentType(ORACLE_ANSI); 
        helpTestConnectorBinding(ct, "TestOracleANSI"); //$NON-NLS-1$
    }
 
    /**
     * Test that setting properties on an Oracle 8 connector binding does something.
     *  
     * @throws Exception
     * @since 5.0
     */
    public void testCreateORACLE_8ConnectorBinding() throws Exception {
        
        ComponentType ct = config.getComponentType(ORACLE_8); 
        helpTestConnectorBinding(ct, "ORACLE_8"); //$NON-NLS-1$
    }
 
    /**
     * Test that setting properties on an DB2 connector binding does something.
     *  
     * @throws Exception
     * @since 5.0
     */
    public void testCreateDB2ConnectorBinding() throws Exception {
        
        ComponentType ct = config.getComponentType(DB2); 
        helpTestConnectorBinding(ct, "DB2"); //$NON-NLS-1$
    }
 
    /**
     * Test that setting properties on an Sybase ANSI connector binding does something.
     *  
     * @throws Exception
     * @since 5.0
     */
    public void testCreateSYBASE_ANSIConnectorBinding() throws Exception {
        
        ComponentType ct = config.getComponentType(SYBASE_ANSI); 
        helpTestConnectorBinding(ct, "SYBASE_ANSI"); //$NON-NLS-1$
    }
    
    /**
     * Test that setting properties on an Sybase ANSI connector binding does something.
     *  
     * @throws Exception
     * @since 5.0
     */
    public void testCreateSYBASE_11ConnectorBinding() throws Exception {
        
        ComponentType ct = config.getComponentType(SYBASE_11); 
        helpTestConnectorBinding(ct, "SYBASE_11"); //$NON-NLS-1$
    }
    
    /**
     * Test that setting properties on an Sybase ANSI connector binding does something.
     *  
     * @throws Exception
     * @since 5.0
     */
    public void testCreateSQL_SERVERConnectorBinding() throws Exception {
        
        ComponentType ct = config.getComponentType(SQL_SERVER); 
        helpTestConnectorBinding(ct, "SQL_SERVER"); //$NON-NLS-1$
    }
    
    /**
     * Test that setting properties on an Sybase ANSI connector binding does something.
     *  
     * @throws Exception
     * @since 5.0
     */
    public void testCreateJDBC_GENERICConnectorBinding() throws Exception {
        
        ComponentType ct = config.getComponentType(JDBC_GENERIC); 
        helpTestConnectorBinding(ct, "JDBC_GENERIC"); //$NON-NLS-1$
    }
    
    /**
     * Test that setting properties on an Sybase ANSI connector binding does something.
     *  
     * @throws Exception
     * @since 5.0
     */
    public void testCreateMS_ACCESSConnectorBinding() throws Exception {
        
        ComponentType ct = config.getComponentType(MS_ACCESS); 
        helpTestConnectorBinding(ct, "MS_ACCESS"); //$NON-NLS-1$
    }
    
    /**
     * Test that setting properties on an Sybase ANSI connector binding does something.
     *  
     * @throws Exception
     * @since 5.0
     */
    public void testCreateJDBC_ODBCConnectorBinding() throws Exception {
        
        ComponentType ct = config.getComponentType(JDBC_ODBC); 
        helpTestConnectorBinding(ct, "JDBC_ODBC"); //$NON-NLS-1$
    }
    
    /**
     * Test that setting properties on an Sybase ANSI connector binding does something.
     *  
     * @throws Exception
     * @since 5.0
     */
    public void testCreateTEXT_FILEConnectorBinding() throws Exception {
        
        ComponentType ct = config.getComponentType(TEXT_FILE); 
        helpTestConnectorBinding(ct, "TEXT_FILE"); //$NON-NLS-1$
    }
    
    /**
     * Test that setting properties on an Sybase ANSI connector binding does something.
     *  
     * @throws Exception
     * @since 5.0
     */
    public void testCreateMYSQLConnectorBinding() throws Exception {
        
        ComponentType ct = config.getComponentType(MYSQL); 
        helpTestConnectorBinding(ct, "MYSQL"); //$NON-NLS-1$
    }
    
    /**
     * Test that setting properties on an Sybase ANSI connector binding does something.
     *  
     * @throws Exception
     * @since 5.0
     */
    public void testCreatePOSTGRESQLConnectorBinding() throws Exception {
        
        ComponentType ct = config.getComponentType(POSTGRESQL); 
        helpTestConnectorBinding(ct, "POSTGRESQL"); //$NON-NLS-1$
    }
    
    /**
     * Test that setting properties on an Sybase ANSI connector binding does something.
     *  
     * @throws Exception
     * @since 5.0
     */
    public void testCreateAPACHE_DERBY_EMBEDDEDConnectorBinding() throws Exception {
        
        ComponentType ct = config.getComponentType(APACHE_DERBY_EMBEDDED); 
        helpTestConnectorBinding(ct, "APACHE_DERBY_EMBEDDED"); //$NON-NLS-1$
    }
    
    /**
     * Test that setting properties on an Sybase ANSI connector binding does something.
     *  
     * @throws Exception
     * @since 5.0
     */
    public void testCreateAPACHE_DERBY_NETWORKConnectorBinding() throws Exception {
        
        ComponentType ct = config.getComponentType(APACHE_DERBY_NETWORK); 
        helpTestConnectorBinding(ct, "APACHE_DERBY_NETWORK"); //$NON-NLS-1$
    }
    
    /**
     * Test that setting properties on an Sybase ANSI connector binding does something.
     *  
     * @throws Exception
     * @since 5.0
     */
    public void testCreateSECURE_DATA_SOURCEConnectorBinding() throws Exception {
        
        ComponentType ct = config.getComponentType(SECURE_DATA_SOURCE); 
        helpTestConnectorBinding(ct, "SECURE_DATA_SOURCE"); //$NON-NLS-1$
    }
    
    /**
     * Test that setting properties on an Sybase ANSI connector binding does something.
     *  
     * @throws Exception
     * @since 5.0
     */
    public void testCreateSOAPConnectorBinding() throws Exception {
        
        ComponentType ct = config.getComponentType(SOAP); 
        helpTestConnectorBinding(ct, "SOAP"); //$NON-NLS-1$
    }
    
    /**
     * Test that setting properties on an Sybase ANSI connector binding does something.
     *  
     * @throws Exception
     * @since 5.0
     */
    public void testCreateXML_FILEConnectorBinding() throws Exception {
        
        ComponentType ct = config.getComponentType(XML_FILE); 
        helpTestConnectorBinding(ct, "XML_FILE"); //$NON-NLS-1$
    }
    
    /**
     * Test that setting properties on an Sybase ANSI connector binding does something.
     *  
     * @throws Exception
     * @since 5.0
     */
    public void testCreateXML_HTTPConnectorBinding() throws Exception {
        
        ComponentType ct = config.getComponentType(XML_HTTP); 
        helpTestConnectorBinding(ct, "XML_HTTP"); //$NON-NLS-1$
    }
    
    //===================================================================================
    // Helpers
    //===================================================================================

    /** 
     * Create a connector binding for the specified ComponentType.  Test that connector
     * binding props are same as defaults for that connector type after connector binding
     * is modified to contain the defaults.
     * 
     * @param ct the component type specific to the type of connector binding.
     * @param bindingName a name for the test binding.
     * @since 5.0
     */
    private void helpTestConnectorBinding(ComponentType ct, String bindingName) {
        ConfigurationObjectEditor coe = new BasicConfigurationObjectEditor();
        ConnectorBinding binding = coe.createConnectorComponent(Configuration.NEXT_STARTUP_ID,
                                                                 (ComponentTypeID) ct.getID(),
                                                                 bindingName,
                                                                 null);
        
        Properties props = binding.getProperties();
        assertNotNull("Props are null!", props); //$NON-NLS-1$
        
        Properties defaultProps = ct.getDefaultPropertyValues();
        assertNotNull("Default Props are null!", defaultProps); //$NON-NLS-1$

        binding = (ConnectorBinding)coe.modifyProperties(binding, defaultProps, ConfigurationObjectEditor.ADD);
        
        props = binding.getProperties();
        assertNotEmpty("Assigned connector binding props are empty!", props); //$NON-NLS-1$
        
        assertEquals("Expected Binding props to be equal to default props!", props, defaultProps); //$NON-NLS-1$
    }
 
    /** 
     * Assert props has at least one member.
     * 
     * @param message
     * @param props
     * @since 5.0
     */
    private void assertNotEmpty(String message,
                                Properties props) {
        if ( props == null || props.keySet().size() == 0 ) {
            throw new AssertionFailedError(message);
        }
    }

    /**
     * Import a configuration file to work with.
     *  
     * @param fileToImport
     * @param configID
     * @return
     * @since 5.0
     */
    private ConfigurationModelContainer importConfigurationModel(File fileToImport, ConfigurationID configID) throws Exception {
        Collection configObjects = null;
        ConfigurationObjectEditor editor = new BasicConfigurationObjectEditor(false);
        ConfigurationModelContainerImpl configModel = null;
        XMLConfigurationImportExportUtility io = new XMLConfigurationImportExportUtility();
        FileInputStream inputStream = new FileInputStream(fileToImport);
        configObjects = io.importConfigurationObjects(inputStream, editor, configID.getFullName());
        configModel = new ConfigurationModelContainerImpl();
        configModel.setConfigurationObjects(configObjects);            
        return configModel;
    }
    
}
