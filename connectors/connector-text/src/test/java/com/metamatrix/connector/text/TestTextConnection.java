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

package com.metamatrix.connector.text;

import java.util.Map;
import java.util.Properties;

import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;

import junit.framework.TestCase;

import com.metamatrix.cdk.api.EnvironmentUtility;
import com.metamatrix.core.util.UnitTestUtil;

/**
 */
public class TestTextConnection extends TestCase {

    /**
     * @param name
     */
    public TestTextConnection(String name) {
        super(name);
    }
    
    public void testDefect10371() throws Exception {    
        Properties props = new Properties();
        String descFile = UnitTestUtil.getTestDataPath() + "/EmployeeTestDataSalary.txt"; //$NON-NLS-1$
        props.put(TextPropertyNames.DESCRIPTOR_FILE, descFile);
        
        ConnectorEnvironment env = EnvironmentUtility.createEnvironment(props, false);
        TextConnector txr = new TextConnector();
        txr.start(env);
        TextConnection conn = (TextConnection)txr.getConnection(null);
        
        Map actualProps = conn.metadataProps;
        assertNotNull(actualProps);
    }

    /**
     * Test partial startup property - test default - should default to allow partial startup
     */
    public void testCase4284Default() throws Exception {        
        Properties props = new Properties();
        String descFile = UnitTestUtil.getTestDataPath() + "/testDescriptorDelimited.txt"; //$NON-NLS-1$
            
        props.put(TextPropertyNames.DESCRIPTOR_FILE, descFile);
        
        ConnectorEnvironment env = EnvironmentUtility.createEnvironment(props, false);
        TextConnector txr = new TextConnector();
    	txr.start(env);
    }

    /**
     * Test partial startup property - disallow partial startup
     */
    public void testCase4284DisallowPartial() throws Exception {        
        Properties props = new Properties();
        String descFile = UnitTestUtil.getTestDataPath() + "/testDescriptorDelimited.txt"; //$NON-NLS-1$
            
        props.put(TextPropertyNames.DESCRIPTOR_FILE, descFile);
        props.put(TextPropertyNames.PARTIAL_STARTUP_ALLOWED, "false"); //$NON-NLS-1$
        
        ConnectorEnvironment env = EnvironmentUtility.createEnvironment(props, false);
        TextConnector txr = new TextConnector();
        try {
        	txr.start(env);
        	fail("expected exception"); //$NON-NLS-1$
        } catch (ConnectorException e) {
            String m1 = "Error parsing property string text.library2.location"; //$NON-NLS-1$
            String m2 = "Data file not found at this location:"; //$NON-NLS-1$
            String m3 = UnitTestUtil.getTestDataPath() + "/libraryDelimited2.txt, for group TEXT.LIBRARY2"; //$NON-NLS-1$
            
        	String message = e.getMessage();
            if ( (message.indexOf(m1) == -1) || message.indexOf(m2) == -1 || message.indexOf(m3) == -1)
                fail("Error message doesnt match the expected message"); //$NON-NLS-1$
                //assertEquals("Error message doesnt match the expected message","Error parsing property string text.library2.location = testdata/libraryDelimited2.txt: Data file not found at this location: testdata/libraryDelimited2.txt, for group TEXT.LIBRARY2",message);
        }
    }

    /**
     * Test partial startup property - allow partial startup
     */
    public void testCase4284AllowPartial() throws Exception {        
        Properties props = new Properties();
        String descFile = UnitTestUtil.getTestDataPath() + "/testDescriptorDelimited.txt"; //$NON-NLS-1$
        
        props.put(TextPropertyNames.DESCRIPTOR_FILE, descFile);
        props.put(TextPropertyNames.PARTIAL_STARTUP_ALLOWED, "true"); //$NON-NLS-1$
        
        ConnectorEnvironment env = EnvironmentUtility.createEnvironment(props, false);
        TextConnector txr = new TextConnector();
    	txr.start(env);
    }

}
