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

package com.metamatrix.connector.xmlsource.soap;

import java.io.File;
import java.util.Properties;

import junit.framework.TestCase;

import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;

import com.metamatrix.cdk.api.EnvironmentUtility;
import com.metamatrix.core.util.UnitTestUtil;


/** 
 */
public class TestSoapConnection extends TestCase{
    
    public void testNoWSDL() throws Exception {
        Properties props = new Properties();
        ConnectorEnvironment env = EnvironmentUtility.createEnvironment(props, false);
        
        try {
            new SoapConnection(env);
            fail("WSDL is not set; must have failed"); //$NON-NLS-1$
        } catch (ConnectorException e) {
            //pass
        }
    }

    public void testWSDLLoad() throws Exception {
        File wsdlFile = new File(UnitTestUtil.getTestDataPath()+"/stockquotes.xml"); //$NON-NLS-1$
        Properties props = new Properties();
        props.setProperty("wsdl", wsdlFile.toURL().toString()); //$NON-NLS-1$  
        ConnectorEnvironment env = EnvironmentUtility.createEnvironment(props, false);
        
        SoapConnection conn = new SoapConnection(env);
        assertTrue(conn.isConnected());
        assertEquals("StockQuotes", conn.service.getServiceName().getLocalPart()); //$NON-NLS-1$
        assertTrue("Operation Not Found", conn.operationsMap.containsKey("GetQuote")); //$NON-NLS-1$ //$NON-NLS-2$
        assertFalse("Operation Should Not have Found", conn.operationsMap.containsKey("GetQuoteX")); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals(4, conn.operationsMap.size());            
        
        // release connection
        conn.close();
        assertFalse(conn.isConnected());
    }
    
    public void testFindOperation() throws Exception {
        File wsdlFile = new File(UnitTestUtil.getTestDataPath()+"/stockquotes.xml"); //$NON-NLS-1$
        Properties props = new Properties();
        props.setProperty("wsdl", wsdlFile.toURL().toString()); //$NON-NLS-1$  
        ConnectorEnvironment env = EnvironmentUtility.createEnvironment(props, false);
        
        SoapConnection conn = new SoapConnection(env);
        ServiceOperation operation = conn.findOperation("GetQuote"); //$NON-NLS-1$
        assertNotNull("failed to find the operation", operation); //$NON-NLS-1$
    }    
}
