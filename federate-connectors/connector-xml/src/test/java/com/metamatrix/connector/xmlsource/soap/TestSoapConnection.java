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

package com.metamatrix.connector.xmlsource.soap;

import java.io.File;
import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.cdk.api.EnvironmentUtility;
import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.exception.ConnectorException;


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
        
        try {
            SoapConnection conn = new SoapConnection(env);
            assertTrue(conn.isConnected());
            assertEquals("StockQuotes", conn.service.getServiceName().getLocalPart()); //$NON-NLS-1$
            assertTrue("Operation Not Found", conn.operationsMap.containsKey("GetQuote")); //$NON-NLS-1$ //$NON-NLS-2$
            assertFalse("Operation Should Not have Found", conn.operationsMap.containsKey("GetQuoteX")); //$NON-NLS-1$ //$NON-NLS-2$
            assertEquals(4, conn.operationsMap.size());            
            
            // release connection
            conn.release();
            assertFalse(conn.isConnected());
        } catch (ConnectorException e) {
            e.printStackTrace();
            fail(e.getMessage()); 
        }
    }
    
    public void testFindOperation() throws Exception {
        File wsdlFile = new File(UnitTestUtil.getTestDataPath()+"/stockquotes.xml"); //$NON-NLS-1$
        Properties props = new Properties();
        props.setProperty("wsdl", wsdlFile.toURL().toString()); //$NON-NLS-1$  
        ConnectorEnvironment env = EnvironmentUtility.createEnvironment(props, false);
        
        try {
            SoapConnection conn = new SoapConnection(env);
            ServiceOperation operation = conn.findOperation("GetQuote"); //$NON-NLS-1$
            if (operation == null) {
                fail("failed to find the operation"); //$NON-NLS-1$
            }
        }catch(ConnectorException e) {
            fail();
        }        
    }    
}
