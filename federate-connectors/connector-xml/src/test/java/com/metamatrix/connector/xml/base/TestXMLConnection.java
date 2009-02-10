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

package com.metamatrix.connector.xml.base;



import junit.framework.TestCase;

import com.metamatrix.connector.api.ExecutionContext;
import com.metamatrix.connector.exception.ConnectorException;


public class TestXMLConnection extends TestCase {
    
    private static final XMLConnector CONNECTOR = ProxyObjectFactory.getDefaultXMLConnector();
    


	public TestXMLConnection() {
		super();
	}


	public TestXMLConnection(String arg0) {
		super(arg0);
	}
    
//	removing hansel while testing clover
/*	
    public static Test suite() {
    	return new CoverageDecorator(XMLConnectionTest.class, new Class[] {XMLConnectionImpl.class}); 
    }
*/    
    
    public void testInit() {
        try {
        	ExecutionContext ctx = ProxyObjectFactory.getDefaultSecurityContext();
        	XMLConnectionImpl connection = (XMLConnectionImpl) CONNECTOR.getConnection(ctx);
        } catch (ConnectorException ex) {
            ex.printStackTrace();
            fail(ex.getMessage());
        }
        
    }
        
    public void testRelease() {
        XMLConnectionImpl conn = getXMLConnection();
        conn.close();        
    }
    
    public void testGetQueryId() {
        XMLConnectionImpl conn = getXMLConnection();
        String id = conn.getQueryId();
        assertNotNull("queryId is null", id);
        assertEquals(id, ProxyObjectFactory.getDefaultExecutionContext().getRequestIdentifier());
    }
    
    public void testGetConnector() {
        XMLConnectionImpl conn = getXMLConnection();
        assertNotNull("XMLConnectionImpl is null", conn.getConnector());
        
    }
    
    private XMLConnectionImpl getXMLConnection() {
        XMLConnectionImpl connection;
        try {
            connection = (XMLConnectionImpl) CONNECTOR.getConnection(ProxyObjectFactory.getDefaultSecurityContext());           
        } catch (Exception e) {
            connection = null;
            assertTrue("The connection is null", false);
        }            
        assertNotNull("XMLConnectionImpl is null", connection);
        return connection;
        
    }
    
}
