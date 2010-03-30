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

package org.teiid.connector.xml;

import junit.framework.TestCase;

import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.xml.file.FakeFileManagedConnectionfactory;
import org.teiid.connector.xml.file.FileConnection;
import org.teiid.connector.xml.file.FileConnector;


public class TestXMLConnector extends TestCase {
    
    private ConnectorEnvironment m_env;
    
	public TestXMLConnector() {
		super();
	}

	/**
	 * @param arg0
	 */
	public TestXMLConnector(String arg0) {
		super(arg0);
	}
    
    @Override
	public void setUp() {        
        m_env = FakeFileManagedConnectionfactory.getDefaultFileProps();                
    }
        
    public void testStart() throws Exception {
    	FileConnector connector = new FileConnector();
        connector.initialize(m_env);
    }
    
    public void testGetConnection() {
    	FileConnector connector = new FileConnector();
                        
        try {
	         connector.initialize(m_env);
	         FileConnection conn = (FileConnection) connector.getConnection();
	         assertNotNull("XMLConnectionImpl is null", conn);
        } catch (ConnectorException ex) {
            ex.printStackTrace();
            fail(ex.getMessage());
        }
    } 
    
    
    public void testUnsetState() {
    	FileConnector connector = new FileConnector();
        
        try {
            connector.getConnection();
            fail("connector created a connection with unset state");           
        } catch (ConnectorException e) {
            
        }       
    }
    
   
}
