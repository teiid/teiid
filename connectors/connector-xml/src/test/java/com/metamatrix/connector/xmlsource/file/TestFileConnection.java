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

package com.metamatrix.connector.xmlsource.file;

import java.util.Properties;

import junit.framework.TestCase;

import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;

import com.metamatrix.cdk.api.EnvironmentUtility;
import com.metamatrix.core.util.UnitTestUtil;


/** 
 */
public class TestFileConnection extends TestCase {

    
    public void testBadDirectory() {
        Properties props = new Properties();
        props.setProperty("DirectoryLocation", "BadDirectory"); //$NON-NLS-1$  //$NON-NLS-2$
        ConnectorEnvironment env = EnvironmentUtility.createEnvironment(props, false);
        
        try {
            new FileConnection(env);
            fail("Must have failed because of bad directory location"); //$NON-NLS-1$
        } catch (ConnectorException e) {
        }            
    }
    
    
    public void testGoodDirectory() {
        String file = UnitTestUtil.getTestDataPath(); 
        Properties props = new Properties();
        props.setProperty("DirectoryLocation", file); //$NON-NLS-1$  
        ConnectorEnvironment env = EnvironmentUtility.createEnvironment(props, false);
        
        try {
            FileConnection conn = new FileConnection(env);
            assertTrue(conn.isConnected());
        } catch (ConnectorException e) {
            fail("mast have passed connection"); //$NON-NLS-1$
        }            
    }    
    
    public void testNoDirectory() {
        Properties props = new Properties();
        ConnectorEnvironment env = EnvironmentUtility.createEnvironment(props, false);
        
        try {
            new FileConnection(env);
            fail("Must have failed because of bad directory location"); //$NON-NLS-1$
        } catch (ConnectorException e) {
        }            
    }    
    
}
