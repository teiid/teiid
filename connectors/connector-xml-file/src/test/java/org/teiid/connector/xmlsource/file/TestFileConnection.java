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

package org.teiid.connector.xmlsource.file;

import junit.framework.TestCase;

import org.mockito.Mockito;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ConnectorLogger;

import com.metamatrix.core.util.UnitTestUtil;


/** 
 */
public class TestFileConnection extends TestCase {

    
    public void testBadDirectory() {
        FileManagedConnectionFactory config = Mockito.mock(FileManagedConnectionFactory.class);
        Mockito.stub(config.getLogger()).toReturn(Mockito.mock(ConnectorLogger.class));
        Mockito.stub(config.getDirectoryLocation()).toReturn("BadDirectory");        
        
        try {
            new FileConnection(config);
            fail("Must have failed because of bad directory location"); //$NON-NLS-1$
        } catch (ConnectorException e) {
        }            
    }
    
    
    public void testGoodDirectory() {
        String file = UnitTestUtil.getTestDataPath(); 

        FileManagedConnectionFactory config = Mockito.mock(FileManagedConnectionFactory.class);
        Mockito.stub(config.getLogger()).toReturn(Mockito.mock(ConnectorLogger.class));
        Mockito.stub(config.getDirectoryLocation()).toReturn(file);        
        
        try {
            FileConnection conn = new FileConnection(config);
            assertTrue(conn.isConnected());
        } catch (ConnectorException e) {
            fail("mast have passed connection"); //$NON-NLS-1$
        }            
    }    
    
    public void testNoDirectory() {
        FileManagedConnectionFactory config = Mockito.mock(FileManagedConnectionFactory.class);
        Mockito.stub(config.getLogger()).toReturn(Mockito.mock(ConnectorLogger.class));
        
        try {
            new FileConnection(config);
            fail("Must have failed because of bad directory location"); //$NON-NLS-1$
        } catch (ConnectorException e) {
        }            
    }    
    
}
