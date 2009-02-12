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

package com.metamatrix.xa.arjuna;

import java.sql.SQLException;


import junit.framework.TestCase;



public class TestXAConnectorRecovery extends TestCase {

    private static final String CONN2 = "conn2";//$NON-NLS-1$
    private static final String CONN1 = "conn1";//$NON-NLS-1$

    boolean failToCreateXAResource = false;
    
    // make sure hasMoreResources working as expected
    public void testhasMoreResources() throws Exception {
        XAConnectorRecovery.addConnector(CONN1, new TestArjunaRecovery.FakeXAConnectionSource(new FakeXAConnection(CONN1))); 
        XAConnectorRecovery.addConnector(CONN2, new TestArjunaRecovery.FakeXAConnectionSource(new FakeXAConnection(CONN2))); 
        XAConnectorRecovery recovery = new XAConnectorRecovery();

        // start out with false
        assertFalse(recovery.hasMoreResources());
        
        // first cycle get resources and at the end of the fir
        assertTrue(recovery.hasMoreResources());
        assertNotNull(recovery.getXAResource());
        assertTrue(recovery.hasMoreResources());
        assertNotNull(recovery.getXAResource());

        // one false
        assertFalse(recovery.hasMoreResources());
        
        // and again trues.
        assertTrue(recovery.hasMoreResources());
        assertNotNull(recovery.getXAResource());
        assertTrue(recovery.hasMoreResources());
        assertNotNull(recovery.getXAResource());        
    }
    
    // make sure all the resources are available in the cycles; not just few
    public void testXARecoveryResources() throws Exception {
    	XAConnectorRecovery.addConnector(CONN1, new TestArjunaRecovery.FakeXAConnectionSource(new FakeXAConnection(CONN1))); 
        XAConnectorRecovery.addConnector(CONN2, new TestArjunaRecovery.FakeXAConnectionSource(new FakeXAConnection(CONN2))); 
        
        XAConnectorRecovery recovery = new XAConnectorRecovery();
        
        // start out with false
        assertFalse(recovery.hasMoreResources());
        
        // first cycle get resources and at the end of the fir
        assertTrue(recovery.hasMoreResources());
        FakeXAResource res1 = (FakeXAResource)recovery.getXAResource();
        assertEquals(CONN2, res1.name);
        
        assertTrue(recovery.hasMoreResources());
        FakeXAResource res2 = (FakeXAResource)recovery.getXAResource();
        assertEquals(CONN1, res2.name);
    }    
    
    // when the connection fails to create the XAResouce, that connection must be released.
    public void testConnectionReleaseOnFail() throws Exception {
    	FakeXAConnection conn1 = new FakeXAConnection(CONN1);
    	FakeXAConnection conn2 = new FakeXAConnection(CONN2);
    	XAConnectorRecovery.addConnector(CONN1, new TestArjunaRecovery.FakeXAConnectionSource(conn1)); 
        XAConnectorRecovery.addConnector(CONN2, new TestArjunaRecovery.FakeXAConnectionSource(conn2)); 
                
        XAConnectorRecovery recovery = new XAConnectorRecovery();
        
        // start out with false
        assertFalse(recovery.hasMoreResources());
        
        // first cycle get resources and at the end of the fir
        assertTrue(recovery.hasMoreResources());
        FakeXAResource res1 = (FakeXAResource)recovery.getXAResource();
        assertEquals(CONN2, res1.name);
        
        conn1.failToCreateResource(true);
        conn2.failToCreateResource(true);
        assertTrue(recovery.hasMoreResources());
        try {
            recovery.getXAResource();
            fail("should have failed to get a resource"); //$NON-NLS-1$
        }catch(SQLException e) {
            // pass
            assertTrue(conn1.released);
        }
    }     
    
    
    // make sure the same connection is used over and over again
    public void testConnectionHeld() throws Exception {
    	XAConnectorRecovery.addConnector(CONN1, new TestArjunaRecovery.FakeXAConnectionSource(new FakeXAConnection(CONN1))); 
        XAConnectorRecovery.addConnector(CONN2, new TestArjunaRecovery.FakeXAConnectionSource(new FakeXAConnection(CONN2))); 
                
        XAConnectorRecovery recovery = new XAConnectorRecovery();
        
        // start out with false
        assertFalse(recovery.hasMoreResources());
        
        // first cycle get resources and at the end of the fir
        recovery.hasMoreResources();
        recovery.getXAResource();

        recovery.hasMoreResources();
        recovery.getXAResource();

        assertFalse(recovery.hasMoreResources());

        // now lets set the connection on the conn2 to null, since the 
        // connection should be cached we should be still get a resource. 
        FakeXAResource res1 = (FakeXAResource)recovery.getXAResource();
        assertEquals(CONN2, res1.name);        
    }
}
