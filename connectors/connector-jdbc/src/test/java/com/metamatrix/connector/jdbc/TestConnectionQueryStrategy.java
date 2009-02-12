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

package com.metamatrix.connector.jdbc;

import java.sql.Connection;

import com.metamatrix.core.util.SimpleMock;

import junit.framework.TestCase;


/** 
 * Test case for ConnectionQueryStrategy
 * @since 4.3
 */
public class TestConnectionQueryStrategy extends TestCase {
    
    private final static String TEST_QUERY = "select 'x' from dual"; //$NON-NLS-1$
    
    private ConnectionQueryStrategy strategy;
    private FakeConnection fakeConnection;
    private Connection connection;
    
    public TestConnectionQueryStrategy(String name) {
        super(name);
    }
    
    public void setUp() throws Exception {
        strategy = new ConnectionQueryStrategy(TEST_QUERY);
        
        fakeConnection = new FakeConnection();
        connection = SimpleMock.createSimpleMock(fakeConnection, Connection.class);
    }
    

    /**
     * Tests ConnectionQueryStrategy.isConnectionAlive()
     * @since 4.3
     */
    public void testIsConnectionAlive() {
        //closed connections should not be 'alive'
    	fakeConnection.closed = true;      
        assertFalse(strategy.isConnectionAlive(connection));
        
        
        //open connections should be 'alive'
        fakeConnection.closed = false;      
        assertTrue(strategy.isConnectionAlive(connection));
        
        //failed connections should not be 'alive'
        fakeConnection.fail = true;     
        assertFalse(strategy.isConnectionAlive(connection));
        
    }
    
}
