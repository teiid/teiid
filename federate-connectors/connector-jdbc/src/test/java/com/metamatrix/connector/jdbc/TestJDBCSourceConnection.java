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

package com.metamatrix.connector.jdbc;

import java.sql.Connection;
import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.metamatrix.cdk.api.EnvironmentUtility;
import com.metamatrix.core.util.SimpleMock;
import com.metamatrix.data.api.ConnectorEnvironment;


/** 
 * Test case for JDBCSourceConnection
 * @since 4.3
 */
public class TestJDBCSourceConnection extends TestCase {
    
    private final static String TEST_QUERY = "select 'x' from dual"; //$NON-NLS-1$
    
    
    private FakeConnection fakeConnection;
    private Connection connection;
    private ConnectorEnvironment environment;
    private ConnectionQueryStrategy strategy;
    
    
    public TestJDBCSourceConnection(String name) {
        super(name);
        
    }
    
    public static Test suite() {
        TestSuite suite = new TestSuite("TestJDBCSourceConnection"); //$NON-NLS-1$
        suite.addTestSuite(TestJDBCSourceConnection.class);
        return suite;
    }
    
    public void setUp() throws Exception {
        fakeConnection = new FakeConnection();
        connection = SimpleMock.createSimpleMock(fakeConnection, Connection.class);

        final Properties properties = new Properties();
        properties.setProperty(JDBCPropertyNames.EXT_SQL_TRANSLATOR_CLASS, 
            "com.metamatrix.connector.jdbc.extension.impl.BasicSQLTranslator");  //$NON-NLS-1$
        properties.setProperty(JDBCPropertyNames.EXT_RESULTS_TRANSLATOR_CLASS, 
            "com.metamatrix.connector.jdbc.extension.impl.BasicResultsTranslator");  //$NON-NLS-1$
        properties.setProperty(JDBCPropertyNames.EXT_CAPABILITY_CLASS, 
            "com.metamatrix.data.basic.BasicConnectorCapabilities");  //$NON-NLS-1$
        
        environment = EnvironmentUtility.createEnvironment(properties, false); 
        
        strategy = new ConnectionQueryStrategy(TEST_QUERY, 300000);
        strategy.setQueryInterval(-1);
    }
    

    /**
     * Tests JDBCSourceConnection.isConnectionAlive() with a ConnectionQueryStrategy
     * @since 4.3
     */
    public void testIsAlive() throws Exception {
        JDBCSourceConnection sourceConnection = new JDBCSourceConnection(connection, environment, strategy); 
        
        //closed connections should not be 'alive'        
        fakeConnection.closed = true;
        assertFalse(sourceConnection.isAlive());        
        
        //open connections should be 'alive'
        fakeConnection.closed = false;
        assertTrue(sourceConnection.isAlive());
        
        //failed connections should not be 'alive'
        fakeConnection.fail = true;
        assertFalse(sourceConnection.isAlive());
        
    }
    
    /**
     * Tests JDBCSourceConnection.isConnectionAlive() with a null ConnectionStrategy
     * @since 4.3
     */
    public void testIsAliveNullStrategy() throws Exception {
        JDBCSourceConnection sourceConnection = new JDBCSourceConnection(connection, environment, null); 
        
        //closed connections should not be 'alive'        
        fakeConnection.closed = true;
        assertFalse(sourceConnection.isAlive());        
        
        //open connections should be 'alive'
        fakeConnection.closed = false;
        assertTrue(sourceConnection.isAlive());
        
        //without a strategy, failed connections are detected as 'alive'
        fakeConnection.fail = true;
        assertTrue(sourceConnection.isAlive());
        
    }
    
}
