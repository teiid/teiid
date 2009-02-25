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

package org.teiid.connector.jdbc;

import java.sql.Connection;
import java.util.Properties;

import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.basic.BasicConnectorCapabilities;
import org.teiid.connector.jdbc.JDBCPropertyNames;
import org.teiid.connector.jdbc.JDBCSourceConnection;
import org.teiid.connector.jdbc.translator.Translator;

import junit.framework.TestCase;

import com.metamatrix.cdk.api.EnvironmentUtility;
import com.metamatrix.core.util.SimpleMock;


/** 
 * Test case for JDBCSourceConnection
 * @since 4.3
 */
public class TestJDBCSourceConnection extends TestCase {
    
    private FakeConnection fakeConnection;
    private Connection connection;
    private ConnectorEnvironment environment;
    
    
    public TestJDBCSourceConnection(String name) {
        super(name);
    }
    
    public void setUp() throws Exception {
        fakeConnection = new FakeConnection();
        connection = SimpleMock.createSimpleMock(fakeConnection, Connection.class);

        final Properties properties = new Properties();
        properties.setProperty(JDBCPropertyNames.EXT_CAPABILITY_CLASS, BasicConnectorCapabilities.class.getName());  
        
        environment = EnvironmentUtility.createEnvironment(properties, false); 
    }
    

    /**
     * Tests JDBCSourceConnection.isConnectionAlive() with a ConnectionQueryStrategy
     * @since 4.3
     */
    public void testIsAlive() throws Exception {
        JDBCSourceConnection sourceConnection = new JDBCSourceConnection(connection, environment, new Translator() {
        	@Override
        	public String getConnectionTestQuery() {
        		return "select 1";
        	}
        }); 
        
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
        JDBCSourceConnection sourceConnection = new JDBCSourceConnection(connection, environment, new Translator() {
        	@Override
        	public String getConnectionTestQuery() {
        		return null;
        	}
        }); 
        
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
