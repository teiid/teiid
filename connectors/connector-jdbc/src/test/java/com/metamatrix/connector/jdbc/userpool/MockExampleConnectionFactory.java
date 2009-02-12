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

package com.metamatrix.connector.jdbc.userpool;

import java.sql.Driver;
import java.util.Properties;

import com.metamatrix.connector.api.Connection;
import com.metamatrix.connector.exception.ConnectorException;
import com.metamatrix.connector.jdbc.JDBCUserIdentityConnectionFactory;

/**
 * Overrides createConnection stuff so we can fake obtaining a connection.
 */
public class MockExampleConnectionFactory extends JDBCUserIdentityConnectionFactory {

    public void shutdown() {}
	
    @Override
    protected Connection createJDBCConnection(
        Driver driver,
        String url,
        int transactionIsolationLevel,
        Properties userProps)
        throws ConnectorException {
            
        String user = userProps.getProperty("user"); //$NON-NLS-1$
        String password = userProps.getProperty("password"); //$NON-NLS-1$
                    
        return new MockSourceConnection(url, transactionIsolationLevel, user, password);            
    }

    @Override
    protected Driver createDriver(String driverClassName) throws ConnectorException {
    	return null;
    }
    
    @Override
    protected void validateURL(Driver driver, String url)
    		throws ConnectorException {
    }
}
