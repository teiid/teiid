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

/*
 */
package com.metamatrix.connector.jdbc;

import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;

import com.metamatrix.connector.api.Connection;
import com.metamatrix.connector.api.ConnectorEnvironment;
import com.metamatrix.connector.api.ConnectorLogger;
import com.metamatrix.connector.api.ExecutionContext;
import com.metamatrix.connector.exception.ConnectorException;
import com.metamatrix.connector.pool.SingleIdentityFactory;

/**
 * Factory to create JDBCSourceConnection for SingleIdentity.
 */
public class JDBCSingleIdentityConnectionFactory extends JDBCSourceConnectionFactory {
    public static final String INVALID_AUTHORIZATION_SPECIFICATION_NO_SUBCLASS = "28000"; //$NON-NLS-1$
    private Driver driver;
    private String url;
    private int transIsoLevel;
    private Properties userProps;
    protected ConnectionListener connectionListener = new DefaultConnectionListener();
    private ConnectorLogger logger;

    public JDBCSingleIdentityConnectionFactory() {
		super(new SingleIdentityFactory());
	}
    
    public void initialize(ConnectorEnvironment env) throws ConnectorException {
        super.initialize(env);
        verifyConnectionProperties(env.getProperties());
        logger = env.getLogger();
        // attempt to get a connection to verify that properties are valid
        testConnection();
    }
    
    protected void verifyConnectionProperties(Properties connectionProps) throws ConnectorException{
        // Find driver 
        String driverClassName = connectionProps.getProperty(JDBCPropertyNames.DRIVER_CLASS);
        driver = createDriver(driverClassName);

        // check URL
        url = connectionProps.getProperty(JDBCPropertyNames.URL);
        validateURL(driver, url);

        // Build connection properties from user name and password
        String username = connectionProps.getProperty(JDBCPropertyNames.USERNAME);
        String password = connectionProps.getProperty(JDBCPropertyNames.PASSWORD);
        userProps = new Properties();
        if (username != null && username.trim().length() > 0) {
            userProps.setProperty("user", username.trim()); //$NON-NLS-1$
        }

        if ( password != null && password.trim().length() > 0 ) {
            userProps.setProperty("password", password.trim()); //$NON-NLS-1$
        }
        
        transIsoLevel = interpretTransactionIsolationLevel( connectionProps.getProperty(JDBCPropertyNames.TRANSACTION_ISOLATION_LEVEL));        
    }
    
    private void testConnection() throws ConnectorException {
        try {
            Connection connection = getConnection(null);
            connection.close();
        } catch (ConnectorException e) {
            SQLException ex = (SQLException)e.getCause();
            String sqlState = ex.getSQLState();
            if (sqlState != null && INVALID_AUTHORIZATION_SPECIFICATION_NO_SUBCLASS.equals(sqlState)) {
                throw e;
            }
            this.logger.logError(e.getMessage(), e);
        }           
    }
            
    public Connection getConnection(ExecutionContext ctx) throws ConnectorException {
        return createJDBCConnection(driver, url, transIsoLevel, userProps);
    }

    protected int getTransactionIsolation(){
        return this.transIsoLevel;
    }
    
    /**
     * Connection Listener only used in the SingleIdentityConnections for now.
     * @see com.metamatrix.connector.jdbc.JDBCSourceConnectionFactory#getConnectionListener()
     */
    protected ConnectionListener getConnectionListener() {
        return connectionListener;
    }
    
}
