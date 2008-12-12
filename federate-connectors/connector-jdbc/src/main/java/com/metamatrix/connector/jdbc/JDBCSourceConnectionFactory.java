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

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;

import com.metamatrix.common.util.exception.SQLExceptionUnroller;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.pool.SourceConnection;
import com.metamatrix.data.pool.SourceConnectionFactory;

/**
 * Represents a base class for a JDBC source connection factory.  Subclasses
 * are expected to obtain the properties for their connection in different
 * ways (either from connector properties, from security context, or from
 * some lookup to an external source).  
 */
public abstract class JDBCSourceConnectionFactory implements SourceConnectionFactory {
    
    protected static final int NO_ISOLATION_LEVEL_SET = Integer.MIN_VALUE;
    
    private ConnectorEnvironment environment;
    
    /**How often (in ms) to test that the data source is available by establishing a new connection.*/
    protected int sourceConnectionTestInterval;

    /**
     *
     */
    public JDBCSourceConnectionFactory() {        
    }

    public void initialize(ConnectorEnvironment env) throws ConnectorException {
        this.environment = env;
        Properties props = env.getProperties();
        String value = props.getProperty(SourceConnection.SOURCE_CONNECTION_TEST_INTERVAL, SourceConnection.DEFAULT_SOURCE_CONNECTION_TEST_INTERVAL);
        this.sourceConnectionTestInterval = (Integer.parseInt(value) * 1000);
    }
    
    protected ConnectorEnvironment getConnectorEnvironment() {
        return this.environment;
    }
    
    protected Driver createDriver(String driverClassName) throws ConnectorException {
        // Verify required items
        if (driverClassName == null || driverClassName.trim().length() == 0) {
            throw new ConnectorException(JDBCPlugin.Util.getString("JDBCSourceConnectionFactory.Missing_JDBC_driver_class_name_1")); //$NON-NLS-1$
        }
        try {
        	Class clazz = Thread.currentThread().getContextClassLoader().loadClass(driverClassName);
            return (Driver) clazz.newInstance();
        } catch(Exception e) {
            throw new ConnectorException(e, JDBCPlugin.Util.getString("JDBCSourceConnectionFactory.Unable_to_load_the_JDBC_driver_class_6", driverClassName)); //$NON-NLS-1$
        }
    }
        
    protected void validateURL(Driver driver, String url) throws ConnectorException {
        if (url == null || url.trim().length() == 0) {
            throw new ConnectorException(JDBCPlugin.Util.getString("JDBCSourceConnectionFactory.Missing_JDBC_database_name_3")); //$NON-NLS-1$
        }

        boolean acceptsURL = false;
        try {
            acceptsURL = driver.acceptsURL(url);
        } catch ( SQLException e ) {
            throw new ConnectorException(e);
        }
        if(!acceptsURL ){
            throw new ConnectorException(JDBCPlugin.Util.getString("JDBCSourceConnectionFactory.Driver__7", driver.getClass().getName(), url)); //$NON-NLS-1$
        }
    }
  
    /**
     * This creates a JDBC connection.
     * @throws ConnectorException  if there is an error establishing the connection.
     */
    protected SourceConnection createJDBCConnection(Driver driver, String url, int transactionIsolationLevel, Properties userProps) throws ConnectorException {
        Connection connection = null;

        // Connect
        try {
            connection = driver.connect(url, userProps);
            if(transactionIsolationLevel != NO_ISOLATION_LEVEL_SET){
                connection.setTransactionIsolation(transactionIsolationLevel);
            }
        } catch ( SQLException e ) {
            throw new ConnectorException(e);
        }

        return new JDBCSourceConnection(connection, this.environment, createConnectionStrategy(), getConnectionListener());
    }

    /**
     * Subclass can override this method to provide an actual ConnectionListener
     */
    protected ConnectionListener getConnectionListener() {
        return null;
    }
    
    /**
     * Subclass can override this method to provide an actual ConnectionStrategy
     */
    protected ConnectionStrategy createConnectionStrategy() {
        return null;        
    }

    protected int interpretTransactionIsolationLevel( String levelStr ) throws ConnectorException {
        int isoLevel = NO_ISOLATION_LEVEL_SET;
        if(levelStr == null || levelStr.trim().length() == 0){
            return isoLevel;
        }
        
        levelStr = levelStr.toUpperCase();

        if (levelStr.equals("TRANSACTION_READ_UNCOMMITTED")) { //$NON-NLS-1$
            isoLevel = Connection.TRANSACTION_READ_UNCOMMITTED;
        } else if (levelStr.equals("TRANSACTION_READ_COMMITTED")) {//$NON-NLS-1$
            isoLevel = Connection.TRANSACTION_READ_COMMITTED;
        } else if (levelStr.equals("TRANSACTION_REPEATABLE_READ")) {//$NON-NLS-1$
            isoLevel = Connection.TRANSACTION_REPEATABLE_READ;
        } else if (levelStr.equals("TRANSACTION_SERIALIZABLE")) {//$NON-NLS-1$
            isoLevel = Connection.TRANSACTION_SERIALIZABLE;
        } else if (levelStr.equals("TRANSACTION_NONE")) {//$NON-NLS-1$
            isoLevel = Connection.TRANSACTION_NONE;
        } else {
            throw new ConnectorException(JDBCPlugin.Util.getString("JDBCSourceConnectionFactory.til", levelStr)); //$NON-NLS-1$
        }
        return isoLevel;
    }
    
    protected void shutdown() {
        
    }

}
