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
 * com.metamatrix.connector.jdbc.ssl.JDBCSequeLinkSingleIdentityConnectionFactory
 * Created by JChoate on Jan 28, 2005
 * (c) 2005 MetaMatrix, Inc.
 */
package com.metamatrix.connector.jdbc.ssl;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;

import com.metamatrix.connector.jdbc.ConnectionQueryStrategy;
import com.metamatrix.connector.jdbc.ConnectionStrategy;
import com.metamatrix.connector.jdbc.JDBCPropertyNames;
import com.metamatrix.connector.jdbc.JDBCSingleIdentityConnectionFactory;
import com.metamatrix.connector.jdbc.JDBCSourceConnection;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.pool.SourceConnection;

public class JDBCSequeLinkSingleIdentityConnectionFactory extends
		JDBCSingleIdentityConnectionFactory {
	
    private static final String JDBC_CAPABILITIES   = "com.metamatrix.connector.jdbc.JDBCCapabilities"; //$NON-NLS-1$
    private static final String ORACLE_CAPABILITIES = "com.metamatrix.connector.jdbc.oracle.OracleCapabilities"; //$NON-NLS-1$
    private static final String DB2_CAPABILITIES    = "com.metamatrix.connector.jdbc.db2.DB2Capabilities"; //$NON-NLS-1$
    private static final String SYBASE_CAPABILITIES = "com.metamatrix.connector.jdbc.sybase.SybaseCapabilities"; //$NON-NLS-1$
    private static final String SQLSERVER_CAPABILITIES = "com.metamatrix.connector.jdbc.sqlserver.SqlServerCapabilities"; //$NON-NLS-1$
    
    // Fix for Case 4049 - USTranscom
    // By default we were not creating a connectionStrategy and therefore not testing connections
    // when pulling them from the pool, this caused problems when the SequeLink proxy would go down
    // and come back up since we never removed the bad connection. 
    // Adding this logic lets us run the test query when getting connections from the pool. 
    // This is a slight hack, ultimately we want to add an option to Oracle, DB2, etc.. connectorTypes
    // to select ssl mode and remove this Connector type. 
    protected ConnectionStrategy createConnectionStrategy() {

        String capabilityClass = this.getConnectorEnvironment().getProperties().getProperty(JDBCPropertyNames.EXT_CAPABILITY_CLASS, JDBC_CAPABILITIES); 
        String queryTest = null;
        
        if (capabilityClass.equals(ORACLE_CAPABILITIES)) {
            queryTest = "Select 'x' from DUAL"; //$NON-NLS-1$
        } else if (capabilityClass.equals(DB2_CAPABILITIES)) {
            queryTest = "Select 'x' from sysibm.systables where 1 = 2"; //$NON-NLS-1$
        } else if (capabilityClass.equals(SYBASE_CAPABILITIES)) {
            queryTest = "Select 'x'"; //$NON-NLS-1$
        } else if (capabilityClass.equals(SQLSERVER_CAPABILITIES)) {
            queryTest = "Select 'x'"; //$NON-NLS-1$
        } else {
            return null;
        }
        return new ConnectionQueryStrategy(queryTest, this.sourceConnectionTestInterval);        
    }

    /**
     * This creates a JDBC connection.
     * It overrides the functionality in abstract class com.metamatrix.connector.jdbc.JDBCSourceConnectionFactory
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

        return new JDBCSourceConnection(connection, getConnectorEnvironment(), createConnectionStrategy(), getConnectionListener());
    }
}
