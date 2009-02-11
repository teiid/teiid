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
package com.metamatrix.connector.jdbc.xa;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Properties;

import javax.sql.DataSource;
import javax.sql.XAConnection;
import javax.sql.XADataSource;

import com.metamatrix.connector.api.ExecutionContext;
import com.metamatrix.connector.exception.ConnectorException;
import com.metamatrix.connector.jdbc.JDBCPlugin;
import com.metamatrix.connector.jdbc.JDBCPropertyNames;
import com.metamatrix.connector.jdbc.JDBCSingleIdentityConnectionFactory;
import com.metamatrix.connector.pool.PoolAwareConnection;

/**
 * JDBCSingleIdentityDSConnectionFactory
 *
 * <p>Creates connections from DataSources both XA and non-XA.</p>
 */
public class JDBCSingleIdentityDSConnectionFactory extends JDBCSingleIdentityConnectionFactory{
    private DataSource ds;
    private String resourceName;

    protected void verifyConnectionProperties(final Properties connectionProps) throws ConnectorException {
        // Get the JDBC properties ...
        String dataSourceClassName = connectionProps.getProperty(JDBCPropertyNames.DRIVER_CLASS);
        String username = connectionProps.getProperty(XAJDBCPropertyNames.USER);
        String password = connectionProps.getProperty(XAJDBCPropertyNames.PASSWORD);
        String serverName = connectionProps.getProperty(XAJDBCPropertyNames.SERVER_NAME);
        String serverPort = connectionProps.getProperty(XAJDBCPropertyNames.PORT_NUMBER);

        // Unique resource name for this connector
        final StringBuffer dataSourceResourceName = new StringBuffer(connectionProps.getProperty(XAJDBCPropertyNames.DATASOURCE_NAME, "XADS")); //$NON-NLS-1$
        dataSourceResourceName.append('_'); 
        dataSourceResourceName.append(serverName);
        dataSourceResourceName.append('_'); 
        dataSourceResourceName.append(connectionProps.getProperty(XAJDBCPropertyNames.CONNECTOR_ID));
        resourceName = dataSourceResourceName.toString();
        connectionProps.setProperty( XAJDBCPropertyNames.DATASOURCE_NAME, resourceName);

        // Verify required items
        if (dataSourceClassName == null || dataSourceClassName.trim().length() == 0) {
            throw new ConnectorException(JDBCPlugin.Util.getString("JDBCSourceConnectionFactory.Missing_JDBC_driver_class_name_1")); //$NON-NLS-1$
        }
        dataSourceClassName = dataSourceClassName.trim();
        if ( serverName == null || serverName.trim().length() == 0 ) {
            throw new ConnectorException(JDBCPlugin.Util.getString("JDBCSourceConnectionFactory.MissingProp",  //$NON-NLS-1$
                    XAJDBCPropertyNames.SERVER_NAME));
        }
        if ( serverPort == null || serverPort.trim().length() == 0 ) {
            throw new ConnectorException(JDBCPlugin.Util.getString("JDBCSourceConnectionFactory.MissingProp",  //$NON-NLS-1$
                    XAJDBCPropertyNames.PORT_NUMBER));
        }
        if ( username == null || username.trim().length() == 0 ) {
            throw new ConnectorException(JDBCPlugin.Util.getString("JDBCSourceConnectionFactory.MissingProp",  //$NON-NLS-1$
                    XAJDBCPropertyNames.USER));
        }
        if ( password == null || password.trim().length() == 0 ) {
            throw new ConnectorException(JDBCPlugin.Util.getString("JDBCSourceConnectionFactory.MissingProp",  //$NON-NLS-1$
                    XAJDBCPropertyNames.PASSWORD));
        }

        // create data source
        final DataSource baseDs;
        try {
        	Class clazz = Thread.currentThread().getContextClassLoader().loadClass(dataSourceClassName);
            baseDs = (DataSource) clazz.newInstance();
        } catch(Exception e) {
            throw new ConnectorException(e,JDBCPlugin.Util.getString("JDBCSourceConnectionFactory.Unable_to_load_the_JDBC_driver_class_6", dataSourceClassName)); //$NON-NLS-1$
        }

        setDSProperties(connectionProps, baseDs);

        ds = baseDs;
    }

    @Override
    public PoolAwareConnection getConnection(ExecutionContext context) throws ConnectorException {
        try{
            XAConnection conn = ((XADataSource)ds).getXAConnection();
            Connection sqlConn = conn.getConnection();
            if(getTransactionIsolation() != NO_ISOLATION_LEVEL_SET && getTransactionIsolation() != Connection.TRANSACTION_NONE){
                sqlConn.setTransactionIsolation(getTransactionIsolation());
            }
            return new JDBCSourceXAConnection(sqlConn, conn, getConnectorEnvironment(), createConnectionStrategy(), getConnectionListener());
        }catch(SQLException se){
            throw new ConnectorException(se);
        }
    }

    /**
     * @param props
     * @param dataSource
     */
    protected void setDSProperties(final Properties props, final DataSource dataSource) throws ConnectorException {
        // Move all prop names to lower case so we can use reflection to get
        // method names and look them up in the connection props.
        final Properties connProps = lowerCaseAllPropNames(props);
        final Method[] methods = dataSource.getClass().getMethods();
        for (int i = 0; i < methods.length; i++) {
            final Method method = methods[i];
            final String methodName = method.getName();
            // If setter ...
            if ( methodName.startsWith("set") && method.getParameterTypes().length == 1 ) { //$NON-NLS-1$
                // Get the property name
                final String propertyName = methodName.substring(3);    // remove the "set"
                final String propertyValue = (String) connProps.get(propertyName.toLowerCase());
                if ( propertyValue != null ) {  
                    final Class argType = method.getParameterTypes()[0];
                    final Object[] params = new Object[1];
                    if ( argType == Integer.TYPE ) {
                        params[0] = Integer.decode(propertyValue);
                    } else if ( argType == Boolean.TYPE ) {
                        params[0] = Boolean.valueOf(propertyValue);
                    } else if ( argType == String.class ) {
                        params[0] = propertyValue;
                    }

                    // Actually set the property ...
                    //getConnectorEnvironment().getLogger().logTrace("setDSProperties - Setting property \"" + propertyName +  //$NON-NLS-1$
                    //                                               "\" = \"" + params[0] + "\" on \"" + props.getProperty(JDBCPropertyNames.DRIVER_CLASS) + "\"");                     //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                    try {
                        method.invoke(dataSource, params);
                    } catch (Throwable e) {
                        final Object[] msgParams = new Object[]{propertyName, propertyValue};
                        final String msg = JDBCPlugin.Util.getString("JDBCSingleIdentityDSConnectionFactory.Unable_to_set_DataSource_property", msgParams); //$NON-NLS-1$
                        throw new ConnectorException(msg); 
                    }
                }
            }
        }
    }

    private Properties lowerCaseAllPropNames(final Properties connectionProps) {
        final Properties lcProps = new Properties();
        final Iterator itr = connectionProps.keySet().iterator();
        while ( itr.hasNext() ) {
            final String name = (String) itr.next();
            Object propValue = connectionProps.get(name);
            if (propValue instanceof String) {
                // we're only interested in prop values of type String
                // here since we'll be looking for params to reflected methods
                lcProps.setProperty(name.toLowerCase(), (String)propValue);
            } // if
        }
        return lcProps;
    }
    
}
