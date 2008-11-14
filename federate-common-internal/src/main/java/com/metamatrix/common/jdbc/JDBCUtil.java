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

package com.metamatrix.common.jdbc;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import javax.sql.DataSource;

import com.metamatrix.api.exception.MetaMatrixException;
import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.connection.ManagedConnectionException;
import com.metamatrix.common.util.ErrorMessageKeys;
import com.metamatrix.common.util.crypto.CryptoException;
import com.metamatrix.common.util.crypto.CryptoUtil;
import com.metamatrix.core.util.StringUtil;

public class JDBCUtil {
    /**
     * The environment property name for the class of the driver.
     * This property is optional.
     */
    public static final String DRIVER = "com.metamatrix.common.util.JDBCUtil.Driver"; //$NON-NLS-1$

    /**
     * The environment property name for the protocol that is to be used.  For JDBC, the connection
     * URL information is created of the form "jdbc:subprotocol:subname", where the value
     * of the PROTOCOL property is used for the "subprotocol:subname" portion.
     * This property is required.
     */
    public static final String PROTOCOL = "com.metamatrix.common.util.JDBCUtil.Protocol"; //$NON-NLS-1$

    /**
     * The environment property name for the database name.  This may include the server name and port number,
     * per the driver's requirements.
     * This property is required.
     */
    public static final String DATABASE = "com.metamatrix.common.util.JDBCUtil.Database"; //$NON-NLS-1$

    /**
     * The environment property name for the username that is to be used for connecting to the metadata store.
     * This property is required.
     */
    public static final String USERNAME = "com.metamatrix.common.util.JDBCUtil.User"; //$NON-NLS-1$

    /**
     * The environment property name for the password that is to be used for connecting to the metadata store.
     * This property is required.
     */
    public static final String PASSWORD = "com.metamatrix.common.util.JDBCUtil.Password"; //$NON-NLS-1$

    private static final String JDBC_PREFIX = "jdbc:";//$NON-NLS-1$
    // This is the prop name the JDBC driver expects for the password property
    private static final String DRIVER_PWD_PROP_NAME = "password"; //$NON-NLS-1$
    // This is the prop name the JDBC driver expects for the password property
    private static final String DRIVER_USER_PROP_NAME = "user"; //$NON-NLS-1$
    
    // This is the prop name the JDBCXA driver expects for the port number
    private static final String XADRIVER_PORT_NUMBER = "portNumber"; //$NON-NLS-1$
    // This is the prop name the JDBCXA driver expects for the server name
    private static final String XADRIVER_SERVER_NAME = "serverName"; //$NON-NLS-1$
    // This is the prop name the JDBCXA driver expects for the database name
    private static final String XADRIVER_DATABASE_NAME = "databaseName"; //$NON-NLS-1$

    public static Connection decryptAndCreateJDBCConnection(Properties env) throws MetaMatrixException {

        Properties jdbcEnv;
        // Decrypt connection password
        try {
            jdbcEnv = CryptoUtil.propertyDecrypt(JDBCUtil.PASSWORD, env);
        } catch ( CryptoException e ) {
            throw new MetaMatrixException(e, ErrorMessageKeys.CM_UTIL_ERR_0175, CommonPlugin.Util.getString(ErrorMessageKeys.CM_UTIL_ERR_0175));
        }

        return JDBCUtil.createJDBCConnection(jdbcEnv);

    }

    /**
     * This creates a JDBC connection.
     * @throws ManagedConnectionException if there is an error establishing the connection.
     */
    public static Connection createJDBCConnection( Properties env ) throws MetaMatrixException {
        Connection connection = null;

        // Get the JDBC properties ...
        String driverClassName = env.getProperty(DRIVER);
	    String username        = env.getProperty(USERNAME);
// default the password to blank so that the connection can still
// be tried and the driver will throw an appropriate password
// exception.
	    String password        = env.getProperty(PASSWORD, ""); //$NON-NLS-1$

	    // Verify required items
	    if (driverClassName == null || driverClassName.trim().length() == 0) {
	        throw new MetaMatrixException(ErrorMessageKeys.CM_UTIL_ERR_0176, CommonPlugin.Util.getString(ErrorMessageKeys.CM_UTIL_ERR_0176));
	    }
        
        String url = getDatabaseURL(env);

        Properties props = new Properties();

        if (username != null && username.trim().length() > 0) {
            props.setProperty(DRIVER_USER_PROP_NAME,username.trim());
            props.setProperty(DRIVER_PWD_PROP_NAME,password.trim());
        }

        try {
            Driver driver = null;
            try {
                //create a new driver instance each time this method (openConnection) is called.
                //could use a "manager" class to keep a map of the drivers loaded if
                //this method is called frequently.
                driver = (Driver)Class.forName(driverClassName).newInstance();
            } catch(Exception e) {
                throw new MetaMatrixException(e,ErrorMessageKeys.CM_UTIL_ERR_0179, CommonPlugin.Util.getString(ErrorMessageKeys.CM_UTIL_ERR_0179, driverClassName));
            }
            if(!driver.acceptsURL(url)){
                throw new MetaMatrixException(ErrorMessageKeys.CM_UTIL_ERR_0180, CommonPlugin.Util.getString(ErrorMessageKeys.CM_UTIL_ERR_0180, driverClassName, url));
            }

            connection = driver.connect(url, props);
        } catch ( Exception e ) {
            throw new MetaMatrixException(e, ErrorMessageKeys.CM_UTIL_ERR_0181, CommonPlugin.Util.getString(ErrorMessageKeys.CM_UTIL_ERR_0181, url));
        }

        return connection;
    }

    public static String getDatabaseURL( Properties env ) throws MetaMatrixException {
	    String protocol        = env.getProperty(PROTOCOL);
	    String database        = env.getProperty(DATABASE);

	    if (database == null || database.trim().length() == 0) {
            throw new MetaMatrixException(ErrorMessageKeys.CM_UTIL_ERR_0178, CommonPlugin.Util.getString(ErrorMessageKeys.CM_UTIL_ERR_0178));
        }
        // if the database property contains the whole url and the protocol is empty then 
        // use the database as the url
        if (database.startsWith(JDBC_PREFIX) && (protocol == null || protocol.trim().length() == 0)) {
             return database;    
        } else if (protocol == null || protocol.trim().length() == 0) {
	        throw new MetaMatrixException(ErrorMessageKeys.CM_UTIL_ERR_0177, CommonPlugin.Util.getString(ErrorMessageKeys.CM_UTIL_ERR_0177));
	    } 
	    return JDBC_PREFIX + env.getProperty(PROTOCOL) + ":" + env.getProperty(DATABASE); //$NON-NLS-1$ 
    }
    
        /**
     * This creates a DataSource connection.
     */
    public static Connection createJDBCXAConnection( Properties env ) throws MetaMatrixException, SQLException {
        Connection connection = null;

        // Get the JDBC properties ...
        String driverClassName = env.getProperty(DRIVER);
	    String protocol        = env.getProperty(PROTOCOL);
	    String database        = env.getProperty(DATABASE);
	    String username        = env.getProperty(USERNAME);
// default the password to blank so that the connection can still
// be tried and the driver will throw an appropriate password
// exception.
	    String password        = env.getProperty(PASSWORD, ""); //$NON-NLS-1$

	    // Verify required items
	    if (driverClassName == null || driverClassName.trim().length() == 0) {
	        throw new MetaMatrixException(ErrorMessageKeys.CM_UTIL_ERR_0176, CommonPlugin.Util.getString(ErrorMessageKeys.CM_UTIL_ERR_0176));
	    }
        
        if (database == null || database.trim().length() == 0) {
            throw new MetaMatrixException(ErrorMessageKeys.CM_UTIL_ERR_0178, CommonPlugin.Util.getString(ErrorMessageKeys.CM_UTIL_ERR_0178));
        }

        String dbUrl = null;
        // if the database property contains the whole url and the protocol is empty then 
        // use the database as the url
        if (database.startsWith(JDBC_PREFIX) && (protocol == null || protocol.trim().length() == 0)) {
        	dbUrl = database;    
        } else if (protocol == null || protocol.trim().length() == 0) {
	        throw new MetaMatrixException(ErrorMessageKeys.CM_UTIL_ERR_0177, CommonPlugin.Util.getString(ErrorMessageKeys.CM_UTIL_ERR_0177));
	    } else {
	    	dbUrl = JDBCUtil.getDatabaseURL(env); 
	    }

        Properties props = new Properties();

        if (username != null && username.trim().length() > 0) {
            props.setProperty(DRIVER_USER_PROP_NAME,username.trim());
            props.setProperty(DRIVER_PWD_PROP_NAME,password.trim());
        }

        // create data source
        Object dsClass = null;
        try {
        	dsClass = Class.forName(driverClassName).newInstance();
        } catch(Exception e) {
            throw new MetaMatrixException(e, "Unable to load the JDBC driver class " + driverClassName); //$NON-NLS-1$
        }
        // Make sure its and instanceof BaseDataSource
        if(dsClass instanceof DataSource) {
        	DataSource baseDS = (DataSource)dsClass;
        	// Get database url, parse and set datasource connection properties
        	parseURL(dbUrl,props);
        	// Set properties on DataSource
        	setDSProperties(props,baseDS);
        	// Get Connection
        	try {
				connection = baseDS.getConnection();
			} catch (Exception e) {
	            throw new MetaMatrixException(e, "Error getting connection"); //$NON-NLS-1$
			}
        } else {
	        throw new MetaMatrixException("Driver class is not of type DataSource"); //$NON-NLS-1$
        }

        return connection;
    }
    
    /**
     * Parse URL for DataSource connection properties and add to connectionProps.
     * @param url
     * @param connectionProps
     */
    private static void parseURL(final String url, final Properties connectionProps) {
        // Will be: [jdbc:mmx:dbType://aHost:aPort], [DatabaseName=aDataBase], [CollectionID=aCollectionID], ...
        final List urlParts = StringUtil.split(url, ";"); //$NON-NLS-1$

        // Will be: [jdbc:mmx:dbType:], [aHost:aPort]
        final List protoHost = StringUtil.split((String)urlParts.get(0), "//"); //$NON-NLS-1$

        // Will be: [aHost], [aPort]
        final List hostPort = StringUtil.split((String) protoHost.get(1), ":"); //$NON-NLS-1$
        connectionProps.setProperty(XADRIVER_SERVER_NAME, (String)hostPort.get(0));
        connectionProps.setProperty(XADRIVER_PORT_NUMBER, (String)hostPort.get(1));

        // For "databaseName", "SID", and all optional props
        // (<propName1>=<propValue1>;<propName2>=<propValue2>;...)
        for ( int i = 1; i < urlParts.size(); i++ ) {
            final String nameVal = (String) urlParts.get( i );
            // Will be: [propName], [propVal]
            final List aProp = StringUtil.split(nameVal, "="); //$NON-NLS-1$
            if ( aProp.size() > 1) {
                final String propName = (String) aProp.get(0);
                if ( propName.equalsIgnoreCase(XADRIVER_DATABASE_NAME) ) {
                    connectionProps.setProperty(XADRIVER_DATABASE_NAME, (String) aProp.get(1));
                } else {
                    // Set optional prop names lower case so that we can find
                    // set method names for them when we introspect the DataSource
                    connectionProps.setProperty(propName.toLowerCase(), (String) aProp.get(1));
                }
            }
        }
    }
    
    /**
     * @param props
     * @param dataSource
     */
    private static void setDSProperties(final Properties props, final DataSource dataSource) throws MetaMatrixException {
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
                    try {
                        method.invoke(dataSource, params);
                    } catch (Throwable e) {
                        final Object[] msgParams = new Object[]{propertyName, propertyValue};
                        final String msg = CommonPlugin.Util.getString(ErrorMessageKeys.CM_UTIL_ERR_0184, msgParams);  
                        throw new MetaMatrixException(ErrorMessageKeys.CM_UTIL_ERR_0184, msg); 
                    }
                }
            }
        }
    }

    private static Properties lowerCaseAllPropNames(final Properties connectionProps) {
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
