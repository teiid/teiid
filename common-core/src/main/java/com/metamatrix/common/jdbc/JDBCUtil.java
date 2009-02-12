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

package com.metamatrix.common.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.util.Properties;

import com.metamatrix.api.exception.MetaMatrixException;
import com.metamatrix.common.util.crypto.CryptoUtil;
import com.metamatrix.core.CorePlugin;
import com.metamatrix.core.ErrorMessageKeys;

public class JDBCUtil {
    /**
     * The environment property name for the class of the driver.
     * This property is optional.
     */
    public static final String DRIVER = "metamatrix.common.pooling.jdbc.Driver"; //$NON-NLS-1$

    /**
     * The environment property name for the protocol that is to be used.  For JDBC, the connection
     * URL information is created of the form "jdbc:subprotocol:subname", where the value
     * of the PROTOCOL property is used for the "subprotocol:subname" portion.
     * This property is required.
     */
    public static final String PROTOCOL = "metamatrix.common.pooling.jdbc.Protocol"; //$NON-NLS-1$

    /**
     * The environment property name for the database name.  This may include the server name and port number,
     * per the driver's requirements.
     * This property is required.
     */
    public static final String DATABASE = "metamatrix.common.pooling.jdbc.Database"; //$NON-NLS-1$

    /**
     * The environment property name for the username that is to be used for connecting to the metadata store.
     * This property is required.
     */
    public static final String USERNAME = "metamatrix.common.pooling.jdbc.User"; //$NON-NLS-1$

    /**
     * The environment property name for the password that is to be used for connecting to the metadata store.
     * This property is required.
     */
    public static final String PASSWORD = "metamatrix.common.pooling.jdbc.Password"; //$NON-NLS-1$

    private static final String JDBC_PREFIX = "jdbc:";//$NON-NLS-1$
    // This is the prop name the JDBC driver expects for the password property
    private static final String DRIVER_PWD_PROP_NAME = "password"; //$NON-NLS-1$
    // This is the prop name the JDBC driver expects for the password property
    private static final String DRIVER_USER_PROP_NAME = "user"; //$NON-NLS-1$
    
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
	    if (CryptoUtil.isValueEncrypted(password)) {
	    	password = CryptoUtil.stringDecrypt(password); 
	    }
	    // Verify required items
	    if (driverClassName == null || driverClassName.trim().length() == 0) {
	        throw new MetaMatrixException(ErrorMessageKeys.CM_UTIL_ERR_0176, CorePlugin.Util.getString(ErrorMessageKeys.CM_UTIL_ERR_0176));
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
                throw new MetaMatrixException(e,ErrorMessageKeys.CM_UTIL_ERR_0179, CorePlugin.Util.getString(ErrorMessageKeys.CM_UTIL_ERR_0179, driverClassName));
            }
            if(!driver.acceptsURL(url)){
                throw new MetaMatrixException(ErrorMessageKeys.CM_UTIL_ERR_0180, CorePlugin.Util.getString(ErrorMessageKeys.CM_UTIL_ERR_0180, driverClassName, url));
            }

            connection = driver.connect(url, props);
        } catch ( Exception e ) {
            throw new MetaMatrixException(e, ErrorMessageKeys.CM_UTIL_ERR_0181, CorePlugin.Util.getString(ErrorMessageKeys.CM_UTIL_ERR_0181, url));
        }

        return connection;
    }

    public static String getDatabaseURL( Properties env ) throws MetaMatrixException {
	    String protocol        = env.getProperty(PROTOCOL);
	    String database        = env.getProperty(DATABASE);

	    if (database == null || database.trim().length() == 0) {
            throw new MetaMatrixException(ErrorMessageKeys.CM_UTIL_ERR_0178, CorePlugin.Util.getString(ErrorMessageKeys.CM_UTIL_ERR_0178));
        }
        // if the database property contains the whole url and the protocol is empty then 
        // use the database as the url
        if (database.startsWith(JDBC_PREFIX) && (protocol == null || protocol.trim().length() == 0)) {
             return database;    
        } else if (protocol == null || protocol.trim().length() == 0) {
	        throw new MetaMatrixException(ErrorMessageKeys.CM_UTIL_ERR_0177, CorePlugin.Util.getString(ErrorMessageKeys.CM_UTIL_ERR_0177));
	    } 
	    return JDBC_PREFIX + env.getProperty(PROTOCOL) + ":" + env.getProperty(DATABASE); //$NON-NLS-1$ 
    }
    
}
