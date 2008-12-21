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

package com.metamatrix.platform.config.persistence.impl.jdbc;

import java.sql.*;
import java.util.Properties;

import com.metamatrix.common.config.JDBCConnectionPoolHelper;
import com.metamatrix.common.config.api.exceptions.ConfigurationConnectionException;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.pooling.api.ResourcePool;
import com.metamatrix.common.pooling.api.exception.ResourcePoolException;
import com.metamatrix.common.pooling.jdbc.JDBCConnectionResource;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.platform.PlatformPlugin;
import com.metamatrix.platform.config.persistence.api.PersistentConnection;
import com.metamatrix.platform.config.persistence.api.PersistentConnectionFactory;
import com.metamatrix.platform.config.persistence.impl.ConfigurationModelAdapterImpl;
import com.metamatrix.platform.util.ErrorMessageKeys;

public class JDBCPersistentConnectionFactory
	extends PersistentConnectionFactory {

    /**
     * The environment property name for the class of the driver.
     * This property is optional.
     */
    public static final String DRIVER = "metamatrix.config.jdbc.persistent.readerDriver"; //$NON-NLS-1$


    /**
     * The environment property name for the protocol that is to be used.  For JDBC, the connection
     * URL information is created of the form "jdbc:subprotocol:subname", where the value
     * of the PROTOCOL property is used for the "subprotocol:subname" portion.
     * This property is required.
     */
    public static final String PROTOCOL = "metamatrix.config.jdbc.persistent.readerProtocol"; //$NON-NLS-1$

    /**
     * The environment property name for the database name.  This may include the server name and port number,
     * per the driver's requirements.
     * This property is required.
     */
    public static final String DATABASE = "metamatrix.config.jdbc.persistent.readerDatabase"; //$NON-NLS-1$

    /**
     * The environment property name for the username that is to be used for connecting to the metadata store.
     * This property is required.
     */
    public static final String USERNAME = "metamatrix.config.jdbc.persistent.readerPrincipal"; //$NON-NLS-1$

    /**
     * The environment property name for the password that is to be used for connecting to the metadata store.
     * This property is required.
     */
    public static final String PASSWORD = "metamatrix.config.jdbc.persistent.readerPassword"; //$NON-NLS-1$

    private static final String USER="CONFIGURATION"; //$NON-NLS-1$
	private Properties connProps;
    private boolean usePooling;

	/**
	 * Constructor for JDBCPersistentConnectionFactory.
	 * @param factoryProperties
	 */
	public JDBCPersistentConnectionFactory(Properties factoryProperties) {
		super(factoryProperties);
//		System.out.println("JDBC Persistence is being used for the Repository");


	}

    /**
     * Constructor for JDBCPersistentConnectionFactory.
     * @param factoryProperties
     */
    public JDBCPersistentConnectionFactory(Properties factoryProperties, Boolean useResourcePooling) {
        super(factoryProperties);
        this.usePooling = useResourcePooling.booleanValue();
        
//      System.out.println("JDBC Persistence is being used for the Repository");


    }

	/**
	 * Creates the the of {@link PersistentConnection} required to communicate
	 * to the configuration repository.
	 * @param properties are the settings for persistence storage
	 * @return PersistentSource class for handling configuration storage
	 */
	public PersistentConnection createPersistentConnection()
		throws ConfigurationException {

		connProps = validateProperties(getProperties());

		Connection conn = createConnection(connProps);

		ConfigurationModelAdapterImpl adapter =
			new ConfigurationModelAdapterImpl();

		JDBCPersistentConnection fps =
			new JDBCPersistentConnection(conn, adapter, getProperties());


		return fps;

	}

	/**
	 * Called allow the persistent connection to initialize itself prior to
	 * its first use.
	 */
	public Connection createConnection(Properties props) throws ConfigurationException {
        DriverManager.setLoginTimeout(480);
        Connection connection = null;
        if (usePooling) {
            try {
                connection = JDBCConnectionPoolHelper.getConnection(ResourcePool.JDBC_SHARED_CONNECTION_POOL, USER);
            } catch (ResourcePoolException e) {
                throw new ConfigurationException(e);

            }

        
        } else {
            connection = JDBCPersistentUtil.getConnection(props);

        }
        return connection;

	}


	protected Properties validateProperties(Properties props) throws ConfigurationException {
       Properties envClone = PropertiesUtils.clone(props, false);

        String driver;
        String database;
        String username;
        String password;
        String protocol;

		driver = props.getProperty(DRIVER);
		if (driver != null && driver.length() > 0) {
//		System.out.println("JDBC Persistence using Persistent Properties");


	    	protocol        = props.getProperty(PROTOCOL);
	    	database        = props.getProperty(DATABASE);
	    	username        = props.getProperty(USERNAME);
	    	password        = props.getProperty(PASSWORD);

	  // the persistent properties were not passed, check for pooling properties.
	  // the pooling properties are what exist in the bootstratp file
		} else {
//		System.out.println("JDBC Persistence using Pooling Properties");



        	driver = props.getProperty(JDBCConnectionResource.DRIVER);
 	    	protocol        = props.getProperty(JDBCConnectionResource.PROTOCOL);
	    	database        = props.getProperty(JDBCConnectionResource.DATABASE);
	    	username        = props.getProperty(JDBCConnectionResource.USERNAME);
	    	password        = props.getProperty(JDBCConnectionResource.PASSWORD);
		}

	    // Verify required items
	    if (driver == null || driver.trim().length() == 0) {
	        throw new ConfigurationConnectionException(ErrorMessageKeys.CONFIG_0030, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0030));
	    }
	    envClone.setProperty(DRIVER, driver);
	    if (protocol != null && protocol.trim().length() > 0) {
		 	envClone.setProperty(PROTOCOL, protocol);
	    }
	   
	    if (database == null || database.trim().length() == 0) {
	        throw new ConfigurationConnectionException(ErrorMessageKeys.CONFIG_0032, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0032));
	    }
	    envClone.setProperty(DATABASE, database);

	    if (username != null && username.length() > 0) {
	    	envClone.setProperty(USERNAME, username);
	    }

	    if (password != null && password.length() > 0) {
	    	envClone.setProperty(PASSWORD, password);
	    }

		return envClone;

	}

}
