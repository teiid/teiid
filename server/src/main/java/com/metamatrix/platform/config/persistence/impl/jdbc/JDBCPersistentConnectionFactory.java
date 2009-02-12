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

package com.metamatrix.platform.config.persistence.impl.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import com.metamatrix.common.config.JDBCConnectionPoolHelper;
import com.metamatrix.common.config.api.exceptions.ConfigurationConnectionException;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.config.model.ConfigurationModelContainerAdapter;
import com.metamatrix.platform.config.persistence.api.PersistentConnection;
import com.metamatrix.platform.config.persistence.api.PersistentConnectionFactory;

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

	/**
	 * Constructor for JDBCPersistentConnectionFactory.
	 * @param factoryProperties
	 */
	public JDBCPersistentConnectionFactory(Properties factoryProperties) {
		super(factoryProperties);
	}

	/**
	 * Creates the the of {@link PersistentConnection} required to communicate
	 * to the configuration repository.
	 * @param properties are the settings for persistence storage
	 * @return PersistentSource class for handling configuration storage
	 */
	public PersistentConnection createPersistentConnection()
		throws ConfigurationException {

		Connection conn;
		try {
			conn = JDBCConnectionPoolHelper.getInstance().getConnection();
		} catch (SQLException e) {
			throw new ConfigurationConnectionException(e);
		}

		ConfigurationModelContainerAdapter adapter =
			new ConfigurationModelContainerAdapter();

		JDBCPersistentConnection fps =
			new JDBCPersistentConnection(conn, adapter, getProperties());


		return fps;

	}

}
