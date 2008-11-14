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

import java.sql.Connection;
import java.util.Properties;

import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.exceptions.ConfigurationConnectionException;
import com.metamatrix.common.jdbc.JDBCUtil;
import com.metamatrix.platform.PlatformPlugin;
import com.metamatrix.platform.config.persistence.api.PersistentConnection;
import com.metamatrix.platform.config.persistence.api.PersistentConnectionFactory;
import com.metamatrix.platform.util.ErrorMessageKeys;

public class JDBCPersistentUtil {



    public static ConfigurationModelContainer readModel(String url,
    									String protocol,
    									String driver,
    									String user,
    									String pw,
    									ConfigurationID configID) throws Exception {

		Properties props = new Properties();
		props.setProperty(JDBCPersistentConnectionFactory.DATABASE, url);
		props.setProperty(JDBCPersistentConnectionFactory.DRIVER, driver);
		props.setProperty(JDBCPersistentConnectionFactory.PASSWORD, pw);
		props.setProperty(JDBCPersistentConnectionFactory.USERNAME, user);
		if (protocol != null && protocol.trim().length() > 0) {
			props.setProperty(JDBCPersistentConnectionFactory.PROTOCOL, protocol);
		}

		return readModel(props, configID);
	}




    public static ConfigurationModelContainer readModel(Properties props, ConfigurationID configID) throws Exception {

            PersistentConnectionFactory pf = PersistentConnectionFactory.createPersistentConnectionFactory(props);

            PersistentConnection readin = pf.createPersistentConnection();

            ConfigurationModelContainer model = readin.read(configID);

            return model;

	}

    public static void writeModel(String url,
    									String protocol,
    									String driver,
    									String user,
    									String pw,
    									ConfigurationModelContainer model, String principal) throws Exception {

		Properties props = new Properties();
		props.setProperty(JDBCPersistentConnectionFactory.DATABASE, url);
		props.setProperty(JDBCPersistentConnectionFactory.DRIVER, driver);
		props.setProperty(JDBCPersistentConnectionFactory.PASSWORD, pw);
		props.setProperty(JDBCPersistentConnectionFactory.USERNAME, user);
		if (protocol != null && protocol.trim().length() > 0) {
			props.setProperty(JDBCPersistentConnectionFactory.PROTOCOL, protocol);
		}		

		writeModel(props, model, principal);
	}


	public static void writeModel(Properties props, ConfigurationModelContainer model, String principal) throws Exception {

            PersistentConnectionFactory pf = PersistentConnectionFactory.createPersistentConnectionFactory(props);

            PersistentConnection writer = pf.createPersistentConnection();

            writer.write(model, principal);
	}

    /**
     * env properties are exprected to be that of {@see JDBCPersistentConnection}
     */

    public static Connection getConnection(Properties env) throws ConfigurationConnectionException {
        // Get the JDBC properties ...

     	String driverClassName = env.getProperty(JDBCPersistentConnectionFactory.DRIVER);
	    String protocol        = env.getProperty(JDBCPersistentConnectionFactory.PROTOCOL);
	    String database        = env.getProperty(JDBCPersistentConnectionFactory.DATABASE);
	    String username        = env.getProperty(JDBCPersistentConnectionFactory.USERNAME);
	    String password        = env.getProperty(JDBCPersistentConnectionFactory.PASSWORD);
	    // Verify required items
	    if (driverClassName == null || driverClassName.trim().length() == 0) {
	        throw new ConfigurationConnectionException(ErrorMessageKeys.CONFIG_0030, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0030));
	    }

	    if (database == null || database.trim().length() == 0) {
	        throw new ConfigurationConnectionException(ErrorMessageKeys.CONFIG_0032, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0032));
	    }

		Properties props = new Properties();
		props.setProperty(JDBCUtil.DATABASE, database);
		props.setProperty(JDBCUtil.DRIVER, driverClassName);
       	if (protocol != null && protocol.trim().length() > 0) {
			props.setProperty(JDBCUtil.PROTOCOL, protocol);
		}
		props.setProperty(JDBCUtil.USERNAME, username);
		props.setProperty(JDBCUtil.PASSWORD, password);

		try {
			Connection connection = JDBCUtil.decryptAndCreateJDBCConnection(props);
			return connection;
		} catch (Exception e) {
			throw new ConfigurationConnectionException(e, ErrorMessageKeys.CONFIG_0033, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0033));
		}
    }

    /**
     * env properties are exprected to be that of JDBCConnectionResource
     */
//    public static Connection getConnectionx( Properties env ) throws ConfigurationConnectionException{
//         // Get the JDBC properties ...
//		Connection connection = null;
//
//        String keyPwd           = env.getProperty(PasswordCryptoFactory.PASS_KEY_NAME);
//
//        try {
//            // initialize the factory since current configuration
//            // should be the first to access the database
//            PasswordCryptoFactory.init(keyPwd.toCharArray());
//        } catch (CryptoException e) {
//            throw new ConfigurationConnectionException(e, ErrorMessageKeys.CONFIG_0034, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0034));
//        }
//
//       //String dName = "No Descriptor Created";
//        try {
//
//
//            ResourceDescriptor descriptor = JDBCConnectionPoolHelper.createDescriptor(Configuration.NEXT_STARTUP_ID,
//                                        ResourcePool.JDBC_SHARED_CONNECTION_POOL,
//                                        env);
//
//
//            connection = JDBCConnectionPoolHelper.getConnection(descriptor, "CurrentConfiguration"); //$NON-NLS-1$
//
//
//        } catch(Exception e) {
//            e.printStackTrace();
//            throw new ConfigurationConnectionException(e, ErrorMessageKeys.CONFIG_0035, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0035));
//
//        }
//
//        if ( connection == null ) {
//            throw new ConfigurationConnectionException(ErrorMessageKeys.CONFIG_0036, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0036));
//        }
//
//        return connection;
//
//    }




}
