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

package com.metamatrix.platform.config.persistence.api;

import java.util.Arrays;
import java.util.Properties;

import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.core.MetaMatrixCoreException;
import com.metamatrix.core.util.ReflectionHelper;
import com.metamatrix.platform.config.ConfigMessages;
import com.metamatrix.platform.config.ConfigPlugin;
import com.metamatrix.platform.config.persistence.impl.file.FilePersistentConnectionFactory;
import com.metamatrix.platform.config.persistence.impl.jdbc.JDBCPersistentConnectionFactory;

public abstract class PersistentConnectionFactory {

	public static final String PERSISTENT_FACTORY_NAME = "metamatrix.config.persistent.factory"; //$NON-NLS-1$


	public static final String FILE_FACTORY_NAME = FilePersistentConnectionFactory.class.getName();
	public static final String JDBC_FACTORY_NAME = JDBCPersistentConnectionFactory.class.getName();

	private Properties properties;

	public PersistentConnectionFactory(Properties factoryProperties) {
		this.properties = factoryProperties;
	}

	public Properties getProperties() {
		return properties;
	}
    /**
     * createPersistentConnectionFactory is used for bootstrapping the system.
     * The connection is normally only used for starting the system, then
     * then {@link #createPersistentConnectionFactory} is used.   
     * @param props
     * @return
     * @throws ConfigurationException
     */
    public static final PersistentConnectionFactory createPersistentConnectionFactory(Properties props) throws ConfigurationException {

        Properties properties = PropertiesUtils.clone(props, false);
        String factoryName = properties.getProperty(PERSISTENT_FACTORY_NAME);

        if (factoryName == null || factoryName.trim().length() == 0) {
            // if no factory name, then check if this a file connection
            if (isFileFactory(properties)) {
                return new FilePersistentConnectionFactory(properties);
            }

            if (isJDBCFactory(properties)) {
            	return new JDBCPersistentConnectionFactory(properties);
            }

            throw new ConfigurationException(ConfigMessages.CONFIG_0009, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0009, PERSISTENT_FACTORY_NAME));

        }
        try {
            return (PersistentConnectionFactory) ReflectionHelper.create(factoryName, Arrays.asList(properties), Thread.currentThread().getContextClassLoader()); 
            //create(factoryName, args);
        } catch (MetaMatrixCoreException err) {
            throw new ConfigurationException(err, ConfigMessages.CONFIG_0013, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0013, factoryName));
            
        }
    }

	/**
	 * Creates the the of {@link PersistentConnection} required to communicate
	 * to the configuration repository.
	 * @param properties are the settings for persistence storage
	 * @return PersistentSource class for handling configuration storage
	 */
	public abstract PersistentConnection createPersistentConnection()
		throws ConfigurationException;


	private static boolean isFileFactory(Properties props) {
		String configFileName = props.getProperty("metamatrix.config.ns.filename");

		return configFileName != null && configFileName.length() > 0;
	}

	private static boolean isJDBCFactory(Properties props) {
		String driver = props.getProperty("metamatrix.config.jdbc.persistent.readerDriver");

		return driver != null && driver.length() > 0;
	}

}
