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

import java.util.Properties;

import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.config.model.ConfigurationModelContainerAdapter;
import com.metamatrix.common.connection.ManagedConnectionException;
import com.metamatrix.common.extensionmodule.ExtensionModuleManager;
import com.metamatrix.platform.config.persistence.impl.file.FilePersistentConnection;

public class PersistentConnectionFactory {

	public static final String PERSISTENT_FACTORY_NAME = "metamatrix.config.persistent.factory"; //$NON-NLS-1$

	public static final String FILE_FACTORY_NAME = FilePersistentConnection.class.getName();

	private Properties properties;

	public PersistentConnectionFactory(Properties factoryProperties) {
		this.properties = factoryProperties;
	}

	public Properties getProperties() {
		return properties;
	}

	/**
	 * Creates the the of {@link PersistentConnection} required to communicate
	 * to the configuration repository.
	 * @param properties are the settings for persistence storage
	 * @return PersistentSource class for handling configuration storage
	 */
	public PersistentConnection createPersistentConnection(boolean readOnly)
		throws ConfigurationException {

        if (isFileFactory(properties)) {
            return new FilePersistentConnection(properties, new ConfigurationModelContainerAdapter());
        }
        ExtensionModuleManager manager = ExtensionModuleManager.getInstance();
		try {
	    	return new ExtensionModuleConnection(readOnly?manager.getReadTransaction():manager.getWriteTransaction());
		} catch (ManagedConnectionException e) {
			throw new ConfigurationException(e);
		}
	}

	private static boolean isFileFactory(Properties props) {
		if (FILE_FACTORY_NAME.equals(props.getProperty(PERSISTENT_FACTORY_NAME))) {
			return true;
		}
		String configFileName = props.getProperty(FilePersistentConnection.CONFIG_NS_FILE_NAME_PROPERTY);

		return configFileName != null && configFileName.length() > 0;
	}

}
