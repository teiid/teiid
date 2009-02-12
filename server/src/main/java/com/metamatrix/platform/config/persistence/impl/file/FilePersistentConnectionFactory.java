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

package com.metamatrix.platform.config.persistence.impl.file;

import java.util.Properties;

import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.config.model.ConfigurationModelContainerAdapter;
import com.metamatrix.platform.PlatformPlugin;
import com.metamatrix.platform.config.persistence.api.PersistentConnection;
import com.metamatrix.platform.config.persistence.api.PersistentConnectionFactory;
import com.metamatrix.platform.util.ErrorMessageKeys;

public class FilePersistentConnectionFactory extends PersistentConnectionFactory {

	/**
	 * Constructor for FilePersistentConnectionFactory.
	 */
	public FilePersistentConnectionFactory(Properties factoryProperties) throws ConfigurationException {
		super(factoryProperties);

		String filename = factoryProperties.getProperty(FilePersistentConnection.CONFIG_NS_FILE_NAME_PROPERTY);
		if (filename != null && filename.length() > 0) {
		} else {
			throw new ConfigurationException(ErrorMessageKeys.CONFIG_0029, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0029,
					FilePersistentConnection.CONFIG_NS_FILE_NAME_PROPERTY ));
		}
	}


	/**
	 * Creates the the of {@link PersistentConnection} required to communicate
	 * to the configuration repository.
	 * @param properties are the settings for persistence storage
	 * @return PersistentSource class for handling configuration storage
	 */
	public PersistentConnection createPersistentConnection()
		throws ConfigurationException {


		ConfigurationModelContainerAdapter adapter =
			new ConfigurationModelContainerAdapter();

		FilePersistentConnection fps =
			new FilePersistentConnection(this.getProperties(), adapter);

		return fps;
	}

}
