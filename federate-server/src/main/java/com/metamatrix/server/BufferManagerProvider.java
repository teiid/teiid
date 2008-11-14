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

package com.metamatrix.server;

import java.util.Properties;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.buffer.BufferManagerFactory;
import com.metamatrix.common.buffer.BufferManagerPropertyNames;
import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.util.FileUtils;

@Singleton
class BufferManagerProvider implements Provider<BufferManager> {

	Host host;
	String vmName;
	
	@Inject
	public BufferManagerProvider(@Named(Configuration.HOST) Host host, @Named(Configuration.VMNAME)String vmName) {
		this.host = host;
		this.vmName = vmName;
	}
	
	public BufferManager get() {
        try {
			// Get the properties for BufferManager
			Properties props = new Properties();
			props.putAll(CurrentConfiguration.getProperties());
			// Create buffer manager
			String dataDir = host.getDataDirectory();
			String relativeLoc = props.getProperty("metamatrix.buffer.relative.storageDirectory"); //$NON-NLS-1$
			
			String dir = FileUtils.buildDirectoryPath(new String[] {dataDir, relativeLoc});
			props.setProperty(BufferManagerPropertyNames.BUFFER_STORAGE_DIRECTORY, dir);

			return BufferManagerFactory.getServerBufferManager(host.getFullName()+"-"+vmName, props); //$NON-NLS-1$
		} catch (Exception e) {
			throw new MetaMatrixRuntimeException(e);
		}
	}

}
