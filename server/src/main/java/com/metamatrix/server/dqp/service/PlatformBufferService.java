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

package com.metamatrix.server.dqp.service;

import java.util.Properties;

import org.teiid.dqp.internal.cache.DQPContextCache;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.application.ApplicationEnvironment;
import com.metamatrix.common.application.exception.ApplicationInitializationException;
import com.metamatrix.common.application.exception.ApplicationLifecycleException;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.buffer.BufferManagerPropertyNames;
import com.metamatrix.common.buffer.StorageManager;
import com.metamatrix.common.buffer.impl.BufferManagerImpl;
import com.metamatrix.common.buffer.storage.file.FileStorageManager;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.core.util.FileUtils;
import com.metamatrix.dqp.service.BufferService;
import com.metamatrix.server.Configuration;


/**
 */
public class PlatformBufferService implements BufferService {

    private BufferManager bufferMgr;
    private Properties props;
    
    private String processName;
    private Host host;
    private DQPContextCache contextCache;
    
    @Inject
    public PlatformBufferService(@Named(Configuration.HOST) Host host, @Named(Configuration.PROCESSNAME) String processName, DQPContextCache cache) {
    	this.host = host;
    	this.processName = processName;
    	this.contextCache = cache;
	}

    /* 
     * @see com.metamatrix.dqp.service.BufferService#getBufferManager()
     */
    public BufferManager getBufferManager() {
        return this.bufferMgr;
    }

    /* 
     * @see com.metamatrix.common.application.ApplicationService#initialize(java.util.Properties)
     */
    public void initialize(Properties props) throws ApplicationInitializationException {
    	this.props = PropertiesUtils.clone(props);
    }

    /* 
     * @see com.metamatrix.common.application.ApplicationService#start(com.metamatrix.common.application.ApplicationEnvironment)
     */
    public void start(ApplicationEnvironment environment) throws ApplicationLifecycleException {
    	// Get the properties for BufferManager
		// Create buffer manager
		String dataDir = host.getDataDirectory();
		String relativeLoc = props.getProperty("metamatrix.buffer.relative.storageDirectory"); //$NON-NLS-1$
		
		String dir = FileUtils.buildDirectoryPath(new String[] {dataDir, relativeLoc});
		props.setProperty(BufferManagerPropertyNames.BUFFER_STORAGE_DIRECTORY, dir);

		try {
			bufferMgr = getServerBufferManager(host.getFullName()+"-"+processName, props); //$NON-NLS-1$
		} catch (MetaMatrixComponentException e) {
			throw new ApplicationLifecycleException(e);
		} 
    }
    
    /**
     * Helper to get a buffer manager all set up for unmanaged standalone use.  This is
     * typically used for testing or when memory is not an issue.
     * @param lookup Lookup implementation to use
     * @param props Configuration properties
     * @return BufferManager ready for use
     */
    public static BufferManager getServerBufferManager(String lookup, Properties props) throws MetaMatrixComponentException {
        Properties bmProps = PropertiesUtils.clone(props, false);
        // Construct buffer manager
        BufferManager bufferManager = new BufferManagerImpl();
        bufferManager.initialize(lookup, bmProps);

        // Get the properties for FileStorageManager and create.
        StorageManager fsm = new FileStorageManager();
        fsm.initialize(bmProps);
        bufferManager.setStorageManager(fsm);

        return bufferManager;
    }

    /* 
     * @see com.metamatrix.common.application.ApplicationService#stop()
     */
    public void stop() throws ApplicationLifecycleException {
    	bufferMgr.stop();
    }

	@Override
	public DQPContextCache getContextCache() {
		return this.contextCache;
	}


}
