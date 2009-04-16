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
import com.metamatrix.common.buffer.BufferManagerFactory;
import com.metamatrix.common.buffer.BufferManagerPropertyNames;
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
			bufferMgr = BufferManagerFactory.getServerBufferManager(host.getFullName()+"-"+processName, props); //$NON-NLS-1$
		} catch (MetaMatrixComponentException e) {
			throw new ApplicationLifecycleException(e);
		} 
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
