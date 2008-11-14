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

package com.metamatrix.dqp.embedded.services;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.application.ApplicationEnvironment;
import com.metamatrix.common.application.exception.ApplicationInitializationException;
import com.metamatrix.common.application.exception.ApplicationLifecycleException;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.buffer.BufferManagerPropertyNames;
import com.metamatrix.common.buffer.StorageManager;
import com.metamatrix.common.buffer.impl.BufferManagerImpl;
import com.metamatrix.common.buffer.storage.file.FileStorageManager;
import com.metamatrix.common.buffer.storage.memory.MemoryStorageManager;
import com.metamatrix.dqp.embedded.DQPEmbeddedPlugin;
import com.metamatrix.dqp.service.BufferService;
import com.metamatrix.dqp.service.ConfigurationService;
import com.metamatrix.dqp.service.DQPServiceNames;
import com.metamatrix.dqp.service.DQPServiceRegistry;

/**
 * Implement the BufferService for the DQP Embedded component.  This implementation
 * may use either an all-memory model (which is prone to OutOfMemoryErrors) or 
 * a mixed disk/memory model which requires use of a directory on the disk 
 * for file service access.
 */
public class EmbeddedBufferService extends EmbeddedBaseDQPService implements BufferService {

    // Constants
    private static final String DEFAULT_MANAGEMENT_INTERVAL = "0"; //$NON-NLS-1$
    private static final String DEFAULT_LOG_STATS_INTERVAL = DEFAULT_MANAGEMENT_INTERVAL;
    private static final String DEFAULT_SESSION_USE_PERCENTAGE = "100"; //$NON-NLS-1$
    private static final String DEFAULT_ID_CREATOR = "com.metamatrix.common.buffer.impl.LongIDCreator"; //$NON-NLS-1$
    private static final String DEFAULT_MAX_OPEN_FILES = "10"; //$NON-NLS-1$
    
    // Instance
    private BufferManager bufferMgr;

    /**
     * 
     */
    public EmbeddedBufferService(DQPServiceRegistry svcRegistry) 
        throws MetaMatrixComponentException{
        super(DQPServiceNames.BUFFER_SERVICE, svcRegistry);
    }

    /**  
     * @param props
     * @throws ApplicationInitializationException
     */
    public void initializeService(Properties props) throws ApplicationInitializationException {
        try {
            
            ConfigurationService configurationSvc = this.getConfigurationService();
            
            boolean useDisk = configurationSvc.useDiskBuffering();
            File bufferDir = configurationSvc.getDiskBufferDirectory();                 
            String memAvail = configurationSvc.getBufferMemorySize();
            String processorBatchSize = configurationSvc.getProcessorBatchSize();
            String connectorBatchSize = configurationSvc.getConnectorBatchSize();
                
            // Set up buffer configuration properties
            Properties bufferProps = new Properties();                                  
            bufferProps.setProperty(BufferManagerPropertyNames.ID_CREATOR, DEFAULT_ID_CREATOR); 
            bufferProps.setProperty(BufferManagerPropertyNames.SESSION_USE_PERCENTAGE, DEFAULT_SESSION_USE_PERCENTAGE); 
            bufferProps.setProperty(BufferManagerPropertyNames.LOG_STATS_INTERVAL, DEFAULT_LOG_STATS_INTERVAL); 
            bufferProps.setProperty(BufferManagerPropertyNames.MANAGEMENT_INTERVAL, DEFAULT_MANAGEMENT_INTERVAL); 
            bufferProps.setProperty(BufferManagerPropertyNames.MEMORY_AVAILABLE, memAvail);
            bufferProps.setProperty(BufferManagerPropertyNames.BUFFER_STORAGE_DIRECTORY, bufferDir.getCanonicalPath());
            bufferProps.setProperty(BufferManagerPropertyNames.PROCESSOR_BATCH_SIZE, processorBatchSize); 
            bufferProps.setProperty(BufferManagerPropertyNames.CONNECTOR_BATCH_SIZE, connectorBatchSize); 
            // These are not set yet..
            //CONNECTOR_BATCH_SIZE
            //PROCESSOR_BATCH_SIZE
            
            // Construct and initialize the buffer manager
            this.bufferMgr = new BufferManagerImpl();
            this.bufferMgr.initialize("local", bufferProps); //$NON-NLS-1$
            
            // If necessary, add disk storage manager
            if(useDisk) {
                // Get the properties for FileStorageManager and create.
                Properties fsmProps = new Properties();
                fsmProps.setProperty(BufferManagerPropertyNames.BUFFER_STORAGE_DIRECTORY, bufferDir.getCanonicalPath());
                fsmProps.setProperty(BufferManagerPropertyNames.MAX_OPEN_FILES, DEFAULT_MAX_OPEN_FILES);
                StorageManager fsm = new FileStorageManager();
                fsm.initialize(fsmProps);        
                this.bufferMgr.addStorageManager(fsm);
                
                // start the file storrage manager in clean state
                // wise FileStorageManager is smart enough to clen up after itself
                cleanDirectory(bufferDir);
            }
            
            // Add unmanaged memory storage manager
            this.bufferMgr.addStorageManager(new MemoryStorageManager());
                     
        } catch(MetaMatrixComponentException e) { 
            throw new ApplicationInitializationException(e, DQPEmbeddedPlugin.Util.getString("LocalBufferService.Failed_initializing_buffer_manager._8")); //$NON-NLS-1$
        } catch(IOException e) {
            throw new ApplicationInitializationException(e, DQPEmbeddedPlugin.Util.getString("LocalBufferService.Failed_initializing_buffer_manager._8")); //$NON-NLS-1$            
        } 
    }

    /**
     * Clean the file storage directory on startup 
     * @param dir
     * @since 4.3
     */
    void cleanDirectory(File file) {
        if (file.exists()) {
            File[] files = file.listFiles();
            for (int i = 0; i < files.length; i++) {
                files[i].delete();                
            }
        }
    }
    /* 
     * @see com.metamatrix.common.application.ApplicationService#start(com.metamatrix.common.application.ApplicationEnvironment)
     */
    public void startService(ApplicationEnvironment environment) throws ApplicationLifecycleException {

    }
   
    /* 
     * @see com.metamatrix.common.application.ApplicationService#bind()
     */
    public void bindService() throws ApplicationLifecycleException {
    }

    /* 
     * @see com.metamatrix.common.application.ApplicationService#unbind()
     */
    public void unbindService() throws ApplicationLifecycleException {
    }

    /* 
     * @see com.metamatrix.common.application.ApplicationService#stop()
     */
    public void stopService() throws ApplicationLifecycleException {
        try {
            bufferMgr.stop();

            // Delete the buffer directory
            ConfigurationService configurationSvc = this.getConfigurationService();
            File bufferDir = configurationSvc.getDiskBufferDirectory();
            cleanDirectory(bufferDir);
            bufferDir.delete();
            
        } catch (MetaMatrixComponentException e) {
            throw new ApplicationLifecycleException(e);
        }        
    }

    /* 
     * @see com.metamatrix.dqp.service.BufferService#getBufferManager()
     */
    public BufferManager getBufferManager() {
        return this.bufferMgr;
    }
}
