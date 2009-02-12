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

package com.metamatrix.common.buffer;

import java.util.Properties;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.buffer.impl.BufferManagerImpl;
import com.metamatrix.common.buffer.storage.file.FileStorageManager;
import com.metamatrix.common.buffer.storage.memory.MemoryStorageManager;
import com.metamatrix.common.util.PropertiesUtils;

/**
 * <p>Factory for BufferManager instances.  One method will get
 * a server buffer manager, as it should be instantiated in a running
 * MetaMatrix server.  That BufferManager is configured mostly by the
 * passed in properties.</p>
 *
 * <p>The other method returns a stand-alone, in-memory buffer manager.  This
 * is typically used for either in-memory testing or any time the
 * query processor component is not expected to run out of memory, such as
 * within the modeler.</p>
 */
public class BufferManagerFactory {

	private static BufferManager INSTANCE;
	
    /**
     * Helper to get a buffer manager all set up for unmanaged standalone use.  This is
     * typically used for testing or when memory is not an issue.
     * @return BufferManager ready for use
     */
    public static BufferManager getStandaloneBufferManager() throws MetaMatrixComponentException {
    	if (INSTANCE == null) {
	        BufferManager bufferMgr = new BufferManagerImpl();
	        Properties props = new Properties();
	        props.setProperty(BufferManagerPropertyNames.MEMORY_AVAILABLE, String.valueOf(Long.MAX_VALUE));
	        props.setProperty(BufferManagerPropertyNames.SESSION_USE_PERCENTAGE, "100"); //$NON-NLS-1$
	        props.setProperty(BufferManagerPropertyNames.LOG_STATS_INTERVAL, "0"); //$NON-NLS-1$
	        props.setProperty(BufferManagerPropertyNames.MANAGEMENT_INTERVAL, "0"); //$NON-NLS-1$
	        bufferMgr.initialize("local", props); //$NON-NLS-1$
	
	        // Add unmanaged memory storage manager
	        bufferMgr.addStorageManager(new MemoryStorageManager());
	        bufferMgr.addStorageManager(new MemoryStorageManager() {
	        	@Override
	        	public int getStorageType() {
	        		return StorageManager.TYPE_FILE;
	        	}
	        });
	        INSTANCE = bufferMgr;
    	}

        return INSTANCE;
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
        bufferManager.addStorageManager(fsm);

        // Create MemoryStorageManager
        StorageManager msm = new MemoryStorageManager();
        msm.initialize(bmProps);
        bufferManager.addStorageManager(msm);

        return bufferManager;
    }

}
