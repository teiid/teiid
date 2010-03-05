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

package org.teiid.services;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;

import org.teiid.dqp.internal.cache.DQPContextCache;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.cache.CacheFactory;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.buffer.impl.BufferManagerImpl;
import com.metamatrix.common.buffer.impl.FileStorageManager;
import com.metamatrix.common.buffer.impl.MemoryStorageManager;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.dqp.embedded.DQPEmbeddedPlugin;
import com.metamatrix.dqp.service.BufferService;

/**
 * Implement the BufferService for the DQP Embedded component.  This implementation
 * may use either an all-memory model (which is prone to OutOfMemoryErrors) or 
 * a mixed disk/memory model which requires use of a directory on the disk 
 * for file service access.
 */
public class BufferServiceImpl implements BufferService, Serializable {
	private static final long serialVersionUID = -6217808623863643531L;

	private static final int DEFAULT_MAX_OPEN_FILES = 256;
	
    // Instance
    private BufferManagerImpl bufferMgr;
	private File bufferDir;
	private boolean useDisk = true;
	private int memorySize = 64;
	private DQPContextCache contextCache;
	private int processorBatchSize = 2000;
	private int connectorBatchSize = 2000;
	private CacheFactory cacheFactory;
    private int maxOpenFiles = DEFAULT_MAX_OPEN_FILES;
    private long maxFileSize = 2L; // 2GB
    private int maxProcessingBatchesColumns = BufferManager.DEFAULT_MAX_PROCESSING_BATCHES;
    private int maxReserveBatchColumns = BufferManager.DEFAULT_RESERVE_BUFFERS;
	
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

    public void start(){
    	try {

            // Construct and initialize the buffer manager
            this.bufferMgr = new BufferManagerImpl();
            this.bufferMgr.setConnectorBatchSize(Integer.valueOf(connectorBatchSize));
            this.bufferMgr.setProcessorBatchSize(Integer.valueOf(processorBatchSize));
            this.bufferMgr.setMaxReserveBatchColumns(this.maxReserveBatchColumns);
            this.bufferMgr.setMaxProcessingBatchColumns(this.maxProcessingBatchesColumns);
            
            this.bufferMgr.initialize();
            
            // If necessary, add disk storage manager
            if(useDisk) {
                // Get the properties for FileStorageManager and create.
                FileStorageManager fsm = new FileStorageManager();
                fsm.setStorageDirectory(bufferDir.getCanonicalPath());
                fsm.setMaxFileSize(maxFileSize);
                fsm.setMaxOpenFiles(maxOpenFiles);
                fsm.initialize();        
                this.bufferMgr.setStorageManager(fsm);
                
                // start the file storage manager in clean state
                // wise FileStorageManager is smart enough to clen up after itself
                cleanDirectory(bufferDir);
            } else {
            	this.bufferMgr.setStorageManager(new MemoryStorageManager());
            }
            
        } catch(MetaMatrixComponentException e) { 
            throw new MetaMatrixRuntimeException(e, DQPEmbeddedPlugin.Util.getString("LocalBufferService.Failed_initializing_buffer_manager._8")); //$NON-NLS-1$
        } catch(IOException e) {
            throw new MetaMatrixRuntimeException(e, DQPEmbeddedPlugin.Util.getString("LocalBufferService.Failed_initializing_buffer_manager._8")); //$NON-NLS-1$            
        }
    }
   
    public void stop() {
        bufferMgr.shutdown();

        // Delete the buffer directory
        if (bufferDir != null) {
	        cleanDirectory(bufferDir);
	        bufferDir.delete();
        }
    }

    public BufferManager getBufferManager() {
        return this.bufferMgr;
    }

	@Override
	public DQPContextCache getContextCache() {
		return this.contextCache;
	}
	
	public void setContextCache(DQPContextCache cache) {
		this.contextCache = cache;
	}
	
	public void setUseDisk(boolean flag) {
		this.useDisk = flag;
	}

	public void setDiskDirectory(String dir) {
		this.bufferDir = new File(dir, "buffer");
		if (!bufferDir.exists()) {
			this.bufferDir.mkdirs();
		}
	}

	public void setBufferMemorySizeInMB(int size) {
		this.memorySize = size;
	}

	public void setProcessorBatchSize(int size) {
		this.processorBatchSize = size;
	}
	public void setConnectorBatchSize(int size) {
		this.connectorBatchSize = size;
	}

	public File getBufferDirectory() {
		return bufferDir;
	}

	public boolean isUseDisk() {
		return useDisk;
	}

	public int getBufferMemorySizeInMB() {
		return memorySize;
	}

	public int getProcessorBatchSize() {
		return processorBatchSize;
	}

	public int getConnectorBatchSize() {
		return connectorBatchSize;
	}

	@Override
	public CacheFactory getCacheFactory() {
		return this.cacheFactory;
	}
	
	public void setCacheFactory(CacheFactory cf) {
		this.cacheFactory = cf;
	}
	
    public void setMaxFileSize(long maxFileSize) {
    	this.maxFileSize = maxFileSize;
	}
    
    public void setMaxOpenFiles(int maxOpenFiles) {
		this.maxOpenFiles = maxOpenFiles;
	}	
    
    public void setMaxReserveBatchColumns(int value) {
		this.maxReserveBatchColumns = value;
	}    
    
    public void setMaxProcessingBatchesColumns(int value) {
    	this.maxProcessingBatchesColumns  = value;
    }
}
