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

import org.jboss.managed.api.annotation.ManagementComponent;
import org.jboss.managed.api.annotation.ManagementObject;
import org.jboss.managed.api.annotation.ManagementProperties;
import org.jboss.managed.api.annotation.ManagementProperty;
import org.teiid.dqp.internal.cache.DQPContextCache;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.cache.CacheFactory;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.buffer.impl.BufferManagerImpl;
import com.metamatrix.common.buffer.impl.FileStorageManager;
import com.metamatrix.common.buffer.impl.MemoryStorageManager;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.util.FileUtils;
import com.metamatrix.dqp.embedded.DQPEmbeddedPlugin;
import com.metamatrix.dqp.service.BufferService;

/**
 * Implement the BufferService for the DQP Embedded component.  This implementation
 * may use either an all-memory model (which is prone to OutOfMemoryErrors) or 
 * a mixed disk/memory model which requires use of a directory on the disk 
 * for file service access.
 */
@ManagementObject(componentType=@ManagementComponent(type="teiid",subtype="dqp"), properties=ManagementProperties.EXPLICIT)
public class BufferServiceImpl implements BufferService, Serializable {
	private static final long serialVersionUID = -6217808623863643531L;

    // Instance
    private BufferManagerImpl bufferMgr;
	private File bufferDir;
	private boolean useDisk = true;
	private DQPContextCache contextCache;
	private int processorBatchSize = BufferManager.DEFAULT_PROCESSOR_BATCH_SIZE;
	private int connectorBatchSize = BufferManager.DEFAULT_CONNECTOR_BATCH_SIZE;
	private CacheFactory cacheFactory;
    private int maxOpenFiles = FileStorageManager.DEFAULT_MAX_OPEN_FILES;
    private long maxFileSize = FileStorageManager.DEFAULT_MAX_FILESIZE; // 2GB
    private int maxProcessingBatchesColumns = BufferManager.DEFAULT_MAX_PROCESSING_BATCHES;
    private int maxReserveBatchColumns = BufferManager.DEFAULT_RESERVE_BUFFERS;
	
    /**
     * Clean the file storage directory on startup 
     * @param dir
     * @since 4.3
     */
    void cleanDirectory(File file) {
    	FileUtils.removeChildrenRecursively(file);
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
		return this.useDisk;
	}

	@ManagementProperty(description="The max row count of a batch sent internally within the query processor. Should be <= the connectorBatchSize. (default 256)")
	public int getProcessorBatchSize() {
		return this.processorBatchSize;
	}

	@ManagementProperty(description="The max row count of a batch from a connector. Should be even multiple of processorBatchSize. (default 512)")
	public int getConnectorBatchSize() {
		return this.connectorBatchSize;
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
    
    public void setMaxReserveBatchColumns(int value) {
		this.maxReserveBatchColumns = value;
	}    
    
    public void setMaxProcessingBatchesColumns(int value) {
    	this.maxProcessingBatchesColumns  = value;
    }

    @ManagementProperty(description="Max file size for buffer files (default 2GB)")
	public long getMaxFileSize() {
		return maxFileSize;
	}

    @ManagementProperty(description="#The number of batch columns guarenteed to a processing operation.  Set this value lower if the workload typically" + 
    		"processes larger numbers of concurrent queries with large intermediate results from operations such as sorting, " + 
    		"grouping, etc. (default 128)")
	public int getMaxProcessingBatchesColumns() {
		return maxProcessingBatchesColumns;
	}

    @ManagementProperty(description="#The number of batch columns to allow in memory (default 16384).  " + 
    		"This value should be set lower or higher depending on the available memory to Teiid in the VM.  " + 
    		"16384 is considered a good default for a dedicated 32-bit VM running Teiid with a 1 gig heap.")
	public int getMaxReserveBatchColumns() {
		return maxReserveBatchColumns;
	}
}
