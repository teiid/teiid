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
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.impl.BufferManagerImpl;
import org.teiid.common.buffer.impl.FileStorageManager;
import org.teiid.common.buffer.impl.MemoryStorageManager;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.FileUtils;
import org.teiid.dqp.service.BufferService;
import org.teiid.runtime.RuntimePlugin;


/**
 * Implements the BufferService.  This implementation
 * may use either an all-memory model (which is typically only for testing) or 
 * a mixed disk/memory model which requires use of a directory on the disk 
 * for file service access.
 */
@ManagementObject(name="BufferService", componentType=@ManagementComponent(type="teiid",subtype="dqp"), properties=ManagementProperties.EXPLICIT)
public class BufferServiceImpl implements BufferService, Serializable {
	private static final long serialVersionUID = -6217808623863643531L;
	private static final long MB = 1<<20;

    // Instance
    private BufferManagerImpl bufferMgr;
	private File bufferDir;
	private boolean useDisk = true;
	private int processorBatchSize = BufferManager.DEFAULT_PROCESSOR_BATCH_SIZE;
	private int connectorBatchSize = BufferManager.DEFAULT_CONNECTOR_BATCH_SIZE;
    private int maxOpenFiles = FileStorageManager.DEFAULT_MAX_OPEN_FILES;
    private long maxFileSize = FileStorageManager.DEFAULT_MAX_FILESIZE; // 2GB
    private int maxProcessingBatchesColumns = BufferManager.DEFAULT_MAX_PROCESSING_BATCHES;
    private int maxReserveBatchColumns = BufferManager.DEFAULT_RESERVE_BUFFERS;
    private long maxBufferSpace = FileStorageManager.DEFAULT_MAX_BUFFERSPACE;
	private FileStorageManager fsm;
	
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
                fsm = new FileStorageManager();
                fsm.setStorageDirectory(bufferDir.getCanonicalPath());
                fsm.setMaxFileSize(maxFileSize);
                fsm.setMaxOpenFiles(maxOpenFiles);
                fsm.setMaxBufferSpace(maxBufferSpace*MB);
                fsm.initialize();        
                this.bufferMgr.setStorageManager(fsm);
                
                // start the file storage manager in clean state
                // wise FileStorageManager is smart enough to clen up after itself
                cleanDirectory(bufferDir);
            } else {
            	this.bufferMgr.setStorageManager(new MemoryStorageManager());
            }
            
        } catch(TeiidComponentException e) { 
            throw new TeiidRuntimeException(e, RuntimePlugin.Util.getString("LocalBufferService.Failed_initializing_buffer_manager._8")); //$NON-NLS-1$
        } catch(IOException e) {
            throw new TeiidRuntimeException(e, RuntimePlugin.Util.getString("LocalBufferService.Failed_initializing_buffer_manager._8")); //$NON-NLS-1$            
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
	
	public void setUseDisk(boolean flag) {
		this.useDisk = flag;
	}

	public void setDiskDirectory(String dir) {
		this.bufferDir = new File(dir, "buffer"); //$NON-NLS-1$
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

    public void setMaxFileSize(long maxFileSize) {
    	this.maxFileSize = maxFileSize;
	}
    
    public void setMaxReserveBatchColumns(int value) {
		this.maxReserveBatchColumns = value;
	}    
    
    public void setMaxProcessingBatchesColumns(int value) {
    	this.maxProcessingBatchesColumns  = value;
    }

    @ManagementProperty(description="Max file size, in MB, for buffer files (default 2GB)")
	public long getMaxFileSize() {
		return maxFileSize;
	}
    
    @ManagementProperty(description="Max open buffer files (default 64)")
    public void setMaxOpenFiles(int maxOpenFiles) {
		this.maxOpenFiles = maxOpenFiles;
	}

    @ManagementProperty(description="The number of batch columns guarenteed to a processing operation.  Set this value lower if the workload typically" + 
    		"processes larger numbers of concurrent queries with large intermediate results from operations such as sorting, " + 
    		"grouping, etc. (default 128)")
	public int getMaxProcessingBatchesColumns() {
		return maxProcessingBatchesColumns;
	}

    @ManagementProperty(description="The number of batch columns to allow in memory (default 16384).  " + 
    		"This value should be set lower or higher depending on the available memory to Teiid in the VM.  " + 
    		"16384 is considered a good default for a dedicated 32-bit VM running Teiid with a 1 gig heap.")
	public int getMaxReserveBatchColumns() {
		return maxReserveBatchColumns;
	}
    
    @ManagementProperty(description="Max file storage space, in MB, to be used for buffer files (default 50G)")
	public long getMaxBufferSpace() {
		return maxBufferSpace;
	}
    
    public void setMaxBufferSpace(long maxBufferSpace) {
		this.maxBufferSpace = maxBufferSpace;
	}

    @ManagementProperty(description="The currently used file buffer space in MB.", readOnly=true)
    public long getUserBufferSpace() {
    	if (fsm != null) {
    		return fsm.getUsedBufferSpace()/MB;
    	}
    	return 0;
    }

    @ManagementProperty(description="The total number of batches added to the buffer mananger.", readOnly=true)
	public long getBatchesAdded() {
		return bufferMgr.getBatchesAdded();
	}

    @ManagementProperty(description="The total number of batches read from storage.", readOnly=true)
	public long getReadCount() {
		return bufferMgr.getReadCount();
	}

    @ManagementProperty(description="The total number of batches written to storage.", readOnly=true)
    public long getWriteCount() {
		return bufferMgr.getWriteCount();
	}

    @ManagementProperty(description="The total number of batch read attempts.", readOnly=true)
	public long getReadAttempts() {
		return bufferMgr.getReadAttempts();
	}
}
