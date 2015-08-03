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

import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.StorageManager;
import org.teiid.common.buffer.TupleBufferCache;
import org.teiid.common.buffer.impl.BufferFrontedFileStoreCache;
import org.teiid.common.buffer.impl.BufferManagerImpl;
import org.teiid.common.buffer.impl.EncryptedStorageManager;
import org.teiid.common.buffer.impl.FileStorageManager;
import org.teiid.common.buffer.impl.MemoryStorageManager;
import org.teiid.common.buffer.impl.SplittableStorageManager;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.FileUtils;
import org.teiid.dqp.service.BufferService;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.runtime.RuntimePlugin;


/**
 * Implements the BufferService.  This implementation
 * may use either an all-memory model (which is typically only for testing) or 
 * a mixed disk/memory model which requires use of a directory on the disk 
 * for file service access.
 */
public class BufferServiceImpl implements BufferService, Serializable {
	private static final long serialVersionUID = -6217808623863643531L;
	private static final long MB = 1<<20;

    // Instance
    private BufferManagerImpl bufferMgr;
	private File bufferDir;
	private boolean useDisk = true;
	private boolean encryptFiles = false;
	private int processorBatchSize = BufferManager.DEFAULT_PROCESSOR_BATCH_SIZE;
    private int maxOpenFiles = FileStorageManager.DEFAULT_MAX_OPEN_FILES;
    private long maxFileSize = SplittableStorageManager.DEFAULT_MAX_FILESIZE; // 2GB
    private int maxProcessingKb = BufferManager.DEFAULT_MAX_PROCESSING_KB;
    private int maxReserveKb = BufferManager.DEFAULT_RESERVE_BUFFER_KB;
    private long maxBufferSpace = FileStorageManager.DEFAULT_MAX_BUFFERSPACE>>20;
    private boolean inlineLobs = true;
    private long memoryBufferSpace = -1;
    private int maxStorageObjectSize = BufferFrontedFileStoreCache.DEFAuLT_MAX_OBJECT_SIZE;
    private boolean memoryBufferOffHeap;
	private FileStorageManager fsm;
	private BufferFrontedFileStoreCache fsc;
	private int workingMaxReserveKb;
	
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
            this.bufferMgr.setProcessorBatchSize(processorBatchSize);
            this.bufferMgr.setMaxReserveKB(this.maxReserveKb);
            this.bufferMgr.setMaxProcessingKB(this.maxProcessingKb);
            this.bufferMgr.setInlineLobs(inlineLobs);
            this.bufferMgr.initialize();
            
            // If necessary, add disk storage manager
            if(useDisk) {
        		LogManager.logDetail(LogConstants.CTX_DQP, "Starting BufferManager using", bufferDir); //$NON-NLS-1$
        		if (!bufferDir.exists()) {
        			this.bufferDir.mkdirs();
        		}
            	// start the file storage manager in clean state
                // wise FileStorageManager is smart enough to clean up after itself
                cleanDirectory(bufferDir);
                // Get the properties for FileStorageManager and create.
                fsm = new FileStorageManager();
                fsm.setStorageDirectory(bufferDir.getCanonicalPath());
                fsm.setMaxOpenFiles(maxOpenFiles);
                fsm.setMaxBufferSpace(maxBufferSpace*MB);
                SplittableStorageManager ssm = new SplittableStorageManager(fsm);
                ssm.setMaxFileSize(maxFileSize);
                StorageManager sm = ssm;
                if (encryptFiles) {
                	sm = new EncryptedStorageManager(ssm);
                }
                fsc = new BufferFrontedFileStoreCache();
                fsc.setBufferManager(this.bufferMgr);
                fsc.setMaxStorageObjectSize(maxStorageObjectSize);
                fsc.setDirect(memoryBufferOffHeap);
                int batchOverheadKB = (int)(this.memoryBufferSpace<0?(this.bufferMgr.getMaxReserveKB()<<8):this.memoryBufferSpace)>>20;
        		this.bufferMgr.setMaxReserveKB(Math.max(0, this.bufferMgr.getMaxReserveKB() - batchOverheadKB));
                if (memoryBufferSpace < 0) {
                	//use approximately 25% of what's set aside for the reserved
                	fsc.setMemoryBufferSpace(((long)this.bufferMgr.getMaxReserveKB()) << 8);
                } else {
                	//scale from MB to bytes
                	fsc.setMemoryBufferSpace(memoryBufferSpace << 20);
                }
                if (!memoryBufferOffHeap && this.maxReserveKb < 0) {
            		//adjust the value
            		this.bufferMgr.setMaxReserveKB(this.bufferMgr.getMaxReserveKB() - (int)Math.min(this.bufferMgr.getMaxReserveKB(), (fsc.getMemoryBufferSpace()>>10)));
                }
                fsc.setStorageManager(sm);
                fsc.initialize();
                this.bufferMgr.setCache(fsc);
                this.workingMaxReserveKb = this.bufferMgr.getMaxReserveKB();
            } else {
            	this.bufferMgr.setCache(new MemoryStorageManager());
            }
            
        } catch(TeiidComponentException e) { 
             throw new TeiidRuntimeException(RuntimePlugin.Event.TEIID40039, e, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40039));
        } catch(IOException e) {
             throw new TeiidRuntimeException(RuntimePlugin.Event.TEIID40039, e, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40039));
        }
    }
   
    public void stop() {
    	LogManager.logDetail(LogConstants.CTX_DQP, "Stopping BufferManager using", bufferDir); //$NON-NLS-1$
    	if (bufferMgr != null) {
    		bufferMgr.shutdown();
    		bufferMgr = null;
    	}

        // Delete the buffer directory
        if (bufferDir != null) {
	        cleanDirectory(bufferDir);
	        bufferDir.delete();
        }
    }

    public BufferManagerImpl getBufferManager() {
        return this.bufferMgr;
    }
    
    @Override
    public TupleBufferCache getTupleBufferCache() {
    	return this.bufferMgr;
    }
	
	public void setUseDisk(boolean flag) {
		this.useDisk = flag;
	}

	public void setDiskDirectory(String dir) {
		this.bufferDir = new File(dir, "buffer"); //$NON-NLS-1$
	}

	public void setProcessorBatchSize(int size) {
		this.processorBatchSize = size;
	}

	public void setInlineLobs(boolean inlineLobs) {
		this.inlineLobs = inlineLobs;
	}

	public File getBufferDirectory() {
		return bufferDir;
	}

	public boolean isUseDisk() {
		return this.useDisk;
	}
	
	public boolean isInlineLobs() {
		return inlineLobs;
	}

	public int getProcessorBatchSize() {
		return this.processorBatchSize;
	}

    public void setMaxFileSize(long maxFileSize) {
    	this.maxFileSize = maxFileSize;
	}
    
	public long getMaxFileSize() {
		return maxFileSize;
	}
    
    public void setMaxOpenFiles(int maxOpenFiles) {
		this.maxOpenFiles = maxOpenFiles;
	}

    public int getMaxProcessingKb() {
		return maxProcessingKb;
	}

    public int getMaxReservedKb() {
		return maxReserveKb;
	}
    
    public void setMaxProcessingKb(int maxProcessingKb) {
		this.maxProcessingKb = maxProcessingKb;
	}
    
    public void setMaxReserveKb(int maxReserveKb) {
		this.maxReserveKb = maxReserveKb;
	}
    
	public long getMaxBufferSpace() {
		return maxBufferSpace;
	}
    
    public void setMaxBufferSpace(long maxBufferSpace) {
		this.maxBufferSpace = maxBufferSpace;
	}
    
    public void setMemoryBufferOffHeap(boolean memoryBufferOffHeap) {
		this.memoryBufferOffHeap = memoryBufferOffHeap;
	}

    public void setMemoryBufferSpace(int memoryBufferSpace) {
		this.memoryBufferSpace = memoryBufferSpace;
	}

    public void setMaxStorageObjectSize(int maxStorageObjectSize) {
		this.maxStorageObjectSize = maxStorageObjectSize;
	}    

    public long getUsedDiskBufferSpaceMB() {
    	if (fsm != null) {
    		return fsm.getUsedBufferSpace()/MB;
    	}
    	return 0;
    }

	public long getHeapCacheMemoryInUseKB() {
		return bufferMgr.getActiveBatchBytes()/1024 + workingMaxReserveKb - bufferMgr.getMaxReserveKB();
	}

	public long getHeapMemoryInUseByActivePlansKB() {
		return workingMaxReserveKb - bufferMgr.getReserveBatchBytes()/1024;
	}
	
	public long getDiskReadCount() {
		if (fsc != null) {
			return fsc.getStorageReads();
		}
		return 0;
	}
	
    public long getDiskWriteCount() {
    	if (fsc != null) {
    		return fsc.getStorageWrites();
    	}
    	return 0;
    }
    
    public long getMemoryBufferUsedKB() {
    	if (fsc != null) {
    		return fsc.getMemoryInUseBytes() >> 10;
    	}
    	return 0;
    }
    
    public long getCacheReadCount() {
    	return bufferMgr.getReadCount();
    }
    
    public long getCacheWriteCount() {
    	return bufferMgr.getWriteCount();
    }    
	
	public long getReadAttempts() {
		return bufferMgr.getReadAttempts();
	}

    public int getMemoryBufferSpace() {
		return (int)memoryBufferSpace;
	}

    public int getMaxStorageObjectSize() {
		return maxStorageObjectSize;
	}

    public boolean isMemoryBufferOffHeap() {
		return memoryBufferOffHeap;
	}    
    
    public boolean isEncryptFiles() {
		return encryptFiles;
	}
    
    public void setEncryptFiles(boolean encryptFiles) {
		this.encryptFiles = encryptFiles;
	}
}
