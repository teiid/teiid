/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

    //general batch properties
    private int processorBatchSize = BufferManager.DEFAULT_PROCESSOR_BATCH_SIZE;
    private boolean inlineLobs = true;

    // storage layers - only used if useDisk is true
    private boolean useDisk = true;
    private int maxStorageObjectSize = BufferFrontedFileStoreCache.DEFAULT_MAX_OBJECT_SIZE;
    private BufferFrontedFileStoreCache fsc;
    private FileStorageManager fsm;

    //reserve / heap properties
    private int maxProcessingKb = BufferManager.DEFAULT_MAX_PROCESSING_KB;
    private int maxReservedHeapKb = BufferManager.DEFAULT_RESERVE_BUFFER_KB;

    //fixed memory properties
    private long fixedMemoryBufferSpaceMb = -1;
    private boolean fixedMemoryBufferOffHeap;

    //disk properties
    private File bufferDir;
    private boolean encryptFiles = false;
    private int maxOpenFiles = FileStorageManager.DEFAULT_MAX_OPEN_FILES;
    private long maxFileSize = SplittableStorageManager.DEFAULT_MAX_FILESIZE; // 2GB
    private long maxDiskBufferSpace = FileStorageManager.DEFAULT_MAX_BUFFERSPACE>>20;

    private long vmMaxMemory = Runtime.getRuntime().maxMemory();
    private SessionServiceImpl sessionService;

    /**
     * Clean the file storage directory on startup
     * @since 4.3
     */
    void cleanDirectory(File file) {
        FileUtils.removeChildrenRecursively(file);
    }

    public void start(){
        try {
            // Construct and initialize the buffer manager
            this.bufferMgr = new BufferManagerImpl(false);
            this.bufferMgr.setProcessorBatchSize(processorBatchSize);
            this.bufferMgr.setMaxReserveKB(this.maxReservedHeapKb);
            this.bufferMgr.setMaxProcessingKB(this.maxProcessingKb);
            this.bufferMgr.setInlineLobs(inlineLobs);
            this.bufferMgr.setSessionService(sessionService);
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
                fsm.setMaxBufferSpace(maxDiskBufferSpace*MB);
                SplittableStorageManager ssm = new SplittableStorageManager(fsm);
                ssm.setMaxFileSize(maxFileSize);
                StorageManager sm = ssm;
                if (encryptFiles) {
                    sm = new EncryptedStorageManager(ssm);
                }
                fsc = new BufferFrontedFileStoreCache();
                fsc.setBufferManager(this.bufferMgr);
                fsc.setMaxStorageObjectSize(maxStorageObjectSize);
                fsc.setDirect(fixedMemoryBufferOffHeap);
                if (fixedMemoryBufferSpaceMb < 0) {
                    //use approximately 40% of what's set aside for the reserved accounting for conversion from kb to bytes
                    long autoMaxBufferSpace = 4*(((long)this.bufferMgr.getMaxReserveKB())<<10)/10;
                    if (this.maxReservedHeapKb >= 0) {
                        //if the max reserve has been - it may be too high
                        //across most vm sizes we use about 1/4 of the remaining memory for the fixed buffer
                        autoMaxBufferSpace = Math.min(autoMaxBufferSpace, (vmMaxMemory - (((long)this.bufferMgr.getMaxReserveKB())<<10))/4);
                    }
                    fsc.setMemoryBufferSpace(autoMaxBufferSpace);
                } else {
                    //scale from MB to bytes
                    fsc.setMemoryBufferSpace(fixedMemoryBufferSpaceMb << 20);
                }
                //estimate inode/batch overhead
                long batchAndInodeOverheadKB = fsc.getMemoryBufferSpace()>>(fixedMemoryBufferOffHeap?19:17);
                this.bufferMgr.setMaxReserveKB((int)Math.max(0, this.bufferMgr.getMaxReserveKB() - batchAndInodeOverheadKB));
                if (this.maxReservedHeapKb < 0) {
                    if (fixedMemoryBufferOffHeap) {
                        //the default is too large if off heap
                        this.bufferMgr.setMaxReserveKB(8*this.bufferMgr.getMaxReserveKB()/10);
                    } else {
                        //adjust the value for the main memory buffer
                        this.bufferMgr.setMaxReserveKB((int)Math.max(0, this.bufferMgr.getMaxReserveKB() - (fsc.getMemoryBufferSpace()>>10)));
                    }
                }
                fsc.setStorageManager(sm);
                fsc.initialize();
                this.bufferMgr.setCache(fsc);
            } else {
                MemoryStorageManager msm = new MemoryStorageManager();
                SplittableStorageManager ssm = new SplittableStorageManager(msm);
                ssm.setMaxFileSizeDirect(MemoryStorageManager.MAX_FILE_SIZE);
                this.bufferMgr.setCache(msm);
                this.bufferMgr.setStorageManager(ssm);
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

    public void setMaxReservedHeapMb(int maxReservedHeap) {
        this.maxReservedHeapKb = (int)Math.min(Integer.MAX_VALUE, ((long)maxReservedHeap)<<10);
    }

    public int getMaxReservedHeapMb() {
        return this.maxReservedHeapKb >> 10;
    }

    public void setMaxProcessingKb(int maxProcessingKb) {
        this.maxProcessingKb = maxProcessingKb;
    }

    @Deprecated
    public void setMaxReserveKb(int maxReserveKb) {
        this.maxReservedHeapKb = maxReserveKb;
    }

    public long getMaxDiskBufferSpaceMb() {
        return maxDiskBufferSpace;
    }

    public void setMaxDiskBufferSpaceMb(long maxBufferSpace) {
        this.maxDiskBufferSpace = maxBufferSpace;
    }

    public void setFixedMemoryBufferOffHeap(boolean memoryBufferOffHeap) {
        this.fixedMemoryBufferOffHeap = memoryBufferOffHeap;
    }

    public void setFixedMemoryBufferSpaceMb(int memoryBufferSpace) {
        this.fixedMemoryBufferSpaceMb = memoryBufferSpace;
    }

    @Deprecated
    public void setMaxStorageObjectSize(int maxStorageObjectSize) {
        this.maxStorageObjectSize = maxStorageObjectSize;
    }

    public void setMaxStorageObjectSizeKb(int maxStorageObjectSize) {
        this.maxStorageObjectSize = (int)Math.min(Integer.MAX_VALUE, ((long)maxStorageObjectSize)<<10);
    }

    public long getUsedDiskBufferSpaceMb() {
        if (fsm != null) {
            return fsm.getUsedBufferSpace()/MB;
        }
        return 0;
    }

    public int getTotalOutOfDiskErrors() {
        if (fsm != null) {
            return fsm.getOutOfDiskErrorCount();
        }
        return 0;
    }

    public long getHeapBufferInUseKb() {
        return bufferMgr.getActiveBatchBytes()/1024;
    }

    public long getMemoryReservedByActivePlansKb() {
        return this.bufferMgr.getMaxReserveKB() - bufferMgr.getReserveBatchBytes()/1024;
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

    public long getMemoryBufferUsedKb() {
        if (fsc != null) {
            return fsc.getMemoryInUseBytes() >> 10;
        }
        return 0;
    }

    public long getStorageReadCount() {
        return bufferMgr.getReadCount();
    }

    public long getStorageWriteCount() {
        return bufferMgr.getWriteCount();
    }

    public long getReadAttempts() {
        return bufferMgr.getReadAttempts();
    }

    public int getFixedMemoryBufferSpaceMb() {
        return (int)fixedMemoryBufferSpaceMb;
    }

    public int getMaxStorageObjectSizeKb() {
        return maxStorageObjectSize >> 10;
    }

    public boolean isFixedMemoryBufferOffHeap() {
        return fixedMemoryBufferOffHeap;
    }

    public boolean isEncryptFiles() {
        return encryptFiles;
    }

    public void setEncryptFiles(boolean encryptFiles) {
        this.encryptFiles = encryptFiles;
    }

    public void setBufferManager(BufferManagerImpl bufferManager) {
        this.bufferMgr = bufferManager;
    }

    public void setVmMaxMemory(long vmMaxMemory) {
        this.vmMaxMemory = vmMaxMemory;
    }

    public void setSessionService(SessionServiceImpl sessionService) {
        this.sessionService = sessionService;
    }

}
