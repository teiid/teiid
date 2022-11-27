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

package org.teiid.common.buffer;

import org.teiid.common.buffer.impl.BufferFrontedFileStoreCache;
import org.teiid.common.buffer.impl.BufferManagerImpl;
import org.teiid.common.buffer.impl.MemoryStorageManager;
import org.teiid.common.buffer.impl.SplittableStorageManager;
import org.teiid.core.TeiidComponentException;


/**
 * <p>Factory for BufferManager instances.  One method will get
 * a server buffer manager, as it should be instantiated in a running
 * MetaMatrix server.  That BufferManager is configured mostly by the
 * passed in properties.
 *
 * <p>The other method returns a stand-alone, in-memory buffer manager.  This
 * is typically used for either in-memory testing or any time the
 * query processor component is not expected to run out of memory, such as
 * within the modeler.
 */
public class BufferManagerFactory {

    private static BufferManagerImpl INSTANCE;

    /**
     * Helper to get a buffer manager all set up for unmanaged standalone use.  This is
     * typically used for testing or when memory is not an issue.
     * @return BufferManager ready for use
     */
    public static BufferManagerImpl getStandaloneBufferManager() {
        if (INSTANCE == null) {
            BufferManagerImpl bufferMgr = createBufferManager();
            INSTANCE = bufferMgr;
        }

        return INSTANCE;
    }

    public static BufferManagerImpl createBufferManager() {
        return initBufferManager(new BufferManagerImpl());
    }

    public static BufferManagerImpl getTestBufferManager(long bytesAvailable, int procBatchSize) {
        BufferManagerImpl bufferManager = new BufferManagerImpl();
        bufferManager.setProcessorBatchSize(procBatchSize);
        bufferManager.setMaxProcessingKB((int) (bytesAvailable/1024));
        bufferManager.setMaxReserveKB((int) (bytesAvailable/1024));
        return initBufferManager(bufferManager);
    }

    public static BufferManagerImpl initBufferManager(BufferManagerImpl bufferManager) {
        try {
            bufferManager.initialize();
            bufferManager.setUseWeakReferences(false);
            MemoryStorageManager storageManager = new MemoryStorageManager();
            SplittableStorageManager ssm = new SplittableStorageManager(storageManager);
            ssm.setMaxFileSizeDirect(MemoryStorageManager.MAX_FILE_SIZE);
            BufferFrontedFileStoreCache fsc = new BufferFrontedFileStoreCache();
            fsc.setBufferManager(bufferManager);
            //use conservative allocations
            fsc.setDirect(false); //allow the space to be GCed easily
            fsc.setMaxStorageObjectSize(1<<20);
            fsc.setMemoryBufferSpace(1<<21);
            fsc.setStorageManager(ssm);
            fsc.initialize();
            bufferManager.setCache(fsc);
            return bufferManager;
        } catch (TeiidComponentException e) {
            throw new RuntimeException(e);
        }
    }

}
