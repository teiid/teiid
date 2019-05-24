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

package org.teiid.common.buffer.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.teiid.common.buffer.FileStore;
import org.teiid.common.buffer.StorageManager;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.query.QueryPlugin;

/**
 * Represents a FileStore that holds blocks of a fixed size.
 */
class BlockStore {
    final long blockSize;
    final ConcurrentBitSet blocksInUse;
    final FileStore[] stores;
    final ReentrantReadWriteLock[] locks;

    public BlockStore(StorageManager storageManager, int blockSize, int blockCountLog, int concurrencyLevel) {
        this.blockSize = blockSize;
        int blockCount = 1 << blockCountLog;
        this.blocksInUse = new ConcurrentBitSet(blockCount, concurrencyLevel);
        this.blocksInUse.setCompact(true);
        this.stores = new FileStore[concurrencyLevel];
        this.locks = new ReentrantReadWriteLock[concurrencyLevel];
        for (int i = 0; i < stores.length; i++) {
            this.stores[i] = storageManager.createFileStore(String.valueOf(blockSize) + '_' + i);
            this.locks[i] = new ReentrantReadWriteLock();
        }

    }

    int getAndSetNextClearBit(PhysicalInfo info) {
        int result = blocksInUse.getAndSetNextClearBit();
        if (result == -1) {
             throw new TeiidRuntimeException(QueryPlugin.Event.TEIID30059, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30059, blockSize));
        }
        if (LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.DETAIL)) {
            LogManager.logDetail(LogConstants.CTX_BUFFER_MGR, "Allocating storage data block", result, "of size", blockSize, "to", info); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        }
        return result;
    }

    int writeToStorageBlock(PhysicalInfo info,
            InputStream is) throws IOException {
        int block = getAndSetNextClearBit(info);
        int segment = block/blocksInUse.getBitsPerSegment();
        boolean success = false;
        this.locks[segment].writeLock().lock();
        try {
            FileStore fs = stores[segment];
            long blockOffset = (block%blocksInUse.getBitsPerSegment())*blockSize;
            //TODO: there is still an extra buffer being created here, we could FileChannels to do better
            byte[] b = new byte[BufferFrontedFileStoreCache.BLOCK_SIZE];
            int read = 0;
            long newLength = blockOffset+blockSize;
            if (fs.getLength() < newLength) {
                //grow by whole blocks
                //TODO: could pad the growth
                fs.setLength(newLength);
            }
            while ((read = is.read(b, 0, b.length)) != -1) {
                fs.write(blockOffset, b, 0, read);
                blockOffset+=read;
            }
            success = true;
        } finally {
            locks[segment].writeLock().unlock();
            if (!success) {
                blocksInUse.clear(block);
                block = BufferFrontedFileStoreCache.EMPTY_ADDRESS;
            }
        }
        return block;
    }

}