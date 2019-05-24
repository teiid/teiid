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

import org.teiid.common.buffer.BaseCacheEntry;
import org.teiid.common.buffer.CacheKey;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.query.QueryPlugin;

/**
 * Represents the memory buffer and storage state of an object.
 * It is important to minimize the amount of data held here.
 * Currently should be 60 bytes.
 */
final class PhysicalInfo extends BaseCacheEntry {

    static final Exception sizeChanged = new Exception();

    final Long gid;
    //the memory inode and block count
    int inode = BufferFrontedFileStoreCache.EMPTY_ADDRESS;
    int memoryBlockCount;
    //the storage block and BlockStore index
    int block = BufferFrontedFileStoreCache.EMPTY_ADDRESS;
    byte sizeIndex = 0;
    //state flags
    boolean pinned; //indicates that the entry is being read
    boolean evicting; //indicates that the entry will be moved out of the memory buffer
    boolean loading; //used by tier 1 cache to prevent double loads
    boolean adding; //used to prevent double adds

    int sizeEstimate;

    PhysicalInfo(Long gid, Long id, int inode, long lastAccess, int sizeEstimate) {
        super(new CacheKey(id, lastAccess, 0));
        this.inode = inode;
        this.gid = gid;
        this.sizeEstimate = sizeEstimate;
    }

    void setSize(int size) throws Exception {
        int newMemoryBlockCount = (size>>BufferFrontedFileStoreCache.LOG_BLOCK_SIZE) + ((size&BufferFrontedFileStoreCache.BLOCK_MASK)>0?1:0);
        if (this.memoryBlockCount != 0) {
            if (newMemoryBlockCount != memoryBlockCount) {
                throw sizeChanged;
            }
            return; //no changes
        }
        this.memoryBlockCount = newMemoryBlockCount;
        while (newMemoryBlockCount > 1) {
            this.sizeIndex++;
            newMemoryBlockCount = (newMemoryBlockCount>>1) + ((newMemoryBlockCount&0x01)==0?0:1);
        }
    }

    void await(boolean donePinning, boolean doneEvicting) {
        while ((donePinning && pinned) || (doneEvicting && evicting)) {
            try {
                wait();
            } catch (InterruptedException e) {
                 throw new TeiidRuntimeException(QueryPlugin.Event.TEIID30043, e);
            }
        }
    }

    synchronized void lockForLoad() {
        while (loading) {
            try {
                wait();
            } catch (InterruptedException e) {
                 throw new TeiidRuntimeException(QueryPlugin.Event.TEIID30044, e);
            }
        }
        loading = true;
    }

    synchronized void unlockForLoad() {
        assert loading;
        loading = false;
        notifyAll();
    }

}
