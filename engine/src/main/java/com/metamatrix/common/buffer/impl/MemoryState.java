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

package com.metamatrix.common.buffer.impl;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.metamatrix.common.buffer.TupleSourceID;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.core.log.MessageLevel;
import com.metamatrix.dqp.util.LogConstants;

/**
 * <p>This class represents the memory state of the BufferManagerImpl.  The 
 * critical thing to know is what batches are in memory but not being 
 * used and what batches in memory but being used.  The access patterns 
 * for these two types of information are very different, so they are stored
 * in very different data structures.</p>
 * 
 * <p>The unpinned batches are stored in a linked list, ordered by a least
 * recently used timestamp.  This is optimized for cleanup.  It's not very good 
 * for finding stuff but fortunately, we don't need to most of the time.</p>
 * 
 * <p>The pinned batches are stored in a Map, keyed by TupleSourceID.  The value 
 * is another map, keyed by beginRow, with a ManagedBatch as the value.  This 
 * is really good for finding batches quickly, which is exactly what we need 
 * to do with pinned batches.</p>
 * 
 * <p>All methods on this class are synchronized to preserve state.</p>
 */
class MemoryState {
    
    private static ThreadLocal PINNED_BY_THREAD = new ThreadLocal();
    
    //memory availability when reserveMemory() is called
    static final int MEMORY_AVAILABLE = 1;
    static final int MEMORY_EXCEED_MAX = 2; //exceed buffer manager max memory
    static final int MEMORY_EXCEED_SESSION_MAX = 3; //exceed session max memory

    // Configuration, used to get available memory info
    private BufferConfig config;

    // Keep track of how memory we are using
    private volatile long memoryUsed = 0;

    // Track the currently pinned stuff by TupleSourceID for easy lookup
    private Map pinned = new HashMap();     
    
    // Track the currently unpinned stuff in a sorted list, sorted by access time
    private static final Comparator BATCH_COMPARATOR = new BatchComparator();
    private List unpinned = new ArrayList();
    
    /**
     * Constructor for MemoryState, based on config.
     * @param config Configuration
     */
    public MemoryState(BufferConfig config) {
        this.config = config;
    }

    /**
     * Fill the stats object with stats about memory
     * @param stats Stats info to be filled
     */
    public void fillStats(BufferStats stats) {
        stats.memoryUsed = this.memoryUsed;
        stats.memoryFree = config.getTotalAvailableMemory() - memoryUsed;
    }

    /**
     * Get the amount of memory currently being used in bytes
     * @return Used memory, in bytes
     */
    public long getMemoryUsed() {
        return this.memoryUsed;    
    }

    /**
     * Check for whether the specified amount of memory can be reserved,
     * and if so reserve it.  This is done in the same method so that the
     * memory is not taken away by a different thread between checking and 
     * reserving - standard "test and set" behavior. 
     * @param bytes Bytes requested
     * @return One of MEMORY_AVAILABLE, MEMORY_EXCEED_MAX, or MEMORY_EXCEED_SESSION_MAX
     */
    public synchronized int reserveMemory(long bytes, TupleGroupInfo groupInfo) {
        //check session limit first
        long sessionMax = config.getMaxAvailableSession();
        if(sessionMax - groupInfo.getGroupMemoryUsed() < bytes) {
            return MEMORY_EXCEED_SESSION_MAX;
        }
        
        //then check the total memory limit
        long max = config.getTotalAvailableMemory();
        if(max - memoryUsed < bytes) {
            return MEMORY_EXCEED_MAX;
        }
        
        /* NOTE1
         * Since the groupInfo call is being made in the synchronized block for the entire buffer,
         * groupInfo doesn't need additional locking.
         */
        groupInfo.reserveMemory(bytes);
        memoryUsed += bytes;
        
        return MEMORY_AVAILABLE;
    }

    /**
     * Release memory 
     * @param bytes Bytes to release
     */
    public synchronized void releaseMemory(long bytes, TupleGroupInfo groupInfo) {
        // see NOTE1
        groupInfo.releaseMemory(bytes);
        memoryUsed -= bytes;
    }
    
    /**
     * Get the amount of memory currently being used for the specified group in bytes 
     * @param groupInfo TupleGroupInfo
     * @return Used memory, in bytes
     */
    public synchronized long getGroupMemoryUsed(TupleGroupInfo groupInfo) {
        // see NOTE1
        return groupInfo.getGroupMemoryUsed();
    }
    
    /**
     * Add a pinned batch
     * @param batch Pinned batch to add
     */
    public void addPinned(ManagedBatch batch) {
        synchronized (this) {
            addPinnedInternal(pinned, batch);
        }
        Map theadPinned = (Map)PINNED_BY_THREAD.get();
        if (theadPinned == null) {
            theadPinned = new HashMap();
            PINNED_BY_THREAD.set(theadPinned);
        }
        addPinnedInternal(theadPinned, batch);
        if (LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.TRACE )) {
            batch.captureCallStack();
        }
    }

    private void addPinnedInternal(Map pinnedMap, ManagedBatch batch) {
        TupleSourceID tsID = batch.getTupleSourceID();
        Map tsPinned = (Map) pinnedMap.get(tsID);
        if(tsPinned == null) {
            tsPinned = new HashMap();
            pinnedMap.put(tsID, tsPinned);
        } 
        
        // Add batch, indexed by beginRow
        tsPinned.put(new Integer(batch.getBeginRow()), batch);
    }
    
    /**
     * Remove a pinned batch, if not found do nothing and return null
     * @param tsID Tuple source id
     * @param beginRow Beginning row
     * @return Removed batch or null if not found
     */
    public ManagedBatch removePinned(TupleSourceID tsID, int beginRow) {
        ManagedBatch result = null;
        synchronized (this) {
            result = removePinnedInternal(pinned, tsID, beginRow);
        }
        if (result != null) {
            Map theadPinned = (Map)PINNED_BY_THREAD.get();
            if (theadPinned != null) {
                removePinnedInternal(theadPinned, tsID, beginRow);
            }
        }
        return result;
    }

    private ManagedBatch removePinnedInternal(Map pinnedMap, TupleSourceID tsID,
                                              int beginRow) {
        Map tsPinned = (Map) pinnedMap.get(tsID);
        if(tsPinned != null) { 
            ManagedBatch mbatch = (ManagedBatch) tsPinned.remove(new Integer(beginRow));
            
            if(tsPinned.size() == 0) { 
                pinnedMap.remove(tsID);
            }
            
            return mbatch; 
        }
        return null;
    }

    /**
     * Add an unpinned batch
     * @param batch Unpinned batch to add
     */        
    public synchronized void addUnpinned(ManagedBatch batch) {
        batch.updateLastAccessed();
  
        if(unpinned.isEmpty()) {
            unpinned.add(batch);
            return;
        }
        int size = unpinned.size() - 1;
        for(int i=size; i>=0; i--) {
            ManagedBatch listBatch = (ManagedBatch)unpinned.get(i); 
            if(BATCH_COMPARATOR.compare(batch, listBatch) >= 0) {
                unpinned.add(i + 1, batch);
                break;
            } 
        }
    }
    
    /**
     * Remove an unpinned batch
     * @param batch Batch to remove
     */
    public synchronized void removeUnpinned(ManagedBatch batch) {
        unpinned.remove(batch);
    }

    /**
     * Get an iterator on all unpinned batches, typically for clean up 
     * purposes.  This iterator is "safe" in that it is based on a copy
     * of the real list and will not be invalidated by changes to the original
     * list.  However, this means that it also may contain batches that 
     * are no longer in the unpinned list, so the user of this iterator 
     * should check that each batch is still unpinned.
     * @return Safe (but possibly out of date) iterator on unpinned batches
     */
    public synchronized Iterator getAllUnpinned() {
        List copy = new ArrayList(unpinned);
        return copy.iterator();    
    }
    
    public synchronized Map getAllPinned() {
        return new HashMap(pinned);    
    }
        
    public Map getPinnedByCurrentThread() {
        return (Map)PINNED_BY_THREAD.get();
    }

}
