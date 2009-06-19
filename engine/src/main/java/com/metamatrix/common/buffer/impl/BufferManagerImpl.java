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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.buffer.IndexedTupleSource;
import com.metamatrix.common.buffer.LobTupleBatch;
import com.metamatrix.common.buffer.MemoryNotAvailableException;
import com.metamatrix.common.buffer.StorageManager;
import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.common.buffer.TupleSourceID;
import com.metamatrix.common.buffer.TupleSourceNotFoundException;
import com.metamatrix.common.lob.LobChunk;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.types.Streamable;
import com.metamatrix.core.log.MessageLevel;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.dqp.util.LogConstants;
import com.metamatrix.query.execution.QueryExecPlugin;

/**
 * <p>Default implementation of BufferManager.  This buffer manager implementation
 * assumes the usage of a StorageManager of type memory and optionally (preferred)
 * an additional StorageManager of type FILE or DISK.  If no persistent manager
 * is specified, everything managed by this BufferManager is assumed to fit in
 * memory.  This can be useful for testing or for small uses.</p>
 *
 * <p>Lots of state is cached in memory.  The tupleSourceMap contains a map of
 * TupleSourceID --> TupleSourceInfo.  Everything about a particular tuple
 * source is stored there.  The memoryState contains everything pertaining to
 * memory management.  The config contains all config info.</p>
 */
public class BufferManagerImpl implements BufferManager {
    
    // Initialized stuff
    private String lookup;
    private BufferConfig config;

    // Cache tuple source info in memory 
    private Map<TupleSourceID, TupleSourceInfo> tupleSourceMap = new ConcurrentHashMap<TupleSourceID, TupleSourceInfo>();
    // groupName (String) -> TupleGroupInfo map
    private Map groupInfos = new HashMap();

    // Storage managers
    private StorageManager memoryMgr;
    private StorageManager diskMgr;

    // ID creator
    private AtomicLong currentTuple = new AtomicLong(0);

    // Memory management
    private MemoryState memoryState;

    // Trigger to handle management and stats logging
    private Timer timer;

    // Collected stats
    private AtomicInteger pinRequests = new AtomicInteger(0);
    private AtomicInteger pinFailures = new AtomicInteger(0);
    private AtomicInteger pinnedFromDisk = new AtomicInteger(0);
    private AtomicInteger cleanings = new AtomicInteger(0);
    private AtomicLong totalCleaned = new AtomicLong(0);

    /**
     * See {@link com.metamatrix.common.buffer.BufferManagerPropertyNames} for a
     * description of all the properties.
     * @param lookup An object telling the buffer manager what his location is and
     * how to find other buffer managers of different locations
     * @param properties Properties to configure the buffer manager
     */
    public synchronized void initialize(String lookup, Properties properties) throws MetaMatrixComponentException {
        this.lookup = lookup;

        // Set up config based on properties
        this.config = new BufferConfig(properties);

        // Set up memory state object
        this.memoryState = new MemoryState(config);

        // Set up alarms based on config
        if(this.config.getManagementInterval() > 0) {
            TimerTask mgmtTask = new TimerTask() {
                public void run() {
                    clean(0);
                }
            };
            getTimer().schedule(mgmtTask, 0, this.config.getManagementInterval());
        }
        if(this.config.getLogStatInterval() > 0) {
            TimerTask statTask = new TimerTask() {
                public void run() {
                    BufferStats stats = getStats();
                    stats.log();
                }
            };
            getTimer().schedule(statTask, 0, this.config.getLogStatInterval());
        }
    }

	private Timer getTimer() {
		if (timer == null) {
			timer = new Timer("BufferManagementThread", true); //$NON-NLS-1$
		}
		return timer;
	}

    /**
     * Get the configuration of the buffer manager
     * @return Configuration
     */
    public BufferConfig getConfig() {
        return this.config;
    }

    /**
     * Construct a BufferStats object by looking at all the state.  This should
     * be multi-thread safe but calling this method clears several counters and
     * resets that state.
     * @return Buffer statistics
     */
    public BufferStats getStats() {
        BufferStats stats = new BufferStats();
        
        // Get memory info
        this.memoryState.fillStats(stats);
        
        // Get picture of what's happening
        Set copyKeys = tupleSourceMap.keySet();
        Iterator infoIter = copyKeys.iterator();
        stats.numTupleSources = copyKeys.size();

        // Walk through each info and count where all the batches are -
        // lock only a single info at a time to minimize locking
        while(infoIter.hasNext()) {
            Object key = infoIter.next();
            TupleSourceInfo info = tupleSourceMap.get(key);
            if(info == null) {
                continue;
            }

            synchronized(info) {
                Iterator batchIter = info.getBatchIterator();
                while(batchIter.hasNext()) {
                    ManagedBatch batch = (ManagedBatch) batchIter.next();
                    switch(batch.getLocation()) {
                        case ManagedBatch.PERSISTENT:
                            stats.numPersistentBatches++;
                            break;
                        case ManagedBatch.PINNED:
                            stats.numPinnedBatches++;
                            
                            if ( LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.TRACE )) {
                                stats.pinnedManagedBatches.add(batch);
                            }

                            break;
                        case ManagedBatch.UNPINNED:
                            stats.numUnpinnedBatches++;
                            break;
                    }
                }
            }
        }

        stats.pinRequests = this.pinRequests.get();
        stats.pinSuccesses = this.pinRequests.get() - this.pinFailures.get();
        stats.pinnedFromMemory = this.pinRequests.get() - this.pinnedFromDisk.get();
		stats.numCleanings = this.cleanings.get();
        stats.totalCleaned = this.totalCleaned.get();

        return stats;
    }

    /**
     * Get processor batch size
     * @return Number of rows in a processor batch
     */
    public int getProcessorBatchSize() {        
        return config.getProcessorBatchSize();
    }

    /**
     * Get connector batch size
     * @return Number of rows in a connector batch
     */
    public int getConnectorBatchSize() {
        return config.getConnectorBatchSize();
    }

    /**
     * Add a storage manager to this buffer manager, order is unimportant
     * @param storageManager Storage manager to add
     */
    public void addStorageManager(StorageManager storageManager) {
    	Assertion.isNotNull(storageManager);
        if(storageManager.getStorageType() == StorageManager.TYPE_MEMORY) {
            this.memoryMgr = storageManager;
        } else {
        	Assertion.isNull(diskMgr);
            this.diskMgr = storageManager;
        }
    }

    /**
     * Register a new tuple source and return a unique ID for it.
     * @param schema List of ElementSymbol
     * @param groupName Group name
     * @param tupleSourceType Type of tuple source as defined in BufferManager constants
     * @return New unique ID for this tuple source
     * @throws MetaMatrixComponentException If internal server error occurs
     */
    public TupleSourceID createTupleSource(List schema, String[] types, String groupName, TupleSourceType tupleSourceType)
    throws MetaMatrixComponentException {

        TupleSourceID newID = new TupleSourceID(String.valueOf(this.currentTuple.getAndIncrement()), this.lookup);
		TupleSourceInfo info = new TupleSourceInfo(newID, schema, types, getGroupInfo(groupName), tupleSourceType);
		tupleSourceMap.put(newID, info);

        if (LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.DETAIL)) {
            LogManager.logDetail(LogConstants.CTX_BUFFER_MGR, new Object[]{"Creating TupleSource:", newID, "of type "+tupleSourceType}); //$NON-NLS-1$ //$NON-NLS-2$
        }

        return newID;
    }

    /**
     * Remove a tuple source based on ID
     * @throws TupleSourceNotFoundException If tuple source not found
     * @throws MetaMatrixComponentException If internal server error occurs
     */
    public void removeTupleSource(TupleSourceID tupleSourceID)
    throws TupleSourceNotFoundException, MetaMatrixComponentException {

        if(tupleSourceID == null || tupleSourceID.getLocation() == null) {
            // this is a bogus tuple source ID
            return;
        }
        if (LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.DETAIL)) {
            LogManager.logDetail(LogConstants.CTX_BUFFER_MGR, new Object[]{"Removing TupleSource:", tupleSourceID}); //$NON-NLS-1$
        }

        // Remove from main map first
        TupleSourceInfo info = this.tupleSourceMap.remove(tupleSourceID);
        if(info == null) {
        	throw new TupleSourceNotFoundException(QueryExecPlugin.Util.getString("BufferManagerImpl.tuple_source_not_found", tupleSourceID)); //$NON-NLS-1$
        }

        // Walk through batches and determine whether memory or disk cleanup needs to occur
        synchronized(info) {
            if(! info.isRemoved()) {
                info.setRemoved();

                Iterator iter = info.getBatchIterator();
                while(iter.hasNext()) {
                    ManagedBatch batch = (ManagedBatch) iter.next();
                    switch(batch.getLocation()) {
                        case ManagedBatch.UNPINNED:
                            memoryState.removeUnpinned(batch);
                            memoryState.releaseMemory(batch.getSize(), info.getGroupInfo());
                            break;
                        case ManagedBatch.PINNED:
                            memoryState.removePinned(info.getTupleSourceID(), batch.getBeginRow());
                            memoryState.releaseMemory(batch.getSize(), info.getGroupInfo());
                            break;
                    }
                }
            }
        }

        // Remove memory storage
        this.memoryMgr.removeBatches(tupleSourceID);

        // Remove disk storage
        if (this.diskMgr != null){
            this.diskMgr.removeBatches(tupleSourceID);
        }
        
        // remove any dependent tuple sources on this tuple source
        // lob frame work uses the parent tuple's name as group name to
        // tie it back to the original source.
        removeTupleSources(tupleSourceID.getStringID());
    }

    /**
     * Remove all the tuple sources with the specified group name.  Typically the group
     * name is really a session identifier and this is called to remove all tuple sources
     * used by a particular session.
     * @param groupName Name of the group
     * @throws MetaMatrixComponentException If internal server error occurs
     */
    public void removeTupleSources(String groupName)
    throws MetaMatrixComponentException {

        if (LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.DETAIL)) {
            LogManager.logDetail(LogConstants.CTX_BUFFER_MGR, new Object[]{"Removing TupleSources for group", groupName}); //$NON-NLS-1$
        }

        // Get tuple sources to remove
        List removeList = new ArrayList();
    	Iterator iter = this.tupleSourceMap.keySet().iterator();
    	while(iter.hasNext()) {
    		TupleSourceID tsID = (TupleSourceID) iter.next();
    		TupleSourceInfo info = this.tupleSourceMap.get(tsID);
            if(info != null && ! info.isRemoved()) {
                // group names can be null - have to do a comparison with nulls here
                String infoGroup = info.getGroupInfo().getGroupName();
                if(infoGroup == null) {
                    if(groupName == null) {
                        removeList.add(tsID);
                    }
                } else if(infoGroup.equals(groupName)) {
                    removeList.add(tsID);
                }
    		}
    	}
        
        synchronized(groupInfos) {
            groupInfos.remove(groupName);
        }

        // Remove them
        if(removeList.size() > 0) {
	        MetaMatrixComponentException ex = null;

	        Iterator removeIter = removeList.iterator();
	        while(removeIter.hasNext()) {
	     		TupleSourceID tsID = (TupleSourceID) removeIter.next();

	     		try {
	     			this.removeTupleSource(tsID);
	     		} catch(TupleSourceNotFoundException e) {
	     			// ignore and go on
	     		} catch(MetaMatrixComponentException e) {
	     			if(ex == null) {
	     				ex = e;
	     			}
	     		}
	        }

            if(ex != null) {
            	throw ex;
            }
        }
    }

    /**
     * Get a tuple source to walk through the rows for a particular
     * tupleSourceID.
     * @param tupleSourceID Tuple source identifier
     * @return TupleSource
     * @throws TupleSourceNotFoundException If tuple source not found
     * @throws MetaMatrixComponentException If an internal server error occurred
     */
    public IndexedTupleSource getTupleSource(TupleSourceID tupleSourceID)
    throws TupleSourceNotFoundException, MetaMatrixComponentException {

		TupleSourceInfo info = getTupleSourceInfo(tupleSourceID, true);
        int batchSize = this.config.getProcessorBatchSize();
        return new TupleSourceImpl(this, tupleSourceID, info.getTupleSchema(), batchSize);
    }

    /**
     * Get the tuple schema for a particular tuple source
     * @param tupleSourceID Tuple source identifier
     * @return List of elements describing tuple source
     * @throws TupleSourceNotFoundException If tuple source not found
     * @throws MetaMatrixComponentException If an internal server error occurred
     */
    public List getTupleSchema(TupleSourceID tupleSourceID)
    throws TupleSourceNotFoundException, MetaMatrixComponentException {

        TupleSourceInfo info = getTupleSourceInfo(tupleSourceID, true);
        return info.getTupleSchema();
    }

    /**
     * Set the status for a particular tuple source
     * @param tupleSourceID Tuple source identifier
     * @param status New status
     * @throws TupleSourceNotFoundException If tuple source not found
     * @throws MetaMatrixComponentException If an internal server error occurred
     */
    public void setStatus(TupleSourceID tupleSourceID, TupleSourceStatus status)
    throws TupleSourceNotFoundException, MetaMatrixComponentException {

        if (LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.DETAIL)) {
            LogManager.logDetail(LogConstants.CTX_BUFFER_MGR, new Object[]{"Setting status for", tupleSourceID, "to", status}); //$NON-NLS-1$ //$NON-NLS-2$
        }

        TupleSourceInfo info = getTupleSourceInfo(tupleSourceID, true);
  		synchronized(info) {
       		info.setStatus(status);
		}
    }

    /**
     * Get the status for a particular tuple source
     * @param tupleSourceID Tuple source identifier
     * @return Status
     * @throws TupleSourceNotFoundException If tuple source not found
     * @throws MetaMatrixComponentException If an internal server error occurred
     */
    public TupleSourceStatus getStatus(TupleSourceID tupleSourceID)
    throws TupleSourceNotFoundException, MetaMatrixComponentException {

        TupleSourceInfo info = getTupleSourceInfo(tupleSourceID, true);
    	return info.getStatus();
    }

    /**
     * Get the row count for a particular tuple source
     * @param tupleSourceID Tuple source identifier
     * @return Row count
     * @throws TupleSourceNotFoundException If tuple source not found
     * @throws MetaMatrixComponentException If an internal server error occurred
     */
    public int getRowCount(TupleSourceID tupleSourceID)
        throws TupleSourceNotFoundException, MetaMatrixComponentException {

        TupleSourceInfo info = getTupleSourceInfo(tupleSourceID, true);
    	return info.getRowCount();
    }

    /**
     * Add a batch to the given tuple source.  It is assumed that batches are
     * added in order, so the row count is reset to the end row of this batch.
     * @param tupleSourceID Tuple source identifier
     * @param tupleBatch New batch
     * @throws TupleSourceNotFoundException If tuple source not found
     * @throws MetaMatrixComponentException If an internal server error occurred
     */
    public void addTupleBatch(TupleSourceID tupleSourceID, TupleBatch tupleBatch)
    throws TupleSourceNotFoundException, MetaMatrixComponentException {

        if (LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.TRACE)) {
            LogManager.logTrace(LogConstants.CTX_BUFFER_MGR, new Object[]{"AddTupleBatch for", tupleSourceID, "with " + tupleBatch.getRowCount() + " rows"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        }
        
        if (tupleBatch.getRowCount() == 0 && !tupleBatch.getTerminationFlag()) {
        	return;
        }
        
        // Look up info
        TupleSourceInfo info = getTupleSourceInfo(tupleSourceID, true);
        
        // if there are lobs in source then we need to keep manage then
        // in a separate tuple sources.
        if (info.lobsInSource()) {
            createTupleSourcesForLobs(tupleSourceID, tupleBatch);
        }        

        // Determine where to store
        long bytes = SizeUtility.getBatchSize(info.getTypes(), tupleBatch.getAllTuples());
        tupleBatch.setSize(bytes);
        short location = ManagedBatch.PERSISTENT;
                
        if(memoryState.reserveMemory(bytes, info.getGroupInfo()) == MemoryState.MEMORY_AVAILABLE) {
            location = ManagedBatch.UNPINNED;
        }

        synchronized(info) {
            if(info.isRemoved()) {
                memoryState.releaseMemory(bytes, info.getGroupInfo());
                throw new TupleSourceNotFoundException(QueryExecPlugin.Util.getString("BufferManagerImpl.tuple_source_not_found", tupleSourceID)); //$NON-NLS-1$
            }

            // Store into storage manager
            try {
                if(location == ManagedBatch.PERSISTENT) {
                    this.diskMgr.addBatch(tupleSourceID, tupleBatch, info.getTypes());
                } else {
                    this.memoryMgr.addBatch(tupleSourceID, tupleBatch, info.getTypes());
                }
            } catch(MetaMatrixComponentException e) {
                // If we were storing to memory, clean up memory we reserved
                if(location != ManagedBatch.PERSISTENT) {
                    memoryState.releaseMemory(bytes, info.getGroupInfo());
                }
                throw e;
            }

            // Update tuple source state
            ManagedBatch managedBatch = new ManagedBatch(tupleSourceID, tupleBatch.getBeginRow(), tupleBatch.getEndRow(), bytes);
            managedBatch.setLocation(location);

            // Add to memory state if in memory
            if(location == ManagedBatch.UNPINNED) {
                this.memoryState.addUnpinned(managedBatch);
            }

            // Update info with new rows
            info.addBatch(managedBatch);
            info.setRowCount(tupleBatch.getEndRow());
        }
    }

    /**
     * Pin a tuple source in memory and return it.  This batch must be unpinned by
     * passed the identical tuple source ID and beginning row.
     * @param tupleSourceID Tuple source identifier
     * @param beginRow Beginning row
     * @param maxEndRow Max end row to return
     * @throws TupleSourceNotFoundException If tuple source not found
     * @throws MetaMatrixComponentException If an internal server error occurred
     * @throws MemoryNotAvailableException If memory was not available for the pin
     */
    public TupleBatch pinTupleBatch(TupleSourceID tupleSourceID, int beginRow, int maxEndRow)
        throws TupleSourceNotFoundException, MemoryNotAvailableException, MetaMatrixComponentException {

        if (LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.TRACE)) {
            LogManager.logTrace(LogConstants.CTX_BUFFER_MGR, new Object[]{"Pinning tupleBatch for", tupleSourceID, "beginRow:", beginRow, "maxEndRow:", maxEndRow}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        }

        this.pinRequests.incrementAndGet();

        TupleBatch memoryBatch = null;
        int endRow = 0;
        int count = 0;
        int pass = 0;

        TupleSourceInfo info = getTupleSourceInfo(tupleSourceID, true);
        long memoryRequiredByBatch = 0;
        int memoryAvailability = MemoryState.MEMORY_AVAILABLE;
        while(pass < 2) {
            if(pass == 1) {
                if(memoryAvailability == MemoryState.MEMORY_EXCEED_MAX) {
                    clean(memoryRequiredByBatch);
                }else {
                    //exceed session limit
                    clean(memoryRequiredByBatch, info.getGroupInfo()); 
                }
            }
            
            synchronized(info) {
                ManagedBatch mbatch = info.getBatch(beginRow);
                if(mbatch == null) {
                    return new TupleBatch(beginRow, Collections.EMPTY_LIST);
    
                } else if(mbatch.getLocation() == ManagedBatch.PINNED) {
                    // Load batch from memory - already pinned
                    memoryBatch = this.memoryMgr.getBatch(tupleSourceID, beginRow, info.getTypes());
    
                } else if(mbatch.getLocation() == ManagedBatch.UNPINNED) {
                    // Already in memory - just move from unpinned to pinned
                    mbatch.setLocation(ManagedBatch.PINNED);
                    this.memoryState.removeUnpinned(mbatch);
                    this.memoryState.addPinned(mbatch);
    
                    // Load batch from memory
                    memoryBatch = this.memoryMgr.getBatch(tupleSourceID, beginRow, info.getTypes());
                                                
                } else if(mbatch.getLocation() == ManagedBatch.PERSISTENT) {
                    memoryRequiredByBatch = mbatch.getSize();
                    
                    // Try to reserve some memory
                    if((memoryAvailability = memoryState.reserveMemory(memoryRequiredByBatch, info.getGroupInfo())) != MemoryState.MEMORY_AVAILABLE) {
                        if(pass == 0) {
                            // Break and try to clean - it is important to break out of the synchronized block
                            // here so that the clean does not cause a deadlock on this TupleSourceInfo
                            pass++;
                            continue;
                        } 

                        // Failed to reserve the memory even after a clean, so record the failure and fail the pin
                        this.pinFailures.incrementAndGet();

                        // Couldn't reserve memory, so throw exception
                        throw new MemoryNotAvailableException(QueryExecPlugin.Util.getString("BufferManagerImpl.no_memory_available")); //$NON-NLS-1$
                    }

                    this.pinnedFromDisk.incrementAndGet();

                    // Memory was reserved, so move from persistent to memory and pin
                    int internalBeginRow = mbatch.getBeginRow();
                    memoryBatch = diskMgr.getBatch(tupleSourceID, internalBeginRow, info.getTypes());
    
                    try {
                        memoryMgr.addBatch(tupleSourceID, memoryBatch, info.getTypes());
                    } catch(MetaMatrixComponentException e) {
                        memoryState.releaseMemory(mbatch.getSize(), info.getGroupInfo());
                        throw e;
                    }
    
                    try {
                        diskMgr.removeBatch(tupleSourceID, internalBeginRow);
                    } catch(TupleSourceNotFoundException e) {
                    } catch(MetaMatrixComponentException e) {
                        memoryState.releaseMemory(memoryRequiredByBatch, info.getGroupInfo());
                        try {
                            memoryMgr.removeBatch(tupleSourceID, internalBeginRow);
                        } catch(Exception e2) {
                            // ignore
                        }
                        throw e;
                    }
    
                    mbatch.setLocation(ManagedBatch.PINNED);
                    this.memoryState.addPinned(mbatch);
                }
                
                //if the client request previous batch, the end row 
                //is smaller than the begin row
                if(beginRow > maxEndRow) {
                    endRow = Math.min(beginRow, memoryBatch.getEndRow());
                    beginRow = Math.max(maxEndRow, memoryBatch.getBeginRow());
                }else {
                    endRow = Math.min(maxEndRow, memoryBatch.getEndRow());
                }
                count = endRow - beginRow + 1;
                if(count > 0) {
                    mbatch.pin();
                }
            }
            
            break;
        }

        // Batch should now be pinned in memory, so grab it and build a correctly
        // sized batch to return
        if(memoryBatch.getRowCount() == 0 || count == 0 || (beginRow == memoryBatch.getBeginRow() && count == memoryBatch.getRowCount())) {
            return memoryBatch;
        }

        int firstOffset = beginRow - memoryBatch.getBeginRow();
        List[] memoryRows = memoryBatch.getAllTuples();
        List[] rows = new List[count];
        System.arraycopy(memoryRows, firstOffset, rows, 0, count);
        return new TupleBatch(beginRow, rows);
    }

    /**
     * Unpin a tuple source batch.
     * @param tupleSourceID Tuple source identifier
     * @param beginRow Beginning row
     * @throws TupleSourceNotFoundException If tuple source not found
     * @throws MetaMatrixComponentException If an internal server error occurred
     */
    public void unpinTupleBatch(TupleSourceID tupleSourceID, int beginRow, int endRow)
        throws TupleSourceNotFoundException, MetaMatrixComponentException {

        if (LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.TRACE)) {
            LogManager.logTrace(LogConstants.CTX_BUFFER_MGR, new Object[]{"Unpinning tupleBatch for", tupleSourceID, "beginRow: " + beginRow}); //$NON-NLS-1$ //$NON-NLS-2$
        }

        TupleSourceInfo info = getTupleSourceInfo(tupleSourceID, true);
        synchronized(info) {
            ManagedBatch mbatch = info.getBatch(beginRow);
            if(mbatch == null || mbatch.getLocation() != ManagedBatch.PINNED) {
                return;
            }
            
            mbatch.unpin();

            // Determine whether batch itself should be unpinned
            if(! mbatch.hasPinnedRows()) {
                mbatch.setLocation(ManagedBatch.UNPINNED);
                memoryState.removePinned(tupleSourceID, mbatch.getBeginRow());
                memoryState.addUnpinned(mbatch);
            }
        }
    }

	/**
	 * Gets the final row count if tuple source is FULL, otherwise returns -1.
	 * @param tupleSourceID Tuple source identifier
	 * @return Final row count if status == FULL, -1 otherwise
     * @throws TupleSourceNotFoundException If tuple source not found
     * @throws MetaMatrixComponentException If an internal server error occurred
	 */
	public int getFinalRowCount(TupleSourceID tupleSourceID)
    	throws TupleSourceNotFoundException, MetaMatrixComponentException {

        TupleSourceInfo info = getTupleSourceInfo(tupleSourceID, true);
        synchronized(info) {
    		if(info.getStatus() == TupleSourceStatus.FULL) {
    			return info.getRowCount();
    		}
    		return -1;
        }
    }

    /**
     * Get the tuple source info for the specified tuple source ID
     * @param tupleSourceID Tuple source to get
     * @param throwException Whether to throw exception if the TupleSourceInfo is not found
     * @return Tuple source info
     * @throws TupleSourceNotFoundException If not found
     */
    TupleSourceInfo getTupleSourceInfo(TupleSourceID tupleSourceID, boolean throwException)
    throws TupleSourceNotFoundException {

        // Look up info
        TupleSourceInfo info = this.tupleSourceMap.get(tupleSourceID);

        if(info == null && throwException) {
        	throw new TupleSourceNotFoundException(QueryExecPlugin.Util.getString("BufferManagerImpl.tuple_source_not_found", tupleSourceID)); //$NON-NLS-1$
        }
        return info;
    }

    /**
     * Clean the memory state, using LRU.  This can be done either via the background
     * cleaning thread or actively if someone wants memory and none is free.
     */
    protected void clean(long memoryRequired) {
        // Defect 14573 - this method needs to know how much memory is required, so that (even if we're not past the active memory
        // threshold) if the memory available is less than the memory required, we should clean up unpinned batches.
        long targetLevel = config.getActiveMemoryLevel();
        long totalMemory = config.getTotalAvailableMemory();
        long released = 0;

        Iterator unpinnedIter = this.memoryState.getAllUnpinned();
        while(unpinnedIter.hasNext() && // If there are unpinned batches in memory, AND
              // Defect 14573 - if we require more than what's available, then cleanup regardless of the threshold
              (memoryRequired > totalMemory - memoryState.getMemoryUsed() || // if the memory needed is more than what's available, or
               memoryState.getMemoryUsed() > targetLevel)){ // if we've crossed the active memory threshold, then cleanup
            
            ManagedBatch batch = (ManagedBatch) unpinnedIter.next();
            TupleSourceID tsID = batch.getTupleSourceID();

            released += releaseMemory(batch, tsID);
        }

        if(released > 0) {
            this.cleanings.incrementAndGet();
            this.totalCleaned.addAndGet(released);
        }
    
    }
    
    /**
     * Over memory limit for this session. Clean the memory for this session.
     * Clean the memory state, using LRU.  This can be done actively if someone wants memory and none is free.
     */
    protected void clean(long memoryRequired, TupleGroupInfo targetGroupInfo) throws TupleSourceNotFoundException{
        boolean cleanForSessionSucceeded = false;
        long released = 0;

        Iterator unpinnedIter = this.memoryState.getAllUnpinned();
        while(unpinnedIter.hasNext()) {
            ManagedBatch batch = (ManagedBatch) unpinnedIter.next();
            TupleSourceID tsID = batch.getTupleSourceID();
            TupleSourceInfo tsInfo = getTupleSourceInfo(tsID, false);
            if(tsInfo == null) {
                //may be removed by another thread
                continue;
            }
            if(!tsInfo.getGroupInfo().equals(targetGroupInfo)) {
                //continue if they are not the same tuple group
                continue;
            }
            
            long groupMemoryUsed = memoryState.getGroupMemoryUsed(targetGroupInfo);
            //if the memory needed is more than what is available for the session, then cleanup. Otherwise, break the loop.
            if(memoryRequired <= config.getMaxAvailableSession() - groupMemoryUsed) {
                cleanForSessionSucceeded = true;
                break;
            }                
            
            released += releaseMemory(batch, tsID);
        }

        if(released > 0) {
            this.cleanings.incrementAndGet();
            this.totalCleaned.addAndGet(released);
        }
        
        if(!cleanForSessionSucceeded) {
            //if we cannot clean enough memory for this session, it fails
            return;
        }
        
        //make sure it is not over the buffer manager memory limit
        clean(memoryRequired);
    }
    
    /**
     * Release the memory for the given unpinned batch.
     * @param batch Batch to be released from memory
     * @param tsID ID of the tuple source to be released from memory 
     * @return The size of memory released in bytes
     * @since 4.3
     */
    private long releaseMemory(ManagedBatch batch, TupleSourceID tsID) {
        // Find info and lock on it
        try {
            TupleSourceInfo info = getTupleSourceInfo(tsID, false);
            if(info == null) {
                return 0;
            }

            synchronized(info) {
                if(info.isRemoved()) {
                    return 0;
                }

                // Re-get the batch and check that it still exists and is unpinned
                batch = info.getBatch(batch.getBeginRow());
                if(batch == null || batch.getLocation() != ManagedBatch.UNPINNED) {
                    return 0;
                }

                // This batch is still unpinned - move to persistent storage
                int beginRow = batch.getBeginRow();
                TupleBatch dataBatch = null;
                try {
                    dataBatch = memoryMgr.getBatch(tsID, beginRow, info.getTypes());
                } catch(TupleSourceNotFoundException e) {
                    return 0;
                } catch(MetaMatrixComponentException e) {
                    return 0;
                }

                try {
                    diskMgr.addBatch(tsID, dataBatch, info.getTypes());
                } catch(MetaMatrixComponentException e) {
                    // Can't move
                    return 0;
                }

                try {
                    memoryMgr.removeBatch(tsID, beginRow);
                } catch(TupleSourceNotFoundException e) {
                    // ignore
                } catch(MetaMatrixComponentException e) {
                    // ignore
                }

                // Update memory
                batch.setLocation(ManagedBatch.PERSISTENT);
                memoryState.removeUnpinned(batch);
                memoryState.releaseMemory(batch.getSize(), info.getGroupInfo());

                return batch.getSize();
            }
        } catch(TupleSourceNotFoundException e) {
            // ignore, go to next batch
            return 0;
        }    
    }
    
    /**
     * Gets a TupleGroupInfo representing the group 
     * @param groupName Logical grouping name for tuple sources. Can be null.
     * @return TupleGroupInfo for the group. Never null.
     * @since 4.3
     */
    private TupleGroupInfo getGroupInfo(String groupName) {
        TupleGroupInfo info = null;
        synchronized(groupInfos) {
            info = (TupleGroupInfo)groupInfos.get(groupName);
            if (info == null) {
                // If it doesn't exist, create a new one
                info = new TupleGroupInfo(groupName);
                groupInfos.put(groupName, info);
            }
        }
        return info;
    }

    /** 
     * @see com.metamatrix.common.buffer.BufferManager#stop()
     */
    public synchronized void stop() {
    	if (timer != null) {
    		timer.cancel();
    	}
    }
    
    /**
     * If a tuple batch is being added with Lobs, then maintain the LOB
     * objects in a separate TupleSource than from the original, so that
     * the original can only serilize the id, but the otherone can serialize 
     * the contents. 
     * @param batch
     */
    private void createTupleSourcesForLobs(TupleSourceID parentId, TupleBatch batch) 
        throws MetaMatrixComponentException, TupleSourceNotFoundException {

        TupleSourceInfo info = getTupleSourceInfo(parentId, false);
        List parentSchema = info.getTupleSchema();        
        List[] rows = batch.getAllTuples();
        
        // walk through the results and find all the lobs
        for (int row = 0; row < rows.length; row++) {
            
            int col = 0;
            for (Iterator i = rows[row].iterator(); i.hasNext();) {                                                
                Object anObj = i.next();
                
                if (anObj instanceof Streamable) {                                
                    // once lob is found check to see if this has already been assigned
                    // a streming id or not; if one is not assigned create one and assign it
                    // to the lob; if one is already assigned just return; 
                    // this will prohibit calling lob on itself into this routine.
                    Streamable lob = (Streamable)anObj;                  
                    
                    if (lob.getReferenceStreamId() == null || lobIsNotKnownInTupleSourceMap( lob, parentId) ) {                        
                        List schema = new ArrayList();
                        schema.add(parentSchema.get(col));
                        
                        TupleSourceID id = createTupleSource(schema, new String[] {info.getTypes()[col]}, parentId.getStringID(), TupleSourceType.PROCESSOR);
                        lob.setReferenceStreamId(id.getStringID());
                        
                        List results = new ArrayList();
                        results.add(lob);
                        
                        List listOfRows = new ArrayList();
                        listOfRows.add(results);
                        
                        // these batches are wrapped in a special marker batch tag
                        // which are saved from forcing them to disk.
                        LobTupleBatch separateBatch = new LobTupleBatch(1, listOfRows);
                        separateBatch.setTerminationFlag(true);
                        
                        // now save this as separate tuple source.
                        addTupleBatch(id, separateBatch);
                    } else {                        
                        // this means the XML object being moved from one tuple to another tuple
                        // i.e. one plan to another plan. So update the group info.

                        // First update the reference tuple source.
                        if (!lob.getReferenceStreamId().equals(parentId.getStringID())) {
                            TupleSourceID id = new TupleSourceID(lob.getReferenceStreamId());
                            TupleSourceInfo lobInfo = getTupleSourceInfo(id, false);
                            lobInfo.setGroupInfo(getGroupInfo(parentId.getStringID()));
                            
                            // if the lob moving parent has a assosiated persistent
                            // tuple source, then move that one to same parent too.
                            if (lob.getPersistenceStreamId() != null) {
                                id = new TupleSourceID(lob.getPersistenceStreamId());
                                lobInfo = getTupleSourceInfo(id, false);
                                lobInfo.setGroupInfo(getGroupInfo(parentId.getStringID()));                                
                            }                            
                        }
                    }
                }
                col++;
            }
        }
    }
    
    private boolean lobIsNotKnownInTupleSourceMap( Streamable lob, TupleSourceID parentId) throws TupleSourceNotFoundException {
        /*
         * The need for this defensive feature arises because there are multiple uses of the TupleSourceMap which
         * are somewhat inconsistent with one another.  In the case of LOBs we use the parent/child group feature
         * of tuplesources to associate a parent tuplesource containing metadata about the LOB with a second
         * tuplesource that contains the LOB.  When such a group is no longer needed  (for example, see SubqueryProcessorUtility.close()),
         * removing the child tupleSources has the unfortunate side effect of leaving the actual LOBs with references to
         * tuplesources that no longer exist, and are therefore no longer in the tupleSourceMap.
         * 
         * This test ensures that such orphaned LOBs will be treated correctly (TEIID-54).
         * 
         */
        if (!lob.getReferenceStreamId().equals(parentId.getStringID())) {
            TupleSourceID id = new TupleSourceID(lob.getReferenceStreamId());
            TupleSourceInfo lobInfo = getTupleSourceInfo(id, false);

            if ( lobInfo == null ) {
                return true; // is not known
            }
            return false;   // is known
        }        
        return false;   // don't care if known
    }
    

    /**  
     * @see com.metamatrix.common.buffer.BufferManager#addStreamablePart(com.metamatrix.common.buffer.TupleSourceID, com.metamatrix.common.lob.LobChunk, int)
     */
    public void addStreamablePart(TupleSourceID tupleSourceID, LobChunk streamChunk, int beginRow) 
        throws TupleSourceNotFoundException, MetaMatrixComponentException {
        
        // Look up info
        TupleSourceInfo info = getTupleSourceInfo(tupleSourceID, true);
        short location = ManagedBatch.PERSISTENT;
        
        synchronized(info) {

            List data = new ArrayList();
            data.add(streamChunk);
            TupleBatch batch = new TupleBatch(beginRow, new List[] {data});
            this.diskMgr.addBatch(tupleSourceID, batch, info.getTypes());                
                        
            // Update tuple source state (we could calculate the size of stream if need to)
            ManagedBatch managedBatch = new ManagedBatch(tupleSourceID, beginRow, batch.getEndRow(), 0);
            managedBatch.setLocation(location);

            // Update info with new rows
            info.addBatch(managedBatch);
            info.setRowCount(batch.getEndRow());
        }                
    }

    /** 
     * @see com.metamatrix.common.buffer.BufferManager#getStreamable(com.metamatrix.common.buffer.TupleSourceID)
     */
    public LobChunk getStreamablePart(final TupleSourceID tupleSourceID, int beginRow) 
        throws TupleSourceNotFoundException, MetaMatrixComponentException {
        
        final TupleSourceInfo info = getTupleSourceInfo(tupleSourceID, true);
        TupleBatch batch = diskMgr.getBatch(tupleSourceID, beginRow, info.getTypes());
        LobChunk chunk = (LobChunk)batch.getAllTuples()[0].get(0);
        
        return chunk;        
    }

    /** 
     * @see com.metamatrix.common.buffer.BufferManager#releasePinnedBatches()
     */
    public void releasePinnedBatches() throws MetaMatrixComponentException {
        Map threadPinned = memoryState.getPinnedByCurrentThread();
        if (threadPinned == null) {
            return;
        }
        for (Iterator i = threadPinned.entrySet().iterator(); i.hasNext();) {
            Map.Entry entry = (Map.Entry)i.next();
            i.remove();
            TupleSourceID tsid = (TupleSourceID)entry.getKey();
            Map pinnedBatches = (Map)entry.getValue();
            try {
                for (Iterator j = pinnedBatches.values().iterator(); j.hasNext();) {
                    ManagedBatch batch = (ManagedBatch)j.next();
                    
                    //TODO: add trace logging about the batch that is being unpinned
                    unpinTupleBatch(tsid, batch.getBeginRow(), batch.getEndRow());
                }
            } catch (TupleSourceNotFoundException err) {
                continue;
            }
        }
    }
    
    /**
     * for testing purposes 
     */
    public int getPinnedCount() {
        Map pinned = memoryState.getAllPinned();
        
        int count = 0;
        
        if (pinned == null) {
            return count;
        }
        
        for (Iterator i = pinned.values().iterator(); i.hasNext();) {
            count += ((Map)i.next()).size();
        }
        
        return count;
    }
}
