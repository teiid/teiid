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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ListIterator;
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
import com.metamatrix.common.buffer.MemoryNotAvailableException;
import com.metamatrix.common.buffer.StorageManager;
import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.common.buffer.TupleSourceID;
import com.metamatrix.common.buffer.TupleSourceNotFoundException;
import com.metamatrix.common.lob.LobChunk;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.common.types.Streamable;
import com.metamatrix.core.log.MessageLevel;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.dqp.DQPPlugin;
import com.metamatrix.dqp.util.LogConstants;
import com.metamatrix.query.execution.QueryExecPlugin;
import com.metamatrix.query.sql.symbol.Expression;

/**
 * <p>Default implementation of BufferManager.  This buffer manager implementation
 * assumes the usage of a StorageManager of type memory and optionally (preferred)
 * an additional StorageManager of type FILE.</p>
 */
public class BufferManagerImpl implements BufferManager {

	//memory availability when reserveMemory() is called
	static final int MEMORY_AVAILABLE = 1;
	static final int MEMORY_EXCEED_MAX = 2; //exceed buffer manager max memory
	static final int MEMORY_EXCEED_SESSION_MAX = 3; //exceed session max memory

    private static ThreadLocal<Set<ManagedBatch>> PINNED_BY_THREAD = new ThreadLocal<Set<ManagedBatch>>() {
    	protected Set<ManagedBatch> initialValue() {
    		return new HashSet<ManagedBatch>();
    	};
    };

    // Initialized stuff
    private String lookup;
    private BufferConfig config;

    // Cache tuple source info in memory 
    private Map<TupleSourceID, TupleSourceInfo> tupleSourceMap = new ConcurrentHashMap<TupleSourceID, TupleSourceInfo>();
    // groupName (String) -> TupleGroupInfo map
    private Map<String, TupleGroupInfo> groupInfos = new HashMap<String, TupleGroupInfo>();

    // Storage manager
    private StorageManager diskMgr;

    // ID creator
    private AtomicLong currentTuple = new AtomicLong(0);

    // Keep track of how memory usage
    private volatile long memoryUsed = 0;
    
    // Track the currently unpinned stuff in a sorted set
    private Set<ManagedBatch> unpinned = Collections.synchronizedSet(new LinkedHashSet<ManagedBatch>());
    
    // Trigger to handle management and stats logging
    private Timer timer;

    // Collected stats
    private AtomicInteger pinRequests = new AtomicInteger(0);
    private AtomicInteger pinFailures = new AtomicInteger(0);
    private AtomicInteger pinnedFromDisk = new AtomicInteger(0);
    private AtomicInteger cleanings = new AtomicInteger(0);
    private AtomicLong totalCleaned = new AtomicLong(0);
    private AtomicInteger pinned = new AtomicInteger();

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

        // Set up alarms based on config
        if(this.config.getManagementInterval() > 0) {
            TimerTask mgmtTask = new TimerTask() {
                public void run() {
                    clean(0, null);
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
        stats.memoryUsed = this.memoryUsed;
        stats.memoryFree = config.getTotalAvailableMemory() - memoryUsed;
        
        // Get picture of what's happening
        Set<TupleSourceID> copyKeys = tupleSourceMap.keySet();
        Iterator<TupleSourceID> infoIter = copyKeys.iterator();
        stats.numTupleSources = copyKeys.size();

        // Walk through each info and count where all the batches are -
        // lock only a single info at a time to minimize locking
        while(infoIter.hasNext()) {
            TupleSourceID key = infoIter.next();
            TupleSourceInfo info = tupleSourceMap.get(key);
            if(info == null) {
                continue;
            }

            synchronized(info) {
                Iterator<ManagedBatch> batchIter = info.getBatchIterator();
                while(batchIter.hasNext()) {
                    ManagedBatch batch = batchIter.next();
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
    public void setStorageManager(StorageManager storageManager) {
    	Assertion.isNotNull(storageManager);
    	Assertion.isNull(diskMgr);
        this.diskMgr = storageManager;
    }

    /**
     * Register a new tuple source and return a unique ID for it.
     * @param groupName Group name
     * @param tupleSourceType Type of tuple source as defined in BufferManager constants
     * @param schema List of ElementSymbol
     * @return New unique ID for this tuple source
     * @throws MetaMatrixComponentException If internal server error occurs
     */
    public TupleSourceID createTupleSource(List schema, String groupName, TupleSourceType tupleSourceType)
    throws MetaMatrixComponentException {

        TupleSourceID newID = new TupleSourceID(String.valueOf(this.currentTuple.getAndIncrement()), this.lookup);
        TupleGroupInfo tupleGroupInfo = getGroupInfo(groupName);
		TupleSourceInfo info = new TupleSourceInfo(newID, schema, getTypeNames(schema), tupleGroupInfo, tupleSourceType);
        tupleGroupInfo.getTupleSourceIDs().add(newID);
		tupleSourceMap.put(newID, info);
        if (LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.DETAIL)) {
            LogManager.logDetail(LogConstants.CTX_BUFFER_MGR, new Object[]{"Creating TupleSource:", newID, "of type "+tupleSourceType}); //$NON-NLS-1$ //$NON-NLS-2$
        }

        return newID;
    }
    
    /**
     * Gets the data type names for each of the input expressions, in order.
     * @param expressions List of Expressions
     * @return
     * @since 4.2
     */
    public static String[] getTypeNames(List expressions) {
        String[] types = new String[expressions.size()];
        for (ListIterator i = expressions.listIterator(); i.hasNext();) {
            Expression expr = (Expression)i.next();
            types[i.previousIndex()] = DataTypeManager.getDataTypeName(expr.getType());
        }
        return types;
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

                Iterator<ManagedBatch> iter = info.getBatchIterator();
                while(iter.hasNext()) {
                    ManagedBatch batch = iter.next();
                    switch(batch.getLocation()) {
                        case ManagedBatch.UNPINNED:
                            this.unpinned.remove(batch);
                            releaseMemory(batch.getSize(), info.getGroupInfo());
                            break;
                        case ManagedBatch.PINNED:
                        	PINNED_BY_THREAD.get().remove(batch);
                        	this.pinned.getAndDecrement();
                            releaseMemory(batch.getSize(), info.getGroupInfo());
                            break;
                    }
                }
            }
        }
        TupleGroupInfo tupleGroupInfo = info.getGroupInfo();
		tupleGroupInfo.getTupleSourceIDs().remove(tupleSourceID);
        
        // Remove disk storage
        if (this.diskMgr != null){
            this.diskMgr.removeBatches(tupleSourceID);
        }
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

        TupleGroupInfo tupleGroupInfo = null;
        synchronized(groupInfos) {
        	tupleGroupInfo = groupInfos.remove(groupName);
        }
        if (tupleGroupInfo == null) {
        	return;
        }
        List<TupleSourceID> tupleSourceIDs = null;
        synchronized (tupleGroupInfo.getTupleSourceIDs()) {
			tupleSourceIDs = new ArrayList<TupleSourceID>(tupleGroupInfo.getTupleSourceIDs());
		}
        MetaMatrixComponentException ex = null;

        for (TupleSourceID tsID : tupleSourceIDs) {
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
        
        if (info.lobsInSource()) {
            correctLobReferences(info, tupleBatch);
        }        

        // Determine where to store
        long bytes = SizeUtility.getBatchSize(info.getTypes(), tupleBatch.getAllTuples());
        tupleBatch.setSize(bytes);
        short location = ManagedBatch.PERSISTENT;
                
        if(reserveMemory(bytes, info.getGroupInfo()) == BufferManagerImpl.MEMORY_AVAILABLE) {
            location = ManagedBatch.UNPINNED;
        }

        synchronized(info) {
            if(info.isRemoved()) {
            	if(location != ManagedBatch.PERSISTENT) {
            		releaseMemory(bytes, info.getGroupInfo());
            	}
                throw new TupleSourceNotFoundException(QueryExecPlugin.Util.getString("BufferManagerImpl.tuple_source_not_found", tupleSourceID)); //$NON-NLS-1$
            }

            // Update tuple source state
            ManagedBatch managedBatch = new ManagedBatch(tupleSourceID, tupleBatch.getBeginRow(), tupleBatch.getEndRow(), bytes);
            managedBatch.setLocation(location);

            // Store into storage manager
            try {
                if(location == ManagedBatch.PERSISTENT) {
                    this.diskMgr.addBatch(tupleSourceID, tupleBatch, info.getTypes());
                } else {
                    managedBatch.setBatch(tupleBatch);
                }
            } catch(MetaMatrixComponentException e) {
                // If we were storing to memory, clean up memory we reserved
                if(location != ManagedBatch.PERSISTENT) {
                    releaseMemory(bytes, info.getGroupInfo());
                }
                throw e;
            }
            
            // Add to memory state if in memory
            if(location == ManagedBatch.UNPINNED) {
            	this.unpinned.add(managedBatch);
            }

            // Update info with new rows
            info.addBatch(managedBatch);
            info.setRowCount(tupleBatch.getEndRow());
        }
    }
    
    /**
     * Pin a tuple batch in memory and return it.  This batch must be unpinned by
     * passed the identical tuple source ID and beginning row.
     * NOTE: the returned {@link TupleBatch} will have the exact same bounds as the {@link ManagedBatch}
     * @param tupleSourceID Tuple source identifier
     * @param beginRow Beginning row
     * @throws TupleSourceNotFoundException If tuple source not found
     * @throws MetaMatrixComponentException If an internal server error occurred
     * @throws MemoryNotAvailableException If memory was not available for the pin
     */
    public TupleBatch pinTupleBatch(TupleSourceID tupleSourceID, int beginRow)
        throws TupleSourceNotFoundException, MemoryNotAvailableException, MetaMatrixComponentException {

        if (LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.TRACE)) {
            LogManager.logTrace(LogConstants.CTX_BUFFER_MGR, new Object[]{"Pinning tupleBatch for", tupleSourceID, "beginRow:", beginRow}); //$NON-NLS-1$ //$NON-NLS-2$
        }

        this.pinRequests.incrementAndGet();

        TupleSourceInfo info = getTupleSourceInfo(tupleSourceID, true);
        int memoryAvailability = BufferManagerImpl.MEMORY_AVAILABLE;

        ManagedBatch mbatch = null;
        synchronized (info) {
        	mbatch = info.getBatch(beginRow);
        }
        if (mbatch == null || beginRow == mbatch.getEndRow() + 1) {
        	return new TupleBatch(beginRow, Collections.EMPTY_LIST);
        }
        long memoryRequiredByBatch = mbatch.getSize();

        TupleBatch memoryBatch = null;

        for (int pass = 0; pass < 2; pass++) {
        	if (memoryAvailability == BufferManagerImpl.MEMORY_EXCEED_MAX) {
        		clean(memoryRequiredByBatch, null);
        	} else if (memoryAvailability == BufferManagerImpl.MEMORY_EXCEED_SESSION_MAX) {
        		clean(memoryRequiredByBatch, info.getGroupInfo());
        	}
            
            synchronized(info) {
                if(mbatch.getLocation() == ManagedBatch.PINNED) {
                    // Load batch from memory - already pinned
                    memoryBatch = mbatch.getBatch();
                    break;
                }
                if(mbatch.getLocation() == ManagedBatch.UNPINNED) {
                    // Already in memory - just move from unpinned to pinned
                    this.unpinned.remove(mbatch);
                    pin(mbatch);
    
                    // Load batch from memory
                    memoryBatch = mbatch.getBatch();
                    break;                            
                } 
                memoryRequiredByBatch = mbatch.getSize();
                
                // Try to reserve some memory
                if((memoryAvailability = reserveMemory(memoryRequiredByBatch, info.getGroupInfo())) != BufferManagerImpl.MEMORY_AVAILABLE) {
                	continue;
                }

                this.pinnedFromDisk.incrementAndGet();

                // Memory was reserved, so move from persistent to memory and pin
                int internalBeginRow = mbatch.getBeginRow();
                memoryBatch = diskMgr.getBatch(tupleSourceID, internalBeginRow, info.getTypes());

                mbatch.setBatch(memoryBatch);
                if (info.lobsInSource()) {
                	correctLobReferences(info, memoryBatch);
                }
                pin(mbatch);
            }
        }
        
        if (memoryBatch == null) {
            // Failed to reserve the memory even after a clean, so record the failure and fail the pin
            this.pinFailures.incrementAndGet();
            throw new MemoryNotAvailableException(QueryExecPlugin.Util.getString("BufferManagerImpl.no_memory_available")); //$NON-NLS-1$
        }

        return memoryBatch;
    }
    
	private void pin(ManagedBatch mbatch) {
		mbatch.setLocation(ManagedBatch.PINNED);
		PINNED_BY_THREAD.get().add(mbatch);
		if (!mbatch.hasPinnedRows()) {
			pinned.getAndIncrement();
		}
		mbatch.pin();
	}

    /**
     * Unpin a tuple source batch.
     * @param tupleSourceID Tuple source identifier
     * @param beginRow Beginning row
     * @throws TupleSourceNotFoundException If tuple source not found
     * @throws MetaMatrixComponentException If an internal server error occurred
     */
    public void unpinTupleBatch(TupleSourceID tupleSourceID, int beginRow)
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
                PINNED_BY_THREAD.get().remove(mbatch);
                this.unpinned.add(mbatch);
                pinned.getAndDecrement();
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
     * This can be done actively if someone wants memory and none is free.
     */
    protected void clean(long memoryRequired, TupleGroupInfo targetGroupInfo) {
    	cleanLobTupleSource();
    	
    	long released = 0;

        long targetLevel = config.getActiveMemoryLevel();
        long totalMemory = config.getTotalAvailableMemory();
        
        boolean generalCleaningDone = false;
        List<ManagedBatch> toClean = null;
        synchronized (unpinned) {
        	//TODO: re-implement without having to scan and compete
			toClean = new ArrayList<ManagedBatch>(unpinned);
		}
        for (ManagedBatch batch : toClean) {
            TupleSourceID tsID = batch.getTupleSourceID();
            TupleSourceInfo tsInfo = this.tupleSourceMap.get(tsID);
            if(tsInfo == null) {
                //may be removed by another thread
                continue;
            }

            long currentMemoryUsed = memoryUsed;
            if (!generalCleaningDone && (memoryRequired <= totalMemory - currentMemoryUsed && // if the memory needed is more than what's available, or
                currentMemoryUsed <= targetLevel)) { // if we've crossed the active memory threshold, then cleanup
            	generalCleaningDone = true;
            }
            if (generalCleaningDone) {
            	if (targetGroupInfo == null) {
            		break;
            	}
                if (targetGroupInfo == tsInfo.getGroupInfo()) {
    	            //if the memory needed is more than what is available for the session, then cleanup. Otherwise, break the loop.
    	            if(memoryRequired <= config.getMaxAvailableSession() - targetGroupInfo.getGroupMemoryUsed()) {
    	            	break;
    	            }                
                } 
            }
            
            released += releaseMemory(batch, tsInfo);
        }

        if(released > 0) {
            this.cleanings.incrementAndGet();
            this.totalCleaned.addAndGet(released);
        }
    }

    //TODO: run asynch
	private void cleanLobTupleSource() {
		String tupleSourceId = TupleSourceInfo.getStaleLobTupleSource();
		if (tupleSourceId != null) {
        	try {
				removeTupleSource(new TupleSourceID(tupleSourceId));
			} catch (TupleSourceNotFoundException e) {
			} catch (MetaMatrixComponentException e) {
				LogManager.logDetail(LogConstants.CTX_BUFFER_MGR, e, "Exception removing stale lob tuple source"); //$NON-NLS-1$
			}
        }
	}
        
    /**
     * Release the memory for the given unpinned batch.
     * @param batch Batch to be released from memory
     * @param tsID ID of the tuple source to be released from memory 
     * @return The size of memory released in bytes
     * @since 4.3
     */
    private long releaseMemory(ManagedBatch batch, TupleSourceInfo info) {
        // Find info and lock on it
        synchronized(info) {
            if(info.isRemoved() || batch.getLocation() != ManagedBatch.UNPINNED) {
                return 0;
            }

            // This batch is still unpinned - move to persistent storage
            TupleBatch dataBatch = batch.getBatch();

            try {
                diskMgr.addBatch(info.getTupleSourceID(), dataBatch, info.getTypes());
            } catch(MetaMatrixComponentException e) {
                // Can't move
                return 0;
            }

            batch.setBatch(null);

            // Update memory
            batch.setLocation(ManagedBatch.PERSISTENT);
            this.unpinned.remove(batch);
            releaseMemory(batch.getSize(), info.getGroupInfo());

            return batch.getSize();
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
            info = groupInfos.get(groupName);
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
     * If a tuple batch is being added with Lobs, then references to
     * the lobs will be held on the {@link TupleSourceInfo} 
     * @param batch
     */
    @SuppressWarnings("unchecked")
	private void correctLobReferences(TupleSourceInfo info, TupleBatch batch) {
        List parentSchema = info.getTupleSchema();        
        List[] rows = batch.getAllTuples();
        int columns = parentSchema.size();
        // walk through the results and find all the lobs
        for (int row = 0; row < rows.length; row++) {
            for (int col = 0; col < columns; col++) {                                                
                Object anObj = rows[row].get(col);
                
                if (!(anObj instanceof Streamable<?>)) {
                	continue;
                }
                Streamable lob = (Streamable)anObj;                  
                info.addLobReference(lob);
                if (lob.getReference() == null) {
                	lob.setReference(info.getLobReference(lob.getReferenceStreamId()).getReference());
                }
            }
        }
    }
    
    @Override
    public Streamable<?> getStreamable(TupleSourceID id, String referenceId) throws TupleSourceNotFoundException, MetaMatrixComponentException {
    	TupleSourceInfo tsInfo = getTupleSourceInfo(id, true);
    	Streamable<?> s = tsInfo.getLobReference(referenceId);
    	if (s == null) {
    		throw new MetaMatrixComponentException(DQPPlugin.Util.getString("ProcessWorker.wrongdata")); //$NON-NLS-1$
    	}
    	return s;
    }
    
    @Override
    public void setPersistentTupleSource(TupleSourceID id, Streamable<?> s) throws TupleSourceNotFoundException {
    	cleanLobTupleSource();
    	TupleSourceInfo tsInfo = getTupleSourceInfo(id, true);
    	s.setPersistenceStreamId(id.getStringID());
    	tsInfo.setContainingLobReference(s);
    }

    /**  
     * @see com.metamatrix.common.buffer.BufferManager#addStreamablePart(com.metamatrix.common.buffer.TupleSourceID, com.metamatrix.common.lob.LobChunk, int)
     */
    public void addStreamablePart(TupleSourceID tupleSourceID, LobChunk streamChunk, int beginRow) 
        throws TupleSourceNotFoundException, MetaMatrixComponentException {
        
        // Look up info
        TupleSourceInfo info = getTupleSourceInfo(tupleSourceID, true);
        
        synchronized(info) {
            List<LobChunk> data = new ArrayList<LobChunk>();
            data.add(streamChunk);
            TupleBatch batch = new TupleBatch(beginRow, new List[] {data});
            this.diskMgr.addBatch(tupleSourceID, batch, info.getTypes());                
                        
            // Update tuple source state (we could calculate the size of stream if need to)
            ManagedBatch managedBatch = new ManagedBatch(tupleSourceID, beginRow, batch.getEndRow(), 0);
            managedBatch.setLocation(ManagedBatch.PERSISTENT);

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
    	MetaMatrixComponentException e = null;
    	List<ManagedBatch> pinnedByThread = new ArrayList<ManagedBatch>(PINNED_BY_THREAD.get());
    	for (ManagedBatch managedBatch : pinnedByThread) {
    		try {
    			//TODO: add trace logging about the batch that is being unpinned
    			unpinTupleBatch(managedBatch.getTupleSourceID(), managedBatch.getBeginRow());
    		} catch (TupleSourceNotFoundException err) {
    		} catch (MetaMatrixComponentException err) {
    			e = err;
    		}
		}
    	if (e != null) {
    		throw e;
    	}
    }
    
    /**
     * Check for whether the specified amount of memory can be reserved,
     * and if so reserve it.  This is done in the same method so that the
     * memory is not taken away by a different thread between checking and 
     * reserving - standard "test and set" behavior. 
     * @param bytes Bytes requested
     * @return One of MEMORY_AVAILABLE, MEMORY_EXCEED_MAX, or MEMORY_EXCEED_SESSION_MAX
     */
    private synchronized int reserveMemory(long bytes, TupleGroupInfo groupInfo) {
        //check session limit first
        long sessionMax = config.getMaxAvailableSession();
        if(sessionMax - groupInfo.getGroupMemoryUsed() < bytes) {
            return BufferManagerImpl.MEMORY_EXCEED_SESSION_MAX;
        }
        
        //then check the total memory limit
        long max = config.getTotalAvailableMemory();
        if(max - memoryUsed < bytes) {
            return BufferManagerImpl.MEMORY_EXCEED_MAX;
        }
        
        groupInfo.reserveMemory(bytes);
        memoryUsed += bytes;
        
        return BufferManagerImpl.MEMORY_AVAILABLE;
    }

    /**
     * Release memory 
     * @param bytes Bytes to release
     */
    private synchronized void releaseMemory(long bytes, TupleGroupInfo groupInfo) {
        groupInfo.releaseMemory(bytes);
        memoryUsed -= bytes;
    }
    
    /**
     * for testing purposes 
     */
    public int getPinnedCount() {
    	return pinned.get();
    }
}
