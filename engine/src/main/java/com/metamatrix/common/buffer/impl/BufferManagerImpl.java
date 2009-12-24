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
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.buffer.StorageManager;
import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.common.buffer.TupleBuffer;
import com.metamatrix.common.buffer.TupleSourceID;
import com.metamatrix.common.buffer.TupleSourceNotFoundException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.core.log.MessageLevel;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.dqp.util.LogConstants;
import com.metamatrix.query.execution.QueryExecPlugin;
import com.metamatrix.query.sql.symbol.Expression;

/**
 * <p>Default implementation of BufferManager.</p>
 * Responsible for creating/tracking TupleBuffers and providing access to the StorageManager
 */
public class BufferManagerImpl implements BufferManager, StorageManager {

    private String lookup;
    
	// Configuration 
    private int connectorBatchSize = BufferManager.DEFAULT_CONNECTOR_BATCH_SIZE;
    private int processorBatchSize = BufferManager.DEFAULT_PROCESSOR_BATCH_SIZE;
    private int maxProcessingBatches = BufferManager.DEFAULT_MAX_PROCESSING_BATCHES;
    
    private Map<TupleSourceID, TupleBuffer> tupleSourceMap = new ConcurrentHashMap<TupleSourceID, TupleBuffer>();
    private Map<String, Set<TupleSourceID>> groupInfos = new HashMap<String, Set<TupleSourceID>>();

    private StorageManager diskMgr;

    private AtomicLong currentTuple = new AtomicLong(0);
    
    public int getMaxProcessingBatches() {
		return maxProcessingBatches;
	}
    
    public void setMaxProcessingBatches(int maxProcessingBatches) {
		this.maxProcessingBatches = maxProcessingBatches;
	}

    /**
     * Get processor batch size
     * @return Number of rows in a processor batch
     */
    public int getProcessorBatchSize() {        
        return this.processorBatchSize;
    }

    /**
     * Get connector batch size
     * @return Number of rows in a connector batch
     */
    public int getConnectorBatchSize() {
        return this.connectorBatchSize;
    }
    
    public void setConnectorBatchSize(int connectorBatchSize) {
        this.connectorBatchSize = connectorBatchSize;
    } 

    public void setProcessorBatchSize(int processorBatchSize) {
        this.processorBatchSize = processorBatchSize;
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
    
    public StorageManager getStorageManager() {
		return diskMgr;
	}
    
    @Override
    public TupleBuffer createTupleBuffer(List elements, String groupName,
    		TupleSourceType tupleSourceType)
    		throws MetaMatrixComponentException {
        TupleSourceID newID = new TupleSourceID(String.valueOf(this.currentTuple.getAndIncrement()), this.lookup);
        Set<TupleSourceID> tupleGroupInfo = getGroupInfo(groupName, true);
        TupleBuffer tupleBuffer = new TupleBuffer(this, groupName, newID, elements, getTypeNames(elements), getProcessorBatchSize());
        tupleGroupInfo.add(newID);
		tupleSourceMap.put(newID, tupleBuffer);
        if (LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.DETAIL)) {
            LogManager.logDetail(LogConstants.CTX_BUFFER_MGR, new Object[]{"Creating TupleBuffer:", newID, "of type "+tupleSourceType}); //$NON-NLS-1$ //$NON-NLS-2$
        }
        return tupleBuffer;
    }
    
    /**
     * Gets the data type names for each of the input expressions, in order.
     * @param expressions List of Expressions
     * @return
     * @since 4.2
     */
    private static String[] getTypeNames(List expressions) {
    	if (expressions == null) {
    		return null;
    	}
        String[] types = new String[expressions.size()];
        for (ListIterator i = expressions.listIterator(); i.hasNext();) {
            Expression expr = (Expression)i.next();
            types[i.previousIndex()] = DataTypeManager.getDataTypeName(expr.getType());
        }
        return types;
    }

    /**
     * Remove all the tuple sources with the specified group name.  Typically the group
     * name is really a session identifier and this is called to remove all tuple sources
     * used by a particular session.
     * @param groupName Name of the group
     * @throws MetaMatrixComponentException If internal server error occurs
     */
    public void removeTupleBuffers(String groupName)
    throws MetaMatrixComponentException {

        if (LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.DETAIL)) {
            LogManager.logDetail(LogConstants.CTX_BUFFER_MGR, new Object[]{"Removing TupleBuffers for group", groupName}); //$NON-NLS-1$
        }

        Set<TupleSourceID> tupleGroupInfo = null;
        synchronized(groupInfos) {
        	tupleGroupInfo = groupInfos.remove(groupName);
        }
        if (tupleGroupInfo == null) {
        	return;
        }
        List<TupleSourceID> tupleSourceIDs = null;
        synchronized (tupleGroupInfo) {
			tupleSourceIDs = new ArrayList<TupleSourceID>(tupleGroupInfo);
		}
        for (TupleSourceID tsID : tupleSourceIDs) {
     		try {
     			TupleBuffer tupleBuffer = getTupleBuffer(tsID);
     			tupleBuffer.remove();
     		} catch(TupleSourceNotFoundException e) {
     			// ignore and go on
     		}
        }
    }

    /**
     * Get the tuple source info for the specified tuple source ID
     * @param tupleSourceID Tuple source to get
     * @return Tuple source info
     * @throws TupleSourceNotFoundException If not found
     */
    public TupleBuffer getTupleBuffer(TupleSourceID tupleSourceID) throws TupleSourceNotFoundException {
    	TupleBuffer info = this.tupleSourceMap.get(tupleSourceID);

        if(info == null) {
        	throw new TupleSourceNotFoundException(QueryExecPlugin.Util.getString("BufferManagerImpl.tuple_source_not_found", tupleSourceID)); //$NON-NLS-1$
        }
        return info;
    }
    
    /**
     * Gets a TupleGroupInfo representing the group 
     * @param groupName Logical grouping name for tuple sources. Can be null.
     * @return TupleGroupInfo for the group. Never null.
     * @since 4.3
     */
    private Set<TupleSourceID> getGroupInfo(String groupName, boolean create) {
    	Set<TupleSourceID> info = null;
        synchronized(groupInfos) {
            info = groupInfos.get(groupName);
            if (info == null && create) {
                // If it doesn't exist, create a new one
                info = Collections.synchronizedSet(new HashSet<TupleSourceID>());
                groupInfos.put(groupName, info);
            }
        }
        return info;
    }

	@Override
	public void addBatch(TupleSourceID sourceID, TupleBatch batch,
			String[] types) throws MetaMatrixComponentException {
		if (LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.TRACE)) {
            LogManager.logTrace(LogConstants.CTX_BUFFER_MGR, new Object[]{"AddTupleBatch for", sourceID, "with " + batch.getRowCount() + " rows"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        }
		this.diskMgr.addBatch(sourceID, batch, types);
	}

	@Override
	public TupleBatch getBatch(TupleSourceID sourceID, int beginRow,
			String[] types) throws TupleSourceNotFoundException,
			MetaMatrixComponentException {
		if (LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.TRACE)) {
            LogManager.logTrace(LogConstants.CTX_BUFFER_MGR, new Object[]{"Getting tupleBatch for", sourceID, "beginRow:", beginRow}); //$NON-NLS-1$ //$NON-NLS-2$
        }
		return this.diskMgr.getBatch(sourceID, beginRow, types);
	}

	@Override
	public void initialize(Properties props)
			throws MetaMatrixComponentException {
		this.lookup = "local"; //$NON-NLS-1$
		PropertiesUtils.setBeanProperties(this, props, "metamatrix.buffer"); //$NON-NLS-1$
	}

	@Override
	public void removeBatches(TupleSourceID sourceID) {
		if (LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.DETAIL)) {
            LogManager.logDetail(LogConstants.CTX_BUFFER_MGR, new Object[]{"Removing TupleBuffer:", sourceID}); //$NON-NLS-1$
        }
		TupleBuffer tupleBuffer = this.tupleSourceMap.remove(sourceID);
		if (tupleBuffer != null) {
			Set<TupleSourceID> ids = getGroupInfo(tupleBuffer.getGroupName(), false);
			if (ids != null) {
				ids.remove(sourceID);
			}
		}
		this.diskMgr.removeBatches(sourceID);
	}

	@Override
	public void shutdown() {
		this.diskMgr.shutdown();
	}
    
}
