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

import java.util.List;
import java.util.ListIterator;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import javax.xml.transform.Source;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.buffer.FileStore;
import com.metamatrix.common.buffer.StorageManager;
import com.metamatrix.common.buffer.TupleBuffer;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.common.types.InputStreamFactory;
import com.metamatrix.common.types.SQLXMLImpl;
import com.metamatrix.common.types.SourceTransform;
import com.metamatrix.common.types.StandardXMLTranslator;
import com.metamatrix.common.types.Streamable;
import com.metamatrix.common.types.XMLType;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.log.MessageLevel;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.dqp.util.LogConstants;
import com.metamatrix.query.processor.xml.XMLUtil;
import com.metamatrix.query.sql.symbol.Expression;

/**
 * <p>Default implementation of BufferManager.</p>
 * Responsible for creating/tracking TupleBuffers and providing access to the StorageManager
 */
public class BufferManagerImpl implements BufferManager, StorageManager {

	// Configuration 
    private int connectorBatchSize = BufferManager.DEFAULT_CONNECTOR_BATCH_SIZE;
    private int processorBatchSize = BufferManager.DEFAULT_PROCESSOR_BATCH_SIZE;
    private int maxProcessingBatches = BufferManager.DEFAULT_MAX_PROCESSING_BATCHES;
    private int reserveBatches = BufferManager.DEFAULT_RESERVE_BUFFERS;
    private int maxReserveBatches = BufferManager.DEFAULT_RESERVE_BUFFERS;
    private ReentrantLock lock = new ReentrantLock(true);
    private Condition batchesFreed = lock.newCondition();
    
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
    	String newID = String.valueOf(this.currentTuple.getAndIncrement());
        TupleBuffer tupleBuffer = new TupleBuffer(this, newID, elements, getTypeNames(elements), getProcessorBatchSize());
        if (LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.DETAIL)) {
            LogManager.logDetail(LogConstants.CTX_BUFFER_MGR, new Object[]{"Creating TupleBuffer:", newID, "of type "+tupleSourceType}); //$NON-NLS-1$ //$NON-NLS-2$
        }
        return tupleBuffer;
    }
    
    @Override
    public FileStore createFileStore(String name) {
    	if (LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.DETAIL)) {
            LogManager.logDetail(LogConstants.CTX_BUFFER_MGR, "Creating FileStore:", name); //$NON-NLS-1$ 
        }
    	return this.diskMgr.createFileStore(name);
    }
    
	@Override
	public void initialize(Properties props) throws MetaMatrixComponentException {
		PropertiesUtils.setBeanProperties(this, props, "metamatrix.buffer"); //$NON-NLS-1$
		DataTypeManager.addSourceTransform(Source.class, new SourceTransform<Source, XMLType>() {
			@Override
			public XMLType transform(Source value) {
				if (value instanceof InputStreamFactory) {
					return new XMLType(new SQLXMLImpl((InputStreamFactory)value));
				}
				StandardXMLTranslator sxt = new StandardXMLTranslator(value, null);
				SQLXMLImpl sqlxml;
				try {
					sqlxml = XMLUtil.saveToBufferManager(BufferManagerImpl.this, sxt, Streamable.STREAMING_BATCH_SIZE_IN_BYTES);
				} catch (MetaMatrixComponentException e) {
					throw new MetaMatrixRuntimeException(e);
				}
				return new XMLType(sqlxml);
			}
		});
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
    
    @Override
    public void releaseBuffers(int count) {
    	lock.lock();
    	try {
	    	this.reserveBatches += count;
	    	batchesFreed.signalAll();
    	} finally {
    		lock.unlock();
    	}
    }	
    
    @Override
    public int reserveBuffers(int count, boolean wait) throws MetaMatrixComponentException {
    	lock.lock();
	    try {
	    	while (wait && count > this.reserveBatches && this.reserveBatches < this.maxReserveBatches / 2) {
	    		try {
					batchesFreed.await();
				} catch (InterruptedException e) {
					throw new MetaMatrixComponentException(e);
				}
	    	}	
	    	this.reserveBatches -= count;
	    	if (this.reserveBatches >= 0) {
	    		return count;
	    	}
	    	int result = count + this.reserveBatches;
	    	this.reserveBatches = 0;
	    	return result;
	    } finally {
    		lock.unlock();
    	}
    }

	public void shutdown() {
	}
    
}
