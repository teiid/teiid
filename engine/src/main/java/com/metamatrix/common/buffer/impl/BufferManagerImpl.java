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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import javax.xml.transform.Source;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.buffer.BatchManager;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.buffer.FileStore;
import com.metamatrix.common.buffer.StorageManager;
import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.common.buffer.TupleBuffer;
import com.metamatrix.common.buffer.BatchManager.ManagedBatch;
import com.metamatrix.common.buffer.FileStore.FileStoreOutputStream;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.common.types.InputStreamFactory;
import com.metamatrix.common.types.SQLXMLImpl;
import com.metamatrix.common.types.SourceTransform;
import com.metamatrix.common.types.StandardXMLTranslator;
import com.metamatrix.common.types.Streamable;
import com.metamatrix.common.types.XMLType;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.log.MessageLevel;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.query.execution.QueryExecPlugin;
import com.metamatrix.query.processor.xml.XMLUtil;

/**
 * <p>Default implementation of BufferManager.</p>
 * Responsible for creating/tracking TupleBuffers and providing access to the StorageManager.
 * </p>
 * The buffering strategy attempts to purge batches from the least recently used TupleBuffer
 * from before (which wraps around circularly) the last used batch.  This attempts to compensate 
 * for our tendency to read buffers in a forward manner.  If our processing algorithms are changed 
 * to use alternating ascending/descending access, then the buffering approach could be replaced 
 * with a simple LRU.
 * 
 * TODO: allow for cached stores to use lru - (result set/mat view)
 * TODO: account for row/content based sizing (difficult given value sharing)
 * TODO: account for memory based lobs (it would be nice if the approximate buffer size matched at 100kB)
 * TODO: add detection of pinned batches to prevent unnecessary purging of non-persistent batches
 *       - this is not necessary for already persistent batches, since we hold a weak reference
 */
public class BufferManagerImpl implements BufferManager, StorageManager {
	
	private static final int IO_BUFFER_SIZE = 1 << 14;
	
	/**
	 * Holder for active batches
	 */
	private class TupleBufferInfo {
		TreeMap<Integer, ManagedBatchImpl> batches = new TreeMap<Integer, ManagedBatchImpl>();
		Integer lastUsed = null;
		
		ManagedBatchImpl removeBatch(int row) {
			ManagedBatchImpl result = batches.remove(row);
			if (result != null) {
				activeBatchColumnCount -= result.columnCount;
			}
			return result;
		}
	}
	
	private final class ManagedBatchImpl implements ManagedBatch {
		final private String id;
		final private FileStore store;
		
		private long offset = -1;
		private boolean persistent;
		private volatile TupleBatch activeBatch;
		private volatile Reference<TupleBatch> batchReference;
		private int beginRow;
		private int columnCount;
		
		public ManagedBatchImpl(String id, FileStore store, TupleBatch batch) {
            LogManager.logTrace(com.metamatrix.common.log.LogConstants.CTX_BUFFER_MGR, "Add batch to BufferManager", batchAdded.incrementAndGet()); //$NON-NLS-1$
			this.id = id;
			this.store = store;
			this.activeBatch = batch;
			this.beginRow = batch.getBeginRow();
			List[] allTuples = batch.getAllTuples();
			if (allTuples.length > 0) {
				columnCount = allTuples[0].size();
			}
		}

		private void addToCache(boolean update) {
			synchronized (activeBatches) {
				TupleBatch batch = this.activeBatch;
				if (batch == null) {
					return; //already removed
				}
				activeBatchColumnCount += columnCount;
				TupleBufferInfo tbi = null;
				if (update) {
					tbi = activeBatches.remove(this.id);
				} else {
					tbi = activeBatches.get(this.id);
				}
				if (tbi == null) {
					tbi = new TupleBufferInfo();
					update = true;
				} 
				if (update) {
					activeBatches.put(this.id, tbi);
				}
				Assertion.isNull(tbi.batches.put(this.beginRow, this));
			}
		}

		@Override
		public TupleBatch getBatch(boolean cache, String[] types) throws MetaMatrixComponentException {
			LogManager.logTrace(com.metamatrix.common.log.LogConstants.CTX_BUFFER_MGR, "Reading batch from disk", readAttempts.incrementAndGet(), "reference hits", referenceHit.get()); //$NON-NLS-1$ //$NON-NLS-2$
			synchronized (activeBatches) {
				TupleBufferInfo tbi = activeBatches.remove(this.id);
				if (tbi != null) { 
					boolean put = true;
					if (!cache) {
						tbi.removeBatch(this.beginRow);
						if (tbi.batches.isEmpty()) {
							put = false;
						}
					}
					if (put) {
						tbi.lastUsed = this.beginRow;
						activeBatches.put(this.id, tbi);
					}
				}
			}
			persistBatchReferences();
			synchronized (this) {
				TupleBatch batch = this.activeBatch;
				if (batch != null){
					return batch;
				}
				Reference<TupleBatch> ref = this.batchReference;
				this.batchReference = null;
				if (ref != null) {
					batch = ref.get();
					if (batch != null) {
						if (cache) {
							this.activeBatch = batch;
				        	addToCache(true);
						} 
						referenceHit.getAndIncrement();
						return batch;
					}
				}
				LogManager.logTrace(com.metamatrix.common.log.LogConstants.CTX_BUFFER_MGR, "Reading batch from disk", readCount.incrementAndGet()); //$NON-NLS-1$
				try {
		            ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(store.createInputStream(this.offset), IO_BUFFER_SIZE));
		            batch = new TupleBatch();
		            batch.setDataTypes(types);
		            batch.readExternal(ois);
			        batch.setDataTypes(null);
			        if (cache) {
			        	this.activeBatch = batch;
			        	addToCache(true);
			        }
					return batch;
		        } catch(IOException e) {
		        	throw new MetaMatrixComponentException(e, QueryExecPlugin.Util.getString("FileStoreageManager.error_reading", id)); //$NON-NLS-1$
		        } catch (ClassNotFoundException e) {
		        	throw new MetaMatrixComponentException(e, QueryExecPlugin.Util.getString("FileStoreageManager.error_reading", id)); //$NON-NLS-1$
		        }
			}
		}

		public synchronized void persist() throws MetaMatrixComponentException {
			try {
				TupleBatch batch = activeBatch;
				if (batch != null) {
					if (!persistent) {
						LogManager.logTrace(com.metamatrix.common.log.LogConstants.CTX_BUFFER_MGR, "Writing batch to disk", writeCount.incrementAndGet()); //$NON-NLS-1$
						synchronized (store) {
							offset = store.getLength();
							OutputStream fsos = new BufferedOutputStream(store.createOutputStream(), IO_BUFFER_SIZE);
				            ObjectOutputStream oos = new ObjectOutputStream(fsos);
				            batch.writeExternal(oos);
				            oos.flush();
				            oos.close();
						}
					}
					this.batchReference = new WeakReference<TupleBatch>(batch);
				}
			} catch (IOException e) {
				throw new MetaMatrixComponentException(e);
			} finally {
				persistent = true;
				activeBatch = null;
			}
		}

		public void remove() {
			synchronized (activeBatches) {
				TupleBufferInfo tbi = activeBatches.get(this.id);
				if (tbi != null && tbi.removeBatch(this.beginRow) != null && tbi.batches.isEmpty()) {
					activeBatches.remove(this.id);
				}
			}
			activeBatch = null;
			batchReference = null;
		}
		
		@Override
		public String toString() {
			return "ManagedBatch " + id + " " + activeBatch; //$NON-NLS-1$ //$NON-NLS-2$
		}
	}
	
	// Configuration 
    private int connectorBatchSize = BufferManager.DEFAULT_CONNECTOR_BATCH_SIZE;
    private int processorBatchSize = BufferManager.DEFAULT_PROCESSOR_BATCH_SIZE;
    private int maxProcessingBatches = BufferManager.DEFAULT_MAX_PROCESSING_BATCHES;
    private int maxReserveBatchColumns = BufferManager.DEFAULT_RESERVE_BUFFERS;
    private volatile int reserveBatchColumns = BufferManager.DEFAULT_RESERVE_BUFFERS;
    
    private ReentrantLock lock = new ReentrantLock(true);
    private Condition batchesFreed = lock.newCondition();
    
    private volatile int activeBatchColumnCount = 0;
    private Map<String, TupleBufferInfo> activeBatches = new LinkedHashMap<String, TupleBufferInfo>();
    
    private StorageManager diskMgr;

    private AtomicLong currentTuple = new AtomicLong();
    private AtomicInteger batchAdded = new AtomicInteger();
    private AtomicInteger readCount = new AtomicInteger();
	private AtomicInteger writeCount = new AtomicInteger();
	private AtomicInteger readAttempts = new AtomicInteger();
	private AtomicInteger referenceHit = new AtomicInteger();
	
	@Override
    public int getMaxProcessingBatchColumns() {
		return maxProcessingBatches;
	}
    
    public void setMaxProcessingBatchColumns(int maxProcessingBatches) {
		this.maxProcessingBatches = Math.max(0, maxProcessingBatches);
	}

    /**
     * Get processor batch size
     * @return Number of rows in a processor batch
     */
    @Override
    public int getProcessorBatchSize() {        
        return this.processorBatchSize;
    }

    /**
     * Get connector batch size
     * @return Number of rows in a connector batch
     */
    @Override
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
    public TupleBuffer createTupleBuffer(final List elements, String groupName,
    		TupleSourceType tupleSourceType) {
    	final String newID = String.valueOf(this.currentTuple.getAndIncrement());
    	
    	BatchManager batchManager = new BatchManager() {
    		private FileStore store;

    		@Override
    		public ManagedBatch createManagedBatch(TupleBatch batch)
    				throws MetaMatrixComponentException {
    			if (this.store == null) {
    				this.store = createFileStore(newID);
    				this.store.setCleanupReference(this);
    			}
    			ManagedBatchImpl mbi = new ManagedBatchImpl(newID, store, batch);
    			mbi.addToCache(false);
    			persistBatchReferences();
    			return mbi;
    		}

    		@Override
    		public void remove() {
    			if (this.store != null) {
    				this.store.remove();
    				this.store = null;
    			}
    		}
    	};
        TupleBuffer tupleBuffer = new TupleBuffer(batchManager, newID, elements, getProcessorBatchSize());
        if (LogManager.isMessageToBeRecorded(com.metamatrix.common.log.LogConstants.CTX_BUFFER_MGR, MessageLevel.DETAIL)) {
            LogManager.logDetail(com.metamatrix.common.log.LogConstants.CTX_BUFFER_MGR, new Object[]{"Creating TupleBuffer:", newID, "of type "+tupleSourceType}); //$NON-NLS-1$ //$NON-NLS-2$
        }
        return tupleBuffer;
    }
    
    @Override
    public FileStore createFileStore(String name) {
    	if (LogManager.isMessageToBeRecorded(com.metamatrix.common.log.LogConstants.CTX_BUFFER_MGR, MessageLevel.DETAIL)) {
            LogManager.logDetail(com.metamatrix.common.log.LogConstants.CTX_BUFFER_MGR, "Creating FileStore:", name); //$NON-NLS-1$ 
        }
    	return this.diskMgr.createFileStore(name);
    }
    
	@Override
	public void initialize() throws MetaMatrixComponentException {
		DataTypeManager.addSourceTransform(Source.class, new SourceTransform<Source, XMLType>() {
			@Override
			public XMLType transform(Source value) {
				if (value instanceof InputStreamFactory) {
					return new XMLType(new SQLXMLImpl((InputStreamFactory)value));
				}
				StandardXMLTranslator sxt = new StandardXMLTranslator(value);
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
    
    @Override
    public void releaseBuffers(int count) {
    	if (count < 1) {
    		return;
    	}
    	lock.lock();
    	try {
	    	this.reserveBatchColumns += count;
	    	batchesFreed.signalAll();
    	} finally {
    		lock.unlock();
    	}
    }	
    
    @Override
    public int reserveBuffers(int count, BufferReserveMode mode) {
    	lock.lock();
	    try {
	    	if (mode == BufferReserveMode.WAIT) {
		    	int waitCount = 0;
		    	while (count - waitCount > this.reserveBatchColumns) {
		    		try {
						batchesFreed.await(100, TimeUnit.MILLISECONDS);
					} catch (InterruptedException e) {
						throw new MetaMatrixRuntimeException(e);
					}
					waitCount++;
		    	}	
	    	}
	    	if (this.reserveBatchColumns >= count || mode == BufferReserveMode.FORCE) {
		    	this.reserveBatchColumns -= count;
	    		return count;
	    	}
	    	int result = Math.max(0, this.reserveBatchColumns);
	    	this.reserveBatchColumns -= result;
	    	return result;
	    } finally {
    		lock.unlock();
    		persistBatchReferences();
    	}
    }
    
	void persistBatchReferences() {
		if (activeBatchColumnCount == 0 || activeBatchColumnCount <= reserveBatchColumns) {
			int memoryCount = activeBatchColumnCount + maxReserveBatchColumns - reserveBatchColumns;
			if (DataTypeManager.isValueCacheEnabled()) {
				if (memoryCount < maxReserveBatchColumns / 8) {
					DataTypeManager.setValueCacheEnabled(false);
				}
			} else if (memoryCount > maxReserveBatchColumns / 4) {
				DataTypeManager.setValueCacheEnabled(true);
			}
			return;
		}
		while (true) {
			ManagedBatchImpl mb = null;
			synchronized (activeBatches) {
				if (activeBatchColumnCount == 0 || activeBatchColumnCount * 5 < reserveBatchColumns * 4) {
					break;
				}
				Iterator<TupleBufferInfo> iter = activeBatches.values().iterator();
				TupleBufferInfo tbi = iter.next();
				Map.Entry<Integer, ManagedBatchImpl> entry = null;
				if (tbi.lastUsed != null) {
					entry = tbi.batches.floorEntry(tbi.lastUsed - 1);
				}
				if (entry == null) {
					entry = tbi.batches.lastEntry();
				} 
				tbi.removeBatch(entry.getKey());
				if (tbi.batches.isEmpty()) {
					iter.remove();
				}
				mb = entry.getValue();
			}
			try {
				mb.persist();
			} catch (MetaMatrixComponentException e) {
				LogManager.logDetail(com.metamatrix.common.log.LogConstants.CTX_BUFFER_MGR, e, "Error persisting batch, attempts to read that batch later will result in an exception"); //$NON-NLS-1$
			}
		}
	}
	
	@Override
	public int getSchemaSize(List elements) {
		return elements.size();
	}
	
    public void setMaxReserveBatchColumns(int maxReserve) {
    	this.maxReserveBatchColumns = maxReserve;
		this.reserveBatchColumns = maxReserve;
	}

	public void shutdown() {
	}
    
}
