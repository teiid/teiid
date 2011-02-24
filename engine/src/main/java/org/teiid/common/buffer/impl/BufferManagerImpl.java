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

package org.teiid.common.buffer.impl;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.teiid.common.buffer.BatchManager;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.FileStore;
import org.teiid.common.buffer.LobManager;
import org.teiid.common.buffer.STree;
import org.teiid.common.buffer.StorageManager;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.common.buffer.BatchManager.ManagedBatch;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.Assertion;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.query.QueryPlugin;
import org.teiid.query.processor.relational.ListNestedSortComparator;
import org.teiid.query.sql.symbol.Expression;


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
	private static final int COMPACTION_THRESHOLD = 1 << 25; //start checking at 32 megs
	
	private final class BatchManagerImpl implements BatchManager {
		private final String id;
		private final int columnCount;
		private volatile FileStore store;
		private Map<Long, long[]> physicalMapping = new ConcurrentHashMap<Long, long[]>();
		private ReadWriteLock compactionLock = new ReentrantReadWriteLock();
		private AtomicLong unusedSpace = new AtomicLong();
		private int[] lobIndexes;

		private BatchManagerImpl(String newID, int columnCount, int[] lobIndexes) {
			this.id = newID;
			this.columnCount = columnCount;
			this.store = createFileStore(id);
			this.store.setCleanupReference(this);
			this.lobIndexes = lobIndexes;
		}
		
		public FileStore createStorage(String prefix) {
			return createFileStore(id+prefix);
		}

		@Override
		public ManagedBatch createManagedBatch(TupleBatch batch, boolean softCache)
				throws TeiidComponentException {
			ManagedBatchImpl mbi = new ManagedBatchImpl(batch, this, softCache);
			mbi.addToCache(false);
			persistBatchReferences();
			return mbi;
		}
		
		private boolean shouldCompact(long offset) {
			return offset > COMPACTION_THRESHOLD && unusedSpace.get() * 4 > offset * 3;
		}
		
		private long getOffset() throws TeiidComponentException {
			long offset = store.getLength();
			if (!shouldCompact(offset)) {
				return offset;
			}
			try {
				this.compactionLock.writeLock().lock();
				offset = store.getLength();
				//retest the condition to ensure that compaction is still needed
				if (!shouldCompact(offset)) {
					return offset;
				}
				FileStore newStore = createFileStore(id);
				newStore.setCleanupReference(this);
				byte[] buffer = new byte[IO_BUFFER_SIZE];
				List<long[]> values = new ArrayList<long[]>(physicalMapping.values());
				Collections.sort(values, new Comparator<long[]>() {
					@Override
					public int compare(long[] o1, long[] o2) {
						return Long.signum(o1[0] - o2[0]);
					}
				});
				for (long[] info : values) {
					long oldOffset = info[0];
					info[0] = newStore.getLength();
					int size = (int)info[1];
					while (size > 0) {
						int toWrite = Math.min(IO_BUFFER_SIZE, size);
						store.readFully(oldOffset, buffer, 0, toWrite);
						newStore.write(buffer, 0, toWrite);
						size -= toWrite;
					}
				}
				store.remove();
				store = newStore;
				long oldOffset = offset;
				offset = store.getLength();
				LogManager.logTrace(LogConstants.CTX_BUFFER_MGR, "Compacted store", id, "pre-size", oldOffset, "post-size", offset); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
				return offset;
			} finally {
				this.compactionLock.writeLock().unlock();
			}
		}

		@Override
		public void remove() {
			this.store.remove();
		}
	}

	/**
	 * Holder for active batches
	 */
	private class TupleBufferInfo {
		TreeMap<Integer, ManagedBatchImpl> batches = new TreeMap<Integer, ManagedBatchImpl>();
		Integer lastUsed = null;
		
		ManagedBatchImpl removeBatch(int row) {
			ManagedBatchImpl result = batches.remove(row);
			if (result != null) {
				activeBatchColumnCount -= result.batchManager.columnCount;
			}
			return result;
		}
	}
	
	private final class ManagedBatchImpl implements ManagedBatch {
		private boolean persistent;
		private boolean softCache;
		private volatile TupleBatch activeBatch;
		private volatile Reference<TupleBatch> batchReference;
		private int beginRow;
		private BatchManagerImpl batchManager;
		private long id;
		private LobManager lobManager;
		
		public ManagedBatchImpl(TupleBatch batch, BatchManagerImpl manager, boolean softCache) {
			this.softCache = softCache;
			id = batchAdded.incrementAndGet();
            LogManager.logTrace(LogConstants.CTX_BUFFER_MGR, "Add batch to BufferManager", id); //$NON-NLS-1$
			this.activeBatch = batch;
			this.beginRow = batch.getBeginRow();
			this.batchManager = manager;
			if (this.batchManager.lobIndexes != null) {
				this.lobManager = new LobManager();
			}
		}
		
		@Override
		public void setPrefersMemory(boolean prefers) {
			this.softCache = prefers;
			//TODO: could recreate the reference
		}

		private void addToCache(boolean update) {
			synchronized (activeBatches) {
				TupleBatch batch = this.activeBatch;
				if (batch == null) {
					return; //already removed
				}
				activeBatchColumnCount += batchManager.columnCount;
				TupleBufferInfo tbi = null;
				if (update) {
					tbi = activeBatches.remove(batchManager.id);
				} else {
					tbi = activeBatches.get(batchManager.id);
				}
				if (tbi == null) {
					tbi = new TupleBufferInfo();
					update = true;
				} 
				if (update) {
					activeBatches.put(batchManager.id, tbi);
				}
				Assertion.isNull(tbi.batches.put(this.beginRow, this));
			}
		}

		@Override
		public TupleBatch getBatch(boolean cache, String[] types) throws TeiidComponentException {
			long reads = readAttempts.incrementAndGet();
			LogManager.logTrace(LogConstants.CTX_BUFFER_MGR, batchManager.id, "getting batch", reads, "reference hits", referenceHit.get()); //$NON-NLS-1$ //$NON-NLS-2$
			synchronized (activeBatches) {
				TupleBufferInfo tbi = activeBatches.remove(batchManager.id);
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
						activeBatches.put(batchManager.id, tbi);
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
				long count = readCount.incrementAndGet();
				LogManager.logTrace(LogConstants.CTX_BUFFER_MGR, batchManager.id, id, "reading batch from disk, total reads:", count); //$NON-NLS-1$
				try {
					this.batchManager.compactionLock.readLock().lock();
					long[] info = batchManager.physicalMapping.get(this.id);
					ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(batchManager.store.createInputStream(info[0]), IO_BUFFER_SIZE));
		            batch = new TupleBatch();
		            batch.setDataTypes(types);
		            batch.readExternal(ois);
		            batch.setRowOffset(this.beginRow);
			        batch.setDataTypes(null);
					if (lobManager != null) {
						for (List<?> tuple : batch.getTuples()) {
							lobManager.updateReferences(batchManager.lobIndexes, tuple);
						}
					}
			        if (cache) {
			        	this.activeBatch = batch;
			        	addToCache(true);
			        }
					return batch;
		        } catch(IOException e) {
		        	throw new TeiidComponentException(e, QueryPlugin.Util.getString("FileStoreageManager.error_reading", batchManager.id)); //$NON-NLS-1$
		        } catch (ClassNotFoundException e) {
		        	throw new TeiidComponentException(e, QueryPlugin.Util.getString("FileStoreageManager.error_reading", batchManager.id)); //$NON-NLS-1$
		        } finally {
		        	this.batchManager.compactionLock.readLock().unlock();
		        }
			}
		}

		public synchronized void persist() throws TeiidComponentException {
			boolean lockheld = false;
            try {
				TupleBatch batch = activeBatch;
				if (batch != null) {
					if (!persistent) {
						long count = writeCount.incrementAndGet();
						LogManager.logTrace(LogConstants.CTX_BUFFER_MGR, batchManager.id, id, "writing batch to disk, total writes: ", count); //$NON-NLS-1$
						long offset = 0;
						if (lobManager != null) {
							for (List<?> tuple : batch.getTuples()) {
								lobManager.updateReferences(batchManager.lobIndexes, tuple);
							}
						}
						synchronized (batchManager.store) {
							offset = batchManager.getOffset();
							OutputStream fsos = new BufferedOutputStream(batchManager.store.createOutputStream(), IO_BUFFER_SIZE);
				            ObjectOutputStream oos = new ObjectOutputStream(fsos);
				            batch.writeExternal(oos);
				            oos.close();
				            long size = batchManager.store.getLength() - offset;
				            long[] info = new long[] {offset, size};
				            batchManager.physicalMapping.put(this.id, info);
						}
						LogManager.logTrace(LogConstants.CTX_BUFFER_MGR, batchManager.id, id, "batch written starting at:", offset); //$NON-NLS-1$
					}
					if (softCache) {
						this.batchReference = new SoftReference<TupleBatch>(batch);
					} else {
						this.batchReference = new WeakReference<TupleBatch>(batch);
					}
				}
			} catch (IOException e) {
				throw new TeiidComponentException(e);
			} catch (Throwable e) {
				throw new TeiidComponentException(e);
			} finally {
				persistent = true;
				activeBatch = null;
				if (lockheld) {
					this.batchManager.compactionLock.writeLock().unlock();
				}
			}
		}

		public void remove() {
			synchronized (activeBatches) {
				TupleBufferInfo tbi = activeBatches.get(batchManager.id);
				if (tbi != null && tbi.removeBatch(this.beginRow) != null) {
					if (tbi.batches.isEmpty()) {
						activeBatches.remove(batchManager.id);
					}
				}
			}
			long[] info = batchManager.physicalMapping.remove(id);
			if (info != null) {
				batchManager.unusedSpace.addAndGet(info[1]); 
			}
			activeBatch = null;
			batchReference = null;
		}
		
		@Override
		public String toString() {
			return "ManagedBatch " + batchManager.id + " " + activeBatch; //$NON-NLS-1$ //$NON-NLS-2$
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
	private Map<String, TupleReference> tupleBufferMap = new ConcurrentHashMap<String, TupleReference>();
	private ReferenceQueue<TupleBuffer> tupleBufferQueue = new ReferenceQueue<TupleBuffer>();
    
    
    private StorageManager diskMgr;

    private AtomicLong tsId = new AtomicLong();
    private AtomicLong batchAdded = new AtomicLong();
    private AtomicLong readCount = new AtomicLong();
	private AtomicLong writeCount = new AtomicLong();
	private AtomicLong readAttempts = new AtomicLong();
	private AtomicLong referenceHit = new AtomicLong();
	
	public long getBatchesAdded() {
		return batchAdded.get();
	}
	
	public long getReadCount() {
		return readCount.get();
	}
	
	public long getWriteCount() {
		return writeCount.get();
	}
	
	public long getReadAttempts() {
		return readAttempts.get();
	}
	
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
    	final String newID = String.valueOf(this.tsId.getAndIncrement());
    	int[] lobIndexes = LobManager.getLobIndexes(elements);
    	BatchManager batchManager = new BatchManagerImpl(newID, elements.size(), lobIndexes);
        TupleBuffer tupleBuffer = new TupleBuffer(batchManager, newID, elements, lobIndexes, getProcessorBatchSize());
        LogManager.logDetail(LogConstants.CTX_BUFFER_MGR, "Creating TupleBuffer:", newID, "of type ", tupleSourceType); //$NON-NLS-1$ //$NON-NLS-2$
        return tupleBuffer;
    }
    
    public STree createSTree(final List elements, String groupName, int keyLength) {
    	String newID = String.valueOf(this.tsId.getAndIncrement());
    	int[] lobIndexes = LobManager.getLobIndexes(elements);
    	BatchManager bm = new BatchManagerImpl(newID, elements.size(), lobIndexes);
    	BatchManager keyManager = new BatchManagerImpl(String.valueOf(this.tsId.getAndIncrement()), keyLength, null);
    	int[] compareIndexes = new int[keyLength];
    	for (int i = 1; i < compareIndexes.length; i++) {
			compareIndexes[i] = i;
		}
        LogManager.logDetail(LogConstants.CTX_BUFFER_MGR, "Creating STree:", newID); //$NON-NLS-1$ 
    	return new STree(keyManager, bm, new ListNestedSortComparator(compareIndexes), getProcessorBatchSize(), keyLength, TupleBuffer.getTypeNames(elements));
    }

    @Override
    public FileStore createFileStore(String name) {
        LogManager.logDetail(LogConstants.CTX_BUFFER_MGR, "Creating FileStore:", name); //$NON-NLS-1$ 
    	return this.diskMgr.createFileStore(name);
    }
    
	@Override
	public void initialize() throws TeiidComponentException {
		
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
						throw new TeiidRuntimeException(e);
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
			} catch (TeiidComponentException e) {
				LogManager.logDetail(LogConstants.CTX_BUFFER_MGR, e, "Error persisting batch, attempts to read that batch later will result in an exception"); //$NON-NLS-1$
			}
		}
	}
	
	@Override
	public int getSchemaSize(List elements) {
		int total = 0;
		//we make a assumption that the average column size under 64bits is approximately 128bytes
		//this includes alignment, row/array, and reference overhead
		for (Object element : elements) {
			Class<?> type = ((Expression)element).getType();
			if (type == DataTypeManager.DefaultDataClasses.STRING) {
				total += 256; //assumes an "average" string length of approximately 100 chars
			} else if (type == DataTypeManager.DefaultDataClasses.DATE 
					|| type == DataTypeManager.DefaultDataClasses.TIME 
					|| type == DataTypeManager.DefaultDataClasses.TIMESTAMP) {
				total += 32;
			} else if (type == DataTypeManager.DefaultDataClasses.LONG 
					|| type	 == DataTypeManager.DefaultDataClasses.DOUBLE) {
				total += 20;
			} else if (type == DataTypeManager.DefaultDataClasses.INTEGER 
					|| type == DataTypeManager.DefaultDataClasses.FLOAT) {
				total += 14;
			} else if (type == DataTypeManager.DefaultDataClasses.CHAR 
					|| type == DataTypeManager.DefaultDataClasses.SHORT) {
				total += 10;
			} else if (type == DataTypeManager.DefaultDataClasses.BOOLEAN 
					|| type == DataTypeManager.DefaultDataClasses.BYTE
					|| type == DataTypeManager.DefaultDataClasses.NULL) {
				//even if value caching is turned off we don't bother counting 
				//the additional references that may exist to boolean and byte values
				total += 8;
			} else if (type == DataTypeManager.DefaultDataClasses.OBJECT) {
				total += 1024;
			} else {
				total += 512; //assumes buffer overhead in the case of lobs
				//however the account for lobs is misleading as the lob
				//references are not actually removed from memory
			}
		}
		return Math.max(1, total/128);
	}
	
    public void setMaxReserveBatchColumns(int maxReserve) {
    	this.maxReserveBatchColumns = maxReserve;
		this.reserveBatchColumns = maxReserve;
	}

	public void shutdown() {
	}

	@Override
	public void addTupleBuffer(TupleBuffer tb) {
		cleanDefunctTupleBuffers();
		this.tupleBufferMap.put(tb.getId(), new TupleReference(tb, this.tupleBufferQueue));
	}
	
	@Override
	public TupleBuffer getTupleBuffer(String id) {		
		cleanDefunctTupleBuffers();
		Reference<TupleBuffer> r = this.tupleBufferMap.get(id);
		if (r != null) {
			return r.get();
		}
		return null;
	}
	
	private void cleanDefunctTupleBuffers() {
		while (true) {
			Reference r = this.tupleBufferQueue.poll();
			if (r == null) {
				break;
			}
			this.tupleBufferMap.remove(((TupleReference)r).id);
		}
	}
	
	static class TupleReference extends WeakReference<TupleBuffer>{
		String id;
		public TupleReference(TupleBuffer referent, ReferenceQueue<? super TupleBuffer> q) {
			super(referent, q);
			id = referent.getId();
		}
	}
}
