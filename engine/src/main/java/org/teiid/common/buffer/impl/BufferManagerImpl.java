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
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
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
import org.teiid.dqp.internal.process.DQPConfiguration;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
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
 * TODO: add detection of pinned batches to prevent unnecessary purging of non-persistent batches
 *       - this is not necessary for already persistent batches, since we hold a weak reference
 */
public class BufferManagerImpl implements BufferManager, StorageManager {
	
	private static final int TARGET_BYTES_PER_ROW = 1 << 11; //2k bytes per row
	private static final int IO_BUFFER_SIZE = 1 << 14;
	private static final int COMPACTION_THRESHOLD = 1 << 25; //start checking at 32 megs
	
	private final class CleanupHook implements org.teiid.common.buffer.BatchManager.CleanupHook {
		
		private Long id;
		private WeakReference<BatchManagerImpl> ref;
		
		CleanupHook(Long id, WeakReference<BatchManagerImpl> batchManager) {
			this.id = id;
			this.ref = batchManager;
		}
		
		public void cleanup() {
			BatchManagerImpl batchManager = ref.get();
			if (batchManager == null) {
				return;
			}
			cleanupManagedBatch(batchManager, id);
		}
		
	}
	
	private final class BatchManagerImpl implements BatchManager {
		final String id;
		volatile FileStore store;
		Map<Long, long[]> physicalMapping = new HashMap<Long, long[]>();
		long tail;
		ConcurrentSkipListSet<Long> freed = new ConcurrentSkipListSet<Long>(); 
		ReadWriteLock compactionLock = new ReentrantReadWriteLock();
		AtomicLong unusedSpace = new AtomicLong();
		private int[] lobIndexes;
		SizeUtility sizeUtility;
		private WeakReference<BatchManagerImpl> ref = new WeakReference<BatchManagerImpl>(this);

		private BatchManagerImpl(String newID, int[] lobIndexes) {
			this.id = newID;
			this.store = createFileStore(id);
			this.store.setCleanupReference(this);
			this.lobIndexes = lobIndexes;
			this.sizeUtility = new SizeUtility();
		}
		
		private void freeBatch(Long batch) {
			long[] info = physicalMapping.remove(batch);
			if (info != null) { 
				unusedSpace.addAndGet(info[1]); 
				if (info[0] + info[1] == tail) {
					tail -= info[1];
				}
			}
		}
		
		public FileStore createStorage(String prefix) {
			return createFileStore(id+prefix);
		}

		@Override
		public ManagedBatch createManagedBatch(TupleBatch batch, boolean softCache)
				throws TeiidComponentException {
			ManagedBatchImpl mbi = new ManagedBatchImpl(batch, ref, softCache);
			mbi.addToCache(false);
			persistBatchReferences();
			return mbi;
		}
		
		private long getOffset() throws TeiidComponentException {
			if (store.getLength() <= compactionThreshold || unusedSpace.get() * 4 <= store.getLength() * 3) {
				return tail;
			}
			if (LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.DETAIL)) {
				LogManager.logDetail(LogConstants.CTX_DQP, "Running full compaction on", id); //$NON-NLS-1$
			}
			byte[] buffer = new byte[IO_BUFFER_SIZE];
			TreeSet<long[]> bySize = new TreeSet<long[]>(new Comparator<long[]>() {
				@Override
				public int compare(long[] o1, long[] o2) {
					int signum = Long.signum(o1[1] - o2[1]);
					if (signum == 0) {
						//take the upper address first
						return Long.signum(o2[0] - o1[0]);
					}
					return signum;
				}
			});
			TreeSet<long[]> byAddress = new TreeSet<long[]>(new Comparator<long[]>() {
				
				@Override
				public int compare(long[] o1, long[] o2) {
					return Long.signum(o1[0] - o2[0]);
				}
			});
			bySize.addAll(physicalMapping.values());
			byAddress.addAll(physicalMapping.values());
			long lastEndAddress = 0;
			unusedSpace.set(0);
			long minFreeSpace = 1 << 11;
			while (!byAddress.isEmpty()) {
				long[] info = byAddress.pollFirst();
				bySize.remove(info);

				long currentOffset = info[0];
				long space = currentOffset - lastEndAddress;
				while (space > 0 && !bySize.isEmpty()) {
					long[] smallest = bySize.first();
					if (smallest[1] > space) {
						break;
					}
					bySize.pollFirst();
					byAddress.remove(smallest);
					move(smallest, lastEndAddress, buffer);
					space -= smallest[1];
					lastEndAddress += smallest[1];
				}
				
				if (space <= minFreeSpace) {
					unusedSpace.addAndGet(space);
				} else {
					move(info, lastEndAddress, buffer);
				}
				lastEndAddress = info[0] + info[1];
			}
			long oldLength = store.getLength();
			store.truncate(lastEndAddress);
			tail = lastEndAddress;
			if (LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.DETAIL)) {
				LogManager.logDetail(LogConstants.CTX_BUFFER_MGR, "Compacted store", id, "pre-size", oldLength, "post-size", store.getLength()); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
			}
			return tail;
		}
		
		private void move(long[] toMove, long newOffset, byte[] buffer) throws TeiidComponentException {
			long oldOffset = toMove[0];
			toMove[0] = newOffset;
			int size = (int)toMove[1];
			while (size > 0) {
				int toWrite = Math.min(IO_BUFFER_SIZE, size);
				store.readFully(oldOffset, buffer, 0, toWrite);
				store.write(newOffset, buffer, 0, toWrite);
				size -= toWrite;
				oldOffset += toWrite;
				newOffset += toWrite;
			}
		}

		@Override
		public void remove() {
			this.store.remove();
		}
		
		@Override
		public String toString() {
			return id;
		}
	}

	/**
	 * Holder for active batches
	 */
	private class TupleBufferInfo {
		TreeMap<Long, ManagedBatchImpl> batches = new TreeMap<Long, ManagedBatchImpl>();
		Long lastUsed = null;
		
		ManagedBatchImpl removeBatch(Long row) {
			ManagedBatchImpl result = batches.remove(row);
			if (result != null) {
				activeBatchKB -= result.sizeEstimate;
				if (result.softCache) {
					BatchSoftReference ref = (BatchSoftReference)result.batchReference;
					if (ref != null) {
						maxReserveKB += ref.sizeEstimate;
						ref.sizeEstimate = 0;
						ref.clear();
					}
				}
			}
			return result;
		}
	}
	
	private final class ManagedBatchImpl implements ManagedBatch {
		private boolean persistent;
		private boolean softCache;
		private volatile TupleBatch activeBatch;
		private volatile Reference<TupleBatch> batchReference;
		private WeakReference<BatchManagerImpl> managerRef;
		private Long id;
		private LobManager lobManager;
		private int sizeEstimate;
		
		public ManagedBatchImpl(TupleBatch batch, WeakReference<BatchManagerImpl> ref, boolean softCache) {
			this.softCache = softCache;
			id = batchAdded.incrementAndGet();
			this.activeBatch = batch;
			this.managerRef = ref;
			BatchManagerImpl batchManager = ref.get();
			if (batchManager.lobIndexes != null) {
				this.lobManager = new LobManager();
			}
			sizeEstimate = (int) Math.max(1, batchManager.sizeUtility.getBatchSize(batch) / 1024);
			if (LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.TRACE)) {
				LogManager.logTrace(LogConstants.CTX_BUFFER_MGR, "Add batch to BufferManager", id, "with size estimate", sizeEstimate); //$NON-NLS-1$ //$NON-NLS-2$
			}
		}
		
		@Override
		public void setPrefersMemory(boolean prefers) {
			this.softCache = prefers;
			//TODO: could recreate the reference
		}

		private void addToCache(boolean update) {
			BatchManagerImpl batchManager = managerRef.get();
			if (batchManager == null) {
				remove();
				return;
			}
			synchronized (activeBatches) {
				TupleBatch batch = this.activeBatch;
				if (batch == null) {
					return; //already removed
				}
				activeBatchKB += sizeEstimate;
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
				tbi.batches.put(this.id, this);
			}
		}

		@Override
		public TupleBatch getBatch(boolean cache, String[] types) throws TeiidComponentException {
			BatchManagerImpl batchManager = managerRef.get();
			if (batchManager == null) {
				remove();
				throw new AssertionError("Already removed"); //$NON-NLS-1$
			}
			long reads = readAttempts.incrementAndGet();
			if (LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.TRACE)) {
				LogManager.logTrace(LogConstants.CTX_BUFFER_MGR, batchManager.id, "getting batch", reads, "reference hits", referenceHit.get()); //$NON-NLS-1$ //$NON-NLS-2$
			}
			synchronized (activeBatches) {
				for (int i = 0; i < 10; i++) {
					BatchSoftReference ref = (BatchSoftReference)SOFT_QUEUE.poll();
					if (ref == null) {
						break;
					}
					maxReserveKB += ref.sizeEstimate;
					ref.sizeEstimate = 0;
					ref.clear();
				}
				TupleBufferInfo tbi = activeBatches.remove(batchManager.id);
				if (tbi != null) { 
					boolean put = true;
					if (!cache) {
						tbi.removeBatch(this.id);
						if (tbi.batches.isEmpty()) {
							put = false;
						}
					}
					if (put) {
						tbi.lastUsed = this.id;
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
				if (LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.TRACE)) {
					LogManager.logDetail(LogConstants.CTX_BUFFER_MGR, batchManager.id, id, "reading batch from disk, total reads:", count); //$NON-NLS-1$
				}
				try {
					batchManager.compactionLock.readLock().lock();
					long[] info = batchManager.physicalMapping.get(this.id);
					ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(batchManager.store.createInputStream(info[0]), IO_BUFFER_SIZE));
		            batch = new TupleBatch();
		            batch.setRowOffset(ois.readInt());
		            batch.setDataTypes(types);
		            batch.readExternal(ois);
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
		        	batchManager.compactionLock.readLock().unlock();
		        }
			}
		}

		public synchronized void persist() throws TeiidComponentException {
			final BatchManagerImpl batchManager = managerRef.get();
			if (batchManager == null) {
				remove();
				return;
			}
			boolean lockheld = false;
            try {
				TupleBatch batch = activeBatch;
				if (batch != null) {
					if (!persistent) {
						long count = writeCount.incrementAndGet();
						if (LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.DETAIL)) {
							LogManager.logDetail(LogConstants.CTX_BUFFER_MGR, batchManager.id, id, "writing batch to disk, total writes: ", count); //$NON-NLS-1$
						}
						if (lobManager != null) {
							for (List<?> tuple : batch.getTuples()) {
								lobManager.updateReferences(batchManager.lobIndexes, tuple);
							}
						}
						batchManager.compactionLock.writeLock().lock();
						Long free = null;
						while ((free = batchManager.freed.pollFirst()) != null) {
							batchManager.freeBatch(free);
						}
						lockheld = true;
						final long offset = batchManager.getOffset();
						ExtensibleBufferedOutputStream fsos = new ExtensibleBufferedOutputStream(new byte[IO_BUFFER_SIZE]) {
							
							@Override
							protected void flushDirect() throws IOException {
								try {
									batchManager.store.write(offset + bytesWritten, buf, 0, count);
								} catch (TeiidComponentException e) {
									throw new IOException(e);
								}
							}
						};
			            ObjectOutputStream oos = new ObjectOutputStream(fsos);
			            oos.writeInt(batch.getBeginRow());
			            batch.writeExternal(oos);
			            oos.close();
			            long size = fsos.getBytesWritten();
			            long[] info = new long[] {offset, size};
			            batchManager.physicalMapping.put(this.id, info);
			            batchManager.tail = Math.max(batchManager.tail, offset + size);
						if (LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.TRACE)) {
							LogManager.logTrace(LogConstants.CTX_BUFFER_MGR, batchManager.id, id, "batch written starting at:", offset); //$NON-NLS-1$
						}
					}
					if (softCache) {
						this.batchReference = new BatchSoftReference(batch, SOFT_QUEUE, sizeEstimate);
						synchronized (activeBatches) {
							maxReserveKB -= sizeEstimate;
						}
					} else if (useWeakReferences) {
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
					batchManager.compactionLock.writeLock().unlock();
				}
			}
		}

		public void remove() {
			activeBatch = null;
			batchReference = null;
			BatchManagerImpl batchManager = managerRef.get();
			if (batchManager != null) {
				cleanupManagedBatch(batchManager, id);
			}
		}
				
		@Override
		public CleanupHook getCleanupHook() {
			return new CleanupHook(id, managerRef);
		}
		
		@Override
		public String toString() {
			return "ManagedBatch " + managerRef.get() + " " + this.id + " " + activeBatch; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		}
	}
	
	private static class BatchSoftReference extends SoftReference<TupleBatch> {

		private int sizeEstimate;
		
		public BatchSoftReference(TupleBatch referent,
				ReferenceQueue<? super TupleBatch> q, int sizeEstimate) {
			super(referent, q);
			this.sizeEstimate = sizeEstimate;
		}
	}
	
	private static ReferenceQueue<? super TupleBatch> SOFT_QUEUE = new ReferenceQueue<TupleBatch>();
	
	// Configuration 
    private int connectorBatchSize = BufferManager.DEFAULT_CONNECTOR_BATCH_SIZE;
    private int processorBatchSize = BufferManager.DEFAULT_PROCESSOR_BATCH_SIZE;
    //set to acceptable defaults for testing
    private int maxProcessingKB = 1 << 11; 
    private Integer maxProcessingKBOrig;
    private int maxReserveKB = 1 << 25;
    private volatile int reserveBatchKB;
    private int maxActivePlans = DQPConfiguration.DEFAULT_MAX_ACTIVE_PLANS; //used as a hint to set the reserveBatchKB
    private boolean useWeakReferences = true;
    private boolean inlineLobs = true;
    private int targetBytesPerRow = TARGET_BYTES_PER_ROW;
	private int compactionThreshold = COMPACTION_THRESHOLD;

    private ReentrantLock lock = new ReentrantLock(true);
    private Condition batchesFreed = lock.newCondition();
    
    private volatile int activeBatchKB = 0;
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
    public int getMaxProcessingKB() {
		return maxProcessingKB;
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
    
    public void setTargetBytesPerRow(int targetBytesPerRow) {
		this.targetBytesPerRow = targetBytesPerRow;
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
    	BatchManager batchManager = new BatchManagerImpl(newID, lobIndexes);
        TupleBuffer tupleBuffer = new TupleBuffer(batchManager, newID, elements, lobIndexes, getProcessorBatchSize(elements));
        if (LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.DETAIL)) {
        	LogManager.logDetail(LogConstants.CTX_BUFFER_MGR, "Creating TupleBuffer:", newID, elements, Arrays.toString(tupleBuffer.getTypes()), "of type", tupleSourceType); //$NON-NLS-1$ //$NON-NLS-2$
        }
    	tupleBuffer.setInlineLobs(inlineLobs);
        return tupleBuffer;
    }
    
    private void cleanupManagedBatch(BatchManagerImpl batchManager, Long id) {
		synchronized (activeBatches) {
			TupleBufferInfo tbi = activeBatches.get(batchManager.id);
			if (tbi != null && tbi.removeBatch(id) != null) {
				if (tbi.batches.isEmpty()) {
					activeBatches.remove(batchManager.id);
				}
			}
		}
		
		if (batchManager.compactionLock.writeLock().tryLock()) {
			try {
				batchManager.freeBatch(id);
			} finally {
				batchManager.compactionLock.writeLock().unlock();
			}
		} else {
			batchManager.freed.add(id);
		}
    }
    
    public STree createSTree(final List elements, String groupName, int keyLength) {
    	String newID = String.valueOf(this.tsId.getAndIncrement());
    	int[] lobIndexes = LobManager.getLobIndexes(elements);
    	BatchManager bm = new BatchManagerImpl(newID, lobIndexes);
    	BatchManager keyManager = new BatchManagerImpl(String.valueOf(this.tsId.getAndIncrement()), null);
    	int[] compareIndexes = new int[keyLength];
    	for (int i = 1; i < compareIndexes.length; i++) {
			compareIndexes[i] = i;
		}
    	if (LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.DETAIL)) {
    		LogManager.logDetail(LogConstants.CTX_BUFFER_MGR, "Creating STree:", newID); //$NON-NLS-1$
    	}
    	return new STree(keyManager, bm, new ListNestedSortComparator(compareIndexes), getProcessorBatchSize(elements), getProcessorBatchSize(elements.subList(0, keyLength)), keyLength, TupleBuffer.getTypeNames(elements));
    }

    @Override
    public FileStore createFileStore(String name) {
    	if (LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.DETAIL)) {
    		LogManager.logDetail(LogConstants.CTX_BUFFER_MGR, "Creating FileStore:", name); //$NON-NLS-1$
    	}
    	return this.diskMgr.createFileStore(name);
    }
        
    public void setMaxActivePlans(int maxActivePlans) {
		this.maxActivePlans = maxActivePlans;
	}
    
    public void setMaxProcessingKB(int maxProcessingKB) {
		this.maxProcessingKB = maxProcessingKB;
	}
    
    public void setMaxReserveKB(int maxReserveBatchKB) {
		this.maxReserveKB = maxReserveBatchKB;
	}
    
	@Override
	public void initialize() throws TeiidComponentException {
		int maxMemory = (int)Math.min(Runtime.getRuntime().maxMemory() / 1024, Integer.MAX_VALUE);
		maxMemory -= 300 * 1024; //assume 300 megs of overhead for the AS/system stuff
		if (maxReserveKB < 0) {
			this.maxReserveKB = 0;
			int one_gig = 1024 * 1024;
			if (maxMemory > one_gig) {
				//assume 75% of the memory over the first gig
				this.maxReserveKB += (int)Math.max(0, (maxMemory - one_gig) * .75);
			}
			this.maxReserveKB += Math.max(0, Math.min(one_gig, maxMemory) * .5);
    	}
		this.reserveBatchKB = this.maxReserveKB;
		if (this.maxProcessingKBOrig == null) {
			//store the config value so that we can be reinitialized (this is not a clean approach)
			this.maxProcessingKBOrig = this.maxProcessingKB;
		}
		if (this.maxProcessingKBOrig < 0) {
			this.maxProcessingKB = Math.max(Math.min(8 * processorBatchSize, Integer.MAX_VALUE), (int)(.1 * maxMemory)/maxActivePlans);
		} 
	}
	
    @Override
    public void releaseBuffers(int count) {
    	if (count < 1) {
    		return;
    	}
    	if (LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.TRACE)) {
    		LogManager.logTrace(LogConstants.CTX_BUFFER_MGR, "Releasing buffer space", count); //$NON-NLS-1$
    	}
    	lock.lock();
    	try {
	    	this.reserveBatchKB += count;
	    	batchesFreed.signalAll();
    	} finally {
    		lock.unlock();
    	}
    }	
    
    @Override
    public int reserveBuffers(int count, BufferReserveMode mode) {
    	if (LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.TRACE)) {
    		LogManager.logTrace(LogConstants.CTX_BUFFER_MGR, "Reserving buffer space", count, mode); //$NON-NLS-1$
    	}
    	lock.lock();
	    try {
	    	if (mode == BufferReserveMode.WAIT) {
	    		//don't wait for more than is available
	    		int waitCount = Math.min(count, this.maxReserveKB);
		    	while (waitCount > 0 && waitCount > this.reserveBatchKB) {
		    		try {
						batchesFreed.await(100, TimeUnit.MILLISECONDS);
					} catch (InterruptedException e) {
						throw new TeiidRuntimeException(e);
					}
					waitCount /= 2;
		    	}	
	    	}
	    	if (this.reserveBatchKB >= count || mode == BufferReserveMode.FORCE) {
		    	this.reserveBatchKB -= count;
	    		return count;
	    	}
	    	int result = Math.max(0, this.reserveBatchKB);
	    	this.reserveBatchKB -= result;
	    	return result;
	    } finally {
    		lock.unlock();
    		persistBatchReferences();
    	}
    }
    
	void persistBatchReferences() {
		if (activeBatchKB == 0 || activeBatchKB <= reserveBatchKB) {
    		int memoryCount = activeBatchKB + maxReserveKB - reserveBatchKB;
			if (DataTypeManager.isValueCacheEnabled()) {
    			if (memoryCount < maxReserveKB / 8) {
					DataTypeManager.setValueCacheEnabled(false);
				}
			} else if (memoryCount > maxReserveKB / 4) {
				DataTypeManager.setValueCacheEnabled(true);
			}
			return;
		}
		while (true) {
			ManagedBatchImpl mb = null;
			synchronized (activeBatches) {
				if (activeBatchKB == 0 || activeBatchKB < reserveBatchKB * .8) {
					break;
				}
				Iterator<TupleBufferInfo> iter = activeBatches.values().iterator();
				TupleBufferInfo tbi = iter.next();
				Map.Entry<Long, ManagedBatchImpl> entry = null;
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
				LogManager.logError(LogConstants.CTX_BUFFER_MGR, e, "Error persisting batch, attempts to read that batch later will result in an exception"); //$NON-NLS-1$
			}
		}
	}
	
	@Override
	public int getProcessorBatchSize(List<? extends Expression> schema) {
		return getSizeEstimates(schema)[0];
	}
	
	private int[] getSizeEstimates(List<? extends Expression> elements) {
		int total = 0;
		boolean isValueCacheEnabled = DataTypeManager.isValueCacheEnabled();
		for (Expression element : elements) {
			Class<?> type = element.getType();
			total += SizeUtility.getSize(isValueCacheEnabled, type);
		}
		//assume 64-bit
		total += 8*elements.size() + 36;  // column list / row overhead
		
		//nominal targetBytesPerRow but can scale up or down
		
		int totalCopy = total;
		boolean less = totalCopy < targetBytesPerRow;
		int rowCount = processorBatchSize;
		
		for (int i = 0; i < 2; i++) {
			if (less) {
				totalCopy <<= 2;
			} else {
				totalCopy >>= 2;
			}
			if (less && totalCopy > targetBytesPerRow
					|| !less && totalCopy < targetBytesPerRow) {
				break;
			}
			if (less) {
				rowCount <<= 1;
			} else {
				rowCount >>= 1;
			}
		}
		rowCount = Math.max(1, rowCount);
		total *= rowCount; 
		return new int[]{rowCount, Math.max(1, total / 1024)};
	}
	
	@Override
	public int getSchemaSize(List<? extends Expression> elements) {
		return getSizeEstimates(elements)[1];
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
			Reference<?> r = this.tupleBufferQueue.poll();
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
	
	public void setUseWeakReferences(boolean useWeakReferences) {
		this.useWeakReferences = useWeakReferences;
	}
	
	public void setInlineLobs(boolean inlineLobs) {
		this.inlineLobs = inlineLobs;
	}
	
	public void setCompactionThreshold(int compactionThreshold) {
		this.compactionThreshold = compactionThreshold;
	}
	
}
