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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.teiid.client.BatchSerializer;
import org.teiid.common.buffer.AutoCleanupUtil;
import org.teiid.common.buffer.BatchManager;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.Cache;
import org.teiid.common.buffer.CacheEntry;
import org.teiid.common.buffer.FileStore;
import org.teiid.common.buffer.LobManager;
import org.teiid.common.buffer.STree;
import org.teiid.common.buffer.Serializer;
import org.teiid.common.buffer.StorageManager;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.common.buffer.AutoCleanupUtil.Removable;
import org.teiid.common.buffer.LobManager.ReferenceMode;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.DataTypeManager.WeakReferenceHashedValueCache;
import org.teiid.dqp.internal.process.DQPConfiguration;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.query.processor.relational.ListNestedSortComparator;
import org.teiid.query.sql.symbol.Expression;


/**
 * <p>Default implementation of BufferManager.</p>
 * Responsible for creating/tracking TupleBuffers and providing access to the StorageManager.
 * </p>
 * 
 * TODO: add detection of pinned batches to prevent unnecessary purging of non-persistent batches
 *       - this is not necessary for already persistent batches, since we hold a weak reference
 */
public class BufferManagerImpl implements BufferManager, StorageManager {
	
	private final class BatchManagerImpl implements BatchManager, Serializer<List<? extends List<?>>> {
		final Long id;
		SizeUtility sizeUtility;
		private WeakReference<BatchManagerImpl> ref = new WeakReference<BatchManagerImpl>(this);
		AtomicBoolean prefersMemory = new AtomicBoolean();
		String[] types;
		private LobManager lobManager;

		private BatchManagerImpl(Long newID, String[] types) {
			this.id = newID;
			this.sizeUtility = new SizeUtility(types);
			this.types = types;
			cache.createCacheGroup(newID);
		}
		
		@Override
		public Long getId() {
			return id;
		}
		
		public void setLobManager(LobManager lobManager) {
			this.lobManager = lobManager;
		}
		
		@Override
		public String[] getTypes() {
			return types;
		}
		
		@Override
		public boolean prefersMemory() {
			return prefersMemory.get();
		}
		
		@Override
		public void setPrefersMemory(boolean prefers) {
			//TODO: it's only expected to move from not preferring to prefefring
			this.prefersMemory.set(prefers);
		}
		
		@Override
		public boolean useSoftCache() {
			return prefersMemory.get();
		}
		
		@Override
		public Reference<? extends BatchManager> getBatchManagerReference() {
			return ref;
		}
		
		@Override
		public Long createManagedBatch(List<? extends List<?>> batch)
				throws TeiidComponentException {
			int sizeEstimate = getSizeEstimate(batch);
			Long oid = batchAdded.getAndIncrement();
			CacheEntry ce = new CacheEntry(oid);
			ce.setObject(batch);
			ce.setSizeEstimate(sizeEstimate);
			ce.setSerializer(this.ref);
			return addCacheEntry(ce, this);
		}

		@Override
		public List<? extends List<?>> deserialize(ObjectInputStream ois)
				throws IOException, ClassNotFoundException {
			List<? extends List<?>> batch = BatchSerializer.readBatch(ois, types);
			if (lobManager != null) {
				for (List<?> list : batch) {
					try {
						lobManager.updateReferences(list, ReferenceMode.ATTACH);
					} catch (TeiidComponentException e) {
						throw new TeiidRuntimeException(e);
					}
				}
			}
			return batch;
		}
		
		@Override
		public void serialize(List<? extends List<?>> obj,
				ObjectOutputStream oos) throws IOException {
			//it's expected that the containing structure has updated the lob manager
			BatchSerializer.writeBatch(oos, types, obj);
		}
		
		public int getSizeEstimate(List<? extends List<?>> obj) {
			return (int) Math.max(1, sizeUtility.getBatchSize(obj) / 1024);
		}
		
		@SuppressWarnings("unchecked")
		@Override
		public List<List<?>> getBatch(Long batch, boolean retain)
				throws TeiidComponentException {
			cleanSoftReferences();
			long reads = readAttempts.incrementAndGet();
			if (LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.TRACE)) {
				LogManager.logTrace(LogConstants.CTX_BUFFER_MGR, id, "getting batch", batch, "total reads", reads, "reference hits", referenceHit.get()); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
			}
			CacheEntry ce = fastGet(batch, prefersMemory.get(), retain);
			if (ce != null) {
				return (List<List<?>>)(!retain?ce.nullOut():ce.getObject());
			}
			synchronized (this) {
				ce = fastGet(batch, prefersMemory.get(), retain);
				if (ce != null) {
					return (List<List<?>>)(!retain?ce.nullOut():ce.getObject());
				}
				long count = readCount.incrementAndGet();
				if (LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.TRACE)) {
					LogManager.logDetail(LogConstants.CTX_BUFFER_MGR, id, id, "reading batch", batch, "from storage, total reads:", count); //$NON-NLS-1$ //$NON-NLS-2$
				}
				ce = cache.get(batch, this);
				if (ce == null) {
					throw new AssertionError("Batch not found in storage " + batch); //$NON-NLS-1$
				}
				if (!retain) {
					cache.remove(this.id, batch);
				}
				ce.setSerializer(this.ref);
				ce.setPersistent(true);
				if (retain) {
					addMemoryEntry(ce);
				}
			}	
			return (List<List<?>>)ce.getObject();
		}
		
		@Override
		public void remove(Long batch) {
			cleanSoftReferences();
			BufferManagerImpl.this.remove(id, batch, prefersMemory.get());
		}

		@Override
		public void remove() {
			removeCacheGroup(id, prefersMemory.get());
		}

		@Override
		public String toString() {
			return id.toString();
		}
	}
	
	private static class BatchSoftReference extends SoftReference<CacheEntry> {

		private int sizeEstimate;
		private Long key;
		
		public BatchSoftReference(CacheEntry referent,
				ReferenceQueue<? super CacheEntry> q, int sizeEstimate) {
			super(referent, q);
			this.sizeEstimate = sizeEstimate;
			this.key = referent.getId();
		}
	}

	private static final int TARGET_BYTES_PER_ROW = 1 << 11; //2k bytes per row
	private static ReferenceQueue<CacheEntry> SOFT_QUEUE = new ReferenceQueue<CacheEntry>();
	
	// Configuration 
    private int connectorBatchSize = BufferManager.DEFAULT_CONNECTOR_BATCH_SIZE;
    private int processorBatchSize = BufferManager.DEFAULT_PROCESSOR_BATCH_SIZE;
    //set to acceptable defaults for testing
    private int maxProcessingKB = 1 << 11; 
    private Integer maxProcessingKBOrig;
    private AtomicInteger maxReserveKB = new AtomicInteger(1 << 18);
    private volatile int reserveBatchKB;
    private int maxActivePlans = DQPConfiguration.DEFAULT_MAX_ACTIVE_PLANS; //used as a hint to set the reserveBatchKB
    private boolean useWeakReferences = true;
    private boolean inlineLobs = true;
    private int targetBytesPerRow = TARGET_BYTES_PER_ROW;
    private int maxSoftReferences;

    private ReentrantLock lock = new ReentrantLock(true);
    private Condition batchesFreed = lock.newCondition();
    
    private AtomicInteger activeBatchKB = new AtomicInteger();
    
    //tiered memory entries.  the first tier is just a queue of adds/gets.  once accessed again, the entry moves to the tenured tier.
    private LinkedHashMap<Long, CacheEntry> memoryEntries = new LinkedHashMap<Long, CacheEntry>(16, .75f, false);
    private LinkedHashMap<Long, CacheEntry> tenuredMemoryEntries = new LinkedHashMap<Long, CacheEntry>(16, .75f, true);
    
    //limited size reference caches based upon the memory settings
    private WeakReferenceHashedValueCache<CacheEntry> weakReferenceCache; 
    private Map<Long, BatchSoftReference> softCache = Collections.synchronizedMap(new LinkedHashMap<Long, BatchSoftReference>(16, .75f, false) {
		private static final long serialVersionUID = 1L;

		protected boolean removeEldestEntry(Map.Entry<Long,BatchSoftReference> eldest) {
    		if (size() > maxSoftReferences) {
    			BatchSoftReference bsr = eldest.getValue();
    			maxReserveKB.addAndGet(bsr.sizeEstimate);
    			bsr.clear();
    			return true;
    		}
    		return false;
    	};
    });
    
    Cache cache;
    
	private Map<String, TupleReference> tupleBufferMap = new ConcurrentHashMap<String, TupleReference>();
	private ReferenceQueue<TupleBuffer> tupleBufferQueue = new ReferenceQueue<TupleBuffer>();
    
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

    @Override
    public TupleBuffer createTupleBuffer(final List elements, String groupName,
    		TupleSourceType tupleSourceType) {
    	final Long newID = this.tsId.getAndIncrement();
    	int[] lobIndexes = LobManager.getLobIndexes(elements);
    	String[] types = TupleBuffer.getTypeNames(elements);
    	BatchManagerImpl batchManager = createBatchManager(newID, types);
    	LobManager lobManager = null;
    	FileStore lobStore = null;
		if (lobIndexes != null) {
			lobStore = createFileStore(newID + "_lobs"); //$NON-NLS-1$
			lobManager = new LobManager(lobIndexes, lobStore);
			batchManager.setLobManager(lobManager);
		}
    	TupleBuffer tupleBuffer = new TupleBuffer(batchManager, String.valueOf(newID), elements, lobManager, getProcessorBatchSize(elements));
    	if (lobStore != null) {
    		AutoCleanupUtil.setCleanupReference(batchManager, lobStore);
    	}
        if (LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.DETAIL)) {
        	LogManager.logDetail(LogConstants.CTX_BUFFER_MGR, "Creating TupleBuffer:", newID, elements, Arrays.toString(types), "of type", tupleSourceType); //$NON-NLS-1$ //$NON-NLS-2$
        }
    	tupleBuffer.setInlineLobs(inlineLobs);
        return tupleBuffer;
    }
    
    public STree createSTree(final List elements, String groupName, int keyLength) {
    	Long newID = this.tsId.getAndIncrement();
    	int[] lobIndexes = LobManager.getLobIndexes(elements);
    	String[] types = TupleBuffer.getTypeNames(elements);
    	BatchManagerImpl bm = createBatchManager(newID, types);
    	LobManager lobManager = null;
    	if (lobIndexes != null) {
			lobManager = new LobManager(lobIndexes, null); //persistence is not expected yet - later we might utilize storage for out-of-line lob values
			bm.setLobManager(lobManager);
		}
    	BatchManager keyManager = createBatchManager(this.tsId.getAndIncrement(), Arrays.copyOf(types, keyLength));
    	int[] compareIndexes = new int[keyLength];
    	for (int i = 1; i < compareIndexes.length; i++) {
			compareIndexes[i] = i;
		}
    	if (LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.DETAIL)) {
    		LogManager.logDetail(LogConstants.CTX_BUFFER_MGR, "Creating STree:", newID); //$NON-NLS-1$
    	}
    	return new STree(keyManager, bm, new ListNestedSortComparator(compareIndexes), getProcessorBatchSize(elements), getProcessorBatchSize(elements.subList(0, keyLength)), keyLength, lobManager);
    }

	private BatchManagerImpl createBatchManager(final Long newID, String[] types) {
		BatchManagerImpl bm = new BatchManagerImpl(newID, types);
		final AtomicBoolean prefersMemory = bm.prefersMemory;
    	AutoCleanupUtil.setCleanupReference(bm, new Removable() {
			
			@Override
			public void remove() {
				BufferManagerImpl.this.removeCacheGroup(newID, prefersMemory.get());
			}
		});
		return bm;
	}

    @Override
    public FileStore createFileStore(String name) {
    	if (LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.DETAIL)) {
    		LogManager.logDetail(LogConstants.CTX_BUFFER_MGR, "Creating FileStore:", name); //$NON-NLS-1$
    	}
    	return this.cache.createFileStore(name);
    }
    
    public Cache getCache() {
		return cache;
	}
        
    public void setMaxActivePlans(int maxActivePlans) {
		this.maxActivePlans = maxActivePlans;
	}
    
    public void setMaxProcessingKB(int maxProcessingKB) {
		this.maxProcessingKB = maxProcessingKB;
	}
    
    public void setMaxReserveKB(int maxReserveBatchKB) {
		this.maxReserveKB.set(maxReserveBatchKB);
	}
    
	@Override
	public void initialize() throws TeiidComponentException {
		int maxMemory = (int)Math.min(Runtime.getRuntime().maxMemory() / 1024, Integer.MAX_VALUE);
		maxMemory -= 300 * 1024; //assume 300 megs of overhead for the AS/system stuff
		if (getMaxReserveKB() < 0) {
			this.setMaxReserveKB(0);
			int one_gig = 1024 * 1024;
			if (maxMemory > one_gig) {
				//assume 75% of the memory over the first gig
				this.maxReserveKB.addAndGet(((int)Math.max(0, (maxMemory - one_gig) * .75)));
			}
			this.maxReserveKB.addAndGet(((int)Math.max(0, Math.min(one_gig, maxMemory) * .5)));
    	}
		this.reserveBatchKB = this.getMaxReserveKB();
		if (this.maxProcessingKBOrig == null) {
			//store the config value so that we can be reinitialized (this is not a clean approach)
			this.maxProcessingKBOrig = this.maxProcessingKB;
		}
		if (this.maxProcessingKBOrig < 0) {
			this.maxProcessingKB = Math.max(Math.min(8 * processorBatchSize, Integer.MAX_VALUE), (int)(.1 * maxMemory)/maxActivePlans);
		} 
		int memoryBatches = (this.maxProcessingKB * maxActivePlans + this.getMaxReserveKB()) / (processorBatchSize * targetBytesPerRow / 1024);
		int logSize = 39 - Integer.numberOfLeadingZeros(memoryBatches);
		if (useWeakReferences) {
			weakReferenceCache = new WeakReferenceHashedValueCache<CacheEntry>(Math.min(20, logSize));
		}
		this.maxSoftReferences = 1 << Math.max(28, logSize+2);
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
	    		int waitCount = Math.min(count, this.getMaxReserveKB());
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
		if (activeBatchKB.get() == 0 || activeBatchKB.get() <= reserveBatchKB) {
    		int memoryCount = activeBatchKB.get() + getMaxReserveKB() - reserveBatchKB;
			if (DataTypeManager.isValueCacheEnabled()) {
    			if (memoryCount < getMaxReserveKB() / 8) {
					DataTypeManager.setValueCacheEnabled(false);
				}
			} else if (memoryCount > getMaxReserveKB() / 4) {
				DataTypeManager.setValueCacheEnabled(true);
			}
			return;
		}
		boolean first = true;
		while (true) {
			CacheEntry ce = null;
			synchronized (memoryEntries) {
				if (activeBatchKB.get() == 0 || activeBatchKB.get() < reserveBatchKB * .8) {
					break;
				}
				if (first) { //let one entry per persist cycle loose its tenure.  this helps us be more write avoident.
					first = false;
					if (!tenuredMemoryEntries.isEmpty()) {
						Iterator<Map.Entry<Long, CacheEntry>> iter = tenuredMemoryEntries.entrySet().iterator();
						Map.Entry<Long, CacheEntry> entry = iter.next();
						iter.remove();
						memoryEntries.put(entry.getKey(), entry.getValue());
					}
				}
				LinkedHashMap<Long, CacheEntry> toDrain = memoryEntries;
				if (memoryEntries.isEmpty()) {
					toDrain = tenuredMemoryEntries;
					if (tenuredMemoryEntries.isEmpty()) {
						break;
					}
				}
				Iterator<CacheEntry> iter = toDrain.values().iterator();
				ce = iter.next();
				iter.remove();
				activeBatchKB.addAndGet(-ce.getSizeEstimate());
			}
			persist(ce);
		}
	}

	void persist(CacheEntry ce) {
		Serializer<?> s = ce.getSerializer().get();
		if (s == null) {
			return;
		}
		if (!ce.isPersistent()) {
			long count = writeCount.incrementAndGet();
			if (LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.DETAIL)) {
				LogManager.logDetail(LogConstants.CTX_BUFFER_MGR, ce.getId(), "writing batch to storage, total writes: ", count); //$NON-NLS-1$
			}
			try {
				cache.add(ce, s);
			} catch (Throwable e) {
				LogManager.logError(LogConstants.CTX_BUFFER_MGR, e, "Error persisting batch, attempts to read batch "+ ce.getId() +" later will result in an exception"); //$NON-NLS-1$ //$NON-NLS-2$
			}
			ce.setPersistent(true);
		}
		if (s.useSoftCache()) {
			createSoftReference(ce);
		} else if (useWeakReferences) {
			weakReferenceCache.getValue(ce); //a get will set the value
		}
	}

	private void createSoftReference(CacheEntry ce) {
		BatchSoftReference ref = new BatchSoftReference(ce, SOFT_QUEUE, ce.getSizeEstimate()/2);
		softCache.put(ce.getId(), ref);
		maxReserveKB.addAndGet(- ce.getSizeEstimate()/2);
	}
	
	/**
	 * Get a CacheEntry without hitting the cache
	 */
	CacheEntry fastGet(Long batch, boolean prefersMemory, boolean retain) {
		CacheEntry ce = null;
		synchronized (memoryEntries) {
			if (retain) {
				ce = tenuredMemoryEntries.get(batch);
				if (ce == null) {
					ce = memoryEntries.remove(batch);
					if (ce != null) {
						tenuredMemoryEntries.put(batch, ce);
					}
				} 
			} else {
				ce = tenuredMemoryEntries.remove(batch);
				if (ce == null) {
					ce = memoryEntries.remove(batch);
				}
			}
		}
		if (ce != null) {
			if (!retain) {
				BufferManagerImpl.this.remove(ce, true);
			}
			return ce;
		}
		if (prefersMemory) {
			BatchSoftReference bsr = softCache.remove(batch);
			if (bsr != null) {
				ce = bsr.get();
				if (ce != null) {
					maxReserveKB.addAndGet(bsr.sizeEstimate);
				}
			}
		} else if (useWeakReferences) {
			ce = weakReferenceCache.getByHash(batch);
			if (ce == null || !ce.getId().equals(batch)) {
				return null;
			}
		}
		if (ce != null && ce.getObject() != null) {
			referenceHit.getAndIncrement();
			if (retain) {
				addMemoryEntry(ce);
			} else {
				BufferManagerImpl.this.remove(ce, false);
			}
			return ce;
		}
		return null;
	}
	
	void remove(Long gid, Long batch, boolean prefersMemory) {
		if (LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.TRACE)) {
			LogManager.logTrace(LogConstants.CTX_BUFFER_MGR, "Removing batch from BufferManager", batch); //$NON-NLS-1$
		}
		CacheEntry ce = fastGet(batch, prefersMemory, false);
		if (ce == null) {
			cache.remove(gid, batch);
		} else {
			ce.nullOut();
		}
	}

	private void remove(CacheEntry ce, boolean inMemory) {
		if (inMemory) {
			activeBatchKB.addAndGet(-ce.getSizeEstimate());
		}
		Serializer<?> s = ce.getSerializer().get();
		if (s != null) {
			cache.remove(s.getId(), ce.getId());
		}
	}
	
	Long addCacheEntry(CacheEntry ce, Serializer<?> s) {
		if (LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.TRACE)) {
			LogManager.logTrace(LogConstants.CTX_BUFFER_MGR, "Add batch to BufferManager", ce.getId(), "with size estimate", ce.getSizeEstimate()); //$NON-NLS-1$ //$NON-NLS-2$
		}
		cache.addToCacheGroup(s.getId(), ce.getId());
		addMemoryEntry(ce);
		return ce.getId();
	}
	
	void addMemoryEntry(CacheEntry ce) {
		persistBatchReferences();
		synchronized (memoryEntries) {
			memoryEntries.put(ce.getId(), ce);
		}
		activeBatchKB.getAndAdd(ce.getSizeEstimate());
	}
	
	void removeCacheGroup(Long id, boolean prefersMemory) {
		cleanSoftReferences();
		Collection<Long> vals = cache.removeCacheGroup(id);
		for (Long val : vals) {
			//TODO: we will unnecessarily call remove on the cache, but that should be low cost
			fastGet(val, prefersMemory, false);
		}
	}
	
	void cleanSoftReferences() {
		for (int i = 0; i < 10; i++) {
			BatchSoftReference ref = (BatchSoftReference)SOFT_QUEUE.poll();
			if (ref == null) {
				break;
			}
			softCache.remove(ref.key);
			maxReserveKB.addAndGet(ref.sizeEstimate);
			ref.clear();
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
		
		for (int i = 0; i < 3; i++) {
			if (less) {
				totalCopy <<= 1;
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

	private int getMaxReserveKB() {
		return maxReserveKB.get();
	}
	
	public void setCache(Cache cache) {
		this.cache = cache;
	}
	
}
