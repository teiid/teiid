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

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.client.BatchSerializer;
import org.teiid.client.ResizingArrayList;
import org.teiid.client.util.ExceptionUtil;
import org.teiid.common.buffer.AutoCleanupUtil;
import org.teiid.common.buffer.AutoCleanupUtil.Removable;
import org.teiid.common.buffer.BatchManager;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.Cache;
import org.teiid.common.buffer.CacheEntry;
import org.teiid.common.buffer.CacheKey;
import org.teiid.common.buffer.FileStore;
import org.teiid.common.buffer.LobManager;
import org.teiid.common.buffer.LobManager.ReferenceMode;
import org.teiid.common.buffer.STree;
import org.teiid.common.buffer.Serializer;
import org.teiid.common.buffer.StorageManager;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.DataTypeManager.WeakReferenceHashedValueCache;
import org.teiid.core.types.Streamable;
import org.teiid.core.util.Assertion;
import org.teiid.dqp.internal.process.DQPConfiguration;
import org.teiid.dqp.internal.process.RequestWorkItem;
import org.teiid.dqp.service.SessionService;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.query.QueryPlugin;
import org.teiid.query.ReplicatedObject;
import org.teiid.query.processor.relational.ListNestedSortComparator;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.util.CommandContext;
import org.teiid.query.util.Options;


/**
 * <p>Default implementation of BufferManager.
 * Responsible for creating/tracking TupleBuffers and providing access to the StorageManager.
 *
 *
 * TODO: add detection of pinned batches to prevent unnecessary purging of non-persistent batches
 *       - this is not necessary for already persistent batches, since we hold a weak reference
 *
 * TODO: add a pre-fetch for tuplebuffers or some built-in correlation logic with the queue.
 */
public class BufferManagerImpl implements BufferManager, ReplicatedObject<String>, SessionKiller {

    private static final int SYSTEM_OVERHEAD_MEGS = 150;

    /**
     * Async cleaner attempts to age out old entries and to reduce the memory size when
     * little is reserved.
     */
    private static final int MAX_READ_AGE = 1<<19;
    private static final class Cleaner extends TimerTask {
        WeakReference<BufferManagerImpl> bufferRef;
        private volatile boolean canceled;

        public Cleaner(BufferManagerImpl bufferManagerImpl) {
            this.bufferRef = new WeakReference<BufferManagerImpl>(bufferManagerImpl);
        }

        @Override
        public void run() {
            while (true) {
                BufferManagerImpl impl = this.bufferRef.get();
                if (impl == null) {
                    this.cancel();
                    return;
                }
                AutoCleanupUtil.doCleanup(false);
                impl.cleaning.set(true);
                try {
                    checkForOrphanedMemoryEntries(impl);
                    long evicted = impl.doEvictions(impl.maxProcessingBytes, true, impl.initialEvictionQueue);
                    if (evicted != 0 && LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.TRACE)) {
                        LogManager.logTrace(LogConstants.CTX_BUFFER_MGR, "Async eviction run", evicted, impl.reserveBatchBytes.get(), impl.maxReserveBytes, impl.activeBatchBytes.get()); //$NON-NLS-1$
                    }
                    if (evicted < impl.maxProcessingBytes) {
                        long secondEvicted = impl.doEvictions(impl.maxProcessingBytes/2, true, impl.evictionQueue);
                        if (secondEvicted != 0 && LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.TRACE)) {
                            LogManager.logTrace(LogConstants.CTX_BUFFER_MGR, "Async eviction run", secondEvicted, impl.reserveBatchBytes.get(), impl.maxReserveBytes, impl.activeBatchBytes.get()); //$NON-NLS-1$
                        }
                    }
                } catch (Throwable t) {
                    if (ExceptionUtil.getExceptionOfType(t, InterruptedException.class) != null) {
                        return;
                    }
                    LogManager.logDetail(LogConstants.CTX_BUFFER_MGR, t, "Exception during cleaning run"); //$NON-NLS-1$
                }
                if (canceled) {
                    return;
                }
                synchronized (this) {
                    impl.cleaning.set(false);
                    try {
                        //wait for a while before cleanning more
                        //we'll be woken up by a processing thread if needed
                        this.wait(5000);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
            }
        }

        private void checkForOrphanedMemoryEntries(BufferManagerImpl impl) {
            if (impl.memoryEntries.size() <= impl.evictionQueue.getSize() + impl.initialEvictionQueue.getSize() + CONCURRENCY_LEVEL) {
                return;
            }
            int count = 0;
            for (CacheEntry entry : impl.memoryEntries.values()) {
                boolean added = false;
                synchronized (entry) {
                    if (entry.isPersistent()) {
                        added = impl.evictionQueue.add(entry);
                    } else {
                        added = impl.initialEvictionQueue.add(entry);
                    }
                }
                if (added) {
                    count++;
                }
            }
            if (count > CONCURRENCY_LEVEL) {
                LogManager.logWarning(LogConstants.CTX_BUFFER_MGR, "Detected an unexpected number of orphaned heap cache memory entries."); //$NON-NLS-1$
            }
        }

        @Override
        public boolean cancel() {
            this.canceled = true;
            return super.cancel();
        }
    }

    private final class Remover implements Removable {
        private Long id;
        private AtomicBoolean prefersMemory;

        public Remover(Long id, AtomicBoolean prefersMemory) {
            this.id = id;
            this.prefersMemory = prefersMemory;
        }

        @Override
        public void remove() {
            removeCacheGroup(id, prefersMemory.get());
        }
    }

    /**
     * This estimate is based upon adding the value to 2/3 maps and having CacheEntry/PhysicalInfo keys
     */
    private static final long BATCH_OVERHEAD = 128;

    final class BatchManagerImpl implements BatchManager, Serializer<List<? extends List<?>>> {
        final Long id;
        SizeUtility sizeUtility;
        private WeakReference<BatchManagerImpl> ref = new WeakReference<BatchManagerImpl>(this);
        private PhantomReference<Object> cleanup;
        AtomicBoolean prefersMemory = new AtomicBoolean();
        String[] types;
        private LobManager lobManager;
        private long totalSize;
        private long currentSize;
        private long rowsSampled;
        private boolean removed;
        private boolean sizeWarning;

        private BatchManagerImpl(Long newID, Class<?>[] types) {
            this.id = newID;
            this.sizeUtility = new SizeUtility(types);
            this.types = new String[types.length];
            for (int i = 0; i < types.length; i++) {
                this.types[i] = DataTypeManager.getDataTypeName(types[i]);
            }
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
            //TODO: it's only expected to move from not preferring to preferring
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
        public Long createManagedBatch(List<? extends List<?>> batch,
                Long previous, boolean removeOld)
                throws TeiidComponentException {
            if (removed) {
                throw new TeiidComponentException(id + " has already been removed"); //$NON-NLS-1$
            }
            if (cleanup == null) {
                cache.createCacheGroup(id);
                cleanup = AutoCleanupUtil.setCleanupReference(this, new Remover(id, prefersMemory));
            }
            CacheEntry old = null;
            if (previous != null) {
                old = fastGet(previous, prefersMemory.get(), true);
                //check to see if we can reuse the existing entry
                if (removeOld) {
                    if (old != null) {
                        synchronized (old) {
                            int oldRowCount = ((List)old.getObject()).size();
                            if (!old.isPersistent() && (batch.size() > (oldRowCount>>2) && batch.size() < (oldRowCount<<1))) {
                                old.setObject(batch);
                                return previous;
                            }
                        }
                    }
                    remove(previous);
                }
            }
            int sizeEstimate = getSizeEstimate(batch);
            updateEstimates(sizeEstimate, false);
            totalSize += sizeEstimate;
            rowsSampled += batch.size();
            Long oid = batchAdded.getAndIncrement();
            CacheKey key = new CacheKey(oid, readAttempts.get(), old!=null?old.getKey().getOrderingValue():0);
            CacheEntry ce = new CacheEntry(key, sizeEstimate, batch, this.ref, false);
            if (!cache.addToCacheGroup(id, ce.getId())) {
                this.remove();
                throw new TeiidComponentException(QueryPlugin.Event.TEIID31138, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31138, id));
            }
            overheadBytes.addAndGet(BATCH_OVERHEAD);
            if (LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.TRACE)) {
                LogManager.logTrace(LogConstants.CTX_BUFFER_MGR, "Add batch to BufferManager", this.id, ce.getId(), "with size estimate", ce.getSizeEstimate()); //$NON-NLS-1$ //$NON-NLS-2$
            }
            addMemoryEntry(ce);
            return oid;
        }

        private void updateEstimates(long sizeEstimate, boolean remove) throws TeiidComponentException {
            if (remove) {
                sizeEstimate = -sizeEstimate;
            }
            currentSize += sizeEstimate;
            if (!remove && currentSize > maxBatchManagerSizeEstimate) {
                if (enforceMaxBatchManagerSizeEstimate) {
                    this.remove();
                    throw new TeiidComponentException(QueryPlugin.Event.TEIID31261, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31261, maxBatchManagerSizeEstimate, id));
                }
                if (!sizeWarning) {
                    LogManager.logWarning(LogConstants.CTX_BUFFER_MGR, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31292, maxBatchManagerSizeEstimate, id));
                    sizeWarning = true;
                }
            }
            CommandContext threadLocalContext = CommandContext.getThreadLocalContext();
            if (threadLocalContext != null) {
                long bytesUsed = threadLocalContext.getSession().addAndGetBytesUsed(sizeEstimate);
                if (!remove && bytesUsed > maxSessionBatchManagerSizeEstimate) {
                    //TODO: kill the session?
                    this.remove();
                    throw new TeiidComponentException(QueryPlugin.Event.TEIID31262, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31262, maxSessionBatchManagerSizeEstimate, threadLocalContext.getSession().getSessionId(), id));
                }
            }
        }

        @Override
        public List<? extends List<?>> deserialize(ObjectInput ois)
                throws IOException, ClassNotFoundException {
            List<? extends List<?>> batch = BatchSerializer.readBatch(ois, types);
            if (lobManager != null) {
                for (int i = batch.size() - 1; i >= 0; i--) {
                    try {
                        lobManager.updateReferences(batch.get(i), ReferenceMode.ATTACH);
                    } catch (TeiidComponentException e) {
                         throw new TeiidRuntimeException(QueryPlugin.Event.TEIID30052, e);
                    }
                }
            }
            return batch;
        }

        @Override
        public void serialize(List<? extends List<?>> obj,
                ObjectOutput oos) throws IOException {
            ResizingArrayList<?> list = null;
            if (obj instanceof ResizingArrayList<?>) {
                list = (ResizingArrayList<?>)obj;
            }
            try {
                //it's expected that the containing structure has updated the lob manager
                BatchSerializer.writeBatch(oos, types, obj);
            } catch (RuntimeException e) {
                if (ExceptionUtil.getExceptionOfType(e, ClassCastException.class) != null) {
                    throw e;
                }
                //there is a chance of a concurrent persist while modifying
                //in which case we want to swallow this exception
                if (list == null) {
                    throw e;
                }
                LogManager.logDetail(LogConstants.CTX_BUFFER_MGR, e, "Possible Concurrent Modification", id); //$NON-NLS-1$
            }
        }

        public int getSizeEstimate(List<? extends List<?>> obj) {
            return (int) Math.max(1, sizeUtility.getBatchSize(DataTypeManager.isValueCacheEnabled(), obj));
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
            if (removed) {
                throw new TeiidComponentException("Already removed " + id); //$NON-NLS-1$
            }
            CacheEntry ce = fastGet(batch, prefersMemory.get(), retain);
            if (ce != null) {
                if (!retain) {
                    updateEstimates(ce.getSizeEstimate(), true);
                }
                return (List<List<?>>)(!retain?ce.nullOut():ce.getObject());
            }
            //obtain a granular lock to prevent double memory loading
            Object o = cache.lockForLoad(batch, this);
            try {
                ce = fastGet(batch, prefersMemory.get(), retain);
                if (ce != null) {
                    if (!retain) {
                        updateEstimates(ce.getSizeEstimate(), true);
                    }
                    return (List<List<?>>)(!retain?ce.nullOut():ce.getObject());
                }
                long count = readCount.incrementAndGet();
                if (LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.DETAIL)) {
                    LogManager.logDetail(LogConstants.CTX_BUFFER_MGR, id, "reading batch", batch, "from storage, total reads:", count); //$NON-NLS-1$ //$NON-NLS-2$
                }
                ce = cache.get(o, batch, this.ref);
                if (ce == null) {
                    throw new TeiidComponentException("Batch not found in storage " + batch); //$NON-NLS-1$
                }
                if (!retain) {
                    updateEstimates(ce.getSizeEstimate(), true);
                    removeFromCache(this.id, batch);
                    persistBatchReferences(ce.getSizeEstimate());
                } else {
                    addMemoryEntry(ce);
                }
            } finally {
                cache.unlockForLoad(o);
            }
            return (List<List<?>>)ce.getObject();
        }

        @Override
        public void remove(Long batch) {
            Integer sizeEstimate = BufferManagerImpl.this.remove(id, batch, prefersMemory.get());
            if (sizeEstimate != null) {
                try {
                    updateEstimates(sizeEstimate, true);
                } catch (TeiidComponentException e) {
                }
            }
        }

        @Override
        public void remove() {
            this.removed = true;
            if (cleanup != null) {
                try {
                    updateEstimates(currentSize, true);
                } catch (TeiidComponentException e) {
                }
                removeCacheGroup(id, prefersMemory.get());
                AutoCleanupUtil.removeCleanupReference(cleanup);
                cleanup = null;
            }
        }

        @Override
        public String toString() {
            return id.toString();
        }

        @Override
        public int getRowSizeEstimate() {
            if (rowsSampled == 0) {
                return 0;
            }
            return (int)(totalSize/rowsSampled);
        }

        @Override
        public String describe(List<? extends List<?>> obj) {
            return "Batch of " + (obj==null?0:obj.size()) + " rows of " + types; //$NON-NLS-1$ //$NON-NLS-2$
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

    static final int CONCURRENCY_LEVEL = 32; //TODO: make this configurable since it is roughly the same as max active plans
    private static final int TARGET_BYTES_PER_ROW = 1 << 11; //2k bytes per row
    private static ReferenceQueue<CacheEntry> SOFT_QUEUE = new ReferenceQueue<CacheEntry>();

    // Configuration
    private int processorBatchSize = BufferManager.DEFAULT_PROCESSOR_BATCH_SIZE;
    //set to acceptable defaults for testing
    private int maxProcessingBytes = 1 << 21;
    private Integer maxProcessingBytesOrig;
    long maxReserveBytes = 1 << 28;
    AtomicLong reserveBatchBytes = new AtomicLong();
    AtomicLong overheadBytes = new AtomicLong();
    private int maxActivePlans = DQPConfiguration.DEFAULT_MAX_ACTIVE_PLANS; //used as a hint to set the reserveBatchKB
    private boolean useWeakReferences = true;
    private boolean inlineLobs = true;
    private int targetBytesPerRow = TARGET_BYTES_PER_ROW;
    private int maxSoftReferences;
    private int nominalProcessingMemoryMax = maxProcessingBytes;

    private ReentrantLock lock = new ReentrantLock();
    private Condition batchesFreed = lock.newCondition();

    AtomicLong activeBatchBytes = new AtomicLong();

    private AtomicLong readAttempts = new AtomicLong();
    //TODO: consider the size estimate in the weighting function
    LrfuEvictionQueue<CacheEntry> evictionQueue = new LrfuEvictionQueue<CacheEntry>(readAttempts);
    LrfuEvictionQueue<CacheEntry> initialEvictionQueue = new LrfuEvictionQueue<CacheEntry>(readAttempts);
    ConcurrentHashMap<Long, CacheEntry> memoryEntries = new ConcurrentHashMap<Long, CacheEntry>(16, .75f, CONCURRENCY_LEVEL);

    //limited size reference caches based upon the memory settings
    private WeakReferenceHashedValueCache<CacheEntry> weakReferenceCache;
    private Map<Long, BatchSoftReference> softCache = Collections.synchronizedMap(new LinkedHashMap<Long, BatchSoftReference>(16, .75f, false) {
        private static final long serialVersionUID = 1L;

        protected boolean removeEldestEntry(Map.Entry<Long,BatchSoftReference> eldest) {
            if (size() > maxSoftReferences) {
                BatchSoftReference bsr = eldest.getValue();
                clearSoftReference(bsr);
                return true;
            }
            return false;
        }

    });

    private Cache cache;
    private StorageManager storageManager;

    private Map<String, TupleReference> tupleBufferMap = new ConcurrentHashMap<String, TupleReference>();
    private ReferenceQueue<TupleBuffer> tupleBufferQueue = new ReferenceQueue<TupleBuffer>();

    private AtomicLong tsId = new AtomicLong();
    private AtomicLong batchAdded = new AtomicLong();
    private AtomicLong readCount = new AtomicLong();
    private AtomicLong writeCount = new AtomicLong();
    private AtomicLong referenceHit = new AtomicLong();

    private static Timer SHARED_TIMER;
    private Timer timer;

    private Cleaner cleaner;
    private AtomicBoolean cleaning = new AtomicBoolean();

    private long maxFileStoreLength = Long.MAX_VALUE;
    private long maxBatchManagerSizeEstimate = Long.MAX_VALUE;
    private boolean enforceMaxBatchManagerSizeEstimate = false;
    private long maxSessionBatchManagerSizeEstimate = Long.MAX_VALUE;

    private SessionService sessionService;

    public BufferManagerImpl() {
        this(true);
    }

    public BufferManagerImpl(boolean sharedTimer) {
        this.cleaner = new Cleaner(this);
        if (sharedTimer) {
            if (SHARED_TIMER == null) {
                SHARED_TIMER = new Timer("BufferManager Cleaner", true); //$NON-NLS-1$
            }
            timer = SHARED_TIMER;
        } else {
            timer = new Timer("BufferManager Cleaner", true); //$NON-NLS-1$
        }
        timer.schedule(cleaner, 100);
    }

    void clearSoftReference(BatchSoftReference bsr) {
        synchronized (bsr) {
            overheadBytes.addAndGet(-bsr.sizeEstimate);
            bsr.sizeEstimate = 0;
        }
        bsr.clear();
    }

    private Integer removeFromCache(Long gid, Long batch) {
        Integer result = cache.remove(gid, batch);
        if (result != null) {
            overheadBytes.addAndGet(-BATCH_OVERHEAD);
        }
        return result;
    }

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
    public int getMaxProcessingSize() {
        return maxProcessingBytes;
    }

    public long getReserveBatchBytes() {
        return reserveBatchBytes.get();
    }

    /**
     * Get processor batch size
     * @return Number of rows in a processor batch
     */
    @Override
    public int getProcessorBatchSize() {
        return this.processorBatchSize;
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
        Class<?>[] types = getTypeClasses(elements);
        BatchManagerImpl batchManager = createBatchManager(newID, types);
        LobManager lobManager = null;
        if (lobIndexes != null) {
            FileStore lobStore = createFileStore(newID + "_lobs"); //$NON-NLS-1$
            lobManager = new LobManager(lobIndexes, lobStore);
            batchManager.setLobManager(lobManager);
        }
        TupleBuffer tupleBuffer = new TupleBuffer(batchManager, String.valueOf(newID), elements, lobManager, getProcessorBatchSize(elements));
        if (LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.DETAIL)) {
            LogManager.logDetail(LogConstants.CTX_BUFFER_MGR, "Creating TupleBuffer:", newID, elements, Arrays.toString(types), "batch size", tupleBuffer.getBatchSize(), "of type", tupleSourceType); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        }
        tupleBuffer.setInlineLobs(inlineLobs);
        return tupleBuffer;
    }

    public STree createSTree(final List<? extends Expression> elements, String groupName, int keyLength) {
        Long newID = this.tsId.getAndIncrement();
        int[] lobIndexes = LobManager.getLobIndexes(elements);
        Class<?>[] types = getTypeClasses(elements);
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
            LogManager.logDetail(LogConstants.CTX_BUFFER_MGR, "Creating STree:", newID, keyLength, elements); //$NON-NLS-1$
        }
        return new STree(keyManager, bm, new ListNestedSortComparator(compareIndexes).defaultNullOrder(getOptions().getDefaultNullOrder()), getProcessorBatchSize(elements.subList(0, keyLength)), getProcessorBatchSize(elements), keyLength, lobManager);
    }

    private static Class<?>[] getTypeClasses(final List<? extends Expression> elements) {
        Class<?>[] types = new Class[elements.size()];
        for (ListIterator<? extends Expression> i = elements.listIterator(); i.hasNext();) {
            Expression expr = i.next();
            Class<?> type = expr.getType();
            Assertion.isNotNull(type);
            types[i.previousIndex()] = type;
        }
        return types;
    }

    BatchManagerImpl createBatchManager(final Long newID, Class<?>[] types) {
        return new BatchManagerImpl(newID, types);
    }

    @Override
    public FileStore createFileStore(String name) {
        if (LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.TRACE)) {
            LogManager.logTrace(LogConstants.CTX_BUFFER_MGR, "Creating FileStore:", name); //$NON-NLS-1$
        }

        FileStore delegate = this.storageManager.createFileStore(name);

        ConstrainedFileStore wrapper = new ConstrainedFileStore(delegate, this);

        wrapper.setMaxLength(this.maxFileStoreLength);
        CommandContext cc = CommandContext.getThreadLocalContext();
        if (cc != null) {
            wrapper.setSession(cc.getSession());
        }
        return wrapper;
    }

    @Override
    public long getMaxStorageSpace() {
        return this.storageManager.getMaxStorageSpace();
    }

    public Cache getCache() {
        return cache;
    }

    public void setMaxActivePlans(int maxActivePlans) {
        this.maxActivePlans = maxActivePlans;
    }

    public void setMaxProcessingKB(int maxProcessingKB) {
        if (maxProcessingKB > -1) {
            this.maxProcessingBytes = maxProcessingKB<<10;
        } else {
            this.maxProcessingBytes = -1;
        }
    }

    public void setMaxReserveKB(int maxReserveBatchKB) {
        if (maxReserveBatchKB > -1) {
            long maxReserve = ((long)maxReserveBatchKB)<<10;
            this.maxReserveBytes = maxReserve;
            this.reserveBatchBytes.set(maxReserve);
        } else {
            this.maxReserveBytes = -1;
        }
    }

    @Override
    public void initialize() throws TeiidComponentException {
        long maxMemory = Runtime.getRuntime().maxMemory();
        maxMemory = Math.max(0, maxMemory - (SYSTEM_OVERHEAD_MEGS << 20)); //assume an overhead for the AS/system stuff
        if (getMaxReserveKB() < 0) {
            this.maxReserveBytes = 0;
            int one_gig = 1 << 30;
            if (maxMemory > one_gig) {
                //assume 50% of the memory over the first gig
                this.maxReserveBytes = (long)Math.max(0, (maxMemory - one_gig) * .5);
            }
            this.maxReserveBytes += Math.max(0, Math.min(one_gig, maxMemory) * .4);
        }
        this.reserveBatchBytes.set(maxReserveBytes);
        if (this.maxProcessingBytesOrig == null) {
            //store the config value so that we can be reinitialized (this is not a clean approach)
            this.maxProcessingBytesOrig = this.maxProcessingBytes;
        }
        if (this.maxProcessingBytesOrig < 0) {
            this.maxProcessingBytes = (int)Math.min(Math.max(processorBatchSize * targetBytesPerRow * 16L, (.07 * maxMemory)/Math.pow(maxActivePlans, .8)),  Integer.MAX_VALUE);
        }
        if (this.storageManager != null) {
            long max = this.storageManager.getMaxStorageSpace();
            /* previously linearly subdividing the max filestore length
             * meant that you would need to set high amounts of storage
             * to accommodate a single large filestore. since we'll kill sessions based
             * upon memory usage, we now divide the space more loosely
             */
            this.maxFileStoreLength  = max/(long)Math.pow(Math.max(4, maxActivePlans), .6);
            //note the increase of the storage / memory buffer values here to normalize to heap estimates
            //batches in serialized form are much more compact

            this.maxBatchManagerSizeEstimate = (long)(.8*((((long)this.getMaxReserveKB())<<10) + (max<<3) + (cache.getMemoryBufferSpace()<<3))/Math.sqrt(maxActivePlans));
            if (this.options != null) {
                this.maxSessionBatchManagerSizeEstimate = this.options.getMaxSessionBufferSizeEstimate();
                this.enforceMaxBatchManagerSizeEstimate = this.options.isEnforceSingleMaxBufferSizeEstimate();
            }
            this.maxBatchManagerSizeEstimate = Math.min(maxBatchManagerSizeEstimate, maxSessionBatchManagerSizeEstimate);
        }
        //make a guess at the max number of batches
        long memoryBatches = maxMemory / (processorBatchSize * targetBytesPerRow);
        //memoryBatches represents a full batch, so assume that most will be smaller
        int logSize = 67 - Long.numberOfLeadingZeros(memoryBatches);
        if (useWeakReferences) {
            weakReferenceCache = new WeakReferenceHashedValueCache<CacheEntry>(Math.min(30, logSize));
        }
        this.maxSoftReferences = 1 << Math.min(30, logSize);
        this.nominalProcessingMemoryMax = (int)Math.max(Math.min(this.maxReserveBytes, 2*this.maxProcessingBytes), Math.min(Integer.MAX_VALUE, 2*this.maxReserveBytes/maxActivePlans));
    }

    void setNominalProcessingMemoryMax(int nominalProcessingMemoryMax) {
        this.nominalProcessingMemoryMax = nominalProcessingMemoryMax;
    }

    @Override
    public void releaseOrphanedBuffers(long count) {
        releaseBuffers(count, false);
    }

    @Override
    public void releaseBuffers(int count) {
        releaseBuffers(count, true);
    }

    private void releaseBuffers(long count, boolean updateContext) {
        if (count < 1) {
            return;
        }
        if (updateContext) {
            if (LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.TRACE)) {
                LogManager.logTrace(LogConstants.CTX_BUFFER_MGR, "Releasing buffer space", count); //$NON-NLS-1$
            }
            CommandContext context = CommandContext.getThreadLocalContext();
            if (context != null) {
                context.addAndGetReservedBuffers((int)-count);
            }
        } else {
            if (LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.INFO)) {
                LogManager.logTrace(LogConstants.CTX_BUFFER_MGR, "Releasing orphaned buffer space", count); //$NON-NLS-1$
            }
        }
        lock.lock();
        try {
            this.reserveBatchBytes.addAndGet(count);
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
        CommandContext context = CommandContext.getThreadLocalContext();
        int existing = 0;
        if (context != null) {
            existing = (int)Math.min(Integer.MAX_VALUE, context.addAndGetReservedBuffers(0));
        }
        int result = count;
        if (mode == BufferReserveMode.FORCE) {
            reserve(count, context);
        } else {
            lock.lock();
            try {
                count = Math.min(count, nominalProcessingMemoryMax - existing);
                result = noWaitReserve(count, false, context);
            } finally {
                lock.unlock();
            }
        }
        persistBatchReferences(result);
        return result;
    }

    private void reserve(int count, CommandContext context) {
        this.reserveBatchBytes.addAndGet(-count);
        if (context != null) {
            context.addAndGetReservedBuffers(count);
        }
    }

    @Override
    public int reserveBuffersBlocking(int count, long[] val, boolean force) throws BlockedException {
        if (LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.TRACE)) {
            LogManager.logTrace(LogConstants.CTX_BUFFER_MGR, "Reserving buffer space", count, force); //$NON-NLS-1$
        }
        assert count >= 0;
        if (count == 0) {
            return 0;
        }
        int result = 0;
        int count_orig = count;
        CommandContext context = CommandContext.getThreadLocalContext();
        long reserved = 0;
        if (context != null) {
            reserved = context.addAndGetReservedBuffers(0);
            //TODO: in theory we have to check the whole stack as we could be
            //issuing embedded queries back to ourselves
        }
        count = Math.min(count, (int)Math.min(Integer.MAX_VALUE, nominalProcessingMemoryMax - reserved));
        if (count_orig != count && !force) {
            return 0; //is not possible to reserve the desired amount
        }
        result = noWaitReserve(count, true, context);
        if (result == 0) {
            if (val[0]++ == 0) {
                val[1] = System.currentTimeMillis();
            }
            if (val[1] > 1) {
                long last = val[1];
                val[1] = System.currentTimeMillis();
                try {
                    lock.lock();
                    if (val[1] - last < 10) {
                        //if the time difference is too close, then wait to prevent tight spins
                        //but we can't wait too long as we don't want to thread starve the system
                        batchesFreed.await(20, TimeUnit.MILLISECONDS);
                    }
                    if ((val[0] << (force?16:18)) > count) {
                        //aging out
                        //TOOD: ideally we should be using a priority queue and better scheduling
                        if (!force) {
                            return 0;
                        }
                        reserve(count_orig, context);
                        result = count_orig;
                    } else {
                        int min = 0;
                        if (force) {
                            min = 2*count/3;
                        } else {
                            min = 4*count/5;
                        }
                        //if a sample looks good proceed
                        if (reserveBatchBytes.get() > min){
                            reserve(count_orig, context);
                            result = count_orig;
                        }
                    }
                } catch (InterruptedException e) {
                    throw new TeiidRuntimeException(e);
                } finally {
                    lock.unlock();
                }
            }
            if (result == 0) {
                if (context != null) {
                    RequestWorkItem workItem = context.getWorkItem();
                    if (workItem != null) {
                        //if we have a workitem (non-test scenario) then before
                        //throwing blocked on memory to indicate there's more work
                        workItem.moreWork();
                    }
                }
                throw BlockedException.BLOCKED_ON_MEMORY_EXCEPTION;
            }
        }
        if (force && result < count_orig) {
            reserve(count_orig - result, context);
            result = count_orig;
        }
        val[0] = 0;
        persistBatchReferences(result);
        return result;
    }

    private int noWaitReserve(int count, boolean allOrNothing, CommandContext context) {
        boolean success = false;
        for (int i = 0; !success && i < 2; i++) {
            long reserveBatch = this.reserveBatchBytes.get();
            long overhead = this.overheadBytes.get();
            long current = reserveBatch - overhead;
            if (allOrNothing) {
                if (count > current) {
                    return 0;
                }
            } else if (count > current) {
                count = (int)Math.max(0, current);
            }
            if (count == 0) {
                return 0;
            }
            if (this.reserveBatchBytes.compareAndSet(reserveBatch, reserveBatch - count)) {
                success = true;
            }
        }
        //the value is changing rapidly, but we've already potentially adjusted the value twice, so just proceed
        if (!success) {
            this.reserveBatchBytes.addAndGet(-count);
        }
        if (context != null) {
            context.addAndGetReservedBuffers(count);
        }
        return count;
    }

    void persistBatchReferences(int max) {
        if (max <= 0) {
            return;
        }
        long activeBatch = activeBatchBytes.get() + overheadBytes.get();
        long reserveBatch = reserveBatchBytes.get();
        long memoryCount = activeBatch + maxReserveBytes - reserveBatch;
        if (memoryCount <= maxReserveBytes) {
            if (DataTypeManager.USE_VALUE_CACHE && DataTypeManager.isValueCacheEnabled() && memoryCount < maxReserveBytes / 8) {
                DataTypeManager.setValueCacheEnabled(false);
            }
            return;
        } else if (DataTypeManager.USE_VALUE_CACHE) {
            DataTypeManager.setValueCacheEnabled(true);
        }
        if (cleaning.compareAndSet(false, true)) {
            synchronized (cleaner) {
                cleaner.notify();
            }
        }
        //we delay work here as there should be excess vm space, we are using an overestimate, and we want the cleaner to do the work if possible
        //TODO: track sizes held by each queue independently
        long maxToFree = Math.min(max, memoryCount - maxReserveBytes);
        LrfuEvictionQueue<CacheEntry> first = initialEvictionQueue;
        LrfuEvictionQueue<CacheEntry> second = evictionQueue;
        if (evictionQueue.getSize() > 2*initialEvictionQueue.getSize()) {
            //attempt to evict from the non-initial queue first as these should essentially be cost "free" and hopefully the reference cache can mitigate
            //the cost of rereading
            first = evictionQueue;
            second = initialEvictionQueue;
        }
        maxToFree -= doEvictions(maxToFree, false, first);
        if (maxToFree > 0) {
            maxToFree = Math.min(maxToFree, activeBatchBytes.get() + overheadBytes.get() - reserveBatchBytes.get());
            if (maxToFree > 0) {
                doEvictions(maxToFree, false, second);
            }
        }
    }

    long doEvictions(long maxToFree, boolean ageOut, LrfuEvictionQueue<CacheEntry> queue) {
        if (queue == evictionQueue) {
            maxToFree = Math.min(maxToFree, this.maxProcessingBytes);
        }
        long freed = 0;
        while (freed <= maxToFree && (
                ageOut
                || (queue == evictionQueue && activeBatchBytes.get() + overheadBytes.get() + this.maxReserveBytes/2 > reserveBatchBytes.get()) //nominal cleaning criterion
                || (queue != evictionQueue && activeBatchBytes.get() + overheadBytes.get() + 3*this.maxReserveBytes/4 > reserveBatchBytes.get()))) { //assume that basically all initial batches will need to be written out at some point
            CacheEntry ce = queue.firstEntry(!ageOut);
            if (ce == null) {
                break;
            }
            synchronized (ce) {
                if (!memoryEntries.containsKey(ce.getId())) {
                    if (!ageOut) {
                        queue.remove(ce);
                    }
                    continue; //not currently a valid eviction
                }
            }
            if (ageOut) {
                long lastAccess = ce.getKey().getLastAccess();
                long currentTime = readAttempts.get();
                long age = currentTime - lastAccess;
                if (age < MAX_READ_AGE) {
                    ageOut = false;
                    continue;
                }
                queue.remove(ce);
            }
            boolean evicted = true;
            try {
                evicted = evict(ce);
            } catch (Throwable e) {
                LogManager.logError(LogConstants.CTX_BUFFER_MGR, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30017, ce.getId() ));
            } finally {
                if (evicted) {
                    synchronized (ce) {
                        if (memoryEntries.remove(ce.getId()) != null) {
                            Serializer<?> s = ce.getSerializer();
                            if (LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.TRACE)) {
                                LogManager.logTrace(LogConstants.CTX_BUFFER_MGR, "Removing batch from heap cache", s!=null?s.getId():null, ce.getId()); //$NON-NLS-1$
                            }
                            freed += ce.getSizeEstimate();
                            long result = activeBatchBytes.addAndGet(-ce.getSizeEstimate());
                            assert result >= 0 || !LrfuEvictionQueue.isSuspectSize(activeBatchBytes);
                            queue.remove(ce); //ensures that an intervening get will still be cleaned
                        }
                    }
                }
            }
        }
        return freed;
    }

    boolean evict(CacheEntry ce) throws Exception {
        Serializer<?> s = ce.getSerializer();
        if (s == null) {
            return true;
        }
        boolean persist = false;
        synchronized (ce) {
            if (!ce.isPersistent()) {
                persist = true;
                ce.setPersistent(true);
            }
        }
        if (persist) {
            long count = writeCount.incrementAndGet();
            if (LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.DETAIL)) {
                LogManager.logDetail(LogConstants.CTX_BUFFER_MGR, s.getId(), ce.getId(), "writing batch to storage, total writes: ", count); //$NON-NLS-1$
            }
        }
        boolean result = cache.add(ce, s);
        if (s.useSoftCache()) {
            createSoftReference(ce);
        } else if (useWeakReferences) {
            weakReferenceCache.getValue(ce); //a get will set the value
        }
        return result;
    }

    private void createSoftReference(CacheEntry ce) {
        //if we don't set aside some reserve, we
        //will push the soft ref out of memory potentially too quickly
        int sizeEstimate = ce.getSizeEstimate()/2;
        BatchSoftReference ref = new BatchSoftReference(ce, SOFT_QUEUE, sizeEstimate);
        softCache.put(ce.getId(), ref);
        overheadBytes.addAndGet(sizeEstimate);
    }

    /**
     * Get a CacheEntry without hitting storage
     */
    CacheEntry fastGet(Long batch, Boolean prefersMemory, boolean retain) {
        CacheEntry ce = null;
        if (retain) {
            ce = memoryEntries.get(batch);
        } else {
            ce = memoryEntries.remove(batch);
        }
        if (ce != null) {
            synchronized (ce) {
                if (retain) {
                    //there is a minute chance the batch was evicted
                    //this call ensures that we won't leak
                    if (memoryEntries.containsKey(batch)) {
                        if (ce.isPersistent()) {
                            evictionQueue.touch(ce);
                        } else {
                            initialEvictionQueue.touch(ce);
                        }
                    }
                } else {
                    evictionQueue.remove(ce);
                    if (!ce.isPersistent()) {
                        initialEvictionQueue.remove(ce);
                    }
                }
            }
            if (!retain) {
                BufferManagerImpl.this.remove(ce, true);
            }
            return ce;
        }
        if (prefersMemory == null || prefersMemory) {
            BatchSoftReference bsr = softCache.remove(batch);
            if (bsr != null) {
                ce = bsr.get();
                if (ce != null) {
                    clearSoftReference(bsr);
                }
            }
        }
        if (ce == null && (prefersMemory == null || !prefersMemory) && useWeakReferences) {
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

    private Options options;

    private Integer remove(Long gid, Long batch, boolean prefersMemory) {
        if (LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.TRACE)) {
            LogManager.logTrace(LogConstants.CTX_BUFFER_MGR, "Removing batch from BufferManager", gid, batch); //$NON-NLS-1$
        }
        cleanSoftReferences();
        CacheEntry ce = fastGet(batch, prefersMemory, false);
        Integer result = null;
        if (ce == null) {
            result = removeFromCache(gid, batch);
        } else {
            result = ce.getSizeEstimate();
            ce.nullOut();
        }
        return result;
    }

    private void remove(CacheEntry ce, boolean inMemory) {
        Serializer<?> s = ce.getSerializer();
        if (inMemory) {
            if (LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.TRACE)) {
                LogManager.logTrace(LogConstants.CTX_BUFFER_MGR, "Removing batch from heap cache", s!=null?s.getId():null, ce.getId()); //$NON-NLS-1$
            }
            long result = activeBatchBytes.addAndGet(-ce.getSizeEstimate());
            assert result >= 0 || !LrfuEvictionQueue.isSuspectSize(activeBatchBytes);
        }
        assert !LrfuEvictionQueue.isSuspectSize(activeBatchBytes);
        if (s != null) {
            removeFromCache(s.getId(), ce.getId());
        }
    }

    void addMemoryEntry(CacheEntry ce) {
        persistBatchReferences(ce.getSizeEstimate());
        synchronized (ce) {
            memoryEntries.put(ce.getId(), ce);
            if (!ce.isPersistent()) {
                initialEvictionQueue.add(ce);
            } else {
                evictionQueue.touch(ce);
            }
        }
        activeBatchBytes.getAndAdd(ce.getSizeEstimate());
    }

    void removeCacheGroup(Long id, Boolean prefersMemory) {
        cleanSoftReferences();
        if (cache == null) {
            return; //this could be called after shutdown
        }
        Collection<Long> vals = cache.removeCacheGroup(id);
        long overhead = vals.size() * BATCH_OVERHEAD;
        overheadBytes.addAndGet(-overhead);
        if (!vals.isEmpty()) {
            for (Long val : vals) {
                //TODO: we will unnecessarily call remove on the cache, but that should be low cost
                fastGet(val, prefersMemory, false);
            }
        }
    }

    void cleanSoftReferences() {
        for (int i = 0; i < 10; i++) {
            BatchSoftReference ref = (BatchSoftReference)SOFT_QUEUE.poll();
            if (ref == null) {
                break;
            }
            softCache.remove(ref.key);
            clearSoftReference(ref);
        }
    }

    @Override
    public int getProcessorBatchSize(List<? extends Expression> schema) {
        return getSizeEstimates(schema)[0];
    }

    private int[] getSizeEstimates(List<? extends Expression> elements) {
        int total = 0;
        boolean isValueCacheEnabled = DataTypeManager.isValueCacheEnabled();
        for (int i = elements.size() - 1; i >= 0; i--) {
            Class<?> type = elements.get(i).getType();
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
        return new int[]{rowCount, Math.max(1, total)};
    }

    @Override
    public int getSchemaSize(List<? extends Expression> elements) {
        return getSizeEstimates(elements)[1];
    }

    public void shutdown() {
        this.cache.shutdown();
        this.cache = null;
        this.memoryEntries.clear();
        this.evictionQueue.getEvictionQueue().clear();
        this.initialEvictionQueue.getEvictionQueue().clear();
        this.cleaner.cancel();
        if (this.timer != SHARED_TIMER) {
            this.timer.cancel();
        }
    }

    @Override
    public void addTupleBuffer(TupleBuffer tb) {
        cleanDefunctTupleBuffers();
        this.tupleBufferMap.put(tb.getId(), new TupleReference(tb, this.tupleBufferQueue));
    }

    @Override
    public void distributeTupleBuffer(String uuid, TupleBuffer tb) {
        tb.setId(uuid);
        addTupleBuffer(tb);
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

    @Override
    public void getState(OutputStream ostream) {
    }

    @Override
    public void getState(String state_id, OutputStream ostream) {
        TupleBuffer buffer = this.getTupleBuffer(state_id);
        if (buffer != null) {
            try {
                ObjectOutputStream out = new ObjectOutputStream(ostream);
                getTupleBufferState(out, buffer);
                out.flush();
            } catch (TeiidComponentException e) {
                 throw new TeiidRuntimeException(QueryPlugin.Event.TEIID30054, e);
            } catch (IOException e) {
                 throw new TeiidRuntimeException(QueryPlugin.Event.TEIID30055, e);
            }
        }
    }

    private void getTupleBufferState(ObjectOutputStream out, TupleBuffer buffer) throws TeiidComponentException, IOException {
        out.writeLong(buffer.getRowCount());
        out.writeInt(buffer.getBatchSize());
        out.writeObject(buffer.getTypes());
        for (int row = 1; row <= buffer.getRowCount(); row+=buffer.getBatchSize()) {
            TupleBatch b = buffer.getBatch(row);
            BatchSerializer.writeBatch(out, buffer.getTypes(), b.getTuples());
        }
    }

    @Override
    public void setState(InputStream istream) {
    }

    @Override
    public void setState(String state_id, InputStream istream) {
        TupleBuffer buffer = this.getTupleBuffer(state_id);
        if (buffer == null) {
            try {
                ObjectInputStream in = new ObjectInputStream(istream);
                setTupleBufferState(state_id, in);
            } catch (IOException e) {
                 throw new TeiidRuntimeException(QueryPlugin.Event.TEIID30056, e);
            } catch(ClassNotFoundException e) {
                 throw new TeiidRuntimeException(QueryPlugin.Event.TEIID30057, e);
            } catch(TeiidComponentException e) {
                 throw new TeiidRuntimeException(QueryPlugin.Event.TEIID30058, e);
            }
        }
    }

    private void setTupleBufferState(String state_id, ObjectInputStream in) throws IOException, ClassNotFoundException, TeiidComponentException {
        long rowCount = in.readLong();
        int batchSize = in.readInt();
        String[] types = (String[])in.readObject();

        List<ElementSymbol> schema = new ArrayList<ElementSymbol>(types.length);
        for (int i = 0; i < types.length; i++) {
            ElementSymbol es = new ElementSymbol("x"); //$NON-NLS-1$
            es.setType(DataTypeManager.getDataTypeClass(types[i]));
            schema.add(es);
        }
        TupleBuffer buffer = createTupleBuffer(schema, "cached", TupleSourceType.FINAL); //$NON-NLS-1$
        buffer.setBatchSize(batchSize);
        buffer.setId(state_id);

        for (int row = 1; row <= rowCount; row+=batchSize) {
            List<List<Object>> batch = BatchSerializer.readBatch(in, types);
            for (int i = 0; i < batch.size(); i++) {
                buffer.addTuple(batch.get(i));
            }
        }
        if (buffer.getRowCount() != rowCount) {
            buffer.remove();
            throw new IOException(QueryPlugin.Util.getString("not_found_cache")); //$NON-NLS-1$
        }
        buffer.close();
        addTupleBuffer(buffer);
    }

    @Override
    public void setAddress(Serializable address) {
    }

    @Override
    public void droppedMembers(Collection<Serializable> addresses) {
    }

    public void setInlineLobs(boolean inlineLobs) {
        this.inlineLobs = inlineLobs;
    }

    public int getMaxReserveKB() {
        return (int)(maxReserveBytes>>10);
    }

    public void setCache(Cache cache) {
        this.cache = cache;
        this.storageManager = cache;
    }

    public int getMemoryCacheEntries() {
        return memoryEntries.size();
    }

    public long getActiveBatchBytes() {
        return activeBatchBytes.get();
    }

    @Override
    public boolean hasState(String stateId) {
        return this.getTupleBuffer(stateId) != null;
    }

    public long getReferenceHits() {
        return referenceHit.get();
    }

    @Override
    public void persistLob(Streamable<?> lob, FileStore store,
            byte[] bytes) throws TeiidComponentException {
        LobManager.persistLob(lob, store, bytes, inlineLobs, DataTypeManager.MAX_LOB_MEMORY_BYTES);
    }

    public void invalidCacheGroup(Long gid) {
        removeCacheGroup(gid, null);
    }

    @Override
    public void setOptions(Options options) {
        this.options = options;
    }

    @Override
    public Options getOptions() {
        if (options == null) {
            options = new Options();
        }
        return options;
    }

    public void setStorageManager(StorageManager ssm) {
        this.storageManager = ssm;
    }

    public void setMaxSessionBatchManagerSizeEstimate(
            long maxSessionBatchManagerSizeEstimate) {
        this.maxSessionBatchManagerSizeEstimate = maxSessionBatchManagerSizeEstimate;
    }

    public void setMaxBatchManagerSizeEstimate(
            long maxBatchManagerSizeEstimate) {
        this.maxBatchManagerSizeEstimate = maxBatchManagerSizeEstimate;
    }

    public void setEnforceMaxBatchManagerSizeEstimate(
            boolean enforceMaxBatchManagerSizeEstimate) {
        this.enforceMaxBatchManagerSizeEstimate = enforceMaxBatchManagerSizeEstimate;
    }

    private AtomicBoolean killing = new AtomicBoolean();

    @Override
    public boolean killLargestConsumer() {
        if (sessionService == null) {
            return false;
        }

        //this could be called from competing threads
        //we only need 1 session killed at a time
        //TODO: may need to introduce a further timeout between killings
        if (killing.compareAndSet(false, true)) {
            try {
                //this is called infrequently and the should only be on the order of
                //10's of thousands of sessions, so just sort here
                SessionMetadata toKill = sessionService.getActiveSessions().stream().max((s, s1) -> {
                    return Long.compare(s.getBytesUsed(),
                            s1.getBytesUsed());
                }).orElse(null);
                if (toKill == null) {
                    return false;
                }
                LogManager.logInfo(LogConstants.CTX_DQP, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31294, toKill.getSessionId(), toKill.getBytesUsed()));
                sessionService.terminateSession(toKill.getSessionId(), "Buffer Manager"); //$NON-NLS-1$
                return true;
            } finally {
                killing.set(false);
            }
        }
        //can't wait indefinitely as we may introduce a deadlock
        for (int i = 0; i < 10; i++) {
            if (killing.get()) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    throw new TeiidRuntimeException(e);
                }
            }
        }

        return true;
    }

    public void setSessionService(SessionService sessionService) {
        this.sessionService = sessionService;
    }

}
