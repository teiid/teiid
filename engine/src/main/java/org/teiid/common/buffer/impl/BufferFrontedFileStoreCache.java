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
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.teiid.common.buffer.AutoCleanupUtil;
import org.teiid.common.buffer.BaseCacheEntry;
import org.teiid.common.buffer.Cache;
import org.teiid.common.buffer.CacheEntry;
import org.teiid.common.buffer.CacheKey;
import org.teiid.common.buffer.FileStore;
import org.teiid.common.buffer.Serializer;
import org.teiid.common.buffer.StorageManager;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.ExecutorUtils;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.query.QueryPlugin;

/**
 * Implements storage against a {@link FileStore} abstraction using a fronting
 * memory buffer with a filesystem paradigm.  All objects must go through the 
 * memory (typically off-heap) buffer so that they can be put into their appropriately 
 * sized storage bucket.
 * 
 * The memory uses a 31bit address space on top of 2^13 byte blocks.
 * 
 * Therefore there is 2^31*2^13 = 2^44 or 16 terabytes max of addressable space.
 * This is well beyond any current needs.
 * 
 * The 64 byte inode format is:
 * 14 32 bit direct block pointers
 * 1  32 bit block indirect pointer
 * 1  32 bit block doubly indirect pointer (should be rarely used)
 * 
 * This means that the maximum number of blocks available to an object is
 * 14 + (2^13)/4 + ((2^13)/4)^2 ~= 2^22
 * 
 * Thus the max serialized object size is:     2^22*(2^13)  ~= 32GB.
 * 
 * Typically the max object size will be much smaller, such as 8MB.
 * 
 * Inodes are held separately from the data/index blocks, and introduce an overhead
 * that is ~ 1/128th the size of memory buffer.
 * 
 * The filesystem stores are broken up into block specific sizes starting with 8KB.
 * 
 * The root directory "physicalMapping" is held in memory for performance.  It will grow in
 * proportion to the number of tables/tuplebuffers in use.
 * 
 * TODO: compact tail storage blocks.  there may be dangling blocks causing us to consume disk space.
 * we should at least reclaim tail space if the end block is removed.  for now we are just relying
 * on the compact option of {@link ConcurrentBitSet} to keep the blocks at the start of the
 * files.
 * 
 * The locking is as fine grained as possible to prevent contention.  See {@link PhysicalInfo} for
 * flags that are used when it is used as a lock.  It is important to not access the
 * group maps when a {@link PhysicalInfo} lock is held.
 */
public class BufferFrontedFileStoreCache implements Cache<PhysicalInfo>, StorageManager {
	
	private static final int EVICTION_SCANS = 5;

	public static final int DEFAuLT_MAX_OBJECT_SIZE = 1 << 23;
	
	static final int ADDRESS_BITS = 31;
	static final int SYSTEM_MASK = 1<<ADDRESS_BITS;
	static final int BYTES_PER_BLOCK_ADDRESS = 4;
	static final int INODE_BYTES = 16*BYTES_PER_BLOCK_ADDRESS;
	static final int LOG_INODE_SIZE = 6;
	static final int DIRECT_POINTERS = 14;
	static final int EMPTY_ADDRESS = -1;
	
	//TODO allow the block size to be configurable. 8k is a reasonable default up to a gig, but we could be more efficient with larger blocks from there.
	//the rationale for a smaller block size is to reduce internal fragmentation, which is critical when maintaining a relatively small buffer < 256MB
	static final int LOG_BLOCK_SIZE = 13;

	public static final long MAX_ADDRESSABLE_MEMORY = 1l<<(ADDRESS_BITS+LOG_BLOCK_SIZE);
	
	static final int BLOCK_SIZE = 1 << LOG_BLOCK_SIZE;
	static final int BLOCK_MASK = BLOCK_SIZE - 1;
	static final int ADDRESSES_PER_BLOCK = BLOCK_SIZE/BYTES_PER_BLOCK_ADDRESS;
	static final int MAX_INDIRECT = DIRECT_POINTERS + ADDRESSES_PER_BLOCK;
	static final int MAX_DOUBLE_INDIRECT = MAX_INDIRECT + ADDRESSES_PER_BLOCK * ADDRESSES_PER_BLOCK;
	
	private enum Mode {
		GET,
		UPDATE,
		ALLOCATE
	}
	
	private final class InodeBlockManager implements BlockManager {
		private int inode;
		private ByteBuffer inodeBuffer;
		private final long gid;
		private final long oid;
		private int blockSegment;

		InodeBlockManager(long gid, long oid, int inode) {
			this.inode = inode;
			this.gid = gid;
			this.oid = oid;
			this.blockSegment = blocksInuse.getNextSegment();
		}
		
		@Override
		public int getInode() {
			return inode;
		}

		@Override
		public ByteBuffer getBlock(int index) {
			int dataBlock = getOrUpdateDataBlockIndex(index, EMPTY_ADDRESS, Mode.GET);
			return blockByteBuffer.getByteBuffer(dataBlock);
		}
				
		private int getOrUpdateDataBlockIndex(int index, int value, Mode mode) {
			if (index >= MAX_DOUBLE_INDIRECT || (mode == Mode.ALLOCATE && index >= maxMemoryBlocks)) {
				throw new TeiidRuntimeException("Max block number exceeded.  Increase the maxStorageObjectSize to support larger storage objects.  Alternatively you could make the processor batch size smaller."); //$NON-NLS-1$
			}
			int dataBlock = 0;
			int position = 0;
			ByteBuffer info = getInodeBlock();
			if (index >= MAX_INDIRECT) {
				position = BYTES_PER_BLOCK_ADDRESS*(DIRECT_POINTERS+1);
				ByteBuffer next = updateIndirectBlockInfo(info, index, position, MAX_INDIRECT, value, mode);
				if (next != info) {
					info = next;
					//should have traversed to the secondary
					int indirectAddressBlock = (index - MAX_INDIRECT) / ADDRESSES_PER_BLOCK;
					position = indirectAddressBlock * BYTES_PER_BLOCK_ADDRESS;
					if (mode == Mode.ALLOCATE && position + BYTES_PER_BLOCK_ADDRESS < BLOCK_SIZE) {
						info.putInt(position + BYTES_PER_BLOCK_ADDRESS, EMPTY_ADDRESS);
					}
					next = updateIndirectBlockInfo(info, index, position, MAX_INDIRECT + indirectAddressBlock * ADDRESSES_PER_BLOCK,  value, mode);
					if (next != info) {
						info = next;
						position = ((index - MAX_INDIRECT)%ADDRESSES_PER_BLOCK) * BYTES_PER_BLOCK_ADDRESS;
					}
				}
			} else if (index >= DIRECT_POINTERS) {
				//indirect
				position = BYTES_PER_BLOCK_ADDRESS*DIRECT_POINTERS;
				ByteBuffer next = updateIndirectBlockInfo(info, index, position, DIRECT_POINTERS, value, mode);
				if (next != info) {
					info = next;
					position = (index - DIRECT_POINTERS) * BYTES_PER_BLOCK_ADDRESS;
				}
			} else {
				position = BYTES_PER_BLOCK_ADDRESS*index;
			}
			if (mode == Mode.ALLOCATE) {
				dataBlock = nextBlock(true);
				info.putInt(position, dataBlock);
				if (mode == Mode.ALLOCATE && position + BYTES_PER_BLOCK_ADDRESS < BLOCK_SIZE) {
					//maintain the invariant that the next pointer is empty
					info.putInt(position + BYTES_PER_BLOCK_ADDRESS, EMPTY_ADDRESS);
				}
			} else {
				dataBlock = info.getInt(position);
				if (mode == Mode.UPDATE) {
					info.putInt(position, value);
				}
			}
			return dataBlock;
		}
		
		private ByteBuffer updateIndirectBlockInfo(ByteBuffer buf, int index, int position, int cutOff, int value, Mode mode) {
			int sib_index = buf.getInt(position);
			if (index == cutOff) {
				if (mode == Mode.ALLOCATE) {
					sib_index = nextBlock(false);
					buf.putInt(position, sib_index);
				} else if (mode == Mode.UPDATE && value == EMPTY_ADDRESS) {
					freeDataBlock(sib_index);
					return buf;
				}
			}
			return blockByteBuffer.getByteBuffer(sib_index);
		}

		/**
		 * Get the next dataBlock.  When the memory buffer is full we have some
		 * book keeping to do.
		 * @return
		 */
		private int nextBlock(boolean data) {
			int next = EMPTY_ADDRESS;
			memoryEvictionLock.readLock().lock();
			boolean readLocked = true;
			try {
				if ((next = blocksInuse.getAndSetNextClearBit(blockSegment)) == EMPTY_ADDRESS) {
					memoryEvictionLock.readLock().unlock();
					readLocked = false;
					next = evictFromMemoryBuffer(true);
				}
			} finally {
				if (readLocked) {
					memoryEvictionLock.readLock().unlock();
				}
			}
			if (LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.TRACE)) {
				LogManager.logTrace(LogConstants.CTX_DQP, "Allocating", data?"data":"index", "block", next, "to", gid, oid); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
			}
			return next;
		}

		@Override
		public void freeBlock(int index) {
			int dataBlock = getOrUpdateDataBlockIndex(index, EMPTY_ADDRESS, Mode.UPDATE);
			freeDataBlock(dataBlock);
		}

		private void freeDataBlock(int dataBlock) {
			if (LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.TRACE)) {
				LogManager.logTrace(LogConstants.CTX_DQP, "freeing data block", dataBlock, "for", gid, oid); //$NON-NLS-1$ //$NON-NLS-2$
			}
			blocksInuse.clear(dataBlock);
		}
		
		private ByteBuffer getInodeBlock() {
			if (inodeBuffer == null) {
				if (inode == EMPTY_ADDRESS) {
					this.inode = inodesInuse.getAndSetNextClearBit();
					if (this.inode == -1) {
						throw new AssertionError("Out of inodes"); //$NON-NLS-1$
					}
					if (LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.DETAIL)) {
						LogManager.logDetail(LogConstants.CTX_DQP, "Allocating inode", this.inode, "to", gid, oid); //$NON-NLS-1$ //$NON-NLS-2$
					}
					ByteBuffer bb = getInodeBlock();
					bb.putInt(EMPTY_ADDRESS);
				}
				inodeBuffer = inodeByteBuffer.getByteBuffer(inode);
			}
			return inodeBuffer;
		}

		@Override
		public int free(boolean acquire) {
			if (this.inode == EMPTY_ADDRESS) {
				return EMPTY_ADDRESS;
			}
			ByteBuffer bb = getInodeBlock();
			int dataBlockToAcquire = bb.getInt(0);
			int indirectIndexBlock = bb.getInt(BYTES_PER_BLOCK_ADDRESS*DIRECT_POINTERS);
			int doublyIndirectIndexBlock = bb.getInt(BYTES_PER_BLOCK_ADDRESS*(DIRECT_POINTERS+1));
			boolean freedAll = freeBlock(acquire?BYTES_PER_BLOCK_ADDRESS:0, bb, DIRECT_POINTERS-(acquire?1:0), true);
			if (LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.DETAIL)) {
				LogManager.logDetail(LogConstants.CTX_DQP, "freeing inode", inode, "for", gid, oid); //$NON-NLS-1$ //$NON-NLS-2$
			}
			inodesInuse.clear(inode);
			if (!freedAll || indirectIndexBlock == EMPTY_ADDRESS) {
				return acquire?dataBlockToAcquire:EMPTY_ADDRESS;
			}
			freedAll = freeIndirectBlock(indirectIndexBlock);
			if (!freedAll || doublyIndirectIndexBlock == EMPTY_ADDRESS) {
				return acquire?dataBlockToAcquire:EMPTY_ADDRESS;
			}
			bb = blockByteBuffer.getByteBuffer(doublyIndirectIndexBlock);
			freeBlock(0, bb, ADDRESSES_PER_BLOCK, false);
			freeDataBlock(doublyIndirectIndexBlock);
			return acquire?dataBlockToAcquire:EMPTY_ADDRESS;
		}

		private boolean freeIndirectBlock(int indirectIndexBlock) {
			ByteBuffer bb = blockByteBuffer.getByteBuffer(indirectIndexBlock);
			boolean freedAll = freeBlock(0, bb, ADDRESSES_PER_BLOCK, true);
			freeDataBlock(indirectIndexBlock);
			return freedAll;
		}

		private boolean freeBlock(int startPosition, ByteBuffer ib, int numPointers, boolean primary) {
			ib.position(startPosition);
			for (int i = 0; i < numPointers; i++) {
				int dataBlock = ib.getInt();
				if (dataBlock == EMPTY_ADDRESS) {
					return false;
				}
				if (primary) {
					freeDataBlock(dataBlock);
				} else {
					freeIndirectBlock(dataBlock);
				}
			}
			return true;
		}

		@Override
		public ByteBuffer allocateBlock(int blockNum) {
			int dataBlock = getOrUpdateDataBlockIndex(blockNum, EMPTY_ADDRESS, Mode.ALLOCATE);
			return blockByteBuffer.getByteBuffer(dataBlock);
		}
	}

	private StorageManager storageManager;
	private int maxStorageObjectSize = DEFAuLT_MAX_OBJECT_SIZE;
	private long memoryBufferSpace = 1 << 26; //64MB
	private boolean direct;
	
	private int maxMemoryBlocks;
	private AtomicLong readAttempts = new AtomicLong();
	LrfuEvictionQueue<PhysicalInfo> memoryBufferEntries = new LrfuEvictionQueue<PhysicalInfo>(readAttempts);
	private Semaphore memoryWritePermits; //prevents deadlock waiting for free blocks
	private ReentrantReadWriteLock memoryEvictionLock = new ReentrantReadWriteLock(true);
	
	private int blocks;
	private ConcurrentBitSet blocksInuse;
	private BlockByteBuffer blockByteBuffer;

	private ConcurrentBitSet inodesInuse;
	private BlockByteBuffer inodeByteBuffer;
	
	//root directory
	private ConcurrentHashMap<Long, Map<Long, PhysicalInfo>> physicalMapping = new ConcurrentHashMap<Long, Map<Long, PhysicalInfo>>(16, .75f, BufferManagerImpl.CONCURRENCY_LEVEL);
	private BlockStore[] sizeBasedStores;
	
	private AtomicBoolean cleanerRunning = new AtomicBoolean();
	private ExecutorService asynchPool = ExecutorUtils.newFixedThreadPool(1, "FileStore Worker"); //$NON-NLS-1$
	private final Runnable cleaningTask = new Runnable() {
		
		@Override
		public void run() {
			try {
				while (lowBlocks(false)) {
					if (evictFromMemoryBuffer(false) == EMPTY_ADDRESS) {
						break;
					}
				}
			} finally {
				cleanerRunning.set(false);
			}
		}
	};
	private int cleaningThreshold;
	private int criticalCleaningThreshold;
	
	private AtomicLong storageWrites = new AtomicLong();
	private AtomicLong storageReads = new AtomicLong();
	
	@Override
	public void initialize() throws TeiidComponentException {
		storageManager.initialize();
		memoryBufferSpace = Math.max(memoryBufferSpace, maxStorageObjectSize);
		blocks = (int) Math.min(Integer.MAX_VALUE, (memoryBufferSpace>>LOG_BLOCK_SIZE));
		inodesInuse = new ConcurrentBitSet(blocks+1, BufferManagerImpl.CONCURRENCY_LEVEL);
		blocksInuse = new ConcurrentBitSet(blocks, BufferManagerImpl.CONCURRENCY_LEVEL);
		this.blockByteBuffer = new BlockByteBuffer(30, blocks, LOG_BLOCK_SIZE, direct);
		//ensure that we'll run out of blocks first
		this.inodeByteBuffer = new BlockByteBuffer(30, blocks+1, LOG_INODE_SIZE, direct);
		memoryWritePermits = new Semaphore(Math.max(1, (int)Math.min(memoryBufferSpace/maxStorageObjectSize, Integer.MAX_VALUE)));
		maxMemoryBlocks = Math.min(MAX_DOUBLE_INDIRECT, maxStorageObjectSize>>LOG_BLOCK_SIZE);
		//try to maintain enough freespace so that writers don't block in cleaning
		cleaningThreshold = Math.min(maxMemoryBlocks<<4, blocks>>1);
		criticalCleaningThreshold = Math.min(maxMemoryBlocks<<2, blocks>>2);
		//account for index pointer block overhead
		if (maxMemoryBlocks > DIRECT_POINTERS) {
			maxMemoryBlocks--;
		}
		if (maxMemoryBlocks > MAX_INDIRECT) {
			int indirect = maxMemoryBlocks-MAX_INDIRECT;
			maxMemoryBlocks -= (indirect/ADDRESSES_PER_BLOCK + (indirect%ADDRESSES_PER_BLOCK>0?1:0) + 1);
		}
		List<BlockStore> stores = new ArrayList<BlockStore>();
		int size = BLOCK_SIZE;
		do {
			if ((size>>1) >= maxStorageObjectSize) {
				size>>=1;  //adjust the last block size if needed
			}
			stores.add(new BlockStore(this.storageManager, size, 30, BufferManagerImpl.CONCURRENCY_LEVEL>>2));
			size <<=2;
		} while (size>>2 < maxStorageObjectSize);
		this.sizeBasedStores = stores.toArray(new BlockStore[stores.size()]);
	}
	
	boolean lowBlocks(boolean critical) {
		int bitsSet = blocksInuse.getBitsSet();
		return bitsSet > 0 && (blocks - bitsSet < (critical?criticalCleaningThreshold:cleaningThreshold)) && memoryBufferEntries.firstEntry(false) != null;
	}
	
	InodeBlockManager getBlockManager(long gid, long oid, int inode) {
		return new InodeBlockManager(gid, oid, inode);
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean add(CacheEntry entry, Serializer s) {
		if (LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.DETAIL)) {
			LogManager.logDetail(LogConstants.CTX_DQP, "adding object", s.getId(), entry.getId()); //$NON-NLS-1$
		}
		boolean newEntry = false;
		InodeBlockManager blockManager = null;
		boolean hasPermit = false;
		PhysicalInfo info = null;
		boolean success = false;
		try {
			Map<Long, PhysicalInfo> map = physicalMapping.get(s.getId());
			if (map == null) {
				return true; //already removed
			}
			info = map.get(entry.getId());
			if (info == null) {
				synchronized (map) {
					info = map.get(entry.getId());
					if (info == null) {
						newEntry = true;
						if (!map.containsKey(entry.getId())) {
							return true; //already removed
						}
						info = new PhysicalInfo(s.getId(), entry.getId(), EMPTY_ADDRESS);
						map.put(entry.getId(), info);
					}
				}
			}
			if (!newEntry) {
				synchronized (info) {
					if (info.inode == EMPTY_ADDRESS && info.block == EMPTY_ADDRESS) {
						return false; //someone else is responsible for adding this cache entry
					}
					if (info.evicting || info.inode != EMPTY_ADDRESS
							|| !shouldPlaceInMemoryBuffer(0, info)) {
						return true; //safe to remove from tier 1 
					}
					//second chance re-add to the cache, we assume that serialization would be faster than a disk read
				}
			}
			//proactively create freespace
			if (!cleanerRunning.get()) {
				if (lowBlocks(false) && cleanerRunning.compareAndSet(false, true)) {
					LogManager.logDetail(LogConstants.CTX_DQP, "Starting memory buffer cleaner"); //$NON-NLS-1$
					asynchPool.execute(cleaningTask);
					if (lowBlocks(true)) {
						//do a non-blocking removal before we're forced to block
						evictFromMemoryBuffer(false);
					}
				}
			} else if (lowBlocks(true)) {
				//do a non-blocking removal before we're forced to block
				evictFromMemoryBuffer(false);
			}
			memoryWritePermits.acquire();
			hasPermit = true;
			blockManager = getBlockManager(s.getId(), entry.getId(), EMPTY_ADDRESS);
			BlockOutputStream bos = new BlockOutputStream(blockManager);
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeInt(entry.getSizeEstimate());
            s.serialize(entry.getObject(), oos);
            oos.close();
        	//synchronized to ensure proper cleanup from a concurrent removal 
            synchronized (map) {
            	if (physicalMapping.containsKey(s.getId()) && map.containsKey(entry.getId())) {
        			synchronized (info) {
            			info.inode = blockManager.getInode();
            			info.setSize(bos.getBytesWritten());
                		memoryBufferEntries.touch(info, newEntry);
					}
            		success = true;
            	}
			}
		} catch (Throwable e) {
			LogManager.logError(LogConstants.CTX_BUFFER_MGR, e, "Error persisting batch, attempts to read batch "+ entry.getId() +" later will result in an exception"); //$NON-NLS-1$ //$NON-NLS-2$
		} finally {
			if (hasPermit) {
				memoryWritePermits.release();
			}
			if (!success && blockManager != null) {
				blockManager.free(false);
			}
		}
        return true;
	}
	
	@Override
	public PhysicalInfo lockForLoad(Long oid, Serializer<?> serializer) {
		Map<Long, PhysicalInfo> map = physicalMapping.get(serializer.getId());
		if (map == null) {
			return null;
		}
		PhysicalInfo info = map.get(oid);
		if (info == null) {
			return null;
		}
		synchronized (info) {
			while (info.loading) {
				try {
					info.wait();
				} catch (InterruptedException e) {
					throw new TeiidRuntimeException(e);
				}
			}
			info.loading = true;
		}
		return info;
	}
	
	@Override
	public void unlockForLoad(PhysicalInfo info) {
		if (info == null) {
			return;
		}
		synchronized (info) {
			assert info.loading;
			info.loading = false;
			info.notifyAll();
		}
	}
	
	@Override
	public CacheEntry get(PhysicalInfo info, Long oid, Serializer<?> serializer) throws TeiidComponentException {
		if (info == null) {
			return null;
		}
		long currentTime = readAttempts.incrementAndGet();
		InputStream is = null;
		boolean inStorage = false;
		try {
			synchronized (info) {
				await(info, true, false);
				if (info.inode != EMPTY_ADDRESS) {
					info.pinned = true;
					memoryBufferEntries.touch(info, false); 
					if (LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.DETAIL)) {
						LogManager.logDetail(LogConstants.CTX_DQP, "Getting object at inode", info.inode, serializer.getId(), oid); //$NON-NLS-1$
					}
					BlockManager manager = getBlockManager(serializer.getId(), oid, info.inode);
					is = new BlockInputStream(manager, info.memoryBlockCount);
				} else if (info.block != EMPTY_ADDRESS) {
					assert !info.pinned;
					inStorage = true;
					storageReads.incrementAndGet();
					if (LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.DETAIL)) {
						LogManager.logDetail(LogConstants.CTX_DQP, "Getting object at block", info.block, info.sizeIndex, serializer.getId(), oid); //$NON-NLS-1$
					}
					BlockStore blockStore = sizeBasedStores[info.sizeIndex];
					FileStore fs = blockStore.stores[info.block/blockStore.blocksInUse.getBitsPerSegment()];
					long blockOffset = (info.block%blockStore.blocksInUse.getBitsPerSegment())*blockStore.blockSize;
					is = fs.createInputStream(blockOffset, info.memoryBlockCount<<LOG_BLOCK_SIZE);
				} else {
					return null;
				}
			}
			if (inStorage && shouldPlaceInMemoryBuffer(currentTime, info) && this.memoryWritePermits.tryAcquire()) {
				BlockManager manager = null;
				boolean success = false;
				try {
					manager = getBlockManager(info.gid, info.getId(), EMPTY_ADDRESS);
					ExtensibleBufferedOutputStream os = new BlockOutputStream(manager);
		            ObjectConverterUtil.write(os, is, -1);
		            synchronized (info) {
			            info.inode = manager.getInode();
			            info.pinned = true;
						memoryBufferEntries.touch(info, false);
					}
					is = new BlockInputStream(manager, info.memoryBlockCount);
					success = true;
				} finally {
					this.memoryWritePermits.release();
					if (!success && manager != null) {
						manager.free(false);
						synchronized (info) {
							info.inode = EMPTY_ADDRESS;
						}
					}
				}
			}
			CacheEntry ce = new CacheEntry(new CacheKey(oid, 1, 1));
			ObjectInputStream ois = new ObjectInputStream(is);
			ce.setSizeEstimate(ois.readInt());
			ce.setObject(serializer.deserialize(ois));
			ce.setPersistent(true);
			return ce;
        } catch(IOException e) {
        	throw new TeiidComponentException(e, QueryPlugin.Util.getString("FileStoreageManager.error_reading", oid)); //$NON-NLS-1$
        } catch (ClassNotFoundException e) {
        	throw new TeiidComponentException(e, QueryPlugin.Util.getString("FileStoreageManager.error_reading", oid)); //$NON-NLS-1$
        } finally {
        	synchronized (info) {
				info.pinned = false;
				info.notifyAll();
			}
        }
	}

	/**
	 * Determine if an object should be in the memory buffer.
	 * Adds are indicated by a current time of 0.
	 * @param currentTime
	 * @param info
	 * @return
	 */
	private boolean shouldPlaceInMemoryBuffer(long currentTime, PhysicalInfo info) {
		PhysicalInfo lowest = memoryBufferEntries.firstEntry(false);
		CacheKey key = info.getKey();
		return (blocksInuse.getTotalBits() - blocksInuse.getBitsSet()) > (criticalCleaningThreshold + info.memoryBlockCount)
				|| (lowest != null && lowest.block != EMPTY_ADDRESS 
						&& lowest.getKey().getOrderingValue() < (currentTime>0?memoryBufferEntries.computeNextOrderingValue(currentTime, key.getLastAccess(), key.getOrderingValue()):key.getOrderingValue()));
	}
	
	@Override
	public FileStore createFileStore(String name) {
		return storageManager.createFileStore(name);
	}
	
	public void setDirect(boolean direct) {
		this.direct = direct;
	}
	
	@Override
	public void addToCacheGroup(Long gid, Long oid) {
		Map<Long, PhysicalInfo> map = physicalMapping.get(gid);
		if (map == null) {
			return;
		}
		map.put(oid, null);
	}
	
	@Override
	public void createCacheGroup(Long gid) {
		physicalMapping.put(gid, Collections.synchronizedMap(new HashMap<Long, PhysicalInfo>()));
	}
	
	@Override
	public void remove(Long gid, Long id) {
		Map<Long, PhysicalInfo> map = physicalMapping.get(gid);
		if (map == null) {
			return;
		}
		PhysicalInfo info = map.remove(id);
		free(info, false, false);
	}

	@Override
	public Collection<Long> removeCacheGroup(Long gid) {
		Map<Long, PhysicalInfo> map = physicalMapping.remove(gid);
		if (map == null) {
			return Collections.emptySet();
		}
		synchronized (map) {
			for (Map.Entry<Long, PhysicalInfo> entry : map.entrySet()) {
				free(entry.getValue(), false, false);
			}
			return map.keySet();
		}
	}
	
	/**
	 * Multi-purpose method to free memory.  Modes are:
	 * demote && !acquireDataBlock -> push out of memory buffer onto disk
	 * demote && acquireDataBlock -> push out of memory and reuse a datablock
	 * !demote -> full removal from memory and disk
	 */
	int free(PhysicalInfo info, boolean demote, boolean acquireDataBlock) {
		if (info == null) {
			return EMPTY_ADDRESS;
		}
		Long oid = info.getId();
		int result = EMPTY_ADDRESS;
		BlockManager bm = null;
		int block = EMPTY_ADDRESS;
		int memoryBlockCount;
		int sizeIndex;
		synchronized (info) {
			//if we're a demotion then the free flag was already checked and set 
			if (!demote) {
				//let a pending free finish - it would be nice if we could pre-empt
				//since we can save some work, but this should be rare enough 
				//to just block
				await(info, false, true);
				info.evicting = true;
			} else {
				assert info.evicting;
			}
			block = info.block;
			memoryBlockCount = info.memoryBlockCount;
			sizeIndex = info.sizeIndex;
			if (info.inode != EMPTY_ADDRESS) {
				bm = getBlockManager(info.gid, oid, info.inode);
			} else if (demote) {
				return EMPTY_ADDRESS;
			}
			//release the lock to perform the transfer
			//for straight removals this is a little wasteful
		}
		try {
			if (demote && block == EMPTY_ADDRESS) {
				storageWrites.getAndIncrement();
				BlockInputStream is = new BlockInputStream(bm, memoryBlockCount);
				BlockStore blockStore = sizeBasedStores[sizeIndex];
				block = getAndSetNextClearBit(blockStore);
				FileStore fs = blockStore.stores[block/blockStore.blocksInUse.getBitsPerSegment()];
				long blockOffset = (block%blockStore.blocksInUse.getBitsPerSegment())*blockStore.blockSize;
				byte[] b = new byte[BLOCK_SIZE];
				int read = 0;
				try {
					while ((read = is.read(b, 0, b.length)) != -1) {
						fs.write(blockOffset, b, 0, read);
						blockOffset+=read;
					}
				} catch (Throwable e) {
					//shouldn't happen, but we'll invalidate this write and continue
					demote = false;
					//just continue to free
					LogManager.logError(LogConstants.CTX_DQP, e, "Error transferring block to storage " + oid); //$NON-NLS-1$
				}
			}
		} finally {
			//ensure post conditions
			synchronized (info) {
				//it is possible for a read to happen while evicting.
				//that's ok, we'll just wait for it to finish
				await(info, true, false);
				info.evicting = false;
				info.notifyAll();
				assert bm == null || info.inode != EMPTY_ADDRESS;
				if (info.inode != EMPTY_ADDRESS) {
					info.inode = EMPTY_ADDRESS;
					memoryBufferEntries.remove(info);
				}
				if (block != EMPTY_ADDRESS) {
					if (demote) {
						info.block = block;
					} else {
						BlockStore blockStore = sizeBasedStores[info.sizeIndex];
						blockStore.blocksInUse.clear(info.block);
						info.block = EMPTY_ADDRESS;
					}
				}
				if (bm != null) {
					result = bm.free(acquireDataBlock);
				}
			}
		}
		return result;
	}

	private void await(PhysicalInfo info, boolean pinned, boolean evicting) {
		while ((pinned && info.pinned) || (evicting && info.evicting)) {
			try {
				info.wait();
			} catch (InterruptedException e) {
				throw new TeiidRuntimeException(e);
			}
		}
	}
	
	static int getAndSetNextClearBit(BlockStore bs) {
		int result = bs.blocksInUse.getAndSetNextClearBit();
		if (result == -1) {
			throw new TeiidRuntimeException("Out of blocks of size " + bs.blockSize); //$NON-NLS-1$
		}
		return result;
	}
	
	/**
	 * Eviction routine.  When space is exhausted data blocks are acquired from
	 * memory entries.
	 * @param acquire
	 * @return
	 */
	int evictFromMemoryBuffer(boolean acquire) {
		boolean writeLocked = false;
		int next = -1;
		try {
			for (int i = 0; i < EVICTION_SCANS && next == EMPTY_ADDRESS; i++) {
				//doing a cleanup may trigger the purging of resources
				AutoCleanupUtil.doCleanup();
				//scan the eviction queue looking for a victim
				Iterator<PhysicalInfo> iter = memoryBufferEntries.getEvictionQueue().iterator();
				while (((!acquire && lowBlocks(false)) || (acquire && (next = blocksInuse.getAndSetNextClearBit()) == EMPTY_ADDRESS)) && iter.hasNext()) {
					PhysicalInfo info = iter.next();
					synchronized (info) {
						if (info.inode == EMPTY_ADDRESS) {
							continue;
						}
						if (info.pinned || info.evicting) {
							if (!acquire || i != EVICTION_SCANS - 1) {
								continue;
							}
							if (acquire && !writeLocked) {
								//stop the world - prevent any other thread from taking a free block
								//until this one is satisfied
								memoryEvictionLock.writeLock().lock();
								writeLocked = true;
							}
							//wait for the read/eviction to be over 
							await(info, true, true);
							if (info.inode == EMPTY_ADDRESS) {
								continue;
							}
						}
						//mark as evicting early so that other evictFromMemoryCalls don't select this same entry
						info.evicting = true;
					}
					next = free(info, true, acquire);
					if (!acquire) {
						next = 0; //let the cleaner know that we made progress
					}
					break;
				}
			} 
			if (acquire && next == -1) {
				throw new AssertionError("Could not free space for pending write"); //$NON-NLS-1$
			}
		} finally {
			if (writeLocked) {
				memoryEvictionLock.writeLock().unlock();
			}
		}
		return next;
	}
	
	public void setStorageManager(StorageManager storageManager) {
		this.storageManager = storageManager;
	}
	
	public StorageManager getStorageManager() {
		return storageManager;
	}
	
	public void setMemoryBufferSpace(long maxBufferSpace) {
		this.memoryBufferSpace = Math.min(maxBufferSpace, MAX_ADDRESSABLE_MEMORY);
	}
	
	public int getInodesInUse() {
		return this.inodesInuse.getBitsSet();
	}
	
	public int getDataBlocksInUse() {
		return this.blocksInuse.getBitsSet();
	}
	
	public void setMaxStorageObjectSize(int maxStorageBlockSize) {
		this.maxStorageObjectSize = maxStorageBlockSize;
	}
	
	public long getStorageReads() {
		return storageReads.get();
	}
	
	public long getStorageWrites() {
		return storageWrites.get();
	}
	
	public long getMemoryBufferSpace() {
		return memoryBufferSpace;
	}

}

/**
 * Represents the memory buffer and storage state of an object.
 * It is important to minimize the amount of data held here.
 * Currently should be 48 bytes.
 */
final class PhysicalInfo extends BaseCacheEntry {
	final Long gid;
	//the memory inode and block count
	int inode = BufferFrontedFileStoreCache.EMPTY_ADDRESS;
	int memoryBlockCount;
	//the storage block and BlockStore index
	int block = BufferFrontedFileStoreCache.EMPTY_ADDRESS;
	byte sizeIndex = 0;
	//state flags
	boolean pinned; //indicates that the entry is being read
	boolean evicting; //indicates that the entry will be moved out of the memory buffer
	boolean loading; //used by tier 1 cache to prevent double loads
	
	public PhysicalInfo(Long gid, Long id, int inode) {
		super(new CacheKey(id, 0, 0));
		this.inode = inode;
		this.gid = gid;
	}
	
	public void setSize(int size) {
		this.memoryBlockCount = (size>>BufferFrontedFileStoreCache.LOG_BLOCK_SIZE) + ((size&BufferFrontedFileStoreCache.BLOCK_MASK)>0?1:0);
		int blocks = memoryBlockCount;
		while (blocks >= 1) {
			this.sizeIndex++;
			blocks>>=2;
		}
	}
	
}
