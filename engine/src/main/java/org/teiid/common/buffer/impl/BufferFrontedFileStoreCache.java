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
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.teiid.common.buffer.AutoCleanupUtil;
import org.teiid.common.buffer.BaseCacheEntry;
import org.teiid.common.buffer.Cache;
import org.teiid.common.buffer.CacheEntry;
import org.teiid.common.buffer.FileStore;
import org.teiid.common.buffer.Serializer;
import org.teiid.common.buffer.StorageManager;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidRuntimeException;
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
 */
public class BufferFrontedFileStoreCache implements Cache, StorageManager {
	
	static final int ADDRESS_BITS = 31;
	static final int SYSTEM_MASK = 1<<ADDRESS_BITS;
	static final int BYTES_PER_BLOCK_ADDRESS = 4;
	static final int INODE_BYTES = 16*BYTES_PER_BLOCK_ADDRESS;
	static final int LOG_INODE_SIZE = 6;
	static final int DIRECT_POINTERS = 14;
	static final int EMPTY_ADDRESS = -1;
	
	//TODO allow the block size to be configurable
	static final int LOG_BLOCK_SIZE = 13;
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

		InodeBlockManager(long gid, long oid, int inode) {
			this.inode = inode;
			this.gid = gid;
			this.oid = oid;
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
				throw new TeiidRuntimeException("Max block number exceeded"); //$NON-NLS-1$
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
				if ((next = blocksInuse.getAndSetNextClearBit()) == EMPTY_ADDRESS) {
					memoryEvictionLock.readLock().unlock();
					readLocked = false;
					next = evictFromMemoryBuffer();
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
		public int freeBlock(int index, boolean steal) {
			int dataBlock = getOrUpdateDataBlockIndex(index, EMPTY_ADDRESS, Mode.UPDATE);
			if (!steal) {
				freeDataBlock(dataBlock);
			}
			return dataBlock;
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
		public int free(boolean steal) {
			if (this.inode == EMPTY_ADDRESS) {
				return EMPTY_ADDRESS;
			}
			ByteBuffer bb = getInodeBlock();
			int dataBlockToSteal = bb.getInt(0);
			int indirectIndexBlock = bb.getInt(BYTES_PER_BLOCK_ADDRESS*DIRECT_POINTERS);
			int doublyIndirectIndexBlock = bb.getInt(BYTES_PER_BLOCK_ADDRESS*(DIRECT_POINTERS+1));
			boolean freedAll = freeBlock(steal?BYTES_PER_BLOCK_ADDRESS:0, bb, DIRECT_POINTERS, true);
			if (LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.DETAIL)) {
				LogManager.logDetail(LogConstants.CTX_DQP, "freeing inode", inode, "for", gid, oid); //$NON-NLS-1$ //$NON-NLS-2$
			}
			inodesInuse.clear(inode);
			if (!freedAll || indirectIndexBlock == EMPTY_ADDRESS) {
				return steal?dataBlockToSteal:EMPTY_ADDRESS;
			}
			freedAll = freeIndirectBlock(indirectIndexBlock);
			if (!freedAll || doublyIndirectIndexBlock == EMPTY_ADDRESS) {
				return steal?dataBlockToSteal:EMPTY_ADDRESS;
			}
			bb = blockByteBuffer.getByteBuffer(doublyIndirectIndexBlock);
			freeBlock(0, bb, ADDRESSES_PER_BLOCK, false);
			freeDataBlock(doublyIndirectIndexBlock);
			return steal?dataBlockToSteal:EMPTY_ADDRESS;
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

	private static class PhysicalInfo extends BaseCacheEntry {
		int inode = EMPTY_ADDRESS;
		int block = EMPTY_ADDRESS;
		int sizeIndex = 0;
		final int memoryBlockCount;
		final Long gid;
		
		public PhysicalInfo(Long gid, Long id, int inode, int size) {
			super(id);
			this.inode = inode;
			this.gid = gid;
			this.memoryBlockCount = (size>>LOG_BLOCK_SIZE) + ((size&BLOCK_MASK)>0?1:0);
			int blocks = memoryBlockCount;
			while (blocks >= 1) {
				this.sizeIndex++;
				blocks>>=2;
			}
		}
	}
	
	double crfLamda = .0001;
	
	StorageManager storageManager;
	int maxStorageObjectSize = 1 << 23; //8MB
	private long memoryBufferSpace = 1 << 27;
	private boolean direct;
	
	int maxMemoryBlocks;
	private AtomicLong readAttempts = new AtomicLong();
	PartiallyOrderedCache<Long, PhysicalInfo> memoryBufferEntries = new PartiallyOrderedCache<Long, PhysicalInfo>(16, .75f, BufferManagerImpl.CONCURRENCY_LEVEL) {
		
		@Override
		protected void recordAccess(Long key, PhysicalInfo value, boolean initial) {
			long lastAccess = value.getLastAccess();
			value.setLastAccess(readAttempts.get());
			if (initial && lastAccess == 0) {
				return;
			}
			double orderingValue = value.getOrderingValue();
			orderingValue = computeNextOrderingValue(value.getLastAccess(), lastAccess, orderingValue);
			value.setOrderingValue(orderingValue);
		}

	};
	private Semaphore memoryWritePermits; //prevents deadlock waiting for free blocks
	ReentrantReadWriteLock memoryEvictionLock = new ReentrantReadWriteLock();
	
	private int blocks;
	private ConcurrentBitSet blocksInuse;
	private BlockByteBuffer blockByteBuffer;

	private ConcurrentBitSet inodesInuse;
	private BlockByteBuffer inodeByteBuffer;
	
	//root directory
	private ConcurrentHashMap<Long, Map<Long, PhysicalInfo>> physicalMapping = new ConcurrentHashMap<Long, Map<Long, PhysicalInfo>>(16, .75f, BufferManagerImpl.CONCURRENCY_LEVEL);
	private BlockStore[] sizeBasedStores;
	
	@Override
	public void initialize() throws TeiidComponentException {
		storageManager.initialize();
		blocks = (int) Math.min(Integer.MAX_VALUE, (memoryBufferSpace>>LOG_BLOCK_SIZE));
		inodesInuse = new ConcurrentBitSet(blocks+1, BufferManagerImpl.CONCURRENCY_LEVEL);
		blocksInuse = new ConcurrentBitSet(blocks, BufferManagerImpl.CONCURRENCY_LEVEL);
		this.blockByteBuffer = new BlockByteBuffer(30, blocks, LOG_BLOCK_SIZE, direct);
		//ensure that we'll run out of blocks first
		this.inodeByteBuffer = new BlockByteBuffer(30, blocks+1, LOG_INODE_SIZE, direct);
		memoryBufferSpace = Math.max(memoryBufferSpace, maxStorageObjectSize);
		memoryWritePermits = new Semaphore(Math.max(1, (int)Math.min(memoryBufferSpace/maxStorageObjectSize, Integer.MAX_VALUE)));
		maxMemoryBlocks = Math.min(MAX_DOUBLE_INDIRECT, maxStorageObjectSize>>LOG_BLOCK_SIZE);
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
			stores.add(new BlockStore(this.storageManager, size, 30));
			size <<=2;
		} while (size>>2 < maxStorageObjectSize);
		this.sizeBasedStores = stores.toArray(new BlockStore[stores.size()]);
	}
	
	double computeNextOrderingValue(long currentTime,
			long lastAccess, double orderingValue) {
		orderingValue = 
			//Frequency component
			orderingValue*Math.pow(1-crfLamda, currentTime - lastAccess)
			//recency component
			+ Math.pow(currentTime, crfLamda);
		return orderingValue;
	}
	
	InodeBlockManager getBlockManager(long gid, long oid, int inode) {
		return new InodeBlockManager(gid, oid, inode);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void add(CacheEntry entry, Serializer s) {
		if (LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.DETAIL)) {
			LogManager.logDetail(LogConstants.CTX_DQP, "adding object", s.getId(), entry.getId()); //$NON-NLS-1$
		}
		InodeBlockManager blockManager = null;
		boolean hasPermit = false;
		PhysicalInfo info = null;
		boolean newEntry = true;
		boolean success = false;
		try {
			Map<Long, PhysicalInfo> map = physicalMapping.get(s.getId());
			if (map == null) {
				return; //already removed
			}
			info = map.get(entry.getId());
			if (info == null) {
				if (!map.containsKey(entry.getId())) {
					return; //already removed
				}
			} else {
				newEntry = false;
				synchronized (info) {
					if (info.inode != EMPTY_ADDRESS || !shouldPlaceInMemoryBuffer(readAttempts.get(), info)) {
						success = true;
						return; 
					}
				}
			}
			memoryWritePermits.acquire();
			hasPermit = true;
			blockManager = getBlockManager(s.getId(), entry.getId(), EMPTY_ADDRESS);
			ExtensibleBufferedOutputStream fsos = new BlockOutputStream(blockManager);
            ObjectOutputStream oos = new ObjectOutputStream(fsos);
            oos.writeInt(entry.getSizeEstimate());
            s.serialize(entry.getObject(), oos);
            oos.close();
            synchronized (map) {
            	//synchronize to ensure proper cleanup from a concurrent removal 
            	if (physicalMapping.containsKey(s.getId()) && map.containsKey(entry.getId())) {
            		if (newEntry) {
	           			info = new PhysicalInfo(s.getId(), entry.getId(), blockManager.getInode(), fsos.getBytesWritten());
		                map.put(entry.getId(), info);
	            		memoryBufferEntries.put(entry.getId(), info);
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
	}

	@Override
	public CacheEntry get(Long oid, Serializer<?> serializer) throws TeiidComponentException {
		long currentTime = readAttempts.incrementAndGet();
		try {
			Map<Long, PhysicalInfo> map = physicalMapping.get(serializer.getId());
			if (map == null) {
				return null;
			}
			final PhysicalInfo info = map.get(oid);
			if (info == null) {
				return null;
			}
			CacheEntry ce = new CacheEntry(oid);
			InputStream is = null;
			synchronized (info) {
				if (info.inode != EMPTY_ADDRESS) {
					memoryBufferEntries.get(oid); //touch this entry
					if (LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.DETAIL)) {
						LogManager.logDetail(LogConstants.CTX_DQP, "Getting object at inode", info.inode, serializer.getId(), oid); //$NON-NLS-1$
					}
					BlockManager manager = getBlockManager(serializer.getId(), oid, info.inode);
					is = new BlockInputStream(manager, info.memoryBlockCount, false);
				} else if (info.block != EMPTY_ADDRESS) {
					if (LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.DETAIL)) {
						LogManager.logDetail(LogConstants.CTX_DQP, "Getting object at block", info.block, info.sizeIndex, serializer.getId(), oid); //$NON-NLS-1$
					}
					BlockStore blockStore = sizeBasedStores[info.sizeIndex];
					FileStore fs = blockStore.stores[info.block/blockStore.blocksInUse.getBitsPerSegment()];
					long blockOffset = (info.block%blockStore.blocksInUse.getBitsPerSegment())*blockStore.blockSize;
					is = fs.createInputStream(blockOffset);
					if (shouldPlaceInMemoryBuffer(currentTime, info) && this.memoryWritePermits.tryAcquire()) {
						BlockManager manager = null;
						try {
							manager = getBlockManager(info.gid, info.getId(), EMPTY_ADDRESS);
							ExtensibleBufferedOutputStream os = new BlockOutputStream(manager);
				            ObjectConverterUtil.write(os, is, -1);
							memoryBufferEntries.put(info.getId(), info);
							is = new BlockInputStream(manager, info.memoryBlockCount, false);
						} finally {
							this.memoryWritePermits.release();
						}
					} else {
						this.toString();
					}
				} else {
					return null;
				}
			}
			ObjectInputStream ois = new ObjectInputStream(is);
			ce.setSizeEstimate(ois.readInt());
			ce.setLastAccess(1);
			ce.setOrderingValue(1);
			ce.setObject(serializer.deserialize(ois));
			ce.setPersistent(true);
			return ce;
        } catch(IOException e) {
        	throw new TeiidComponentException(e, QueryPlugin.Util.getString("FileStoreageManager.error_reading", oid)); //$NON-NLS-1$
        } catch (ClassNotFoundException e) {
        	throw new TeiidComponentException(e, QueryPlugin.Util.getString("FileStoreageManager.error_reading", oid)); //$NON-NLS-1$
        }
	}

	private boolean shouldPlaceInMemoryBuffer(long currentTime, PhysicalInfo info) {
		Map.Entry<PhysicalInfo, Long> lowest = memoryBufferEntries.firstEntry();
		return lowest == null 
				|| (blocksInuse.getTotalBits() - blocksInuse.getBitsSet()) > (info.memoryBlockCount<<3)
				|| lowest.getKey().getOrderingValue() < computeNextOrderingValue(currentTime, info.getLastAccess(), info.getOrderingValue());
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
		free(id, info, false);
	}

	@Override
	public Collection<Long> removeCacheGroup(Long gid) {
		Map<Long, PhysicalInfo> map = physicalMapping.remove(gid);
		if (map == null) {
			return Collections.emptySet();
		}
		synchronized (map) {
			for (Map.Entry<Long, PhysicalInfo> entry : map.entrySet()) {
				free(entry.getKey(), entry.getValue(), false);
			}
			return map.keySet();
		}
	}
	
	int free(Long oid, PhysicalInfo info, boolean demote) {
		memoryBufferEntries.remove(oid);
		if (info == null) {
			return EMPTY_ADDRESS;
		}
		synchronized (info) {
			memoryBufferEntries.remove(oid);
			if (info.inode == EMPTY_ADDRESS) {
				return EMPTY_ADDRESS;
			}
			BlockManager bm = getBlockManager(info.gid, oid, info.inode);
			info.inode = EMPTY_ADDRESS;
			if (demote) {
				if (info.block == EMPTY_ADDRESS) {
					BlockInputStream is = new BlockInputStream(bm, info.memoryBlockCount, true);
					BlockStore blockStore = sizeBasedStores[info.sizeIndex];
					FileStore fs = blockStore.stores[info.block/blockStore.blocksInUse.getBitsPerSegment()];
					info.block = getAndSetNextClearBit(blockStore);
					long blockOffset = (info.block%blockStore.blocksInUse.getBitsPerSegment())*blockStore.blockSize;
					byte[] b = new byte[BLOCK_SIZE];
					int read = 0;
					boolean errored = false;
					while ((read = is.read(b, 0, b.length)) != -1) {
						if (!errored) {
							try {
								fs.write(blockOffset, b, 0, read);
								blockOffset+=read;
							} catch (Throwable e) {
								//just continue to free
								errored = true;
								LogManager.logError(LogConstants.CTX_DQP, e, "Error transferring block to storage " + oid); //$NON-NLS-1$
							}
						}
					}
					return is.free(true);
				}
				return bm.free(true);
			}
			bm.free(false);
			if (info.block != EMPTY_ADDRESS) {
				BlockStore blockStore = sizeBasedStores[info.sizeIndex];
				blockStore.blocksInUse.clear(info.block);
				info.block = EMPTY_ADDRESS;
			}
		}
		return EMPTY_ADDRESS;
	}
	
	static int getAndSetNextClearBit(BlockStore bs) {
		int result = bs.blocksInUse.getAndSetNextClearBit();
		if (result == -1) {
			throw new TeiidRuntimeException("Out of blocks of size " + bs.blockSize); //$NON-NLS-1$
		}
		return result;
	}
	
	/**
	 * Stop the world eviction.  Hopefully this should rarely happen.
	 * @return the stole dataBlock
	 */
	int evictFromMemoryBuffer() {
		memoryEvictionLock.writeLock().lock();
		int next = -1;
		boolean writeLocked = true;
		try {
			for (int i = 0; i < 10 && next == EMPTY_ADDRESS; i++) {
				AutoCleanupUtil.doCleanup();
				Iterator<Map.Entry<PhysicalInfo, Long>> iter = memoryBufferEntries.getEvictionQueue().entrySet().iterator();
				while ((next = blocksInuse.getAndSetNextClearBit()) == EMPTY_ADDRESS && iter.hasNext()) {
					Map.Entry<PhysicalInfo, Long> entry = iter.next();
					PhysicalInfo info = entry.getKey();
					synchronized (info) {
						if (info.inode == EMPTY_ADDRESS) {
							continue;
						}
						memoryEvictionLock.writeLock().unlock();
						writeLocked = false;
						next = free(entry.getValue(), info, true);
					}
					break;
				}
			} 
			if (next == -1) {
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
		this.memoryBufferSpace = Math.min(maxBufferSpace, 1l<<(ADDRESS_BITS+LOG_BLOCK_SIZE));
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
	
}
