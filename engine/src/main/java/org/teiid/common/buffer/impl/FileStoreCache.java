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
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import org.teiid.common.buffer.Cache;
import org.teiid.common.buffer.CacheEntry;
import org.teiid.common.buffer.FileStore;
import org.teiid.common.buffer.Serializer;
import org.teiid.common.buffer.StorageManager;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.query.QueryPlugin;

/**
 * Implements storage against a {@link FileStore} abstraction using a filesystem paradigm.
 * The filesystem uses a 31bit address space on top of 2^14 byte blocks.
 * 
 * Therefore there is 2^31*2^14 = 2^45 or 32 terabytes max of addressable space.
 * 
 * Some amount of the space is taken up by system information (inodes and use flags). 
 * This is held in a separate file.
 * 
 * The 64 byte inode format is:
 * 14 32 bit direct block pointers
 * 1  32 bit block indirect pointer
 * 1  32 bit block doubly indirect pointer
 * 
 * The data block format is:
 * data bytes | long gid | long oid | three byte int block num
 * 
 * The gid/oid are stored with the block so that defrag/compaction can be performed
 * with minimal blocking/lookups.
 * 
 * This means that the maximum number of blocks available to a "file" is
 * 14 + (2^14-18)/4 + ((2^14-18)/4)^2 ~= 2^24
 * 
 * Thus the max serialized object size is:     2^24*(2^14-18)  ~= 256GB.
 * 
 * The root directory "physicalMapping" is held in memory for performance,
 * but could itself be switched to using a block map.  It will grow in
 * proportion to the number of tables/tuplebuffers in use.
 * 
 * TODO: defragment
 * TODO: lobs could also be supported in this structure.
 */
public class FileStoreCache implements Cache, StorageManager {
	
	//TODO allow the block size to be configurable
	static final int LOG_BLOCK_SIZE = 14;
	static final int ADDRESS_BITS = 31;
	static final int SYSTEM_MASK = 1<<ADDRESS_BITS;
	static final int BLOCK_SIZE = 1 << LOG_BLOCK_SIZE;
	static final int BLOCK_MASK = BLOCK_SIZE - 1;
	static final int BLOCK_OVERHEAD = 8+8+3;
	static final int BLOCK_DATA_BYTES = BLOCK_SIZE - BLOCK_OVERHEAD;
	static final int BYTES_PER_BLOCK_ADDRESS = 4;
	static final int ADDRESSES_PER_BLOCK = BLOCK_DATA_BYTES/BYTES_PER_BLOCK_ADDRESS;
	static final int INODE_BYTES = 16*BYTES_PER_BLOCK_ADDRESS;
	static final int DIRECT_POINTERS = 14;
	static final int MAX_INDIRECT = DIRECT_POINTERS + ADDRESSES_PER_BLOCK;
	static final int MAX_DOUBLE_INDIRECT = MAX_INDIRECT + ADDRESSES_PER_BLOCK * ADDRESSES_PER_BLOCK;
	static final int EMPTY_ADDRESS = -1;

	private final class BlockOutputStream extends
			ExtensibleBufferedOutputStream {
		private final BlockManager blockManager;
		int blockNum = -1;
		BlockInfo bi;

		private BlockOutputStream(BlockManager blockManager) {
			this.blockManager = blockManager;
		}

		@Override
		protected ByteBuffer newBuffer() {
			bi = blockManager.allocateBlock(++blockNum);
			return bi.buf;
		}

		@Override
		protected void flushDirect() throws IOException {
			blockManager.updateBlock(bi);
			bi = null;
		}
	}

	private final class BitSetBlockManager implements BlockManager {
		final int offset;
		
		BitSetBlockManager(int offset) {
			this.offset = offset;
		}
		
		@Override
		public void updateBlock(BlockInfo info) {
			updatePhysicalBlock(info);
		}

		@Override
		public BlockInfo getBlock(int index) {
			return getPhysicalBlock(index + offset, true, false);
		}
		
		@Override
		public BlockInfo allocateBlock(int index) {
			return getPhysicalBlock(index + offset, true, true);
		}
		
		@Override
		public int getInode() {
			throw new AssertionError();
		}

		@Override
		public void freeBlock(int index) {
			throw new AssertionError();
		}

		@Override
		public void free() {
			throw new AssertionError();
		}

	}
	
	private enum Mode {
		GET,
		UPDATE,
		ALLOCATE
	}
	
	static final class BlockInfo {
		final boolean system;
		final int inodeOffset;
		final ByteBuffer buf;
		final int physicalAddress;
		boolean dirty;
		
		BlockInfo(boolean system, ByteBuffer ib, int index, int inodeOffset) {
			this.system = system;
			this.buf = ib;
			this.physicalAddress = index;
			this.inodeOffset = inodeOffset;
		}
	}
	
	private final class InodeBlockManager implements BlockManager {
		private final int inode;
		private final long gid;
		private final long oid;
		private int lastBlock = -1;
		boolean trackLast;

		InodeBlockManager(long gid, long oid, int inode) {
			if (inode == EMPTY_ADDRESS) {
				synchronized (inodesInuse) {
					inode = inodesInuse.nextClearBit(0);
					if (inode == -1) {
						throw new TeiidRuntimeException("no inodes available"); //$NON-NLS-1$
					}
					if (LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.TRACE)) {
						LogManager.logTrace(LogConstants.CTX_DQP, "Allocating inode", inode, "to", gid, oid); //$NON-NLS-1$ //$NON-NLS-2$
					}
					inodesInuse.set(inode, true);
				}
				int inodeBlock = inode/inodesPerBlock;
				int inodePosition = INODE_BYTES*(inode%inodesPerBlock);
				BlockInfo bb = getInodeSubBlock(inodeBlock, inodePosition);
				bb.buf.putInt(EMPTY_ADDRESS);
				updatePhysicalBlock(bb);
			}
			this.inode = inode;
			this.gid = gid;
			this.oid = oid;
		}
		
		@Override
		public int getInode() {
			return inode;
		}

		@Override
		public void updateBlock(BlockInfo info) {
			updatePhysicalBlock(info);
		}

		@Override
		public BlockInfo getBlock(int index) {
			int dataBlock = getOrUpdateDataBlockIndex(index, EMPTY_ADDRESS, Mode.GET);
			BlockInfo bb = getPhysicalBlock(dataBlock, false, false);
			bb.buf.position(0);
			bb.buf.limit(BLOCK_DATA_BYTES);
			return bb;
		}
				
		private int getOrUpdateDataBlockIndex(int index, int value, Mode mode) {
			if (index >= MAX_DOUBLE_INDIRECT) {
				throw new TeiidRuntimeException("Max block number exceeded"); //$NON-NLS-1$
			}
			int dataBlock = 0;
			int position = 0;
			int inodePosition = INODE_BYTES*(inode%inodesPerBlock);
			BlockInfo info = getInodeSubBlock(inode/inodesPerBlock, inodePosition);
			if (index >= MAX_INDIRECT) {
				position = BYTES_PER_BLOCK_ADDRESS*(DIRECT_POINTERS+1);
				BlockInfo next = updateIndirectBlockInfo(info, index, position, MAX_INDIRECT, MAX_DOUBLE_INDIRECT+1, value, mode);
				if (next != info) {
					info = next;
					//should have traversed to the secondary
					int indirectAddressBlock = (index - MAX_INDIRECT) / ADDRESSES_PER_BLOCK;
					position = indirectAddressBlock * BYTES_PER_BLOCK_ADDRESS;
					if (mode == Mode.ALLOCATE && position + BYTES_PER_BLOCK_ADDRESS < BLOCK_DATA_BYTES) {
						info.buf.putInt(position + BYTES_PER_BLOCK_ADDRESS, EMPTY_ADDRESS);
					}
					next = updateIndirectBlockInfo(info, index, position, MAX_INDIRECT + indirectAddressBlock * ADDRESSES_PER_BLOCK,  MAX_DOUBLE_INDIRECT + 2 + indirectAddressBlock, value, mode);
					if (next != info) {
						info = next;
						position = ((index - MAX_INDIRECT)%ADDRESSES_PER_BLOCK) * BYTES_PER_BLOCK_ADDRESS;
						if (mode == Mode.ALLOCATE && position + BYTES_PER_BLOCK_ADDRESS < BLOCK_DATA_BYTES) {
							info.buf.putInt(position + BYTES_PER_BLOCK_ADDRESS, EMPTY_ADDRESS);
						}
					}
				}
			} else if (index >= DIRECT_POINTERS) {
				//indirect
				position = BYTES_PER_BLOCK_ADDRESS*DIRECT_POINTERS;
				BlockInfo next = updateIndirectBlockInfo(info, index, position, DIRECT_POINTERS, MAX_DOUBLE_INDIRECT, value, mode);
				if (next != info) {
					info = next;
					position = (index - DIRECT_POINTERS) * BYTES_PER_BLOCK_ADDRESS;
					if (mode == Mode.ALLOCATE && position + BYTES_PER_BLOCK_ADDRESS < BLOCK_DATA_BYTES) {
						info.buf.putInt(position + BYTES_PER_BLOCK_ADDRESS, EMPTY_ADDRESS);
					}
				}
			} else {
				position = BYTES_PER_BLOCK_ADDRESS*index;
				if (mode == Mode.ALLOCATE) {
					info.buf.putInt(position + BYTES_PER_BLOCK_ADDRESS, EMPTY_ADDRESS);
				}
			}
			if (mode == Mode.ALLOCATE) {
				dataBlock = nextBlock();
				info.buf.putInt(position, dataBlock);
				updatePhysicalBlock(info);
			} else {
				dataBlock = info.buf.getInt(position);
				if (mode == Mode.UPDATE) {
					info.buf.putInt(position, value);
					updatePhysicalBlock(info);
				}
			}
			return dataBlock;
		}
		
		private BlockInfo updateIndirectBlockInfo(BlockInfo info, int index, int position, int cutOff, int blockId, int value, Mode mode) {
			int sib_index = info.buf.getInt(position);
			boolean newBlock = false;
			if (index == cutOff) {
				if (mode == Mode.ALLOCATE) {
					sib_index = nextBlock();
					info.buf.putInt(position, sib_index);
					updatePhysicalBlock(info);
					newBlock = true;
				} else if (mode == Mode.UPDATE && value == EMPTY_ADDRESS) {
					freeDataBlock(sib_index);
					return info;
				}
			}
			info = getPhysicalBlock(sib_index, false, false);
			if (newBlock) {
				putBlockId(blockId, info.buf);
			}
			return info;
		}

		private int nextBlock() {
			int next = -1;
			synchronized (blocksInuse) {
				next = blocksInuse.nextClearBit(lastBlock + 1);
				if (next == -1) {
					throw new TeiidRuntimeException("no freespace available"); //$NON-NLS-1$
				}
				blocksInuse.set(next, true);
			}
			if (LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.TRACE)) {
				LogManager.logTrace(LogConstants.CTX_DQP, "Allocating block", next, "to", gid, oid); //$NON-NLS-1$ //$NON-NLS-2$
			}
			if (trackLast) {
				lastBlock = next;
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
			blockCache.remove(dataBlock);
			blocksInuse.set(dataBlock, false);
		}
		
		BlockInfo getInodeSubBlock(int inodeBlock, int inodePosition) {
			BlockInfo bi = getPhysicalBlock(inodeBlock + blocksInUseBlocks + inodesInUseBlocks, true, false);
			ByteBuffer bb = bi.buf.duplicate();
			bb.position(inodePosition);
			bb.limit(inodePosition + INODE_BYTES);
			bb = bb.slice();
			return new BlockInfo(true, bb, inodeBlock + blocksInUseBlocks + inodesInUseBlocks, inodePosition);
		}

		@Override
		public void free() {
			int inodeBlock = inode/inodesPerBlock;
			int inodePosition = INODE_BYTES*(inode%inodesPerBlock);
			BlockInfo bi = getInodeSubBlock(inodeBlock, inodePosition);
			ByteBuffer ib = bi.buf;
			int indirectIndexBlock = ib.getInt(ib.position() + BYTES_PER_BLOCK_ADDRESS*DIRECT_POINTERS);
			int doublyIndirectIndexBlock = ib.getInt(ib.position() + BYTES_PER_BLOCK_ADDRESS*(DIRECT_POINTERS+1));
			boolean freedAll = freeBlock(ib, DIRECT_POINTERS, true);
			if (LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.TRACE)) {
				LogManager.logTrace(LogConstants.CTX_DQP, "freeing inode", inode, "for", gid, oid); //$NON-NLS-1$ //$NON-NLS-2$
			}
			inodesInuse.set(inode, false);
			if (!freedAll || indirectIndexBlock == EMPTY_ADDRESS) {
				return;
			}
			freedAll = freeIndirectBlock(indirectIndexBlock);
			if (!freedAll || doublyIndirectIndexBlock == EMPTY_ADDRESS) {
				return;
			}
			BlockInfo bb = getPhysicalBlock(doublyIndirectIndexBlock, false, false);
			freeBlock(bb.buf, ADDRESSES_PER_BLOCK, false);
			freeDataBlock(doublyIndirectIndexBlock);
		}

		private boolean freeIndirectBlock(int indirectIndexBlock) {
			BlockInfo bb = getPhysicalBlock(indirectIndexBlock, false, false);
			bb.buf.position(0);
			boolean freedAll = freeBlock(bb.buf, ADDRESSES_PER_BLOCK, true);
			freeDataBlock(indirectIndexBlock);
			return freedAll;
		}

		private boolean freeBlock(ByteBuffer ib, int numPointers, boolean primary) {
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
		public BlockInfo allocateBlock(int blockNum) {
			int dataBlock = getOrUpdateDataBlockIndex(blockNum, EMPTY_ADDRESS, Mode.ALLOCATE);
			BlockInfo bb = getPhysicalBlock(dataBlock, false, true);
			putBlockId(blockNum, bb.buf);
			bb.buf.position(0);
			bb.buf.limit(BLOCK_DATA_BYTES);
			return bb;
		}

		private void putBlockId(int blockNum, ByteBuffer bb) {
			bb.position(BLOCK_DATA_BYTES);
			bb.putLong(gid);
			bb.putLong(oid);
			bb.put((byte)(blockNum >> 16));
			bb.putShort((short)blockNum);
		}

	}

	private StorageManager storageManager;
	private long maxBufferSpace = FileStorageManager.DEFAULT_MAX_BUFFERSPACE;
	private int inodes;
	private int blocks;
	private int inodesPerBlock;
	private int inodesInUseBlocks;
	private int blocksInUseBlocks;
	private FileStore store;
	private FileStore systemStore;
	private BlockBitSetTree blocksInuse;
	private BlockBitSetTree inodesInuse;

	//root directory
	ConcurrentHashMap<Long, BlockClosedLongIntHashTable> physicalMapping = new ConcurrentHashMap<Long, BlockClosedLongIntHashTable>();
	
	//block caching
	int blockCacheSize = 128; //2 MB
	ReentrantLock blockLock = new ReentrantLock();
	LinkedHashMap<Integer, BlockInfo> blockCache = new LinkedHashMap<Integer, BlockInfo>() {
		private static final long serialVersionUID = -4240291744435552008L;
		BlockInfo eldestEntry = null;

		protected boolean removeEldestEntry(Map.Entry<Integer,BlockInfo> eldest) {
			if (size() > blockCacheSize) {
				BlockInfo bi = eldest.getValue();
				if (bi.dirty) {
					eldestEntry = bi;
				}
				return true;
			}
			return false;
		}
		
		public BlockInfo put(Integer key, BlockInfo value) {
			blockLock.lock();
			value.dirty = true;
			try {
				return super.put(key, value);
			} finally {
				BlockInfo toUpdate = eldestEntry;
				eldestEntry = null;
				blockLock.unlock();
				if (toUpdate != null) {
					updatePhysicalBlockDirect(toUpdate);
				}
			}
		}
		
		public BlockInfo get(Object key) {
			blockLock.lock();
			try {
				return super.get(key);
			} finally {
				blockLock.unlock();
			}
		}
		
		public BlockInfo remove(Object key) {
			blockLock.lock();
			try {
				return super.remove(key);
			} finally {
				blockLock.unlock();
			}
		}
	};
	
	BlockInfo getPhysicalBlock(int block, boolean system, boolean allocate) {
		if (block < 0) {
			throw new AssertionError("invalid block address " + block); //$NON-NLS-1$
		}
		try {
			int key = block;
			if (system) {
				key |= SYSTEM_MASK;
			}
			BlockInfo result = blockCache.get(key);
			assert result == null || !allocate; 
			if (result == null) {
				ByteBuffer bb = null;
				if (system) {
					bb = systemStore.getBuffer(block<<LOG_BLOCK_SIZE, BLOCK_SIZE, allocate);
				} else {
					bb = store.getBuffer(block<<LOG_BLOCK_SIZE, BLOCK_SIZE, allocate);
				}
				result = new BlockInfo(system, bb, block, -1);
				blockLock.lock();
				try {
					BlockInfo existing = blockCache.get(key);
					if (existing != null) {
						return existing;
					}
					blockCache.put(key, result);
				} finally {
					blockLock.unlock();
				}
				return result;
			}
			return result;
		} catch (IOException e) {
			throw new TeiidRuntimeException(e);
		}
	}
	
	void updatePhysicalBlock(BlockInfo bi) {
		int key = bi.physicalAddress;
		if (bi.system) {
			key |= SYSTEM_MASK;
		}
		if (bi.inodeOffset >= 0) {
			blockLock.lock();
			try {
				BlockInfo actual = blockCache.get(key);
				if (actual == null) {
					//we're not in the cache, so just update storage
					updatePhysicalBlockDirect(bi);
				} else {
					//TODO: check to see we're sharing the same buffer
					for (int i = 0; i < INODE_BYTES; i++) {
						actual.buf.put(bi.inodeOffset + i, bi.buf.get(i));
					}
				}
			} finally {
				blockLock.unlock();
			}
			return;
		}
		blockCache.put(key, bi);
	}

	private void updatePhysicalBlockDirect(BlockInfo bi) {
		try {
			bi.buf.rewind();
			if (!bi.system) {
				store.updateFromBuffer(bi.buf, bi.physicalAddress<<LOG_BLOCK_SIZE);
			} else {
				systemStore.updateFromBuffer(bi.buf, bi.physicalAddress<<LOG_BLOCK_SIZE);				
			}
		} catch (IOException e) {
			throw new TeiidRuntimeException(e);
		}
	}
	
	InodeBlockManager getBlockManager(long gid, long oid, int inode) {
		return new InodeBlockManager(gid, oid, inode);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void add(CacheEntry entry, Serializer s) {
		boolean success = false;
		InodeBlockManager blockManager = null;
		try {
			BlockClosedLongIntHashTable map = physicalMapping.get(s.getId());
			if (map == null) {
				return;
			}
			blockManager = getBlockManager(s.getId(), entry.getId(), EMPTY_ADDRESS);
			blockManager.trackLast = true;
			ExtensibleBufferedOutputStream fsos = new BlockOutputStream(blockManager);
            ObjectOutputStream oos = new ObjectOutputStream(fsos);
            oos.writeInt(entry.getSizeEstimate());
            s.serialize(entry.getObject(), oos);
            oos.close();
            map.put(entry.getId(), blockManager.getInode());
            success = true;
		} catch (Throwable e) {
			LogManager.logError(LogConstants.CTX_BUFFER_MGR, e, "Error persisting batch, attempts to read batch "+ entry.getId() +" later will result in an exception"); //$NON-NLS-1$ //$NON-NLS-2$
		} finally {
			if (!success && blockManager != null) {
				blockManager.free();
			}
		}
	}

	@Override
	public CacheEntry get(Long oid, Serializer<?> serializer) throws TeiidComponentException {
		try {
			BlockClosedLongIntHashTable map = physicalMapping.get(serializer.getId());
			if (map == null) {
				return null;
			}
			final int inode = map.get(oid);
			if (inode == EMPTY_ADDRESS) {
				return null;
			}
			final BlockManager manager = getBlockManager(serializer.getId(), oid, inode);
			ObjectInputStream ois = new ObjectInputStream(new InputStream() {
				
				int blockIndex;
				BlockInfo buf;
				
				@Override
				public int read() throws IOException {
					ensureBytes();
					return buf.buf.get() & 0xff;
				}

				private void ensureBytes() {
					if (buf == null || buf.buf.remaining() == 0) {
						if (buf != null) {
							buf = null;
						}
						buf = manager.getBlock(blockIndex++);
					}
				}
				
				@Override
				public int read(byte[] b, int off, int len)
						throws IOException {
					ensureBytes();
					len = Math.min(len, buf.buf.remaining());
					buf.buf.get(b, off, len);
					return len;
				}
			});
			CacheEntry ce = new CacheEntry(oid);
			ce.setSizeEstimate(ois.readInt());
			ce.setObject(serializer.deserialize(ois));
			ce.setPersistent(true);
			return ce;
        } catch(IOException e) {
        	throw new TeiidComponentException(e, QueryPlugin.Util.getString("FileStoreageManager.error_reading", oid)); //$NON-NLS-1$
        } catch (ClassNotFoundException e) {
        	throw new TeiidComponentException(e, QueryPlugin.Util.getString("FileStoreageManager.error_reading", oid)); //$NON-NLS-1$
        }
	}
	
	@Override
	public FileStore createFileStore(String name) {
		return storageManager.createFileStore(name);
	}
	
	@Override
	public void initialize() throws TeiidComponentException {
		storageManager.initialize();
		int logSpace = Math.min(45, 63 - Long.numberOfLeadingZeros(this.maxBufferSpace));
		
		blocks = 1 << Math.min(Math.max(12, logSpace -LOG_BLOCK_SIZE +1), ADDRESS_BITS); //blocks per segment

		inodes = blocks>>1;
		inodesPerBlock = BLOCK_SIZE/INODE_BYTES;
		
		inodesInUseBlocks = computeInuseBlocks(inodes);
		blocksInUseBlocks = computeInuseBlocks(blocks);
		
		inodesInuse = new BlockBitSetTree(inodes - 1, new BitSetBlockManager(0));
		blocksInuse = new BlockBitSetTree(blocks - 1, new BitSetBlockManager(inodesInUseBlocks));
		
		store = storageManager.createFileStore("data_store"); //$NON-NLS-1$
		systemStore = storageManager.createFileStore("system_store"); //$NON-NLS-1$
	}
	
	static int computeInuseBlocks(int number) {
		int blockCount = (number>>LOG_BLOCK_SIZE) + ((number&BLOCK_MASK)>0?1:0);
		return (blockCount>>3) + ((blockCount&7)>0?1:0);	
	}
		
	@Override
	public void addToCacheGroup(Long gid, Long oid) {
		BlockClosedLongIntHashTable map = physicalMapping.get(gid);
		if (map == null) {
			return;
		}
		map.put(oid, EMPTY_ADDRESS);
	}
	
	@Override
	public void createCacheGroup(Long gid) {
		BlockClosedLongIntHashTable map = new BlockClosedLongIntHashTable(getBlockManager(gid, -1, EMPTY_ADDRESS));
		physicalMapping.put(gid, map);
	}
	
	@Override
	public void remove(Long gid, Long id) {
		BlockClosedLongIntHashTable map = physicalMapping.get(gid);
		if (map == null) {
			return;
		}
		int inode = map.remove(id);
		free(gid, id, inode);
	}

	@Override
	public Collection<Long> removeCacheGroup(Long gid) {
		BlockClosedLongIntHashTable map = physicalMapping.remove(gid);
		if (map == null) {
			return Collections.emptySet();
		}
		Map<Long, Integer> values = map.remove();
		for (Map.Entry<Long, Integer> entry : values.entrySet()) {
			if (entry.getValue() != null) {
				free(gid, entry.getKey(), entry.getValue());
			}
		}
		return values.keySet();
	}
	
	void free(Long gid, Long oid, int inode) {
		if (inode == EMPTY_ADDRESS) {
			return;
		}
		BlockManager bm = getBlockManager(gid, oid, inode);
		bm.free();
	}
	
	public void setStorageManager(StorageManager storageManager) {
		this.storageManager = storageManager;
	}
	
	public StorageManager getStorageManager() {
		return storageManager;
	}
	
	public void setMaxBufferSpace(long maxBufferSpace) {
		this.maxBufferSpace = maxBufferSpace;
	}
	
	public int getInodesInUse() {
		return this.inodesInuse.getBitsSet();
	}
	
	public int getDataBlocksInUse() {
		return this.blocksInuse.getBitsSet();
	}
	
}
