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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.teiid.common.buffer.Cache;
import org.teiid.common.buffer.CacheEntry;
import org.teiid.common.buffer.FileStore;
import org.teiid.common.buffer.Serializer;
import org.teiid.common.buffer.StorageManager;
import org.teiid.core.TeiidComponentException;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.query.QueryPlugin;

/**
 * A minimally blocking Cache using {@link FileStore}s.
 * 
 * Storage files with significant unused space are compacted after reaching a size threshold.
 * Compacting the empty space may be costly as it is currently implemented by blocking all
 * read/write operations against the group.
 * 
 * Since empty is concentrated at the beginning of the store a better approach could
 * be to users smaller file segments and move batches off of the beginning.
 * 
 * There is unfortunately a significant memory footprint per group.
 */
public class FileStoreCache implements Cache {
	
	private static class CacheGroup {
		private static final int RECLAIM_TAIL_SIZE = IO_BUFFER_SIZE << 5;
		private static final int MAX_FREE_SPACE = 1 << 11;
		FileStore store;
		long tail;
		long unusedSpace = 0;
		ReadWriteLock lock = new ReentrantReadWriteLock();
		Map<Long, long[]> physicalMapping = Collections.synchronizedMap(new HashMap<Long, long[]>());
		List<Long> freed = Collections.synchronizedList(new LinkedList<Long>()); 
		
		CacheGroup(FileStore store) {
			this.store = store;
		}
		
		void freeBatch(Long batch) throws IOException {
			long[] info = physicalMapping.remove(batch);
			if (info != null) { 
				if (info[0] + info[1] == tail) {
					tail -= info[1];
					if (store.getLength() - tail > RECLAIM_TAIL_SIZE) {
						store.setLength(tail);						
					}
				} else {
					unusedSpace += info[1]; 
				}
			}
		}
		
		private long getOffset(Long gid, long compactionThreshold) throws IOException {
			long currentLength = store.getLength();
			if (currentLength <= compactionThreshold || unusedSpace * 4 <= currentLength * 3) {
				return tail;
			}
			if (LogManager.isMessageToBeRecorded(LogConstants.CTX_DQP, MessageLevel.DETAIL)) {
				LogManager.logDetail(LogConstants.CTX_DQP, "Running full compaction on", gid); //$NON-NLS-1$
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
			synchronized (physicalMapping) {
				for (long[] value : physicalMapping.values()) {
					if (value == null) {
						continue;
					}
					bySize.add(value);
					byAddress.add(value);
				}
			}
			long lastEndAddress = 0;
			long usedSpace = tail - unusedSpace;
			while (!byAddress.isEmpty()) {
				long[] info = byAddress.pollFirst();
				bySize.remove(info);

				long currentOffset = info[0];
				long space = currentOffset - lastEndAddress;
				boolean movedLast = false;
				while (space > 0 && !bySize.isEmpty()) {
					long[] last = byAddress.last();
					if (last[1] > space) {
						break;
					}
					movedLast = true;
					byAddress.pollLast();
					bySize.remove(last);
					move(last, lastEndAddress, buffer);
					space -= last[1];
					lastEndAddress += last[1];
				}
				if (movedLast && !byAddress.isEmpty()) {
					long[] last = byAddress.last();
					long currentLastEndAddress = last[0] + last[1]; 
					if (currentLastEndAddress < currentLength>>1) {
						lastEndAddress = currentLastEndAddress;
						break;
					}
				}
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
				
				if (space > MAX_FREE_SPACE) {
					move(info, lastEndAddress, buffer);
				}
				lastEndAddress = info[0] + info[1];
			}
			store.setLength(lastEndAddress);
			tail = lastEndAddress;
			unusedSpace = lastEndAddress - usedSpace;
			if (LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.DETAIL)) {
				LogManager.logDetail(LogConstants.CTX_BUFFER_MGR, "Compacted store", gid, "pre-size", currentLength, "post-size", lastEndAddress); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
			}
			return tail;
		}
		
		private void move(long[] toMove, long newOffset, byte[] buffer) throws IOException {
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
	}

	private static final int COMPACTION_THRESHOLD = 1 << 24; //start checking at 16 megs
	private static final int IO_BUFFER_SIZE = 1<<13;
	int compactionThreshold = COMPACTION_THRESHOLD;
	private ConcurrentHashMap<Long, CacheGroup> cacheGroups = new ConcurrentHashMap<Long, CacheGroup>();
	private StorageManager storageManager;
	
	@Override
	public void add(CacheEntry entry, Serializer s) throws Exception {
		final CacheGroup group = cacheGroups.get(s.getId());
		if (group == null) {
			return;
		}

		group.lock.writeLock().lock();
		try {
			synchronized (group.freed) {
				while (!group.freed.isEmpty()) {
					group.freeBatch(group.freed.remove(0));
				}
			}
			final ByteBuffer buffer = ByteBuffer.allocate(IO_BUFFER_SIZE);
			final long offset = group.getOffset(s.getId(), compactionThreshold);
			ExtensibleBufferedOutputStream fsos = new ExtensibleBufferedOutputStream() {
				@Override
				protected ByteBuffer newBuffer() {
					buffer.rewind();
					return buffer;
				}
				
				@Override
				protected int flushDirect(int i) throws IOException {
					group.store.write(offset + bytesWritten, buffer.array(), 0, i);
					return i;
				}
			};
	        ObjectOutputStream oos = new ObjectOutputStream(fsos);
	        oos.writeInt(entry.getSizeEstimate());
	        oos.writeLong(entry.getLastAccess());
	        oos.writeDouble(entry.getOrderingValue());
	        s.serialize(entry.getObject(), oos);
	        oos.close();
	        long size = fsos.getBytesWritten();
	        long[] info = new long[] {offset, size};
	        group.physicalMapping.put(entry.getId(), info);
	        group.tail = Math.max(group.tail, offset + size);
			if (LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.TRACE)) {
				LogManager.logTrace(LogConstants.CTX_BUFFER_MGR, s.getId(), entry.getId(), "batch written starting at:", offset); //$NON-NLS-1$
			}
		} finally {
			group.lock.writeLock().unlock();
		}
	}

	@Override
	public void createCacheGroup(Long gid) {
		cacheGroups.put(gid, new CacheGroup(storageManager.createFileStore(String.valueOf(gid))));
	}

	@Override
	public CacheEntry get(Long id, Serializer<?> serializer)
			throws TeiidComponentException {
		CacheGroup group = cacheGroups.get(serializer.getId());
		if (group == null) {
			return null;
		}
		try {
			group.lock.readLock().lock();
			long[] info = group.physicalMapping.get(id);
			if (info == null) {
				return null;
			}
			ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(group.store.createInputStream(info[0]), IO_BUFFER_SIZE));
			CacheEntry ce = new CacheEntry(id);
			ce.setSizeEstimate(ois.readInt());
			ce.setLastAccess(ois.readLong());
			ce.setOrderingValue(ois.readDouble());
			ce.setObject(serializer.deserialize(ois));
			return ce;
        } catch(IOException e) {
        	throw new TeiidComponentException(e, QueryPlugin.Util.getString("FileStoreageManager.error_reading", id)); //$NON-NLS-1$
        } catch (ClassNotFoundException e) {
        	throw new TeiidComponentException(e, QueryPlugin.Util.getString("FileStoreageManager.error_reading", id)); //$NON-NLS-1$
        } finally {
        	group.lock.readLock().unlock();
        }
	}

	@Override
	public void remove(Long gid, Long id) {
		CacheGroup group = cacheGroups.get(gid);
		if (group == null) {
			return;
		}
		if (group.lock.writeLock().tryLock()) {
			try {
				try {
					group.freeBatch(id);
				} catch (IOException e) {
					LogManager.logError(LogConstants.CTX_BUFFER_MGR, e, "Error removing batch"); //$NON-NLS-1$
				}
			} finally {
				group.lock.writeLock().unlock();
			}
		} else {
			group.freed.add(id);
		}
	}
	
	@Override
	public void addToCacheGroup(Long gid, Long oid) {
		CacheGroup group = cacheGroups.get(gid);
		if (group == null) {
			return;
		}
		group.physicalMapping.put(oid, null);
	}

	@Override
	public Collection<Long> removeCacheGroup(Long gid) {
		CacheGroup group = cacheGroups.remove(gid);
		if (group == null) {
			return Collections.emptyList();
		}
		group.store.remove();
		synchronized (group.physicalMapping) {
			return new ArrayList<Long>(group.physicalMapping.keySet());
		}
	}

	@Override
	public FileStore createFileStore(String name) {
		return storageManager.createFileStore(name);
	}

	@Override
	public void initialize() throws TeiidComponentException {
		this.storageManager.initialize();
	}
	
	public void setStorageManager(StorageManager storageManager) {
		this.storageManager = storageManager;
	}
	
	public StorageManager getStorageManager() {
		return storageManager;
	}
	
	public void setCompactionThreshold(int compactionThreshold) {
		this.compactionThreshold = compactionThreshold;
	}

}
