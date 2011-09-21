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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
 * Implements storage and caching against a {@link FileStore} abstraction.
 * caching uses a block paradigm, where the first block has the format
 *   long gid | long oid | short tail bytes | short sector table length | [3 byte int sector ...] 
 *   
 * TODO: back non-fragmented block information onto the block address
 */
public class FileStoreCache implements Cache, StorageManager {
	
	private static final int IO_BUFFER_SIZE = 1 << 14;
	private static final int DATA_START = 1 << 13;
	private static final int MAX_BLOCKS = 2724; //implies that the max size is ~ 42.5 mb
	
	static class TryFreeParameter {
		Long gid;
		Long oid;
		Integer block;

		public TryFreeParameter(Long gid, Long oid, Integer block) {
			this.gid = gid;
			this.oid = oid;
			this.block = block;
		}
	}
	
	private class Segment {
		int id;
		ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
		List<TryFreeParameter> freed = Collections.synchronizedList(new ArrayList<TryFreeParameter>()); 
		FileStore store;
		BitSetTree inuse = new BitSetTree();
		BitSetTree fragmentedFlags = new BitSetTree();
		
		public Segment(int id) {
			this.id = id;
		}
		
		void tryFree(Long gid, Long oid, Integer block) {
			if (lock.writeLock().tryLock()) {
				try {
					free(gid, oid, block);
				} finally {
					lock.writeLock().unlock();
				}
			} else {
				freed.add(new TryFreeParameter(gid, oid, block));
			}
		}
		
		private void free(Long gid, Long oid, Integer block) {
			if (block == null) {
				Map<Long, Integer> map = physicalMapping.get(gid);
				if (map == null) {
					return;
				}
				block = map.remove(oid);
				if (block == null) {
					return;
				}
			}
			byte[] buf = new byte[DATA_START];
			try {
				inuse.set(block, false);
				boolean fragmented = false;
				if (fragmentedFlags.get(block)) {
					fragmented = true;
					store.read(block << 14, buf, 0, buf.length);
					fragmentedFlags.set(block, false);
				} else {
					//TODO: if not fragmented come up with a better approach than a disk read
					store.read(block << 14, buf, 0, 20);
				}
				ByteBuffer bb = ByteBuffer.wrap(buf);
				bb.position(18);
				short blocks = bb.getShort();
				if (!fragmented) {
					for (int i = block + 1; i < blocks; i++) {
						inuse.set(i, false);
					}
				} else {
					for (short i = 0; i < blocks; i++) {
						int toFree = getNextBlock(bb);
						inuse.set(toFree, false);
					}
				}
			} catch (IOException e) {
				throw new TeiidRuntimeException(e, "Could not read intial block to process freeing " + oid); //$NON-NLS-1$
			}
		}

		@SuppressWarnings("unchecked")
		void add(final CacheEntry entry, final Serializer s) throws TeiidComponentException {
			lock.writeLock().lock();
			try {
				List<TryFreeParameter> toFree = Collections.emptyList();
				synchronized (freed) {
					if (!freed.isEmpty()) {
						toFree = new ArrayList<TryFreeParameter>(freed);
						freed.clear();
					}
				}
				for (TryFreeParameter param : toFree) {
					free(param.gid, param.oid, param.block);
				}
				ExtensibleBufferedOutputStream fsos = new ExtensibleBufferedOutputStream(new byte[IO_BUFFER_SIZE]) {
					
					byte[] firstBytes = null;
					List<Integer> blocks = new ArrayList<Integer>(8);
					boolean isFragmented;
					int lastCount;
					int start;
					
					@Override
					protected void flushDirect() throws IOException {
						lastCount = count;
						if (firstBytes == null) {
							start = nextBlock(-1);
							firstBytes = buf;
							//need a new buffer only if there could be bytes remaining
							if (count == buf.length) {
								buf = new byte[IO_BUFFER_SIZE];
							}
						} else {
							int last = -1;
							if (!blocks.isEmpty()) {
								last = blocks.get(blocks.size()-1);
							} else {
								last = start;
							}
							int next = nextBlock(last);
							blocks.add(next);
							if (next != last + 1) {
								isFragmented = true;
							}
							if (blocks.size() > MAX_BLOCKS) {
								//TODO handle this case
								throw new TeiidRuntimeException("Exceeded max persistent object size" + entry.getId()); //$NON-NLS-1$
							}
							store.write(next << 14, buf, 0, count);
						}
					}
					
					@Override
					public void close() throws IOException {
						super.close();
						ByteBuffer bb = ByteBuffer.wrap(firstBytes);
						bb.putLong(s.getId());
						bb.putLong(entry.getId());
						bb.putShort((short)(lastCount - (blocks.isEmpty()?DATA_START:0)));
						bb.putShort((short)blocks.size());
						for (Integer i : blocks) {
							bb.put((byte)(i.intValue()>>16));
							bb.putShort((short)i.intValue());
						}
						store.write(start << 14, firstBytes, 0, firstBytes.length);
						if (LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.TRACE)) {
							LogManager.logTrace(LogConstants.CTX_BUFFER_MGR, id, entry.getId(), "batch written starting at:", start); //$NON-NLS-1$
						}
						Map<Long, Integer> map = physicalMapping.get(s.getId());
						if (map == null) {
							return;
						}
						map.put(entry.getId(), start);
						inuse.set(start, true);
						for (Integer i : blocks) {
							inuse.set(i, true);
						}
						if (isFragmented) {
							fragmentedFlags.set(start, true);
						}
					}

				};
				fsos.count = DATA_START; 
	            ObjectOutputStream oos = new ObjectOutputStream(fsos);
	            oos.writeInt(entry.getSizeEstimate());
	            s.serialize(entry.getObject(), oos);
	            oos.close();
			} catch (IOException e) {
				throw new TeiidComponentException(e);
			} finally {
				lock.writeLock().unlock();
			}
			
			if (entry.getId().longValue() == 451) {
				get(entry.getId(), s);
			}
		}
		
		int nextBlock(int fromIndex) {
			int next = inuse.nextClearBit(fromIndex + 1);
			if (next == -1) {
				throw new TeiidRuntimeException("no freespace available on segment" + id); //$NON-NLS-1$
			}
			return next;
		}
		
		@SuppressWarnings("unchecked")
		CacheEntry get(Long oid, Serializer serializer) throws TeiidComponentException {
			lock.readLock().lock();
			try {
				Map<Long, Integer> map = physicalMapping.get(serializer.getId());
				if (map == null) {
					return null;
				}
				final Integer startBlock = map.get(oid);
				if (startBlock == null) {
					return null;
				}
				ObjectInputStream ois = new ObjectInputStream(new InputStream() {
					
					int count;
					int pos;
					byte[] buf = new byte[IO_BUFFER_SIZE];
					ByteBuffer firstBytes;
					int currentBlock;
					short tailBytes;
					short totalBlocks;
					short blockNum;
					
					@Override
					public int read() throws IOException {
						if (pos == count) {
							if (firstBytes == null) {
								store.readFully(startBlock << 14, buf, 0, buf.length);
								firstBytes = ByteBuffer.wrap(buf);
								firstBytes.position(16);
								tailBytes = firstBytes.getShort();
								totalBlocks = firstBytes.getShort();
								count = buf.length;
								pos = DATA_START;
							} else {
								buf = new byte[IO_BUFFER_SIZE];
								//TODO: defrag on read
								if (count == buf.length) {
									currentBlock = getNextBlock(firstBytes);
									blockNum++;
									pos = 0;
								}
								int length = blockNum == totalBlocks?tailBytes:buf.length;
								store.readFully(currentBlock << 14, buf, 0, length);
								count = length;
							}
						} 
						return buf[pos++] & 0xff;
					}
				});
				CacheEntry ce = new CacheEntry(oid);
				ce.setSizeEstimate(ois.readInt());
				ce.setObject(serializer.deserialize(ois));
				ce.setPersistent(true);
				return ce;
	        } catch(IOException e) {
	        	throw new TeiidComponentException(e, QueryPlugin.Util.getString("FileStoreageManager.error_reading", id)); //$NON-NLS-1$
	        } catch (ClassNotFoundException e) {
	        	throw new TeiidComponentException(e, QueryPlugin.Util.getString("FileStoreageManager.error_reading", id)); //$NON-NLS-1$
	        } finally {
	        	lock.readLock().unlock();
	        }
		}
	}
	
	static int getNextBlock(ByteBuffer bb) {
		int block = (bb.get() & 0xff)<< 16;
		block += (bb.getShort() & 0xffff);
		return block;
	}
	
	private StorageManager storageManager;
	private Segment[] segments;
	private ConcurrentHashMap<Long, Map<Long, Integer>> physicalMapping = new ConcurrentHashMap<Long, Map<Long,Integer>>();
	private int segmentCount = 32;
	
	@Override
	public void add(CacheEntry entry, Serializer<?> s) {
		Segment seg = getSegment(entry.getId());
		try {
			seg.add(entry, s);
		} catch (Throwable e) {
			LogManager.logError(LogConstants.CTX_BUFFER_MGR, e, "Error persisting batch, attempts to read batch "+ entry.getId() +" later will result in an exception"); //$NON-NLS-1$ //$NON-NLS-2$
		}
	}
	
	@Override
	public CacheEntry get(Long id, Serializer<?> serializer) throws TeiidComponentException {
		Segment seg = getSegment(id);
		return seg.get(id, serializer);
	}
	
	private Segment getSegment(Long id) {
		return segments[id.hashCode() & (segments.length - 1)];
	}
	
	@Override
	public FileStore createFileStore(String name) {
		return storageManager.createFileStore(name);
	}
	
	@Override
	public void initialize() throws TeiidComponentException {
		storageManager.initialize();
		int numSegments = 31 - Integer.numberOfLeadingZeros(this.segmentCount);
		segments = new Segment[1 << numSegments];
		for (int i = 0; i < segments.length; i++) {
			segments[i] = new Segment(i);
			segments[i].store = storageManager.createFileStore("segment_" + i); //$NON-NLS-1$
		}
	}
		
	@Override
	public void addToCacheGroup(Long gid, Long oid) {
		Map<Long, Integer> map = physicalMapping.get(gid);
		if (map == null) {
			return;
		}
		map.put(oid, null);
	}
	
	@Override
	public void createCacheGroup(Long gid) {
		physicalMapping.put(gid, Collections.synchronizedMap(new HashMap<Long, Integer>()));
	}
	
	@Override
	public void remove(Long gid, Long id) {
		Map<Long, Integer> map = physicalMapping.get(gid);
		if (map == null) {
			return;
		}
		Integer block = map.remove(id);
		if (block != null) {
			Segment s = getSegment(id);
			s.tryFree(gid, id, block);
		}
	}
	
	@Override
	public Collection<Long> removeCacheGroup(Long gid) {
		Map<Long, Integer> values = physicalMapping.remove(gid);
		if (values == null) {
			return Collections.emptySet();
		}
		synchronized (values) {
			for (Map.Entry<Long, Integer> entry : values.entrySet()) {
				if (entry.getValue() != null) {
					Segment s = getSegment(entry.getKey());
					s.tryFree(gid, entry.getKey(), entry.getValue());
				}
			}
			return new HashSet<Long>(values.keySet());
		}
	}
	
	public void setSegmentCount(int segmentCount) {
		this.segmentCount = segmentCount;
	}

	public void setStorageManager(StorageManager storageManager) {
		this.storageManager = storageManager;
	}
	
	public StorageManager getStorageManager() {
		return storageManager;
	}
	
}
