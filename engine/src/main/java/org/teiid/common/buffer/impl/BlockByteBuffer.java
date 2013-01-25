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

import java.nio.ByteBuffer;

/**
 * Provides buffer slices or blocks off of a central
 * set of buffers.
 */
public class BlockByteBuffer {
	
	private final static class ThreadLocalByteBuffer extends ThreadLocal<ByteBuffer> {
		private final ByteBuffer byteBuffer;
		
		public ThreadLocalByteBuffer(ByteBuffer byteBuffer) {
			this.byteBuffer = byteBuffer;
		}
		
		protected ByteBuffer initialValue() {
			return byteBuffer.duplicate();
		}
	}

	private int blockAddressBits;
	private int segmentAddressBits;
	private int segmentSize;
	private int blockSize;
	private int blockCount;
	private ThreadLocal<ByteBuffer>[] buffers;
	
	/**
	 * Creates a new {@link BlockByteBuffer} where each buffer segment will be
	 * 1 << segmentAddressBits (max of 30), and a total size of (1 << blockAddressBits)*blockCount.
	 * @param segmentAddressBits
	 * @param blockCount
	 * @param blockAddressBits
	 * @param direct
	 */
	@SuppressWarnings("unchecked")
	public BlockByteBuffer(int segmentAddressBits, int blockCount, int blockAddressBits, boolean direct) {
		this.segmentAddressBits = segmentAddressBits;
		this.blockAddressBits = blockAddressBits;
		this.blockSize = 1 << blockAddressBits;
		this.segmentSize = 1 << this.segmentAddressBits;
		this.blockCount = blockCount;
		long size = ((long)blockCount)<<blockAddressBits;
		int fullSegments = (int)size>>segmentAddressBits;
		int lastSegmentSize = (int) (size&(segmentSize-1));
		int segments = fullSegments;
		if (lastSegmentSize > 0) {
			segments++;
		}
		buffers = new ThreadLocal[segments];
		for (int i = 0; i < fullSegments; i++) {
			buffers[i] = new ThreadLocalByteBuffer(allocate(segmentSize, direct));
		}
		if (lastSegmentSize > 0) {
			buffers[fullSegments] = new ThreadLocalByteBuffer(allocate(lastSegmentSize, direct));
		}
	}

	public static ByteBuffer allocate(int size, boolean direct) {
		if (direct) {
			ByteBuffer newBuffer = ByteBuffer.allocateDirect(size);
			int longsPerSegment = size>>3;
			//manually initialize until java 7 when it's mandated (although this may already have been performed)
			for (int j = 0; j < longsPerSegment; j++) {
				newBuffer.putLong(0);
			}
			return newBuffer;
		}
		return ByteBuffer.allocate(size);
	}
	
	/**
	 * Return a buffer positioned at the given start byte.
	 * It is assumed that the caller will handle blocks in
	 * a thread safe manner.
	 * @param startIndex
	 * @return
	 */
	public ByteBuffer getByteBuffer(int block) {
		if (block < 0 || block >= blockCount) {
			throw new IndexOutOfBoundsException("Invalid block " + block); //$NON-NLS-1$
		}
		int segment = block>>(segmentAddressBits-blockAddressBits);
		ByteBuffer bb = buffers[segment].get();
		bb.rewind();
		int position = (block<<blockAddressBits)&(segmentSize-1);
		bb.limit(position + blockSize);
		bb.position(position);
		return bb;
	}
	
}
