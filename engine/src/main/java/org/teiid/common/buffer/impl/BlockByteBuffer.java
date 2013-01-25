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
	
	private static class BlockByteBufferData {
		int blockAddressBits;
		int segmentAddressBits;
		int segmentSize;
		int blockSize;
		int blockCount;
	}

	private BlockByteBufferData data;
	private ByteBuffer[] origBuffers;
	private ByteBuffer[] buffers;
	
	/**
	 * Creates a new {@link BlockByteBuffer} where each buffer segment will be
	 * 1 << segmentAddressBits (max of 30), and a total size of (1 << blockAddressBits)*blockCount.
	 * @param segmentAddressBits
	 * @param blockCount
	 * @param blockAddressBits
	 * @param direct
	 */
	public BlockByteBuffer(int segmentAddressBits, int blockCount, int blockAddressBits, boolean direct) {
		this.data = new BlockByteBufferData();
		this.data.segmentAddressBits = segmentAddressBits;
		this.data.blockAddressBits = blockAddressBits;
		this.data.blockSize = 1 << blockAddressBits;
		this.data.segmentSize = 1 << this.data.segmentAddressBits;
		this.data.blockCount = blockCount;
		long size = ((long)blockCount)<<blockAddressBits;
		int fullSegments = (int)size>>segmentAddressBits;
		int lastSegmentSize = (int) (size&(data.segmentSize-1));
		int segments = fullSegments;
		if (lastSegmentSize > 0) {
			segments++;
		}
		origBuffers = new ByteBuffer[segments];
		buffers = new ByteBuffer[segments];
		for (int i = 0; i < fullSegments; i++) {
			origBuffers[i] = allocate(data.segmentSize, direct);
		}
		if (lastSegmentSize > 0) {
			origBuffers[fullSegments] = allocate(lastSegmentSize, direct);
		}
	}
	
	private BlockByteBuffer() {
		
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
	
	public BlockByteBuffer duplicate() {
		BlockByteBuffer dup = new BlockByteBuffer();
		dup.data = data;
		dup.origBuffers = origBuffers;
		dup.buffers = new ByteBuffer[dup.origBuffers.length];
		return dup;
	}
	
	/**
	 * Return a buffer positioned at the given start byte.
	 * It is assumed that the caller will handle blocks in
	 * a thread safe manner.
	 * @param startIndex
	 * @return
	 */
	public ByteBuffer getByteBuffer(int block) {
		if (block < 0 || block >= data.blockCount) {
			throw new IndexOutOfBoundsException("Invalid block " + block); //$NON-NLS-1$
		}
		int segment = block>>(data.segmentAddressBits-data.blockAddressBits);
		ByteBuffer bb = buffers[segment];
		if (bb == null) {
			bb = buffers[segment] = origBuffers[segment].duplicate();
		} else {
			bb.rewind();	
		}
		int position = (block<<data.blockAddressBits)&(data.segmentSize-1);
		bb.limit(position + data.blockSize);
		bb.position(position);
		return bb;
	}
		
}
