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

package org.teiid.core.util;

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

/**
 * A dynamic buffer that limits copying overhead
 */
public class MultiArrayOutputStream extends OutputStream {
	
	private byte bufferIndex;
	private int index;
	private int count;
	private byte[][] bufs = new byte[15][];
	
	public MultiArrayOutputStream(int initialSize) {
		bufs[0] = new byte[initialSize];
	}
	
	public void reset(int newIndex) {
		Assertion.assertTrue(newIndex < bufs[0].length);
		while (bufferIndex > 0) {
			bufs[bufferIndex--] = null;
		}
		count = index = newIndex;
	}
	
	@Override
	public void write(int b) throws IOException {
		int newIndex = index + 1;
        byte[] buf = bufs[bufferIndex];
		if (newIndex > buf.length) {
        	buf = bufs[++bufferIndex] = new byte[buf.length << 1];
        	buf[0] = (byte)b;
        	index = 1;
        } else {
        	buf[index] = (byte)b;
        	index = newIndex;
        }
		count++;
	}
	
	@Override
	public void write(byte[] b, int off, int len) throws IOException {
        int newIndex = index + len;
        byte[] buf = bufs[bufferIndex];
		if (newIndex > buf.length) {
        	int copyLen = Math.min(buf.length - index, len);
        	if (copyLen > 0) {
        		System.arraycopy(b, off, buf, index, copyLen);
        	}
        	int to = off + len;
        	int nextIndex = len - copyLen;
        	int diff = (buf.length << 1) - nextIndex;
        	if (diff > 0) {
        		to += diff;
        	}
        	bufs[++bufferIndex] = Arrays.copyOfRange(b, off + copyLen, to);
        	index = nextIndex;
        } else {
        	System.arraycopy(b, off, buf, index, len);
        	index = newIndex;
        }
		count += len;
	}
	
	public void writeTo(DataOutput out) throws IOException {
		for (byte i = 0; i <= bufferIndex; i++) {
    		byte[] b = bufs[i];
    		out.write(b, 0, bufferIndex == i?index:b.length);
    	}
	}
	
	public int getCount() {
		return count;
	}
	
	public byte[][] getBuffers() {
		return bufs;
	}
	
	public int getIndex() {
		return index;
	}
	
}
