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

package org.teiid.core.types;

import java.util.Arrays;

import org.teiid.core.util.Assertion;
import org.teiid.core.util.PropertiesUtils;

public final class BinaryType implements Comparable<BinaryType> {
	
	private byte[] bytes;
	
	public BinaryType(byte[] bytes) {
		Assertion.isNotNull(bytes);
		//to be truly immutable we should clone here
		this.bytes = bytes;
	}
	
	/**
	 * 
	 * @return the actual bytes - no modifications should be performed
	 */
	public byte[] getBytesDirect() {
		return this.bytes;
	}
	
	/**
	 * 
	 * @return a copy of the bytes
	 */
	public byte[] getBytes() {
		return Arrays.copyOf(bytes, bytes.length);
	}
	
	/**
	 * Get the byte value at a given index
	 * @param index
	 * @return
	 */
	public byte getByte(int index) {
		return bytes[index]; 
	}
	
	public int getLength() {
		return bytes.length;
	}
	
	@Override
	public int compareTo(BinaryType o) {
		int len1 = getLength();
		int len2 = o.getLength();
		int n = Math.min(len1, len2);
	    for (int i = 0; i < n; i++) {
	    	//unsigned comparison
			int b1 = bytes[i] & 0xff;
			int b2 = o.bytes[i] & 0xff;
			if (b1 != b2) {
			    return b1 - b2;
			}
	    }
		return len1 - len2;
	}
	
	@Override
	public int hashCode() {
		return Arrays.hashCode(bytes);
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (!(obj instanceof BinaryType)) {
			return false;
		}
		BinaryType other = (BinaryType)obj;
		return Arrays.equals(this.bytes, other.bytes);
	}
	
	@Override
	public String toString() {
		return PropertiesUtils.toHex(bytes);
	}
	
	public BlobType toBlob() {
		return new BlobType(BlobType.createBlob(bytes));
	}

}
