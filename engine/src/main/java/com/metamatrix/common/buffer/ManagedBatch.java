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

package com.metamatrix.common.buffer;

import java.lang.ref.SoftReference;

class ManagedBatch {

    private int beginRow;
    private SoftReference<TupleBatch> batchReference;
    private long offset;
    private int length;
    
    /**
     * Constructor for ManagedBatch.
     */
    public ManagedBatch(TupleBatch batch) {
        this.beginRow = batch.getBeginRow();
        this.batchReference = new SoftReference<TupleBatch>(batch);
    }
    
    /**
     * Get the begin row, must be >= 1
     * @return Begin row
     */
    public int getBeginRow() {
        return this.beginRow;
    }
    
    public String toString() {
        return "ManagedBatch[" + beginRow + "]"; //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public TupleBatch getBatch() {
		return this.batchReference.get();
	}
    
    public void setBatchReference(TupleBatch batch) {
		this.batchReference = new SoftReference<TupleBatch>(batch);
	}
    
    public int getLength() {
		return length;
	}
    
    public long getOffset() {
		return offset;
	}
    
    public void setLength(int length) {
		this.length = length;
	}
    
    public void setOffset(long offset) {
		this.offset = offset;
	}
        
}
