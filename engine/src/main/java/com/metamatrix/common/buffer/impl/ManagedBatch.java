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

package com.metamatrix.common.buffer.impl;

import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;

import com.metamatrix.common.buffer.TupleSourceID;
import com.metamatrix.core.util.HashCodeUtil;

/**
 * A ManagedBatch basically describes a TupleBatch that is being managed and 
 * appends a bunch of additional state used within the BufferManagerImpl.  The 
 * batch is described by it's beginRow and endRow and it's size is in bytes.  
 * The lastAccessed field records the last accessed time in memory.  The location 
 * tells where the batch is currently stored, as defined in the TupleSourceInfo 
 * constants.  The pinned intervals describe which rows within the batch are 
 * currently pinned.
 */
class ManagedBatch {

    /** Constant for the location of this batch - stored in persistent storage */
    public final static short PERSISTENT = 0;

    /** Constant for the location of this batch - stored in memory, but not currently in use */
    public final static short UNPINNED = 1;

    /** Constant for the location of this batch - stored in memory and currently in use */
    public final static short PINNED = 2;

    private TupleSourceID tupleSourceID;
    private int beginRow;
    private int endRow;
    private long size;    
    private long lastAccessed; 
    private int location;  
    private int pinnedCount;
    
    // logging
    private static int STACK_LEVELS_TO_OMIT = 2;
    private static int STACK_LEVELS_TO_CAPTURE = 5;

    private List whoCalledUs;
    private String sCallStackTimeStamp;
    
    /**
     * Constructor for ManagedBatch.
     */
    public ManagedBatch(TupleSourceID tupleSourceID, int begin, int end, long size) {
        this.tupleSourceID = tupleSourceID;
        this.beginRow = begin;
        this.endRow = end;
        this.size = size;
        updateLastAccessed();       
    }
    
    /**
     * Get the tuple source ID 
     * @return Tuple sourceID
     */
    public TupleSourceID getTupleSourceID() {
        return this.tupleSourceID;
    }
    
    /**
     * Get the begin row, must be >= 1
     * @return Begin row
     */
    public int getBeginRow() {
        return this.beginRow;
    }
    
    /**
     * Get the end row, inclusive
     * @return End row
     */
    public int getEndRow() {
        return this.endRow;
    }

    /**
     * Get the size of the batch in bytes
     * @return Size, in bytes
     */
    public long getSize() {
        return this.size;
    }
    
    /**
     * Get the last accessed timestamp.
     * @return Last accessed timestamp
     */    
    public long getLastAccessed() {
        return this.lastAccessed;
    }
    
    /**
     * Update the last accessed timestamp from system clock
     */
    public void updateLastAccessed() {
        lastAccessed = System.currentTimeMillis();    
    }    
    
    /**
     * Get the location of the batch, as defined in constants
     * @return Location
     */
    public int getLocation() {
        return this.location;
    }
    
    /**
     * Set location of the batch, as defined in constants
     */
    public void setLocation(int location) {
        this.location = location;
    }
    
    /**
     * Check whether this managed batch has any pinned rows
     * @return True if any are pinned
     */
    public boolean hasPinnedRows() {
        return pinnedCount > 0;
    }
    
    public void pin() {
    	this.pinnedCount++;
    }
    
    public void unpin() {
    	this.pinnedCount--;
    }
    
    /**
     * Capture partial stack trace to express who owns this batch 
     */
    public void captureCallStack() {
        /*
         * If more detail is needed StackTraceElement has several useful methods.
         */
        StackTraceElement[] elements =  new Exception().getStackTrace();
        
        whoCalledUs = new LinkedList();
        
        for ( int i = STACK_LEVELS_TO_OMIT; i < elements.length && i < STACK_LEVELS_TO_OMIT + STACK_LEVELS_TO_CAPTURE; i++ ) {
            whoCalledUs.add(elements[ i ].toString());
        }
        sCallStackTimeStamp = new Timestamp(System.currentTimeMillis()).toString();
    }    
    
    /**
     * Returns call stack 
     */
    public List getCallStack() {
        
        return whoCalledUs;
    }    
    
    /**
     * Returns call stack timestamp 
     */
    public String getCallStackTimeStamp() {
        
        return this.sCallStackTimeStamp;
    }    

    /**
     * Compare two managed batches for equality - this check 
     * is made based on TupleSourceID and beginRow.
     * @param obj Other object
     * @return True if equal
     */
    public boolean equals(Object obj) {
        if(obj == this) {
            return true;
        }
        
        if(obj == null || ! (obj instanceof ManagedBatch)) {
            return false;
        }
        
        ManagedBatch other = (ManagedBatch) obj;
        return ( this.getTupleSourceID().equals(other.getTupleSourceID()) &&
                  this.getBeginRow() == other.getBeginRow() );
        
    }
    
    /**
     * Get hash code, based on tupleSourceID and beginRow
     * @return Hash code
     */
    public int hashCode() {
        return HashCodeUtil.hashCode(beginRow, tupleSourceID);
    }
    
    public String toString() {
        return "ManagedBatch[" + tupleSourceID + ", " + beginRow + ", " + endRow + "]"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }
}
