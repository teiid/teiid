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

import java.util.*;

import com.metamatrix.common.buffer.TupleSourceID;
import com.metamatrix.common.buffer.BufferManager.TupleSourceStatus;
import com.metamatrix.common.buffer.BufferManager.TupleSourceType;
import com.metamatrix.common.types.DataTypeManager;

/**
 * Describe a TupleSource and all important information about it.
 */
public class TupleSourceInfo {
    
    private TupleSourceType type;       // Type of TupleSource, as defined in BufferManager constants
    private TupleSourceID tsID;
    private List schema;
    private String[] types;
    private int rowCount;
    private TupleSourceStatus status;
    private TupleGroupInfo groupInfo;  
    private boolean removed = false;
    private TreeMap<Integer, ManagedBatch> batches = new TreeMap<Integer, ManagedBatch>();

    private boolean lobs;
    
    /**
     * Construct a TupleSourceInfo given information about it.
     * @param tsID Identifier 
     * @param schema Schema describing tuple source
     * @param groupInfo The tuple gorup to which this tuple source belongs
     * @param type Type of tuple source, as defined in BufferManager constants
     */
    public TupleSourceInfo(TupleSourceID tsID, List schema, String[] types, TupleGroupInfo groupInfo, TupleSourceType type) {
        this.tsID = tsID;
        this.schema = schema;
        this.types = types;
        this.groupInfo = groupInfo;
        this.status = TupleSourceStatus.ACTIVE;
        this.rowCount = 0;   
        this.type = type;
        this.lobs = checkForLobs();
    }
    
    public void addBatch(ManagedBatch batch) {
        batches.put(batch.getBeginRow(), batch);
    }
    
    public ManagedBatch getBatch(int beginRow) {
        Map.Entry<Integer, ManagedBatch> entry = batches.floorEntry(beginRow);
        if (entry != null && entry.getValue().getEndRow() >= beginRow) {
        	return entry.getValue();
        }
        return null;
    }
    
    public void removeBatch(int beginRow) {
        batches.remove(new Integer(beginRow));
    }
    
    public Iterator<ManagedBatch> getBatchIterator() {
        return batches.values().iterator();
    }
    
    /**
     * Get the tuple source identifier
     * @return Tuple source identifier
     */
    public TupleSourceID getTupleSourceID() { 
        return this.tsID;
    }
    
    /**
     * Get the tuple schema describing a tuple source
     * @return Schema
     */
    public List getTupleSchema() { 
        return this.schema;
    }
    
    /**
     * Get group this tuple source is in. Never null
     * @return TupleGroupInfo instance representing the tuple group to which this tuple source belongs.
     */
    public TupleGroupInfo getGroupInfo() { 
        return this.groupInfo;      
    }    
    
    void setGroupInfo(TupleGroupInfo info) { 
        this.groupInfo = info;       
    } 
    
    /**
     * Get current row count
     * @return Row count
     */
    public int getRowCount() { 
        return this.rowCount;
    }
    
    /**
     * Set the current row count
     * @param rows New row count
     */
    public void setRowCount(int rows) { 
        this.rowCount = rows;
    }

    /**
     * Get status of this tuple source, as defined in BufferManager constants
     * @return Status
     */
    public TupleSourceStatus getStatus() { 
        return this.status;    
    }
    
    /**
     * Set status of this tuple source
     * @param status New status
     */
	public void setStatus(TupleSourceStatus status) {
		this.status = status;	
	}

    /**
     * Get type of this tuple source
     * @return Type of tuple source
     */
    public TupleSourceType getType() {
        return this.type;
    }
     
    /**
     * Check whether this tuple source has been removed.  Due to
     * synchronization, this tuple source may have been removed 
     * between the time it was retrieved and now.
     */       
    public boolean isRemoved() {
        return this.removed;    
    }
    
    /**
     * Set a flag that this tuple source has been removed
     */
    public void setRemoved() {
        this.removed = true;
    }

    public boolean lobsInSource() {
        return this.lobs;
    }
    
    public String[] getTypes() {
        return types;
    }
    
    /**
     * Get string representation
     * @return String representation
     */
    public String toString() {
        return "TupleSourceInfo[" + this.tsID + "]";     //$NON-NLS-1$ //$NON-NLS-2$
    }            
    
    
    private boolean checkForLobs() {
        boolean lob = false;
        if (types != null) {
            for (int i = 0; i < types.length; i++) {
                lob |= DataTypeManager.isLOB(types[i]);
            }
        }
        else {
            // if incase the user did not specify the types, then make
            // them walk the batch; pay the penalty of performence
            return true;
        }
        return lob;
    }
}
