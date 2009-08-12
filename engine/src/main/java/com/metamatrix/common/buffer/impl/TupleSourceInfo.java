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

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import com.metamatrix.common.buffer.TupleSourceID;
import com.metamatrix.common.buffer.BufferManager.TupleSourceStatus;
import com.metamatrix.common.buffer.BufferManager.TupleSourceType;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.common.types.Streamable;

/**
 * Describe a TupleSource and all important information about it.
 */
public class TupleSourceInfo {
    
	private static final AtomicLong LOB_ID = new AtomicLong();
	private static final ReferenceQueue<Streamable<?>> LOB_QUEUE = new ReferenceQueue<Streamable<?>>();
	
	private static class LobReference extends PhantomReference<Streamable<?>> {
		
		String persistentStreamId;
		
		public LobReference(Streamable<?> lob) {
			super(lob, LOB_QUEUE);
			this.persistentStreamId = lob.getPersistenceStreamId();
		}		
	}
	
    private TupleSourceType type;       // Type of TupleSource, as defined in BufferManager constants
    private TupleSourceID tsID;
    private List schema;
    private String[] types;
    private int rowCount;
    private TupleSourceStatus status = TupleSourceStatus.ACTIVE;
    private TupleGroupInfo groupInfo;  
    private boolean removed = false;
    private TreeMap<Integer, ManagedBatch> batches = new TreeMap<Integer, ManagedBatch>();
    private Map<String, Streamable<?>> lobReferences; //references to contained lobs
    private boolean lobs;
    
    @SuppressWarnings("unused")
	private LobReference containingLobReference; //reference to containing lob
    
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
        this.type = type;
        this.lobs = checkForLobs();
    }
    
    public void setContainingLobReference(Streamable<?> s) {
		this.containingLobReference = new LobReference(s);
	}
    
    public void addLobReference(Streamable<Object> lob) {
    	String id = lob.getReferenceStreamId();
    	if (id == null) {
    		id = String.valueOf(LOB_ID.getAndIncrement());
    		lob.setReferenceStreamId(id);
    	}
    	if (this.lobReferences == null) {
    		this.lobReferences = Collections.synchronizedMap(new HashMap<String, Streamable<?>>());
    	}
    	this.lobReferences.put(id, lob);
    }
    
    public static String getStaleLobTupleSource() {
    	LobReference ref = (LobReference)LOB_QUEUE.poll();
    	if (ref == null) {
    		return null;
    	}
    	return ref.persistentStreamId;
    }
    
    public Streamable<?> getLobReference(String id) {
    	if (this.lobReferences == null) {
    		return null;
    	}
    	return this.lobReferences.get(id);
    }
    
    public void addBatch(ManagedBatch batch) {
        batches.put(batch.getBeginRow(), batch);
    }
    
    /**
     * Returns the batch containing the begin row or null
     * if it doesn't exist
     * @param beginRow
     * @return
     */
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
        if (types == null) {
            // assume the worst
        	return true;
        }
        for (int i = 0; i < types.length; i++) {
            if (DataTypeManager.isLOB(types[i]) || types[i] == DataTypeManager.DefaultDataTypes.OBJECT) {
            	return true;
            }
        }
        return false;
    }
}
