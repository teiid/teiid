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

package com.metamatrix.query.metadata;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.metamatrix.core.util.LRUCache;

/**
 * This class represents a temporary metadata ID.  A temporary metadata ID 
 * does not exist in a real metadata source.  Rather, it is used temporarily 
 * in context of processing a single query.  This metadata ID can be used to 
 * represent either a group or an element depending on the constructor used.
 */
public class TempMetadataID implements Serializable {
    
	private static final int LOCAL_CACHE_SIZE = 8;
	
    private String ID;      // never null, upper cased fully-qualified string
    private int position = -1; //used for order by symbol resolving.  refers to the position in the select clause
    private Class<?> type;     // type of this element, only for element
    private List<TempMetadataID> elements;  // of TempMetadataID, only for group
    private Object originalMetadataID;
    private boolean isVirtual = true;   // virtual by default
    private boolean isTempTable;
    private Collection<TempMetadataID> accessPatterns;
    private int cardinality = QueryMetadataInterface.UNKNOWN_CARDINALITY;
    private LRUCache localCache;
    private boolean scalarGroup;
    
    /**
     * Constructor for group form of metadata ID.
     * @param ID Fully-qualified, upper-case name of ID
     * @param elements List of TempMetadataID representing elements
     */
    public TempMetadataID(String ID, List<TempMetadataID> elements) {
        this.ID = ID;
        this.elements = elements;
    }
    
    /**
     * Constructor for group form of metadata ID.
     * @param ID Fully-qualified, upper-case name of ID
     * @param elements List of TempMetadataID representing elements
     * @param isVirtual whether or not the group is a virtual group
     */
    public TempMetadataID(String ID, List<TempMetadataID> elements, boolean isVirtual, boolean isTempTable) {
        this.ID = ID;
        this.elements = elements;
        this.isVirtual = isVirtual;
        this.isTempTable = isTempTable;
    }
    
    /**
     * Constructor for element form of metadata ID.
     * @param ID Fully-qualified, upper-case name of ID
     * @param type Type of elements List of TempMetadataID representing elements
     */
    public TempMetadataID(String ID, Class<?> type) { 
        this.ID = ID;
        this.type = type;
    }
    
    /**
     * Constructor for element form of metadata ID with the underlying element.
     * @param ID Fully-qualified, upper-case name of ID
     * @param type Type of elements List of TempMetadataID representing elements
     * @param metadataID the orginal metadataID
     */
    public TempMetadataID(String ID, Class<?> type, Object metadataID) { 
        this.ID = ID;
        this.type = type;
        this.originalMetadataID = metadataID;
    }

    /**
     * Get ID value 
     * @return ID value
     */
    public String getID() { 
        return this.ID;
    }
    
    /**
     * Get type - only valid for elements
     * @return Type for elements, null for groups
     */
    public Class<?> getType() { 
        return this.type;
    }
    
    /**
     * Get elements - only valid for groups
     * @return List of TempMetadataID for groups, null for elements
     */
    public List<TempMetadataID> getElements() { 
        return this.elements;
    }
    
    /**
     * add a element to the temp table.
     * @param elem
     */
    protected void addElement(TempMetadataID elem) {
        if (this.elements != null) {
            this.elements.add(elem);
        }
        if (this.localCache != null) {
        	this.localCache.clear();
        }
    }

    /**
     * Check whether this group is virtual
     * @return True if virtual
     */
    public boolean isVirtual() {
        return this.isVirtual;    
    }
    
    /**
     * Whether it is a temporary table  
     * @return
     * @since 5.5
     */
    public boolean isTempTable() {
        return this.isTempTable;
    }
    
    /**
     * Return string representation of ID
     * @return String representation
     */                
    public String toString() { 
        return ID;
    }
    
    /**
     * Compare this temp metadata ID with another object.
     * @return True if obj is another TempMetadataID with same ID value
     */
    public boolean equals(Object obj) { 
        if(this == obj) { 
            return true;
        }
        
        if(obj == null || !(obj instanceof TempMetadataID)) { 
            return false;
        }
        return this.getID().equals( ((TempMetadataID) obj).getID());
    }       
    
    /**
     * Return hash code
     * @return Hash code value for object
     */
    public int hashCode() {
        return this.ID.hashCode();
    }
    
    public void setOriginalMetadataID(Object metadataId) {
        this.originalMetadataID = metadataId;
    }
    
    /** 
     * @return Returns the originalMetadataID.
     * @since 4.3
     */
    public Object getOriginalMetadataID() {
        return this.originalMetadataID;
    }

    public Collection<TempMetadataID> getAccessPatterns() {
        if (this.accessPatterns == null) {
            return Collections.emptyList();
        }
        return this.accessPatterns;
    }

    public void setAccessPatterns(Collection<TempMetadataID> accessPatterns) {
        this.accessPatterns = accessPatterns;
    }

    public int getCardinality() {
        return this.cardinality;
    }

    public void setCardinality(int cardinality) {
        this.cardinality = cardinality;
    }

    public void setTempTable(boolean isTempTable) {
        this.isTempTable = isTempTable;
    }

    public int getPosition() {
        return this.position;
    }

    public void setPosition(int position) {
        this.position = position;
    }  
    
    Object getProperty(Object key) {
    	if (this.localCache != null) {
    		return this.localCache.get(key);
    	}
    	return null;
    }
    
    Object setProperty(Object key, Object value) {
		if (this.localCache == null) {
			this.localCache = new LRUCache(LOCAL_CACHE_SIZE);
    	}
		return this.localCache.put(key, value);
    }

	public boolean isScalarGroup() {
		return scalarGroup;
	}

	public void setScalarGroup(boolean scalarGroup) {
		this.scalarGroup = scalarGroup;
	}
		
}
