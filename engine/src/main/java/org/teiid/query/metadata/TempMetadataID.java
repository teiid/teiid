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

package org.teiid.query.metadata;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.teiid.core.util.LRUCache;
import org.teiid.metadata.AbstractMetadataRecord.DataModifiable;
import org.teiid.metadata.AbstractMetadataRecord.Modifiable;
import org.teiid.query.mapping.relational.QueryNode;
import org.teiid.query.sql.lang.CacheHint;
import org.teiid.query.sql.symbol.SingleElementSymbol;

/**
 * This class represents a temporary metadata ID.  A temporary metadata ID 
 * does not exist in a real metadata source.  Rather, it is used temporarily 
 * in context of processing a single query.  This metadata ID can be used to 
 * represent either a group or an element depending on the constructor used.
 * 
 * TODO: we should be using the real metadata objects, but internal and
 * designer legacy keep us on the temp framework
 */
public class TempMetadataID implements Serializable, Modifiable, DataModifiable {
    
	private static final long serialVersionUID = -1879211827339120135L;
	private static final int LOCAL_CACHE_SIZE = 8;
	
	private static final int MOD_COUNT_FOR_COST_UPDATE = 8;
	
	public static class TableData {
		Collection<TempMetadataID> accessPatterns;
		List<TempMetadataID> elements;
		int cardinality = QueryMetadataInterface.UNKNOWN_CARDINALITY;
		List<TempMetadataID> primaryKey;
		QueryNode queryNode;
		LRUCache<Object, Object> localCache;
		CacheHint cacheHint;
		List<List<TempMetadataID>> keys;
		List<List<TempMetadataID>> indexes;
		long lastDataModification;
		long lastModified = System.currentTimeMillis();
		int modCount;
		
		public long getLastDataModification() {
			return lastDataModification;
		}
		
		public void removed() {
			this.lastModified = -1;
		}
		
		public void dataModified(int updateCount) {
			if (updateCount == 0) {
				return;
			}
			long ts = System.currentTimeMillis();
			modCount += updateCount;
			if (modCount > MOD_COUNT_FOR_COST_UPDATE) {
				this.lastModified = ts;
				modCount = 0;
			}
			this.lastDataModification = ts;
		}
		
		public long getLastModified() {
			return lastModified;
		}
		
	}
	
	private static TableData DUMMY_DATA = new TableData();
	
	public enum Type {
		VIRTUAL,
		TEMP,
		SCALAR,
		XML
	}
	
    private String ID;      // never null, upper cased fully-qualified string
    private String name;
    private Type metadataType = Type.VIRTUAL;
    private Object originalMetadataID;
    
    private TableData data;
    
	//Column metadata
    private int position;
    private Class<?> type;     // type of this element, only for element
    private boolean autoIncrement;
    private boolean notNull;
    private boolean updatable;
    
    /**
     * Constructor for group form of metadata ID.
     * @param ID Fully-qualified, upper-case name of ID
     * @param elements List of TempMetadataID representing elements
     */
    public TempMetadataID(String ID, List<TempMetadataID> elements) {
        this(ID, elements, Type.VIRTUAL);
    }
    
    /**
     * Constructor for group form of metadata ID.
     * @param ID Fully-qualified, upper-case name of ID
     * @param elements List of TempMetadataID representing elements
     * @param isVirtual whether or not the group is a virtual group
     */
    public TempMetadataID(String ID, List<TempMetadataID> elements, Type type) {
        this.data = new TableData();
        this.ID = ID;
        this.data.elements = elements;
        int pos = 1;
        for (TempMetadataID tempMetadataID : elements) {
			tempMetadataID.setPosition(pos++);
		}
        this.name = ID;
        this.metadataType = type;
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
    
    @Override
    public long getLastDataModification() {
    	return getTableData().getLastDataModification();
    }
    
    @Override
    public long getLastModified() {
    	return getTableData().getLastModified();
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
        return this.getTableData().elements;
    }
    
    /**
     * add a element to the temp table.
     * @param elem
     */
    protected void addElement(TempMetadataID elem) {
        if (this.getTableData().elements != null) {
            this.getTableData().elements.add(elem);
            elem.setPosition(this.getTableData().elements.size());
        }
        if (this.getTableData().localCache != null) {
        	this.getTableData().localCache.clear();
        }
    }

    /**
     * Check whether this group is virtual
     * @return True if virtual
     */
    public boolean isVirtual() {
        return metadataType == Type.VIRTUAL;    
    }
    
    /**
     * Whether it is a temporary table  
     * @return
     * @since 5.5
     */
    public boolean isTempTable() {
        return this.metadataType == Type.TEMP;
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
        
        if(!(obj instanceof TempMetadataID)) { 
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
        if (this.getTableData().accessPatterns == null) {
            return Collections.emptyList();
        }
        return this.getTableData().accessPatterns;
    }

    public void setAccessPatterns(Collection<TempMetadataID> accessPatterns) {
        this.getTableData().accessPatterns = accessPatterns;
    }

    public int getCardinality() {
        return this.getTableData().cardinality;
    }

    public void setCardinality(int cardinality) {
        this.getTableData().cardinality = cardinality;
    }

    public void setTempTable(boolean isTempTable) {
        if (isTempTable) {
        	this.metadataType = Type.TEMP;
        } else {
        	this.metadataType = Type.VIRTUAL;
        }
    }

    Object getProperty(Object key) {
    	if (this.getTableData().localCache != null) {
    		return this.getTableData().localCache.get(key);
    	}
    	return null;
    }
    
    Object setProperty(Object key, Object value) {
		if (this.getTableData().localCache == null) {
			this.getTableData().localCache = new LRUCache<Object, Object>(LOCAL_CACHE_SIZE);
    	}
		return this.getTableData().localCache.put(key, value);
    }

	public boolean isScalarGroup() {
		return this.metadataType == Type.SCALAR;
	}
	
	public void setMetadataType(Type metadataType) {
		this.metadataType = metadataType;
	}
	
	public Type getMetadataType() {
		return metadataType;
	}

	public List<TempMetadataID> getPrimaryKey() {
		return getTableData().primaryKey;
	}
	
	public void setPrimaryKey(List<TempMetadataID> primaryKey) {
		this.getTableData().primaryKey = primaryKey;
	}
	
	public int getPosition() {
		return position;
	}
	
	public void setPosition(int position) {
		this.position = position;
	}
	
	public QueryNode getQueryNode() {
		return getTableData().queryNode;
	}
	
	public void setQueryNode(QueryNode queryNode) {
		this.getTableData().queryNode = queryNode;
	}
	
	public CacheHint getCacheHint() {
		return getTableData().cacheHint;
	}
	
	public void setCacheHint(CacheHint cacheHint) {
		this.getTableData().cacheHint = cacheHint;
	}
	
	public List<List<TempMetadataID>> getIndexes() {
		return getTableData().indexes;
	}
	
	public void addIndex(List<TempMetadataID> index) {
		if (this.getTableData().indexes == null) {
			this.getTableData().indexes = new LinkedList<List<TempMetadataID>>();
		}
		this.getTableData().indexes.add(index);
	}
	
	public List<List<TempMetadataID>> getUniqueKeys() {
		return getTableData().keys;
	}
	
	public void addUniqueKey(List<TempMetadataID> key) {
		if (this.getTableData().keys == null) {
			this.getTableData().keys = new LinkedList<List<TempMetadataID>>();
		}
		this.getTableData().keys.add(key);
	}

	public TableData getTableData() {
		if (data == null) {
			return DUMMY_DATA;
		}
		return data;
	}

	public boolean isAutoIncrement() {
		return autoIncrement;
	}
	
	public void setAutoIncrement(boolean autoIncrement) {
		this.autoIncrement = autoIncrement;
	}

	public boolean isNotNull() {
		return notNull;
	}
	
	public void setNotNull(boolean notNull) {
		this.notNull = notNull;
	}
	
	public void setUpdatable(boolean updatable) {
		this.updatable = updatable;
	}
	
	public boolean isUpdatable() {
		return updatable;
	}

	public String getName() {
		if (this.name == null) {
			this.name = SingleElementSymbol.getShortName(this.ID);
		}
		return this.name;
	}
	
}
