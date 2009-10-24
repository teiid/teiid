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

package org.teiid.connector.metadata.runtime;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * TableRecordImpl
 */
public class TableRecordImpl extends ColumnSetRecordImpl {

	public enum Type {
		Table,
		View,
		Document,
		XmlMappingClass,
		XmlStagingTable,
		MaterializedTable
	}
	
    private int cardinality;
    private Type tableType;
    private String materializedTableID;
    private String materializedStageTableID;
    private boolean isVirtual;
    private boolean isSystem;
    private boolean isMaterialized;
    private boolean supportsUpdate;
    private List<ForeignKeyRecordImpl> foriegnKeys;
    private List<KeyRecord> indexes;
    private List<KeyRecord> uniqueKeys;
    private List<KeyRecord> accessPatterns;
    private KeyRecord primaryKey;

    //view information
	private String selectTransformation;
    private String insertPlan;
    private String updatePlan;
    private String deletePlan;
    private String materializedStageTableName;
    private String materializedTableName;

    //XML specific
    private List<String> bindings;
	private List<String> schemaPaths;
	private String resourcePath;
	
    public List<String> getBindings() {
		return bindings;
	}

	public void setBindings(List<String> bindings) {
		this.bindings = bindings;
	}

	public List<String> getSchemaPaths() {
		return schemaPaths;
	}

	public void setSchemaPaths(List<String> schemaPaths) {
		this.schemaPaths = schemaPaths;
	}

    public int getCardinality() {
        return cardinality;
    }

    public boolean isVirtual() {
        return isVirtual;
    }

    public boolean isMaterialized() {
        return isMaterialized;
    }

    public boolean isPhysical() {
        return !isVirtual();
    }

    public boolean isSystem() {
        return isSystem;
    }

    public Type getTableType() {
    	if (tableType == null) {
    		return Type.Table;
    	}
        return tableType;
    }

    public String getMaterializedStageTableID() {
        return this.materializedStageTableID;
    }

    public String getMaterializedTableID() {
        return this.materializedTableID;
    }

    public boolean supportsUpdate() {
        return supportsUpdate;
    }

    /**
     * @param i
     */
    public void setCardinality(int i) {
        cardinality = i;
    }

    /**
     * @param i
     */
    public void setTableType(Type i) {
        tableType = i;
    }

    /**
     * @param b
     */
    public void setSupportsUpdate(boolean b) {
        supportsUpdate = b;
    }

    /**
     * @param b
     */
    public void setVirtual(boolean b) {
		isVirtual = b;
    }

    /**
     * @param isMaterialized The isMaterialized to set.
     * @since 4.2
     */
    public void setMaterialized(boolean isMaterialized) {
        this.isMaterialized = isMaterialized;
    }

    /**
     * @param b
     */
    public void setSystem(boolean b) {
        isSystem = b;
    }

    /**
     * @param materializedStageTableID The materializedStageTableID to set.
     * @since 4.2
     */
    public void setMaterializedStageTableID(String materializedStageTableID) {
        this.materializedStageTableID = materializedStageTableID;
    }

    /**
     * @param materializedTableID The materializedTableID to set.
     * @since 4.2
     */
    public void setMaterializedTableID(String materializedTableID) {
        this.materializedTableID = materializedTableID;
    }

    public String getInsertPlan() {
    	return insertPlan;
    }
    
    public String getUpdatePlan() {
    	return updatePlan;
    }
    
    public String getDeletePlan() {
    	return deletePlan;
    }
    
    public void setInsertPlan(String insertPlan) {
		this.insertPlan = insertPlan;
	}
    
    public void setUpdatePlan(String updatePlan) {
		this.updatePlan = updatePlan;
	}
    
    public void setDeletePlan(String deletePlan) {
		this.deletePlan = deletePlan;
	}
    
    public List<ForeignKeyRecordImpl> getForeignKeys() {
    	return this.foriegnKeys;
    }
    
    public void setForiegnKeys(List<ForeignKeyRecordImpl> foriegnKeys) {
		this.foriegnKeys = foriegnKeys;
	}
    
    public List<KeyRecord> getIndexes() {
    	return this.indexes;
    }
    
    public void setIndexes(List<KeyRecord> indexes) {
		this.indexes = indexes;
	}
    
    public List<KeyRecord> getUniqueKeys() {
    	return this.uniqueKeys;
    }
    
    public void setUniqueKeys(List<KeyRecord> uniqueKeys) {
		this.uniqueKeys = uniqueKeys;
	}
    
    public List<KeyRecord> getAccessPatterns() {
    	return this.accessPatterns;
    }
    
    public void setAccessPatterns(List<KeyRecord> accessPatterns) {
		this.accessPatterns = accessPatterns;
	}
    
    public KeyRecord getPrimaryKey() {
    	return this.primaryKey;
    }
    
    public void setPrimaryKey(KeyRecord primaryKey) {
		this.primaryKey = primaryKey;
	}
    
    public String getSelectTransformation() {
		return selectTransformation;
	}
    
    public void setSelectTransformation(String selectTransformation) {
		this.selectTransformation = selectTransformation;
	}
    
    public String getMaterializedStageTableName() {
    	return this.materializedStageTableName;
    }
    
    public String getMaterializedTableName() {
    	return this.materializedTableName;
    }
    
    public void setMaterializedStageTableName(String materializedStageTableName) {
		this.materializedStageTableName = materializedStageTableName;
	}
    
    public void setMaterializedTableName(String materializedTableName) {
		this.materializedTableName = materializedTableName;
	}

	public void setResourcePath(String resourcePath) {
		this.resourcePath = resourcePath;
	}

	public String getResourcePath() {
		return resourcePath;
	}
	
	public Collection<KeyRecord> getAllKeys() { 
		Collection<KeyRecord> keys = new LinkedList<KeyRecord>();
		if (getPrimaryKey() != null) {
			keys.add(getPrimaryKey());
		}
		keys.addAll(getForeignKeys());
		keys.addAll(getAccessPatterns());
		keys.addAll(getIndexes());
		keys.addAll(getUniqueKeys());
		return keys;
	}
	
    public String toString() {
        StringBuffer sb = new StringBuffer(100);
        sb.append(getClass().getSimpleName());
        sb.append(" name="); //$NON-NLS-1$
        sb.append(getName());
        sb.append(", nameInSource="); //$NON-NLS-1$
        sb.append(getNameInSource());
        sb.append(", uuid="); //$NON-NLS-1$
        sb.append(getUUID());
        return sb.toString();
    }

}