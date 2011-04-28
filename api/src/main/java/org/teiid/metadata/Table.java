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

package org.teiid.metadata;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.teiid.core.types.DataTypeManager;

public class Table extends ColumnSet<Schema> {

	private static final long serialVersionUID = 4891356771125218672L;

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
    private boolean isVirtual;
    private boolean isSystem;
    private boolean isMaterialized;
    private boolean supportsUpdate;
    private List<ForeignKey> foriegnKeys = new LinkedList<ForeignKey>();
    private List<KeyRecord> indexes = new LinkedList<KeyRecord>();
    private List<KeyRecord> uniqueKeys = new LinkedList<KeyRecord>();
    private List<KeyRecord> accessPatterns = new LinkedList<KeyRecord>();
    private KeyRecord primaryKey;

    //view information
	private String selectTransformation;
    private String insertPlan;
    private String updatePlan;
    private String deletePlan;
    private Table materializedStageTable;
    private Table materializedTable;

    //XML specific
    private List<String> bindings;
	private List<String> schemaPaths;
	private String resourcePath;
	
	private transient long lastModified;
	private transient long lastDataModification;
	
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
		this.insertPlan = DataTypeManager.getCanonicalString(insertPlan);
	}
    
    public void setUpdatePlan(String updatePlan) {
		this.updatePlan = DataTypeManager.getCanonicalString(updatePlan);
	}
    
    public void setDeletePlan(String deletePlan) {
		this.deletePlan = DataTypeManager.getCanonicalString(deletePlan);
	}
    
    public List<ForeignKey> getForeignKeys() {
    	return this.foriegnKeys;
    }
    
    public void setForiegnKeys(List<ForeignKey> foriegnKeys) {
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
		this.selectTransformation = DataTypeManager.getCanonicalString(selectTransformation);
	}
    
    public Table getMaterializedStageTable() {
		return materializedStageTable;
	}
    
    public Table getMaterializedTable() {
		return materializedTable;
	}
    
    public void setMaterializedStageTable(Table materializedStageTable) {
		this.materializedStageTable = materializedStageTable;
	}
    
    public void setMaterializedTable(Table materializedTable) {
		this.materializedTable = materializedTable;
	}

	public void setResourcePath(String resourcePath) {
		this.resourcePath = DataTypeManager.getCanonicalString(resourcePath);
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
	
	@Override
	public void addColumn(Column column) {
		super.addColumn(column);
		column.setParent(this);
	}
	
	public long getLastDataModification() {
		return lastDataModification;
	}
	
	public long getLastModified() {
		return lastModified;
	}
	
	public void setLastDataModification(long lastDataModification) {
		this.lastDataModification = lastDataModification;
	}
	
	public void setLastModified(long lastModified) {
		this.lastModified = lastModified;
	}
	
	public void setTableStats(TableStats stats) {
		if (stats.getCardinality() != null) {
			setCardinality(stats.getCardinality());
		}
	}
	
}