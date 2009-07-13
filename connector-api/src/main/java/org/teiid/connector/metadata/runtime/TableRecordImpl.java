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

/**
 * TableRecordImpl
 */
public class TableRecordImpl extends ColumnSetRecordImpl {

    private int cardinality;
    private int tableType;
    private String primaryKeyID;
    private String materializedTableID;
    private String materializedStageTableID;
    private boolean isVirtual;
    private boolean isSystem;
    private boolean isMaterialized;
    private boolean supportsUpdate;
    private String insertPlan;
    private String updatePlan;
    private String deletePlan;
    private Collection<ForeignKeyRecordImpl> foriegnKeys;
    private Collection<ColumnSetRecordImpl> indexes;
    private Collection<ColumnSetRecordImpl> uniqueKeys;
    private Collection<ColumnSetRecordImpl> accessPatterns;
    private ColumnSetRecordImpl primaryKey;
    private TransformationRecordImpl selectTransformation;
    private String materializedStageTableName;
    private String materializedTableName;
    
    public TableRecordImpl() {
		super((short)-1);
	}
    
    //==================================================================================
    //                     I N T E R F A C E   M E T H O D S
    //==================================================================================

    /* (non-Javadoc)
     * @see com.metamatrix.modeler.core.metadata.runtime.TableRecord#getCardinality()
     */
    public int getCardinality() {
        return cardinality;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.modeler.core.metadata.runtime.TableRecord#getPrimaryKeyID()
     */
    public String getPrimaryKeyID() {
        return primaryKeyID;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.modeler.core.metadata.runtime.TableRecord#isVirtual()
     */
    public boolean isVirtual() {
        return isVirtual;
    }

    /**
     * @see com.metamatrix.modeler.core.metadata.runtime.TableRecord#isMaterialized()
     * @since 4.2
     */
    public boolean isMaterialized() {
        return isMaterialized;
    }

    /**
     * @see com.metamatrix.modeler.core.metadata.runtime.TableRecord#isPhysical()
     */
    public boolean isPhysical() {
        return !isVirtual();
    }

    /**
     * @see com.metamatrix.modeler.core.metadata.runtime.TableRecord#isSystem()
     */
    public boolean isSystem() {
        return isSystem;
    }

    /*
     * @see com.metamatrix.modeler.core.metadata.runtime.TableRecord#getTableType()
     */
    public int getTableType() {
        return tableType;
    }

    /**
     * @see com.metamatrix.modeler.core.metadata.runtime.TableRecord#getMaterializedStageTableID()
     * @since 4.2
     */
    public String getMaterializedStageTableID() {
        return this.materializedStageTableID;
    }
    /**
     * @see com.metamatrix.modeler.core.metadata.runtime.TableRecord#getMaterializedTableID()
     * @since 4.2
     */
    public String getMaterializedTableID() {
        return this.materializedTableID;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.modeler.core.metadata.runtime.TableRecord#supportsUpdate()
     */
    public boolean supportsUpdate() {
        return supportsUpdate;
    }

    // ==================================================================================
    //                      P U B L I C   M E T H O D S
    // ==================================================================================

    /**
     * @param i
     */
    public void setCardinality(int i) {
        cardinality = i;
    }

    /**
     * @param i
     */
    public void setTableType(int i) {
        tableType = i;
    }

    /**
     * @param object
     */
    public void setPrimaryKeyID(String keyID) {
        primaryKeyID = keyID;
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
    
    public Collection<ForeignKeyRecordImpl> getForeignKeys() {
    	return this.foriegnKeys;
    }
    
    public void setForiegnKeys(Collection<ForeignKeyRecordImpl> foriegnKeys) {
		this.foriegnKeys = foriegnKeys;
	}
    
    public Collection<ColumnSetRecordImpl> getIndexes() {
    	return this.indexes;
    }
    
    public void setIndexes(Collection<ColumnSetRecordImpl> indexes) {
		this.indexes = indexes;
	}
    
    public Collection<ColumnSetRecordImpl> getUniqueKeys() {
    	return this.uniqueKeys;
    }
    
    public void setUniqueKeys(Collection<ColumnSetRecordImpl> uniqueKeys) {
		this.uniqueKeys = uniqueKeys;
	}
    
    public Collection<ColumnSetRecordImpl> getAccessPatterns() {
    	return this.accessPatterns;
    }
    
    public void setAccessPatterns(Collection<ColumnSetRecordImpl> accessPatterns) {
		this.accessPatterns = accessPatterns;
	}
    
    public ColumnSetRecordImpl getPrimaryKey() {
    	return this.primaryKey;
    }
    
    public void setPrimaryKey(ColumnSetRecordImpl primaryKey) {
		this.primaryKey = primaryKey;
	}
    
    public TransformationRecordImpl getSelectTransformation() {
		return selectTransformation;
	}
    
    public void setSelectTransformation(
			TransformationRecordImpl selectTransformation) {
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

    public String toString() {
        StringBuffer sb = new StringBuffer(100);
        sb.append(getClass().getSimpleName());
        sb.append(" name="); //$NON-NLS-1$
        sb.append(getName());
        sb.append(", nameInSource="); //$NON-NLS-1$
        sb.append(getNameInSource());
        sb.append(", uuid="); //$NON-NLS-1$
        sb.append(getUUID());
        sb.append(", pathInModel="); //$NON-NLS-1$
        sb.append(getPath());
        return sb.toString();
    }

}