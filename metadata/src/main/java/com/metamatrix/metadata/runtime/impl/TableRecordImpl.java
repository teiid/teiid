/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.metadata.runtime.impl;

import java.util.Collection;

import com.metamatrix.modeler.core.metadata.runtime.TableRecord;

/**
 * TableRecordImpl
 */
public class TableRecordImpl extends ColumnSetRecordImpl implements TableRecord {

    private int cardinality;
    private int tableType;
    private Object primaryKeyID;
    private Object materializedTableID;
    private Object materializedStageTableID;
    private Collection foreignKeyIDs;
    private Collection indexIDs;
    private Collection uniqueKeyIDs;
    private Collection accessPatternIDs;
    private boolean isVirtual;
    private boolean isSystem;
    private boolean isMaterialized;
    private boolean supportsUpdate;

    // ==================================================================================
    //                        C O N S T R U C T O R S
    // ==================================================================================

    public TableRecordImpl() {
    	this(new MetadataRecordDelegate());
    }
    
    protected TableRecordImpl(MetadataRecordDelegate delegate) {
    	this.delegate = delegate;
    }

    //==================================================================================
    //                     I N T E R F A C E   M E T H O D S
    //==================================================================================

    /* (non-Javadoc)
     * @see com.metamatrix.modeler.core.metadata.runtime.TableRecord#getAccessPatternIDs()
     */
    public Collection getAccessPatternIDs() {
        return accessPatternIDs;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.modeler.core.metadata.runtime.TableRecord#getCardinality()
     */
    public int getCardinality() {
        return cardinality;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.modeler.core.metadata.runtime.TableRecord#getForeignKeyIDs()
     */
    public Collection getForeignKeyIDs() {
        return foreignKeyIDs;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.modeler.core.metadata.runtime.TableRecord#getIndexIDs()
     */
    public Collection getIndexIDs() {
        return indexIDs;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.modeler.core.metadata.runtime.TableRecord#getPrimaryKeyID()
     */
    public Object getPrimaryKeyID() {
        return primaryKeyID;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.modeler.core.metadata.runtime.TableRecord#getUniqueKeyIDs()
     */
    public Collection getUniqueKeyIDs() {
        return uniqueKeyIDs;
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
    public Object getMaterializedStageTableID() {
        return this.materializedStageTableID;
    }
    /**
     * @see com.metamatrix.modeler.core.metadata.runtime.TableRecord#getMaterializedTableID()
     * @since 4.2
     */
    public Object getMaterializedTableID() {
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
    public void setPrimaryKeyID(Object keyID) {
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
     * @param collection
     */
    public void setAccessPatternIDs(Collection collection) {
        accessPatternIDs = collection;
    }

    /**
     * @param collection
     */
    public void setForeignKeyIDs(Collection collection) {
        foreignKeyIDs = collection;
    }

    /**
     * @param materializedStageTableID The materializedStageTableID to set.
     * @since 4.2
     */
    public void setMaterializedStageTableID(Object materializedStageTableID) {
        this.materializedStageTableID = materializedStageTableID;
    }

    /**
     * @param materializedTableID The materializedTableID to set.
     * @since 4.2
     */
    public void setMaterializedTableID(Object materializedTableID) {
        this.materializedTableID = materializedTableID;
    }

    /**
     * @param collection
     */
    public void setIndexIDs(Collection collection) {
        indexIDs = collection;
    }

    /**
     * @param collection
     */
    public void setUniqueKeyIDs(Collection collection) {
        uniqueKeyIDs = collection;
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