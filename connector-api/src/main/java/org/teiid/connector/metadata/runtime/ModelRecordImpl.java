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

import com.metamatrix.core.vdb.ModelType;

/**
 * ModelRecordImpl
 */
public class ModelRecordImpl extends AbstractMetadataRecord {

    private int modelType;
    private int maxSetSize;
    private boolean isVisible = true;
    private boolean supportsDistinct;
    private boolean supportsJoin;
    private boolean supportsOrderBy;
    private boolean supportsOuterJoin;
    private boolean supportsWhereAll;
    private String primaryMetamodelUri;

    /**
     * @see com.metamatrix.modeler.core.metadata.runtime.ModelRecord#getPrimaryMetamodelUri()
     */
    public String getPrimaryMetamodelUri() {
        return primaryMetamodelUri;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.modeler.core.metadata.runtime.ModelRecord#getMaxSetSize()
     */
    public int getMaxSetSize() {
        return maxSetSize;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.modeler.core.metadata.runtime.ModelRecord#isVisible()
     */
    public boolean isVisible() {
        return isVisible;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.modeler.core.metadata.runtime.ModelRecord#supportsDistinct()
     */
    public boolean supportsDistinct() {
        return supportsDistinct;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.modeler.core.metadata.runtime.ModelRecord#supportsJoin()
     */
    public boolean supportsJoin() {
        return supportsJoin;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.modeler.core.metadata.runtime.ModelRecord#supportsOrderBy()
     */
    public boolean supportsOrderBy() {
        return supportsOrderBy;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.modeler.core.metadata.runtime.ModelRecord#supportsOuterJoin()
     */
    public boolean supportsOuterJoin() {
        return supportsOuterJoin;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.modeler.core.metadata.runtime.ModelRecord#supportsWhereAll()
     */
    public boolean supportsWhereAll() {
        return supportsWhereAll;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.modeler.core.metadata.runtime.ModelRecord#getModelType()
     */
    public int getModelType() {
        return modelType;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.modeler.core.metadata.runtime.ModelRecord#isPhysical()
     */
    public boolean isPhysical() {
        if (getModelType() == ModelType.PHYSICAL) {
            return true;
        }
        return false;
    }

    // ==================================================================================
    //                      P U B L I C   M E T H O D S
    // ==================================================================================

    /**
     * @param string
     */
    public void setPrimaryMetamodelUri(String string) {
        primaryMetamodelUri = string;
    }

    /**
     * @param b
     */
    public void setVisible(boolean b) {
        isVisible = b;
    }

    /**
     * @param i
     */
    public void setMaxSetSize(int i) {
        maxSetSize = i;
    }

    /**
     * @param b
     */
    public void setSupportsDistinct(boolean b) {
        supportsDistinct = b;
    }

    /**
     * @param b
     */
    public void setSupportsJoin(boolean b) {
        supportsJoin = b;
    }

    /**
     * @param b
     */
    public void setSupportsOrderBy(boolean b) {
        supportsOrderBy = b;
    }

    /**
     * @param b
     */
    public void setSupportsOuterJoin(boolean b) {
        supportsOuterJoin = b;
    }

    /**
     * @param b
     */
    public void setSupportsWhereAll(boolean b) {
        supportsWhereAll = b;
    }

    /**
     * @param i
     */
    public void setModelType(int i) {
        modelType = i;
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
