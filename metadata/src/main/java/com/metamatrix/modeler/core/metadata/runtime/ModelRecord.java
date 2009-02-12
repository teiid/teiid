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

package com.metamatrix.modeler.core.metadata.runtime;

/**
 * ModelRecord
 */
public interface ModelRecord extends MetadataRecord {
    
    /**
     * Return the URI string for the primary metamodel
     * @return
     */
    String getPrimaryMetamodelUri();
    
    /**
     * Check if OrderBys are supported by these
     * @return true if orderBys are supported
     */
    boolean supportsOrderBy();
    
    /**
     * Check if model supports outer joins
     * @return true if outer joins are supported
     */
    boolean supportsOuterJoin();
    
    /**
     * Check if full table scans are supported
     * @return true if full table scans are supported
     */
    boolean supportsWhereAll();
    
    /**
     * Check if distinct are supported
     * @return true if distinct is supported
     */
    boolean supportsDistinct();
    
    /**
     * Check if joins are supported on this model
     * @return true if joins are supported
     */
    boolean supportsJoin();
    
    /**
     * Check if the model is visible
     * @return
     */
    boolean isVisible();
    
    /**
     * Get the maxSet size allowed
     * @return maximum allowed size in a SET criteria
     */
    int getMaxSetSize();    
    
    /**
     * Check if the model represents a physical model
     * @return
     */
    boolean isPhysical();
    
    /**
     * Return integer indicating the type of Model it is. 
     * @return int
     *
     * @see com.metamatrix.modeler.core.metadata.runtime.MetadataConstants.MODEL_TYPES
     */
    int getModelType();

}
