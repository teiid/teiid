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

package com.metamatrix.modeler.core.metadata.runtime;

import java.util.Collection;
import java.util.List;

/**
 * TableRecord
 */
public interface TableRecord extends MetadataRecord  {
    
    /**
     * Constants for perperties stored on a TableRecord 
     * @since 4.3
     */
    public interface TableRecordProperties {

        String ELEMENTS_IN_GROUP = "elementsInGroup";  //$NON-NLS-1$
        String INDEXES_IN_GROUP = "indexesInGroup";  //$NON-NLS-1$
        String UNIQUEKEYS_IN_GROUP = "uksInGroup";  //$NON-NLS-1$
        String FOREIGNKEYS_IN_GROUP = "fksInGroup";  //$NON-NLS-1$
        String ACCESS_PTTRNS_IN_GROUP = "accPttrnsInGroup";  //$NON-NLS-1$
        String QUERY_PLAN        = "queryPlan";  //$NON-NLS-1$
        String INSERT_PLAN       = "insertPlan";  //$NON-NLS-1$
        String UPDATE_PLAN       = "updatePlan";  //$NON-NLS-1$
        String DELETE_PLAN       = "deletePlan";  //$NON-NLS-1$
        String MAPPING_NODE_FOR_RECORD  = "mappingNodeForRecord";  //$NON-NLS-1$
        String TEMPORARY_GROUPS_FOR_DOCUMENT  = "tempGroupsForDocument";  //$NON-NLS-1$
        String SCHEMAS_FOR_DOCUMENT  = "schemasForDocument";  //$NON-NLS-1$
        
    }    
    
    /**
     * Check if UPDATE operations are supported on the table
     * @return true if the table can be used in an UPDATE
     */
    boolean supportsUpdate();

    /**
     * Check if table represents a table in a virtual model
     * @return true if the table is virtual
     */
    boolean isVirtual();

    /**
     * Check if table represents a table in a physical model
     * @return true if the table is physical
     */
    boolean isPhysical();

    /**
     * Check if table represents a table in a system model
     * @return true if the table is system
     */
    boolean isSystem();
    
    /**
     * Check if table represents a materialized table
     * @return true if the table is materialized
     */
    boolean isMaterialized();

    /**
     * Return the table type of this table
     * @see com.metamatrix.modeler.core.metadata.runtime.MetadataConstants.TABLE_TYPES
     * @return a int value from the available table types
     */    
    int getTableType();

    /**
     * Get a list of identifiers for the columns in the table
     * @return a list of identifiers
     * @deprecated columnIDs are no longer stored in the record. Columns can be
     * retrieved by querying for all columns having the same qualified parent table name.
     */
    List getColumnIDs();

    /**
     * Get a list of identifiers for the indexes in the table
     * @return a list of identifiers
     */    
    Collection getIndexIDs();

    /**
     * Get a list of identifiers for the unique keys in the table
     * @return a list of identifiers
     */    
    Collection getUniqueKeyIDs();
    
    /**
     * Get a list of identifiers for the foreign keys in the table
     * @return a list of identifiers
     */    
    Collection getForeignKeyIDs();

    /**
     * Get a materialized table identifier for this table
     * @return an identifier for materialized table
     */
    Object getMaterializedTableID();

    /**
     * Get a materialized staging table identifier for this table
     * @return an identifier for materialized staging table
     */
    Object getMaterializedStageTableID();

    /**
     * Get a primary key identifier in the table
     * @return an identifier for the primary key
     */    
    Object getPrimaryKeyID();

    /**
     * Get a list of identifiers for the access patterns in the table
     * @return a list of identifiers
     */    
    Collection getAccessPatternIDs();
    
    /**
     * Get a cardinality of the table in the table
     * @return the cardinality
     */    
    int getCardinality();    

}
