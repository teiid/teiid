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

import java.util.List;

/**
 * ColumnSetRecord
 */
public interface ColumnSetRecord extends MetadataRecord {

    /**
     * Constants for perperties stored on a ColumnSetRecord 
     * @since 4.3
     */
    public interface ColumnSetRecordProperties {

        String ELEMENTS_IN_INDEX = "elementsInIndex";  //$NON-NLS-1$
        String ELEMENTS_IN_KEY = "elementsInKey";  //$NON-NLS-1$
        String ELEMENTS_IN_ACCESS_PTTRN = "elementsInAccPttrn";  //$NON-NLS-1$
    }

    /**
     * Get a list of identifiers for the columns in the record
     * @return a list of identifiers
     */
    List getColumnIDs();

    /**
     * Get a list of identifiers for the columns in the record
     * @return a list of identifiers
     */
    ListEntryRecord[] getColumnIdEntries();

    /**
     * Return true if the record represents a primary key
     * @return
     */
    boolean isPrimaryKey();

    /**
     * Return true if the record represents a index
     * @return
     */
    boolean isIndex();

    /**
     * Return true if the record represents a access pattern
     * @return
     */
    boolean isAccessPattern();

    /**
     * Return true if the record represents a unique key
     * @return
     */
    boolean isUniqueKey();

    /**
     * Return true if the record represents a result set
     * @return
     */
    boolean isResultSet();
    
    /**
     * Return short indicating the type of KEY it is. 
     * @return short
     *
     * @see com.metamatrix.modeler.core.metadata.runtime.MetadataConstants.KEY_TYPES
     */
    short getType();
}
