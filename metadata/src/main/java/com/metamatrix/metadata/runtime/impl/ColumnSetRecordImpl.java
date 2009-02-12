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

package com.metamatrix.metadata.runtime.impl;

import java.util.ArrayList;
import java.util.List;

import com.metamatrix.modeler.core.index.IndexConstants;
import com.metamatrix.modeler.core.metadata.runtime.ColumnSetRecord;
import com.metamatrix.modeler.core.metadata.runtime.ListEntryRecord;
import com.metamatrix.modeler.core.metadata.runtime.MetadataConstants;

/**
 * ColumnSetRecordImpl
 */
public class ColumnSetRecordImpl extends AbstractMetadataRecord implements ColumnSetRecord {
    
    private List columnIDs;

    // ==================================================================================
    //                        C O N S T R U C T O R S
    // ==================================================================================

    public ColumnSetRecordImpl() {
    	this(new MetadataRecordDelegate());
    }
    
    protected ColumnSetRecordImpl(MetadataRecordDelegate delegate) {
    	this.delegate = delegate;
    }

    //==================================================================================
    //                     I N T E R F A C E   M E T H O D S
    //==================================================================================    

    /** 
     * @see com.metamatrix.modeler.core.metadata.runtime.ColumnSetRecord#getColumnIDs()
     */
    public List getColumnIDs() {
        return columnIDs;
    }

    /** 
     * @see com.metamatrix.modeler.core.metadata.runtime.ColumnSetRecord#getColumnIdEntries()
     */
    public ListEntryRecord[] getColumnIdEntries() {
        final List entryRecords = new ArrayList(columnIDs.size());
        for (int i = 0, n = columnIDs.size(); i < n; i++) {
            final String uuid  = (String)columnIDs.get(i);
            final int position = i+1;
            entryRecords.add( new ListEntryRecordImpl(uuid,position) );
        }
        return (ListEntryRecord[])entryRecords.toArray(new ListEntryRecord[entryRecords.size()]);
    }

    /** 
     * @see com.metamatrix.modeler.core.metadata.runtime.ColumnSetRecord#isAccessPattern()
     */
    public boolean isAccessPattern() {
        if (super.getRecordType() == IndexConstants.RECORD_TYPE.ACCESS_PATTERN) {
            return true;
        }
        return false;
    }

    /** 
     * @see com.metamatrix.modeler.core.metadata.runtime.ColumnSetRecord#isIndex()
     */
    public boolean isIndex() {
        if (super.getRecordType() == IndexConstants.RECORD_TYPE.INDEX) {
            return true;
        }
        return false;
    }

    /** 
     * @see com.metamatrix.modeler.core.metadata.runtime.ColumnSetRecord#isPrimaryKey()
     */
    public boolean isPrimaryKey() {
        if (super.getRecordType() == IndexConstants.RECORD_TYPE.PRIMARY_KEY) {
            return true;
        }
        return false;
    }

    /** 
     * @see com.metamatrix.modeler.core.metadata.runtime.ColumnSetRecord#isUniqueKey()
     */
    public boolean isUniqueKey() {
        if (super.getRecordType() == IndexConstants.RECORD_TYPE.UNIQUE_KEY) {
            return true;
        }
        return false;
    }

    /**
     * @see com.metamatrix.modeler.core.metadata.runtime.ColumnSetRecord#isResultSet()
     */
    public boolean isResultSet() {
        if (super.getRecordType() == IndexConstants.RECORD_TYPE.RESULT_SET) {
            return true;
        }
        return false;
    }

    /** 
     * @see com.metamatrix.modeler.core.metadata.runtime.ColumnSetRecord#getType()
     */
    public short getType() {
        return this.getKeyTypeForRecordType(this.getRecordType());
    }

    // ==================================================================================
    //                      P U B L I C   M E T H O D S
    // ==================================================================================

    /**
     * @param list
     */
    public void setColumnIDs(List list) {
        columnIDs = list;
    }
    
    protected short getKeyTypeForRecordType(final char recordType) {
        switch (recordType) {
            case IndexConstants.RECORD_TYPE.UNIQUE_KEY: return MetadataConstants.KEY_TYPES.UNIQUE_KEY;
            case IndexConstants.RECORD_TYPE.INDEX: return MetadataConstants.KEY_TYPES.INDEX;
            case IndexConstants.RECORD_TYPE.ACCESS_PATTERN: return MetadataConstants.KEY_TYPES.ACCESS_PATTERN;
            case IndexConstants.RECORD_TYPE.PRIMARY_KEY: return MetadataConstants.KEY_TYPES.PRIMARY_KEY;
            case IndexConstants.RECORD_TYPE.FOREIGN_KEY: return MetadataConstants.KEY_TYPES.FOREIGN_KEY;
            default:
                throw new IllegalArgumentException("Invalid record type, for key" + recordType); //$NON-NLS-1$
        }
    }

}