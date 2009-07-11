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

import java.util.ArrayList;
import java.util.List;

/**
 * ColumnSetRecordImpl
 */
public class ColumnSetRecordImpl extends AbstractMetadataRecord {
    
    private List<String> columnIDs;
    private short type;
    private List<ColumnRecordImpl> columns;
    
    public ColumnSetRecordImpl(short type) {
    	this.type = type;
    }

    public List<ColumnRecordImpl> getColumns() {
    	return columns;
    }

    /** 
     * @see com.metamatrix.modeler.core.metadata.runtime.ColumnSetRecord#getColumnIDs()
     */
    public List<String> getColumnIDs() {
        return columnIDs;
    }
    
    /** 
     * Retrieves a list of ColumnRecordImpls containing only id and position information (used by System Tables)
     */
    public List<ColumnRecordImpl> getColumnIdEntries() {
    	int count = getColumnCount();
        final List<ColumnRecordImpl> entryRecords = new ArrayList<ColumnRecordImpl>(count);
        for (int i = 0; i < count; i++) {
            final String uuid  = getUUID(i);
            ColumnRecordImpl impl = new ColumnRecordImpl();
            entryRecords.add( impl );
            impl.setUUID(uuid);
            impl.setPosition(i+1);
        }
        return entryRecords;
    }
    
    private int getColumnCount() {
    	if (columnIDs != null) {
    		return columnIDs.size();
    	}
    	if (columns != null) {
    		return columns.size();
    	}
    	return 0;
    }
    
    private String getUUID(int index) {
    	if (columnIDs != null) {
    		return columnIDs.get(index);
    	}
		return columns.get(index).getUUID();
    }

    /** 
     * @see com.metamatrix.modeler.core.metadata.runtime.ColumnSetRecord#getType()
     */
    public short getType() {
        return type;
    }

    /**
     * @param list
     */
    public void setColumnIDs(List<String> list) {
        columnIDs = list;
    }
    
    public void setColumns(List<ColumnRecordImpl> columns) {
		this.columns = columns;
	}
    
}