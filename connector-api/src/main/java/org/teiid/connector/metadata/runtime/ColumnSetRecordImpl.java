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
    	if (columns != null) {
            final List<ColumnRecordImpl> entryRecords = new ArrayList<ColumnRecordImpl>(columns.size());
            for (int i = 0, n = columns.size(); i < n; i++) {
            	ColumnRecordImpl columnRecordImpl  = columns.get(i);
                final int position = i+1;
                ColumnRecordImpl impl = new ColumnRecordImpl();
                entryRecords.add( impl );
                impl.setUUID(columnRecordImpl.getUUID());
                impl.setPosition(position);
            }
            return entryRecords;
    	}
        final List<ColumnRecordImpl> entryRecords = new ArrayList<ColumnRecordImpl>(columnIDs.size());
        for (int i = 0, n = columnIDs.size(); i < n; i++) {
            final String uuid  = columnIDs.get(i);
            final int position = i+1;
            ColumnRecordImpl impl = new ColumnRecordImpl();
            entryRecords.add( impl );
            impl.setUUID(uuid);
            impl.setPosition(position);
        }
        return entryRecords;
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