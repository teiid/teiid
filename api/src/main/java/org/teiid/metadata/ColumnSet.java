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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class ColumnSet<T extends AbstractMetadataRecord> extends AbstractMetadataRecord {
	
	private static final long serialVersionUID = -1185104601468519829L;

	private List<Column> columns;
    private T parent;
    private transient Map<String, Column> columnMap;
    
    public List<Column> getColumns() {
    	return columns;
    }
    
    /**
     * Get the {@link Column} via a case-insensitive lookup
     * @param name
     * @return the {@link Column} or null if it doesn't exist
     */
    public Column getColumnByName(String name) {
    	if (columns == null || name == null) {
    		return null;
    	}
    	Map<String, Column> map = columnMap;
    	if (map == null) {
    		map = new TreeMap<String, Column>(String.CASE_INSENSITIVE_ORDER);
    		for (Column c : columns) {
				map.put(c.getName(), c);
			}
    		columnMap = map;
    	}
    	return map.get(name);
    }
    
    public void addColumn(Column column) {
    	if (columns == null) {
    		columns = new ArrayList<Column>();
    	}
    	columns.add(column);
    	Map<String, Column> map = columnMap;
    	if (map != null) {
    		map.put(column.getName(), column);
    	}
    }

    public void setColumns(List<Column> columns) {
		this.columns = columns;
		columnMap = null;
	}
    
    @Override
    public T getParent() {
    	return parent;
    }
    
    public void setParent(T parent) {
		this.parent = parent;
	}

}