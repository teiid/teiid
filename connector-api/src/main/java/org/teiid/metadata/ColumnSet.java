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

public class ColumnSet<T extends AbstractMetadataRecord> extends AbstractMetadataRecord {
	
	private static final long serialVersionUID = -1185104601468519829L;

	private List<Column> columns;
    private T parent;
    
    public List<Column> getColumns() {
    	return columns;
    }
    
    public void addColumn(Column column) {
    	if (columns == null) {
    		columns = new ArrayList<Column>();
    	}
    	columns.add(column);
    }

    public void setColumns(List<Column> columns) {
		this.columns = columns;
	}
    
    @Override
    public T getParent() {
    	return parent;
    }
    
    public void setParent(T parent) {
		this.parent = parent;
	}

}