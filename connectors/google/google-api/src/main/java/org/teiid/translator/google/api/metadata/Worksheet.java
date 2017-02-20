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

package org.teiid.translator.google.api.metadata;


import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public class Worksheet {
	private String id;
	private String name;
	private LinkedHashMap<String, Column> columns = new LinkedHashMap<String, Column>();
	private boolean headerEnabled=false;

	public LinkedHashMap<String, Column> getColumns() {
		return columns;
	}

	public List<Column> getColumnsAsList() {
		return new ArrayList<Column>(columns.values());
	}

	public void addColumn(String label, Column column) {
		columns.put(label, column);
	}

	public String getColumnID(String columnLabel) {
		Column column = columns.get(columnLabel);
		if (column == null) {
			return null;
		} 
		return column.getAlphaName();
	}

	public Worksheet( String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}
	
	public int getColumnCount() {
		return columns.size();
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public boolean isHeaderEnabled() {
		return headerEnabled;
	}

	public void setHeaderEnabled(boolean headerEnabled) {
		this.headerEnabled = headerEnabled;
	}

}
