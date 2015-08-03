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


package org.teiid.query.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.teiid.GeneratedKeys;

public class GeneratedKeysImpl implements GeneratedKeys {
	private List<List<?>> keys = new ArrayList<List<?>>();
	private String[] colNames;
	private Class<?>[] types;
	
	protected GeneratedKeysImpl(String[] colNames, Class<?>[] types) {
		this.colNames = colNames;
		this.types = types;
	}
	
	@Override
	public void addKey(List<?> vals) {
		if (vals != null) {
			keys.add(vals);
		}
	}
	
	public List<List<?>> getKeys() {
		return keys;
	}
	
	public String[] getColumnNames() {
		return colNames;
	}
	
	public Class<?>[] getColumnTypes() {
		return types;
	}
	
	public Iterator<List<?>> getKeyIterator() {
		return keys.iterator();
	}

}
