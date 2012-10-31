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

package org.teiid.resource.adapter.google.common;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Object represeting row in Google Spreadsheets.
 * 
 *  TODO metadata should be somehow loaded from google spreadsheets so that the cell values are typed (integer, string etc)
 * @author fnguyen
 *
 */
public class SheetRow {
	private List<Object> row = new ArrayList<Object>();
	
	public SheetRow() {
	}

	public SheetRow(String [] row) {
		this.row = new ArrayList<Object>(Arrays.asList(row));
	}
	

	public void addColumn(Object s)	{
		row.add(s);
	}

	public List<Object> getRow(){
		return row;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((row == null) ? 0 : row.hashCode());
		return result;
	}


	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SheetRow other = (SheetRow) obj;
		if (row == null) {
			if (other.row != null)
				return false;
		} else if (!row.equals(other.row))
			return false;
		return true;
	}
//	
	@Override
	public String toString(){
		return Arrays.toString(row.toArray());
	}

	
}
