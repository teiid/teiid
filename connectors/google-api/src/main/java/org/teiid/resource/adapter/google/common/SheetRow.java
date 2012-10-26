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
	private List<String> row = new ArrayList<String>();
	
	public SheetRow() {
	}

	public SheetRow(String [] row) {
		this.row = new ArrayList<String>(Arrays.asList(row));
	}
	

	public void addColumn(String s)	{
		row.add(s);
	}

	public List<String> getRow(){
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
