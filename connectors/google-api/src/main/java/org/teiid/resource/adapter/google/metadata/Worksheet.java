package org.teiid.resource.adapter.google.metadata;


import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.teiid.resource.adapter.google.common.Util;

public class Worksheet implements Iterable<Column> {
	private String name;
	/**
	 * Header names indexed by alpha col names
	 */
	private Map<String,Column> columnsByAlpha = new TreeMap<String,Column>();
	private int columnCount = 0;


	public void setColumnCount(int columnCount) {
		this.columnCount = columnCount;
	}

	public Worksheet( String name) {
		this.name = name;
	}
	

	public String getName() {
		return name;
	}
	
	
	public Iterator<Column> iterator(){
		return new Iterator<Column>() {
			private int position = 0;
			@Override
			public void remove() {				
			}
			
			@Override
			public Column next() {
				return getColumn(++position);
			}
			
			@Override
			public boolean hasNext() {
				return position+1 <= columnCount; 
			}
		};
	}
	
	public Column getColumn(int col){
		return getColumn(Util.convertColumnIDtoString(col));
	}
	
	/**
	 * Ensures Column is in map
	 * @param header
	 * @return
	 */
	public Column getColumn(String alpha) {
		if (columnsByAlpha.containsKey(alpha))
			return columnsByAlpha.get(alpha);
		
		Column newCol = new  Column();
		newCol.setAlphaName(alpha);
		columnsByAlpha.put(alpha, newCol);
		return newCol;
	}

	public int getColumnCount() {
		// TODO Auto-generated method stub
		return columnCount;
	}



	
	
}
