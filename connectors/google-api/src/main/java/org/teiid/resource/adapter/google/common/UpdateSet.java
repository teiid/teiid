package org.teiid.resource.adapter.google.common;

public class UpdateSet {

	String columnID;
	String value;
	
	public UpdateSet(String columnID, String value){
		this.columnID = columnID;
		this.value = value;
	}

	public String getColumnID() {
		return columnID;
	}

	public void setColumnID(String columnID) {
		this.columnID = columnID;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}
	
	
	
}
