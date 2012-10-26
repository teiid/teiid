package org.teiid.resource.adapter.google.metadata;

public class Column {
	private String alphaName;
	 private SpreadsheetColumnType dataType = SpreadsheetColumnType.STRING;

	public String getAlphaName() {
		return alphaName;
	}

	public void setAlphaName(String alphaName) {
		this.alphaName = alphaName;
	}

	public SpreadsheetColumnType getDataType() {
		return dataType;
	}

	public void setDataType(SpreadsheetColumnType dataType) {
		this.dataType = dataType;
	}
}