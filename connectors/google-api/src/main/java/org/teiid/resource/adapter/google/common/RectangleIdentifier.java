package org.teiid.resource.adapter.google.common;

/**
 * Represents part of the worksheet
 * 
 *
 */
public class RectangleIdentifier {
	private int rowStart;
	private int rowEnd;
	private int colStart;
	private int colEnd;
	
public RectangleIdentifier(int rowStart, int colStart, int rowEnd, int colEnd) throws SpreadsheetOperationException {
	this.rowStart = rowStart;
	this.rowEnd = rowEnd;
	this.colStart = colStart;
	this.colEnd = colEnd;
	checkBounderies();
}

public RectangleIdentifier(int rowStart, String colStart, int rowEnd, String colEnd) throws SpreadsheetOperationException {
	this.rowStart = rowStart;
	this.rowEnd = rowEnd;
	if(colStart.matches("[A-Za-z]*")){
		this.colStart=Util.convertColumnIDtoInt(colStart);
	}else{
		throw new SpreadsheetOperationException("Column identifier has to contain only letters");
	}
	if(colEnd.matches("[A-Za-z]*")){
		this.colEnd=Util.convertColumnIDtoInt(colEnd);
	}else{
		throw new SpreadsheetOperationException("Column identifier has to contain only letters");
	}
	checkBounderies();
}

private void checkBounderies() throws SpreadsheetOperationException {
	if(rowStart<1 || rowEnd<1){
	   throw new SpreadsheetOperationException("Rows boundaries have to be positive integer");
	}
	if(rowEnd-rowStart<0){
	   throw new SpreadsheetOperationException("End row has to be grater then start row.");
	}
	if(colEnd-colStart<0){
	    throw new SpreadsheetOperationException("End column has to be grater then start column.");
	}	
}

public int getRowStart() {
	return rowStart;
}

public int getRowEnd() {
	return rowEnd;
}

public int getColStart() {
	return colStart;
}

public int getColEnd() {
	return colEnd;
}


}
