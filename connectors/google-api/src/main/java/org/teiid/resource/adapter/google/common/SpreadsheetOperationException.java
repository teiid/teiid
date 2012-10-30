package org.teiid.resource.adapter.google.common;

/**
 * Any error during quering or inserting data to Google Spreadsheet
 * @author fnguyen
 *
 */
public class SpreadsheetOperationException  extends RuntimeException{

	public SpreadsheetOperationException() {
		super();
	}

	public SpreadsheetOperationException(String message, Throwable cause) {
		super(message, cause);
	}

	public SpreadsheetOperationException(String message) {
		super(message);
	}

	public SpreadsheetOperationException(Throwable cause) {
		super(cause);
	}

}
