package org.teiid.resource.adapter.google.common;

/**
 * Used for errors during authentication or authorization for GoogleSpreadsheet
 * @author fnguyen
 *
 */
public class SpreadsheetAuthException extends RuntimeException {

	public SpreadsheetAuthException() {
		super();
	}

	public SpreadsheetAuthException(String message, Throwable cause) {
		super(message, cause);
	}

	public SpreadsheetAuthException(String message) {
		super(message);
	}

	public SpreadsheetAuthException(Throwable cause) {
		super(cause);
	}
	
}
