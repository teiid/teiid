package org.teiid.resource.adapter.google.common;


/**
 * This class represents number of updated rows
 * 
 * @author felias
 * 
 */
public class UpdateResult {

	private int expectedNumberOfRows = -1;
	private int actualNumberOfRows = -1;

	/**
	 * 
	 * @param expectedNumberOfRows
	 *            number of rows that should have been updated
	 * @param actualNumberOfRows
	 *            actual number of updated rows
	 */
	public UpdateResult(int expectedNumberOfRows, int actualNumberOfRows) {
		this.expectedNumberOfRows = expectedNumberOfRows;
		this.actualNumberOfRows = actualNumberOfRows;
	}

	/*
	 * Returns number of rows that should have been updated
	 */
	public int getExpectedNumberOfRows() {
		return expectedNumberOfRows;
	}

	/**
	 * Returns actual number of updated rows
	 * 
	 */
	public int getActualNumberOfRows() {
		return actualNumberOfRows;
	}

	/**
	 * Checks whether all rows have been properly updated
	 * 
	 * @return true if all rows have been updated, false if update of one or
	 *         more rows failed
	 */
	public boolean checkResult() {
		if (expectedNumberOfRows == actualNumberOfRows) {
			return true;
		} else if (expectedNumberOfRows > actualNumberOfRows) {
			return false;
		} else {
			throw new SpreadsheetOperationException("Internal error: Number of updated rows is higher than expected.");
		}
	}

}
