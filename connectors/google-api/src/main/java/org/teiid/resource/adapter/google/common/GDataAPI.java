package org.teiid.resource.adapter.google.common;

import java.util.Set;

import org.teiid.resource.adapter.google.metadata.SpreadsheetInfo;
import org.teiid.resource.adapter.google.result.RowsResult;

/**
 * GData API is older API + Java library for Google Spreadsheet service. 
 * 
 * http://code.google.com/p/gdata-java-client/
 * 
 * @author fnguyen
 *
 */
public interface GDataAPI {
	public String getSpreadsheetKeyByTitle(String sheetName);
	
	/**
	 * Returns all rows of the specified spreadsheet.
	 * @param spreadsheetTitle
	 * @param worksheetTitle
	 * @param batchSize
	 * @return
	 */
	public RowsResult getSpreadsheetRows(String spreadsheetTitle, String worksheetTitle, int batchSize);
		
}
