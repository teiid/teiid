package org.teiid.resource.adapter.google.gdata;

import java.util.Iterator;

import org.teiid.resource.adapter.google.common.SheetRow;
import org.teiid.resource.adapter.google.common.SpreadsheetOperationException;
import org.teiid.resource.adapter.google.dataprotocol.GoogleDataProtocolAPI;
import org.teiid.resource.adapter.google.metadata.SpreadsheetInfo;
import org.teiid.resource.adapter.google.metadata.Worksheet;
import org.teiid.resource.adapter.google.result.RowsResult;

import com.google.gdata.data.spreadsheet.SpreadsheetEntry;
import com.google.gdata.data.spreadsheet.WorksheetEntry;

/**
 * Creates metadata by using GData API.
 * 
 * We retrieve worksheet names and possibly headers
 * @author fnguyen
 *
 */
public class SpreadsheetMetadataExtractor {
	private GDataClientLoginAPI gdataAPI = null;
	private GoogleDataProtocolAPI visualizationAPI= null;

	public GoogleDataProtocolAPI getVisualizationAPI() {
		return visualizationAPI;
	}

	public void setVisualizationAPI(GoogleDataProtocolAPI visualizationAPI) {
		this.visualizationAPI = visualizationAPI;
	}

	public GDataClientLoginAPI getGdataAPI() {
		return gdataAPI;
	}

	public void setGdataAPI(GDataClientLoginAPI gdataAPI) {
		this.gdataAPI = gdataAPI;
	}
	
	
	public SpreadsheetInfo extractMetadata(String spreadsheetName){
		SpreadsheetEntry sentry = gdataAPI
				.getSpreadsheetEntryByTitle(spreadsheetName);
		SpreadsheetInfo metadata = new SpreadsheetInfo(spreadsheetName);
		try {
			for (WorksheetEntry wentry : sentry.getWorksheets()) {
				String title = wentry.getTitle().getPlainText();
				Worksheet worksheet = metadata.createWorksheet(title);
				RowsResult rr = visualizationAPI.executeQuery(spreadsheetName, title, "SELECT *", 1,0,1);
				Iterator<SheetRow> resultIterator= rr.iterator();
				if (resultIterator.hasNext()){
					worksheet.setColumnCount(resultIterator.next().getRow().size());
				}else {
					worksheet.setColumnCount(0);
				}

			}
		} catch (Exception ex) {
			throw new SpreadsheetOperationException(
					"Error getting metadata about Spreadsheets worksheet", ex);
		}
		return metadata;
	}

}

