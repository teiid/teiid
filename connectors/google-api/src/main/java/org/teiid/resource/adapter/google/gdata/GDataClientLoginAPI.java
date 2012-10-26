package org.teiid.resource.adapter.google.gdata;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.teiid.resource.adapter.google.auth.AuthHeaderFactory;
import org.teiid.resource.adapter.google.common.GDataAPI;
import org.teiid.resource.adapter.google.common.RectangleIdentifier;
import org.teiid.resource.adapter.google.common.SheetRow;
import org.teiid.resource.adapter.google.common.SpreadsheetOperationException;
import org.teiid.resource.adapter.google.metadata.SpreadsheetInfo;
import org.teiid.resource.adapter.google.result.PartialResultExecutor;
import org.teiid.resource.adapter.google.result.RowsResult;

import com.google.gdata.client.spreadsheet.CellQuery;
import com.google.gdata.client.spreadsheet.FeedURLFactory;
import com.google.gdata.client.spreadsheet.SpreadsheetQuery;
import com.google.gdata.client.spreadsheet.SpreadsheetService;
import com.google.gdata.client.spreadsheet.WorksheetQuery;
import com.google.gdata.data.spreadsheet.CellEntry;
import com.google.gdata.data.spreadsheet.CellFeed;
import com.google.gdata.data.spreadsheet.SpreadsheetEntry;
import com.google.gdata.data.spreadsheet.SpreadsheetFeed;
import com.google.gdata.data.spreadsheet.WorksheetEntry;
import com.google.gdata.data.spreadsheet.WorksheetFeed;

/**
 * Spreadsheet browser implemented by gdata-java-client: http://code.google.com/p/gdata-java-client/
 * 
 * This browser authenticates using Client Login. 
 * 
 * @author fnguyen
 *
 */
public class GDataClientLoginAPI implements GDataAPI {
	private SpreadsheetService service;
	private FeedURLFactory factory;
	private AuthHeaderFactory headerFactory = null;
	
			
	public void setHeaderFactory(AuthHeaderFactory headerFactory) {
		this.headerFactory = headerFactory;
		service.setHeader("Authorization" , headerFactory.getAuthHeader());
	}

	public GDataClientLoginAPI() {
		service = new SpreadsheetService("GdataSpreadsheetBrowser");
		this.factory = FeedURLFactory.getDefault();
	}

	public SpreadsheetEntry getSpreadsheetEntryByTitle(String sheetTitle) {
		SpreadsheetQuery squery = new SpreadsheetQuery(factory.getSpreadsheetsFeedUrl());
		squery.setTitleExact(true);
		squery.setTitleQuery(sheetTitle);
		SpreadsheetFeed feed = getSpreadsheetFeedQuery(squery);
		List<SpreadsheetEntry> entry = feed.getEntries();
		if (entry.size() == 0)
			throw new SpreadsheetOperationException("Couldn't find spreadsheet:"+ sheetTitle);
		
		return entry.get(0);
	}
	

	public String getSpreadsheetKeyByTitle(String sheetName) {
		return getSpreadsheetEntryByTitle(sheetName).getKey();
	}
	
	@Override
	public RowsResult getSpreadsheetRows(String spreadsheetTitle,
			String worksheetTitle, int batchSize) {
	
		WorksheetEntry worksheet = getWorksheetByTitle(spreadsheetTitle,worksheetTitle);
		return new RowsResult(new GDataPartialResultExecutor(worksheet.getCellFeedUrl()), batchSize);
	}
     /**
	 * Returns part of the spreadsheet 
	 * 
	 * @param spreadsheetTitle      Spreadsheet name
	 * @param worksheetTitle        Worksheet name
	 * @param rectangle             Specification of rows,columns which should be returned.
	 * @param batchSize             Size of the batch
	 * @return                      Iterator to returned data
	 */
	public RowsResult getSpreadsheetAsText(String spreadsheetTitle,
	    String worksheetTitle, RectangleIdentifier rectangle,int batchSize) {
		WorksheetEntry worksheet = getWorksheetByTitle(spreadsheetTitle,worksheetTitle);
		
		if (worksheet.getRowCount() < rectangle.getRowEnd())
			throw new SpreadsheetOperationException("Row ["+rectangle.getRowEnd()+"] you have specified in your query doesn't exist in your worksheet.");
		if (worksheet.getColCount() < rectangle.getColEnd())
			throw new SpreadsheetOperationException("Column["+rectangle.getColEnd()+"] you have specified in your query doesn't exist in your worksheet.");
		
		RowsResult rows=new RowsResult(new GetAsTextPartialResultExecutor(worksheet.getCellFeedUrl(),rectangle), batchSize);
	    
		rows.setOffset(rectangle.getRowStart());
		rows.setLimit(rectangle.getRowEnd()-rectangle.getRowStart()+1);
	    return rows;
	}
	
	public class GetAsTextPartialResultExecutor implements PartialResultExecutor {
		private URL cellFeedUrl;
		private RectangleIdentifier rectangle;
		
		public GetAsTextPartialResultExecutor(URL cellFeedUrl,RectangleIdentifier rectangle) {
			this.cellFeedUrl= cellFeedUrl;
			this.rectangle=rectangle;

		}

		
		@Override
		public List<SheetRow> getResultsBatch(int startIndex, int amount) {
			CellQuery cellQuery = new CellQuery(cellFeedUrl);
			cellQuery.setMinimumRow(startIndex);
			//TODO  batch size problem with high amount values (terrible google API) 
			int endIndex=(startIndex+amount-1 < rectangle.getRowEnd()) ? startIndex+amount-1 : rectangle.getRowEnd();
			cellQuery.setMaximumRow(endIndex);
			cellQuery.setMinimumCol(rectangle.getColStart());
			cellQuery.setMaximumCol(rectangle.getColEnd());
			cellQuery.setReturnEmpty(true);
			CellFeed cellfeed= null;
			cellfeed=getCellFeedByQuery(cellQuery);
			

			List<SheetRow> result = new ArrayList<SheetRow>();
			
			SheetRow currentRow = new SheetRow();

			
			for (CellEntry entry : cellfeed.getEntries()){				
				if(entry.getCell().getValue()==null){
				    currentRow.addColumn("");
				}
				else{
					currentRow.addColumn(entry.getCell().getValue());	
				}		
				if (entry.getCell().getCol() == rectangle.getColEnd()) {					
					result.add(currentRow);
					currentRow = new SheetRow();
				}
				
			}												
            return result;
		}
	}


	public List<SpreadsheetEntry> getSpreadsheetEntries() {
		return getSpreadsheetFeed().getEntries();
	}
	
	
	
	public List<CellEntry> getFilledCells(URL cellfeedUrl,int row) {
		CellQuery cellQuery = new CellQuery(cellfeedUrl);
		cellQuery.setMinimumRow(row);
		cellQuery.setMaximumRow(row);
		return getCellFeedByQuery(cellQuery).getEntries();
	}


	private WorksheetEntry getWorksheetByTitle(String spreadsheetTitle,String worksheetTitle) {
		WorksheetQuery wquery = new WorksheetQuery( getSpreadsheetEntryByTitle(spreadsheetTitle).getWorksheetFeedUrl());
		wquery.setTitleExact(true);
		wquery.setTitleQuery(worksheetTitle);
		WorksheetFeed wfeed = getWorksheetFeedByQuery(wquery);
		
		if (wfeed.getEntries().size()==0)
			throw new SpreadsheetOperationException("Couldn't find worksheet "
					+ worksheetTitle+ " in spreadsheet "+ spreadsheetTitle);
		
		return wfeed.getEntries().get(0);		
	}

	public class GDataPartialResultExecutor implements PartialResultExecutor {
		private URL cellfeedUrl;
		
		public GDataPartialResultExecutor(URL cellfeedUrl) {
			this.cellfeedUrl= cellfeedUrl;
			
		}

		@Override
		public List<SheetRow> getResultsBatch(int startIndex, int amount) {
			CellQuery cellQuery = new CellQuery(cellfeedUrl);
			cellQuery.setMinimumRow(startIndex);
			cellQuery.setMaximumRow(startIndex+amount);
			CellFeed cellfeed = getCellFeedByQuery(cellQuery);
			
			List<SheetRow> result = new ArrayList<SheetRow>();
			
			int currentRowId = -1;
			SheetRow currentRow = null;
			for (CellEntry entry : cellfeed.getEntries()){
				if (entry.getCell().getRow() != currentRowId) {
					currentRow =  new SheetRow();
					result.add(currentRow);
					currentRowId = entry.getCell().getRow();
				}
				currentRow.addColumn(entry.getCell().getInputValue());
			}
			return result;
		}

	
	}


	/**
	 * Should handle relogin, exceptions, possibly timeouts.
	 * @return
	 */
	private SpreadsheetFeed getSpreadsheetFeed() {
		try {
			return service.getFeed(factory.getSpreadsheetsFeedUrl(),
				SpreadsheetFeed.class);
		} catch (Exception ex) {
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
			}
			//Try to relogin
			headerFactory.login();
			service.setHeader("Authorization" , headerFactory.getAuthHeader());
			try {
			return service.getFeed(factory.getSpreadsheetsFeedUrl(),
					SpreadsheetFeed.class);
			} catch (Exception ex2){
				throw new SpreadsheetOperationException("Error getting spreadsheet feed. Possibly bad authentication or connection problems. "+ ex2);
			}
		}
	}
	
	

	private SpreadsheetFeed getSpreadsheetFeedQuery(SpreadsheetQuery squery) {
		try {
			return service.getFeed(squery,
				SpreadsheetFeed.class);
		} catch (Exception ex) {
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
			}
			//Try to relogi
			headerFactory.login();
			service.setHeader("Authorization" , headerFactory.getAuthHeader());
			try {
			return service.getFeed(squery,
					SpreadsheetFeed.class);
			} catch (Exception ex2){
				throw new SpreadsheetOperationException("Error getting spreadsheet feed. Possibly bad authentication or connection problems. "+ ex2);
			}
		}
	}
	/**
	 * Should handle relogin, exceptions, possibly timeouts.
	 * @return
	 */
	private CellFeed getCellFeed(URL cellFeedUrl) {
		try {
			return service.getFeed(cellFeedUrl, CellFeed.class);
		} catch (Exception ex) {
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
			}
			//Try to relogin
			headerFactory.login();
			service.setHeader("Authorization" , headerFactory.getAuthHeader());
			 
			try {
				return service.getFeed(cellFeedUrl, CellFeed.class);
			} catch (Exception ex2) {
				throw new SpreadsheetOperationException(
						"Error getting spreadsheet feed. Possibly bad authentication or connection problems. "
								+ ex2);
			}
		}
	}
	
	private WorksheetFeed getWorksheetFeedByQuery(WorksheetQuery wquery) {
		try {
			return service.getFeed(wquery, WorksheetFeed.class);
		} catch (Exception ex) {
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
			}
			//Try to relogin
			headerFactory.login();
			service.setHeader("Authorization" , headerFactory.getAuthHeader());
			 
			try {
				return service.getFeed(wquery, WorksheetFeed.class);
			} catch (Exception ex2) {
				throw new SpreadsheetOperationException(
						"Error getting spreadsheet feed. Possibly bad authentication or connection problems. "
								+ ex2);
			}
		}
	}
	private CellFeed getCellFeedByQuery(CellQuery cellQuery) {
		try {
			return service.getFeed(cellQuery, CellFeed.class);
		} catch (Exception ex) {
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
			}
			//Try to relogin 
			headerFactory.login();
			service.setHeader("Authorization" , headerFactory.getAuthHeader());
			 
			try {
				return service.getFeed(cellQuery, CellFeed.class);
			} catch (Exception ex2) {
				throw new SpreadsheetOperationException(
						"Error getting spreadsheet feed. Possibly bad authentication or connection problems. "
								+ ex2);
			}
		}
	}





}
