package org.teiid.resource.adapter.google.dataprotocol;

import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.teiid.resource.adapter.google.auth.AuthHeaderFactory;
import org.teiid.resource.adapter.google.common.GDataAPI;
import org.teiid.resource.adapter.google.common.SheetRow;
import org.teiid.resource.adapter.google.common.SpreadsheetOperationException;
import org.teiid.resource.adapter.google.result.PartialResultExecutor;
import org.teiid.resource.adapter.google.result.RowsResult;

import au.com.bytecode.opencsv.CSVReader;


/**
 * 
 * This class is used to make requests to Google Visualization Data Protocol. The most important
 * method is executeQuery.
 * 
 * @author fnguyen
 *
 */
public class GoogleDataProtocolAPI {
	private GDataAPI spreadsheetBrowser = null;
	private AuthHeaderFactory headerFactory = null;
	
	
	public AuthHeaderFactory getHeaderFactory() {
		return headerFactory;
	}

	public void setHeaderFactory(AuthHeaderFactory headerFactory) {
		this.headerFactory = headerFactory;
	}

	public void setSpreadSheetBrowser( GDataAPI spreadsheetBrowser ){
		this.spreadsheetBrowser=spreadsheetBrowser;
	}
		
	/**
	 * Most important method that will issue query [1] to specific worksheet. The columns in the query
	 * should be identified by their real alphabetic name (A, B, C...). 
	 * 
	 * There is one important restriction to query. It should not contain offset and limit clauses.
	 * To achieve functionality of offset and limit please use corresponding parameters in this method.
	 * 
	 * 
	 * [1] https://developers.google.com/chart/interactive/docs/querylanguage
	 * 
	 * @param query The query defined in [1]
	 * @param batchSize How big portions of data should be returned by one roundtrip to Google.
	 * @return Iterable RowsResult that will actually perform the roundtrips to Google for data 
	 */
	public RowsResult executeQuery(String spreadsheetTitle, String worksheetTitle,
			String query, int batchSize, Integer offset, Integer limit) {
	
		String key = spreadsheetBrowser.getSpreadsheetKeyByTitle(spreadsheetTitle);
		
		RowsResult result = new RowsResult(new DataProtocolQueryStrategy(key,worksheetTitle,query), batchSize);
		if (offset!= null)
			result.setOffset(offset);
		if (limit != null)
			result.setLimit(limit);
		
		return result;
	}
	
	/**
	 * Logic to query portion of data from Google Visualization Data Protocol. We do not use any special library just simple
	 * Http request. Google sends response back in CSV that we parse afterwards.
	 * 
	 * @author fnguyen
	 *
	 */
	public class DataProtocolQueryStrategy implements PartialResultExecutor {
		private String spreadsheetKey;
		private String worksheetName;
		private String urlEncodedQuery;
		
		public DataProtocolQueryStrategy(String key, String worksheetKey,
				String query) {
			super();
			this.spreadsheetKey = key;
			this.worksheetName = worksheetKey;
			try {
				this.urlEncodedQuery = URLEncoder.encode(query, "utf-8");
			} catch (UnsupportedEncodingException e) {
				throw new SpreadsheetOperationException("Error encoding query: "+query,e);
			}
		}


		public List<SheetRow> getResultsBatch(int startIndex, int amount) {
			String boundariedQuery =null;
			try {
				boundariedQuery = getQueryWithBoundaries(amount, Math.max(0,(startIndex)));
			} catch (UnsupportedEncodingException e) {
				throw new SpreadsheetOperationException("Error creating URL encoded limit clause",e);
			}
			HttpGet get = new HttpGet("https://spreadsheets.google.com/tq?key="+spreadsheetKey+"&sheet="+worksheetName+"&tqx=out:csv&tq="+boundariedQuery);
			get.setHeader("GData-Version", "3.0");
			get.setHeader("Authorization", headerFactory.getAuthHeader());
			
			try {
				return executeAndParse(get);
			} catch (Exception ex) {
				// relogin
				headerFactory.login();
				get.setHeader("Authorization", headerFactory.getAuthHeader());
				try {
					return executeAndParse(get);
				} catch (Exception ex2) {
					throw new SpreadsheetOperationException(
							"Error retrieving batch from Gogole Visualization Data protocol",
							ex2);
				}
			}
		
		}


		private List<SheetRow> executeAndParse(HttpGet get) {
			HttpResponse response = null;
			try {
			
				try {
					response = (new DefaultHttpClient()).execute(get);
				} catch (Exception e) {
					//TODO relogin
					throw new SpreadsheetOperationException("Exception making http request on Google spreadsheet: "+get,e);
				}
				
				if (response.getStatusLine().getStatusCode() == 200)
				{
					CSVReader reader = null;
					try {
						reader = new CSVReader(new InputStreamReader(response
								.getEntity().getContent()));
						String []  line;
						List<SheetRow> result = new ArrayList<SheetRow>();
						
						//CSV reader reads first row in the response as empty if there is no error. Skip it.
						line  = reader.readNext();
						if (line != null &&
								line.length >= 1 && 
								line[0].contains("Error")) {
							if (line[0].contains("Invalid query. Column")){
								throw new SpreadsheetOperationException("You have probably defined more columns that actually exist in the spreadsheet."+
							" Please decrease the columnCount setting or fill in header in the worksheet, that will create actual columns. Error was: "+ line[0]);
							}
						else 
							throw new SpreadsheetOperationException(line[0]);
						}
						
						
						while ((line = reader.readNext()) != null) {
							SheetRow row = new SheetRow();
							for (String col: line)
								row.addColumn(col);
							result.add(row);
						}
						return result;
					} finally {
						reader.close();
					}
				} else if (response.getStatusLine().getStatusCode() == 500){ 
					//500 from server may not be a actual error. It can mean that offset is higher then actual result size. Can be solved by calling "count" first but performance penalty
					return new ArrayList<SheetRow>();
				}
				else {
					throw new SpreadsheetOperationException("Error when getting batch "+response.getStatusLine().getStatusCode()+":" +response.getStatusLine().getReasonPhrase());
				}
				
			} catch (Exception ex){
				throw new SpreadsheetOperationException("Error getting batch from google spreadsheet.",ex);
			} 
		}


		/**
		 * Adds limit, offset to the query. This is slightly more complicated becuase limit/offset must appear
		 * before certain clauses (label, format, options)
		 * @param amount
		 * @param offset
		 * @return
		 * @throws UnsupportedEncodingException
		 */
		private String getQueryWithBoundaries(int amount, int offset) throws UnsupportedEncodingException {
			String[] keywordsToJump = new String[] {"label","format","options"};
			int indexToPut = urlEncodedQuery.length();
			
			for (String jumpIt : keywordsToJump){
				int index = urlEncodedQuery.indexOf(jumpIt);
				
				if (index != -1) {
					indexToPut = index;
					break;
				}
			}
			
			return urlEncodedQuery.substring(0, indexToPut).toString() +URLEncoder.encode(" limit "+amount+" offset "+offset+" ","utf-8").toString()
					+ urlEncodedQuery.substring(indexToPut).toString();
		}		
	}
}





