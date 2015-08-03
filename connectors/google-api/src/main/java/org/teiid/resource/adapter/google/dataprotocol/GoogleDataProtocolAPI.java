/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

package org.teiid.resource.adapter.google.dataprotocol;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.resource.adapter.google.auth.AuthHeaderFactory;
import org.teiid.resource.adapter.google.common.GDataAPI;
import org.teiid.resource.adapter.google.common.SheetRow;
import org.teiid.resource.adapter.google.common.SpreadsheetAuthException;
import org.teiid.resource.adapter.google.common.SpreadsheetOperationException;
import org.teiid.resource.adapter.google.metadata.Column;
import org.teiid.resource.adapter.google.metadata.SpreadsheetColumnType;
import org.teiid.resource.adapter.google.result.PartialResultExecutor;
import org.teiid.resource.adapter.google.result.RowsResult;

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
	public static String ENCODING = "UTF-8"; //$NON-NLS-1$
	private GoogleJSONParser parser = new GoogleJSONParser();
	
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
	
	public List<Column> getMetadata(String spreadsheetTitle, String worksheetTitle) {
		String key = spreadsheetBrowser.getSpreadsheetKeyByTitle(spreadsheetTitle);
		DataProtocolQueryStrategy dpqs = new DataProtocolQueryStrategy(key,worksheetTitle,"SELECT *"); //$NON-NLS-1$
		dpqs.setRetrieveMetadata(true);
		dpqs.getResultsBatch(0, 1);
		return dpqs.getMetadata();
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
		private boolean retrieveMetadata;
		private List<Column> metadata;
		
		public DataProtocolQueryStrategy(String key, String worksheetKey,
				String query) {
			super();
			this.spreadsheetKey = key;
			this.worksheetName = worksheetKey;
			try {
				this.urlEncodedQuery = URLEncoder.encode(query, ENCODING); 
			} catch (UnsupportedEncodingException e) {
				throw new SpreadsheetOperationException(e);
			}
		}

		public void setRetrieveMetadata(boolean retrieveMetadata) {
			this.retrieveMetadata = retrieveMetadata;
		}
		
		public List<Column> getMetadata() {
			return metadata;
		}

		public List<SheetRow> getResultsBatch(int startIndex, int amount) {
			String boundariedQuery =null;
			String worksheet = null;
			try {
				boundariedQuery = getQueryWithBoundaries(amount, Math.max(0,(startIndex)));
				worksheet = URLEncoder.encode(worksheetName, ENCODING);
			} catch (UnsupportedEncodingException e) {
				throw new SpreadsheetOperationException(e);
			}
			HttpGet get = new HttpGet("https://spreadsheets.google.com/tq?key="+spreadsheetKey+"&sheet="+worksheet+"&tqx=responseHandler:x;out:json&tq="+boundariedQuery);  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
			get.setHeader("GData-Version", "3.0");  //$NON-NLS-1$ //$NON-NLS-2$
			get.setHeader("Authorization", headerFactory.getAuthHeader()); //$NON-NLS-1$
			
			try {
				DefaultHttpClient client = new DefaultHttpClient();
				try {
					return executeAndParse(client, get);
				} catch (SpreadsheetAuthException e) {
					// relogin
					headerFactory.login();
					get.setHeader("Authorization", headerFactory.getAuthHeader()); //$NON-NLS-1$
					return executeAndParse(client, get);
				}
			} catch (IOException e) {
				throw new SpreadsheetOperationException("Error retrieving batch from Gogole Visualization Data protocol", e);
			}
		}

		private List<SheetRow> executeAndParse(HttpClient client, HttpGet get) throws IOException {
			HttpResponse response = client.execute(get);

			if (response.getStatusLine().getStatusCode() == 200)
			{
				Reader reader = null;
				try {
					reader = new InputStreamReader(response.getEntity().getContent(), Charset.forName(ENCODING));  
					Map<?, ?> jsonResponse = (Map<?, ?>)parser.parseObject(reader, true);
					String status = (String)jsonResponse.get("status"); //$NON-NLS-1$
					if ("error".equals(status)) { //$NON-NLS-1$
						//TODO: better formatting
						List<Map<?, ?>> errors = (List<Map<?, ?>>) jsonResponse.get("errors"); //$NON-NLS-1$
						List<String> reasons = new ArrayList<String>();
						for (Map<?, ?> map : errors) {
							String reason = (String)map.get("reason"); //$NON-NLS-1$
							if ("user_not_authenticated".equals(reason)) { //$NON-NLS-1$
								throw new SpreadsheetAuthException("User not authenticated");
							}
							reasons.add(reason);
						}
						LogManager.logDetail(LogConstants.CTX_CONNECTOR, "Google request failed", errors); //$NON-NLS-1$
						throw new SpreadsheetOperationException(reasons.toString());							
					}
					//TODO: the warning could be sent to the client via the ExecutionContext
					
					Map<?,?> table = (Map<?,?>)jsonResponse.get("table"); //$NON-NLS-1$
					if (retrieveMetadata) {
						List<Map<?, ?>> cols = (List<Map<?, ?>>) table.get("cols"); //$NON-NLS-1$
						this.metadata = new ArrayList<Column>(cols.size());
						for (Map<?, ?> col : cols) {
							Column c = new Column();
							c.setAlphaName((String) col.get("id")); //$NON-NLS-1$
							String label = (String)col.get("label"); //$NON-NLS-1$
							if (label != null && !label.isEmpty()) {
								c.setLabel(label);
							}
							String type = (String)col.get("type"); //$NON-NLS-1$
							if (type != null) {
								c.setDataType(SpreadsheetColumnType.valueOf(type.toUpperCase()));
							}
							this.metadata.add(c);
						}
					}

					List<SheetRow> result = new ArrayList<SheetRow>();

					List<Map<?,?>> rows = (List<Map<?,?>>) table.get("rows");  //$NON-NLS-1$
					for (Map<?,?> row : rows) {
						SheetRow returnRow = new SheetRow();
						List<Map<?,?>> vals = (List<Map<?,?>>)row.get("c"); //$NON-NLS-1$
						for (Map<?,?> val : vals) {
							if (val == null) {
								returnRow.addColumn(null);
								continue;
							}
							Object object = val.get("v"); //$NON-NLS-1$
							//TODO: empty string values could be interpreted as null
							returnRow.addColumn(object);
						}
						result.add(returnRow);
					}
					
					return result;
				} finally {
					if (reader != null) {
						reader.close();
					}
				}
			} else if (response.getStatusLine().getStatusCode() == 500){ 
				//500 from server may not be a actual error. It can mean that offset is higher then actual result size. Can be solved by calling "count" first but performance penalty
				return new ArrayList<SheetRow>();
			}
			else {
				throw new SpreadsheetOperationException("Error when getting batch "+response.getStatusLine().getStatusCode()+":" +response.getStatusLine().getReasonPhrase());
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
			String[] keywordsToJump = new String[] {"label","format","options"}; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
			int indexToPut = urlEncodedQuery.length();
			
			for (String jumpIt : keywordsToJump){
				int index = urlEncodedQuery.indexOf(jumpIt);
				
				if (index != -1) {
					indexToPut = index;
					break;
				}
			}
			
			return urlEncodedQuery.substring(0, indexToPut).toString() +URLEncoder.encode(" limit "+amount+" offset "+offset+" ",ENCODING).toString() //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
					+ urlEncodedQuery.substring(indexToPut).toString();
		}		
	}
	
}





