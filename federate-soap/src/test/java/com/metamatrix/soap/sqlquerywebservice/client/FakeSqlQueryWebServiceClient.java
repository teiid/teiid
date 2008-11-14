/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.soap.sqlquerywebservice.client;

import com.metamatrix.soap.sqlquerywebservice.helper.Cell;
import com.metamatrix.soap.sqlquerywebservice.helper.ColumnMetadata;
import com.metamatrix.soap.sqlquerywebservice.helper.Connection;
import com.metamatrix.soap.sqlquerywebservice.helper.ConnectionContextualRequest;
import com.metamatrix.soap.sqlquerywebservice.helper.ConnectionlessRequest;
import com.metamatrix.soap.sqlquerywebservice.helper.Data;
import com.metamatrix.soap.sqlquerywebservice.helper.LogInParameters;
import com.metamatrix.soap.sqlquerywebservice.helper.RequestId;
import com.metamatrix.soap.sqlquerywebservice.helper.RequestInfo;
import com.metamatrix.soap.sqlquerywebservice.helper.RequestType;
import com.metamatrix.soap.sqlquerywebservice.helper.Results;
import com.metamatrix.soap.sqlquerywebservice.helper.ResultsRequest;
import com.metamatrix.soap.sqlquerywebservice.helper.Row;
import com.metamatrix.soap.sqlquerywebservice.service.SqlQueryWebService;
import com.metamatrix.soap.sqlquerywebservice.service.SqlQueryWebServiceFault;

/**
 * @author jpoulsen TODO To change the template for this generated type comment
 *         go to Window - Preferences - Java - Code Style - Code Templates
 */
public class FakeSqlQueryWebServiceClient {
	
	private static final RequestType STATEMENT = new RequestType("0"); //$NON-NLS-1$
	private static final RequestType PREPARED_STATEMENT = new RequestType("1"); //$NON-NLS-1$ 

	/**
	 * 
	 */
	public FakeSqlQueryWebServiceClient() {
		super();
		// TODO Auto-generated constructor stub
	}

	public void main(String[] args) throws SqlQueryWebServiceFault {

		SqlQueryWebService service = new SqlQueryWebService();
		long queryTimeout = 600000; // 60 seconds

		LogInParameters info = new LogInParameters(); 
		info.setMmServerUrl("mm://chicago05:12345"); //$NON-NLS-1$
		info.setVdbName("DesignTimeCatalog"); //$NON-NLS-1$
		info.setVdbVersion("1"); //$NON-NLS-1$
		/*
		 * Application specific payload
		 */
		info.setConnectionPayload("MyPayload"); //$NON-NLS-1$

		Connection connection = null;

		connection = service.getConnection(info);

		RequestInfo requestInfo = new RequestInfo();

		requestInfo.setSqlString("SELECT * FROM <MY_TABLE>"); //$NON-NLS-1$
		requestInfo.setRequestType(STATEMENT); // Statement

		ConnectionContextualRequest request = new ConnectionContextualRequest();

		request.setConnection(connection);
		request.setRequestInfo(requestInfo);

		RequestId requestId = null;

		requestId = service.execute(request);

		requestId.setConnectionId(connection.getConnectionId());

		Results results = null;

		ResultsRequest resultsRequest = new ResultsRequest();

		final int batchSize = 300;

		resultsRequest.setStartRow(0);

		/*
		 * Since we start at 0, we subtract 1 to get the end row index.
		 */
		resultsRequest.setEndRow(batchSize - 1);

		/*
		 * we set the current row to be the end row + 1 so that when we go to
		 * get the next batch we dont 'overlap'.
		 */
		int currentRow = batchSize;

		resultsRequest.setRequestId(requestId);

		long startQueryTime = 0;
		long endQueryTime = 0;
		long totalQueryTime = 0;

		// Loop until results are ready for us and process the first batch or
		// the query timeout value is reached/exceeded.
		while (results == null || totalQueryTime >= queryTimeout) {
			startQueryTime = System.currentTimeMillis();
			try {
				results = service.getResults(resultsRequest);
			} catch (Exception e) {
				// handle exception and decide whether to keep polling. break if
				// decision is not to continue polling.
			}
			endQueryTime = System.currentTimeMillis();
			totalQueryTime += (startQueryTime - endQueryTime);
		}

		/*
		 * The results have returned. Now we continue to get batches of results
		 * until and process them until there are no results left to process.
		 */
		Data data = results.getData();
		ColumnMetadata[] metadata = data.getMetadataArray();

		while (data != null && !data.isLast()) {

			processResults(data, metadata);

			resultsRequest.setStartRow(currentRow);

			currentRow = currentRow + batchSize;

			resultsRequest.setEndRow(currentRow - 1);

			try {
				Results nextResults = service.getResults(resultsRequest);
				data = nextResults.getData();

			} catch (Exception e) {
				// handle the situation where we failed to get the next results
				// for the given statement
			}
		}

	}

	public void xxxtestSynchronousExecution(String[] args)
			throws SqlQueryWebServiceFault {

		SqlQueryWebService service = new SqlQueryWebService();

		LogInParameters info = new LogInParameters(); 
		info.setMmServerUrl("mm://chigago05:12345"); //$NON-NLS-1$
		info.setVdbName("DesignTimeCatalog"); //$NON-NLS-1$
		info.setVdbVersion("1"); //$NON-NLS-1$
		/*
		 * Application specific payload
		 */
		info.setConnectionPayload("MyPayload"); //$NON-NLS-1$

		RequestInfo requestInfo = new RequestInfo();

		requestInfo.setSqlString("SELECT * FROM <MY_TABLE>"); //$NON-NLS-1$
		requestInfo.setRequestType(STATEMENT);

		ConnectionlessRequest request = new ConnectionlessRequest();

		request.setIncludeMetadata(true);
		request.setMaxRowsReturned(100);
		request.setRequestInfo(requestInfo);

		/*
		 * All results are returned for this query in one batch, as opposed to
		 * an asyncronous query which may have multiple batches.
		 */
		Results results = service.executeBlocking(request);

		Data data = results.getData();
		ColumnMetadata[] metadata = null;

		processResults(data, metadata);
	}

	public void xxxtestSynchronousUpdateExecution(String[] args)
			throws SqlQueryWebServiceFault {

		SqlQueryWebService service = new SqlQueryWebService();

		LogInParameters info = new LogInParameters(); 
		info.setMmServerUrl("mm://chigago05:12345"); //$NON-NLS-1$
		info.setVdbName("DesignTimeCatalog"); //$NON-NLS-1$
		info.setVdbVersion("1"); //$NON-NLS-1$
		/*
		 * Application specific payload
		 */
		info.setConnectionPayload("MyPayload"); //$NON-NLS-1$

		RequestInfo requestInfo = new RequestInfo();

		requestInfo
				.setSqlString("UPDATE <MY_TABLE> SET <MY_COLUMN> = 1 WHERE <MY_COLUMN> = 2"); //$NON-NLS-1$
		requestInfo.setRequestType(STATEMENT);

		ConnectionlessRequest request = new ConnectionlessRequest();

		request.setIncludeMetadata(false);
		request.setMaxRowsReturned(1);
		request.setRequestInfo(requestInfo);

		/*
		 * All results are returned for this query in one batch, as opposed to
		 * an asyncronous query which may have multiple batches.
		 */
		Results results = service.executeBlocking(request);

		System.out
				.println("There were " + results.getUpdateCount() + " updated."); //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void xxxtestSynchronousStoredProcedureExecution(String[] args)
			throws SqlQueryWebServiceFault {

		SqlQueryWebService service = new SqlQueryWebService();

		LogInParameters info = new LogInParameters(); 
		info.setMmServerUrl("mm://chigago05:12345"); //$NON-NLS-1$
		info.setVdbName("DesignTimeCatalog"); //$NON-NLS-1$
		info.setVdbVersion("1"); //$NON-NLS-1$
		/*
		 * Application specific payload
		 */
		info.setConnectionPayload("MyPayload"); //$NON-NLS-1$

		RequestInfo requestInfo = new RequestInfo();

		/*
		 * Execute myprocedure with two input parameters and two output
		 * parameters.
		 */
		requestInfo.setSqlString("exec myprocedure(?, ?)"); //$NON-NLS-1$
		requestInfo.setRequestType(PREPARED_STATEMENT);
		String[] bindParameters = new String[2];
		bindParameters[0] = "1"; //$NON-NLS-1$
		bindParameters[1] = "Second input parameter"; //$NON-NLS-1$
		requestInfo.setBindParameters(bindParameters);

		ConnectionlessRequest request = new ConnectionlessRequest();

		request.setIncludeMetadata(false);
		request.setMaxRowsReturned(1);
		request.setRequestInfo(requestInfo);

		/*
		 * All results are returned for this query in one batch, as opposed to
		 * an asyncronous query which may have multiple batches.
		 */
		Results results = service.executeBlocking(request);

		Object[] outputParameters = results.getOutputParameters();

		System.out
				.println("The first output parameter is " + outputParameters[0].toString()); //$NON-NLS-1$ 
		System.out
				.println("The second output parameter is " + outputParameters[1].toString()); //$NON-NLS-1$ 
	}

	public void processResults(Data data, ColumnMetadata[] metadata) {

		/*
		 * If metadata was included, we can get the column count from the array
		 * of ColumnMetaData objects. If metadata was not included, we can
		 * derive the column count from the size of the cell array on the first
		 * row.
		 */
		Row[] rows = data.getRows();
		int columnCount = 0;
		if (metadata != null) {
			metadata = data.getMetadataArray();
			columnCount = metadata.length;
		} else {
			if (rows != null && rows.length > 0) {
				columnCount = rows[0].getCells().length;
			}
		}

		for (int i = 0; i < rows.length - 1; i++) {
			for (int j = 0; j < columnCount; j++) {
				Cell cell = rows[i].getCells()[j];
				System.out.println(cell.getValue());
			}

		}
	}

}