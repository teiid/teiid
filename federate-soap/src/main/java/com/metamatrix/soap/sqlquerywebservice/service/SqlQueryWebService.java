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

package com.metamatrix.soap.sqlquerywebservice.service;

import java.util.ArrayList;
import java.util.List;

import org.apache.axis2.AxisFault;
import org.apache.axis2.context.MessageContext;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.api.MMURL_Properties;
import com.metamatrix.common.comm.platform.socket.client.SocketServerConnectionFactory;
import com.metamatrix.core.MetaMatrixCoreException;
import com.metamatrix.core.log.MessageLevel;
import com.metamatrix.dqp.client.ConnectionInfo;
import com.metamatrix.dqp.client.PortableContext;
import com.metamatrix.dqp.client.RequestInfo;
import com.metamatrix.dqp.client.ResultsMetadata;
import com.metamatrix.dqp.client.ServerFacade;
import com.metamatrix.dqp.client.impl.SerializablePortableContext;
import com.metamatrix.dqp.client.impl.ServerConnectionInfo;
import com.metamatrix.dqp.client.impl.ServerFacadeImpl;
import com.metamatrix.dqp.client.impl.ServerRequest;
import com.metamatrix.soap.SOAPPlugin;
import com.metamatrix.soap.security.Credential;
import com.metamatrix.soap.sqlquerywebservice.helper.Cell;
import com.metamatrix.soap.sqlquerywebservice.helper.ColumnMetadata;
import com.metamatrix.soap.sqlquerywebservice.helper.Connection;
import com.metamatrix.soap.sqlquerywebservice.helper.ConnectionContextualRequest;
import com.metamatrix.soap.sqlquerywebservice.helper.ConnectionlessRequest;
import com.metamatrix.soap.sqlquerywebservice.helper.Data;
import com.metamatrix.soap.sqlquerywebservice.helper.LogInParameters;
import com.metamatrix.soap.sqlquerywebservice.helper.RequestId;
import com.metamatrix.soap.sqlquerywebservice.helper.Results;
import com.metamatrix.soap.sqlquerywebservice.helper.ResultsRequest;
import com.metamatrix.soap.sqlquerywebservice.helper.Row;
import com.metamatrix.soap.sqlquerywebservice.helper.SqlWarning;
import com.metamatrix.soap.sqlquerywebservice.log.SqlQueryWebServicePlatformLog;
import com.metamatrix.soap.util.WebServiceUtil;

/**
 * This class is designed to support the querying of the MetaMatrix Server via a SOAP web service node. This class is also
 * designed in such a way as to produce a resulting web service interface that is compliant with the WS-I profile.
 */
public class SqlQueryWebService {

	/**
	 * This is the Server Facade instance that will serve as the client proxy for the MetaMatrix Server for this instance of
	 * SqlQueryWebService.
	 */
	private ServerFacade serverFacade = null;

	/**
	 * Static constants used to add the applicaiton name as an additional property.
	 */
	public static final String APP_NAME_PROP = MMURL_Properties.JDBC.APP_NAME;

	public static final String APP_NAME = "SQL Query Web Service"; //$NON-NLS-1$

	/**
	 * Static constants for Stored Procedure Output parameter types.
	 */
	public static final String OUT_PARAM = "OUT"; //$NON-NLS-1$

	public static final String INOUT_PARAM = "INOUT"; //$NON-NLS-1$

	/**
	 * This method is used to execute the passed in request. Subsequent calls to the getResults() operation can be used to
	 * retrieve the results of the execution of the statement. This is an asynchronous execution of the statement. Polling the
	 * getResults() operation is the only way to determine whether results have been returned from the MetaMatrix Server for this
	 * request execution. It is simply not practical to make this method synchronous. Most SOAP client frameworks and
	 * client/server connection mechanisms will time out long before a long running query has a chance to finish.
	 * 
	 * @param request the ConnectionContextualRequest to use for execution.
	 * @return The RequestId stub that can be subsequently passed in to the getResults method in order to retrieve the results of
	 *         the execution asynchronously.
	 */
	public RequestId execute( final ConnectionContextualRequest request ) throws SqlQueryWebServiceFault {

		final ServerFacade server = getServerFacade();
		final RequestId requestId = new RequestId();
		try {
			final RequestInfo info = getRequestInfo(request);
			final PortableContext portableContext = getPortableContext(request);
			final PortableContext requestContext = server.executeRequest(portableContext, info);
			requestId.setId(requestContext.getPortableString());
			requestId.setConnectionId(request.getConnection().getConnectionId());
		} catch (MetaMatrixException err) {
			throwFaultException(err);
		}
		request.getRequestInfo().getRequestType();

		return requestId;
	}

	/**
	 * This method is used as a shortcut when no Connection state is required on the server side. It will simply make a connection
	 * using the information supplied in the passed in object, and execute the query as defined also in the passed in object. This
	 * method saves server round trips, but sacrifices the performance of keeping a Connection open on the server side when
	 * multiple queries are to be executed. The returned Data contains all of the results retrieved from the server as a result of
	 * executing the query defined in the ConnectionlessRequest object. This method also sacrifices the �batching� functionality
	 * that is available when using the execute, getResults asynchronous method combination. The first x number of rows from the
	 * results will be returned in the return message as specified in the ConnectionlessRequest object. ResultsMetadata will also
	 * be returned in the result set by default.
	 * 
	 * @return Results
	 */
	public Results executeBlocking( final ConnectionlessRequest request ) throws SqlQueryWebServiceFault {

		final ServerFacade server = getServerFacade();
		PortableContext connectionContext = null;
		Results results = null;

		try {
			connectionContext = server.createSession(getConnectionInfo(request.getParameters()));
			final PortableContext requestContext = server.executeRequest(connectionContext, getRequestInfo(request));
			results = buildResults(connectionContext, requestContext, request, request.isIncludeMetadata());
		} catch (MetaMatrixException err) {
			throwFaultException(err);
		} finally {
			if (connectionContext != null) {
				try {

					server.closeSession(connectionContext);

				} catch (MetaMatrixCoreException err) {
					String[] params = new String[] {connectionContext.getPortableString(), err.getMessage()};
					SqlQueryWebServicePlatformLog.getInstance().getLogFile().log(MessageLevel.WARNING,
					                                                             SOAPPlugin.Util.getString("Unable_to_create_the_Detail_for_the_exception_due_to", //$NON-NLS-1$
					                                                                                       params));
				}
			}
		}

		return results;
	}

	/**
	 * This method is used to get the results for a request that has been executed via one of the asynchronous 'execute"
	 * operations on this API. If processing of the request is not complete, this method will return null. Otherwise it will
	 * return the Results of the execution. This method can be called as many times as necessary each resulting in a null return
	 * value until the server side processing of the results is complete. Once the Results are returned from this call, subsequent
	 * calls to this method will result in a fault as the Results from the execution of a request can be retrieved only once.
	 * (this is designed to help prevent multi-threaded access to the cursored position of the ResultSet in the Results object).
	 * When the returned Results instance indicates that there is a ResultSet involved. Those results can be navigated using the
	 * next() and previous() operations of this interface.
	 * 
	 * @param ResultsRequest the requestId as returned from the 'execute' method for the request execution for which you want to
	 *        retrieve the results.
	 * @return Results
	 */
	public Results getResults( final ResultsRequest resultsRequest ) throws SqlQueryWebServiceFault {

		com.metamatrix.dqp.client.Results results = null;
		final PortableContext connectionContext = getPortableContext(resultsRequest.getRequestId().getConnectionId());
		final PortableContext requestContext = getPortableContext(resultsRequest.getRequestId().getId());

		try {
			results = getServerFacade().getBatch(connectionContext,
			                                     requestContext,
			                                     resultsRequest.getStartRow() + 1,
			                                     resultsRequest.getEndRow() + 1,
			                                     resultsRequest.getTimeToWait());
		} catch (MetaMatrixException err) {
			throwFaultException(err);
		}

		/*
		 * If results are null, return, else build the Results object and return
		 */
		return (results == null ? null : createSoapResults(connectionContext,
		                                                   requestContext,
		                                                   results,
		                                                   resultsRequest.isIncludeMetadata()));
	}

	/**
	 * Given a ConnectionLoginParameters instance return a 'Connection' pointer that can be used to address the resulting
	 * persistent MetaMatrix Connection on the server side. Subsequent calls to this SOAP interface that are to use this
	 * Connection will need to pass this Connection object in.
	 * 
	 * @param params the information object that
	 * @return Connection
	 */
	public Connection getConnection( final LogInParameters params ) throws SqlQueryWebServiceFault {

		final ServerFacade server = getServerFacade();
		final ConnectionInfo info = getConnectionInfo(params);

		final Connection connection = new Connection();

		try {

			final PortableContext connectionContext = server.createSession(info);
			connection.setConnectionId(connectionContext.getPortableString());

		} catch (MetaMatrixException err) {
			throwFaultException(err);
		}

		return connection;
	}

	/**
	 * if you close a connection instance, all other open state (Statements, Resultsets) that is related to that connection will
	 * also be cleaned up.
	 * 
	 * @param connection the connection instance to be closed.
	 */
	public void closeConnection( final Connection connection ) throws SqlQueryWebServiceFault {
		final ServerFacade server = getServerFacade();
		final PortableContext connectionContext = getConnectionPortableContext(connection);

		try {

			server.closeSession(connectionContext);

		} catch (MetaMatrixException err) {
			throwFaultException(err);
		}

	}

	/**
	 * This method closes out a request. Closing a request frees up all resources that were used cursoring results or maintaining
	 * any state related to that request in the MetaMatrix Server. If the first batch of results is not yet available for a
	 * request, then calling this method will essentially 'cancel' the request. Subsequent calls using the passedin requestID will
	 * result in a fault being thrown.
	 * 
	 * @param requestID the id of the request to be closed.
	 */
	public void terminateRequest( RequestId requestId ) throws SqlQueryWebServiceFault {
		final ServerFacade server = getServerFacade();
		final PortableContext requestContext = getRequestPortableContext(requestId);
		final PortableContext connectionContext = getConnectionPortableContext(requestId);

		try {

			server.cancelRequest(connectionContext, requestContext);

		} catch (MetaMatrixException err) {
			throwFaultException(err);
		}

	}

	/*
	 * Private methods
	 */

	/**
	 * This method is used to get an instance of a ServerFacade.
	 * 
	 * @return ServerFacade
	 */
	private synchronized  ServerFacade getServerFacade() {

		if (serverFacade == null) {
			serverFacade = new ServerFacadeImpl(SocketServerConnectionFactory.getInstance());
		}

		return serverFacade;

	}

	/**
	 * This method is used to get an instance of PortableContext for a ConnectionContextualRequest.
	 * 
	 * @return PortableContext
	 */
	private PortableContext getPortableContext( final ConnectionContextualRequest request ) {

		return new SerializablePortableContext(request.getConnection().getConnectionId());

	}

	/**
	 * This method is used to get an instance of PortableContext for an id.
	 * 
	 * @return PortableContext
	 */
	private PortableContext getPortableContext( final String id ) {

		return new SerializablePortableContext(id);

	}

	/**
	 * Get a Connection PortableContext for a given Connection.
	 * 
	 * @param connection
	 * @return PortableContext
	 * @since 4.3
	 */
	private PortableContext getConnectionPortableContext( final Connection connection ) {

		return new SerializablePortableContext(connection.getConnectionId());
	}

	/**
	 * Get a Connection PortableContext for a given RequestId.
	 * 
	 * @param requestid
	 * @return PortableContext
	 * @since 4.3
	 */
	private PortableContext getConnectionPortableContext( final RequestId requestId ) {

		return new SerializablePortableContext(requestId.getConnectionId());
	}

	/**
	 * Get a Request PortableContext for a given RequestId.
	 * 
	 * @param requestid
	 * @return PortableContext
	 * @since 4.3
	 */
	private PortableContext getRequestPortableContext( final RequestId requestid ) {
		return new SerializablePortableContext(requestid.getId());
	}

	/**
	 * This method is used to get an instance of RequestInfo.
	 * 
	 * @return PortableContext
	 */
	private RequestInfo getRequestInfo( final ConnectionContextualRequest request ) {
		com.metamatrix.soap.sqlquerywebservice.helper.RequestInfo info = request.getRequestInfo();
		return getRequestInfo(info);

	}

	private ConnectionInfo getConnectionInfo( final LogInParameters params ) throws SqlQueryWebServiceFault {
		ServerConnectionInfo connectionInfo = new ServerConnectionInfo();
		connectionInfo.setServerUrl(params.getMmServerUrl());
		connectionInfo.setVDBName(params.getVdbName());
		connectionInfo.setVDBVersion(params.getVdbVersion());
		connectionInfo.setTrustedPayload(params.getConnectionPayload());

		/*
		 * Set the additional property for the application name.
		 */
		connectionInfo.setOptionalProperty(APP_NAME_PROP, APP_NAME);

		/*
		 * Set additional properties, if any, on the ConnectionInfo object.
		 */
		if (params.getOptionalProperties() != null) {

			for (int i = 0; i < params.getOptionalProperties().length; i++) {
				String propName = params.getOptionalProperties()[i].getPropertyName();
				String propValue = params.getOptionalProperties()[i].getPropertyValue();
				connectionInfo.setOptionalProperty(propName, propValue);
			}
		}

		MessageContext msgCtx = MessageContext.getCurrentMessageContext();
		if (msgCtx == null) {
			throwFaultException(new MetaMatrixProcessingException(SOAPPlugin.Util.getString("SqlQueryWebService.0"))); //$NON-NLS-1$
		}

		Credential credential = null;
		try {
			credential = WebServiceUtil.getCredentials(msgCtx);
		} catch (AxisFault e) {
			throwFaultException(new MetaMatrixProcessingException(e.getMessage()));
		}

		connectionInfo.setUser(credential.getUserName());
		connectionInfo.setPassword(new String(credential.getPassword()));

		return connectionInfo;
	}

	/**
	 * This method is used to get an instance of RequestInfo.
	 * 
	 * @return PortableContext
	 */
	private RequestInfo getRequestInfo( final ConnectionlessRequest request ) {
		com.metamatrix.soap.sqlquerywebservice.helper.RequestInfo info = request.getRequestInfo();
		return getRequestInfo(info);
	}

	private RequestInfo getRequestInfo( final com.metamatrix.soap.sqlquerywebservice.helper.RequestInfo info ) {

		RequestInfo domainInfo = new ServerRequest();
		domainInfo.setBindParameters(info.getBindParameters());
		domainInfo.setCommandPayload(info.getCommandPayload());
		domainInfo.setCursorType(Integer.parseInt(info.getCursorType().getValue()));
		domainInfo.setFetchSize(info.getFetchSize());
		domainInfo.setPartialResults(info.isPartialResults());
		domainInfo.setRequestType(Integer.parseInt(info.getRequestType().getValue()));
		domainInfo.setSql(info.getSqlString());
		domainInfo.setTransactionAutoWrapMode(Integer.parseInt(info.getTransactionAutoWrapMode().getValue()));
		domainInfo.setUseResultSetCache(info.isUseResultSetCache());
		domainInfo.setXMLFormat(info.getXmlFormat());
		domainInfo.setXMLStyleSheet(info.getXmlStyleSheet());
		domainInfo.setXMLValidationMode(info.isXmlValidationMode());

		return domainInfo;
	}

	/**
	 * This method is used to construct a results object for a given context. This is used for synchronous query execution to get
	 * all results.
	 * 
	 * @return Results
	 */
	protected Results buildResults( final PortableContext connectionContext,
	                                final PortableContext requestContext,
	                                final ConnectionlessRequest request,
	                                final boolean includeMetadata ) throws SqlQueryWebServiceFault {

		final ArrayList resultsList = new ArrayList();
		com.metamatrix.dqp.client.Results results = null;

		try {
			results = getServerFacade().getBatch(connectionContext,
			                                     requestContext,
			                                     1,
			                                     request.getMaxRowsReturned(),
			                                     request.getTimeToWait());
		} catch (MetaMatrixException err) {
			throwFaultException(err);
		}

		if (results == null) {
			/*
			 * Per AD, The ONLY time that we will receive a null Results instance back from the call to getBatch is if the request
			 * 'time to wait' has expired. In this case we let the client know this by sending them a fault indicating such.
			 */
			MetaMatrixException e = new MetaMatrixException(
			                                                SOAPPlugin.Util.getString("The_time_to_wait_submitted_with_the_executeBlocking_request")); //$NON-NLS-1$
			throwFaultException(e);
		}

		resultsList.add(results);

		/*
		 * If this wasn't the last batch, go get the rest of the batches.
		 */
		while (!results.isLast() && results.getEndRow() != 0 && results.getEndRow() != request.getMaxRowsReturned()) {
			try {
				results = getServerFacade().getBatch(connectionContext,
				                                     requestContext,
				                                     results.getEndRow() + 1,
				                                     request.getMaxRowsReturned(),
				                                     request.getTimeToWait());
			} catch (MetaMatrixException err) {
				throwFaultException(err);
			}

			if (results == null) {
				/*
				 * Check for null results again.
				 */
				MetaMatrixException e = new MetaMatrixException(
				                                                SOAPPlugin.Util.getString("The_time_to_wait_submitted_with_the_executeBlocking_request")); //$NON-NLS-1$
				throwFaultException(e);
			}

			resultsList.add(results);
		}

		return createBlockingSoapResults(connectionContext, requestContext, resultsList, includeMetadata);
	}

	/**
	 * This method is used to construct a results object from results returned from the server.
	 * 
	 * @return Results
	 */
	private Results createSoapResults( final PortableContext connectionContext,
	                                   final PortableContext requestContext,
	                                   final com.metamatrix.dqp.client.Results results,
	                                   final boolean includeMetadata ) throws SqlQueryWebServiceFault {
		Results soapResults = new Results();
		/*
		 * Build Data object from results.
		 */
		soapResults.setData(getData(connectionContext, requestContext, results, includeMetadata));

		soapResults.setBeginRow(new Integer(results.getBeginRow() - 1));
		soapResults.setEndRow(new Integer(results.getEndRow() - 1));
		soapResults.setHasData(hasData(results));

		/**
		 * Set update count only if this is an update.
		 */
		try {
			if (results.isUpdate()) {
				soapResults.setUpdateCount(new Integer(results.getUpdateCount()));
			} else {
				soapResults.setUpdateCount(new Integer(-1));
			}
		} catch (MetaMatrixException err) {
			throwFaultException(err);
		}

		/*
		 * Get warnings
		 */
		soapResults.setSqlWarnings(getWarnings(results));
		if (soapResults.getSqlWarnings() == null) {
			soapResults.setSqlWarnings(new SqlWarning[] {new SqlWarning()});
		}

		/*
		 * Add any output parmeters to the SOAP Results If there are no output parms, we will stick a "holder" value to prevent
		 * NPE's in the Axis2 ADB client.
		 */
		try {
			soapResults.setOutputParameters(getOutputParameters(results));
			if (soapResults.getOutputParameters() == null) {
				soapResults.setOutputParameters(new String[] {"none"}); //$NON-NLS-1$
			}
		} catch (MetaMatrixException err) {
			throwFaultException(err);
		}

		return soapResults;
	}

	/**
	 * This method is used to construct a results object from the list of results returned from the server. This is used for
	 * building results for the executeBlocking() method.
	 * 
	 * @return Results
	 */
	private Results createBlockingSoapResults( final PortableContext connectionContext,
	                                           final PortableContext requestContext,
	                                           final List resultsList,
	                                           final boolean includeMetadata ) throws SqlQueryWebServiceFault {

		final Results soapResults = new Results();
		for (int i = 0; i < resultsList.size(); i++) {

			com.metamatrix.dqp.client.Results results = (com.metamatrix.dqp.client.Results)resultsList.get(i);
			/*
			 * Build Data object from results.
			 */
			if (i == 0) {

				soapResults.setData(getData(connectionContext, requestContext, results, includeMetadata));

				soapResults.setBeginRow(new Integer(results.getBeginRow() - 1));
				soapResults.setHasData(hasData(results));

				/*
				 * Get warnings If there are no SQLWarnings, we will stick a "holder" value to prevent NPE's in the Axis2 ADB
				 * client.
				 */
				soapResults.setSqlWarnings(getWarnings(results));
				if (soapResults.getSqlWarnings() == null) {
					soapResults.setSqlWarnings(new SqlWarning[] {new SqlWarning()});
				}

				/**
				 * Set update count only if this is an update.
				 */
				try {
					if (results.isUpdate()) {
						soapResults.setUpdateCount(new Integer(results.getUpdateCount()));
					} else {
						soapResults.setUpdateCount(new Integer(-1));
					}
				} catch (MetaMatrixException err) {
					throwFaultException(err);
				}
			} else {
				soapResults.setData(appendData(soapResults.getData(), getData(connectionContext,
				                                                              requestContext,
				                                                              results,
				                                                              includeMetadata)));
			}

			soapResults.setEndRow(new Integer(results.getEndRow() - 1));

			/*
			 * Add any output parmeters to the SOAP Results If there are no output parms, we will stick a "holder" value to
			 * prevent NPE's in the Axis2 ADB client.
			 */
			try {
				soapResults.setOutputParameters(getOutputParameters(results));
				if (soapResults.getOutputParameters() == null) {
					soapResults.setOutputParameters(new String[] {"none"}); //$NON-NLS-1$
				}

			} catch (MetaMatrixException err) {
				throwFaultException(err);
			}
		}
		return soapResults;
	}

	/**
	 * Get SQL Warnings from results
	 * 
	 * @return SqlWarning[] - Array of SQLWarnings
	 */
	private SqlWarning[] getWarnings( final com.metamatrix.dqp.client.Results results ) {

		final Exception[] excArray = results.getWarnings();
		SqlWarning[] warnings = null;
		if (excArray.length > 0) {
			warnings = new SqlWarning[excArray.length];
			for (int j = 0; j < results.getWarnings().length; j++) {
				final Exception exc = excArray[j];
				final SqlWarning warning = new SqlWarning();
				warning.setMessage(exc.getMessage());
				warnings[j] = warning;
			}
		}
		return warnings;
	}

	/**
	 * Get SQL Warnings from results If there are no output parms, we will stick a "holder" value to prevent NPE's in the Axis2
	 * ADB client.
	 * 
	 * @return String[] - Array of Output Parameters
	 */
	private String[] getOutputParameters( final com.metamatrix.dqp.client.Results results )
	    throws MetaMatrixComponentException, MetaMatrixProcessingException {
		String[] outputParameters = null;
		if (results.isLast()) {
			if (results.getParameterCount() > 0) {
				final ArrayList outputParametersList = new ArrayList();
				for (int k = 1; k <= results.getParameterCount(); k++) {
					final int parameterType = results.getParameterType(k);
					if (parameterType == com.metamatrix.dqp.client.Results.PARAMETER_TYPE_OUT
					    || parameterType == com.metamatrix.dqp.client.Results.PARAMETER_TYPE_INOUT) {
						outputParametersList.add(results.getOutputParameter(k));
					}
				}
				outputParameters = new String[outputParametersList.size()];
				for (int l = 0; l < outputParametersList.size(); l++) {
					outputParameters[l] = (String)outputParametersList.get(l);
				}
			}
		}
		return outputParameters;
	}

	/**
	 * This method is used create a data object from the results returned from the server.
	 * 
	 * @return Data
	 */
	private Data getData( final PortableContext connectionContext,
	                      final PortableContext requestContext,
	                      final com.metamatrix.dqp.client.Results results,
	                      final boolean includeMetadata ) throws SqlQueryWebServiceFault {
		Data data = new Data();

		ResultsMetadata metadata = null;
		if (includeMetadata) {
			try {
				// Metadata is only valid for non-update results.
				if (!results.isUpdate()) {
					metadata = getServerFacade().getMetadata(connectionContext, requestContext);
					data.setMetadataArray(getColumnMetaData(metadata));
				}
			} catch (MetaMatrixException err) {
				throwFaultException(err);
			}
		}

		/*
		 * Set boolean indicating whether or not this is the last batch.
		 */
		data.setLast(results.isLast());
		/*
		 * Need to return at least one rowArray value to assure the response message conforms to the structure expected by the
		 * Axis2 ADB client.
		 */
		Row[] rowArray = new Row[results.getRowCount() == 0 ? 1 : results.getRowCount()];
		/*
		 * This counter is used for our rowArray.
		 */
		int rowCounter = 0;
		try {
			for (int i = results.getBeginRow(); i <= results.getEndRow(); i++) {
				Row row = new Row();
				Cell[] cellArray = new Cell[results.getColumnCount()];
				for (int j = 1; j <= results.getColumnCount(); j++) {
					Cell cell = new Cell();
					Object value = results.getValue(i, j);
					if (value != null) {
						cell.setValue(value.toString());
					}
					cellArray[j - 1] = cell;
				}
				row.setCells(cellArray);
				rowArray[rowCounter++] = row;
			}
		} catch (MetaMatrixException err) {
			throwFaultException(err);
		}

		/*
		 * Need to return at least one row/cell value(s) to assure the response message conforms to the structure expected by the
		 * Axis2 ADB client.
		 */
		Row emptyRow = new Row();
		Cell emptyCell = new Cell();
		if (results.getRowCount() == 0) {
			emptyRow.setCells(new Cell[] {emptyCell});
		}

		data.setRows(rowArray);

		return data;
	}

	/**
	 * This method is used append data when building results returned from executeBlocking().
	 * 
	 * @return Data
	 */
	private Data appendData( Data data,
	                         final Data appendData ) {

		/*
		 * Get the current Row array
		 */
		final Row[] arrayOfRow = data.getRows();

		/*
		 * Get the append Row array
		 */
		if (arrayOfRow != null) {
			final Row[] appendArrayOfRow = appendData.getRows();

			/*
			 * Now copy the new array to the existing array and update the Data object
			 */
			if (appendArrayOfRow != null) {
				final Row[] newArray = new Row[arrayOfRow.length + appendArrayOfRow.length];
				System.arraycopy(arrayOfRow, 0, newArray, 0, arrayOfRow.length);
				System.arraycopy(appendArrayOfRow, 0, newArray, arrayOfRow.length, appendArrayOfRow.length);
				data.setRows(newArray);
			}
		}

		return data;
	}

	/**
	 * This method is used create a ColumnMetaData object array from the results returned from the server.
	 * 
	 * @return ColumnMetaData
	 */
	private ColumnMetadata[] getColumnMetaData( final ResultsMetadata metadata ) {

		final int columnCount = metadata.getColumnCount();
		ColumnMetadata[] metadataArray = new ColumnMetadata[columnCount];

		for (int i = 1; i <= metadata.getColumnCount(); i++) {
			ColumnMetadata columnMetadata = new ColumnMetadata();
			columnMetadata.setColumnName(metadata.getColumnName(i));
			columnMetadata.setColumnDataType(metadata.getColumnTypeName(i));
			columnMetadata.setColumnClassName(metadata.getColumnClassName(i));
			columnMetadata.setColumnDisplaySize(metadata.getColumnDisplaySize(i));
			columnMetadata.setCurrency(metadata.isCurrency(i));
			columnMetadata.setGetColumnLabel(metadata.getColumnLabel(i));
			columnMetadata.setNullable(metadata.isNullable(i));
			columnMetadata.setPrecision(metadata.getPrecision(i));
			columnMetadata.setReadOnly(metadata.isReadOnly(i));
			columnMetadata.setScale(metadata.getScale(i));
			columnMetadata.setSearchable(metadata.isSearchable(i));
			columnMetadata.setSigned(metadata.isSigned(i));
			columnMetadata.setTableName(metadata.getTableName(i));
			columnMetadata.setVirtualDatabaseName(metadata.getVirtualDatabaseName(i));
			columnMetadata.setVirtualDatabaseVersion(metadata.getVirtualDatabaseVersion(i));

			metadataArray[i - 1] = columnMetadata;

		}
		return metadataArray;
	}

	private void throwFaultException( MetaMatrixException e ) throws SqlQueryWebServiceFault {

		SqlQueryWebServiceFault fault = SqlQueryWebServiceFault.create(e);

		SqlQueryWebServicePlatformLog.getInstance().getLogFile().log(MessageLevel.ERROR, e, e.getMessage());

		throw fault;
	}

	private boolean hasData( com.metamatrix.dqp.client.Results results ) {
		boolean hasData = true;

		if (results.isUpdate() || results.getColumnCount() == 0) {
			hasData = false;
		}
		return hasData;

	}

	protected void setServerFacade( ServerFacade serverFacade ) {
		this.serverFacade = serverFacade;
	}
}
