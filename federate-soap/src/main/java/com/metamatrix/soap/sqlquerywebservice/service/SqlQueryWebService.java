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

import java.io.IOException;
import java.io.StringReader;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.axis2.AxisFault;
import org.apache.axis2.context.MessageContext;

import com.metamatrix.admin.AdminPlugin;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.api.MMURL;
import com.metamatrix.core.log.MessageLevel;
import com.metamatrix.dqp.client.impl.ServerRequest;
import com.metamatrix.jdbc.MMCallableStatement;
import com.metamatrix.jdbc.MMPreparedStatement;
import com.metamatrix.jdbc.MMStatement;
import com.metamatrix.jdbc.api.ExecutionProperties;
import com.metamatrix.jdbc.api.ResultSetMetaData;
import com.metamatrix.jdbc.api.SQLStates;
import com.metamatrix.jdbc.api.Statement;
import com.metamatrix.soap.SOAPPlugin;
import com.metamatrix.soap.security.Credential;
import com.metamatrix.soap.service.ConnectionSource;
import com.metamatrix.soap.service.PoolingConnectionSource;
import com.metamatrix.soap.sqlquerywebservice.helper.Cell;
import com.metamatrix.soap.sqlquerywebservice.helper.ColumnMetadata;
import com.metamatrix.soap.sqlquerywebservice.helper.ConnectionlessRequest;
import com.metamatrix.soap.sqlquerywebservice.helper.Data;
import com.metamatrix.soap.sqlquerywebservice.helper.LogInParameters;
import com.metamatrix.soap.sqlquerywebservice.helper.Results;
import com.metamatrix.soap.sqlquerywebservice.helper.Row;
import com.metamatrix.soap.sqlquerywebservice.helper.SqlWarning;
import com.metamatrix.soap.sqlquerywebservice.log.SqlQueryWebServicePlatformLog;
import com.metamatrix.soap.util.WebServiceUtil;

/**
 * This class is designed to support the querying of the MetaMatrix Server via a SOAP web service node. This class is also
 * designed in such a way as to produce a resulting web service interface that is complaint with the WS-I profile.
 */
public class SqlQueryWebService {

	public static final String APP_NAME = "SQL Query Web Service"; //$NON-NLS-1$

	/**
	 * Contextually aware credential provider.  Abstracted to facilitate testing.
	 */
	public interface CredentialProvider {
		Credential getCredentials() throws SqlQueryWebServiceFault;
	}
	
	private ConnectionSource connectionSource = PoolingConnectionSource.getInstance();
	private CredentialProvider credentialProvider = new CredentialProvider() {
		@Override
		public Credential getCredentials() throws SqlQueryWebServiceFault {
			MessageContext msgCtx = MessageContext.getCurrentMessageContext();
			if (msgCtx == null) {
				throwFaultException(true, new MetaMatrixProcessingException(SOAPPlugin.Util.getString("SqlQueryWebService.0"))); //$NON-NLS-1$
			}

			Credential credential = null;
			try {
				credential = WebServiceUtil.getCredentials(msgCtx);
			} catch (AxisFault e) {
				throwFaultException(true, e);
			}
			return credential;
		}
	};
	
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
	public Results executeBlocking( final ConnectionlessRequest connectionlessRequest ) throws SqlQueryWebServiceFault {
		Connection conn = null;
		Results results = null;
		Statement stmt = null;
		try {
			Properties connectionProperties = getConnectionInfo(connectionlessRequest.getParameters());
			conn = connectionSource.getConnection(connectionProperties);
			ServerRequest request = getRequestInfo(connectionlessRequest);
            
            if (request.getRequestType() == ServerRequest.REQUEST_TYPE_PREPARED_STATEMENT) {
            	MMPreparedStatement pStmt = conn.prepareStatement(request.getSql(), request.getCursorType(), ResultSet.CONCUR_READ_ONLY).unwrap(MMPreparedStatement.class);
            	stmt = pStmt;
                setParameterValues(request, pStmt);
            } else if (request.getRequestType() == ServerRequest.REQUEST_TYPE_CALLABLE_STATEMENT) {
            	MMCallableStatement cStmt = conn.prepareCall(request.getSql(), request.getCursorType(), ResultSet.CONCUR_READ_ONLY).unwrap(MMCallableStatement.class);
            	stmt = cStmt;
            	setParameterValues(request, cStmt);
            } else {
            	stmt = conn.createStatement(request.getCursorType(), ResultSet.CONCUR_READ_ONLY).unwrap(MMStatement.class);
            }
            if (connectionlessRequest.getMaxRowsReturned() > 0) {
            	stmt.setMaxRows(connectionlessRequest.getMaxRowsReturned());
            }
            if (request.getFetchSize() > 0) {
            	stmt.setFetchSize(request.getFetchSize());
            }
            if (connectionlessRequest.getTimeToWait() > 0) {
            	stmt.setQueryTimeout(connectionlessRequest.getTimeToWait()/1000);
            }
            stmt.setExecutionProperty(ExecutionProperties.PLAN_NOT_ALLOWED, Boolean.TRUE.toString());
            
            if (request.getXMLStyleSheet() != null) {
            	try {
					stmt.attachStylesheet(new StringReader(request.getXMLStyleSheet()));
				} catch (IOException e) {
					throwFaultException(true, e);
				}
            }
            stmt.setExecutionProperty(ExecutionProperties.PROP_PARTIAL_RESULTS_MODE, String.valueOf(request.getPartialResults()));
            stmt.setExecutionProperty(ExecutionProperties.PROP_XML_VALIDATION, String.valueOf(request.getXMLValidationMode()));
            stmt.setExecutionProperty(ExecutionProperties.XML_TREE_FORMAT, String.valueOf(request.getXMLFormat()));

            // Get transaction auto-wrap mode
            int transactionAutowrap = request.getTransactionAutoWrapMode();
            String autowrap = null;
            switch(transactionAutowrap) {
                case ServerRequest.AUTOWRAP_OFF:         autowrap = ExecutionProperties.AUTO_WRAP_OFF;         break;
                case ServerRequest.AUTOWRAP_ON:          autowrap = ExecutionProperties.AUTO_WRAP_ON;          break;
                case ServerRequest.AUTOWRAP_OPTIMISTIC:  autowrap = ExecutionProperties.AUTO_WRAP_OPTIMISTIC;  break;
                case ServerRequest.AUTOWRAP_PESSIMISTIC: autowrap = ExecutionProperties.AUTO_WRAP_PESSIMISTIC; break;
                default: throwFaultException(true, new MetaMatrixProcessingException(AdminPlugin.Util.getString("ServerFacadeImpl.invalid_txnautowrap", transactionAutowrap))); //$NON-NLS-1$
            }
            
            stmt.setExecutionProperty(ExecutionProperties.PROP_TXN_AUTO_WRAP, autowrap);
            stmt.setExecutionProperty(ExecutionProperties.RESULT_SET_CACHE_MODE, String.valueOf(request.getUseResultSetCache()));

            ResultSet rs = null;
            
            if (request.getRequestType() == ServerRequest.REQUEST_TYPE_PREPARED_STATEMENT || request.getRequestType() == ServerRequest.REQUEST_TYPE_PREPARED_STATEMENT) {
				if (((PreparedStatement)stmt).execute()) {
					rs = stmt.getResultSet(); 
				}
			} else {
				if (stmt.execute(request.getSql())) {
					rs = stmt.getResultSet();
				} 
			}

    		results = buildResults(stmt, rs, connectionlessRequest, connectionlessRequest.isIncludeMetadata());
        } catch (SQLException e) {
			throwFaultException(e.getSQLState() == null?true:SQLStates.isUsageErrorState(e.getSQLState()), e);
		} finally {
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					
				} 
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					
				}
			}
		}
		return results;
	}

	private void setParameterValues(ServerRequest request,
			MMPreparedStatement pStmt) throws SQLException {
		Object [] params = request.getBindParameters();
		if (params != null) {
			for (int i = 0; i < params.length; i++) {
				pStmt.setObject(i + 1, params[i]);
			}
		}
	}

	private Properties getConnectionInfo( final LogInParameters params ) throws SqlQueryWebServiceFault {
		Properties connectionInfo = new Properties();
		if (params.getMmServerUrl() != null) {
			connectionInfo.setProperty(MMURL.CONNECTION.SERVER_URL, params.getMmServerUrl());
		}
		if (params.getVdbName() != null) {
			connectionInfo.setProperty(MMURL.JDBC.VDB_NAME, params.getVdbName());
		}
		if (params.getVdbVersion() != null) {
			connectionInfo.setProperty(MMURL.JDBC.VDB_VERSION, params.getVdbVersion());
		}
		if (params.getConnectionPayload() != null) {
			connectionInfo.setProperty(MMURL.CONNECTION.CLIENT_TOKEN_PROP, params.getConnectionPayload());
		}

		/*
		 * Set the additional property for the application name.
		 */
		connectionInfo.setProperty(MMURL.CONNECTION.APP_NAME, APP_NAME);

		/*
		 * Set additional properties, if any, on the ConnectionInfo object.
		 */
		if (params.getOptionalProperties() != null) {

			for (int i = 0; i < params.getOptionalProperties().length; i++) {
				String propName = params.getOptionalProperties()[i].getPropertyName();
				String propValue = params.getOptionalProperties()[i].getPropertyValue();
				connectionInfo.setProperty(propName, propValue);
			}
		}

		Credential credential = this.credentialProvider.getCredentials();
		
		connectionInfo.setProperty(MMURL.CONNECTION.USER_NAME, credential.getUserName());
		connectionInfo.setProperty(MMURL.CONNECTION.PASSWORD, new String(credential.getPassword()));

		return connectionInfo;
	}

	/**
	 * This method is used to get an instance of RequestInfo.
	 * 
	 * @return PortableContext
	 */
	private ServerRequest getRequestInfo( final ConnectionlessRequest request ) {
		com.metamatrix.soap.sqlquerywebservice.helper.RequestInfo info = request.getRequestInfo();
		ServerRequest domainInfo = new ServerRequest();
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
	 * @throws SQLException 
	 */
	protected Results buildResults( final Statement stmt,
									final ResultSet rs,
	                                final ConnectionlessRequest request,
	                                final boolean includeMetadata ) throws SQLException {

		final Results soapResults = new Results();

		getData(soapResults, rs, includeMetadata);
	
		/*
		 * Get warnings If there are no SQLWarnings, we will stick a "holder" value to prevent NPE's in the Axis2 ADB
		 * client.
		 */
		soapResults.setSqlWarnings(getWarnings(stmt.getWarnings()));
	
		soapResults.setUpdateCount(stmt.getUpdateCount());

		/*
		 * Add any output parmeters to the SOAP Results If there are no output parms, we will stick a "holder" value to
		 * prevent NPE's in the Axis2 ADB client.
		 */
		soapResults.setOutputParameters(getOutputParameters(stmt));
		
		return soapResults;
	}

	/**
	 * Get SQL Warnings from results
	 * 
	 * @return SqlWarning[] - Array of SQLWarnings
	 */
	private SqlWarning[] getWarnings( SQLWarning warning ) {
		if (warning == null) {
			return new SqlWarning[] { new SqlWarning() };
		}
		List<SqlWarning> warnings = new ArrayList<SqlWarning>();
		do {
			final SqlWarning toAdd = new SqlWarning();
			toAdd.setMessage(warning.getMessage());
			warnings.add(toAdd);
			warning = warning.getNextWarning();
		} while (warning != null);
		return (SqlWarning[])warnings.toArray();
	}

	private String[] getOutputParameters( final Statement stmt ) throws SQLException {
		String[] result = null;
		if (stmt instanceof CallableStatement) {
			CallableStatement cs = (CallableStatement)stmt;
			
			ParameterMetaData metadata = cs.getParameterMetaData();
			
			int count = metadata.getParameterCount();
			
			final ArrayList<String> outputParametersList = new ArrayList<String>();
			for (int k = 1; k <= count; k++) {
				final int parameterType = metadata.getParameterType(k);
				if (parameterType == ParameterMetaData.parameterModeOut
				    || parameterType == ParameterMetaData.parameterModeInOut) {
					outputParametersList.add(cs.getString(k));
				}
			}
			result = (String[])outputParametersList.toArray();
		}
		
		if (result == null || result.length == 0) {
			return new String[] {"none"}; //$NON-NLS-1$
		}
		return result;
	}

	/**
	 * This method is used create a data object from the results returned from the server.
	 * 
	 * @return Data
	 * @throws SQLException 
	 */
	private void getData( final Results soapResults,
						  final ResultSet rs,
	                      final boolean includeMetadata ) throws SQLException {
		Data data = new Data();
		
		soapResults.setBeginRow(0);
		
		/*
		 * Set boolean indicating whether or not this is the last batch.
		 */
		data.setLast(true);

		Row[] rowArray = null;
		int rowCounter = 0;
		
		if (rs != null) {
			soapResults.setHasData(true);

			// Metadata is only valid for non-update results.
			ResultSetMetaData metadata = rs.getMetaData().unwrap(ResultSetMetaData.class);
			if (includeMetadata) {
				data.setMetadataArray(getColumnMetaData(metadata));
			}

			List<Row> rows = new ArrayList<Row>();
			/*
			 * Need to return at least one rowArray value to assure the response message conforms to the structure expected by the
			 * Axis2 ADB client.
			 */
			while (rs.next()) {
				Row row = new Row();
				Cell[] cellArray = new Cell[metadata.getColumnCount()];
				for (int j = 1; j <= cellArray.length; j++) {
					Cell cell = new Cell();
					String value = rs.getString(j);
					cell.setValue(value);
					cellArray[j - 1] = cell;
				}
				row.setCells(cellArray);
				rows.add(row);
				rowCounter++;
			}
			
			rowArray = (Row[])rows.toArray();
		}

		soapResults.setEndRow(rowCounter);
		/*
		 * Need to return at least one row/cell value(s) to assure the response message conforms to the structure expected by the
		 * Axis2 ADB client.
		 */
		if (rowCounter == 0) {
			Row emptyRow = new Row();
			rowArray = new Row[] {emptyRow};
			Cell emptyCell = new Cell();
			emptyRow.setCells(new Cell[] {emptyCell});
		}

		data.setRows(rowArray);

		soapResults.setData(data);
	}

	/**
	 * This method is used create a ColumnMetaData object array from the results returned from the server.
	 * 
	 * @return ColumnMetaData
	 * @throws SQLException 
	 */
	private ColumnMetadata[] getColumnMetaData( final ResultSetMetaData metadata ) throws SQLException {

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
			columnMetadata.setNullable(metadata.isNullable(i) == ResultSetMetaData.columnNullable);
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

	private void throwFaultException( boolean client, Throwable e ) throws SqlQueryWebServiceFault {

		SqlQueryWebServiceFault fault = SqlQueryWebServiceFault.create(client, e);

		SqlQueryWebServicePlatformLog.getInstance().getLogFile().log(MessageLevel.ERROR, e, e.getMessage());

		throw fault;
	}

	public void setConnectionSource(ConnectionSource connectionSource) {
		this.connectionSource = connectionSource;
	}

	public void setCredentialProvider(CredentialProvider credentialProvider) {
		this.credentialProvider = credentialProvider;
	}

}