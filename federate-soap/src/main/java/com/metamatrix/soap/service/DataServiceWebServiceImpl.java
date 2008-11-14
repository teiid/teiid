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

package com.metamatrix.soap.service;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import javax.xml.soap.MessageFactory;
import javax.xml.soap.SOAPException;
import javax.xml.stream.XMLStreamException;

import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.impl.builder.StAXOMBuilder;
import org.apache.axiom.soap.SOAPBody;
import org.apache.axis2.AxisFault;
import org.apache.axis2.context.MessageContext;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.util.ByteArrayHelper;
import com.metamatrix.common.util.WSDLServletUtil;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.jdbc.api.SQLStates;
import com.metamatrix.soap.SOAPPlugin;
import com.metamatrix.soap.exceptions.SOAPProcessingException;
import com.metamatrix.soap.security.Credential;
import com.metamatrix.soap.util.EndpointUriTranslatorStrategyImpl;
import com.metamatrix.soap.util.WebServiceUtil;

/**
 * 
 * This class is the main implementation of the MetaMatrix Data Service feature.
 * It is the 'router' that marshalls all Data Service web service requests to
 * the MetaMatrix Server and returns the results in the return web service
 * response.
 * 
 * This class represents a 'virtual' web service because it has no set web
 * service interface. An instance of this class will serve all Data Services
 * that are deployed to all VDB's of the MetaMatrix Server.
 */
public class DataServiceWebServiceImpl {

	/*
	 * These are the standard SOAP 1.1 fault codes that we use in the Data
	 * Service web service implementation to report fault conditions to the
	 * user.
	 */
	public static final String SOAP_11_STANDARD_CLIENT_FAULT_CODE = "Client"; //$NON-NLS-1$

	public static final String SOAP_11_STANDARD_SERVER_FAULT_CODE = "Server"; //$NON-NLS-1$

	// Static constant for a empty string
	private static final String EMPTY_STRING = StringUtil.Constants.EMPTY_STRING;

	// Static constant for a double quote.
	private static final String DOUBLE_QUOTE = "\"";//$NON-NLS-1$

	// Static constant for a single quote.
	private static final String SINGLE_QUOTE = "'";//$NON-NLS-1$

	// constant for username part of url
	public static final String USER_NAME = "user"; //$NON-NLS-1$

	public static ConnectionSource connectionSource = new PoolingConnectionSource();

	// constant for password part of url
	public static final String PASSWORD = "password"; //$NON-NLS-1$

	public static final String ERROR_PREFIX = "Procedure error: "; //$NON-NLS-1$

	// constructor
	public DataServiceWebServiceImpl() {
	}

	public DataServiceWebServiceImpl(MessageFactory factory,
			ConnectionSource connectionSource) {
		DataServiceWebServiceImpl.connectionSource = connectionSource;
	}

	/**
	 * @param element
	 * @return
	 */
	public OMElement executeDataService(OMElement element) throws AxisFault {

		MessageContext context = MessageContext.getCurrentMessageContext();

		SOAPBody reqBody = null;
		reqBody = context.getEnvelope().getBody();
		String inputMessage = EMPTY_STRING;

		DataServiceInfo info = null;

		try {
			info = getDataServiceInfo(element);
		} catch (Exception e2) {
			createSOAPFaultMessage(e2, e2.getMessage(),
					SOAP_11_STANDARD_SERVER_FAULT_CODE);
		}

		List bodyElements = getBodyElements(reqBody);

		if (bodyElements != null && bodyElements.size() == 1) {
			/**
			 * Get the root element from the SOAPBody of the request and convert
			 * double quotes to single quotes. DQP cannot process parameters
			 * with embedded double quotes.
			 */
			inputMessage = StringUtil.replaceAll(
					bodyElements.get(0).toString(), DOUBLE_QUOTE, SINGLE_QUOTE);

		} else if (bodyElements.size() > 1) {
			/*
			 * we only allow one root body element in a Data Service SOAP
			 * request, so if there is more than one we throw an exception.
			 */
			String message = SOAPPlugin.Util
					.getString("DataServiceWebServiceImpl.2"); //$NON-NLS-1$
			createSOAPFaultMessage(new SOAPException(message), message,
					SOAP_11_STANDARD_CLIENT_FAULT_CODE);
		}
		String returnFragment = null;
		Connection connection = null;
		try {
			connection = getConnection(info);
			final String procedure = getVirtualProcedure(info);
			
			boolean noParm = false;
			if (inputMessage.equals(StringUtil.Constants.EMPTY_STRING)) {
				noParm = true;
			}
			final String executeStatement = "{?=call " + procedure + (noParm ? "()}" : "(?)}"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

			final CallableStatement statement = connection
					.prepareCall(executeStatement);
			if (!noParm) {
				statement.setString(1, inputMessage);
			}

			statement.setQueryTimeout(getQueryTimeout());

			final boolean hasResultSet = statement.execute();

			if (hasResultSet) {

				// String returnFragment = null;

				final ResultSet set = statement.getResultSet();

				if (set.next()) {
					/*
					 * an XML result set that is appropriate for a Data Service
					 * web service will ALWAYS return a single XML Document
					 * result. The first row in the first column. If there are
					 * additional rows, we throw an exception as this resultset
					 * is not appropriate for a Data Service.
					 */
					returnFragment = set.getString(1);

				} else {
					final String[] params = { procedure };
					LogManager.logError(SOAPPlugin.PLUGIN_ID, SOAPPlugin.Util
							.getString("DataServiceWebServiceImpl.8")); //$NON-NLS-1$
					createSOAPFaultMessage(new Exception(SOAPPlugin.Util
							.getString("DataServiceWebServiceImpl.7", params)), //$NON-NLS-1$
							SOAPPlugin.Util.getString(
									"DataServiceWebServiceImpl.7", params), //$NON-NLS-1$
							SOAP_11_STANDARD_SERVER_FAULT_CODE);
				}

				if (set.next()) {
					final String[] params = { procedure };
					String message = SOAPPlugin.Util.getString(
							"DataServiceWebServiceImpl.1", params); //$NON-NLS-1$
					createSOAPFaultMessage(new SQLException(message), message,
							SOAP_11_STANDARD_SERVER_FAULT_CODE);
				}

				/*
				 * Set OMElement to the result set value
				 */
				element = getElement(returnFragment);

				set.close();
			}

			statement.close();

			/*
			 * If we fall through to here and no XML Fragment has been set on
			 * the returnMessage instance because 'hasResults' was false, then
			 * the return message is an empty message with no body contents. We
			 * do this only because we do not know what to do with a returned
			 * update count. We cannot return it as the body of the message
			 * because we will likely violate the schema type that defines the
			 * return message. The only thing i can think to do is to return an
			 * empty message in this instance. We really should handle this
			 * situation more explicitly in the future. (ie Operations in Web
			 * Service Models in the modeler should be able to be considered
			 * 'update' type operations and return a simple int).
			 */

		} catch (Exception e) {
			String faultcode = SOAP_11_STANDARD_SERVER_FAULT_CODE;
			Object[] params = new Object[] { e };
			String msg = SOAPPlugin.Util.getString(
					"DataServiceWebServiceImpl.6", params); //$NON-NLS-1$
			LogManager.logError(SOAPPlugin.PLUGIN_ID, e, msg);
			if (e instanceof SQLException) {
				final SQLException sqlException = (SQLException) e;
				if (SQLStates.isUsageErrorState(sqlException.getSQLState())) {
					faultcode = SOAP_11_STANDARD_CLIENT_FAULT_CODE;
				}
			}

			element = createSOAPFaultMessage(e, e.getMessage(), faultcode);
		} finally {
			try {
				if (connection != null) {
					connection.close();
				}

			} catch (SQLException e) {
				/*
				 * In this case, we do not return a SOAP fault to the customer.
				 * We simply log this problem in the log. If we do a return from
				 * the finally block, we will override the return in the
				 * try/catch that will either return the 'true' error or return
				 * a valid result document. Either way we do not want to return
				 * a SOAP fault just because closing the connection failed.
				 */

				LogManager.logWarning(SOAPPlugin.PLUGIN_ID, SOAPPlugin.Util
						.getString("DataServiceWebServiceImpl.3")); //$NON-NLS-1$

			}
		}
		return element;
	}

	/**
	 * Establishes a connection to a MetaMatrixServer given the incoming WASP
	 * SOAPMessage instance.
	 * 
	 * @param message
	 *            the incoming SOAP request message that should contain all
	 *            server login information
	 * @throws SOAPException
	 *             if an error occurs
	 */
	protected Connection getConnection(final DataServiceInfo info)
		throws SOAPException, AxisFault {

		final Properties connectionProperties = new Properties();

		String userName = StringUtil.Constants.EMPTY_STRING;
		
		MessageContext msgCtx = MessageContext.getCurrentMessageContext();
		if (msgCtx == null) {
			String message = SOAPPlugin.Util
					.getString("DataServiceWebServiceImpl.0"); //$NON-NLS-1$
			createSOAPFaultMessage(new SQLException(message), message,
					SOAP_11_STANDARD_CLIENT_FAULT_CODE);
		}
		
		Credential credential = null;
		try {
			credential = WebServiceUtil.getCredentials(msgCtx);
		} catch (AxisFault e) {
			throw e;
		}
		
		userName = credential.getUserName();
		connectionProperties.setProperty(ConnectionSource.USERNAME, userName);
		connectionProperties.setProperty(ConnectionSource.PASSWORD, new String(credential.getPassword()));
		connectionProperties.setProperty(ConnectionSource.SERVER_URL, info
				.getServerURL());

		Connection connection = null;

		// get a Connection to the Metamatrix Server

		try {

			connection = connectionSource.getConnection(connectionProperties);

		} catch (Exception e) {
			final String[] param = { userName, e.getMessage() };
			throw new SOAPException(SOAPPlugin.Util.getString(
					"DataServiceWebServiceImpl.12", param) //$NON-NLS-1$
					, e);
		}
		return connection;
	}

	/**
	 * This method simply puts the given parameters into a Properties object
	 * keyed correctly for our JDBC driver and returns that Properties object.
	 * 
	 * @param username
	 * @param password
	 * @return
	 */
	protected Properties getAuthenticationProperties(final String username,
			final String password) {
		final Properties props = new Properties();

		if (username != null) {
			props.setProperty(USER_NAME, username);
		}
		if (password != null) {
			props.setProperty(PASSWORD, password);
		}

		return props;
	}

	protected DataServiceInfo getDataServiceInfo(final OMElement element)
			throws SOAPProcessingException {
		return EndpointUriTranslatorStrategyImpl.getDataServiceInfo();
	}

	protected OMElement createSOAPFaultMessage(final Exception e,
			final String faultMessageString, final String faultCode)
			throws AxisFault {

		LogManager.logError(SOAPPlugin.PLUGIN_ID, e, faultMessageString);

		AxisFault fault = new AxisFault(faultMessageString, faultCode);

		throw fault;
	}

	/**
	 * Get the list of SOAPBodyElements from the Request message.
	 * 
	 * @param SOAPBody
	 *            body
	 * @return List bodyElements
	 */
	private List getBodyElements(final SOAPBody body) {
		List elements = new LinkedList();
		if (body == null) {
			return elements;
		}

		Iterator iter = body.getChildElements();
		if (iter != null) {
			while (iter.hasNext()) {
				Object obj = iter.next();
				if (obj instanceof OMElement) {
					elements.add(obj);
				}
			}
		}
		return elements;
	}

	/**
	 * Create a <@link ResponseBodyElement> from the XML result of the Virtual
	 * Procedure.
	 * 
	 * @param String
	 *            result returned from Virtual Procedure call
	 * @return ResponseBodyElement
	 */
	private OMElement getElement(String result) throws AxisFault {

		StAXOMBuilder builder = null;
		try {
			builder = new StAXOMBuilder(ByteArrayHelper.toInputStream(result
					.getBytes()));
		} catch (XMLStreamException e) {
			Object[] params1 = new Object[] { result };
			Object[] params2 = new Object[] { e.getMessage() };
			String s1 = SOAPPlugin.Util.getString(
					"DataServiceWebServiceImpl.9", params1); //$NON-NLS-1$
			LogManager.logError(SOAPPlugin.PLUGIN_ID, e, s1);
			String s2 = SOAPPlugin.Util.getString(
					"DataServiceWebServiceImpl.10", params2); //$NON-NLS-1$
			createSOAPFaultMessage(e, s2, SOAP_11_STANDARD_SERVER_FAULT_CODE);
		} catch (Exception e) {
			createSOAPFaultMessage(e, e.getMessage(),
					SOAP_11_STANDARD_SERVER_FAULT_CODE);
		}

		OMElement documentElement = builder.getDocumentElement();

		return documentElement;
	}

	/**
	 * Check for a query timeout as a System property. If we don't find one, we
	 * will default to zero (no timeout).
	 * 
	 * @return timeout
	 */
	protected int getQueryTimeout() {
		int timeout = 0;

		if (System.getProperty(WSDLServletUtil.MM_WEBSERVICE_QUERY_TIMEOUT) != null) {
			try {
				timeout = Integer
						.parseInt(System
								.getProperty(WSDLServletUtil.MM_WEBSERVICE_QUERY_TIMEOUT));
			} catch (NumberFormatException nfe) {
				LogManager.logWarning(SOAPPlugin.PLUGIN_ID, nfe, SOAPPlugin.Util
						.getString("DataServiceWebServiceImpl.16")); //$NON-NLS-1$
				timeout = 0;
			}
		}

		return timeout;
	}

	/**
	 * Get the SOAPAction value. This contains the Virtual Procedure name to
	 * execute. We need to change double-quotes with blanks or the mm server
	 * will not be able to parse the SQL string.
	 * 
	 * @param SOAPEnvelope
	 *            resp
	 * @return String soapAction
	 */
	private String getVirtualProcedure(DataServiceInfo info) {
	
		final String storedProc = info.getDataServiceFullPath();

		return StringUtil.replaceAll(storedProc, DOUBLE_QUOTE, EMPTY_STRING);
	}

}