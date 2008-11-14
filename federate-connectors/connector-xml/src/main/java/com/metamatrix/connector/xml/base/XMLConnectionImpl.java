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


package com.metamatrix.connector.xml.base;

import java.io.Serializable;

import com.metamatrix.connector.xml.CachingConnector;
import com.metamatrix.connector.xml.SecureConnectorState;
import com.metamatrix.connector.xml.TrustedPayloadHandler;
import com.metamatrix.connector.xml.XMLConnection;
import com.metamatrix.connector.xml.XMLConnectorState;
import com.metamatrix.data.api.ConnectorCapabilities;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.ConnectorLogger;
import com.metamatrix.data.api.ConnectorMetadata;
import com.metamatrix.data.api.Execution;
import com.metamatrix.data.api.ExecutionContext;
import com.metamatrix.data.api.SecurityContext;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.metadata.runtime.RuntimeMetadata;

public class XMLConnectionImpl implements XMLConnection {

	private CachingConnector connector;

	private ConnectorEnvironment connectorEnv;

	private ConnectorLogger logger;
	
	private String m_queryId;
	private String m_user;
	private Serializable m_executionPayload;

	private Serializable m_trustedPayload;

	private TrustedPayloadHandler payloadHandler;

	public XMLConnectionImpl(CachingConnector connector, SecurityContext context,
			ConnectorEnvironment connectorEnv) throws ConnectorException {
		this.connector = connector;
		this.connectorEnv = connectorEnv;
		logger = connector.getState().getLogger();
		logger.logTrace("XMLConnection initialized for Request Identifier " + context.getRequestIdentifier());
		setQueryId(context.getRequestIdentifier());
		setUser(context.getUser());
		setTrustedPayload(context.getTrustedPayload());
		setExecutionPayload(context.getExecutionPayload());
		processTrustPayload();
	}
	///////////////////////////////////////////////////////////////
	//Connection API Implementation
	public ConnectorCapabilities getCapabilities() {
		return connector.getState().getConnectorCapabilities();
	}

	public Execution createExecution(int executionMode,
			ExecutionContext context, RuntimeMetadata metadata)
			throws ConnectorException {
		try {
			Execution retVal = null;
			String errKey = null;
			switch (executionMode) {
			case ConnectorCapabilities.EXECUTION_MODE.SYNCH_QUERY:
				retVal = new XMLExecutionImpl(this, metadata, context, connectorEnv);
				break;
			case ConnectorCapabilities.EXECUTION_MODE.UPDATE:
				errKey = Messages
						.getString("XMLConnectionImpl.update.not.supported");
				break;
			case ConnectorCapabilities.EXECUTION_MODE.PROCEDURE:
				errKey = Messages.getString("XMLConnectionImpl.no.xml.procedures");
				break;
			default:
				errKey = Messages
						.getString("XMLConnectionImpl.invalid.execution.mode");
			}
			if (errKey != null) {
				throw new ConnectorException(errKey);
			}
			return retVal;
		} catch (RuntimeException e) {
			throw new ConnectorException(e);
		}

	}

	public ConnectorMetadata getMetadata() {
		return null;
	}

	public void release() {
		logger.logTrace("XMLConnection released for RequestIdentifier " + 
				getQueryId());
	}
	//End Connection API Implementation
	///////////////////////////////////////////////////////////////
	
	///////////////////////////////////////////////////////////////
	//XML Connection API Implementation
	public XMLConnectorState getState() {
		return connector.getState();
	}

	public CachingConnector getConnector() {
		return connector;
	}

	public TrustedPayloadHandler getTrustedPayloadHandler() {
		return payloadHandler;
	}

	public String getQueryId() {
		return m_queryId;
	}

	public String getUser() {
		return m_user;
	}
	//End XML Connection API Implementation
	///////////////////////////////////////////////////////////////
	
	private void setQueryId(String queryId) {
		m_queryId = queryId;
	}

	private void setUser(String user) {
		m_user = user;
	}
	
	private void setExecutionPayload(Serializable ser) {
		m_executionPayload = ser;
	}

	private Serializable getExecutionPayload() {
		return m_executionPayload;
	}

	private void setTrustedPayload(Serializable ser) {
		m_trustedPayload = ser;
	}

	private Serializable getTrustedPayload() {
		return m_trustedPayload;
	}

	private void processTrustPayload() throws ConnectorException {
		if (getState() instanceof SecureConnectorStateImpl) {
			payloadHandler = ((SecureConnectorState)getState()).getTrustDeserializerInstance();
			payloadHandler.setExecutionPayload(getExecutionPayload());
			payloadHandler.setTrustedPayload(getTrustedPayload());
			payloadHandler.setConnectorName(connectorEnv.getConnectorName());
			payloadHandler.setLogger(connector.getLogger());
			try {
				payloadHandler.processPayloads();
			} catch (Throwable e) {
				String message  = Messages.getString("XMLConnectionImpl.exception.processing.trusted.payload");
				throw new ConnectorException(e, message);
			}
		}
	}
	
	public ConnectorEnvironment getConnectorEnv() {
		return connectorEnv;
	}
}
