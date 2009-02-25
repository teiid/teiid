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



package com.metamatrix.connector.xml.jms;

import java.util.Hashtable;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.JMSSecurityException;
import javax.jms.Session;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.api.ResultSetExecution;
import org.teiid.connector.basic.BasicConnection;
import org.teiid.connector.language.IQuery;
import org.teiid.connector.language.IQueryCommand;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;

import com.metamatrix.connector.xml.CachingConnector;
import com.metamatrix.connector.xml.IQueryPreprocessor;
import com.metamatrix.connector.xml.TrustedPayloadHandler;
import com.metamatrix.connector.xml.XMLConnection;
import com.metamatrix.connector.xml.XMLConnectorState;

public class JMSConnection extends BasicConnection implements XMLConnection {

	ExecutionContext secCtx;
	ConnectorEnvironment connectorEnv;
	CachingConnector connector;
	private JMSXMLConnectorState state;
	private Context context;
	private ConnectionFactory factory;
	private javax.jms.Connection jmsConnection;
	private Destination outboundDestination;
	private Destination inboundDestination;
	private String password;
	private String userName;
		
	public JMSConnection(CachingConnector connector, ExecutionContext secCtx, ConnectorEnvironment connectorEnv) throws ConnectorException{
		super();
		this.connector = connector;
		this.secCtx = secCtx;
		this.connectorEnv = connectorEnv;
		state = (JMSXMLConnectorState) connector.getState();
		if (null == state) {
			throw new ConnectorException(Messages.getString("JMSConnection.no.state"));
		} else {
			Hashtable properties = new Hashtable();
		    properties.put(Context.INITIAL_CONTEXT_FACTORY, state.getInitialContextFactoryName());
		    properties.put(Context.PROVIDER_URL, state.getPrimaryProviderUrl());
		    
		    try {
				context = new InitialContext(properties);
			} catch (NamingException e) {
				throw new ConnectorException(e, Messages.getString("JMSConnection.cannot.construct.InitialContext"));
			}
			
			try {
				factory = (ConnectionFactory) context.lookup(state.getConnectionFactoryName());
			} catch (NamingException e) {
				throw new ConnectorException(e, Messages.getString("JMSConnection.cannot.get.ConnectionFactory"));
			}
			
			try {
				if(usingCredentials()) {
					jmsConnection = factory.createConnection(getUserName(), getPassword());
				} else {
					jmsConnection = factory.createConnection();
				}
			} catch (JMSSecurityException e) {
				throw new ConnectorException(e, Messages.getString("JMSConnection.auth.failed"));
			} catch (JMSException e) {
				throw new ConnectorException(e, Messages.getString("JMSConnection.cannot.create.Connection"));
			}
			
			try {
				String outboundDestinationName = state.getOutboundJMSDestination();
				if (null != outboundDestinationName) {
					setOutboundDestination((Destination) context.lookup(outboundDestinationName));
				}
			} catch (NamingException e) {
				throw new ConnectorException(e, Messages.getString("JMSConnection.cannot.get.OutboundDestination"));
			}
			
			try {
				String inboundDestinationName = state.getInboundJMSDestination();
				if (null != inboundDestinationName) {
					setInboundDestination( (Destination) context.lookup(inboundDestinationName));
				}
			} catch (NamingException e) {
				throw new ConnectorException(e, Messages.getString("JMSConnection.cannot.get.InboundDestination"));
			}
		}
	}

	public XMLConnectorState getState() {
		return state;
	}

	private String getUserName() {
		return userName;
	}

	private String getPassword() {
		return password;
	}

	private boolean usingCredentials() {
		return (userName != null && password != null);
	}
	
	@Override
	public ResultSetExecution createResultSetExecution(IQueryCommand command,
			ExecutionContext executionContext, RuntimeMetadata metadata)
			throws ConnectorException {
		return new JMSExecution((IQuery)command, this, metadata, executionContext, this.connectorEnv, connector.getLogger());
	}

	@Override
	public void close() {
		try {
			jmsConnection.close();
		} catch (JMSException e) {
			connector.getLogger().logError(Messages.getString("JMSConnection.exception.on.close"), e);
		}
	}

	public Destination getInboundDestination() {
		return inboundDestination;
	}

	private void setInboundDestination(Destination inboundDestination) {
		this.inboundDestination = inboundDestination;
	}

	public Destination getOutboundDestination() {
		return outboundDestination;
	}

	private void setOutboundDestination(Destination outboundDestination) {
		this.outboundDestination = outboundDestination;
	}
	
	public Session getJMSSession() throws JMSException {
		return jmsConnection.createSession(false, state.getAcknowledgementMode());
	}
	
	public IQueryPreprocessor getQueryPreprocessor() {
		return state.getPreprocessor();
	}

	public CachingConnector getConnector() {
		return connector;
	}

	public TrustedPayloadHandler getTrustedPayloadHandler() throws ConnectorException {
		return state.getTrustDeserializerInstance();
	}

	public String getQueryId() {
		return secCtx.getRequestIdentifier();
	}
	
	public String getUser() {
		return secCtx.getUser();
	}
	
	public void start() throws ConnectorException {
		try {
			jmsConnection.start();
		} catch (JMSException e) {
			throw new ConnectorException(e);
		}
	}
	
	public void stop() throws ConnectorException {
		try {
			jmsConnection.close();
		} catch (JMSException e) {
			throw new ConnectorException(e);
		}
	}

	public ConnectorEnvironment getConnectorEnv() {
		return connectorEnv;
	}
}
