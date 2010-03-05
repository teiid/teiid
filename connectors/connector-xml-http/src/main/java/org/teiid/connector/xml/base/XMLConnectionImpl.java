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


package org.teiid.connector.xml.base;

import javax.security.auth.Subject;

import org.teiid.connector.api.ConnectionContext;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.basic.BasicConnection;
import org.teiid.connector.xml.SecureConnectorState;
import org.teiid.connector.xml.StatefulConnector;
import org.teiid.connector.xml.TrustedPayloadHandler;
import org.teiid.connector.xml.XMLConnection;
import org.teiid.connector.xml.XMLConnectorState;

import com.metamatrix.connector.xml.base.Messages;
import com.metamatrix.connector.xml.base.XMLBaseManagedConnectionFactory;

public class XMLConnectionImpl extends BasicConnection implements XMLConnection {

	private StatefulConnector connector;

	private XMLBaseManagedConnectionFactory connectorEnv;
	private TrustedPayloadHandler payloadHandler;
	private Subject subject;
	
	public XMLConnectionImpl(StatefulConnector connector, XMLBaseManagedConnectionFactory connectorEnv) throws ConnectorException {
		this.connector = connector;
		this.connectorEnv = connectorEnv;
		this.subject = ConnectionContext.getSubject();
		processTrustPayload();
	}
	///////////////////////////////////////////////////////////////
	//Connection API Implementation
	@Override
	public void close() {
	}

	//End Connection API Implementation
	///////////////////////////////////////////////////////////////
	
	///////////////////////////////////////////////////////////////
	//XML Connection API Implementation
	public XMLConnectorState getState() {
		return connector.getState();
	}

	public StatefulConnector getConnector() {
		return connector;
	}
	
	public TrustedPayloadHandler getTrustedPayloadHandler() {
		return payloadHandler;
	}	
	
	//End XML Connection API Implementation
	///////////////////////////////////////////////////////////////
	private void processTrustPayload() throws ConnectorException {
		if (getState() instanceof SecureConnectorStateImpl) {
			payloadHandler = ((SecureConnectorState)getState()).getTrustDeserializerInstance();
			payloadHandler.setSubject(this.subject);
			payloadHandler.setLogger(connector.getLogger());
			try {
				payloadHandler.processPayloads();
			} catch (Throwable e) {
				String message  = Messages.getString("XMLConnectionImpl.exception.processing.trusted.payload");
				throw new ConnectorException(e, message);
			}
		}
	}
	
	public XMLBaseManagedConnectionFactory getConnectorEnv() {
		return connectorEnv;
	}
}
