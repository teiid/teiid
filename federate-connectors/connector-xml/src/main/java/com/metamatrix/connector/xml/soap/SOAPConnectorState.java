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



package com.metamatrix.connector.xml.soap;

import java.util.Properties;

import com.metamatrix.connector.xml.DocumentProducer;
import com.metamatrix.connector.xml.XMLExecution;
import com.metamatrix.connector.xml.http.HTTPConnectorState;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.ConnectorLogger;
import com.metamatrix.data.exception.ConnectorException;

/**
 * This class copies the the name of the interface because it is remaining
 *  backwardly compatable with old bindings. 
 */
public class SOAPConnectorState extends HTTPConnectorState implements
		com.metamatrix.connector.xml.SOAPConnectorState {

	com.metamatrix.connector.xml.SOAPConnectorState soapState;
	/**
	 * 
	 */
	public SOAPConnectorState() {
		super();
		soapState = new SOAPConnectorStateImpl();
	}

	
	public Properties getState() {
		Properties props = super.getState(); 
		props.putAll(soapState.getState());
		return props;
	}


	public void setState(ConnectorEnvironment env) throws ConnectorException {
		super.setState(env);
		soapState.setState(env);
	}

	public void setLogger(ConnectorLogger logger) {
		super.setLogger(logger);
		soapState.setLogger(logger);
	}

	/* (non-Javadoc)
	 * @see com.metamatrix.connector.xml.base.XMLConnectorStateImpl#getExecutor(com.metamatrix.connector.xml.base.XMLExecutionImpl)
	 */
	public DocumentProducer makeExecutor(XMLExecution execution) throws ConnectorException {
		return new SOAPExecutor(this, execution);
	}

	/* (non-Javadoc)
	 * @see com.metamatrix.connector.xml.SOAPConnectorState#isEncoded()
	 */
	public boolean isEncoded() {
		return soapState.isEncoded();
	}

	/* (non-Javadoc)
	 * @see com.metamatrix.connector.xml.SOAPConnectorState#isRPC()
	 */
	public boolean isRPC() {
		return soapState.isRPC();
	}

	/* (non-Javadoc)
	 * @see com.metamatrix.connector.xml.SOAPConnectorState#isExceptionOnFault()
	 */
	public boolean isExceptionOnFault() {
		return soapState.isExceptionOnFault();
	}
}
