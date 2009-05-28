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



package com.metamatrix.connector.xml.http;

import java.io.Serializable;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ConnectorLogger;

import com.metamatrix.connector.xml.TrustedPayloadHandler;
import com.metamatrix.connector.xml.XMLConnectorState;

public interface HTTPTrustDeserializer extends TrustedPayloadHandler{

	
	public void setTrustedPayload(Serializable payload);
	
	public void setExecutionPayload(Serializable payload);
	
	public void processPayloads() throws Exception;
	
	public void modifyRequest(HttpClient client, HttpMethod method) throws ConnectorException;

	public void setLogger(ConnectorLogger logger);
	
	public void setConnectorState(XMLConnectorState state);
	
	public void setSystemName(String name); 
	
	//if you desire to override the http basic authentication from the connector
	//binding properties, here is how that would be done:
	
	//user and pwd are Strings extracted from your payload
	
	//client.getState().setAuthenticationPreemptive(true);
	//Credentials defCred = new UsernamePasswordCredentials(user, pwd);
	//client.getState().setCredentials(null, null, defCred);
	
}
