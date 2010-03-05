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

import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorLogger;
import org.teiid.connector.xml.TrustedPayloadHandler;


public abstract class TrustedPayloadBridge implements TrustedPayloadHandler {

	protected String username;
	protected String password;
	protected ConnectorEnvironment connectorEnvorinment;
	protected String m_systemName;
	protected ConnectorLogger m_logger = null;
	protected Subject subject;

	public TrustedPayloadBridge() {
		super();
	}
	
	public String getUser() {
		return username;
	}

	public String getPassword() {
		return password;
	}

	public void setUser(String userName) {
		this.username = userName;
	}

	public void setPassword(String password) {
		this.password = password;
	}
	
	public void setConnectorEnvironment(ConnectorEnvironment connEnv) {
		this.connectorEnvorinment = connEnv;
	}

	public void setSubject(Subject subject) {
		this.subject = subject;
	}

	public void setLogger(ConnectorLogger logger) {
		m_logger = logger;
	}
}
