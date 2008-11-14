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
package com.metamatrix.connector.ldap;

import java.util.Properties;

import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.ConnectorLogger;
import com.metamatrix.data.api.ExecutionContext;
import com.metamatrix.data.api.SecurityContext;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.pool.ConnectorIdentity;
import com.metamatrix.data.pool.SingleIdentity;
import com.metamatrix.data.pool.SourceConnection;
import com.metamatrix.data.pool.SourceConnectionFactory;

/**
 * Factory for creation of new source connections.
 */
public class LDAPSourceConnectionFactory implements SourceConnectionFactory {

	private Properties props;
	private ConnectorLogger logger;
	private int executionMode;

	/**
	 * Constructor.
	 * @param executionMode the execution mode
	 * @param props the Connector properties
	 * @logger the ConnectorLogger
	 */
	public LDAPSourceConnectionFactory(int executionMode, Properties props, ConnectorLogger logger) {
		this.executionMode = executionMode;
		this.props = props;
		this.logger = logger;
	}
	
	/** 
	 * Create a new source connection.
	 * @param id User identity.
	 * (non-Javadoc)
	 * @see com.metamatrix.data.pool.SourceConnectionFactory#createConnection(com.metamatrix.data.pool.ConnectorIdentity)
	 */
	public SourceConnection createConnection(ConnectorIdentity id)
			throws ConnectorException {
        final String msg = LDAPPlugin.Util.getString("LDAPSourceConnectionFactory.creatingConnection"); //$NON-NLS-1$
		logger.logDetail(msg); 
		return new LDAPConnection(executionMode, (ExecutionContext)id.getSecurityContext(), this.props, this.logger);
	}

	/** 
	 * Returns a SingleIdentity, using the cached identity if one exists. 
	 * Note that per-user identities are not supported, and no attempt is made to 
	 * identity each security context separately.
	 * (non-Javadoc)
	 * @see com.metamatrix.data.pool.SourceConnectionFactory#createIdentity(com.metamatrix.data.api.SecurityContext)
	 */
	public ConnectorIdentity createIdentity(SecurityContext context)
			throws ConnectorException {
		return new SingleIdentity(context);
	}

	/** Holds onto the connector environment for later use. 
	 * (non-Javadoc)
	 * @see com.metamatrix.data.pool.SourceConnectionFactory#initialize(com.metamatrix.data.api.ConnectorEnvironment)
	 */
	public void initialize(ConnectorEnvironment env) throws ConnectorException {
	}

	/** Use a single, shared identity to establish
	 * connections to LDAP. No per-user connection pooling is supported.
	 * (non-Javadoc)
	 * @see com.metamatrix.data.pool.SourceConnectionFactory#isSingleIdentity()
	 */
	public boolean isSingleIdentity() {
		return true;
	}

}
