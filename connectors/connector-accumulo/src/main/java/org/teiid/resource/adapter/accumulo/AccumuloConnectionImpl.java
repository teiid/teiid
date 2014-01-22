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

package org.teiid.resource.adapter.accumulo;

import java.util.List;

import javax.resource.ResourceException;
import javax.security.auth.Subject;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.impl.ConnectorImpl;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.teiid.core.util.StringUtil;
import org.teiid.resource.spi.BasicConnection;
import org.teiid.resource.spi.ConnectionContext;
import org.teiid.translator.accumulo.AccumuloConnection;

public class AccumuloConnectionImpl extends BasicConnection implements AccumuloConnection {
	private ConnectorImpl conn;
	private String[] roles;
	
	public AccumuloConnectionImpl(AccumuloManagedConnectionFactory mcf, ZooKeeperInstance inst) throws ResourceException {
		try {
			if (mcf.getRoles() != null) {
				List<String> auths = StringUtil.getTokens(mcf.getRoles(), ",");  //$NON-NLS-1$
				this.roles = auths.toArray(new String[auths.size()]);
			}
			
			String userName = mcf.getUsername();
			String password = mcf.getPassword();
			
			// if security-domain is specified and caller identity is used; then use
			// credentials from subject
			Subject subject = ConnectionContext.getSubject();
			if (subject != null) {
				userName = ConnectionContext.getUserName(subject, mcf, userName);
				password = ConnectionContext.getPassword(subject, mcf, userName, password);
				this.roles = ConnectionContext.getRoles(subject, this.roles);
			}
			this.conn = (ConnectorImpl) inst.getConnector(userName, new PasswordToken(password));
		} catch (AccumuloException e) {
			throw new ResourceException(e);
		} catch (AccumuloSecurityException e) {
			throw new ResourceException(e);
		}
	}
	
	@Override
	public Connector getInstance() {
		return conn;
	}	
	
	@Override
	public void close() throws ResourceException {
		// Where is the close call on instance Accumulo? Am I supposed to 
		// waste resources??
	}

	@Override
	public Authorizations getAuthorizations() {
		if (roles != null && roles.length > 0) {
			return new Authorizations(roles);
		}
		return new Authorizations();
	}
}
