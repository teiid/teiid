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

package com.metamatrix.connector.ldap;

import java.util.Hashtable;
import java.util.Properties;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.ldap.InitialLdapContext;

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ConnectorLogger;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.api.ResultSetExecution;
import org.teiid.connector.api.UpdateExecution;
import org.teiid.connector.basic.BasicConnection;
import org.teiid.connector.language.Command;
import org.teiid.connector.language.Select;
import org.teiid.connector.language.QueryExpression;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;


/** 
 * Represents a connection to an LDAP data source. 
 */
public class LDAPConnection extends BasicConnection {  

	private LDAPManagedConnectionFactory config;
	
	// Standard Connection data members
	private InitialLdapContext initCtx;
	
	// LDAP-specific properties
    public static final String LDAP_AUTH_TYPE = "simple"; //$NON-NLS-1$
    public static final String LDAP_USER_OBJECT_TYPE = "person"; //$NON-NLS-1$
    public static final String LDAP_REFERRAL_MODE = "follow"; //$NON-NLS-1$
	
	private String jndiLdapCtxFactory;

	public LDAPConnection(LDAPManagedConnectionFactory config) throws ConnectorException {
		this(config, LDAPConnectorConstants.JNDI_LDAP_CTX_FACTORY);
	}
	
	public LDAPConnection(LDAPManagedConnectionFactory config, String jndiLdapCtxFactory) throws ConnectorException {
		this.config = config;
		this.jndiLdapCtxFactory = jndiLdapCtxFactory;
			
		checkProperties();

		// Create initial LDAP connection.
		try {
			this.initCtx = initializeLDAPContext();
		} catch(ConnectorException ce) {
            final String msg = LDAPPlugin.Util.getString("LDAPConnection.contextInitFailed"); //$NON-NLS-1$
			throw new ConnectorException(ce, msg);
		}

		this.config.getLogger().logDetail("LDAP Connection has been newly created."); //$NON-NLS-1$
	}
	
	public void setJndiLdapCtxFactory(String jndiLdapCtxFactory) {
		this.jndiLdapCtxFactory = jndiLdapCtxFactory;
	}
	
    /**
     * Helper method to retrieve the LDAP Connector properties.  If any properties are in error,
     * a ConnectorException is thrown.
     * @param props
     */
	private void checkProperties() throws ConnectorException {
		// LDAP URL 
		if(this.config.getLdapUrl() == null) {
            final String msg = LDAPPlugin.Util.getString("LDAPConnection.urlPropNotFound"); //$NON-NLS-1$
            throw new ConnectorException(msg);
		}
		// LDAP Admin User DN
		if(this.config.getLdapAdminUserDN() == null) {
            final String msg = LDAPPlugin.Util.getString("LDAPConnection.adminUserDNPropNotFound"); //$NON-NLS-1$
            throw new ConnectorException(msg);
		}
		// LDAP Admin User Password
		if(this.config.getLdapAdminUserPassword() == null) {
            final String msg = LDAPPlugin.Util.getString("LDAPConnection.adminUserPassPropNotFound"); //$NON-NLS-1$
            throw new ConnectorException(msg);
		}
	}
	
	/**
	 * Setup a standard initial LDAP context using JNDI's context factory.
	 * This method may be extended to support Sun-specific and AD-specific
	 * contexts, in order to support the different paging implementations they provide.
	 * @return the initial LDAP Context
	 */
	private InitialLdapContext initializeLDAPContext() throws ConnectorException {
	  	// Create the root context.
		InitialLdapContext initContext;

		Hashtable connenv = new Hashtable();
		connenv.put(Context.INITIAL_CONTEXT_FACTORY, jndiLdapCtxFactory);
		connenv.put(Context.PROVIDER_URL, this.config.getLdapUrl());
		connenv.put(Context.REFERRAL, LDAP_REFERRAL_MODE);
		// If username is blank, we will perform an anonymous bind.
		// Note: This is not supported when using Sun's VLVs, so remove this if VLVs are used.
		if(!this.config.getLdapAdminUserDN().equals("")) { //$NON-NLS-1$

			connenv.put(Context.SECURITY_AUTHENTICATION, LDAP_AUTH_TYPE);
			connenv.put(Context.SECURITY_PRINCIPAL, this.config.getLdapAdminUserDN());
			connenv.put(Context.SECURITY_CREDENTIALS, this.config.getLdapAdminUserPassword());
		} else {
			this.config.getLogger().logWarning("LDAP Username DN was blank; performing anonymous bind."); //$NON-NLS-1$
			connenv.put(Context.SECURITY_AUTHENTICATION, "none"); //$NON-NLS-1$
		}
		
		if(this.config.getLdapTxnTimeoutInMillis() != -1) { //$NON-NLS-1$
			connenv.put("com.sun.jndi.ldap.connect.timeout", this.config.getLdapTxnTimeoutInMillis()); //$NON-NLS-1$
		}
		
		// Enable connection pooling for the Initial context.
		connenv.put("com.sun.jndi.ldap.connect.pool", "true"); //$NON-NLS-1$ //$NON-NLS-2$
		connenv.put("com.sun.jndi.ldap.connect.pool.debug", "fine"); //$NON-NLS-1$ //$NON-NLS-2$
		
		try {
			initContext = new InitialLdapContext(connenv, null);
		} catch(NamingException ne){ 
            final String msg = LDAPPlugin.Util.getString("LDAPConnection.directoryNamingError",ne.getExplanation()); //$NON-NLS-1$
			throw new ConnectorException(msg);
		} catch(Exception e) {
            final String msg = LDAPPlugin.Util.getString("LDAPConnection.directoryInitError"); //$NON-NLS-1$
			throw new ConnectorException(e, msg); 
		}
		this.config.getLogger().logDetail("Successfully obtained initial LDAP context."); //$NON-NLS-1$
		return initContext;
	}
	
	@Override
	public ResultSetExecution createResultSetExecution(QueryExpression command,
			ExecutionContext executionContext, RuntimeMetadata metadata)
			throws ConnectorException {
		return new LDAPSyncQueryExecution((Select)command, executionContext, metadata, this.initCtx, this.config);
	}
	
	@Override
	public UpdateExecution createUpdateExecution(Command command,
			ExecutionContext executionContext, RuntimeMetadata metadata)
			throws ConnectorException {
		return new LDAPUpdateExecution(command, executionContext, metadata, this.initCtx, this.config);
	}
	
	/** 
	 * Closes LDAP context, effectively closing the connection to LDAP.
	 * (non-Javadoc)
	 * @see com.metamatrix.connector.pool.PoolAwareConnection#closeSource()
	 */
	@Override
    public void close() {
		if(initCtx != null) {
			try {
				initCtx.close();
			} catch(NamingException e) {
				this.config.getLogger().logDetail(LDAPPlugin.Util.getString("LDAPConnection.contextCloseError",e.getExplanation())); //$NON-NLS-1$
			}
		}
		this.config.getLogger().logDetail("LDAP context has been closed."); //$NON-NLS-1$
	}

	/** 
	 * Currently, this method always returns alive. We assume the connection is alive,
	 * and rely on proper timeout values to automatically clean up connections before
	 * any server-side timeout occurs. Rather than incur overhead by rebinding,
	 * we'll assume the connection is always alive, and throw an error when it is actually used,
	 * if the connection fails. This may be a more efficient way of handling failed connections,
	 * with the one tradeoff that stale connections will not be detected until execution time. In
	 * practice, there is no benefit to detecting stale connections before execution time.
	 * 
	 * One possible extension is to implement a UnsolicitedNotificationListener.
	 * (non-Javadoc)
	 * @see com.metamatrix.connector.pool.PoolAwareConnection#isAlive()
	 */
	@Override
	public boolean isAlive() {
		this.config.getLogger().logTrace("LDAP Connection is alive."); //$NON-NLS-1$
		return true;
	}
	
}
