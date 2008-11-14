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

import java.util.Hashtable;
import java.util.Properties;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.ldap.InitialLdapContext;

import com.metamatrix.data.api.Connection;
import com.metamatrix.data.api.ConnectorCapabilities;
import com.metamatrix.data.api.ConnectorLogger;
import com.metamatrix.data.api.ConnectorMetadata;
import com.metamatrix.data.api.Execution;
import com.metamatrix.data.api.ExecutionContext;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.metadata.runtime.RuntimeMetadata;
import com.metamatrix.data.pool.ConnectionPool;
import com.metamatrix.data.pool.SourceConnection;

/** 
 * Represents a connection to an LDAP data source. 
 */
public class LDAPConnection implements Connection, SourceConnection {  

	// Standard Connection data members
	private ConnectorLogger logger;
	private InitialLdapContext initCtx;
	private ConnectionPool pool;
	private Properties props;
	
	// LDAP-specific properties
    public static final String LDAP_AUTH_TYPE = "simple"; //$NON-NLS-1$
    public static final String LDAP_USER_OBJECT_TYPE = "person"; //$NON-NLS-1$
    public static final String LDAP_REFERRAL_MODE = "follow"; //$NON-NLS-1$
	private String ldapURL;
	private String ldapAdminUserDN;
	private String ldapAdminUserPass;
	private String ldapTxnTimeoutInMillis;
	private int ldapMaxCriteria;
	private LDAPConnectorCapabilities myCaps;
	

    /**
     * Constructor.
     * @param executionMode
     * @param ctx
     * @param props
     * @param logger
     */
	public LDAPConnection(int executionMode, ExecutionContext ctx, Properties props, ConnectorLogger logger) throws ConnectorException {
		this.logger = logger;
		this.props = props;
			
		// Get properties for initial connection.
		try {
			parseProperties(props);
		} catch(ConnectorException ce1) {
            final String msg = LDAPPlugin.Util.getString("LDAPConnection.propertyFileParseFailed"); //$NON-NLS-1$
			logger.logError(msg); 
			throw new ConnectorException(ce1, msg); 
		}
		
		// Create and configure capabilities class.
		myCaps = new LDAPConnectorCapabilities();
		myCaps.setInCriteriaSize(ldapMaxCriteria);
		
		// Create initial LDAP connection.
		try {
			this.initCtx = initializeLDAPContext();
		} catch(ConnectorException ce) {
            final String msg = LDAPPlugin.Util.getString("LDAPConnection.contextInitFailed"); //$NON-NLS-1$
			throw new ConnectorException(ce, msg);
		}
		logger.logDetail("LDAP Connection has been newly created."); //$NON-NLS-1$
	}
	
    /**
     * Helper method to retrieve the LDAP Connector properties.  If any properties are in error,
     * a ConnectorException is thrown.
     * @param props
     */
	private void parseProperties(Properties props) throws ConnectorException {
		// LDAP URL 
		if((ldapURL = props.getProperty(LDAPConnectorPropertyNames.LDAP_URL)) == null) {
            final String msg = LDAPPlugin.Util.getString("LDAPConnection.urlPropNotFound"); //$NON-NLS-1$
            throw new ConnectorException(msg);
		}
		// LDAP Admin User DN
		if((ldapAdminUserDN = props.getProperty(LDAPConnectorPropertyNames.LDAP_ADMIN_USER_DN)) == null) {
            final String msg = LDAPPlugin.Util.getString("LDAPConnection.adminUserDNPropNotFound"); //$NON-NLS-1$
            throw new ConnectorException(msg);
		}
		// LDAP Admin User Password
		if((ldapAdminUserPass = props.getProperty(LDAPConnectorPropertyNames.LDAP_ADMIN_USER_PASSWORD)) == null) {
            final String msg = LDAPPlugin.Util.getString("LDAPConnection.adminUserPassPropNotFound"); //$NON-NLS-1$
            throw new ConnectorException(msg);
		}
		// LDAP Txn Timeout in Milliseconds
		if((ldapTxnTimeoutInMillis = props.getProperty(LDAPConnectorPropertyNames.LDAP_TXN_TIMEOUT_IN_MILLIS)) == null) {
			// If unset, leave it at null, since we will default to TCP timeout value.
		}
		// LDAP Max In Criteria
		String ldapMaxCriteriaStr = props.getProperty(LDAPConnectorPropertyNames.LDAP_MAX_CRITERIA);
		if(ldapMaxCriteriaStr!=null) {
			try {
				ldapMaxCriteria = Integer.parseInt(ldapMaxCriteriaStr);
			} catch (NumberFormatException ex) {
	            final String msg = LDAPPlugin.Util.getString("LDAPConnection.maxCriteriaParseError"); //$NON-NLS-1$
	            throw new ConnectorException(msg);
			}
		} else {
            final String msg = LDAPPlugin.Util.getString("LDAPConnection.maxCriteriaPropNotFound"); //$NON-NLS-1$
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
		connenv.put(Context.INITIAL_CONTEXT_FACTORY, LDAPConnectorConstants.JNDI_LDAP_CTX_FACTORY);
		connenv.put(Context.PROVIDER_URL, this.ldapURL);
		connenv.put(Context.REFERRAL, LDAP_REFERRAL_MODE);
		// If username is blank, we will perform an anonymous bind.
		// Note: This is not supported when using Sun's VLVs, so remove this if VLVs are used.
		if(!ldapAdminUserDN.equals("")) { //$NON-NLS-1$

			connenv.put(Context.SECURITY_AUTHENTICATION, LDAP_AUTH_TYPE);
			connenv.put(Context.SECURITY_PRINCIPAL, this.ldapAdminUserDN);
			connenv.put(Context.SECURITY_CREDENTIALS, this.ldapAdminUserPass);
		} else {
			logger.logWarning("LDAP Username DN was blank; performing anonymous bind."); //$NON-NLS-1$
			connenv.put(Context.SECURITY_AUTHENTICATION, "none"); //$NON-NLS-1$
		}
		
		if(ldapTxnTimeoutInMillis != null && !ldapTxnTimeoutInMillis.equals("")) { //$NON-NLS-1$
			connenv.put("com.sun.jndi.ldap.connect.timeout", ldapTxnTimeoutInMillis); //$NON-NLS-1$
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
		logger.logDetail("Successfully obtained initial LDAP context."); //$NON-NLS-1$
		return initContext;
	}
	
	/** 
	 * Create an execution object. 
	 * 
	 * Sun- and AD-specific implementations may extend this method and
	 * create their own execution classes, which may make use of advanced features
	 * that are specific to the platform, such as paging.
	 * @see com.metamatrix.data.api.Connection#createExecution(int, com.metamatrix.data.api.ExecutionContext, com.metamatrix.data.metadata.runtime.RuntimeMetadata)
	 * @param executionMode the execution mode
	 * @param ctx the Execution Context
	 * @param rm the RuntimeMetadata
	 * @return Execution object
	 */
	public Execution createExecution(int executionMode, ExecutionContext ctx,RuntimeMetadata rm) throws ConnectorException {
		switch(executionMode) { 
			case ConnectorCapabilities.EXECUTION_MODE.SYNCH_QUERY:
			{
				return new LDAPSyncQueryExecution(executionMode, ctx, rm, this.logger, this.initCtx, this.props);
			}
			case ConnectorCapabilities.EXECUTION_MODE.UPDATE:
			{
				return new LDAPUpdateExecution(executionMode, ctx, rm, this.logger, this.initCtx);
			}
			default:
			{
	            final String msg = LDAPPlugin.Util.getString("LDAPConnection.unsupportedExecMode"); //$NON-NLS-1$
				throw new ConnectorException(msg);
			}
		}
	}

	/** 
	 * Get the LDAP Connector capabilites
	 * @return ConnectorCapabilities
	 */
	public ConnectorCapabilities getCapabilities() {
		return myCaps;
	}

	/** 
	 * Get the LDAP Connector metadata, returns null.
	 * @return ConnectorMetadata
	 */
	public ConnectorMetadata getMetadata() {
		return null;
	}
	
	/** 
	 * Hold onto the connection pool for future use, so the connection can release itself from the pool when 
	 * asked to do so.
	 */
    public void setConnectionPool(ConnectionPool pool){
        this.pool = pool;
    }
    
    /** 
     * Releases a connection to the pool, without closing it.
     * (non-Javadoc)
     * @see com.metamatrix.data.api.Connection#release()
     */
	
    public void release() {
		logger.logDetail("LDAP Connection is releasing itself to the pool."); //$NON-NLS-1$
		pool.release(this);
	}
	
	/** 
	 * Closes LDAP context, effectively closing the connection to LDAP.
	 * (non-Javadoc)
	 * @see com.metamatrix.data.pool.SourceConnection#closeSource()
	 */
	public void closeSource() throws ConnectorException {
		//logger.logTrace("Attempting to close LDAP context connection.");
		if(initCtx != null) {
			try {
				initCtx.close();
			} catch(NamingException e) {
	            final String msg = LDAPPlugin.Util.getString("LDAPConnection.contextCloseError",e.getExplanation()); //$NON-NLS-1$
				throw new ConnectorException(msg); 
			}
		}
		logger.logDetail("LDAP context has been closed."); //$NON-NLS-1$
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
	 * @see com.metamatrix.data.pool.SourceConnection#isAlive()
	 */
	public boolean isAlive() {
		logger.logTrace("LDAP Connection is alive."); //$NON-NLS-1$
		return true;
	}

	/** 
	 * Currently, we assume that all connections have not failed and are alive.
	 * A connection will automatically fail and throw an error during execution time if there's a problem.
	 * (non-Javadoc)
	 * @see com.metamatrix.data.pool.SourceConnection#isFailed()
	 */
	public boolean isFailed() {
		return false;
	}
}
