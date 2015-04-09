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

package org.teiid.resource.adapter.ldap;

import java.util.Hashtable;

import javax.naming.*;
import javax.naming.directory.*;
import javax.naming.ldap.*;
import javax.resource.ResourceException;
import javax.security.auth.Subject;

import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.resource.spi.BasicConnection;
import org.teiid.resource.spi.ConnectionContext;



/** 
 * Represents a connection to an LDAP data source. 
 */
public class LDAPConnectionImpl extends BasicConnection implements LdapContext  {  

	private LDAPManagedConnectionFactory config;
	
	// Standard Connection data members
	private InitialLdapContext initCtx;

	// LDAP-specific properties
    public static final String LDAP_AUTH_TYPE = "simple"; //$NON-NLS-1$
    public static final String LDAP_USER_OBJECT_TYPE = "person"; //$NON-NLS-1$
    public static final String LDAP_REFERRAL_MODE = "follow"; //$NON-NLS-1$

	public LDAPConnectionImpl(LDAPManagedConnectionFactory config) throws ResourceException {
		this.config = config;
			
		checkProperties();

		// Create initial LDAP connection.
		this.initCtx = initializeLDAPContext();
		LogManager.logDetail(LogConstants.CTX_CONNECTOR, "LDAP Connection has been newly created."); //$NON-NLS-1$
	}
	
    /**
     * Helper method to retrieve the LDAP Connector properties.  If any properties are in error,
     * a ConnectorException is thrown.
     * @param props
     */
	private void checkProperties() throws ResourceException {
		// LDAP URL 
		if(this.config.getLdapUrl() == null) {
            final String msg = LDAPPlugin.Util.getString("LDAPConnection.urlPropNotFound"); //$NON-NLS-1$
            throw new ResourceException(msg);
		}
	}
	
	/**
	 * Setup a standard initial LDAP context using JNDI's context factory.
	 * This method may be extended to support Sun-specific and AD-specific
	 * contexts, in order to support the different paging implementations they provide.
	 * @return the initial LDAP Context
	 */
	private InitialLdapContext initializeLDAPContext() throws ResourceException {
	  	// Create the root context.
		InitialLdapContext initContext;

		Hashtable connenv = new Hashtable();
		connenv.put(Context.INITIAL_CONTEXT_FACTORY, this.config.getLdapContextFactory());
		connenv.put(Context.PROVIDER_URL, this.config.getLdapUrl());
		connenv.put(Context.REFERRAL, LDAP_REFERRAL_MODE);
		
		String userName = this.config.getLdapAdminUserDN();
		String password = this.config.getLdapAdminUserPassword();

		// if security-domain is specified and caller identity is used; then use
		// credentials from subject
		Subject subject = ConnectionContext.getSubject();
		if (subject != null) {
			userName = ConnectionContext.getUserName(subject, this.config, userName);
			password = ConnectionContext.getPassword(subject, this.config, userName, password);
		}
		
		if (userName == null) {
            final String msg = LDAPPlugin.Util.getString("LDAPConnection.adminUserDNPropNotFound"); //$NON-NLS-1$
            throw new ResourceException(msg);
		}
		
		if (password == null) {
            final String msg = LDAPPlugin.Util.getString("LDAPConnection.adminUserPassPropNotFound"); //$NON-NLS-1$
            throw new ResourceException(msg);
		}
		
		// If username is blank, we will perform an anonymous bind.
		// Note: This is not supported when using Sun's VLVs, so remove this if VLVs are used.
		if(!userName.equals("")) { //$NON-NLS-1$
			connenv.put(Context.SECURITY_AUTHENTICATION, LDAP_AUTH_TYPE);
			connenv.put(Context.SECURITY_PRINCIPAL, userName);
			connenv.put(Context.SECURITY_CREDENTIALS, password);
		} else {
			LogManager.logDetail(LogConstants.CTX_CONNECTOR, "LDAP Username DN was blank; performing anonymous bind."); //$NON-NLS-1$
			connenv.put(Context.SECURITY_AUTHENTICATION, "none"); //$NON-NLS-1$
		}
		
		if(this.config.getLdapTxnTimeoutInMillis() != null && this.config.getLdapTxnTimeoutInMillis() != -1) { 
			connenv.put("com.sun.jndi.ldap.connect.timeout", this.config.getLdapTxnTimeoutInMillis()); //$NON-NLS-1$
		}
		
		// Enable connection pooling for the Initial context.
		connenv.put("com.sun.jndi.ldap.connect.pool", "true"); //$NON-NLS-1$ //$NON-NLS-2$
		connenv.put("com.sun.jndi.ldap.connect.pool.debug", "fine"); //$NON-NLS-1$ //$NON-NLS-2$
		
		try {
			initContext = new InitialLdapContext(connenv, null);
		} catch(NamingException ne){ 
            final String msg = LDAPPlugin.Util.getString("LDAPConnection.directoryNamingError",ne.getExplanation()); //$NON-NLS-1$
			throw new ResourceException(msg, ne);
		} 
		LogManager.logDetail(LogConstants.CTX_CONNECTOR, "Successfully obtained initial LDAP context."); //$NON-NLS-1$
		return initContext;
	}
	

	
	/** 
	 * Closes LDAP context, effectively closing the connection to LDAP.
	 * (non-Javadoc)
	 */
	@Override
    public void close() {
		if(initCtx != null) {
			try {
				initCtx.close();
			} catch(NamingException e) {
				LogManager.logDetail(LogConstants.CTX_CONNECTOR, LDAPPlugin.Util.getString("LDAPConnection.contextCloseError",e.getExplanation())); //$NON-NLS-1$
			}
		}
		LogManager.logDetail(LogConstants.CTX_CONNECTOR,"LDAP context has been closed."); //$NON-NLS-1$
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
	 */
	public boolean isAlive() {
		LogManager.logTrace(LogConstants.CTX_CONNECTOR, "LDAP Connection is alive."); //$NON-NLS-1$
		return true;
	}

	public Object lookup(String context) throws NamingException {
		return this.initCtx.lookup(context);
	}

	public Object addToEnvironment(String propName, Object propVal)
			throws NamingException {
		return initCtx.addToEnvironment(propName, propVal);
	}

	public void bind(Name name, Object obj, Attributes attrs)
			throws NamingException {
		initCtx.bind(name, obj, attrs);
	}

	public void bind(Name name, Object obj) throws NamingException {
		initCtx.bind(name, obj);
	}

	public void bind(String name, Object obj, Attributes attrs)
			throws NamingException {
		initCtx.bind(name, obj, attrs);
	}

	public void bind(String name, Object obj) throws NamingException {
		initCtx.bind(name, obj);
	}

	public Name composeName(Name name, Name prefix) throws NamingException {
		return initCtx.composeName(name, prefix);
	}

	public String composeName(String name, String prefix)
			throws NamingException {
		return initCtx.composeName(name, prefix);
	}

	public DirContext createSubcontext(Name name, Attributes attrs)
			throws NamingException {
		return initCtx.createSubcontext(name, attrs);
	}

	public Context createSubcontext(Name name) throws NamingException {
		return initCtx.createSubcontext(name);
	}

	public DirContext createSubcontext(String name, Attributes attrs)
			throws NamingException {
		return initCtx.createSubcontext(name, attrs);
	}

	public Context createSubcontext(String name) throws NamingException {
		return initCtx.createSubcontext(name);
	}

	public void destroySubcontext(Name name) throws NamingException {
		initCtx.destroySubcontext(name);
	}

	public void destroySubcontext(String name) throws NamingException {
		initCtx.destroySubcontext(name);
	}

	public boolean equals(Object obj) {
		return initCtx.equals(obj);
	}

	public ExtendedResponse extendedOperation(ExtendedRequest request)
			throws NamingException {
		return initCtx.extendedOperation(request);
	}

	public Attributes getAttributes(Name name, String[] attrIds)
			throws NamingException {
		return initCtx.getAttributes(name, attrIds);
	}

	public Attributes getAttributes(Name name) throws NamingException {
		return initCtx.getAttributes(name);
	}

	public Attributes getAttributes(String name, String[] attrIds)
			throws NamingException {
		return initCtx.getAttributes(name, attrIds);
	}

	public Attributes getAttributes(String name) throws NamingException {
		return initCtx.getAttributes(name);
	}

	public Control[] getConnectControls() throws NamingException {
		return initCtx.getConnectControls();
	}

	public Hashtable<?, ?> getEnvironment() throws NamingException {
		return initCtx.getEnvironment();
	}

	public String getNameInNamespace() throws NamingException {
		return initCtx.getNameInNamespace();
	}

	public NameParser getNameParser(Name name) throws NamingException {
		return initCtx.getNameParser(name);
	}

	public NameParser getNameParser(String name) throws NamingException {
		return initCtx.getNameParser(name);
	}

	public Control[] getRequestControls() throws NamingException {
		return initCtx.getRequestControls();
	}

	public Control[] getResponseControls() throws NamingException {
		return initCtx.getResponseControls();
	}

	public DirContext getSchema(Name name) throws NamingException {
		return initCtx.getSchema(name);
	}

	public DirContext getSchema(String name) throws NamingException {
		return initCtx.getSchema(name);
	}

	public DirContext getSchemaClassDefinition(Name name)
			throws NamingException {
		return initCtx.getSchemaClassDefinition(name);
	}

	public DirContext getSchemaClassDefinition(String name)
			throws NamingException {
		return initCtx.getSchemaClassDefinition(name);
	}

	public int hashCode() {
		return initCtx.hashCode();
	}

	public NamingEnumeration<NameClassPair> list(Name name)
			throws NamingException {
		return initCtx.list(name);
	}

	public NamingEnumeration<NameClassPair> list(String name)
			throws NamingException {
		return initCtx.list(name);
	}

	public NamingEnumeration<Binding> listBindings(Name name)
			throws NamingException {
		return initCtx.listBindings(name);
	}

	public NamingEnumeration<Binding> listBindings(String name)
			throws NamingException {
		return initCtx.listBindings(name);
	}

	public Object lookup(Name name) throws NamingException {
		return initCtx.lookup(name);
	}

	public Object lookupLink(Name name) throws NamingException {
		return initCtx.lookupLink(name);
	}

	public Object lookupLink(String name) throws NamingException {
		return initCtx.lookupLink(name);
	}

	public void modifyAttributes(Name name, int modOp, Attributes attrs)
			throws NamingException {
		initCtx.modifyAttributes(name, modOp, attrs);
	}

	public void modifyAttributes(Name name, ModificationItem[] mods)
			throws NamingException {
		initCtx.modifyAttributes(name, mods);
	}

	public void modifyAttributes(String name, int modOp, Attributes attrs)
			throws NamingException {
		initCtx.modifyAttributes(name, modOp, attrs);
	}

	public void modifyAttributes(String name, ModificationItem[] mods)
			throws NamingException {
		initCtx.modifyAttributes(name, mods);
	}

	public LdapContext newInstance(Control[] reqCtls) throws NamingException {
		return initCtx.newInstance(reqCtls);
	}

	public void rebind(Name name, Object obj, Attributes attrs)
			throws NamingException {
		initCtx.rebind(name, obj, attrs);
	}

	public void rebind(Name name, Object obj) throws NamingException {
		initCtx.rebind(name, obj);
	}

	public void rebind(String name, Object obj, Attributes attrs)
			throws NamingException {
		initCtx.rebind(name, obj, attrs);
	}

	public void rebind(String name, Object obj) throws NamingException {
		initCtx.rebind(name, obj);
	}

	public void reconnect(Control[] connCtls) throws NamingException {
		initCtx.reconnect(connCtls);
	}

	public Object removeFromEnvironment(String propName) throws NamingException {
		return initCtx.removeFromEnvironment(propName);
	}

	public void rename(Name oldName, Name newName) throws NamingException {
		initCtx.rename(oldName, newName);
	}

	public void rename(String oldName, String newName) throws NamingException {
		initCtx.rename(oldName, newName);
	}

	public NamingEnumeration<SearchResult> search(Name name,
			Attributes matchingAttributes, String[] attributesToReturn)
			throws NamingException {
		return initCtx.search(name, matchingAttributes, attributesToReturn);
	}

	public NamingEnumeration<SearchResult> search(Name name,
			Attributes matchingAttributes) throws NamingException {
		return initCtx.search(name, matchingAttributes);
	}

	public NamingEnumeration<SearchResult> search(Name name, String filterExpr,
			Object[] filterArgs, SearchControls cons) throws NamingException {
		return initCtx.search(name, filterExpr, filterArgs, cons);
	}

	public NamingEnumeration<SearchResult> search(Name name, String filter,
			SearchControls cons) throws NamingException {
		return initCtx.search(name, filter, cons);
	}

	public NamingEnumeration<SearchResult> search(String name,
			Attributes matchingAttributes, String[] attributesToReturn)
			throws NamingException {
		return initCtx.search(name, matchingAttributes, attributesToReturn);
	}

	public NamingEnumeration<SearchResult> search(String name,
			Attributes matchingAttributes) throws NamingException {
		return initCtx.search(name, matchingAttributes);
	}

	public NamingEnumeration<SearchResult> search(String name,
			String filterExpr, Object[] filterArgs, SearchControls cons)
			throws NamingException {
		return initCtx.search(name, filterExpr, filterArgs, cons);
	}

	public NamingEnumeration<SearchResult> search(String name, String filter,
			SearchControls cons) throws NamingException {
		return initCtx.search(name, filter, cons);
	}

	public void setRequestControls(Control[] requestControls)
			throws NamingException {
		initCtx.setRequestControls(requestControls);
	}

	public void unbind(Name name) throws NamingException {
		initCtx.unbind(name);
	}

	public void unbind(String name) throws NamingException {
		initCtx.unbind(name);
	}
	
}
