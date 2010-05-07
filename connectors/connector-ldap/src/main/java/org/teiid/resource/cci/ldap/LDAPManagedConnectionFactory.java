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
package org.teiid.resource.cci.ldap;

import javax.resource.ResourceException;
import javax.resource.cci.Connection;

import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.resource.spi.BasicManagedConnectionFactory;

public class LDAPManagedConnectionFactory extends BasicManagedConnectionFactory {
	
	private static final long serialVersionUID = -1832915223199053471L;

	private String ldapAdminUserDN;
	private String ldapAdminUserPassword;
	private String ldapUrl;
	private long ldapTxnTimeoutInMillis;
	private String ldapContextFactory;
	
	
	@Override
	public Object createConnectionFactory() throws ResourceException {
		return new BasicConnectionFactory() {
			@Override
			public Connection getConnection() throws ResourceException {
				return new LDAPConnectionImpl(LDAPManagedConnectionFactory.this);
			}
		};
	}	
	
	public String getLdapAdminUserDN() {
		return ldapAdminUserDN;
	}
	
	public void setLdapAdminUserDN(String ldapAdminUserDN) {
		this.ldapAdminUserDN = ldapAdminUserDN;
	}
	
	public String getLdapAdminUserPassword() {
		return ldapAdminUserPassword;
	}
	
	public void setLdapAdminUserPassword(String ldapAdminUserPassword) {
		this.ldapAdminUserPassword = ldapAdminUserPassword;
	}
	
	public long getLdapTxnTimeoutInMillis() {
		return ldapTxnTimeoutInMillis;
	}
	
	public void setLdapTxnTimeoutInMillis(Long ldapTxnTimeoutInMillis) {
		this.ldapTxnTimeoutInMillis = ldapTxnTimeoutInMillis.longValue();
	}
	
	public String getLdapUrl() {
		return ldapUrl;
	}
	
	public void setLdapUrl(String ldapUrl) {
		this.ldapUrl = ldapUrl;
	}
	
	public String getLdapContextFactory() {
		return ldapContextFactory;
	}

	public void setLdapContextFactory(String ldapContextFactory) {
		this.ldapContextFactory = ldapContextFactory;
	}	
}
