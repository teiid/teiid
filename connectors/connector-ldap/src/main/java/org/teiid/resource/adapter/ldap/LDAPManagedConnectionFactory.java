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

import javax.resource.ResourceException;

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
	@SuppressWarnings("serial")
	public BasicConnectionFactory<LDAPConnectionImpl> createConnectionFactory() throws ResourceException {
		return new BasicConnectionFactory<LDAPConnectionImpl>() {
			@Override
			public LDAPConnectionImpl getConnection() throws ResourceException {
				ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
				try {
					Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
					return new LDAPConnectionImpl(LDAPManagedConnectionFactory.this);
				} 
				finally {
					Thread.currentThread().setContextClassLoader(contextClassLoader);
				}
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
	
	public Long getLdapTxnTimeoutInMillis() {
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

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((ldapAdminUserDN == null) ? 0 : ldapAdminUserDN.hashCode());
		result = prime * result	+ ((ldapAdminUserPassword == null) ? 0 : ldapAdminUserPassword.hashCode());
		result = prime * result	+ ((ldapContextFactory == null) ? 0 : ldapContextFactory.hashCode());
		result = prime * result	+ (int) (ldapTxnTimeoutInMillis ^ (ldapTxnTimeoutInMillis >>> 32));
		result = prime * result + ((ldapUrl == null) ? 0 : ldapUrl.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		LDAPManagedConnectionFactory other = (LDAPManagedConnectionFactory) obj;
		if (!checkEquals(this.ldapAdminUserDN, other.ldapAdminUserDN)) {
			return false;
		}
		if (!checkEquals(this.ldapAdminUserPassword, other.ldapAdminUserPassword)) {
			return false;
		}
		if (!checkEquals(this.ldapContextFactory, other.ldapContextFactory)) {
			return false;
		}
		if (!checkEquals(this.ldapTxnTimeoutInMillis, other.ldapTxnTimeoutInMillis)) {
			return false;
		}
		if (!checkEquals(this.ldapUrl, other.ldapUrl)) {
			return false;
		}
		return true;
	}	
	
}
