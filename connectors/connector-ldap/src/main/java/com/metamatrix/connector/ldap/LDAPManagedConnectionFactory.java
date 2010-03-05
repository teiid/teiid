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

import org.teiid.connector.basic.BasicManagedConnectionFactory;

public class LDAPManagedConnectionFactory extends BasicManagedConnectionFactory {
	
	private static final long serialVersionUID = -1832915223199053471L;

	private String searchDefaultBaseDN;
	private String ldapAdminUserDN;
	private String ldapAdminUserPassword;
	private boolean restrictToObjectClass = false;
	private long ldapTxnTimeoutInMillis;
	private String ldapUrl;
	private String searchDefaultScope = "SUBTREE_SCOPE";
	
	public String getSearchDefaultBaseDN() {
		return searchDefaultBaseDN;
	}
	
	public void setSearchDefaultBaseDN(String searchDefaultBaseDN) {
		this.searchDefaultBaseDN = searchDefaultBaseDN;
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
	
	public boolean isRestrictToObjectClass() {
		return restrictToObjectClass;
	}
	
	public void setRestrictToObjectClass(Boolean restrictToObjectClass) {
		this.restrictToObjectClass = restrictToObjectClass.booleanValue();
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
	
	public String getSearchDefaultScope() {
		return searchDefaultScope;
	}
	
	public void setSearchDefaultScope(String searchDefaultScope) {
		this.searchDefaultScope = searchDefaultScope;
	}
}
