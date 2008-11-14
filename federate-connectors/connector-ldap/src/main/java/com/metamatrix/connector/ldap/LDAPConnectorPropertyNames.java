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

/**
 * Property names used in the LDAP connector.
 */
public class LDAPConnectorPropertyNames {
    public static final String LDAP_URL = "LdapUrl"; //$NON-NLS-1$
    public static final String LDAP_ADMIN_USER_DN = "LdapAdminUserDN"; //$NON-NLS-1$
    public static final String LDAP_ADMIN_USER_PASSWORD = "LdapAdminUserPassword"; //$NON-NLS-1$
    public static final String LDAP_TXN_TIMEOUT_IN_MILLIS = "LdapTxnTimeoutInMillis"; //$NON-NLS-1$
    public static final String LDAP_MAX_CRITERIA = "LdapMaxCriteria"; //$NON-NLS-1$
    public static final String LDAP_DEFAULT_BASEDN = "SearchDefaultBaseDN"; //$NON-NLS-1$
    public static final String LDAP_DEFAULT_SCOPE = "SearchDefaultScope"; //$NON-NLS-1$
    public static final String LDAP_RESTRICT_TO_OBJECTCLASS = "RestrictToObjectClass"; //$NON-NLS-1$
}
