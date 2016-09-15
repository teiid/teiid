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
package org.teiid.translator.ldap;

import javax.naming.directory.*;

/** 
 * Utility class to maintain list of constants for the LDAPConnector. 
 * Please modify constants here; changes should be reflected throughout
 * the connector code. 
 */
public class LDAPConnectorConstants {

	public static final String ldapDefaultSortName = "guid"; //$NON-NLS-1$
	public static final int ldapDefaultSearchScope = SearchControls.ONELEVEL_SCOPE;
	public static final boolean ldapDefaultIsAscending = true;
		
	public static final String ldapTimestampFormat = "yyyyMMddhhmmss\'Z\'"; //$NON-NLS-1$
}
