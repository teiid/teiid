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

package com.metamatrix.connector.xmlsource.soap;



/** 
 * This properties class which returns the properties defined in the 
 * connector environment. 
 */
public class SoapConnectorProperties {
    public static final String END_POINT = "EndPoint"; //$NON-NLS-1$
    public static final String WSDL = "wsdl"; //$NON-NLS-1$
    public static final String PORT_NAME = "PortName"; //$NON-NLS-1$
    public static final String QUERY_TIMEOUT = "QueryTimeout"; //$NON-NLS-1$    
    public static final String AUTHORIZATION_TYPE = "SecurityType"; //$NON-NLS-1$
    public static final String WS_SECURITY_TYPE = "WSSecurityType"; //$NON-NLS-1$
    public static final String USERNAME = "AuthUserName"; //$NON-NLS-1$
    public static final String PASSWORD = "AuthPassword"; //$NON-NLS-1$
    public static final String SIGNATURE_PROPERTY_FILE = "CryptoPropertyFile"; //$NON-NLS-1$
    public static final String TRUST_TYPE = "TrustType"; //$NON-NLS-1$
    public static final String SAML_PROPERTY_FILE = "SAMLPropertyFile"; //$NON-NLS-1$
    public static final String ENCRYPTION_PROPERTY_FILE = "EncryptPropertyFile"; //$NON-NLS-1$
    public static final String ENCRYPTION_USER= "EncryptUserName"; //$NON-NLS-1$
    
}
