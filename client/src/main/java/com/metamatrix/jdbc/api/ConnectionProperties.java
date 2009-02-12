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

package com.metamatrix.jdbc.api;

import com.metamatrix.common.api.MMURL;


/**
 * MetaMatrix-specific connection properties.  These connection properties can 
 * be set via the java.sql.DriverManager.getConnection(jdbcUrl, connectionProps)
 * method.
 *    
 * WARNING: This class does not contain all possible properties.
 */
public interface ConnectionProperties {

    // constant defined for connection properties indicating the value is a Serializable token
    // that may be used in the connector for this client's requests
    
    /** 
     * Connection property name for trusted session payload.
     * The <i>optional</i> client token that will be passed directly through to connectors,
     * which may use it and/or pass it down to their underlying data source.
     * <p>
     * The form and type of the client payload object is up to the client but it <i>must</i>
     * implement the <code>Serializable</code> interface.  MetaMatrix does nothing with this 
     * object except to make it available for authentication/augmentation/replacement upon 
     * authentication to the system and to connectors that may require it at the data source 
     * level.</p> 
     */
    public static final String PROP_CLIENT_SESSION_PAYLOAD = MMURL.CONNECTION.CLIENT_TOKEN_PROP;

    /**
     * <p>Data source credential sets.  The credentials will be decoded and passed in a 
     * CredentialMap object as the session payload.  It is an error to use this property
     * in conjunction with {@link #PROP_CLIENT_SESSION_PAYLOAD} as the CredentialMap
     * is used as the payload.  If a per-user connection factory is used with the 
     * ConnectionPool in the Connector API, the CredentialMap can be used to obtain
     * per-system credentials and use per-user connection pooling.  In particular, the 
     * MetaMatrix JDBC Connectors have pre-built connection factories that can be 
     * used with credentials for this purpose. </p>
     * 
     * <p>Credentials take the following basic form: <code>credentials=(system=sys1,user=u1,password=p1/
     * system=sys2,user=u2,password=p2)</code>.  Each set of system credentials <b>must</b>
     * contain a system property.  The properties "user" and "password" are also well-known
     * property names used by the connection factory although any property name is allowed
     * and may be used by a connector to extract credentials.</>
     * 
     * <p>Additionally, the credentials property allows an additional attribute before the 
     * credentials list: <code>defaultToLogon</code> as follows:  <code>credentials=defaultToLogon</code
     * or <code>credentials=defaultToLogon,(system=sys1,user=u1,password=p2)</code>.  When this attribute
     * is used, the user's logon credentials are used as defaults when a connector asks for 
     * credentials from the CredentialMap.
     * </p>
     */
    public static final String PROP_CREDENTIALS = MMURL.JDBC.CREDENTIALS;
    
    public static final String DEFAULT_TO_LOGON = "defaultToLogon"; //$NON-NLS-1$

}
