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

/*
 * Date: Jan 23, 2003
 * Time: 5:38:40 PM
 */
package com.metamatrix.connector.jdbc.xa;

/**
 * XAJDBCPropertyNames.
 */
public class XAJDBCPropertyNames {

    /**
     * The environment property used to identify a routing ID.  This value should
     * be used to locate this particular connector from the DQP.
     * @since 4.0
     */
    public static final String CONNECTOR_ID = "ConnectorID"; //$NON-NLS-1$

    /** An XA Transaction-related property. The <code>XADataSource</code>'s unique
     * resource name for the underlying <code>ConnectionPoolDataSource</code> object.
     */
    public static final String DATASOURCE_NAME = "dataSourceName"; //$NON-NLS-1$

    public static final String DATABASE_NAME = "databaseName";//$NON-NLS-1$

    public static final String DESCRIPTION = "description";//$NON-NLS-1$

    public static final String NETWORK_PROTOCOL = "networkProtocol";//$NON-NLS-1$

    public static final String PASSWORD = "password";//$NON-NLS-1$

    public static final String PORT_NUMBER = "portNumber";//$NON-NLS-1$

    public static final String ROLE_NAME = "roleName";//$NON-NLS-1$

    public static final String SERVER_NAME = "serverName";//$NON-NLS-1$

    public static final String USER = "user";//$NON-NLS-1$

    /** <i>Not</i> a standard DataSource property - only for debugging */
    public static final String SPYING = "spyAttributes";//$NON-NLS-1$

    /**
     * Oracle-specific properties
     */
    public static class Oracle {
        public static final String SID = "sid";//$NON-NLS-1$
    }

    /**
     * DB2-specific properties
     */
    public static class DB2 {
        public static final String COLLECTIONID = "CollectionID";//$NON-NLS-1$
        public static final String PACKAGENAME = "PackageName";//$NON-NLS-1$
    }

    public static final String IS_XA = "isXA";//$NON-NLS-1$
    
    /**
     * If true, the XAConnection obtained from the pool will not be 
     * returned until the transaction is finished. 
     */
    public static final String USE_CONNECTION_EXCLUSIVE = "UseConnectionExclusive"; //$NON-NLS-1$


}
