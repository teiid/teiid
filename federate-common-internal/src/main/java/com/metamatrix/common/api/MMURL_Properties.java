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

package com.metamatrix.common.api;


/**
 * The following are the properties for a MetaMatrix JDBC Url:
 * <table cellspacing="0" cellpadding="0" border="1" width="100%">
 *   <tr><td><b>Property Name</b></td><td><b>Type</b></td><td><b>Description</b></td></tr>
 *   <tr><td>applicationName  </td><td><code>String</code></td><td>The <i>optional</i> name of the application using the DataSource.</td></tr>
 *   <tr><td>applicationServer</td><td><code>String</code></td><td>The <i>optional</i> type of application server in which the MetaMatrix Server
 *                                                                 is running; possible values are <code>weblogic</code>,
 *                                                                 <code>jboss</code>, <code>websphere</code>, <code>iiop</code>
 *                                                                 <code>sap_oem</code>.  The value is used to determine the
 *                                                                 {@link javax.naming.spi.InitialContextFactory} implementation class;
 *                                                                 if not supplied, then the first implementation of the
 *                                                                 {@link javax.naming.spi.InitialContextFactory} found on the classpath
 *                                                                 is used.</td></tr>
 *   <tr><td>clientToken      </td><td><code>Serializable</code></td><td>The <i>optional</i> client token that will be passed directly
 *                                                                 through to the connectors, which may use it and/or pass it
 *                                                                 down to their underlying data source.
 *                                                                 <p>
 *                                                                 The form and type of the client token is up to the client but it <i>must</i> implement the
 *                                                                 <code>Serializable</code> interface.  MetaMatrix does nothing with this token except to make it
 *                                                                 available for authentication/augmentation/replacement upon authentication to the system and to
 *                                                                 connectors that may require it at the data source level.
 *                                                                 </p></td></tr>
 *   <tr><td>databaseName     </td><td><code>String</code></td><td>The name of a particular virtual database on a
 *                                                                 MetaMatrix Server.</td></tr>
 *   <tr><td>databaseVersion  </td><td><code>String</code></td><td>The <i>optional</i> version of a particular
 *                                                                 virtual database on a MetaMatrix Server;
 *                                                                 if not supplied, then the latest version is assumed.</td></tr>
 *   <tr><td>dataSourceName   </td><td><code>String</code></td><td>The <i>optional</i> logical name for the underlying
 *                                                                 <code>XADataSource</code> or <code>ConnectionPoolDataSource</code>;
 *                                                                 used only when pooling connections or distributed transactions
 *                                                                 are implemented.</td></tr>
 *   <tr><td>description      </td><td><code>String</code></td><td>The <i>optional</i> description of this data source.</td></tr>
 *   <tr><td>logFile          </td><td><code>String</code></td><td>The <i>optional</i> path and file name to which JDBC Log Statements
 *                                                                 will be written; if none is specified, then no
 *                                                                 Log Statements will be written.</td></tr>
 *   <tr><td>logLevel         </td><td><code>int   </code></td><td>The <i>optional</i> level for logging, which only applies
 *                                                                 if the <code>logFile</code> property is set.  Value must be
 *                                                                 one of the following:
 *                                                                 <ul>
 *                                                                    <li>"<code>0</code>" - no JDBC log messages will be written to the file;
 *                                                                         this is the default</li>
 *                                                                    <li>"<code>1</code>" - all JDBC log messages will be written to the file</li>
 *                                                                    <li>"<code>2</code>" - all JDBC log messages as well as stack traces
 *                                                                         of any exceptions thrown from this driver will be written
 *                                                                         to the file</li>
 *                                                                 </ul>
 *   <tr><td>password         </td><td><code>String</code></td><td>The user's password.</td></tr>
 *   <tr><td>user             </td><td><code>String</code></td><td>The user name to use for the connection.</td></tr>
 *   <tr><td>partialResultsMode      </td><td><code>boolean</code></td><td>Support partial results mode or not. </td></tr>
 *   <tr><td>fetchSize      </td><td><code>int</code></td><td>Set default fetch size for statements, default=500.</td></tr>
 *   <tr><td>showPlan       </td><td><code>boolean</code></td><td>Set whether the server should return plan annotations for each SQL command. default=false</td></tr>
 * <table>
 * </p>
 */
public interface MMURL_Properties {
    /**
     * NOTE: These properties are also maintained in {@link  com.metamatrix.dqp.util.ConnectionProperties}  
     * @since 4.3
     */

    public interface JDBC {
        // constant indicating Virtual database name
        public static final String VDB_NAME = "VirtualDatabaseName"; //$NON-NLS-1$
        // constant indicating Virtual database version
        public static final String VDB_VERSION = "VirtualDatabaseVersion"; //$NON-NLS-1$
        // constant for vdb version part of serverURL
        public static final String VERSION = "version"; //$NON-NLS-1$
        // name of the application which is obtaining connection
        public static final String APP_NAME = "ApplicationName"; //$NON-NLS-1$
        // constant for username part of url
        public static final String USER_NAME = "user"; //$NON-NLS-1$
        // constant for password part of url
        public static final String PASSWORD = "password"; //$NON-NLS-1$
        // string constant that the url contains
        public static final String LOG_FILE = "logFile"; //$NON-NLS-1$
        // string constant that the url contains
        public static final String LOG_LEVEL = "logLevel"; //$NON-NLS-1$
        
        public static final String TRUSTED_PAYLOAD_PROP = "trustedPayload"; //$NON-NLS-1$
        
        public static final String CREDENTIALS = "credentials"; //$NON-NLS-1$

    
        // logging level that would log messages
        public static final int LOG_NONE = 0;
        // logging level that would log error messages
        public static final int LOG_ERROR = 1;
        // logging level that would log all info level messages
        public static final int LOG_INFO = 2;
        // logging level that would traces method calls
        public static final int LOG_TRACE = 3;
        
        public static final String CLIENT_TOKEN_PROP = "clientToken"; //$NON-NLS-1$ 
    }
    
    public interface CONNECTION {
    	public static final String PRODUCT_NAME = "productName"; //$NON-NLS-1$
		public static final String CLIENT_IP_ADDRESS = "clientIpAddress"; //$NON-NLS-1$
		public static final String CLIENT_HOSTNAME = "clientHostName"; //$NON-NLS-1$
		/**
		 * If true, will automatically select a new server instance after a communication exception.
		 * @since 5.6
		 */
		public static final String AUTO_FAILOVER = "autoFailover";  //$NON-NLS-1$
    }
    
    public interface SERVER {
        public static final String SERVER_URL = "serverURL"; //$NON-NLS-1$
        
        /**
         * Non-secure MetaMatrix Protocol.
         */        
        public static final String NON_SECURE_PROTOCOL = "mm"; //$NON-NLS-1$

        /**
         * Non-secure MetaMatrix Protocol.
         */
        public static final String SECURE_PROTOCOL = "mms"; //$NON-NLS-1$

    }
}

