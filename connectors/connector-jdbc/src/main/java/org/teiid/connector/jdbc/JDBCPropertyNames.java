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

package org.teiid.connector.jdbc;

public class JDBCPropertyNames {
    public static final String CONNECTION_SOURCE_CLASS = "ConnectionSource"; //$NON-NLS-1$
    public static final String URL = "URL"; //$NON-NLS-1$
    public static final String USERNAME = "User"; //$NON-NLS-1$
    public static final String PASSWORD = "Password"; //$NON-NLS-1$

    /**
    * This is the property name of the ConnectorService property that defines
    * whether or not String type values in Criteria statements in SQL queries to
    * the data source should have spaces trimmed from them.
    */
    public static final String TRIM_STRINGS = "TrimStrings"; //$NON-NLS-1$

    /**
     * This is the property name used to set the transaction isolation level on
     * a connector's data source.  The value string must be the the same as one of the
     * names of the values of the transaction isolation levels defined in
     * <code>java.sql.Connection</code>.
     * @see java.sql.Connection#setTransactionIsolation(int)
     */
    public static final String TRANSACTION_ISOLATION_LEVEL = "TransactionIsolationLevel"; //$NON-NLS-1$

    /**
     * This is the property name of the ConnectorService property that defines
     * the time zone of the source database.  This property should only be used in 
     * cases where the source database is in a different time zone than the 
     * ConnectorService VM and the database/driver is not already handling 
     * time zones correctly.
     */
    public static final String DATABASE_TIME_ZONE = "DatabaseTimeZone"; //$NON-NLS-1$

     //***** Extension properties *****//
    /**
     * This property is used to specify the implementation of
     * com.metamatrix.data.ConnectorCapabilities. 
     */
    public static final String EXT_CAPABILITY_CLASS= "ExtensionCapabilityClass"; //$NON-NLS-1$

    /**
     * This property is used to specify the implementation of
     * com.metamatrix.connector.jdbc.extension.Translator
     */
    public static final String EXT_TRANSLATOR_CLASS= "ExtensionTranslationClass"; //$NON-NLS-1$

    /**
     * This property can be used to specify the fetch size used from the connector to
     * its underlying source.
     */
    public static final String FETCH_SIZE = "FetchSize"; //$NON-NLS-1$

    /**
     * This property can be used to indicate that prepared statements should be used.
     * This means Literals will be substituted for
     * bind variables.
     * @since 5.0.1 
     */
    public static final String USE_BIND_VARIABLES = "UseBindVariables";    //$NON-NLS-1$
    
    /**
     * This property is used to turn on/off the use of the default comments like
     * connection id and requestid in the source SQL query.
     */
    public static final String USE_COMMENTS_SOURCE_QUERY= "UseCommentsInSourceQuery";    //$NON-NLS-1$    
    
    public static final String CONNECTION_TEST_QUERY = "ConnectionTestQuery"; //$NON-NLS-1$
    
    public static final String IS_VALID_TIMEOUT = "IsValidTimeout"; //$NON-NLS-1$

}
