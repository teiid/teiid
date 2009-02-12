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

package com.metamatrix.platform.security.api.service;

public class AuthorizationServicePropertyNames {

    /**
     * The environment property the determines whether data access authorization will
     * be performed.
     * <br> This property defaults to "false" - no entitlement checking.</br>
     */
    public static final String DATA_ACCESS_AUTHORIZATION_ENABLED = "metamatrix.authorization.dataaccess.CheckingEnabled"; //$NON-NLS-1$

    /**
     * The environment property the determines whether MetaBase authorization will
     * be performed.
     * <br> This property defaults to "false" - no entitlement checking.</br>
     */
    public static final String METABASE_AUTHORIZATION_ENABLED = "metamatrix.authorization.metabase.CheckingEnabled"; //$NON-NLS-1$

    /**
     * The environment property name for the class that is to be used for the ManagedConnectionFactory implementation.
     * This property is required (there is no default).
     */
    public static final String CONNECTION_FACTORY = "security.authorization.connection.Factory"; //$NON-NLS-1$

    /**
     * The environment property name for the number of times a method should be retried
     * with a new connection should a connection exception occur.
     * This property is optional.
     */
    public static final String CONNECTION_RETRIES = "security.authorization.connection.Retries"; //$NON-NLS-1$

    /**
     * The environment property name for the class of the driver.
     * This property is optional.
     */
    public static final String CONNECTION_DRIVER = "security.authorization.connection.Driver"; //$NON-NLS-1$

    /**
     * The environment property name for the protocol for connecting to the authorization store.
     * This property is optional.
     */
    public static final String CONNECTION_PROTOCOL = "security.authorization.connection.Protocol"; //$NON-NLS-1$

    /**
     * The environment property name for the name of the authorization store database.
     * This property is optional.
     */
    public static final String CONNECTION_DATABASE = "security.authorization.connection.Database"; //$NON-NLS-1$

    /**
     * The environment property name for the username that is to be used for connecting to the authorization store.
     * This property is optional.
     */
    public static final String CONNECTION_PRINCIPAL = "security.authorization.connection.Principal"; //$NON-NLS-1$

    /**
     * The environment property name for the password that is to be used for connecting to the authorization store.
     * This property is optional.
     */
    public static final String CONNECTION_PASSWORD = "security.authorization.connection.Password"; //$NON-NLS-1$

    /**
     * The environment property name for the maximum number of milliseconds that a authorization connection
     * may remain unused before it becomes a candidate for garbage collection.
     * This property is optional.
     */
    public static final String CONNECTION_POOL_MAXIMUM_AGE = "security.authorization.connection.MaximumAge"; //$NON-NLS-1$

    /**
     * The environment property name for the maximum number of concurrent users of a single authorization connection.
     * This property is optional.
     */
    public static final String CONNECTION_POOL_MAXIMUM_CONCURRENT_USERS = "security.authorization.connection.MaximumConcurrentReaders"; //$NON-NLS-1$

    /**
     * The default authorization factory class when no class is specified.
     */
	public static final String DEFAULT_FACTORY_CLASS="com.metamatrix.platform.security.authorization.spi.jdbc.JDBCAuthorizationTransactionFactory";

}

