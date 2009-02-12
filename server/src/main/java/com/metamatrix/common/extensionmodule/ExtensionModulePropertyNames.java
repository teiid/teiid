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

package com.metamatrix.common.extensionmodule;

/**
 * Property names defining the properties needed for the
 * {@link com.metamatrix.common.connection.ManagedConnection ManagedConnection}
 * of the
 * {@link ExtensionModuleManager ExtensionModuleManager}
 */
public class ExtensionModulePropertyNames {

    //===================================================================
    //MANAGED CONNECTION / CONNECTION POOL PROPERTIES
    //===================================================================

    /**
     * The environment property name for the class that is to be used for the
     * ManagedConnectionFactory implementation.  <i>Required.</i>
     * This property is required (there is no default).
     */
    public static final String CONNECTION_FACTORY = "metamatrix.extensionsource.connection.Factory"; //$NON-NLS-1$

    /**
     * The environment property name for the class of the driver.
     * This property is optional.
     */
    public static final String CONNECTION_DRIVER = "metamatrix.extensionsource.connection.Driver"; //$NON-NLS-1$

    /**
     * The environment property name for the protocol for connecting.
     * This property is optional.
     */
    public static final String CONNECTION_PROTOCOL = "metamatrix.extensionsource.connection.Protocol"; //$NON-NLS-1$

    /**
     * The environment property name for the name of the data source.
     * This property is optional.
     */
    public static final String CONNECTION_DATABASE = "metamatrix.extensionsource.connection.Database"; //$NON-NLS-1$

    /**
     * The environment property name for the username that is to be used for
     * connecting to the data source.
     * This property is optional.
     */
    public static final String CONNECTION_USERNAME = "metamatrix.extensionsource.connection.User"; //$NON-NLS-1$

    /**
     * The environment property name for the password that is to be used for
     * connecting to the data source.
     * This property is optional.
     */
    public static final String CONNECTION_PASSWORD = "metamatrix.extensionsource.connection.Password"; //$NON-NLS-1$

    /**
     * The environment property name for the maximum number of milliseconds
     * that a data source connection
     * may remain unused before it becomes a candidate for garbage collection.
     * This property is optional.
     */
    public static final String CONNECTION_POOL_MAXIMUM_AGE = "metamatrix.extensionsource.connection.MaximumAge"; //$NON-NLS-1$

    /**
     * The environment property name for the maximum number of concurrent users
     * of a single data source connection.
     * This property is optional.
     */
    public static final String CONNECTION_POOL_MAXIMUM_CONCURRENT_USERS = "metamatrix.extensionsource.connection.MaximumConcurrentReaders"; //$NON-NLS-1$

    /**
     * The default connection factory class to use when one is not specified.
     */
    public static final String DEFAULT_CONNECTION_FACTORY_CLASS="com.metamatrix.common.extensionmodule.spi.jdbc.JDBCExtensionModuleTransactionFactory";

    //===================================================================
    //MESSAGING PROPERTIES
    //===================================================================

    /**
     * The environment property name for the full classname of the MessageBus
     * implementation to use
     * This property is optional.
     */
//    public static final String MESSAGING_MESSAGE_BUS_CLASS = "metamatrix.extensionsource.messaging.MessageBusClass";

}
