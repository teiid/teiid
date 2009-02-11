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

package com.metamatrix.platform.config.api.service;

public class ConfigurationServicePropertyNames {

    /**
     * The environment property name for the class that is to be used for the MetadataConnectionFactory implementation.
     * This property is required (there is no default).
     */
    public static final String CONNECTION_FACTORY = "metamatrix.config.connection.Factory"; //$NON-NLS-1$

    /**
     * The environment property name for the class of the driver.
     * This property is optional.
     */
    public static final String CONNECTION_DRIVER = "metamatrix.config.connection.Driver"; //$NON-NLS-1$

    /**
     * The environment property name for the protocol for connecting to the metadata store.
     * This property is optional.
     */
    public static final String CONNECTION_PROTOCOL = "metamatrix.config.connection.Protocol"; //$NON-NLS-1$

    /**
     * The environment property name for the name of the metadata store database.
     * This property is optional.
     */
    public static final String CONNECTION_DATABASE = "metamatrix.config.connection.Database"; //$NON-NLS-1$

    /**
     * The environment property name for the username that is to be used for connecting to the metadata store.
     * This property is optional.
     */
    public static final String CONNECTION_USERNAME = "metamatrix.config.connection.User"; //$NON-NLS-1$

    /**
     * The environment property name for the password that is to be used for connecting to the metadata store.
     * This property is optional.
     */
    public static final String CONNECTION_PASSWORD = "metamatrix.config.connection.Password"; //$NON-NLS-1$

    /**
     * The environment property name for the maximum number of milliseconds that a metadata connection
     * may remain unused before it becomes a candidate for garbage collection.
     * This property is optional.
     */
    public static final String CONNECTION_POOL_MAXIMUM_AGE = "metamatrix.config.connection.MaximumAge"; //$NON-NLS-1$

    /**
     * The environment property name for the maximum number of concurrent users of a single metadata connection.
     * This property is optional.
     */
    public static final String CONNECTION_POOL_MAXIMUM_CONCURRENT_USERS = "metamatrix.config.connection.MaximumConcurrentReaders"; //$NON-NLS-1$
   
    /**
     * The default connection factory class to use when one is not specified.
     */
    public static final String DEFAULT_CONNECTION_FACTORY_CLASS="com.metamatrix.platform.config.spi.xml.XMLConfigurationConnectorFactory";
}

