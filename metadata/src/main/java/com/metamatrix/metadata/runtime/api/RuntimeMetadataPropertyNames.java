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

package com.metamatrix.metadata.runtime.api;

public class RuntimeMetadataPropertyNames {

    /**
    * The resource name that identifies runtime metadata.  
    */
    public static final String RESOURCE_NAME = "RuntimeMetadata";

    //connection properties
    /**
     * The environment property name for the class that is to be used for the MetadataConnectionFactory implementation.
     * This property is required (there is no default).
     */
    public static final String CONNECTION_FACTORY = "metamatrix.metadata.runtime.connection.Factory";

    /**
     * The environment property name for the class of the driver.
     * This property is optional.
     */
    public static final String CONNECTION_DRIVER = "metamatrix.metadata.runtime.connection.Driver";

    /**
     * The environment property name for the protocol for connecting to the metadata store.
     * This property is optional.
     */
    public static final String CONNECTION_PROTOCOL = "metamatrix.metadata.runtime.connection.Protocol";

    /**
     * The environment property name for the name of the metadata store database.
     * This property is optional.
     */
    public static final String CONNECTION_DATABASE = "metamatrix.metadata.runtime.connection.Database";

    /**
     * The environment property name for the username that is to be used for connecting to the metadata store.
     * This property is optional.
     */
    public static final String CONNECTION_USERNAME = "metamatrix.metadata.runtime.connection.User";

    /**
     * The environment property name for the password that is to be used for connecting to the metadata store.
     * This property is optional.
     */
    public static final String CONNECTION_PASSWORD = "metamatrix.metadata.runtime.connection.Password";

    /**
     * The environment property name for the maximum number of milliseconds that a metadata connection
     * may remain unused before it becomes a candidate for garbage collection.
     * This property is optional.
     */
    public static final String CONNECTION_POOL_MAXIMUM_AGE = "metamatrix.metadata.runtime.connection.MaximumAge";

    /**
     * The environment property name for the maximum number of concurrent users of a single metadata connection.
     * This property is optional.
     */
    public static final String CONNECTION_POOL_MAXIMUM_CONCURRENT_USERS = "metamatrix.metadata.runtime.connection.MaximumConcurrentReaders";

    //Others
    /*
     * The property determines whether to persist virtual database in runtime repository.
     * By default, the value is true, which means the virtual databases will be stored in runtime repository.
     * Otherwise, the virtual database will only stay in memory.
     * The property is optional.
     */
    public static final String PERSIST = "metamatrix.metadata.runtime.persist";

    public static final String METADATA_SUPPLIER_CLASS_NAME = "metamatrix.metadata.runtime.supplierClass";

    public static final String METADATA_RESOURCE_CLASS_NAME = "metamatrix.metadata.runtime.resourceClass";


    public static final String RT_VIRTUAL_MODEL_NAME = "metamatrix.metadata.runtime.virtualmodel.name";

    public static final String RT_VIRTUAL_MODEL_VERSION = "metamatrix.metadata.runtime.virtualmodel.version";

    public static final String RT_PHYSICAL_MODEL_NAME = "metamatrix.metadata.runtime.physicalmodel.name";

    public static final String RT_PHYSICAL_MODEL_VERSION = "metamatrix.metadata.runtime.physicalmodel.version";

    /*
     * Default values for creating VDB not through console
     */
    public static final String RT_USER_VDB_NAME = "metamatrix.metadata.runtime.uservdb.name";
    public static final String RT_USER_VDB_GUID = "metamatrix.metadata.runtime.uservdb.guid";
    public static final String RT_USER_VDB_PRINCIPAL_NAME = "metamatrix.metadata.runtime.uservdb.principalName";
    public static final String RT_USER_VDB_DESCRIPTION = "metamatrix.metadata.runtime.uservdb.description";
}

