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

package com.metamatrix.soap.service;

import java.util.Properties;

import javax.sql.DataSource;

/**
 * This interface defined the constants and methods used to create connection pools.
 * 
 * @since 4.3
 */
public interface ConnectionPoolFactory {

    /**
     * This property defines how many connections will be created when the pool is instantiated.
     */
    public static final String INITIAL_POOL_SIZE_PROPERTY_KEY = "com.metamatrix.soap.service.initpoolsize"; //$NON-NLS-1$

    /**
     * The value for this property defines the maximum number of connections that can be available in the pool at any one time.
     */
    public static final String MAX_ACTIVE_CONNECTIONS_PROPERTY_KEY = "com.metamatrix.soap.service.maxactiveconnections";//$NON-NLS-1$

    /**
     * This value for this property defines the maximum number of connections that are idle in the pool at one time.
     */
    public static final String MAX_IDLE_CONNECTIONS_PROPERTY_KEY = "com.metamatrix.soap.service.maxidleconnections";//$NON-NLS-1$

    /**
     * The value for this property defines the maximum amount of time to block a client that requests a connection from the pool.
     * An Exception will be throw to the client if this amount of time is exceeded.
     */
    public static final String MAX_WAIT_PROPERTY_KEY = "com.metamatrix.soap.service.maxwait";//$NON-NLS-1$

    /**
     * The value for this property defines the minimum number of idle connections that will be available in the pool at any time.
     */
    public static final String MIN_IDLE_COUNT_PROPERTY_KEY = "com.metamatrix.soap.service.minidlecount"; //$NON-NLS-1$

    /**
     * The value for this property defines the amount of time a connection can be idle in the pool before being 'destroyed'.
     */
    public static final String MIN_EVICTABLE_IDLE_TIME_MILLIS_KEY = "com.metamatrix.soap.service.minevictableidletime"; //$NON-NLS-1$

    /**
     * The value for this property defines the amount of time between checks for connections that have exceeded the
     * MIN_EVICTABLE_IDLE_TIME.
     */
    public static final String TIME_BETWEEN_EVICTION_THREAD_RUNS_KEY = "com.metamatrix.soap.service.timebetweenevictionthreadruns"; //$NON-NLS-1$

    /*
     * These are the DBCP pool property defaults we will use for all pools created by this factory instance. These properties can
     * be overridden by setting them as JVM properties. property keys are located in the interface of this class
     * ConnectionPoolFactory.
     */
    public static final int INITIAL_POOL_SIZE_PROPERTY_DEFAULT = 1;
    public static final int MAX_ACTIVE_CONNECTIONS_DEFAULT = 25;
    public static final int MAX_IDLE_CONNECTIONS_DEFAULT = 10;
    public static final int MAX_WAIT_PROPERTY_DEFAULT = 30000;
    public static final int MIN_IDLE_COUNT_PROPERTY_DEFAULT = 0;
    public static final int MIN_EVICTABLE_IDLE_TIME_MILLIS_DEFAULT = 600000;
    public static final int TIME_BETWEEN_EVICTION_THREAD_RUNS_DEFAULT = 60000;

    public DataSource createConnectionPool(Properties connectionProperties);

}
