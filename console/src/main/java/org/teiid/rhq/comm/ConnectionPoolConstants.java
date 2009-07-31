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
package org.teiid.rhq.comm;



/** 
 * These are the properties are used by the {@link ConnectionPool) to create and manage
 * connections.   These properties are passed into the pool when at initialization time.
 * @since 1.0
 */
public interface ConnectionPoolConstants {
     
    /**
     * Indicates the connection factory to use in the connection pool.
     */
    public static final String CONNECTION_FACTORY = "pool.connection.factory"; //$NON-NLS-1$
        
    /**
     * Maximum connections for this pool. Default to 0, which means there is no limit.
     */
//    public static final String MAX_CONNECTIONS = "pool.max_connections"; //$NON-NLS-1$

    /**
     * Idle time of the connection before it should be closed in seconds. Default to 60 seconds.
     */
    public static final String LIVE_AND_UNUSED_TIME = "pool.live_and_unused_time"; //$NON-NLS-1$

    /**
     * Time to wait if the connection is not available in milliseconds.  Default to 2 seconds.
     */
    public static final String WAIT_FOR_SOURCE_TIME = "pool.wait_for_source_time"; //$NON-NLS-1$

    /**
     * Interval for running the cleaning thread in seconds. Default to 60 seconds.
     */
    public static final String CLEANING_INTERVAL = "pool.cleaning_interval"; //$NON-NLS-1$

    /**
     * Whether to enable pool shrinking.  Default to true.
     */
    public static final String ENABLE_SHRINKING = "pool.enable_shrinking"; //$NON-NLS-1$
    
    /**
     * This property is used to specify the length of time between JDBC Source test connections
     * How often (in seconds) to test that the data source is available by establishing a new connection.  Default to 600 seconds.
     */
//    public static final String CONNECTION_TEST_INTERVAL = "pool.connection.test.interval"; //$NON-NLS-1$

    
        


}
