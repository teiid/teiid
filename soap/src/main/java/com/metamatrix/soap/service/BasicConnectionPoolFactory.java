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

import com.metamatrix.common.jdbc.JDBCUtil;
import com.metamatrix.common.jdbc.SimplePooledConnectionSource;

/**
 * This is the basic implementation of the ConnectionPoolFactory interface.
 * 
 * @since 4.3
 */
public class BasicConnectionPoolFactory implements
                                       ConnectionPoolFactory {

    /*
     * This is the set of default properties that are used to drive the way that the pools produced by this factory behaves.
     */
    private static final Properties defaultProperties = new Properties();

    static {
        /*
         * check for system property overrides and if none exist, use the defaults.
         */
        defaultProperties.setProperty(INITIAL_POOL_SIZE_PROPERTY_KEY, String.valueOf(INITIAL_POOL_SIZE_PROPERTY_DEFAULT));
        defaultProperties.setProperty(MAX_ACTIVE_CONNECTIONS_PROPERTY_KEY, String.valueOf(MAX_ACTIVE_CONNECTIONS_DEFAULT));
        defaultProperties.setProperty(MAX_IDLE_CONNECTIONS_PROPERTY_KEY, String.valueOf(MAX_IDLE_CONNECTIONS_DEFAULT));
        defaultProperties.setProperty(MAX_WAIT_PROPERTY_KEY, String.valueOf(MAX_WAIT_PROPERTY_DEFAULT));
        defaultProperties.setProperty(MIN_IDLE_COUNT_PROPERTY_KEY, String.valueOf(MIN_IDLE_COUNT_PROPERTY_DEFAULT));
        defaultProperties.setProperty(MIN_EVICTABLE_IDLE_TIME_MILLIS_KEY, String.valueOf(MIN_EVICTABLE_IDLE_TIME_MILLIS_DEFAULT));
        defaultProperties.setProperty(TIME_BETWEEN_EVICTION_THREAD_RUNS_KEY,
                                      String.valueOf(TIME_BETWEEN_EVICTION_THREAD_RUNS_DEFAULT));
    }

    /**
     * @see com.metamatrix.soap.service.ConnectionPoolFactory#createConnectionPool(java.util.Properties)
     * @since 4.3
     */
    public DataSource createConnectionPool(final Properties poolProperties) {
        if (poolProperties == null) {
            return null;
        }
        Properties p = new Properties(poolProperties);
        p.setProperty(JDBCUtil.DRIVER, "com.metamatrix.jdbc.MMDriver");
        p.setProperty(JDBCUtil.USERNAME, poolProperties.getProperty(ConnectionSource.USERNAME));
        p.setProperty(JDBCUtil.PASSWORD, poolProperties.getProperty(ConnectionSource.PASSWORD));
        p.setProperty(JDBCUtil.DATABASE, poolProperties.getProperty(ConnectionSource.SERVER_URL));
        p.setProperty(SimplePooledConnectionSource.MAXIMUM_RESOURCE_POOL_SIZE, getProperty(MAX_ACTIVE_CONNECTIONS_PROPERTY_KEY));
        p.setProperty(SimplePooledConnectionSource.WAIT_TIME_FOR_RESOURCE, getProperty(MAX_WAIT_PROPERTY_KEY));
        return new SimplePooledConnectionSource(p);
    }

    /**
     * This method will get a property value from the JVM System properties with the default value from the default properties
     * class level instance being the default value.
     * 
     * @param propKey
     *            The key for the property to get
     * @return the value of the property for the passed in key
     * @since 4.3
     */
    protected String getProperty(final String propKey) {
        return System.getProperty(propKey, defaultProperties.getProperty(propKey));
    }

}
