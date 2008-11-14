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

package com.metamatrix.soap.service;

import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;

/**
 * This is the basic implemecntation of the ConnectionPoolFactory interface. It uses the apache dbcp libarary to create DataSource
 * instances that pool connections under the covers.
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
        final BasicDataSource pool = new BasicDataSource();
        pool.setDriverClassName("com.metamatrix.jdbc.MMDriver"); //$NON-NLS-1$
        pool.setUsername(poolProperties.getProperty(ConnectionSource.USERNAME));
        pool.setPassword(poolProperties.getProperty(ConnectionSource.PASSWORD));
        pool.setUrl(poolProperties.getProperty(ConnectionSource.SERVER_URL));
        pool.setInitialSize(Integer.parseInt(getProperty(INITIAL_POOL_SIZE_PROPERTY_KEY)));
        pool.setMaxActive(Integer.parseInt(getProperty(MAX_ACTIVE_CONNECTIONS_PROPERTY_KEY)));
        pool.setMaxIdle(Integer.parseInt(getProperty(MAX_IDLE_CONNECTIONS_PROPERTY_KEY)));
        pool.setMaxWait(Integer.parseInt(getProperty(MAX_WAIT_PROPERTY_KEY)));
        pool.setMinIdle(Integer.parseInt(getProperty(MIN_IDLE_COUNT_PROPERTY_KEY)));
        pool.setTimeBetweenEvictionRunsMillis(Integer.parseInt(getProperty(TIME_BETWEEN_EVICTION_THREAD_RUNS_KEY)));
        pool.setMinEvictableIdleTimeMillis(Integer.parseInt(getProperty(MIN_EVICTABLE_IDLE_TIME_MILLIS_KEY)));

        /*
         * we do this to make sure that if the session has been invalidated, that we find out early and go ahead and clean the
         * connections out of the pool.
         */
        pool.setTestWhileIdle(true);

        return pool;
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
