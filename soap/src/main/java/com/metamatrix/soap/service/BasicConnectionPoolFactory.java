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

import java.io.IOException;
import java.io.InputStream;
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
    public static final String MMPOOL_PROPERTIES_FILENAME = "/teiidpool.properties";  //$NON-NLS-1$

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
        defaultProperties.setProperty(TIME_BETWEEN_EVICTION_THREAD_RUNS_KEY,
                String.valueOf(TIME_BETWEEN_EVICTION_THREAD_RUNS_DEFAULT));
        defaultProperties.setProperty(ConnectionSource.DRIVER_CLASS, "com.metamatrix.jdbc.MMDriver"); //$NON-NLS-1$
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
        /* 1. 'mergedProperties' will contain the available properties, constructed in the correct sequence
         *    so that propery values that should override other values, will override them.
         * 2. 'mergedProperties' must be recreated from scratch every time 'createConnectionPool' is called.
         */
        Properties mergedProperties = createMergedProperties( poolProperties );

        p.setProperty(JDBCUtil.DRIVER, mergedProperties.getProperty(ConnectionSource.DRIVER_CLASS));
        p.setProperty(JDBCUtil.USERNAME, mergedProperties.getProperty(ConnectionSource.USERNAME));
        p.setProperty(JDBCUtil.PASSWORD, mergedProperties.getProperty(ConnectionSource.PASSWORD));
        p.setProperty(JDBCUtil.DATABASE, poolProperties.getProperty(ConnectionSource.SERVER_URL));
        p.setProperty(SimplePooledConnectionSource.MAXIMUM_RESOURCE_POOL_SIZE, mergedProperties.getProperty(MAX_ACTIVE_CONNECTIONS_PROPERTY_KEY));
        p.setProperty(SimplePooledConnectionSource.WAIT_TIME_FOR_RESOURCE, mergedProperties.getProperty(MAX_WAIT_PROPERTY_KEY));
        return new SimplePooledConnectionSource(p);
    }

    protected static Properties createMergedProperties( Properties poolProperties ) {

        // 1. start with the default properties
        Properties mergedProperties = new Properties( defaultProperties );
        
        // 2. add properties from mmpool.properties        
        mergedProperties.putAll( getMMPoolProperties() );

        // 3. add System.properties
        mergedProperties.putAll(System.getProperties());
        
        // 4. add specific poolProperties
        mergedProperties.putAll( poolProperties );
        
        return mergedProperties;
    }
    
    protected static Properties getMMPoolProperties() {
        Properties p = new Properties();

        InputStream is = BasicConnectionPoolFactory.class.getClassLoader().getResourceAsStream( MMPOOL_PROPERTIES_FILENAME ); //$NON-NLS-1$
        
        if ( is != null ) {
            try {
                 p.load( is );
            } catch( IOException ioe ) {
                return new Properties();
            }
        }
        
        return p;
    }

}
