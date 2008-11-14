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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import com.metamatrix.soap.SOAPPlugin;

/**
 * This implmentation of ConnectionSource uses a HashMap to cache instances of Connection Pools with their connection properties
 * as the key.
 */
public class PoolingConnectionSource implements
                                    ConnectionSource {

    /**
     * The internal factory used to create pools.
     */
    private ConnectionPoolFactory poolFactory;
    
    /**
     * The internal Map of Connection pools keyed by the connection properties used to create them.
     */
    private Map pools = new HashMap();

    /**
     * Constructor that instantiates a BasicConnectionPoolFactory for pool creation duties.
     * 
     * 
     */
    public PoolingConnectionSource() {
        poolFactory = new BasicConnectionPoolFactory();
    }

    /**
     *  Constructor that allows the user to pass in their own implementation of ConnectionPoolFactory.
     *  
     * @param poolFactory the connection pool factory that will be used to create connection pools.
     *
     */
    public PoolingConnectionSource(final ConnectionPoolFactory poolFactory) {
        this.poolFactory = poolFactory;
    }

    /**
     * @see com.metamatrix.soap.service.ConnectionSource#getConnection(java.util.Properties)
     * @since 4.3
     */
    public Connection getConnection(final Properties connectionProperties) throws SQLException {
        if (poolFactory == null) {
            throw new SQLException(
                                   SOAPPlugin.Util
                                                  .getString("BasicConnectionPool.The_connection_pool_factory_given_the_Pooling_Connection_Source")); //$NON-NLS-1$
        }

        Object pool = pools.get(connectionProperties);

        if (pool == null) {
            pool = createPool(connectionProperties);
            pools.put(connectionProperties, pool);
        }

        

        final DataSource source = (DataSource)pool;
        if (source != null) {
            return source.getConnection();
        }
        
        throw new SQLException(SOAPPlugin.Util.getString("PoolingConnectionSource.The_DataSource_instance_pulled_from_the")); //$NON-NLS-1$

    }

    /**
     * This method will create a Connection Pool. 
     * @param properties the connection properties to be used to create connections in the pool. 
     * See ConnectionSource for key values.
     * @return A connection pool instance as an Object.
     * @throws SQLException
     *
     */
    private Object createPool(final Properties properties) throws SQLException {
        return poolFactory.createConnectionPool(properties);
    }
    
    /**
     * This method can be used to check to see if this Connection Source has a poool with
     * the passed in properties 
     * @param connectionProperties The properties to check to see if there is a matching pool for in
     * this connection source.
     * @return true if a pool is found with the matching passed in properties.
     *
     */
    protected boolean hasPool(final Properties connectionProperties) {
        return pools.get(connectionProperties)!=null;
    }

}
