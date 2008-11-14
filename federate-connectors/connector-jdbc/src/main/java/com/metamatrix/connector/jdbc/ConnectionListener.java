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

package com.metamatrix.connector.jdbc;

import java.sql.Connection;

import com.metamatrix.data.api.ConnectorEnvironment;

/**
 * Connection listner for the life cycle events for the connection 
 */
public interface ConnectionListener {
    
    /**
     * This call will be invoked right after the connection to the datasource
     * is open by the <code>JDBCConnectionFactory</code>. This will invoked every 
     * connection in pool.
     * @param connection
     */
    void afterConnectionCreation(Connection connection, ConnectorEnvironment env);
    /**
     * This call will be invoked just before the connection to the datasource
     * is relased to the connection pool or closed, which ever action taken by
     * the <code>JDBCConnectionFactory</code>  
     * @param connection
     */    
    void beforeConnectionClose(Connection connection, ConnectorEnvironment env );
}
