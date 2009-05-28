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

package com.metamatrix.jdbc;

import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/** 
 * A marker interface for creating connections Embedded DQP. The interface is 
 * defined so that it can be used with DQP class loading.  
 */
public interface EmbeddedConnectionFactory {
	
    public void initialize(URL bootstrapURL, Properties props) throws SQLException;

    /**
     * Create a Connection to the DQP. This will load a DQP instance if one is not present 
     * @param bootstrapURL
     * @param properties
     * @return Connection to DQP
     * @throws SQLException
     */
    public Connection createConnection(URL bootstrapURL, Properties properties) throws SQLException;  
    
    /**
     * Shutdown the connection factory, including the DQP and all its existing connections 
     */
    public void shutdown() throws SQLException;
}
