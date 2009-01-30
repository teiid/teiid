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

/*
 */
package com.metamatrix.data.pool;

import com.metamatrix.data.exception.ConnectorException;

/**
 * All source-specific connections should implement this interface.  Typically this
 * is a holder object that can be used to do a few useful things on the source-specific 
 * connection.
 * 
 * @deprecated Connection pooling can be provided automatically by the Query Engine.
 */
public interface SourceConnection {

    /**
     * This property is used to specify the length of time between JDBC Source test connections
     * How often (in seconds) to test that the data source is available by establishing a new connection.  Default to 600 seconds.
     */
    public static final String SOURCE_CONNECTION_TEST_INTERVAL = "SourceConnectionTestInterval"; //$NON-NLS-1$

    public final String DEFAULT_SOURCE_CONNECTION_TEST_INTERVAL = "600";  //10 minutes //$NON-NLS-1$
    /**
     * Determine whether the connection is open
     * @return True if open, false if closed or failed.
     */    
    boolean isAlive();

    /**
     * Close the underlying source connection
     * @throws ConnectorException If an error occured while closing
     */
    void closeSource() throws ConnectorException;
}
