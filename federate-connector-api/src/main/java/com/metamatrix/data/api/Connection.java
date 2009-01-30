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

package com.metamatrix.data.api;

import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.metadata.runtime.RuntimeMetadata;
import com.metamatrix.data.pool.PoolAwareConnection;

/**
 * <p>Represents a (typically) connection to this connector.  A connection will be obtained
 * from the connector for every query that is executed, then closed after the query has completed.
 * </p>
 * <p>If pooling is enabled, see {@link PoolAwareConnection} to implement pooling specific behavior.
 * </p>  
 */
public interface Connection {

    /**
     * Get the capabilities of this connector.  The capabilities affect what kinds of 
     * queries (and other commands) will be sent to the connector.
     * @return Connector capabilities
     */
    ConnectorCapabilities getCapabilities();

    /**
     * Create an execution object for the specified mode.  
     * @param executionMode The execution mode required for this execution
     * @param executionContext Provides information about the context that this command is
     * executing within, such as the identifiers for the MetaMatrix command being executed
     * @param metadata Access to runtime metadata if needed to translate the command
     * @return An execution object that MetaMatrix can use to execute the command
     */
    Execution createExecution( int executionMode, ExecutionContext executionContext, RuntimeMetadata metadata ) throws ConnectorException;

    /**
     * Release the connection.  This will be called when MetaMatrix has completed 
     * using the connection for an execution.
     */
    void release();
}

