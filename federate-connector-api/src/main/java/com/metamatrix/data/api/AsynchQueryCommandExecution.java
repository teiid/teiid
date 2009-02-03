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
import com.metamatrix.data.language.IQueryCommand;


/** 
 * The AsynchQueryCommandExecution interface works much like the {@link com.metamatrix.data.api.SynchQueryCommandExecution}
 * except that the connector will be polled for results until they arrive.  The connector
 * implementing this interface is expected to check whether data is available and return it if
 * so.  Otherwise, a null or empty batch should be returned, indicating that no data is available
 * yet.  The connector manager will guarantee that calls to the nextBatch() method for a particular
 * query never overlap.
 *  
 * @since 6.0
 */
public interface AsynchQueryCommandExecution extends Execution, BatchedExecution {

    /**
     * Get polling interval, used to determine how long to wait between poll requests.  
     * @return Interval, in milliseconds, to poll for results, must be > 0
     */
    long getPollInterval();
    
    /**
     * Execute the asynchronous query execution.  The {@link BatchedExecution#nextBatch()} method
     * will be used to poll for results.  
     *  
     * @param query The query to execute
     * @param maxBatchSize The maximum size of any return batch
     * @throws ConnectorException If an error occurs during execution
     */
    void executeAsynch( IQueryCommand query, int maxBatchSize ) throws ConnectorException;

}
