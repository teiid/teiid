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
import com.metamatrix.data.language.IQuery;

/**
 * The synchronous execution represents the case where a connector can 
 * execute and retrieve batches in a synchronous manner.  The Connector Manager
 * will call {@link #execute(IQuery, int)}, then will call {@link #nextBatch()} 
 * method until a batch is returned that has {@link Batch#isLast()} == true.
 * 
 * @deprecated {@link SynchQueryCommandExecution}
 */
public interface SynchQueryExecution extends Execution, BatchedExecution {
    
    /**
     * Execute the synchronous query execution.  Results will be 
     * obtained via the nextBatch command.
     * @param query The query to execute
     * @param maxBatchSize The maximum size of any return batch
     * @throws ConnectorException If an error occurs during execution
     */
    void execute( IQuery query, int maxBatchSize ) throws ConnectorException;

}
