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

package com.metamatrix.connector.metadata.adapter;

import java.util.Iterator;
import java.util.List;

import com.metamatrix.connector.metadata.MetadataConnectorPlugin;
import com.metamatrix.connector.metadata.internal.IObjectQuery;
import com.metamatrix.connector.metadata.internal.MetadataException;
import com.metamatrix.connector.metadata.internal.ObjectQuery;
import com.metamatrix.connector.metadata.internal.ObjectQueryProcessor;
import com.metamatrix.data.api.Batch;
import com.metamatrix.data.api.SynchQueryExecution;
import com.metamatrix.data.basic.BasicBatch;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.language.IQuery;
import com.metamatrix.data.metadata.runtime.RuntimeMetadata;

/**
 * Adapter to expose the object processing logic via the standard connector API.
 * Makes the batches coming from the objectSource match the batch sizes requested by the caller.
 */
public class ObjectSynchExecution implements SynchQueryExecution {
    private final RuntimeMetadata metadata;
    private ObjectQueryProcessor processor;

    private IObjectQuery query;
    private Iterator queryResults;
    private int maxBatchSize;
    private boolean closed;
    private ObjectConnection connection;

    public ObjectSynchExecution(RuntimeMetadata metadata, ObjectConnection connection) {
        this.metadata = metadata;
        this.connection = connection;
    }

    private void throwAwayResults() {
        queryResults = null;
    }

    public synchronized void execute(IQuery query, int maxBatchSize) throws ConnectorException {
        if (closed) {
            throw new ConnectorException(MetadataConnectorPlugin.Util.getString("ObjectSynchExecution.closed")); //$NON-NLS-1$
        }
        this.processor = new ObjectQueryProcessor(connection.getMetadataObjectSource());
        
        this.query = new ObjectQuery(metadata, query);
        this.maxBatchSize = maxBatchSize;  
        try {
			queryResults = processor.process(this.query);
		} catch (MetadataException e) {
			throw new ConnectorException(e);
		}              
    }

    /* 
     * @see com.metamatrix.data.SynchExecution#nextBatch(int)
     */
    public synchronized Batch nextBatch() throws ConnectorException {
        if (closed) {
            throw new ConnectorException(MetadataConnectorPlugin.Util.getString("ObjectSynchExecution.closed")); //$NON-NLS-1$
        }
        int count = 0;
        BasicBatch result = new BasicBatch();
        while (queryResults.hasNext() && count++ < maxBatchSize) {
        	result.addRow((List)queryResults.next());
        }
        if (!queryResults.hasNext()) {
            result.setLast();
        }
        return result;
    }

    /* 
     * @see com.metamatrix.data.Execution#cancel()
     */
    public synchronized void cancel() throws ConnectorException {
        closed = true;
        throwAwayResults();
    }

    /* 
     * @see com.metamatrix.data.Execution#close()
     */
    public synchronized void close() throws ConnectorException {
        /* Defect 18362 - Since cancel/close occur asynchronously, it's possible that queryresults can be set to
         * null at any time. Synchronizing alleviates possible races that might occur when the
         * connector is concurrently calling execute() or nextBatch().
         */
        closed = true;
        throwAwayResults();
    }

}
