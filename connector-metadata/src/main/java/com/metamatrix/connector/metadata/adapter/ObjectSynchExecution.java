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

package com.metamatrix.connector.metadata.adapter;

import java.util.Iterator;
import java.util.List;

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.DataNotAvailableException;
import org.teiid.connector.api.ResultSetExecution;
import org.teiid.connector.basic.BasicExecution;
import org.teiid.connector.language.IQuery;
import org.teiid.connector.metadata.IObjectQuery;
import org.teiid.connector.metadata.ObjectQueryProcessor;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;

import com.metamatrix.connector.metadata.MetadataConnectorPlugin;
import com.metamatrix.connector.metadata.internal.ObjectQuery;

/**
 * Adapter to expose the object processing logic via the standard connector API.
 * Makes the batches coming from the objectSource match the batch sizes requested by the caller.
 */
public class ObjectSynchExecution extends BasicExecution implements ResultSetExecution {
    private final RuntimeMetadata metadata;
    private ObjectQueryProcessor processor;

    private IObjectQuery query;
    private Iterator queryResults;
    private ObjectConnection connection;
    private IQuery command;
    private volatile boolean cancel;

    public ObjectSynchExecution(IQuery command, RuntimeMetadata metadata, ObjectConnection connection) {
        this.metadata = metadata;
        this.connection = connection;
        this.command = command;
    }

    @Override
    public void execute() throws ConnectorException {
        this.processor = new ObjectQueryProcessor(connection.getMetadataObjectSource());
        
        this.query = new ObjectQuery(metadata, command);
		queryResults = processor.process(this.query);
    }
    
    @Override
    public List next() throws ConnectorException, DataNotAvailableException {
    	if (cancel) {
            throw new ConnectorException(MetadataConnectorPlugin.Util.getString("ObjectSynchExecution.closed")); //$NON-NLS-1$
        }
    	if(this.queryResults == null) {
        	return null;
        }
    	if (queryResults.hasNext()) {
    		return (List)queryResults.next();
    	}
    	return null;
    }

    /* 
     * @see com.metamatrix.data.Execution#cancel()
     */
    @Override
    public void cancel() throws ConnectorException {
       cancel = true;
    }

    /* 
     * @see com.metamatrix.data.Execution#close()
     */
    @Override
    public void close() throws ConnectorException {
    }

}
