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

package com.metamatrix.connector.metadata;

import java.util.Iterator;
import java.util.List;

import com.metamatrix.connector.metadata.adapter.ObjectResultsTranslator;
import com.metamatrix.connector.metadata.internal.IObjectSource;
import com.metamatrix.connector.metadata.internal.ObjectProcedure;
import com.metamatrix.connector.metadata.internal.ObjectProcedureProcessor;
import com.metamatrix.data.api.Batch;
import com.metamatrix.data.api.ProcedureExecution;
import com.metamatrix.data.basic.BasicBatch;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.language.IParameter;
import com.metamatrix.data.language.IProcedure;
import com.metamatrix.data.metadata.runtime.RuntimeMetadata;


/** 
 * @since 4.2
 */
public class MetadataProcedureExecution implements ProcedureExecution {

    private final RuntimeMetadata metadata;
    private final ObjectProcedureProcessor processor;

    private int maxBatchSize;
    private ObjectProcedure procedure;
    private Iterator queryResults;
 

    /** 
     * @param metadata
     * @param objectSource
     * @since 4.2
     */
    public MetadataProcedureExecution(final RuntimeMetadata metadata, final IObjectSource objectSource, final ObjectResultsTranslator resultsTranslator) {
        this.metadata = metadata;
        this.processor = new ObjectProcedureProcessor(objectSource, resultsTranslator);
    }

    /** 
     * @see com.metamatrix.data.api.ProcedureExecution#execute(com.metamatrix.data.language.IProcedure, int)
     * @since 4.2
     */
    public void execute(final IProcedure procedure, final int maxBatchSize) throws ConnectorException {
        
        this.procedure = new ObjectProcedure(metadata, procedure);
        this.maxBatchSize = maxBatchSize;
        this.queryResults = processor.process(this.procedure);
        if(this.procedure.getResultSetNameInSource() == null) {
        	queryResults = null;
        }
    }
 

    /** 
     * @see com.metamatrix.data.api.ProcedureExecution#getOutputValue(com.metamatrix.data.language.IParameter)
     * @since 4.2
     */
    public Object getOutputValue(final IParameter parameter) throws ConnectorException {
        if(parameter.getDirection() != IParameter.OUT && parameter.getDirection() != IParameter.INOUT &&  parameter.getDirection() != IParameter.RETURN){
            throw new ConnectorException(MetadataConnectorPlugin.Util.getString("ObjectProcedureExecution.0")); //$NON-NLS-1$
        }
        //TODO: Output parameters are not currently handled
        return null;
    }

    /** 
     * @see com.metamatrix.data.api.Execution#cancel()
     * @since 4.2
     */
    public void cancel() throws ConnectorException {
        throwAwayResults();        
    }

    /** 
     * @see com.metamatrix.data.api.Execution#close()
     * @since 4.2
     */
    public void close() throws ConnectorException {
        throwAwayResults();        
    }

    private void throwAwayResults() {
        queryResults = null;
    }

    /** 
     * @see com.metamatrix.data.api.BatchedExecution#nextBatch()
     * @since 4.2
     */
    public Batch nextBatch() throws ConnectorException {
        if(this.queryResults == null) {
        	return null;
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
}
