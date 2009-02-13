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

package com.metamatrix.connector.metadata;

import java.util.Iterator;
import java.util.List;

import com.metamatrix.connector.api.DataNotAvailableException;
import com.metamatrix.connector.api.ProcedureExecution;
import com.metamatrix.connector.basic.BasicExecution;
import com.metamatrix.connector.exception.ConnectorException;
import com.metamatrix.connector.language.IParameter;
import com.metamatrix.connector.language.IProcedure;
import com.metamatrix.connector.metadata.internal.IObjectSource;
import com.metamatrix.connector.metadata.internal.ObjectProcedure;
import com.metamatrix.connector.metadata.internal.ObjectProcedureProcessor;
import com.metamatrix.connector.metadata.runtime.RuntimeMetadata;


/** 
 * @since 4.2
 */
public class MetadataProcedureExecution extends BasicExecution implements ProcedureExecution {

    private final RuntimeMetadata metadata;
    private final ObjectProcedureProcessor processor;
    private IProcedure proc;
    private ObjectProcedure procedure;
    private Iterator queryResults;
    private volatile boolean cancel;

    /** 
     * @param metadata
     * @param objectSource
     * @since 4.2
     */
    public MetadataProcedureExecution(final IProcedure procedure, final RuntimeMetadata metadata, final IObjectSource objectSource) {
        this.metadata = metadata;
        this.processor = new ObjectProcedureProcessor(objectSource);
        this.proc = procedure;
    }
    
    @Override
    public void execute() throws ConnectorException {
        this.procedure = new ObjectProcedure(metadata, proc);
        this.queryResults = processor.process(this.procedure);
        if(this.procedure.getResultSetNameInSource() == null) {
        	queryResults = null;
        }
    }
    
    @Override
    public List next() throws ConnectorException, DataNotAvailableException {
    	if (cancel) {
            throw new ConnectorException(MetadataConnectorPlugin.Util.getString("ObjectSynchExecution.closed")); //$NON-NLS-1$
        }
    	if(this.queryResults == null) {
        	return null;
        }
    	int count = 0;
    	if (queryResults.hasNext()) {
    		return (List)queryResults.next();
    	}
    	return null;
    }

    /** 
     * @see com.metamatrix.connector.api.ProcedureExecution#getOutputValue(com.metamatrix.connector.language.IParameter)
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
     * @see com.metamatrix.connector.api.Execution#cancel()
     * @since 4.2
     */
    public void cancel() throws ConnectorException {
    	cancel = true;
    }

    /** 
     * @see com.metamatrix.connector.api.Execution#close()
     * @since 4.2
     */
    public void close() throws ConnectorException {
    }

}
