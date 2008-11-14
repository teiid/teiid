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

import com.metamatrix.connector.metadata.MetadataProcedureExecution;
import com.metamatrix.connector.sysadmin.SysAdminProcedureExecution;
import com.metamatrix.data.api.Batch;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.ProcedureExecution;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.language.IParameter;
import com.metamatrix.data.language.IProcedure;
import com.metamatrix.data.metadata.runtime.MetadataID;
import com.metamatrix.data.metadata.runtime.RuntimeMetadata;


/** 
 * @since 4.2
 */
public class ObjectProcedureExecution implements ProcedureExecution {

    private ProcedureExecution redirection = null;
    private ObjectResultsTranslator translator = null;    
    
    private final RuntimeMetadata metadata;
    private ConnectorEnvironment environment;
    
    private ObjectConnection connection;
    


    /** 
     * @param metadata
     * @param objectSource
     * @since 4.2
     */
    public ObjectProcedureExecution(final RuntimeMetadata metadata, ObjectConnection connection, final ObjectResultsTranslator resultsTranslator, ConnectorEnvironment environment) {
        this.metadata = metadata;
        this.translator = resultsTranslator;
        this.environment = environment;
        this.connection = connection;
        
    }

    /** 
     * @see com.metamatrix.data.api.ProcedureExecution#execute(com.metamatrix.data.language.IProcedure, int)
     * @since 4.2
     */
    public void execute(final IProcedure procedure, final int maxBatchSize) throws ConnectorException {

        if (isAdminModel(procedure)) {
            redirection = new SysAdminProcedureExecution(metadata, connection.getSysAdminobjectSource(), environment);
        } else {
            redirection = new MetadataProcedureExecution(metadata, connection.getMetadataObjectSource(), translator);
            
        }
        
        redirection.execute(procedure, maxBatchSize);
    }
 
    /**
     * This will determine which system model is being executed against.
     * The SystemAdmin model is the administration model used to access the AdminAPI 
     * @param procedure
     * @return
     * @throws ConnectorException
     * @since 4.3
     */
    private boolean isAdminModel(IProcedure procedure) throws ConnectorException {
 
        MetadataID mID = procedure.getMetadataID();
        
        String fn = mID.getFullName().toLowerCase();
        if (fn.indexOf("systemadmin") >= 0) {//$NON-NLS-1$

            return true;
            
        }

        
        return false;
    }

    /** 
     * @see com.metamatrix.data.api.ProcedureExecution#getOutputValue(com.metamatrix.data.language.IParameter)
     * @since 4.2
     */
    public Object getOutputValue(final IParameter parameter) throws ConnectorException {
        if (redirection != null) {
            return redirection.getOutputValue(parameter);
        }
        return null;
    }

    /** 
     * @see com.metamatrix.data.api.Execution#cancel()
     * @since 4.2
     */
    public void cancel() throws ConnectorException {
        if (redirection != null) {
            redirection.cancel();
        }
    }

    /** 
     * @see com.metamatrix.data.api.Execution#close()
     * @since 4.2
     */
    public void close() throws ConnectorException {
        if (redirection != null) {
            redirection.close();
        }
    }


    /** 
     * @see com.metamatrix.data.api.BatchedExecution#nextBatch()
     * @since 4.2
     */
    public Batch nextBatch() throws ConnectorException {
        if (redirection != null) {
            return redirection.nextBatch();
        }
        return null;
    }
}
