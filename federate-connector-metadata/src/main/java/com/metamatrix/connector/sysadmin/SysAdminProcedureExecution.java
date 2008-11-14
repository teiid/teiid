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

package com.metamatrix.connector.sysadmin;

import java.util.Collections;
import java.util.List;

import com.metamatrix.connector.sysadmin.extension.IObjectCommand;
import com.metamatrix.connector.sysadmin.extension.ISourceTranslator;
import com.metamatrix.connector.sysadmin.util.SysAdminExecutionHelper;
import com.metamatrix.data.api.Batch;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.ConnectorLogger;
import com.metamatrix.data.api.ProcedureExecution;
import com.metamatrix.data.basic.BasicBatch;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.language.IParameter;
import com.metamatrix.data.language.IProcedure;
import com.metamatrix.data.metadata.runtime.RuntimeMetadata;


/** 
 * @since 4.3
 */
public class SysAdminProcedureExecution implements ProcedureExecution {

    private RuntimeMetadata metadata = null;
    private SysAdminObjectSource api;
      
    private ConnectorLogger logger=null;
        
    private List results = null;
    
    private ISourceTranslator translator=null;
    private int maxBatchSize;
    
    private IObjectCommand objectProcedure;
    
    // Derived from properties
    protected boolean trimString;

    public SysAdminProcedureExecution(final RuntimeMetadata metadata, final Object objectSource, ConnectorEnvironment environment) {

        this.metadata = metadata;
        this.logger = environment.getLogger();
        this.api = (SysAdminObjectSource) objectSource;
        this.metadata = metadata;

        String propStr = environment.getProperties().getProperty(SysAdminPropertyNames.TRIM_STRINGS, "false");//$NON-NLS-1$
        if(propStr != null){
            trimString = Boolean.valueOf(propStr).booleanValue();
        }     
        
        this.translator = api.getSourceTranslator();

     }
  
    
    
    /** 
     * @see com.metamatrix.data.api.ProcedureExecution#execute(com.metamatrix.data.language.IProcedure, int)
     * @since 4.2
     */
    public void execute(IProcedure procedure,
                        int maxBatchSize) throws ConnectorException {
        this.maxBatchSize = maxBatchSize;
        objectProcedure = translator.createObjectCommand(metadata, procedure);
        
        results = api.getObjects(objectProcedure);

    }  
    
   
    public Batch nextBatch() throws ConnectorException {
        if (results != null && results.size() > 0) {
            return SysAdminExecutionHelper.createBatch(results, objectProcedure, maxBatchSize, trimString, this.translator, null);
        }
       
        Batch result = new BasicBatch(Collections.EMPTY_LIST);
        result.setLast();
        
        return result;
    }    
    
   
    /** 
     * @see com.metamatrix.data.api.ProcedureExecution#getOutputValue(com.metamatrix.data.language.IParameter)
     * @since 4.2
     */
    public Object getOutputValue(IParameter parameter) throws ConnectorException {
        return null;
    }
    
    public void cancel() {
        //needs to be implemented
    }
    
    public void close() {
        metadata = null;
        this.api = null;
        this.results = null;
        translator=null;
        objectProcedure = null;
        logger = null;
        
    }    
    
    
    protected void logDetail(String msg) {
        if (logger != null) {
            logger.logDetail(msg); 
        }
    
    }   
    
    protected void logError(String msg, Throwable e) {
        if (logger != null) {
            logger.logError(msg, e); 
        }
    
    }  



}
