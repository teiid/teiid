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

package com.metamatrix.connector.object;

import java.util.List;

import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ConnectorLogger;
import org.teiid.connector.api.DataNotAvailableException;
import org.teiid.connector.api.ProcedureExecution;
import org.teiid.connector.basic.BasicExecution;
import org.teiid.connector.language.IParameter;
import org.teiid.connector.language.IProcedure;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;

import com.metamatrix.connector.object.extension.IObjectCommand;
import com.metamatrix.connector.object.extension.IObjectSource;
import com.metamatrix.connector.object.extension.ISourceTranslator;
import com.metamatrix.connector.object.util.ObjectExecutionHelper;


/** 
 * @since 4.3
 */
public class ObjectProcedureExecution extends BasicExecution implements ProcedureExecution {

    private RuntimeMetadata metadata = null;
    private IObjectSource api;
      
    private ConnectorLogger logger=null;
        
    private List results = null;
    
    private ISourceTranslator translator=null;
    
    private IObjectCommand objectProcedure;
    
    // Derived from properties
    protected boolean trimString;
    private IProcedure procedure;
    private int index;
    
    public ObjectProcedureExecution(IProcedure procedure, IObjectSource sourceapi, 
                                    ISourceTranslator translator, 
                                    RuntimeMetadata metadata,  
                                    ConnectorEnvironment environment) {    
    	this.procedure = procedure;
        this.metadata = metadata;
        this.logger = environment.getLogger();
        this.api = sourceapi;
        this.translator = translator;
        this.metadata = metadata;

        String propStr = environment.getProperties().getProperty(ObjectPropertyNames.TRIM_STRINGS);
        if(propStr != null){
            trimString = Boolean.valueOf(propStr).booleanValue();
        }        

    }
  
    
    
    /** 
     * @see org.teiid.connector.api.ProcedureExecution#execute(org.teiid.connector.language.IProcedure, int)
     * @since 4.2
     */
    public void execute() throws ConnectorException {
        objectProcedure = translator.createObjectCommand(metadata, procedure);
        
        results = api.getObjects(objectProcedure);

        if (results != null) {
        	results = ObjectExecutionHelper.transferResults(results, objectProcedure, trimString, this.translator, null);
        }
    }  

    @Override
    public List next() throws ConnectorException, DataNotAvailableException {
    	if (results == null || index >= results.size()) {
    		return null;
    	}
        return (List)results.get(index++);
    }    
    
    @Override
    public List<?> getOutputParameterValues() throws ConnectorException {
    	throw new UnsupportedOperationException();
    }
   
    public void cancel() {
        //needs to be implemented
    }
    
    public void close() {
        logger = null;
        metadata = null;
        this.results = null;
        translator=null;
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
