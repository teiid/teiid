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

package com.metamatrix.connector.xmlsource.soap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.xml.transform.Source;

import com.metamatrix.connector.xmlsource.XMLSourceExecution;
import com.metamatrix.connector.xmlsource.XMLSourcePlugin;
import com.metamatrix.connector.xmlsource.soap.ServiceOperation.ExcutionFailedException;
import com.metamatrix.connector.xmlsource.soap.SoapConnection.OperationNotFoundException;
import com.metamatrix.data.api.Batch;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.ExecutionContext;
import com.metamatrix.data.basic.BasicBatch;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.language.IParameter;
import com.metamatrix.data.language.IProcedure;
import com.metamatrix.data.metadata.runtime.MetadataObject;
import com.metamatrix.data.metadata.runtime.RuntimeMetadata;


/**
 * A soap call executor - handles all styles doc/literal, rpc/encoded etc. 
 */
public class SoapExecution extends XMLSourceExecution {

    // Connection object.
    SoapConnection connection;
    Map outputValues = null; 
    Source returnValue = null;   
    RuntimeMetadata metadata = null;
    ExecutionContext context;
    
    /** 
     * @param env
     */
    public SoapExecution(ConnectorEnvironment env, RuntimeMetadata metadata, ExecutionContext context, SoapConnection conn) {
        super(env);
        this.connection = conn;
        this.metadata = metadata;
        this.context = context;
    }

    /** 
     * @see com.metamatrix.data.api.ProcedureExecution#execute(com.metamatrix.data.language.IProcedure, int)
     */
    public void execute(IProcedure procedure, int maxBatchSize) throws ConnectorException {
        ArrayList argsList = new ArrayList();

        // look for the name of the Service to return in the metadata, "Name in Source" property
        MetadataObject metaObject = this.metadata.getObject(procedure.getMetadataID());
        String sourceProcedureName = metaObject.getNameInSource();
        
        // if the source procedure name is not supplied then throw an exception
        if (sourceProcedureName == null || sourceProcedureName.length() == 0) {
            String msg = XMLSourcePlugin.Util.getString("source_name_not_supplied", new Object[] {procedure.getProcedureName()}); //$NON-NLS-1$
            XMLSourcePlugin.logError(this.env.getLogger(), msg); 
            throw new ConnectorException(msg);            
        }
        
        XMLSourcePlugin.logInfo(this.env.getLogger(), "exec_soap_procedure", new Object[] {procedure.getProcedureName(), sourceProcedureName}); //$NON-NLS-1$
        
        // extract all the input parameters to send to the service
        for (Iterator i = procedure.getParameters().iterator(); i.hasNext();) {
            IParameter param = (IParameter)i.next();
            if (param.getDirection() == IParameter.IN ) {
                argsList.add(param.getValue());
            }
            else if (param.getDirection() == IParameter.INOUT) {
                argsList.add(param.getValue());
            }         
        }
                
        // convert lists to arrays
        Object[] args = argsList.toArray(new Object[argsList.size()]);
                
        try {
            ServiceOperation operation = this.connection.findOperation(sourceProcedureName);
             
            XMLSourcePlugin.logDetail(this.env.getLogger(), "service_execute", new Object[] {operation.name}); //$NON-NLS-1$
            for (int i = 0; i < args.length; i++) {
                XMLSourcePlugin.logDetail(this.env.getLogger(), "service_params", new Object[] {args[i]}); //$NON-NLS-1$
            }
            
            this.outputValues = new HashMap();
            this.returnValue = operation.execute(args, outputValues);
            this.context.keepExecutionAlive(true);
            XMLSourcePlugin.logDetail(this.env.getLogger(), "xml_contents", new Object[] {this.returnValue}); //$NON-NLS-1$
        } catch (OperationNotFoundException e) {
            XMLSourcePlugin.logError(this.env.getLogger(), "soap_procedure_not_found", new Object[] {sourceProcedureName}, e); //$NON-NLS-1$            
            throw new ConnectorException(e);
        } catch(ExcutionFailedException e) {
            XMLSourcePlugin.logError(this.env.getLogger(), "soap_procedure_failed_execute", new Object[] {sourceProcedureName}, e); //$NON-NLS-1$            
            throw new ConnectorException(e);
        }
    }
    
    /** 
     * @see com.metamatrix.connector.xmlsource.XMLSourceExecution#nextBatch()
     */
    public Batch nextBatch() throws ConnectorException {
        Batch b = new BasicBatch();
        List row = new ArrayList();
        if (this.returnValue != null) {
        	this.context.keepExecutionAlive(true);
            row.add(convertToXMLType(this.returnValue));   
        }
        else {
            row.add(this.returnValue);
        }
        b.addRow(row);
        b.setLast();
        return b;
    }
    
    /** 
     * @see com.metamatrix.data.api.ProcedureExecution#getOutputValue(com.metamatrix.data.language.IParameter)
     */
    public Object getOutputValue(IParameter parameter) throws ConnectorException {
        throw new ConnectorException(XMLSourcePlugin.Util.getString("No_outputs_allowed")); //$NON-NLS-1$
    }

    /** 
     * @see com.metamatrix.data.api.Execution#close()
     */
    public void close() throws ConnectorException {
        // no-op        
    }

    /** 
     * @see com.metamatrix.data.api.Execution#cancel()
     */
    public void cancel() throws ConnectorException {
        // no-op
    }
}
