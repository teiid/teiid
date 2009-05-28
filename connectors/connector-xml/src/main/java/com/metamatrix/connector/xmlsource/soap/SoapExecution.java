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

package com.metamatrix.connector.xmlsource.soap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.xml.transform.Source;

import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.language.IParameter;
import org.teiid.connector.language.IProcedure;
import org.teiid.connector.language.IParameter.Direction;
import org.teiid.connector.metadata.runtime.MetadataObject;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;

import com.metamatrix.connector.xmlsource.XMLSourceExecution;
import com.metamatrix.connector.xmlsource.XMLSourcePlugin;
import com.metamatrix.connector.xmlsource.soap.ServiceOperation.ExcutionFailedException;
import com.metamatrix.connector.xmlsource.soap.SoapConnection.OperationNotFoundException;


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
    private IProcedure procedure;
    
    /** 
     * @param env
     */
    public SoapExecution(IProcedure procedure, ConnectorEnvironment env, RuntimeMetadata metadata, ExecutionContext context, SoapConnection conn) {
        super(env);
        this.connection = conn;
        this.metadata = metadata;
        this.context = context;
        this.procedure = procedure;
    }

    /** 
     * @see org.teiid.connector.api.ProcedureExecution#execute(org.teiid.connector.language.IProcedure, int)
     */
    public void execute() throws ConnectorException {
        ArrayList argsList = new ArrayList();

        // look for the name of the Service to return in the metadata, "Name in Source" property
        MetadataObject metaObject = procedure.getMetadataObject();
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
            if (param.getDirection() == Direction.IN ) {
                argsList.add(param.getValue());
            }
            else if (param.getDirection() == Direction.INOUT) {
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
            XMLSourcePlugin.logDetail(this.env.getLogger(), "xml_contents", new Object[] {this.returnValue}); //$NON-NLS-1$
        } catch (OperationNotFoundException e) {
            XMLSourcePlugin.logError(this.env.getLogger(), "soap_procedure_not_found", new Object[] {sourceProcedureName}, e); //$NON-NLS-1$            
            throw new ConnectorException(e);
        } catch(ExcutionFailedException e) {
            XMLSourcePlugin.logError(this.env.getLogger(), "soap_procedure_failed_execute", new Object[] {sourceProcedureName}, e); //$NON-NLS-1$            
            throw new ConnectorException(e);
        }
    }
    
    @Override
	public Source getReturnValue() {
		return returnValue;
	}
    
}
