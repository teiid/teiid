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

package org.teiid.connector.xml.soap;

import java.sql.SQLXML;
import java.util.List;

import javax.xml.transform.Source;

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.xml.XMLSourcePlugin;
import org.teiid.connector.xml.soap.ServiceOperation.ExcutionFailedException;
import org.teiid.connector.xml.soap.SoapService.OperationNotFoundException;



/**
 * A soap call executor - handles all styles doc/literal, rpc/encoded etc. 
 */
public abstract class SoapDocLiteralRequest {

    // Connection object.
    private SoapManagedConnectionFactory env;
    private SoapService service;
    
    /** 
     * @param env
     */
    public SoapDocLiteralRequest(SoapManagedConnectionFactory env, SoapService service) {
        this.env = env;
        this.service = service;
    }

    protected abstract String getProcedureName();
    
    protected abstract List getInputParameters();
    
    protected abstract SecurityToken getSecurityToken();
    
    /** 
     * @see org.teiid.connector.api.ProcedureExecution#execute(org.teiid.connector.language.IProcedure, int)
     */
    public SQLXML execute() throws ConnectorException {
        

        String sourceProcedureName = getProcedureName();
        
        // if the source procedure name is not supplied then throw an exception
        if (sourceProcedureName == null || sourceProcedureName.length() == 0) {
            String msg = XMLSourcePlugin.Util.getString("source_name_not_supplied"); //$NON-NLS-1$
            throw new ConnectorException(msg);            
        }
        
        this.env.getLogger().logDetail(XMLSourcePlugin.Util.getString("exec_soap_procedure", new Object[] {sourceProcedureName})); //$NON-NLS-1$
        
        // extract all the input parameters to send to the service
        List argsList = getInputParameters();
                
        // convert lists to arrays
        Object[] args = argsList.toArray(new Object[argsList.size()]);
                
        try {
            ServiceOperation operation = this.service.findOperation(sourceProcedureName);
             
            this.env.getLogger().logDetail(XMLSourcePlugin.Util.getString("service_execute", new Object[] {operation.getName()})); //$NON-NLS-1$
            for (int i = 0; i < args.length; i++) {
            	this.env.getLogger().logDetail(XMLSourcePlugin.Util.getString( "service_params", new Object[] {args[i]})); //$NON-NLS-1$
            }
            
            Source returnValue = operation.execute(args, getSecurityToken());
            
            this.env.getLogger().logDetail(XMLSourcePlugin.Util.getString("xml_contents", new Object[] {returnValue})); //$NON-NLS-1$
        
            return convertToXMLType(returnValue);

        } catch (OperationNotFoundException e) {
            throw new ConnectorException(e);
        } catch (ExcutionFailedException e) {
        	throw new ConnectorException(e);
        }
    }
    
    protected SQLXML convertToXMLType(Source value) {
    	return (SQLXML)env.getTypeFacility().convertToRuntimeType(value);
    }
    
    public List<?> getOutputParameterValues() throws ConnectorException {
        throw new ConnectorException(XMLSourcePlugin.Util.getString("No_outputs_allowed")); //$NON-NLS-1$
    }    
}
