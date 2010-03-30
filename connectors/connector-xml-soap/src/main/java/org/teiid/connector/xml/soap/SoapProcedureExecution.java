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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.DataNotAvailableException;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.api.ProcedureExecution;
import org.teiid.connector.basic.BasicExecution;
import org.teiid.connector.language.Argument;
import org.teiid.connector.language.Call;
import org.teiid.connector.language.Argument.Direction;
import org.teiid.connector.metadata.runtime.AbstractMetadataRecord;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;
import org.teiid.connector.xml.XMLSourcePlugin;

/**
 * A soap call executor - handles all styles doc/literal, rpc/encoded etc. 
 */
public class SoapProcedureExecution extends BasicExecution implements ProcedureExecution {

    // Connection object.
	SoapService service;
    RuntimeMetadata metadata = null;
    ExecutionContext context;
    private Call procedure;
    private SoapManagedConnectionFactory env;
    private boolean returnedResult;
    private SQLXML returnValue;
    private SecurityToken securityToken;
    
    /** 
     * @param env
     */
    public SoapProcedureExecution(Call procedure, SoapManagedConnectionFactory env, RuntimeMetadata metadata, ExecutionContext context, SoapService service, SecurityToken securityToken) {
        this.service = service;
        this.metadata = metadata;
        this.context = context;
        this.procedure = procedure;
        this.env = env;
        this.securityToken = securityToken;
    }
    
    /** 
     * @see org.teiid.connector.api.ProcedureExecution#execute(org.teiid.connector.language.Call, int)
     */
    public void execute() throws ConnectorException {
        
    	SoapDocLiteralRequest request = new SoapDocLiteralRequest(this.env, this.service) {
			
			@Override
		    protected String getProcedureName() {
				AbstractMetadataRecord metaObject = procedure.getMetadataObject();
				return metaObject.getNameInSource();
		    }
			
			@Override
		    protected List getInputParameters() {
		    	ArrayList argsList = new ArrayList();
		        // extract all the input parameters to send to the service
		        for (Iterator i = procedure.getArguments().iterator(); i.hasNext();) {
		            Argument param = (Argument)i.next();
		            if (param.getDirection() == Direction.IN ) {
		                argsList.add(param.getArgumentValue().getValue());
		            }
		            else if (param.getDirection() == Direction.INOUT) {
		                argsList.add(param.getArgumentValue().getValue());
		            }         
		        }    	
		        return argsList;
		    }
			
			@Override
			protected SecurityToken getSecurityToken() {
				return securityToken;
			}
		};
		
		// execute the request
		this.returnValue = request.execute();
    }
    
    @Override
    public List<?> next() throws ConnectorException, DataNotAvailableException {
    	if (!returnedResult) {
    		returnedResult = true;
    		return Arrays.asList(this.returnValue);
    	}
    	return null;
    }  
    
    @Override
    public List<?> getOutputParameterValues() throws ConnectorException {
        throw new ConnectorException(XMLSourcePlugin.Util.getString("No_outputs_allowed")); //$NON-NLS-1$
    }    
    
    public void close() throws ConnectorException {
        // no-op
    }

    public void cancel() throws ConnectorException {
        // no-op
    }    
}
