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

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.api.ProcedureExecution;
import org.teiid.connector.api.ResultSetExecution;
import org.teiid.connector.basic.BasicConnection;
import org.teiid.connector.basic.BasicManagedConnectionFactory;
import org.teiid.connector.language.Call;
import org.teiid.connector.language.Select;
import org.teiid.connector.language.QueryExpression;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;

import com.metamatrix.connector.xml.IQueryPreprocessor;



/** 
 * A SOAP based connection object
 */
public class SoapConnection extends BasicConnection {    
    // instance variables
    private boolean connected = false;       
    private SoapService service = null; // wsdl service
    private SoapManagedConnectionFactory config;
    private SecurityToken securityToken;
    
    /** 
     * @param env
     * @throws ConnectorException
     */
    public SoapConnection(SoapManagedConnectionFactory env, SoapService service, SecurityToken token) throws ConnectorException {
       this.config = env;
       this.service = service;
       this.securityToken = token;
    }
    
    @Override
    public ProcedureExecution createProcedureExecution(Call command, ExecutionContext executionContext, RuntimeMetadata metadata)
    		throws ConnectorException {
        return new SoapProcedureExecution(command, this.config, metadata, executionContext, this.service, this.securityToken);
    }
    
	@Override
	public ResultSetExecution createResultSetExecution(QueryExpression command, ExecutionContext executionContext, RuntimeMetadata metadata)
			throws ConnectorException {
    	IQueryPreprocessor preProcessor = getQueryPreProcessor();
    	if (preProcessor != null) {
    		command = preProcessor.preprocessQuery((Select)command, metadata, executionContext, this.config);
    		this.config.getLogger().logTrace("XML Connector Framework: executing command: " + command);
    	}
		return new SOAPResultSetExecution((Select)command, metadata, executionContext, this.config);
	}    
	
	private IQueryPreprocessor getQueryPreProcessor() throws ConnectorException {
		String className = this.config.getQueryPreprocessorClass();
		if (className == null) {
			return null;
		}
		return BasicManagedConnectionFactory.getInstance(IQueryPreprocessor.class, className, null, null);
	}	

	public boolean isConnected() {
        return this.connected;
    }

    @Override
	public void close() {
    }          
}
