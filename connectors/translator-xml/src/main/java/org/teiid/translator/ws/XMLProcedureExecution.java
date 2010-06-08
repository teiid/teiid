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

package org.teiid.translator.ws;

import java.io.IOException;
import java.io.StringReader;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.ws.Dispatch;

import org.teiid.core.types.ClobType;
import org.teiid.language.Argument;
import org.teiid.language.Call;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.TranslatorException;

/**
 * A soap call executor - handles all styles doc/literal, rpc/encoded etc. 
 */
public class XMLProcedureExecution implements ProcedureExecution {

    RuntimeMetadata metadata;
    ExecutionContext context;
    private Call procedure;
    private boolean returnedResult;
    private SQLXML returnValue;
    private Dispatch<Source> dispatch;
    private XMLExecutionFactory executionFactory;
    
    /** 
     * @param env
     */
    public XMLProcedureExecution(Call procedure, RuntimeMetadata metadata, ExecutionContext context, XMLExecutionFactory executionFactory, Dispatch<Source> dispatch) {
        this.metadata = metadata;
        this.context = context;
        this.procedure = procedure;
        this.dispatch = dispatch;
        this.executionFactory = executionFactory;
    }
    
    /** 
     * @see org.teiid.connector.api.ProcedureExecution#execute(org.teiid.connector.language.Call, int)
     */
    public void execute() throws TranslatorException {
        List<Argument> arguments = this.procedure.getArguments();
        
        Object docObject = (Object)arguments.get(0).getArgumentValue().getValue();
        Source source = null;
    	try {
	        if (docObject instanceof SQLXML) {
	        	SQLXML xml = (SQLXML)docObject;
	        	source = xml.getSource(null);
	            if (executionFactory.isLogRequestResponseDocs()) {
	    			LogManager.logDetail(LogConstants.CTX_CONNECTOR, "Request " + xml.getString()); //$NON-NLS-1$
	            }
	        } else if (docObject instanceof Clob) {
	        	Clob clob = (Clob)docObject;
	        	source = new StreamSource(clob.getCharacterStream());
	            if (executionFactory.isLogRequestResponseDocs()) {
	    			try {
						LogManager.logDetail(LogConstants.CTX_CONNECTOR, "Request " + ClobType.getString(clob)); //$NON-NLS-1$
					} catch (IOException e) {
					} 
	            }
	        } else if (docObject instanceof String) {
	        	String string = (String)docObject;
	        	source = new StreamSource(new StringReader(string));
	            if (executionFactory.isLogRequestResponseDocs()) {
	    			LogManager.logDetail(LogConstants.CTX_CONNECTOR, "Request " + string); //$NON-NLS-1$
	            }
	        } else if (docObject != null) {
	        	throw new TranslatorException("Unknown document type, should be one of XML, CLOB, String");
	        }
		} catch (SQLException e) {
			throw new TranslatorException(e);
		}
        
        String endpoint = (String)arguments.get(1).getArgumentValue().getValue();
        String soapAction = (String)arguments.get(2).getArgumentValue().getValue();

		if (soapAction != null) {
			dispatch.getRequestContext().put(Dispatch.SOAPACTION_USE_PROPERTY, Boolean.TRUE);
			dispatch.getRequestContext().put(Dispatch.SOAPACTION_URI_PROPERTY, soapAction);
		}
		
		if (endpoint != null) {
			this.dispatch.getRequestContext().put(Dispatch.ENDPOINT_ADDRESS_PROPERTY, endpoint);
		}
		
		// execute the request
		Source result = this.dispatch.invoke(source);
		this.returnValue = this.executionFactory.convertToXMLType(result);
        if (executionFactory.isLogRequestResponseDocs()) {
        	try {
				LogManager.logDetail(LogConstants.CTX_CONNECTOR, "Response " + this.returnValue.getString()); //$NON-NLS-1$
			} catch (SQLException e) {
			}
        }
    }
    
    @Override
    public List<?> next() throws TranslatorException, DataNotAvailableException {
    	if (!returnedResult) {
    		returnedResult = true;
    		return Arrays.asList(this.returnValue);
    	}
    	return null;
    }  
    
    @Override
    public List<?> getOutputParameterValues() throws TranslatorException {
        return Collections.emptyList();
    }    
    
    public void close() {
    	
    }

    public void cancel() throws TranslatorException {
        // no-op
    }    
}
