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

package org.teiid.translator.xml;

import java.io.StringReader;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.Arrays;
import java.util.List;

import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.ws.Dispatch;

import org.teiid.language.Argument;
import org.teiid.language.Call;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.TranslatorException;

/**
 * A soap call executor - handles all styles doc/literal, rpc/encoded etc. 
 */
public class XMLProcedureExecution implements ProcedureExecution {

    RuntimeMetadata metadata = null;
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
        
		AbstractMetadataRecord metaObject = procedure.getMetadataObject();
		String procedureName =  metaObject.getNameInSource();    	
        if (procedureName == null || procedureName.length() == 0) {
            throw new TranslatorException(XMLPlugin.getString("source_name_not_supplied"));  //$NON-NLS-1$     
        }
		
		// execute the request
		Source result;
		try {
			result = this.dispatch.invoke(buildRequest(procedure.getArguments()));
		} catch (SQLException e1) {
			throw new TranslatorException(e1);
		}
		this.returnValue = this.executionFactory.convertToXMLType(result);
        if (executionFactory.isLogRequestResponseDocs()) {
        	try {
				LogManager.logDetail(LogConstants.CTX_CONNECTOR, this.returnValue.getString());
			} catch (SQLException e) {
			}
        }
		
    }
    
    Source buildRequest(List<Argument> args) throws SQLException, TranslatorException{
    	if (args.size() != 1) {
    		throw new TranslatorException("Expected a single argument to the procedure execution");  //$NON-NLS-1$
    	}
    	Argument arg = args.get(0);
    	Object value = arg.getArgumentValue().getValue();
    	if (value instanceof SQLXML) {
    		return new StreamSource(((SQLXML)value).getCharacterStream());
    	} else if (value instanceof Clob) {
    		return new StreamSource(((Clob)value).getCharacterStream());    		
    	} else if (value != null) {
    		return new StreamSource(new StringReader(value.toString()));
    	} else {
    		//TODO: work around for JBoss native
    		return new StreamSource(new StringReader("<none/>")); //$NON-NLS-1$
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
        throw new TranslatorException(XMLPlugin.getString("No_outputs_allowed")); //$NON-NLS-1$
    }    
    
    public void close() {
        // no-op
    }

    public void cancel() throws TranslatorException {
        // no-op
    }    
}
