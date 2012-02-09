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

import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLXML;
import java.util.Arrays;
import java.util.List;

import javax.activation.DataSource;
import javax.xml.ws.Dispatch;
import javax.xml.ws.WebServiceException;
import javax.xml.ws.Service.Mode;
import javax.xml.ws.handler.MessageContext;
import javax.xml.ws.http.HTTPBinding;

import org.teiid.core.types.ClobImpl;
import org.teiid.core.types.InputStreamFactory;
import org.teiid.language.Argument;
import org.teiid.language.Call;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.WSConnection;

/**
 * http handler 
 */
public class BinaryWSProcedureExecution implements ProcedureExecution {
	
	RuntimeMetadata metadata;
    ExecutionContext context;
    private Call procedure;
    private DataSource returnValue;
    private WSConnection conn;
    WSExecutionFactory executionFactory;
    
    /** 
     * @param env
     */
    public BinaryWSProcedureExecution(Call procedure, RuntimeMetadata metadata, ExecutionContext context, WSExecutionFactory executionFactory, WSConnection conn) {
        this.metadata = metadata;
        this.context = context;
        this.procedure = procedure;
        this.conn = conn;
        this.executionFactory = executionFactory;
    }
    
    public void execute() throws TranslatorException {
        List<Argument> arguments = this.procedure.getArguments();
        
        String method = (String)arguments.get(0).getArgumentValue().getValue();
        Object payload = arguments.get(1).getArgumentValue().getValue();
        String endpoint = (String)arguments.get(2).getArgumentValue().getValue();
    	try {
	        Dispatch<DataSource> dispatch = conn.createDispatch(HTTPBinding.HTTP_BINDING, endpoint, DataSource.class, Mode.MESSAGE); 
	
			if (method == null) {
				method = "POST"; //$NON-NLS-1$
			}
			
			dispatch.getRequestContext().put(MessageContext.HTTP_REQUEST_METHOD, method);
			if (payload != null && !"POST".equalsIgnoreCase(method)) { //$NON-NLS-1$
				throw new WebServiceException(WSExecutionFactory.UTIL.getString("http_usage_error")); //$NON-NLS-1$
			}

			DataSource ds = null;
			if (payload instanceof String) {
				ds = new InputStreamFactory.ClobInputStreamFactory(ClobImpl.createClob(((String)payload).toCharArray()));
			} else if (payload instanceof SQLXML) {
				ds = new InputStreamFactory.SQLXMLInputStreamFactory((SQLXML)payload);
			} else if (payload instanceof Clob) {
				ds = new InputStreamFactory.ClobInputStreamFactory((Clob)payload);
			} else if (payload instanceof Blob) {
				ds = new InputStreamFactory.BlobInputStreamFactory((Blob)payload);
			}
			
			returnValue = dispatch.invoke(ds);
		} catch (WebServiceException e) {
			throw new TranslatorException(e);
		} 
    }

    @Override
    public List<?> next() throws TranslatorException, DataNotAvailableException {
    	return null;
    }  
    
    @Override
    public List<?> getOutputParameterValues() throws TranslatorException {
        return Arrays.asList(returnValue, returnValue.getContentType());
    }    
    
    public void close() {
    	
    }

    public void cancel() throws TranslatorException {
        // no-op
    }    
}
