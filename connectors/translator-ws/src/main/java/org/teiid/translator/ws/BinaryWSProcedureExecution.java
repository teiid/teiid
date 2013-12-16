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
import java.io.InputStream;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.activation.DataSource;
import javax.xml.ws.Dispatch;
import javax.xml.ws.Service.Mode;
import javax.xml.ws.WebServiceException;
import javax.xml.ws.handler.MessageContext;
import javax.xml.ws.http.HTTPBinding;

import org.teiid.connector.DataPlugin;
import org.teiid.core.types.BlobImpl;
import org.teiid.core.types.BlobType;
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

	public static final class StreamingBlob extends BlobImpl {
		InputStream is;

		public StreamingBlob(InputStream is) {
			this.is = is;
		}

		@Override
		public InputStream getBinaryStream() throws SQLException {
			if (this.is == null) {
				throw new SQLException(DataPlugin.Util.gs(DataPlugin.Event.TEIID60019));
			}
			InputStream result = this.is;
			this.is = null;
			return result;
		}
	}

	RuntimeMetadata metadata;
    ExecutionContext context;
    private Call procedure;
    private DataSource returnValue;
    private WSConnection conn;
    WSExecutionFactory executionFactory;
    Map<String, List<String>> customHeaders = new HashMap<String, List<String>>();
    Map<String, Object> responseContext = Collections.emptyMap();
    int responseCode = 200;
    private boolean useResponseContext;
    private boolean alwaysAllowPayloads;

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

    public void setUseResponseContext(boolean useResponseContext) {
		this.useResponseContext = useResponseContext;
	}

    public void setAlwaysAllowPayloads(boolean alwaysAllowPayloads) {
		this.alwaysAllowPayloads = alwaysAllowPayloads;
	}

    public void execute() throws TranslatorException {
        List<Argument> arguments = this.procedure.getArguments();

        String method = (String)arguments.get(0).getArgumentValue().getValue();
        Object payload = arguments.get(1).getArgumentValue().getValue();
        String endpoint = (String)arguments.get(2).getArgumentValue().getValue();
    	try {
	        Dispatch<DataSource> dispatch = this.conn.createDispatch(HTTPBinding.HTTP_BINDING, endpoint, DataSource.class, Mode.MESSAGE);

			if (method == null) {
				method = "POST"; //$NON-NLS-1$
			}

			dispatch.getRequestContext().put(MessageContext.HTTP_REQUEST_METHOD, method);
			if (!this.alwaysAllowPayloads && payload != null && !"POST".equalsIgnoreCase(method)) { //$NON-NLS-1$
				throw new WebServiceException(WSExecutionFactory.UTIL.getString("http_usage_error")); //$NON-NLS-1$
			}

	        Map<String, List<String>> httpHeaders = (Map<String, List<String>>)dispatch.getRequestContext().get(MessageContext.HTTP_REQUEST_HEADERS);
	        for (String key:this.customHeaders.keySet()) {
	        	httpHeaders.put(key, this.customHeaders.get(key));
	        }
	        dispatch.getRequestContext().put(MessageContext.HTTP_REQUEST_HEADERS, httpHeaders);

			DataSource ds = null;
			if (payload instanceof String) {
				ds = new InputStreamFactory.ClobInputStreamFactory(new ClobImpl((String)payload));
			} else if (payload instanceof SQLXML) {
				ds = new InputStreamFactory.SQLXMLInputStreamFactory((SQLXML)payload);
			} else if (payload instanceof Clob) {
				ds = new InputStreamFactory.ClobInputStreamFactory((Clob)payload);
			} else if (payload instanceof Blob) {
				ds = new InputStreamFactory.BlobInputStreamFactory((Blob)payload);
			}

			this.returnValue = dispatch.invoke(ds);
			
			Map<String, Object> rc = dispatch.getResponseContext();
			this.responseCode = (Integer)rc.get(WSConnection.STATUS_CODE);
			if (this.useResponseContext) {
				//it's presumed that the caller will handle the response codes
				this.responseContext = rc;
			} else {
				//TODO: may need to add logic around some 200/300 codes - cxf should at least be logging this
				if (this.responseCode >= 400) {
		    		String message = conn.getStatusMessage(this.responseCode);
		    		throw new TranslatorException(WSExecutionFactory.Event.TEIID15005, WSExecutionFactory.UTIL.gs(WSExecutionFactory.Event.TEIID15005, this.responseCode, message));
				}
			}
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
    	Object result = this.returnValue;
		if (this.returnValue != null && this.procedure.getArguments().size() > 4 && Boolean.TRUE.equals(this.procedure.getArguments().get(3).getArgumentValue().getValue())) {
			try {
				result = new BlobType(new StreamingBlob(this.returnValue.getInputStream()));
			} catch (IOException e) {
				throw new TranslatorException(e);
			}
		}
        return Arrays.asList(result, this.returnValue.getContentType());
    }

    public void close() {

    }

    public void cancel() throws TranslatorException {
        // no-op
    }

    public void addHeader(String name, List<String> value) {
    	this.customHeaders.put(name, value);
    }

    public Object getResponseHeader(String name){
    	return this.responseContext.get(name);
    }

    public int getResponseCode() {
    	return this.responseCode;
    }

}
