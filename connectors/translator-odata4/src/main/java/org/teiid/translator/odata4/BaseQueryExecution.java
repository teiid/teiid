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
package org.teiid.translator.odata4;

import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.Charsets;
import org.apache.olingo.client.core.serialization.JsonDeserializer;
import org.apache.olingo.commons.api.ex.ODataError;
import org.apache.olingo.commons.api.format.AcceptType;
import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.teiid.language.Argument;
import org.teiid.language.Argument.Direction;
import org.teiid.language.Call;
import org.teiid.language.Literal;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.Column;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.WSConnection;
import org.teiid.translator.ws.BinaryWSProcedureExecution;

public class BaseQueryExecution {
    protected WSConnection connection;
    protected ODataExecutionFactory translator;
    protected RuntimeMetadata metadata;
    protected ExecutionContext executionContext;

    public BaseQueryExecution(ODataExecutionFactory translator,
            ExecutionContext executionContext, RuntimeMetadata metadata,
            WSConnection connection) {
        this.metadata = metadata;
        this.executionContext = executionContext;
        this.translator = translator;
        this.connection = connection;
    }

    protected InputStream executeQuery(String method,
            String uri, String payload, String eTag, HttpStatusCode[] expectedStatus)
            throws TranslatorException {
        
        Map<String, List<String>> headers = getDefaultHeaders();
        if (eTag != null) {
            headers.put("If-Match", Arrays.asList(eTag)); //$NON-NLS-1$
        }

        if (payload != null) {
            headers.put("Content-Type", Arrays.asList(
                    ContentType.APPLICATION_JSON.toContentTypeString())); //$NON-NLS-1$ //$NON-NLS-2$
        }

        BinaryWSProcedureExecution execution;
        try {
            execution = invokeHTTP(method, uri, payload, headers);
            for (HttpStatusCode status:expectedStatus) {
                if (status.getStatusCode() == execution.getResponseCode()) {
                    if (execution.getResponseCode() != HttpStatusCode.NO_CONTENT.getStatusCode() 
                            && execution.getResponseCode() != HttpStatusCode.NOT_FOUND.getStatusCode()) {
                        Blob blob = (Blob)execution.getOutputParameterValues().get(0);
                        return blob.getBinaryStream();
                    }
                    // this is success with no-data
                    return null;
                }
            }
        } catch (SQLException e) {
            throw new TranslatorException(e);
        }
        // throw an error
        throw buildError(execution);
        
    }
    
    String getHeader(BinaryWSProcedureExecution execution, String header) {
        Object value = execution.getResponseHeader(header);
        if (value instanceof List) {
            return (String)((List<?>)value).get(0);
        }
        return (String)value;
    }    

    protected TranslatorException buildError(BinaryWSProcedureExecution execution) {
        // do some error handling
        try {
            Blob blob = (Blob)execution.getOutputParameterValues().get(0);
            if (blob != null) {
                JsonDeserializer parser = new JsonDeserializer(false);
                ODataError error = parser.toError(blob.getBinaryStream());            
                return new TranslatorException(ODataPlugin.Util.gs(
                        ODataPlugin.Event.TEIID17013, execution.getResponseCode(),
                        error.getCode(), error.getMessage(), error.getInnerError()));
            } else {
                return new TranslatorException(ODataPlugin.Util.gs(
                        ODataPlugin.Event.TEIID17031));
            }
        }
        catch (Throwable t) {
            return new TranslatorException(t);
        }
    }

    protected BinaryWSProcedureExecution invokeHTTP(String method,
            String uri, String payload, Map<String, List<String>> headers)
            throws TranslatorException {

        if (LogManager.isMessageToBeRecorded(LogConstants.CTX_ODATA, MessageLevel.DETAIL)) {
            try {
                LogManager.logDetail(LogConstants.CTX_ODATA,
                        "Source-URL=", URLDecoder.decode(uri, "UTF-8")); //$NON-NLS-1$ //$NON-NLS-2$
            } catch (UnsupportedEncodingException e) {
            }
        }

        List<Argument> parameters = new ArrayList<Argument>();
        parameters.add(new Argument(Direction.IN, 
                new Literal(method, TypeFacility.RUNTIME_TYPES.STRING), null));
        parameters.add(new Argument(Direction.IN, 
                new Literal(payload, TypeFacility.RUNTIME_TYPES.STRING), null));
        parameters.add(new Argument(Direction.IN, 
                new Literal(uri, TypeFacility.RUNTIME_TYPES.STRING), null));
        parameters.add(new Argument(Direction.IN, 
                new Literal(true, TypeFacility.RUNTIME_TYPES.BOOLEAN), null));
        //the engine currently always associates out params at resolve time even if the 
        // values are not directly read by the call
        parameters.add(new Argument(Direction.OUT, TypeFacility.RUNTIME_TYPES.STRING, null));
        
        Call call = this.translator.getLanguageFactory().createCall(
                ODataExecutionFactory.INVOKE_HTTP, parameters, null);

        BinaryWSProcedureExecution execution = new BinaryWSProcedureExecution(
                call, this.metadata, this.executionContext, null,
                this.connection);
        execution.setUseResponseContext(true);
        execution.setCustomHeaders(headers);
        execution.execute();
        return execution;
    }

    protected Map<String, List<String>> getDefaultHeaders() {
        Map<String, List<String>> headers = new HashMap<String, List<String>>();
        headers.put("Accept", Arrays.asList(ContentType.JSON.toContentTypeString())); //$NON-NLS-1$
        headers.put("Content-Type", Arrays.asList(
                ContentType.APPLICATION_JSON.toContentTypeString())); //$NON-NLS-1$ //$NON-NLS-2$
        if (this.executionContext != null) {
            headers.put("Prefer", Arrays.asList("odata.maxpagesize="+this.executionContext.getBatchSize()));
        }
        return headers;
    }
    
    protected InputStream executeSkipToken(URI nextURL, String baseURL,
            HttpStatusCode[] accepeted) throws TranslatorException {
        String next = nextURL.toString();
        int idx = next.indexOf("$skiptoken="); //$NON-NLS-1$
        
        if (next.toLowerCase().startsWith("http")) { //$NON-NLS-1$
            return executeQuery("GET", nextURL.toString(), null, null, accepeted);

        } else if (idx != -1) {
            String skip = null;
            try {
                skip = next.substring(idx + 11);
                skip = URLDecoder.decode(skip, Charsets.UTF_8.name());
            } catch (UnsupportedEncodingException e) {
                throw new TranslatorException(e);
            }

            String nextUri = baseURL;
            if (baseURL.indexOf('?') == -1) {
                nextUri = baseURL + "?$skiptoken="+skip; //$NON-NLS-1$
            }
            else {
                nextUri = baseURL + "&$skiptoken="+skip; //$NON-NLS-1$
            }
            return executeQuery("GET", nextUri, null, null, accepeted);
        } else {
            throw new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17001, next));
        }        
    }
    
    @SuppressWarnings("unchecked")
    <T extends AbstractMetadataRecord> List<?> buildRow(T record, List<Column> columns,
            Class<?>[] expectedType, Map<String, Object> values) throws TranslatorException {
        List<Object> results = new ArrayList<Object>();
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            T columnParent = (T)column.getParent();            
            String colName = column.getName();
            if (!columnParent.equals(record)) {
                colName = getName(columnParent)+"/"+column.getName();
            }
            Object value = ODataTypeManager.convertTeiidRuntimeType(
                    values.get(colName), ODataMetadataProcessor.getNativeType(column),
                    expectedType[i]);                    
            results.add(value);
        }
        return results;
    }     
    
    public String getName(AbstractMetadataRecord table) {
        if (table.getNameInSource() != null) {
            return table.getNameInSource();
        }
        return table.getName();
    }    
}
