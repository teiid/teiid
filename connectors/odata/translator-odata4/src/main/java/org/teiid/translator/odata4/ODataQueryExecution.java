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

import java.io.IOException;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.language.QueryExpression;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.WSConnection;
import org.teiid.translator.odata4.ODataMetadataProcessor.ODataType;
import org.teiid.translator.ws.BinaryWSProcedureExecution;

public class ODataQueryExecution extends BaseQueryExecution implements ResultSetExecution {
    
    private ODataSQLVisitor visitor;
    private int countResponse = -1;
    private Class<?>[] expectedColumnTypes;
    private ODataResponse response;
    
    public ODataQueryExecution(ODataExecutionFactory translator,
            QueryExpression command, ExecutionContext executionContext,
            RuntimeMetadata metadata, WSConnection connection) throws TranslatorException {
        super(translator, executionContext, metadata, connection);
        
        this.visitor = new ODataSQLVisitor(this.translator, metadata);
        this.visitor.visitNode(command);
        if (!this.visitor.exceptions.isEmpty()) {
            throw visitor.exceptions.get(0);
        }
        
        this.expectedColumnTypes = command.getColumnTypes();
    }

    @Override
    public void execute() throws TranslatorException {
        final String URI = this.visitor.buildURL("");

        if (this.visitor.isCount()) {
            Map<String, List<String>> headers = new TreeMap<String, List<String>>();
            headers.put("Accept", Arrays.asList("text/plain"));  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            
            BinaryWSProcedureExecution execution = invokeHTTP("GET", URI, null, headers); //$NON-NLS-1$
            if (execution.getResponseCode() != HttpStatusCode.OK.getStatusCode()) {
                throw buildError(execution);
            }
            
            Blob blob = (Blob)execution.getOutputParameterValues().get(0);
            try {
                this.countResponse = Integer.parseInt(ObjectConverterUtil.convertToString(blob.getBinaryStream()));
            } catch (IOException e) {
                throw new TranslatorException(e);
            } catch (SQLException e) {
                throw new TranslatorException(e);
            }            
        } else {
            InputStream payload = executeQuery(
                    "GET", URI, null, null, ////$NON-NLS-1$ 
                    new HttpStatusCode[] {
                            HttpStatusCode.OK,
                            HttpStatusCode.NO_CONTENT,                    
                            HttpStatusCode.NOT_FOUND                            
                    }); 
            this.response = new ODataResponse(payload,
                    ODataType.ENTITY_COLLECTION, this.visitor.getODataQuery().getRootDocument()) {
                @Override
                public InputStream nextBatch(java.net.URI uri) throws TranslatorException {
                    return executeSkipToken(uri, URI.toString(), 
                            new HttpStatusCode[] {
                        HttpStatusCode.OK,
                        HttpStatusCode.NO_CONTENT,                    
                        HttpStatusCode.NOT_FOUND                            
                    }); 
                }
            };                      
        }
    }
    
    @Override
    public List<?> next() throws TranslatorException, DataNotAvailableException {
        
        if (visitor.isCount() && this.countResponse != -1) {
            int count = this.countResponse;
            this.countResponse = -1;
            return Arrays.asList(count);
        }

        if (this.response != null) {
            Map<String, Object> row = this.response.getNext();
            if (row != null) {
                return buildRow(visitor.getODataQuery().getRootDocument().getTable(), 
                        visitor.getProjectedColumns(), 
                        this.expectedColumnTypes, row);
            }
        }
        return null;
    }    
    
    @Override
    public void close() {
    }

    @Override
    public void cancel() throws TranslatorException {
    }    
}
