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
import java.sql.Blob;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.olingo.client.api.serialization.ODataDeserializerException;
import org.apache.olingo.client.core.serialization.JsonDeserializer;
import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.format.AcceptType;
import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.teiid.GeneratedKeys;
import org.teiid.language.Command;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;
import org.teiid.translator.WSConnection;
import org.teiid.translator.odata4.ODataUpdateVisitor.OperationType;
import org.teiid.translator.ws.BinaryWSProcedureExecution;

public class ODataUpdateExecution extends BaseQueryExecution implements UpdateExecution {
    private ODataUpdateVisitor visitor;
    private Entity createdEntity;
    private AtomicInteger updateCount;
    
    public ODataUpdateExecution(Command command, ODataExecutionFactory translator,
            ExecutionContext executionContext, RuntimeMetadata metadata,
            WSConnection connection) throws TranslatorException {
        super(translator, executionContext, metadata, connection);
        
        this.visitor = new ODataUpdateVisitor(translator, metadata);
        this.visitor.visitNode(command);
        
        if (!this.visitor.exceptions.isEmpty()) {
            throw this.visitor.exceptions.get(0);
        }
    }

    @Override
    public void close() {
    }

    @Override
    public void cancel() throws TranslatorException {
    }

    @Override
    public void execute() throws TranslatorException {
        ODataUpdateQuery odataQuery = this.visitor.getODataQuery();
        if (this.visitor.getOperationType() == OperationType.DELETE) { //$NON-NLS-1$
            handleDelete(odataQuery);
        }
        else if(this.visitor.getOperationType() == OperationType.UPDATE) { //$NON-NLS-1$
            handleUpdate(odataQuery);
        }
        else if (this.visitor.getOperationType() == OperationType.INSERT) { //$NON-NLS-1$
            handleInsert(odataQuery);
        }
    }

    private void handleUpdate(ODataUpdateQuery odataQuery) throws TranslatorException {
        String uri = odataQuery.buildUpdateSelectionURL("");
        BinaryWSProcedureExecution execution = invokeHTTP("GET",uri,null,getDefaultHeaders());
        if (execution.getResponseCode() == HttpStatusCode.OK.getStatusCode()) {
            // TODO: This loop needs to be executed in batch mode, to be transactionally safe
            while (true) {
                List<?> row = execution.next();
                if (row == null) {
                    break;
                }
                String updateUri = odataQuery.buildUpdateURL("", row);
                executeQuery(odataQuery.getMethod(), updateUri,
                        odataQuery.getPayload(), null,
                        new HttpStatusCode[] { HttpStatusCode.NO_CONTENT });
                this.updateCount.incrementAndGet();
            }
        } else {
            throw buildError(execution);
        }
    }
    
    private void handleDelete(ODataUpdateQuery odataQuery) throws TranslatorException {
        String uri = odataQuery.buildUpdateSelectionURL("");
        BinaryWSProcedureExecution execution = invokeHTTP("GET",uri,null,getDefaultHeaders());
        if (execution.getResponseCode() == HttpStatusCode.OK.getStatusCode()) {
            // TODO: This loop needs to be executed in batch mode, to be transactionally safe
            while (true) {
                List<?> row = execution.next();
                if (row == null) {
                    break;
                }
                String updateUri = odataQuery.buildUpdateURL("", row);
                executeQuery("DELETE", updateUri,
                        odataQuery.getPayload(), null,
                        new HttpStatusCode[] { HttpStatusCode.NO_CONTENT });
                this.updateCount.incrementAndGet();
            }
        } else {
            throw buildError(execution);
        }
    }    

    private void handleInsert(ODataUpdateQuery odataQuery)
            throws TranslatorException {
        try {
            Map<String, List<String>> headers = new HashMap<String, List<String>>();
            headers.put("Accept", Arrays.asList( //$NON-NLS-1$
                    AcceptType.fromContentType(ContentType.JSON).toString())); 
            headers.put("Content-Type", Arrays.asList( //$NON-NLS-1$
                    ContentType.APPLICATION_JSON.toContentTypeString()));  
            headers.put("Prefer", Arrays.asList("return=representation")); //$NON-NLS-1$ 
              
            String uri = odataQuery.buildInsertURL("");
            InputStream response = null;
            BinaryWSProcedureExecution execution = invokeHTTP(odataQuery.getMethod(), 
                    uri, 
                    odataQuery.getPayload(), 
                    headers);
            
            // 201 - the created entity returned
            if (HttpStatusCode.CREATED.getStatusCode() == execution.getResponseCode()) {
                Blob blob = (Blob)execution.getOutputParameterValues().get(0);
                response = blob.getBinaryStream();
            } else if (HttpStatusCode.NO_CONTENT.getStatusCode() == execution.getResponseCode()) {
                // get Location header and get content
                String entityUri = (String)execution.getResponseHeader("Location");
                if (entityUri != null) {
                    // in the cases of property update there will be no Location header
                    response = executeQuery("GET", entityUri, null, 
                            null, new HttpStatusCode[] {HttpStatusCode.OK});
                }
            } else {
                throw buildError(execution);
            }
            
            if (response != null){
                JsonDeserializer serializer = new JsonDeserializer(false);
                this.createdEntity = serializer.toEntity(response).getPayload();
            }
        } catch (ODataDeserializerException e) {
            throw new TranslatorException(e);
        } catch (SQLException e) {
            throw new TranslatorException(e);
        }
    }

    
    @Override
    public int[] getUpdateCounts() throws DataNotAvailableException, TranslatorException {
        if (this.visitor.getOperationType() == OperationType.DELETE) { //$NON-NLS-1$
            return new int[]{this.updateCount.get()};
        }
        else if(this.visitor.getOperationType() == OperationType.UPDATE) { //$NON-NLS-1$
            return new int[]{this.updateCount.get()};
        }
        else if (this.visitor.getOperationType() == OperationType.INSERT) { //$NON-NLS-1$
            if (this.createdEntity != null) {
                if (this.executionContext.getCommandContext().isReturnAutoGeneratedKeys()) {
                    addAutoGeneretedKeys(this.visitor.getODataQuery().getRootDocument().getTable(), 
                            this.createdEntity);
                }                                
            }
            return new int[]{1};
        }
        return new int[] {0};
    }
    
    private void addAutoGeneretedKeys(Table table, Entity entity) throws TranslatorException {
        int cols = table.getPrimaryKey().getColumns().size();
        Class<?>[] columnDataTypes = new Class<?>[cols];
        String[] columnNames = new String[cols];
        //this is typically expected to be an int/long, but we'll be general here.  we may eventual need the type logic off of the metadata importer
        for (int i = 0; i < cols; i++) {
            columnDataTypes[i] = table.getPrimaryKey().getColumns().get(i).getJavaType();
            columnNames[i] = table.getPrimaryKey().getColumns().get(i).getName();
        }
        GeneratedKeys generatedKeys = this.executionContext.getCommandContext().returnGeneratedKeys(columnNames, columnDataTypes);
        List<Object> vals = new ArrayList<Object>(columnDataTypes.length);
        for (int i = 0; i < columnDataTypes.length; i++) {
            Property prop = entity.getProperty(columnNames[i]);
            Object value = ODataTypeManager.convertTeiidInput(prop.getValue(), columnDataTypes[i]);
            vals.add(value); 
        }
        generatedKeys.addKey(vals);
    }
}
