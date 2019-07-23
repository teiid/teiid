/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.teiid.translator.odata4;

import java.io.InputStream;
import java.net.URI;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.olingo.client.api.serialization.ODataDeserializer;
import org.apache.olingo.client.api.serialization.ODataDeserializerException;
import org.apache.olingo.client.core.serialization.AtomDeserializer;
import org.apache.olingo.client.core.serialization.JsonDeserializer;
import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.EntityCollection;
import org.apache.olingo.commons.api.data.Link;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.teiid.GeneratedKeys;
import org.teiid.core.TeiidException;
import org.teiid.language.Command;
import org.teiid.metadata.BaseColumn;
import org.teiid.metadata.Column;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.olingo.common.ODataTypeManager;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;
import org.teiid.translator.odata4.ODataUpdateExecution.EntityCollectionIterator.NotSupportedAccepts;
import org.teiid.translator.odata4.ODataUpdateVisitor.OperationType;
import org.teiid.translator.ws.BinaryWSProcedureExecution;
import org.teiid.translator.ws.WSConnection;

public class ODataUpdateExecution extends BaseQueryExecution implements UpdateExecution {
    private ODataUpdateVisitor visitor;
    private Entity createdEntity;
    private AtomicInteger updateCount = new AtomicInteger();

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
        if (this.visitor.getOperationType() == OperationType.DELETE) {
            EntityCollectionIterator ecv = new JsonEntityCollectionIterator(odataQuery, "DELETE");
            try {
                ecv.performUpdateQuery();
            } catch (NotSupportedAccepts e) {
                ecv = new XMLEntityCollectionIterator(odataQuery, "DELETE");
                try {
                    ecv.performUpdateQuery();
                } catch (NotSupportedAccepts e1) {
                    throw new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17032));
                }
            }
        }
        else if(this.visitor.getOperationType() == OperationType.UPDATE) {
            EntityCollectionIterator ecv = new JsonEntityCollectionIterator(odataQuery, odataQuery.getUpdateMethod());
            try {
                ecv.performUpdateQuery();
            } catch (NotSupportedAccepts e) {
                ecv = new XMLEntityCollectionIterator(odataQuery, odataQuery.getUpdateMethod());
                try {
                    ecv.performUpdateQuery();
                } catch (NotSupportedAccepts e1) {
                    throw new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17032));
                }
            }

        }
        else if (this.visitor.getOperationType() == OperationType.INSERT) {
            handleInsert(odataQuery);
        }
    }


    // TODO: this needs to be submitted as BATCH to be transactionally safe.
    abstract class EntityCollectionIterator {
        private ODataUpdateQuery odataQuery;
        private String method;
        private String uri;

        public EntityCollectionIterator(ODataUpdateQuery odataQuery, String method) throws TranslatorException {
            this.odataQuery = odataQuery;
            this.method = method;
            // fyi, the below is not immutable method
            this.uri = odataQuery.buildUpdateSelectionURL("");
        }

        public void performUpdateQuery() throws TranslatorException, NotSupportedAccepts {
            while (this.uri != null) {
                // this needs to be full metadata, to get to the Entity edit URL, but note Olingo
                // currently does not support as of 4.0.0
                BinaryWSProcedureExecution execution = invokeHTTP("GET",uri,null, headers());
                if (execution.getResponseCode() == HttpStatusCode.OK.getStatusCode()) {
                    EntityCollection entities = null;
                    Blob blob = (Blob)execution.getOutputParameterValues().get(0);
                    try {
                        InputStream response = blob.getBinaryStream();
                        if (response != null){
                            ODataDeserializer serializer = getDeSerializer();
                            entities = serializer.toEntitySet(response).getPayload();
                            URI nextUri = entities.getNext();
                            if (nextUri != null) {
                                this.uri = nextUri.toString();
                            } else {
                                this.uri = null;
                            }
                        }
                    } catch (ODataDeserializerException e) {
                        throw new TranslatorException(e);
                    } catch (SQLException e) {
                        throw new TranslatorException(e);
                    }

                    if (entities != null && !entities.getEntities().isEmpty()) {
                        for (Entity entity:entities.getEntities()) {
                            onEntity(entity, method);
                        }
                    }
                } else {
                    throw new NotSupportedAccepts();
                }
            }
        }

        public void onEntity(Entity entity, String method) throws TranslatorException {
            Link editLink = entity.getEditLink();
            Map<String, List<String>> updateHeaders = getDefaultUpdateHeaders();
            if (entity.getETag() != null) {
                updateHeaders.put("If-Match", Arrays.asList(entity.getETag())); //$NON-NLS-1$
            }
            BinaryWSProcedureExecution update = invokeHTTP(
                    method,
                    editLink.getHref(),
                    method.equals("DELETE")?null:odataQuery.getPayload(entity),
                    updateHeaders);
            if (HttpStatusCode.NO_CONTENT.getStatusCode() == update.getResponseCode()) {
                updateCount.incrementAndGet();
            } else {
                throw buildError(update);
            }

        }

        public abstract Map<String, List<String>> headers();

        public abstract ODataDeserializer getDeSerializer();

        class NotSupportedAccepts extends Exception {}
    }

    class JsonEntityCollectionIterator extends EntityCollectionIterator {

        public JsonEntityCollectionIterator(ODataUpdateQuery odataQuery,
                String method) throws TranslatorException {
            super(odataQuery, method);
        }

        @Override
        public Map<String, List<String>> headers() {
            Map<String, List<String>> headers = new HashMap<String, List<String>>();
            headers.put("Accept", Arrays.asList(ContentType.JSON_FULL_METADATA.toContentTypeString())); //$NON-NLS-1$
            return headers;
        }

        @Override
        public ODataDeserializer getDeSerializer() {
            return new JsonDeserializer(false);
        }
    }

    class XMLEntityCollectionIterator extends EntityCollectionIterator {
        public XMLEntityCollectionIterator(ODataUpdateQuery odataQuery,
                String method) throws TranslatorException {
            super(odataQuery, method);
        }

        @Override
        public Map<String, List<String>> headers() {
            Map<String, List<String>> headers = new HashMap<String, List<String>>();
            headers.put("Accept", Arrays.asList(ContentType.APPLICATION_ATOM_XML.toContentTypeString())); //$NON-NLS-1$
            return headers;
        }

        @Override
        public ODataDeserializer getDeSerializer() {
            return new AtomDeserializer();
        }
    }

    private Map<String, List<String>> getDefaultUpdateHeaders() {
        Map<String, List<String>> headers = new HashMap<String, List<String>>();
        headers.put("Accept", Arrays.asList(ContentType.APPLICATION_JSON.toContentTypeString())); //$NON-NLS-1$
        headers.put("Content-Type", Arrays.asList(ContentType.APPLICATION_JSON.toContentTypeString())); //$NON-NLS-1$
        return headers;
    }

    private void handleInsert(ODataUpdateQuery odataQuery)
            throws TranslatorException {
        try {
            Map<String, List<String>> headers = getDefaultUpdateHeaders();
            headers.put("Prefer", Arrays.asList("return=representation")); //$NON-NLS-1$

            String uri = odataQuery.buildInsertURL("");
            InputStream response = null;
            BinaryWSProcedureExecution execution = invokeHTTP(odataQuery.getInsertMethod(),
                    uri,
                    odataQuery.getPayload(null),
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
        if (this.visitor.getOperationType() == OperationType.DELETE) {
            return new int[]{this.updateCount.get()};
        }
        else if(this.visitor.getOperationType() == OperationType.UPDATE) {
            return new int[]{this.updateCount.get()};
        }
        else if (this.visitor.getOperationType() == OperationType.INSERT) {
            if (this.createdEntity != null) {
                addAutoGeneretedKeys(this.visitor.getODataQuery().getRootDocument().getTable(),
                        this.createdEntity);
            }
            return new int[]{1};
        }
        return new int[] {0};
    }

    private void addAutoGeneretedKeys(Table table, Entity entity) throws TranslatorException {
        List<Column> generated = this.executionContext.getGeneratedKeyColumns();
        if (generated == null) {
            return;
        }
        int cols = generated.size();
        String[] odataTypes = new String[cols];

        // this is typically expected to be an int/long, but we'll be general here.
        // we may eventual need the type logic off of the metadata importer
        for (int i = 0; i < cols; i++) {
            odataTypes[i] = ODataMetadataProcessor.getNativeType(generated.get(i));
        }
        GeneratedKeys generatedKeys = this.executionContext.returnGeneratedKeys();

        List<Object> vals = new ArrayList<Object>(cols);
        for (int i = 0; i < cols; i++) {
            Property prop = entity.getProperty(generatedKeys.getColumnNames()[i]);
            Object value;
            try {
                value = ODataTypeManager.convertToTeiidRuntimeType(generatedKeys.getColumnTypes()[i], prop.getValue(), odataTypes[i],
                        generated.get(i).getProperty(BaseColumn.SPATIAL_SRID, false));
            } catch (TeiidException e) {
                throw new TranslatorException(e);
            }
            vals.add(value);
        }
        generatedKeys.addKey(vals);
    }
}
