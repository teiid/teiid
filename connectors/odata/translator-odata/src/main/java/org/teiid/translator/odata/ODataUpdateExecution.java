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
package org.teiid.translator.odata;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.core.Response.Status;

import org.odata4j.core.OEntities;
import org.odata4j.core.OEntity;
import org.odata4j.core.OProperty;
import org.odata4j.edm.EdmDataServices;
import org.odata4j.edm.EdmEntitySet;
import org.odata4j.format.Entry;
import org.odata4j.format.FormatWriter;
import org.odata4j.format.FormatWriterFactory;
import org.teiid.GeneratedKeys;
import org.teiid.language.Command;
import org.teiid.metadata.Column;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Schema;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;
import org.teiid.translator.ws.BinaryWSProcedureExecution;
import org.teiid.translator.ws.WSConnection;

public class ODataUpdateExecution extends BaseQueryExecution implements UpdateExecution {
    private ODataUpdateVisitor visitor;
    private ODataEntitiesResponse response;

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
        if (this.visitor.getMethod().equals("DELETE")) { //$NON-NLS-1$
            // DELETE
            BinaryWSProcedureExecution execution = executeDirect(this.visitor.getMethod(), this.visitor.buildURL(), null, getDefaultHeaders());
            if (execution.getResponseCode() != Status.OK.getStatusCode() && (execution.getResponseCode() != Status.NO_CONTENT.getStatusCode())) {
                throw buildError(execution);
            }
        }
        else if(this.visitor.getMethod().equals("PUT")) { //$NON-NLS-1$
            // UPDATE
            Schema schema = visitor.getTable().getParent();
            EdmDataServices edm = new TeiidEdmMetadata(schema.getName(), ODataEntitySchemaBuilder.buildMetadata(schema));
            BinaryWSProcedureExecution execution = executeDirect("GET", this.visitor.buildURL(), null, getDefaultHeaders()); //$NON-NLS-1$
            if (execution.getResponseCode() == Status.OK.getStatusCode()) {
                String etag = getHeader(execution, "ETag"); //$NON-NLS-1$
                String payload = buildPayload(this.visitor.getTable().getName(), this.visitor.getPayload(), edm);
                this.response = executeWithReturnEntity(this.visitor.getMethod(), this.visitor.buildURL(), payload, this.visitor.getTable().getName(), edm, etag, Status.OK, Status.NO_CONTENT);
                if (this.response != null) {
                    if (this.response.hasError()) {
                        throw this.response.getError();
                    }
                }
            }
        }
        else if (this.visitor.getMethod().equals("POST")) { //$NON-NLS-1$
            // INSERT
            Schema schema = visitor.getTable().getParent();
            EdmDataServices edm = new TeiidEdmMetadata(schema.getName(), ODataEntitySchemaBuilder.buildMetadata( schema));
            String payload = buildPayload(this.visitor.getTable().getName(), this.visitor.getPayload(), edm);
            this.response = executeWithReturnEntity(this.visitor.getMethod(), this.visitor.buildURL(), payload, this.visitor.getTable().getName(), edm, null, Status.CREATED);
            if (this.response != null) {
                if (this.response.hasError()) {
                    throw this.response.getError();
                }
            }
        }
    }

    private String buildPayload(String entitySet, final List<OProperty<?>> props, EdmDataServices edm) {
        // this is remove the teiid specific model name from the entity type name.
        final EdmEntitySet ees = ODataEntitySchemaBuilder.removeModelName(edm.getEdmEntitySet(entitySet));
        Entry entry =  new Entry() {
            public String getUri() {
              return null;
            }
            public OEntity getEntity() {
              return OEntities.createRequest(ees, props, null);
            }
          };

        StringWriter sw = new StringWriter();
        FormatWriter<Entry> fw = FormatWriterFactory.getFormatWriter(Entry.class, null, "ATOM", null); //$NON-NLS-1$
        fw.write(null, sw, entry);
        return sw.toString();
    }

    @Override
    public int[] getUpdateCounts() throws DataNotAvailableException, TranslatorException {
        if (this.visitor.getMethod().equals("DELETE")) { //$NON-NLS-1$
            //DELETE
            return (this.response != null)?new int[]{1}:new int[]{0};
        }
        else if(this.visitor.getMethod().equals("PUT")) { //$NON-NLS-1$
            // UPDATE;
            // conflicting implementation found where some sent 200 with content; other with 204 no-content
            return (this.response != null)?new int[]{1}:new int[]{0};
        }
        else if (this.visitor.getMethod().equals("POST")) { //$NON-NLS-1$
            //INSERT
            if (this.response != null && this.response.hasRow()) {
                addAutoGeneretedKeys();
                return new int[]{1};
            }
        }
        return new int[] {0};
    }

    private void addAutoGeneretedKeys() {
        OEntity entity = this.response.getResultsIter().next().getEntity();
        List<Column> generated = this.executionContext.getGeneratedKeyColumns();
        if (generated == null) {
            return;
        }
        int cols = generated.size();
        GeneratedKeys generatedKeys = this.executionContext.returnGeneratedKeys();
        List<Object> vals = new ArrayList<Object>(cols);
        for (int i = 0; i < cols; i++) {
            OProperty<?> prop = entity.getProperty(generatedKeys.getColumnNames()[i]);
            Object value = this.translator.retrieveValue(prop.getValue(), generatedKeys.getColumnTypes()[i]);
            vals.add(value);
        }
        generatedKeys.addKey(vals);
    }
}
