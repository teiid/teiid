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

import java.io.InputStreamReader;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import javax.ws.rs.core.Response.Status;

import org.odata4j.core.ODataVersion;
import org.odata4j.core.OObject;
import org.odata4j.core.OSimpleObject;
import org.odata4j.edm.EdmDataServices;
import org.odata4j.format.FormatParser;
import org.odata4j.format.FormatParserFactory;
import org.odata4j.format.FormatType;
import org.odata4j.format.Settings;
import org.teiid.language.Call;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Schema;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.ws.BinaryWSProcedureExecution;
import org.teiid.translator.ws.WSConnection;

public class ODataProcedureExecution extends BaseQueryExecution implements ProcedureExecution {
    private ODataProcedureVisitor visitor;
    private Object returnValue;
    private ODataEntitiesResponse response;
    private Class<?>[] expectedColumnTypes;

    public ODataProcedureExecution(Call command, ODataExecutionFactory translator,  ExecutionContext executionContext, RuntimeMetadata metadata, WSConnection connection) throws TranslatorException {
        super(translator, executionContext, metadata, connection);

        this.visitor = new ODataProcedureVisitor(translator, metadata);
        this.visitor.visitNode(command);

        if (!this.visitor.exceptions.isEmpty()) {
            throw this.visitor.exceptions.get(0);
        }

        this.expectedColumnTypes = command.getResultSetColumnTypes();
    }

    @Override
    public void execute() throws TranslatorException {
        String URI = this.visitor.buildURL();
        Schema schema = visitor.getProcedure().getParent();
        EdmDataServices edm = new TeiidEdmMetadata(schema.getName(), ODataEntitySchemaBuilder.buildMetadata(schema));
        if (this.visitor.hasCollectionReturn()) {
            if (this.visitor.isReturnComplexType()) {
                // complex return
                this.response = executeWithComplexReturn(this.visitor.getMethod(), URI, null, this.visitor.getReturnEntityTypeName(), edm, null, Status.OK, Status.NO_CONTENT);
            }
            else {
                // entity type return
                this.response = executeWithReturnEntity(this.visitor.getMethod(), URI, null, this.visitor.getTable().getName(), edm, null, Status.OK, Status.NO_CONTENT);
            }
            if (this.response != null && this.response.hasError()) {
                throw this.response.getError();
            }
        }
        else {
            try {
                BinaryWSProcedureExecution execution = executeDirect(this.visitor.getMethod(), URI, null, getDefaultHeaders());
                if (execution.getResponseCode() != Status.OK.getStatusCode()) {
                    throw buildError(execution);
                }

                Blob blob = (Blob)execution.getOutputParameterValues().get(0);
                ODataVersion version = getODataVersion(execution);

                // if the procedure is not void
                if (this.visitor.getReturnType() != null) {
                    FormatParser<? extends OObject> parser = FormatParserFactory.getParser(OSimpleObject.class,
                            FormatType.ATOM, new Settings(version, edm, this.visitor.getProcedure().getName(),
                                null, // entitykey
                                true, // isResponse
                                ODataTypeManager.odataType(this.visitor.getReturnType())));

                    OSimpleObject object = (OSimpleObject)parser.parse(new InputStreamReader(blob.getBinaryStream()));
                    this.returnValue = this.translator.retrieveValue(object.getValue(), this.visitor.getReturnTypeClass());
                }
            } catch (SQLException e) {
                throw new TranslatorException(e);
            }
        }
    }

    @Override
    public List<?> next() throws TranslatorException, DataNotAvailableException {
        // Feed based response
        if (this.visitor.hasCollectionReturn() && this.response != null ) {
            return this.response.getNextRow(this.visitor.getReturnColumns(), this.expectedColumnTypes);
        }
        return null;
    }

    @Override
    public List<?> getOutputParameterValues() throws TranslatorException {
        return Arrays.asList(this.returnValue);
    }

    @Override
    public void close() {
    }

    @Override
    public void cancel() throws TranslatorException {
    }
}
