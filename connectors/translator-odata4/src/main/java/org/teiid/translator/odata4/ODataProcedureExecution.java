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
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.olingo.client.api.serialization.ODataDeserializerException;
import org.apache.olingo.client.core.serialization.JsonDeserializer;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.teiid.language.Argument;
import org.teiid.language.Call;
import org.teiid.language.Argument.Direction;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.WSConnection;
import org.teiid.translator.document.DocumentNode;
import org.teiid.translator.odata4.ODataMetadataProcessor.ODataType;

public class ODataProcedureExecution extends BaseQueryExecution implements ProcedureExecution {
    private Object returnValue;
    private ODataResponse response;
    private Class<?>[] expectedColumnTypes;
    private Call command;

    public ODataProcedureExecution(Call command,
            ODataExecutionFactory translator,
            ExecutionContext executionContext, RuntimeMetadata metadata,
            WSConnection connection) throws TranslatorException {
        super(translator, executionContext, metadata, connection);
        this.command = command;
        this.expectedColumnTypes = command.getResultSetColumnTypes();
    }
    
    private boolean isFunction(Procedure proc) {
        ODataType type = ODataType.valueOf(proc.getProperty(ODataMetadataProcessor.ODATA_TYPE, false));
        return type == ODataType.FUNCTION;
    }   

    private ProcedureParameter getReturnParameter() {
        for (ProcedureParameter pp : this.command.getMetadataObject().getParameters()) {
            if (pp.getType() == ProcedureParameter.Type.ReturnValue) {
                return pp;
            }
        }
        return null;
    }
    
    private String getQueryParameters(Call obj) throws TranslatorException {
        StringBuilder sb = new StringBuilder();
        final List<Argument> params = obj.getArguments();
        if (params != null && params.size() != 0) {
            Argument param = null;
            for (int i = 0; i < params.size(); i++) {
                param = params.get(i);
                if (param.getDirection() == Direction.IN || param.getDirection() == Direction.INOUT) {
                    if (i != 0) {
                        sb.append("&"); //$NON-NLS-1$
                    }
                    sb.append(param.getMetadataObject().getName());
                    sb.append(Tokens.EQ);
                    sb.append(ODataTypeManager.convertToODataInput(param.getArgumentValue(), 
                            ODataTypeManager.odataType(param.getType()).getFullQualifiedName()
                            .getFullQualifiedNameAsString()));
                }
            }
        }
        return sb.toString();
    }
    
    private String buildFunctionURL(Call obj, String parameters) {
        StringBuilder sb = new StringBuilder();
        sb.append(obj.getProcedureName());
        if (parameters != null && !parameters.isEmpty()) {
            sb.append("?");
            sb.append(parameters);
        }
        return sb.toString();
    }
    
    @Override
    public void execute() throws TranslatorException {
        String  parameters = getQueryParameters(this.command); 
        try {
            
            InputStream response = null;
            Procedure procedure = this.command.getMetadataObject();
            if (isFunction(procedure)) {
                String URI = buildFunctionURL(this.command, parameters);
                response = executeQuery("GET", URI, null, null, new HttpStatusCode[] { HttpStatusCode.OK });
                handleResponse(procedure, URI, response);
            } else {
                String URI = this.command.getProcedureName();
                response = executeQuery("POST", URI, parameters, null, new HttpStatusCode[] { HttpStatusCode.OK });
                handleResponse(procedure, URI, response);
            }
        } catch (ODataDeserializerException e) {
            throw new TranslatorException(e);
        }
    }

    private void handleResponse(final Procedure procedure, final String baseUri, final InputStream payload)
            throws TranslatorException, ODataDeserializerException {
        if (procedure.getResultSet() != null) {
            ODataType type = ODataType.valueOf(procedure.getResultSet().getProperty(
                    ODataMetadataProcessor.ODATA_TYPE, false));
            this.response = new ODataResponse(payload, type, new DocumentNode()) {
                @Override
                public InputStream nextBatch(java.net.URI uri) throws TranslatorException {
                    return executeSkipToken(uri, baseUri,
                            new HttpStatusCode[] { HttpStatusCode.OK });
                }
            };
        } else if (getReturnParameter() != null) {
            // this is scalar result
            JsonDeserializer parser = new JsonDeserializer(false);
            Property property = parser.toProperty(payload).getPayload();
            if (property.isCollection()) {
                this.returnValue = property.asCollection();
            } else {
                this.returnValue = property.asPrimitive();    
            }
        }
    }

    @Override
    public List<?> next() throws TranslatorException, DataNotAvailableException {
        Procedure procedure = this.command.getMetadataObject();
        if (this.response != null) {
            Map<String, Object> row = this.response.getNext();
            if (row != null) {
                return buildRow(procedure.getResultSet(), 
                        procedure.getResultSet().getColumns(), 
                        this.expectedColumnTypes, row);
            }
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
