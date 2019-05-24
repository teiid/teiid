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
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.olingo.client.api.serialization.ODataDeserializerException;
import org.apache.olingo.client.core.serialization.JsonDeserializer;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeException;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.teiid.language.Argument;
import org.teiid.language.Argument.Direction;
import org.teiid.language.Call;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.olingo.common.ODataTypeManager;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.document.DocumentNode;
import org.teiid.translator.odata4.ODataMetadataProcessor.ODataType;
import org.teiid.translator.ws.WSConnection;
import org.teiid.util.WSUtil;

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

    static String getQueryParameters(Call obj) throws EdmPrimitiveTypeException {
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
                    sb.append(WSUtil.httpURLEncode(param.getMetadataObject().getName()));
                    sb.append(Tokens.EQ);
                    sb.append(WSUtil.httpURLEncode(ODataTypeManager.convertToODataURIValue(param.getArgumentValue().getValue(),
                            ODataTypeManager.odataType(param.getType()).getFullQualifiedName()
                            .getFullQualifiedNameAsString())));
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
        try {
            String  parameters = getQueryParameters(this.command);

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
        } catch (EdmPrimitiveTypeException e) {
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
