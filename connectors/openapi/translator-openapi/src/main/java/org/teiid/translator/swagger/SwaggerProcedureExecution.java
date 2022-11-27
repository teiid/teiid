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
package org.teiid.translator.swagger;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.language.Argument;
import org.teiid.language.Argument.Direction;
import org.teiid.language.Array;
import org.teiid.language.Call;
import org.teiid.language.Expression;
import org.teiid.language.Literal;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.metadata.Column;
import org.teiid.metadata.ColumnSet;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.RestMetadataExtension;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.ws.BinaryWSProcedureExecution;
import org.teiid.translator.ws.WSConnection;
import org.teiid.util.WSUtil;

public class SwaggerProcedureExecution extends BaseQueryExecution implements ProcedureExecution {
    private Object returnValue;
    private SwaggerResponse response;
    private Class<?>[] expectedColumnTypes;
    private Call command;
    private Map<String, Object> responseHeaders;

    public SwaggerProcedureExecution(Call command,
            ExecutionFactory translator,
            ExecutionContext executionContext, RuntimeMetadata metadata,
            WSConnection connection) throws TranslatorException {
        super(translator, executionContext, metadata, connection);
        this.command = command;
        this.expectedColumnTypes = command.getResultSetColumnTypes();
    }

    private ProcedureParameter getReturnParameter() {
        for (ProcedureParameter pp : this.command.getMetadataObject().getParameters()) {
            if (pp.getType() == ProcedureParameter.Type.ReturnValue) {
                return pp;
            }
        }
        return null;
    }

    private BinaryWSProcedureExecution buildWSExecution(Call obj) throws TranslatorException {
        Procedure procedure = obj.getMetadataObject();
        String uri = procedure.getProperty(RestMetadataExtension.URI, false);
        String method = procedure.getProperty(RestMetadataExtension.METHOD, false);

        StringBuilder queryParameters = new StringBuilder();
        StringBuilder formParameters = new StringBuilder();
        Map<String, List<String>> headers = new HashMap<String, List<String>>();
        Object payload = null;
        SwaggerBodyInputDocument input = null; // body payload document
        final List<Argument> params = obj.getArguments();
        if (params != null && params.size() != 0) {
            Argument param = null;
            for (int i = 0; i < params.size(); i++) {
                param = params.get(i);
                ProcedureParameter metadata = param.getMetadataObject();
                String argName = WSUtil.httpURLEncode(param.getMetadataObject().getName());
                if (param.getDirection() == Direction.IN || param.getDirection() == Direction.INOUT) {
                    String in = metadata.getProperty(RestMetadataExtension.PARAMETER_TYPE, false);
                    if (in.equalsIgnoreCase(RestMetadataExtension.ParameterType.QUERY.name())) {
                        if (queryParameters.length() != 0) {
                            queryParameters.append("&"); //$NON-NLS-1$
                        }
                        Object value = param.getExpression();
                        if (value instanceof Array) {
                            addArgumentValue(argName, (Array)value,
                                    metadata.getProperty(SwaggerMetadataProcessor.COLLECION_FORMAT, false),
                                    queryParameters);
                        } else {
                            String argValue = getURLValue((Literal)value);
                            queryParameters.append(argName);
                            queryParameters.append(Tokens.EQ);
                            queryParameters.append(argValue);
                        }
                    } else if (in.equalsIgnoreCase(RestMetadataExtension.ParameterType.PATH.name())) {
                        String argValue = getURLValue(param.getArgumentValue());
                        String regex = "\\{" + argName + "\\}"; //$NON-NLS-1$ //$NON-NLS-2$
                        uri = uri.replaceAll(regex, argValue);
                    } else if (in.equalsIgnoreCase(RestMetadataExtension.ParameterType.FORM.name()) ||
                            in.equalsIgnoreCase(RestMetadataExtension.ParameterType.FORMDATA.name())) {
                        if (formParameters.length() != 0) {
                            formParameters.append("&"); //$NON-NLS-1$
                        }
                        Object value = param.getExpression();
                        if (value instanceof Array) {
                            addArgumentValue(argName, (Array)value,
                                    metadata.getProperty(SwaggerMetadataProcessor.COLLECION_FORMAT, false),
                                    formParameters);
                        } else {
                            formParameters.append(argName);
                            formParameters.append(Tokens.EQ);
                            formParameters.append(getURLValue((Literal)value));
                        }
                    } else if (in.equalsIgnoreCase(RestMetadataExtension.ParameterType.BODY.name())) {
                        if (input == null) {
                            input = new SwaggerBodyInputDocument();
                        }
                        Object expr = param.getExpression();
                        if (expr instanceof Literal) {
                            expr = ((Literal)expr).getValue();
                        }
                        input.addArgument(param.getMetadataObject(), expr);
                    } else if (in.equalsIgnoreCase(RestMetadataExtension.ParameterType.HEADER.name())) {
                        String argValue = param.getArgumentValue().getValue().toString();
                        headers.put(argName, Arrays.asList(argValue));
                    }
                } else {
                    throw new TranslatorException("Not supported parameter");
                }
            }
        }

        String consumes = procedure.getProperty(RestMetadataExtension.CONSUMES, false);
        if (consumes == null) {
            consumes = "application/json";
        }
        if (input != null) {
            try {
                SwaggerSerializer serializer = getSerializer(consumes);
                InputStream oos = serializer.serialize(input);
                payload = ObjectConverterUtil.convertToString(oos);
            } catch (IOException e) {
                throw new TranslatorException(e);
            }
        }

        if (payload == null && formParameters.length() > 0) {
            payload = formParameters.toString();
        }

        headers.put("Content-Type", Arrays.asList(consumes));

        String produces = procedure.getProperty(RestMetadataExtension.PRODUCES, false);
        if (produces == null) {
            produces = "application/json";
        }
        headers.put("Accept", Arrays.asList(produces));

        if (queryParameters.length() > 0) {
            uri = uri+"?"+queryParameters;
        }

        return buildInvokeHTTP(method, uri, payload, headers);
    }

    private String getURLValue(Literal value) {
        if (value.getValue() == null) {
            throw new TeiidRuntimeException("Null is not supported"); //$NON-NLS-1$
        }
        return WSUtil.httpURLEncode(value.getValue().toString());
    }

    private void addArgumentValue(String argName, Array value, String collectionFormat,
            StringBuilder queryStr) {
        List<Expression> exprs = value.getExpressions();

        if (collectionFormat.equalsIgnoreCase("multi")) {
            for (int i = 0; i< exprs.size(); i++) {
                if (i > 0) {
                    queryStr.append("&");
                }
                queryStr.append(argName);
                queryStr.append(Tokens.EQ);
                Literal l = (Literal)exprs.get(i);
                queryStr.append(getURLValue(l));
            }
        } else {
            String delimiter = ",";
            if (collectionFormat.equalsIgnoreCase("csv")) {
                delimiter = ",";
            } else if (collectionFormat.equalsIgnoreCase("ssv")) {
                delimiter = " ";
            } else if (collectionFormat.equalsIgnoreCase("tsv")) {
                delimiter = "\t";
            } else if (collectionFormat.equalsIgnoreCase("pipes")) {
                delimiter = "|";
            }
            queryStr.append(argName);
            queryStr.append(Tokens.EQ);
            for (int i = 0; i< exprs.size(); i++) {
                Literal l = (Literal)exprs.get(i);
                if (i > 0) {
                    queryStr.append(delimiter);
                }
                queryStr.append(getURLValue(l));
            }
        }
    }

    @Override
    public void execute() throws TranslatorException {
        Procedure procedure = this.command.getMetadataObject();

        BinaryWSProcedureExecution execution = buildWSExecution(this.command);
        execution.execute();

        if (execution.getResponseCode() >= 200 && execution.getResponseCode() < 300) {
            this.responseHeaders = execution.getResponseHeaders();
                // Success
                if (procedure.getResultSet() != null) {
                    if (procedure.getResultSet().getColumns().get(0).getName().equals("return")) {
                        // this procedure with return, but headers made this into a resultset.
                        this.returnValue = getReturnValue(execution);
                    } else {
                        try {
                            Blob blob = (Blob)execution.getOutputParameterValues().get(0);
                            InputStream wsResponse = blob.getBinaryStream();
                            Object obj = execution.getResponseHeader("Content-Type");
                            if (obj == null) {
                                throw new TranslatorException(SwaggerPlugin.Event.TEIID28017,
                                        SwaggerPlugin.Util.gs(SwaggerPlugin.Event.TEIID28017, "Not Defined"));
                            } else {
                                List<?> contentType = (List<?>)obj;
                                SwaggerSerializer serializer = getSerializer(contentType.get(0).toString());
                                if (serializer == null) {
                                    throw new TranslatorException(SwaggerPlugin.Event.TEIID28017,
                                            SwaggerPlugin.Util.gs(SwaggerPlugin.Event.TEIID28017, obj.toString()));
                                }
                                handleResponse(procedure, wsResponse, execution.getResponseHeaders(), serializer);
                            }
                        } catch (SQLException e) {
                            throw new TranslatorException(e);
                        }
                    }
                } else if (getReturnParameter() != null) {
                    // this is scalar result
                    this.returnValue = getReturnValue(execution);
                }
        } else if (execution.getResponseCode() == 404) {
            // treat as empty response. Typically that when someone uses it.
        } else {
            throw new TranslatorException(SwaggerPlugin.Event.TEIID28018,
                    SwaggerPlugin.Util.gs(SwaggerPlugin.Event.TEIID28018, execution.getResponseCode()));
        }
    }

    private String getReturnValue(BinaryWSProcedureExecution execution)
            throws TranslatorException {
        try {
            return ObjectConverterUtil.convertToString(
                    ((Blob)execution.getOutputParameterValues().get(0)).getBinaryStream());
        } catch (IOException e) {
            throw new TranslatorException(e);
        } catch (SQLException e) {
            throw new TranslatorException(e);
        }
    }

    public static SwaggerSerializer getSerializer(String contentType) {
        ContentType type = ContentType.parse(contentType);
        if (type.isJSON()) {
            return new JsonSerializer();
        } else if (type.isXML()) {
            throw new AssertionError("XML Not supported");
        }
        return null;
    }

    private void handleResponse(final Procedure procedure,
            final InputStream payload, Map<String, Object> headers, SwaggerSerializer serializer)
            throws TranslatorException {
        this.response = new SwaggerResponse(payload, headers, serializer, isMapResponse(procedure));
    }

    @Override
    public List<?> next() throws TranslatorException, DataNotAvailableException {
        Procedure procedure = this.command.getMetadataObject();
        if (this.response != null) {
            Map<String, Object> row = this.response.getNext();
            if (row != null) {
                row.putAll(this.responseHeaders);
                return buildRow(procedure.getResultSet().getColumns(), this.response.isMapResponse(),
                        this.expectedColumnTypes, row);
            }
        }
        if (this.returnValue != null) {
            Map<String, Object> row = new LinkedHashMap<String, Object>();
            row.put("return", SwaggerTypeManager.convertTeiidRuntimeType(this.returnValue, this.expectedColumnTypes[0]));
            row.putAll(this.responseHeaders);
            this.returnValue = null;
            return buildRow(procedure.getResultSet().getColumns(), false,
                    this.expectedColumnTypes, row);
        }
        return null;
    }

    private boolean isMapResponse(Procedure procedure) {
        ColumnSet<Procedure> columnSet = procedure.getResultSet();
        if (columnSet == null) {
            return false;
        }
        List<Column> columns = columnSet .getColumns();
        if (columns.size() >=2 &&
                columns.get(0).getName().equals(SwaggerMetadataProcessor.KEY_NAME) &&
                columns.get(1).getName().equals(SwaggerMetadataProcessor.KEY_VALUE)) {
            return true;
        }
        return false;
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

    private static class ContentType {

        private static String APPLICATION = "application"; //$NON-NLS-1$
        private static String TYPE_JSON = "json"; //$NON-NLS-1$
        private static String TYPE_XML = "xml"; //$NON-NLS-1$

        private String major;
        private String subtype;

        ContentType(String major, String subtype) {
            this.major = major;
            this.subtype = subtype;
        }

        public boolean isJSON() {
            return major != null && subtype != null && major.equals(APPLICATION) && subtype.equals(TYPE_JSON);
        }

        public boolean isXML() {
            return major != null && subtype != null && major.equals(APPLICATION) && subtype.equals(TYPE_XML);
        }

        public static ContentType parse(String type) {
            int typeIndex = type.indexOf('/'); //$NON-NLS-1$
            int paramIndex = type.indexOf(';'); //$NON-NLS-1$
            String major = null;
            String subtype = null;
            if(typeIndex > 0) {
                major = type.substring(0, typeIndex);
                if (paramIndex > -1) {
                    subtype = type.substring(typeIndex + 1, paramIndex);
                } else {
                    subtype = type.substring(typeIndex + 1);
                }
            }
            return new ContentType(major, subtype);
        }
    }
}
