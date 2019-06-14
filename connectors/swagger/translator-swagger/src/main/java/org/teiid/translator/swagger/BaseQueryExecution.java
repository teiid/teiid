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

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.teiid.core.TeiidException;
import org.teiid.language.Argument;
import org.teiid.language.Argument.Direction;
import org.teiid.language.Call;
import org.teiid.language.Literal;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.metadata.Column;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.ws.BinaryWSProcedureExecution;
import org.teiid.translator.ws.WSConnection;

public class BaseQueryExecution {
    protected WSConnection connection;
    protected ExecutionFactory translator;
    protected RuntimeMetadata metadata;
    protected ExecutionContext executionContext;

    public BaseQueryExecution(ExecutionFactory translator,
            ExecutionContext executionContext, RuntimeMetadata metadata,
            WSConnection connection) {
        this.metadata = metadata;
        this.executionContext = executionContext;
        this.translator = translator;
        this.connection = connection;
    }

    String getHeader(BinaryWSProcedureExecution execution, String header) {
        Object value = execution.getResponseHeader(header);
        if (value instanceof List) {
            return (String)((List<?>)value).get(0);
        }
        return (String)value;
    }

    public BinaryWSProcedureExecution buildInvokeHTTP(String method,
            String uri, Object payload, Map<String, List<String>> headers)
            throws TranslatorException {

        if (LogManager.isMessageToBeRecorded(LogConstants.CTX_CONNECTOR, MessageLevel.DETAIL)) {
            try {
                LogManager.logDetail(LogConstants.CTX_CONNECTOR,
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

        Call call = this.translator.getLanguageFactory().createCall("invokeHttp", parameters, null);

        BinaryWSProcedureExecution execution = new BinaryWSProcedureExecution(
                call, this.metadata, this.executionContext, null,
                this.connection);
        execution.setUseResponseContext(true);
        execution.setCustomHeaders(headers);
        return execution;
    }

    @SuppressWarnings("unchecked")
    List<?> buildRow(List<Column> columns, boolean isMapResponse,
            Class<?>[] expectedType, Map<String, Object> values) throws TranslatorException {
        List<Object> results = new ArrayList<Object>();
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            String colName = column.getName();

            if (column.getNameInSource() != null) {
                colName = column.getNameInSource();
            }

            int arrayIndex = colName.indexOf("[]/");
            if (arrayIndex !=-1) {
                colName = colName.substring(0,arrayIndex)+colName.substring(colName.indexOf("/",arrayIndex+3));
            }

            Object value;
            try {
                if (isMapResponse) {
                    if (colName.equals(SwaggerMetadataProcessor.KEY_NAME)) {
                        value = values.keySet().iterator().next();
                    } else if (colName.equals(SwaggerMetadataProcessor.KEY_VALUE)) {
                        value = values.values().iterator().next();
                    } else {
                        value = values.get(colName);
                    }
                } else {
                    value = values.get(colName);
                }
                value = SwaggerTypeManager.convertTeiidRuntimeType(value, expectedType[i]);
            } catch (TeiidException e) {
                throw new TranslatorException(e);
            }
            results.add(value);
        }
        return results;
    }

}
