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

package org.teiid.translator.ws;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.activation.DataSource;
import javax.xml.ws.Dispatch;
import javax.xml.ws.Service.Mode;
import javax.xml.ws.WebServiceException;
import javax.xml.ws.handler.MessageContext;
import javax.xml.ws.http.HTTPBinding;

import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.teiid.connector.DataPlugin;
import org.teiid.core.types.BlobImpl;
import org.teiid.core.types.BlobType;
import org.teiid.core.types.ClobImpl;
import org.teiid.core.types.InputStreamFactory;
import org.teiid.language.Argument;
import org.teiid.language.Argument.Direction;
import org.teiid.language.Call;
import org.teiid.metadata.ProcedureParameter.Type;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.TranslatorException;

/**
 * http handler
 */
public class BinaryWSProcedureExecution implements ProcedureExecution {

    public static final class StreamingBlob extends BlobImpl {
        InputStream is;

        public StreamingBlob(InputStream is) {
            this.is = is;
        }

        @Override
        public InputStream getBinaryStream() throws SQLException {
            if (this.is == null) {
                throw new SQLException(DataPlugin.Util.gs(DataPlugin.Event.TEIID60019));
            }
            InputStream result = this.is;
            this.is = null;
            return result;
        }
    }

    RuntimeMetadata metadata;
    ExecutionContext context;
    private Call procedure;
    private DataSource returnValue;
    private WSConnection conn;
    WSExecutionFactory executionFactory;
    Map<String, List<String>> customHeaders;
    Map<String, Object> responseContext = Collections.emptyMap();
    int responseCode = 200;
    private boolean useResponseContext;

    public BinaryWSProcedureExecution(Call procedure, RuntimeMetadata metadata, ExecutionContext context, WSExecutionFactory executionFactory, WSConnection conn) {
        this.metadata = metadata;
        this.context = context;
        this.procedure = procedure;
        this.conn = conn;
        this.executionFactory = executionFactory;
    }

    public void setUseResponseContext(boolean useResponseContext) {
        this.useResponseContext = useResponseContext;
    }

    public void execute() throws TranslatorException {
        List<Argument> arguments = this.procedure.getArguments();

        String method = (String)arguments.get(0).getArgumentValue().getValue();
        Object payload = arguments.get(1).getArgumentValue().getValue();
        String endpoint = (String)arguments.get(2).getArgumentValue().getValue();
        try {
            Dispatch<DataSource> dispatch = this.conn.createDispatch(HTTPBinding.HTTP_BINDING, endpoint, DataSource.class, Mode.MESSAGE);

            if (method == null) {
                method = "POST"; //$NON-NLS-1$
            }

            dispatch.getRequestContext().put(MessageContext.HTTP_REQUEST_METHOD, method);
            if (payload != null && !"POST".equalsIgnoreCase(method) && !"PUT".equalsIgnoreCase(method) && !"PATCH".equalsIgnoreCase(method)) { //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                throw new WebServiceException(WSExecutionFactory.UTIL.getString("http_usage_error")); //$NON-NLS-1$
            }

            Map<String, List<String>> httpHeaders = (Map<String, List<String>>)dispatch.getRequestContext().get(MessageContext.HTTP_REQUEST_HEADERS);
            if (customHeaders != null) {
                httpHeaders.putAll(customHeaders);
            }
            if (arguments.size() > 5
                    //designer modeled the return value as an out, which will add an argument in the 5th position that is an out
                    && this.procedure.getMetadataObject() != null
                    && (this.procedure.getMetadataObject().getParameters().get(0).getType() == Type.ReturnValue
                    || arguments.get(5).getMetadataObject().getSourceName().equalsIgnoreCase("headers"))) { //$NON-NLS-1$
                Clob headers = (Clob)arguments.get(5).getArgumentValue().getValue();
                if (headers != null) {
                    parseHeader(httpHeaders, headers);
                }
            }
            dispatch.getRequestContext().put(MessageContext.HTTP_REQUEST_HEADERS, httpHeaders);

            DataSource ds = null;
            if (payload instanceof String) {
                ds = new InputStreamFactory.ClobInputStreamFactory(new ClobImpl((String)payload));
            } else if (payload instanceof SQLXML) {
                ds = new InputStreamFactory.SQLXMLInputStreamFactory((SQLXML)payload);
            } else if (payload instanceof Clob) {
                ds = new InputStreamFactory.ClobInputStreamFactory((Clob)payload);
            } else if (payload instanceof Blob) {
                ds = new InputStreamFactory.BlobInputStreamFactory((Blob)payload);
            }

            this.returnValue = dispatch.invoke(ds);

            Map<String, Object> rc = dispatch.getResponseContext();
            this.responseCode = (Integer)rc.get(WSConnection.STATUS_CODE);
            if (this.useResponseContext) {
                //it's presumed that the caller will handle the response codes
                this.responseContext = rc;
            } else {
                //TODO: may need to add logic around some 200/300 codes - cxf should at least be logging this
                if (this.responseCode >= 400) {
                    String message = conn.getStatusMessage(this.responseCode);
                    throw new TranslatorException(WSExecutionFactory.Event.TEIID15005, WSExecutionFactory.UTIL.gs(WSExecutionFactory.Event.TEIID15005, this.responseCode, message));
                }
            }
        } catch (WebServiceException e) {
            throw new TranslatorException(e);
        } catch (ParseException e) {
            throw new TranslatorException(e);
        } catch (IOException e) {
            throw new TranslatorException(e);
        } catch (SQLException e) {
            throw new TranslatorException(e);
        }
    }

    static void parseHeader(Map<String, List<String>> httpHeaders,
            Clob headers) throws ParseException, TranslatorException, IOException, SQLException {
        SimpleContentHandler sch = new SimpleContentHandler();
        JSONParser parser = new JSONParser();
        Reader characterStream = headers.getCharacterStream();
        try {
            parser.parse(characterStream, sch);
            Object result = sch.getResult();
            if (!(result instanceof Map)) {
                throw new TranslatorException(WSExecutionFactory.Event.TEIID15006, WSExecutionFactory.UTIL.gs(WSExecutionFactory.Event.TEIID15006));
            }
            Map<String, Object> values = (Map)result;
            for (Map.Entry<String, Object> entry : values.entrySet()) {
                if ((entry.getValue() instanceof Map) || entry.getValue() == null) {
                    throw new TranslatorException(WSExecutionFactory.Event.TEIID15006, WSExecutionFactory.UTIL.gs(WSExecutionFactory.Event.TEIID15006));
                }
                if (!(entry.getValue() instanceof List)) {
                    entry.setValue(Arrays.asList(entry.getValue().toString()));
                    continue;
                }
                List<Object> list = (List)entry.getValue();
                for (int i = 0; i < list.size(); i++) {
                    Object value = list.get(i);
                    if (value instanceof Map || value instanceof List) {
                        throw new TranslatorException(WSExecutionFactory.Event.TEIID15006, WSExecutionFactory.UTIL.gs(WSExecutionFactory.Event.TEIID15006));
                    }
                    if (!(value instanceof String)) {
                        list.set(i, value.toString());
                    }
                }
            }
            httpHeaders.putAll((Map) values);
        } finally {
            characterStream.close();
        }

    }

    @Override
    public List<?> next() throws TranslatorException, DataNotAvailableException {
        return null;
    }

    @Override
    public List<?> getOutputParameterValues() throws TranslatorException {
        Object result = this.returnValue;
        if (returnValue != null && procedure.getArguments().size() > 4
                && procedure.getArguments().get(3).getDirection() == Direction.IN
                && Boolean.TRUE.equals(procedure.getArguments().get(3).getArgumentValue().getValue())) {
            try {
                result = new BlobType(new StreamingBlob(this.returnValue.getInputStream()));
            } catch (IOException e) {
                throw new TranslatorException(e);
            }
        }
        return Arrays.asList(result, this.returnValue.getContentType());
    }

    public void close() {

    }

    public void cancel() throws TranslatorException {
        // no-op
    }

    public void setCustomHeaders(Map<String, List<String>> customHeaders) {
        this.customHeaders = customHeaders;
    }

    public Object getResponseHeader(String name){
        return this.responseContext.get(name);
    }

    public Map<String, Object> getResponseHeaders(){
        return new HashMap<String, Object>(this.responseContext) ;
    }

    public int getResponseCode() {
        return this.responseCode;
    }
}
