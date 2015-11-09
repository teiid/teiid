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
package org.teiid.translator.swagger;

import static org.teiid.translator.swagger.SwaggerMetadataProcessor.getPathSeparator;
import static org.teiid.translator.swagger.SwaggerMetadataProcessor.getHttpAction;
import static org.teiid.translator.swagger.SwaggerMetadataProcessor.getHttpPath;
import static org.teiid.translator.swagger.SwaggerMetadataProcessor.getHttpHost;
import static org.teiid.translator.swagger.SwaggerMetadataProcessor.getBaseUrl;
import static org.teiid.translator.swagger.SwaggerMetadataProcessor.isPathParam;
import static org.teiid.translator.swagger.SwaggerMetadataProcessor.isEndPointParam;
import static org.teiid.translator.swagger.SwaggerMetadataProcessor.isPayload;
import static org.teiid.translator.swagger.SwaggerMetadataProcessor.getProcuces;
import static org.teiid.translator.swagger.SwaggerMetadataProcessor.getConsumes;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.activation.DataSource;
import javax.xml.ws.Dispatch;
import javax.xml.ws.Service.Mode;
import javax.xml.ws.handler.MessageContext;
import javax.xml.ws.http.HTTPBinding;

import org.apache.commons.io.IOUtils;
import org.teiid.connector.DataPlugin;
import org.teiid.core.types.BlobImpl;
import org.teiid.core.types.BlobType;
import org.teiid.core.types.ClobImpl;
import org.teiid.core.types.InputStreamFactory;
import org.teiid.json.simple.JSONParser;
import org.teiid.json.simple.ParseException;
import org.teiid.json.simple.SimpleContentHandler;
import org.teiid.language.Argument;
import org.teiid.language.Call;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.WSConnection;
import org.teiid.translator.swagger.SwaggerExecutionFactory.Action;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.joda.JodaModule;

public class SwaggerProcedureExecution implements ProcedureExecution{
    
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
    
    static final class JsonDeserializer {
                
        private ObjectMapper mapper;
        
        public JsonDeserializer() {
            mapper = new ObjectMapper();
            mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
            mapper.enable(SerializationFeature.WRITE_ENUMS_USING_TO_STRING);
            mapper.enable(DeserializationFeature.READ_ENUMS_USING_TO_STRING);
            mapper.registerModule(new JodaModule());
          }
        
        public Object asPrimitive(InputStream input) throws TranslatorException {
            
            try {
                StringWriter output = new StringWriter();
                IOUtils.copy(input, output);
                return output.toString();
            } catch (IOException e) {
                throw new TranslatorException(SwaggerPlugin.Event.TEIID28007, SwaggerPlugin.Util.gs(SwaggerPlugin.Event.TEIID28007, e.getMessage()));
            }
        }
        
        public String asCollection(InputStream input) throws TranslatorException {
            try {
                Map value = mapper.readValue(input, Map.class);
                return this.mapper.writeValueAsString(value);
            } catch (Exception e) {
                throw new TranslatorException(SwaggerPlugin.Event.TEIID28007, SwaggerPlugin.Util.gs(SwaggerPlugin.Event.TEIID28007, e.getMessage()));
            }
        }
    }
    
    static final class Pair {
        
        private boolean idPath = false;
        private String name;
        private String value;
        
        public Pair(boolean idPath, String name, String value) {
            this.idPath = idPath;
            this.name = name;
            this.value = value;
        }
        
        public String getName() {
            return name;
        }
        
        public String getValue() {
            return value;
        }

        public boolean isIdPath() {
            return idPath;
        }
    }
    
    RuntimeMetadata metadata;
    ExecutionContext context;
    private Call procedure;
    private DataSource returnValue;
    private WSConnection conn;
    SwaggerExecutionFactory executionFactory;
    Map<String, List<String>> customHeaders;
    Map<String, Object> responseContext = Collections.emptyMap();
    int responseCode = 200;
    private boolean useResponseContext;

    public SwaggerProcedureExecution(Call procedure, RuntimeMetadata metadata, ExecutionContext context, SwaggerExecutionFactory executionFactory, WSConnection conn) {
        this.metadata = metadata;
        this.context = context;
        this.procedure = procedure;
        this.executionFactory = executionFactory;
        this.conn = conn;
    }
    
    public void setUseResponseContext(boolean useResponseContext) {
        this.useResponseContext = useResponseContext;
    }

    @Override
    public void execute() throws TranslatorException {
        
        Action method = getHttpAction(this.procedure.getMetadataObject());
        
        List<Argument> arguments = this.procedure.getArguments();
        
        Object payload = getPayload(arguments);
        
        Procedure procedure = this.procedure.getMetadataObject();
                
        String endpoint = buildEndPoint(getHttpHost(procedure), getBaseUrl(procedure), getHttpPath(procedure, getParamPair(arguments)));
        
        try {
            Dispatch<DataSource> dispatch = this.conn.createDispatch(HTTPBinding.HTTP_BINDING, endpoint, DataSource.class, Mode.MESSAGE);

            dispatch.getRequestContext().put(MessageContext.HTTP_REQUEST_METHOD, executionFactory.getAction(method));
            
            Map<String, List<String>> httpHeaders = (Map<String, List<String>>)dispatch.getRequestContext().get(MessageContext.HTTP_REQUEST_HEADERS);
            if (customHeaders != null) {
                httpHeaders.putAll(customHeaders);
            }
            
            Clob headers = (Clob)arguments.get(arguments.size() - 1).getArgumentValue().getValue();
            if(null != headers) {
                parseHeader(httpHeaders, headers);
            }
            
            headersValidation(method, httpHeaders, procedure);
            
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
            
            responseContextValidation(httpHeaders.get("Accept"), dispatch.getResponseContext());            
        } catch (Exception e) {
            throw new TranslatorException(e);
        }
 
    }


    private void responseContextValidation(List<String> list, Map<String, Object> rc) throws TranslatorException {
        
        boolean valid = false ;
        if(null != list) {
            for(String type : list) {
                if(returnValue.getContentType().equals(type)) {
                    valid = true;
                }
            }
            
            if(!valid) {
                throw new TranslatorException(SwaggerPlugin.Event.TEIID28006, SwaggerPlugin.Util.gs(SwaggerPlugin.Event.TEIID28006, returnValue.getContentType(), list));
            }
        }
        
        this.responseCode = (Integer)rc.get(WSConnection.STATUS_CODE);
        if (this.useResponseContext) {
            //it's presumed that the caller will handle the response codes
            this.responseContext = rc;
        } else {
            if (this.responseCode >= 400) {
                String message = conn.getStatusMessage(this.responseCode);
                throw new TranslatorException(SwaggerPlugin.Event.TEIID28001, SwaggerPlugin.Util.gs(SwaggerPlugin.Event.TEIID28001, this.responseCode, message));
            }
        }
    }

    private void headersValidation(Action method, Map<String, List<String>> httpHeaders, Procedure procedure) throws TranslatorException {

        Set<String> procudes = getProcuces(procedure);
        if(httpHeaders.get("Accept") != null) { //$NON-NLS-1$
            for(String contentType : httpHeaders.get("Accept")) { //$NON-NLS-1$
                if(!procudes.contains(contentType)){
                    throw new TranslatorException(SwaggerPlugin.Event.TEIID28004, SwaggerPlugin.Util.gs(SwaggerPlugin.Event.TEIID28004, contentType, procudes));
                }
            }
        }
        
        Set<String> consumes = getConsumes(procedure);
        for(String contentType : httpHeaders.get("Content-Type")) { //$NON-NLS-1$
            // CXF client POST, PUT, DELETE invoke need 'Content-Type' in header
            if(!consumes.contains(contentType) && (method.equals(Action.POST) /*|| method.equals(Action.PUT) || method.equals(Action.DELETE)*/)){
                throw new TranslatorException(SwaggerPlugin.Event.TEIID28005, SwaggerPlugin.Util.gs(SwaggerPlugin.Event.TEIID28005, contentType, consumes));
            }
        }
    }

    private Object getPayload(List<Argument> arguments) {
        
        Object payload = null;
        
        for(Argument argument : arguments){
            ProcedureParameter parameter = argument.getMetadataObject();
            if(isPayload(parameter)){
                payload = argument.getArgumentValue().getValue();
                break;
            }
        }
        return payload;
    }

    private void parseHeader(Map<String, List<String>> httpHeaders, Clob headers) throws ParseException, TranslatorException, IOException, SQLException {
        SimpleContentHandler sch = new SimpleContentHandler();
        JSONParser parser = new JSONParser();
        Reader characterStream = headers.getCharacterStream();
        try {
            parser.parse(characterStream, sch);
            Object result = sch.getResult();
            if (!(result instanceof Map)) {
                throw new TranslatorException(SwaggerPlugin.Event.TEIID28002, SwaggerPlugin.Util.gs(SwaggerPlugin.Event.TEIID28002));
            }
            Map<String, Object> values = (Map)result;
            for (Map.Entry<String, Object> entry : values.entrySet()) {
                if ((entry.getValue() instanceof Map) || entry.getValue() == null) {
                    throw new TranslatorException(SwaggerPlugin.Event.TEIID28002, SwaggerPlugin.Util.gs(SwaggerPlugin.Event.TEIID28002));
                }
                if (!(entry.getValue() instanceof List)) {
                    entry.setValue(Arrays.asList(entry.getValue().toString()));
                    continue;
                }
                List<Object> list = (List)entry.getValue();
                for (int i = 0; i < list.size(); i++) {
                    Object value = list.get(i);
                    if (value instanceof Map || value instanceof List) {
                        throw new TranslatorException(SwaggerPlugin.Event.TEIID28002, SwaggerPlugin.Util.gs(SwaggerPlugin.Event.TEIID28002));
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
    
    private List<Pair> getParamPair(List<Argument> arguments) {
        
        List<Pair> paramList = new ArrayList<Pair>();
        for(Argument argument : arguments){
            ProcedureParameter parameter = argument.getMetadataObject();
            if(isEndPointParam(parameter)) {
                continue;
            }
            boolean isPathParam = isPathParam(parameter);
            String name = parameter.getName();
            String value = String.valueOf(argument.getArgumentValue().getValue());
            Pair pair = new Pair(isPathParam, name, value);
            paramList.add(pair);
        }
        
        return paramList;
    }

    private String buildEndPoint(String httpHost, String baseUrl, String apiPath) {
        
        String path = httpHost;
        String pathSeparator = getPathSeparator(); 
        path = trimSlash(path, pathSeparator);
        path += baseUrl;
        path = trimSlash(path, pathSeparator);
        path += apiPath;
        path = trimSlash(path, pathSeparator);
                  
        return path;
    }

    private String trimSlash(String path, String pathSeparator) {
        String value = ""; //$NON-NLS-1$
        if(path.endsWith(pathSeparator)) {
            value = path.substring(0, path.length() -1);
        } else {
            value = path;
        }
        return value;
    }

    @Override
    public List<?> next() throws TranslatorException, DataNotAvailableException {
        return null;
    }

    @Override
    public void close() {
        
    }

    @Override
    public void cancel() throws TranslatorException {
        
    }

    @Override
    public List<?> getOutputParameterValues() throws TranslatorException {
        
        JsonDeserializer parser = new JsonDeserializer();
        Object result = null;
        try {
            if(this.returnValue.getContentType().equals("application/json")){
                result = parser.asCollection(this.returnValue.getInputStream());
            } else if(this.returnValue.getContentType().equals("application/xml")) {
                result = parser.asPrimitive(this.returnValue.getInputStream());
            }
            if(result == null) {
                result = new BlobType(new StreamingBlob(this.returnValue.getInputStream()));
            }
            return Arrays.asList(result);
        } catch (IOException e) {
            throw new TranslatorException(e);
        }
    }
    
    public void setCustomHeaders(Map<String, List<String>> customHeaders) {
        this.customHeaders = customHeaders;
    }

    public Object getResponseHeader(String name){
        return this.responseContext.get(name);
    }

    public int getResponseCode() {
        return this.responseCode;
    }


}
