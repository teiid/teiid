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
import static org.teiid.translator.swagger.SwaggerMetadataProcessor.getCatalogSeparator;
import static org.teiid.translator.swagger.SwaggerMetadataProcessor.getParamSeparator;
import static org.teiid.translator.swagger.SwaggerMetadataProcessor.getParamSeparatorFirst;
import static org.teiid.translator.swagger.SwaggerMetadataProcessor.getParamSeparatorEqual;
import static org.teiid.translator.swagger.SwaggerMetadataProcessor.getHttpAction;
import static org.teiid.translator.swagger.SwaggerMetadataProcessor.getHttpHost;
import static org.teiid.translator.swagger.SwaggerMetadataProcessor.getBaseUrl;
import static org.teiid.translator.swagger.SwaggerMetadataProcessor.isPathParam;
import static org.teiid.translator.swagger.SwaggerMetadataProcessor.isEndPointParam;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.activation.DataSource;
import javax.xml.ws.Dispatch;
import javax.xml.ws.Service.Mode;
import javax.xml.ws.handler.MessageContext;
import javax.xml.ws.http.HTTPBinding;

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
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.WSConnection;
import org.teiid.translator.swagger.SwaggerExecutionFactory.Action;

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
        Object payload = null;
        List<Argument> arguments = this.procedure.getArguments();
        
        String endpoint = buildEndPoint(method, getHttpHost(this.procedure.getMetadataObject()), getBaseUrl(this.procedure.getMetadataObject()),this.procedure.getProcedureName(),arguments);
        
        if(method.equals(Action.POST)) {
            payload = arguments.get(0).getArgumentValue().getValue();
        }
        
        try {
            Dispatch<DataSource> dispatch = this.conn.createDispatch(HTTPBinding.HTTP_BINDING, endpoint, DataSource.class, Mode.MESSAGE);

            dispatch.getRequestContext().put(MessageContext.HTTP_REQUEST_METHOD, executionFactory.getAction(method));
            
            if(payload == null && (method.equals(Action.POST) || method.equals(Action.PUT))){
                throw new TranslatorException(SwaggerPlugin.Event.TEIID28003, SwaggerPlugin.Util.gs(SwaggerPlugin.Event.TEIID28003, method));
            }
            
            Map<String, List<String>> httpHeaders = (Map<String, List<String>>)dispatch.getRequestContext().get(MessageContext.HTTP_REQUEST_HEADERS);
            if (customHeaders != null) {
                httpHeaders.putAll(customHeaders);
            }
            
            Clob headers = (Clob)arguments.get(arguments.size() - 2).getArgumentValue().getValue();
            if(null != headers) {
                parseHeader(httpHeaders, headers);
            }
            
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
                if (this.responseCode >= 400) {
                    String message = conn.getStatusMessage(this.responseCode);
                    throw new TranslatorException(SwaggerPlugin.Event.TEIID28001, SwaggerPlugin.Util.gs(SwaggerPlugin.Event.TEIID28001, this.responseCode, message));
                }
            }
        } catch (Exception e) {
            throw new TranslatorException(e);
        }
 
    }
    
    static void parseHeader(Map<String, List<String>> httpHeaders, Clob headers) throws ParseException, TranslatorException, IOException, SQLException {
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

    private String buildEndPoint(Action method, String httpHost, String baseUrl, String procedureName, List<Argument> arguments) {
        
        String path = null;
        String pathSeparator = getPathSeparator(); 
        String catalogSeparator = getCatalogSeparator(); 
        
        if(httpHost.endsWith(pathSeparator)) {
            httpHost = httpHost.substring(0, httpHost.length() -1);
        }
        
        path = httpHost;
        
        if(!baseUrl.startsWith(pathSeparator)) {
            baseUrl = pathSeparator + baseUrl;
        }
        
        path += baseUrl;
        
        String subPath = procedureName.replace(catalogSeparator, pathSeparator);
        if(!path.endsWith(pathSeparator)) {
            subPath = pathSeparator + subPath ;
        }
        
        path += subPath;
        
        if(method.equals(Action.GET) || method.equals(Action.DELETE)) { 
            boolean first = true;
            for(Argument argument : arguments){
                ProcedureParameter parameter = argument.getMetadataObject();
                if(isEndPointParam(parameter)) {
                    continue;
                }
                boolean isPathParam = isPathParam(parameter); 
                if(isPathParam){
                    if(!path.endsWith(pathSeparator)){
                        path += pathSeparator;
                    }
                    path += argument.getArgumentValue().getValue(); 
                } else {
                    if(first) {
                        path = path + getParamSeparatorFirst() + parameter.getName() + getParamSeparatorEqual() + argument.getArgumentValue().getValue(); 
                        first = false;
                    } else {
                        path = path + getParamSeparator() + parameter.getName() + getParamSeparatorEqual() + argument.getArgumentValue().getValue(); 
                    }
                }
            }
        }
                      
        return path;
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
        Object result = this.returnValue;
        try {
            result = new BlobType(new StreamingBlob(this.returnValue.getInputStream()));
        } catch (IOException e) {
            throw new TranslatorException(e);
        }
        return Arrays.asList(result, this.returnValue.getContentType());
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
