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

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.teiid.core.types.DataTypeManager;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.ProcedureParameter.Type;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.TranslatorProperty.PropertyType;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.WSConnection;
import org.teiid.translator.swagger.SwaggerExecutionFactory.Action;

import io.swagger.models.Operation;
import io.swagger.models.Path;
import io.swagger.models.Swagger;
import io.swagger.models.parameters.Parameter;
import io.swagger.models.parameters.PathParameter;
import io.swagger.models.parameters.QueryParameter;

public class SwaggerMetadataProcessor implements MetadataProcessor<WSConnection>{
    
    private static final String PATH_SEPARATOR = File.separator; 
    private static final String CATALOG_SEPARATOR = "_"; //$NON-NLS-1$
    private static final String PARAMETER_SEPARATOR = "&"; //$NON-NLS-1$
    private static final String PARAMETER_SEPARATOR_FIRST = "?"; //$NON-NLS-1$
    private static final String PARAMETER_SEPARATOR_EQUAL = "="; //$NON-NLS-1$
    private static final String IS_PATH_PARAMETER = "isPathParam"; //$NON-NLS-1$
    private static final String HTTP_ACTION = "httpAction"; //$NON-NLS-1$
    private static final String BASE_URL = "restBaseUrl"; //$NON-NLS-1$
    private static final String HTTP_HOST = "httpHost"; //$NON-NLS-1$
    
    private SwaggerExecutionFactory ef;
    private String httpHost = "http://localhost:8080"; //$NON-NLS-1$
    private String baseUrl = "/"; //$NON-NLS-1$
    
    protected void setExecutionfactory(SwaggerExecutionFactory ef) {
        this.ef = ef;
    }
    
    @Override
    public void process(MetadataFactory mf, WSConnection connection) throws TranslatorException {
        
        Swagger swagger = getSchema(connection);
        baseUrl = swagger.getBasePath();
        if(swagger.getHost() != null && !swagger.getHost().equals("")) { //$NON-NLS-1$
            httpHost = "http://" + swagger.getHost();//$NON-NLS-1$
        }
        
        for(Entry<String, Path> entry : swagger.getPaths().entrySet()) {
            addProcedure(mf, entry.getKey(), entry.getValue());
        }
    }
    
    private void addProcedure(MetadataFactory mf, String key, Path path) {

        String action = getHttpAction(path);
        if(null == action){
            return ;
        }
        
        String procName = formProcName(key);
        Procedure procedure = mf.addProcedure(procName);
        procedure.setVirtual(false);
        procedure.setNameInSource(procName);
        procedure.setProperty(HTTP_ACTION, action);
        procedure.setProperty(BASE_URL, baseUrl);
        procedure.setProperty(HTTP_HOST, httpHost); 
        
        mf.addProcedureParameter("result", TypeFacility.RUNTIME_NAMES.BLOB, Type.ReturnValue, procedure); //$NON-NLS-1$ 
        
        ProcedureParameter param = null;
        
        for(Parameter parameter : getApiParams(path)) {
            String name = parameter.getName();
            String dataType = TypeFacility.RUNTIME_NAMES.STRING;
            String isPathParam = "true"; //$NON-NLS-1$
            if(parameter instanceof PathParameter) {
                PathParameter p  = (PathParameter) parameter;
                String type = p.getType();
                dataType = DataTypeManager.getDataTypeName(TypeFacility.getDataTypeClass(type));
            } else if(parameter instanceof QueryParameter) {
                QueryParameter p  = (QueryParameter) parameter;
                String type = p.getType();
                dataType = DataTypeManager.getDataTypeName(TypeFacility.getDataTypeClass(type));
                isPathParam = "false"; //$NON-NLS-1$
            }
            param = mf.addProcedureParameter(name, dataType, Type.In, procedure); 
            param.setProperty(IS_PATH_PARAMETER, isPathParam); 
            param.setNullType(NullType.No_Nulls);
            param.setDefaultValue("false"); //$NON-NLS-1$
        }
        
        param = mf.addProcedureParameter("headers", TypeFacility.RUNTIME_NAMES.CLOB, Type.In, procedure); //$NON-NLS-1$
        param.setAnnotation("Headers to send"); //$NON-NLS-1$
        param.setNullType(NullType.Nullable);
        
        param = mf.addProcedureParameter("contentType", TypeFacility.RUNTIME_NAMES.STRING, Type.Out, procedure); //$NON-NLS-1$
        param.setAnnotation("return contentType"); //$NON-NLS-1$
        param.setNullType(NullType.Nullable);
    }
    
    private Swagger getSchema(WSConnection conn) throws TranslatorException {
        if (this.ef != null) {
            return this.ef.getSchema(conn);
        }
        return null;
    }

    private String getHttpAction(Path path) {
        if(path.getGet() != null) {
            return "GET";
        } else if (path.getPost() != null) {
            return "POST";
        } else if (path.getDelete() != null) {
            return "DELETE";
        } else if (path.getPut() != null) {
            return "PUT";
        }
        return null;
    }

    private List<Parameter> getApiParams(Path path) {
        
        for(Operation operation : path.getOperations()) {
            return operation.getParameters();
        }  
        
        return new ArrayList<Parameter>();
    }

    private String formProcName(String path) {
        
        String paramParenthesis = "{" ; //$NON-NLS-1$
        
        int endIndex = path.length();
        if(path.contains(paramParenthesis)) {
            endIndex = path.indexOf(paramParenthesis);
        }
        path = path.substring(0, endIndex);
        
        if(path.startsWith(PATH_SEPARATOR)){
            path = path.substring(1);
        }
        
        if(path.endsWith(PATH_SEPARATOR)) {
            path = path.substring(0, path.length() - 1);
        }
        
        return path.replace(PATH_SEPARATOR, CATALOG_SEPARATOR);
    }

    @TranslatorProperty(display = "HttpHost", category = PropertyType.IMPORT, description = "Rest Service Server Http Host")
    public String getHttpHost() {
        return httpHost;
    }

    public void setHttpHost(String httpHost) {
        this.httpHost = httpHost;
    }
    
    static String getPathSeparator(){
        return PATH_SEPARATOR;
    }
    
    static String getCatalogSeparator(){
        return CATALOG_SEPARATOR;
    }
    
    static String getParamSeparator(){
        return PARAMETER_SEPARATOR;
    }
    
    static String getParamSeparatorFirst(){
        return PARAMETER_SEPARATOR_FIRST;
    }
    
    static String getParamSeparatorEqual(){
        return PARAMETER_SEPARATOR_EQUAL;
    }
    
    static Action getHttpAction(Procedure procedure) {
        return Action.valueOf(procedure.getProperty(HTTP_ACTION, false));
    }
    
    static String getHttpHost(Procedure procedure) {
        return procedure.getProperty(HTTP_HOST, false);
    }
    
    static String getBaseUrl(Procedure procedure) {
        return procedure.getProperty(BASE_URL, false);
    }
    
    static boolean isPathParam(ProcedureParameter param){
        String value = param.getProperty(IS_PATH_PARAMETER, false); 
        return value.equals("true"); //$NON-NLS-1$
    }
    
    static boolean isEndPointParam(ProcedureParameter param){
        return param.getName().equals("headers") || param.getName().equals("contentType") || param.getName().equals("body"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }
    
    static boolean isPayload(ProcedureParameter param){
        return param.getType().equals(Type.In) && param.getName().equals("body"); //$NON-NLS-1$
    }
    
}
