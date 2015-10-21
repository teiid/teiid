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
import java.util.Map;

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

import io.swagger.models.Operation;
import io.swagger.models.Path;
import io.swagger.models.Swagger;
import io.swagger.models.parameters.Parameter;
import io.swagger.models.parameters.PathParameter;
import io.swagger.models.parameters.QueryParameter;
import io.swagger.parser.SwaggerParser;

public class SwaggerMetadataProcessor implements MetadataProcessor<WSConnection>{
    
    private String httpHost = "http://localhost:8080"; //$NON-NLS-1$
    
    private String pathSeparator = File.separator; 
    private String paramParenthesis = "{"; //$NON-NLS-1$
    private String catalogSeparator = "_"; //$NON-NLS-1$
    
    private String isPathParamPropertyKey = "isPathParam"; //$NON-NLS-1$
    private String actionPropertyKey = "httpAction"; //$NON-NLS-1$
    private String baseUrlPropertyKey = "restBaseUrl"; //$NON-NLS-1$
    private String pathSeparatorPropertyKey = "pathSeparator"; //$NON-NLS-1$
    private String catalogSeparatorPropertyKey = "catalogSeparator"; //$NON-NLS-1$
    private String httpHostPropertyKey = "httpHost"; //$NON-NLS-1$
    
    private Swagger swagger;
    
    public SwaggerMetadataProcessor(String url){
        swagger = new SwaggerParser().read(url);
    }
    
    @Override
    public void process(MetadataFactory mf, WSConnection connection) throws TranslatorException {
        
        Map<String, Path> pathMap = swagger.getPaths();
        
        for(String key : pathMap.keySet()) {
            Path path = pathMap.get(key);
            String action = getHttpAction(path);
            if(null == action){
                continue;
            }
            String procName = formProcName(key);
            Procedure procedure = mf.addProcedure(procName);
            procedure.setVirtual(false);
            procedure.setNameInSource(procName);
            procedure.setProperty(actionPropertyKey, action);
            procedure.setProperty(baseUrlPropertyKey, swagger.getBasePath());
            procedure.setProperty(pathSeparatorPropertyKey, pathSeparator);
            procedure.setProperty(catalogSeparatorPropertyKey, catalogSeparator);
            procedure.setProperty(httpHostPropertyKey, httpHost);
            
            mf.addProcedureParameter("result", TypeFacility.RUNTIME_NAMES.BLOB, Type.ReturnValue, procedure); //$NON-NLS-1$            
            
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
                ProcedureParameter param = mf.addProcedureParameter(name, dataType, Type.In, procedure); 
                param.setProperty(isPathParamPropertyKey, isPathParam); 
                param.setNullType(NullType.No_Nulls);
                param.setDefaultValue("false"); //$NON-NLS-1$
            }
            
        }
    }
    
    private String getHttpAction(Path path) {
        if(path.getGet() != null) {
            return "GET";
        } else if (path.getPost() != null) {
            return "POST";
        }
        //TODO-- add support more action: PATCH, PUT
        return null;
    }

    private List<Parameter> getApiParams(Path path) {
        
        for(Operation operation : path.getOperations()) {
            return operation.getParameters();
        }  
        
        return new ArrayList<Parameter>();
    }

    private String formProcName(String path) {
        
        int endIndex = path.length();
        if(path.contains(paramParenthesis)) {
            endIndex = path.indexOf(paramParenthesis);
        }
        path = path.substring(0, endIndex);
        
        if(path.startsWith(pathSeparator)){
            path = path.substring(1);
        }
        
        if(path.endsWith(pathSeparator)) {
            path = path.substring(0, path.length() - 1);
        }
        
        return path.replace(pathSeparator, catalogSeparator);
    }

    @TranslatorProperty(display = "HttpHost", category = PropertyType.IMPORT, description = "Rest Service Server Http Host")
    public String getHttpHost() {
        return httpHost;
    }

    public void setHttpHost(String httpHost) {
        this.httpHost = httpHost;
    }


    
}
