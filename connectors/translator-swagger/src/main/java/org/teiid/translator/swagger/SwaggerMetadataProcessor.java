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
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

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
import org.teiid.translator.swagger.SwaggerExecutionFactory.ResultsType;
import org.teiid.translator.swagger.SwaggerExecutionFactory.SecurityType;
import org.teiid.translator.swagger.SwaggerProcedureExecution.Pair;

import io.swagger.models.HttpMethod;
import io.swagger.models.Model;
import io.swagger.models.Operation;
import io.swagger.models.Path;
import io.swagger.models.Response;
import io.swagger.models.Swagger;
import io.swagger.models.auth.SecuritySchemeDefinition;
import io.swagger.models.parameters.Parameter;
import io.swagger.models.parameters.PathParameter;
import io.swagger.models.parameters.QueryParameter;
import io.swagger.models.properties.ArrayProperty;
import io.swagger.models.properties.Property;
import io.swagger.models.properties.RefProperty;

public class SwaggerMetadataProcessor implements MetadataProcessor<WSConnection>{
    
    static class ModelWrapper{
        
        private String pname;
        private Map<String, Model> definitions;
        private Map<String, Property> properties;
        
        public ModelWrapper(String pname, Map<String, Model> definitions, Model model){
            this.pname = pname;
            this.definitions = definitions;
            this.properties = new LinkedHashMap<String, Property>();
            visit(model.getProperties());
        }

        public String getPname() {
            return pname;
        }

        private void visit(Map<String, Property> properties) {
            for(Entry<String, Property> entry : properties.entrySet()) {
                String key = entry.getKey();
                Property value = entry.getValue();
                if(value.getType().equals("ref")){
                    RefProperty refProperty = (RefProperty)value;
                    Model model = this.definitions.get(refProperty.getSimpleRef());
                    ModelWrapper wrapper = new ModelWrapper(this.getPname() == null ? key : this.getPname() + PATH_SEPARATOR + key, definitions, model);
                    this.properties.putAll(wrapper.getProperties());
                } else {
                    this.properties.put(this.getPname() == null ? key : this.getPname() + PATH_SEPARATOR + key, value);
                }
                
            }
        }

        public Map<String, Property> getProperties() {
            return properties;
        }
    }
    
    private static final String PATH_SEPARATOR = File.separator; 
    private static final String COMMA_SEPARATOR = ","; //$NON-NLS-1$
    private static final String PROCEDURE_PRODUCES = "produces"; //$NON-NLS-1$
    private static final String PROCEDURE_CONSUMES = "consumes"; //$NON-NLS-1$
    private static final String PROCEDURE_SECURITY = "security"; //$NON-NLS-1$
    private static final String PROCEDURE_RETURN = "returnType"; //$NON-NLS-1$
    private static final String PARAMETER_SEPARATOR = "&"; //$NON-NLS-1$
    private static final String PARAMETER_SEPARATOR_FIRST = "?"; //$NON-NLS-1$
    private static final String PARAMETER_SEPARATOR_EQUAL = "="; //$NON-NLS-1$
    private static final String IS_PATH_PARAMETER = "isPathParam"; //$NON-NLS-1$
    private static final String BASE_URL = "restBaseUrl"; //$NON-NLS-1$
    private static final String HTTP_HOST = "httpHost"; //$NON-NLS-1$
    private static final String HTTP_ACTION = "httpAction"; //$NON-NLS-1$
    private static final String HTTP_API_PATH = "restApiPath"; //$NON-NLS-1$
    
    private static final String TRUE_STRING = "true"; //$NON-NLS-1$
    private static final String FALSE_STRING = "false"; //$NON-NLS-1$
    
    private SwaggerExecutionFactory ef;
    private String httpHost = "http://localhost:8080"; //$NON-NLS-1$
    private String baseUrl = "/"; //$NON-NLS-1$
    private Map<String, SecuritySchemeDefinition> securityDefinitions;
    private Map<String, Model> definitions;
    
    protected void setExecutionfactory(SwaggerExecutionFactory ef) {
        this.ef = ef;
    }
    
    @Override
    public void process(MetadataFactory mf, WSConnection connection) throws TranslatorException {
        
        Swagger swagger = getSchema(connection);
        baseUrl = swagger.getBasePath();
        if(swagger.getHost() != null && !swagger.getHost().equals("")) { //$NON-NLS-1$
            String scheme = "http"; //$NON-NLS-1$
            if(swagger.getSchemes().size() > 0) {
                scheme = swagger.getSchemes().get(0).toValue();
            }
            httpHost = scheme + "://" + swagger.getHost();//$NON-NLS-1$
        }
        
        this.securityDefinitions = swagger.getSecurityDefinitions();
        this.definitions = swagger.getDefinitions();
//        this.tags = swagger.getTags();
        
        for(Entry<String, Path> entry : swagger.getPaths().entrySet()) {
            addProcedure(mf, entry.getKey(), entry.getValue());
        }
    }
    
    private void addProcedure(MetadataFactory mf, String key, Path path) {
        
        for(Entry<HttpMethod, Operation> entry : path.getOperationMap().entrySet()){
            
            String action = entry.getKey().toString();
            
            Operation operation = entry.getValue();
            String procName = operation.getOperationId();
            String produces = getProduceTypes(operation);
            String consumes = getConsumeTypes(operation);
            String annotation = getOperationSummary(operation);
            String securityType = getSecurityType(operation);
            
            Procedure procedure = mf.addProcedure(procName);
            procedure.setVirtual(false);
            procedure.setNameInSource(procName);
            procedure.setProperty(HTTP_ACTION, action);
            procedure.setProperty(HTTP_API_PATH, key);
            procedure.setProperty(BASE_URL, baseUrl);
            procedure.setProperty(HTTP_HOST, httpHost); 
            procedure.setProperty(PROCEDURE_PRODUCES, produces);
            procedure.setProperty(PROCEDURE_CONSUMES, consumes);
            if(null != securityType) {
                procedure.setProperty(PROCEDURE_SECURITY, securityType);
            }
            procedure.setAnnotation(annotation);
            
            addProcedureParameter(mf, procedure, operation);
            
            ProcedureParameter param = mf.addProcedureParameter("headers", TypeFacility.RUNTIME_NAMES.CLOB, Type.In, procedure); //$NON-NLS-1$
            param.setAnnotation("Headers to send"); //$NON-NLS-1$
            param.setNullType(NullType.Nullable); 
            
            Map<String, Response> respMap = operation.getResponses();
            Response resp = respMap.get("200");
            
            if(isRef(resp)){
                Model model = getReferenceModel(resp.getSchema());
                String returnType = resp.getSchema().getType();
                if(null != model){
                    ModelWrapper wrapper = new ModelWrapper(null, this.definitions, model);
                    for(Entry<String, Property> column : wrapper.getProperties().entrySet()){
                        String name = column.getKey();
                        Property prop = column.getValue();
                        String type = SwaggerTypeManager.teiidType(prop.getType(), prop.getFormat());
                        mf.addProcedureResultSetColumn(name, type, procedure);
                    }
                    procedure.setProperty(PROCEDURE_RETURN, returnType);
                }
            } else {
                mf.addProcedureParameter("result", TypeFacility.RUNTIME_NAMES.OBJECT, Type.ReturnValue, procedure); //$NON-NLS-1$ 
            }
        }

    }

    private Model getReferenceModel(Property schema) {
        if(schema.getType().equals("ref")) {
            RefProperty ref = (RefProperty) schema;
            return definitions.get(ref.getSimpleRef());
        } else if(schema.getType().equals("array")) {
            ArrayProperty arrSchema = (ArrayProperty) schema;
            Property subSchema = arrSchema.getItems();
            if(subSchema.getType().equals("ref")){
                RefProperty ref = (RefProperty) subSchema;
                return definitions.get(ref.getSimpleRef());
            }
        }
        return null;
    }

    private boolean isRef(Response resp) {
        
        if(resp != null) {
            Property schema = resp.getSchema();
            String type = schema.getType();
            if(type.equals("ref")) {
                return true ;
            } else if(type.equals("array")) {
                ArrayProperty arrProp = (ArrayProperty) schema;
                Property subSchema = arrProp.getItems();
                if(subSchema.getType().equals("ref")){
                    return true;
                }
            }
        }
        return false;
    }

    private String getSecurityType(Operation operation) {
        String type = null;
        if(null != operation.getSecurity()){
            Map<String, List<String>> map = operation.getSecurity().get(0);
            String key = map.keySet().iterator().next();
            SecuritySchemeDefinition def = securityDefinitions.get(key);
            type = def.getType();
        }
        return type;
    }

    private void addProcedureParameter(MetadataFactory mf, Procedure procedure, Operation operation) {
        
        for(Parameter parameter : operation.getParameters()) {
            String name = parameter.getName();
            String dataType = TypeFacility.RUNTIME_NAMES.STRING;
            String isPathParam = TRUE_STRING; //$NON-NLS-1$
            if(parameter instanceof PathParameter) {
                PathParameter p  = (PathParameter) parameter;
                String type = p.getType();
                dataType = DataTypeManager.getDataTypeName(TypeFacility.getDataTypeClass(type));
            } else if(parameter instanceof QueryParameter) {
                QueryParameter p  = (QueryParameter) parameter;
                String type = p.getType();
                dataType = DataTypeManager.getDataTypeName(TypeFacility.getDataTypeClass(type));
                isPathParam = FALSE_STRING; //$NON-NLS-1$
            }
            ProcedureParameter param = mf.addProcedureParameter(name, dataType, Type.In, procedure); 
            param.setProperty(IS_PATH_PARAMETER, isPathParam); 
            param.setNullType(NullType.No_Nulls);
            param.setAnnotation(parameter.getDescription());
        }
        
    }

    private String getOperationSummary(Operation operation) {
        String description = operation.getDescription();
        if(description == null || description.equals("")) { //$NON-NLS-1$
            description = operation.getSummary();
        }     
        return description;
    }

    private String getProduceTypes(Operation operation) {
        String produces = ""; //$NON-NLS-1$
        boolean first = true;
        if(operation.getProduces() != null) {
            for(String produce : operation.getProduces()){
                if(first) {
                    produces += produce;
                    first = false;
                } else {
                    produces += COMMA_SEPARATOR;
                    produces += produce;
                }
            }
        }
        return produces;
    }

    private String getConsumeTypes(Operation operation) {
        String consumes = "text/xml; charset=utf-8"; //$NON-NLS-1$
        if(operation.getConsumes() != null){
            for(String consume : operation.getConsumes()){
                consumes += COMMA_SEPARATOR;
                consumes += consume;
            }
        }
        return consumes;
    }


    private Swagger getSchema(WSConnection conn) throws TranslatorException {
        if (this.ef != null) {
            return this.ef.getSchema(conn);
        }
        return null;
    }

    @TranslatorProperty(display = "HttpHost", category = PropertyType.IMPORT, description = "Rest Service Server Http Host")
    public String getHttpHost() {
        return httpHost;
    }

    public void setHttpHost(String httpHost) {
        this.httpHost = httpHost;
    }
    
    static String typeFormat(String type, String format){
        return type + PATH_SEPARATOR + format;
    }
    
    static String getPathSeparator(){
        return PATH_SEPARATOR;
    }
    
    static Action getHttpAction(Procedure procedure) {
        return Action.valueOf(procedure.getProperty(HTTP_ACTION, false));
    }
    
    static SecurityType getSecurityType(Procedure procedure){
        String type = procedure.getProperty(PROCEDURE_SECURITY, false);
        if(type != null && type.equals(SecurityType.BASIC.getType())) {
            return SecurityType.BASIC;
        } else if(type != null && type.equals(SecurityType.OAUTH2.getType())){
            return SecurityType.OAUTH2;
        } else {
            return SecurityType.NO;
        }
    }
    
    static ResultsType getReturnType(Procedure procedure) {
        String type = procedure.getProperty(PROCEDURE_RETURN, false);
        if(type != null && type.equals(ResultsType.REF.getType())){
            return ResultsType.REF;
        } else if(type != null && type.equals(ResultsType.ARRAY.getType())){
            return ResultsType.ARRAY;
        } else {
            return ResultsType.COMPLEX;
        }
    }
    
    static String getHttpHost(Procedure procedure) {
        return procedure.getProperty(HTTP_HOST, false);
    }
    
    static String getBaseUrl(Procedure procedure) {
        return procedure.getProperty(BASE_URL, false);
    }
    
    static String getHttpPath(Procedure procedure, List<Pair> pairs) {
        String path = procedure.getProperty(HTTP_API_PATH, false);
        List<Pair> queryParams = new ArrayList<Pair>(pairs.size());
        for(Pair pair : pairs) {
            if(pair.isIdPath()) {
                String regex = "\\{" + pair.getName() + "\\}"; //$NON-NLS-1$ //$NON-NLS-2$
                path = path.replaceAll(regex, pair.getValue());
            } else {
                queryParams.add(pair);
            }
        }
        if(queryParams.size() > 0) {
            StringBuilder b = new StringBuilder();
            b.append(PARAMETER_SEPARATOR_FIRST);
            for (Pair queryParam : queryParams){
                if (!queryParam.getName().isEmpty()) {
                    b.append(escapeString(queryParam.getName()));
                    b.append(PARAMETER_SEPARATOR_EQUAL);
                    b.append(escapeString(queryParam.getValue()));
                    b.append(PARAMETER_SEPARATOR);
                  }
            }
            String querystring = b.substring(0, b.length() - 1);
            path += querystring ;
        }
        return path ;
    }
    
    private static Object escapeString(String str) {
        try {
            return URLEncoder.encode(str, "utf8").replaceAll("\\+", "%20"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
          } catch (UnsupportedEncodingException e) {
            return str;
          }
    }

    static Set<String> getProcuces(Procedure procedure) {
        Set<String> produceSet = new HashSet<String>();
        String produces = procedure.getProperty(PROCEDURE_PRODUCES, false);
        String[] array = produces.split(COMMA_SEPARATOR);
        for(String produce : array){
            if(!produce.equals("")) { //$NON-NLS-1$
                produceSet.add(produce);
            }
        }
        return produceSet;
    }
    
    static Set<String> getConsumes(Procedure procedure) {
        Set<String> consumeSet = new HashSet<String>();
        String consumes = procedure.getProperty(PROCEDURE_CONSUMES, false);
        String[] array = consumes.split(COMMA_SEPARATOR);
        for(String produce : array){
            if(!produce.equals("")) { //$NON-NLS-1$
                consumeSet.add(produce);
            }
        }
        return consumeSet;
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
