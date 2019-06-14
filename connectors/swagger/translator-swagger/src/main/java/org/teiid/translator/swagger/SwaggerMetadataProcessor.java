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

import java.io.File;
import java.sql.Blob;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.teiid.core.types.DataTypeManager;
import org.teiid.metadata.BaseColumn;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.Column;
import org.teiid.metadata.ExtensionMetadataProperty;
import org.teiid.metadata.MetadataException;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.ProcedureParameter.Type;
import org.teiid.metadata.RestMetadataExtension;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.TranslatorProperty.PropertyType;
import org.teiid.translator.ws.BinaryWSProcedureExecution;
import org.teiid.translator.ws.WSConnection;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.swagger.models.HttpMethod;
import io.swagger.models.Model;
import io.swagger.models.ModelImpl;
import io.swagger.models.Operation;
import io.swagger.models.Path;
import io.swagger.models.RefModel;
import io.swagger.models.Response;
import io.swagger.models.Scheme;
import io.swagger.models.Swagger;
import io.swagger.models.parameters.AbstractSerializableParameter;
import io.swagger.models.parameters.BodyParameter;
import io.swagger.models.parameters.Parameter;
import io.swagger.models.properties.ArrayProperty;
import io.swagger.models.properties.FileProperty;
import io.swagger.models.properties.MapProperty;
import io.swagger.models.properties.ObjectProperty;
import io.swagger.models.properties.Property;
import io.swagger.models.properties.RefProperty;
import io.swagger.parser.SwaggerParser;

public class SwaggerMetadataProcessor implements MetadataProcessor<WSConnection>{
    private static final String ARRAY_SUFFIX = "[]";
    public static final String KEY_NAME = "key_name";
    public static final String KEY_VALUE = "key_value";

    @ExtensionMetadataProperty(applicable=Procedure.class, datatype=String.class, display="URI",
            description="Used to define endpoint of the procedure", required=true)
    public final static String URI = RestMetadataExtension.URI;

    @ExtensionMetadataProperty(applicable=Procedure.class, datatype=String.class, display="Http Method",
            description="Http method used to execute the procedure", required=true,
            allowed="GET,POST,PUT,DELETE,OPTIONS,HEAD,PATCH")
    public final static String METHOD = RestMetadataExtension.METHOD;

    @ExtensionMetadataProperty(applicable=Procedure.class, datatype=String.class, display="Scheme",
            description="Scheme to use http, https etc.",
            allowed="HTTP,HTTPS")
    public final static String SCHEME = RestMetadataExtension.SCHEME;

    @ExtensionMetadataProperty(applicable=Procedure.class, datatype=String.class, display="Produces",
            description="Used to define content type produced by this procedure, default JSON assumed")
    public final static String PRODUCES = RestMetadataExtension.PRODUCES;

    @ExtensionMetadataProperty(applicable=Procedure.class, datatype=String.class, display="Consumes",
            description="Used to define content type consumed by this procedure with body type parameters. Default JSON assumed")
    public final static String CONSUMES = RestMetadataExtension.CONSUMES;

    @ExtensionMetadataProperty(applicable=Procedure.class, datatype=String.class, display="Charset",
            description="Encoding of the return data")
    public final static String CHARSET = RestMetadataExtension.CHARSET;

    @ExtensionMetadataProperty(applicable=ProcedureParameter.class, datatype=String.class, display="Parameter Type",
            description="Parameter type, as to how the parameter is being provided to the procedure", required=true,
            allowed="PATH,QUERY,FORM,FORMDATA,BODY,HEADER")
    public final static String PARAMETER_TYPE = RestMetadataExtension.PARAMETER_TYPE;

    @ExtensionMetadataProperty(applicable=ProcedureParameter.class, datatype=String.class, display="Collection Format",
            description="Determines the format of the array if type array is used, like CSV,TSV etc.",
            allowed="CSV,SSV,TSV,PIPES,MULTI")
    public final static String COLLECION_FORMAT = RestMetadataExtension.COLLECION_FORMAT;


    private String swaggerFilePath;
    private boolean useDefaultHost = true;
    private String preferredScheme;
    private String preferredProduces = "application/json";
    private String preferredConsumes = "application/json";
    private SwaggerExecutionFactory ef;

    public SwaggerMetadataProcessor(SwaggerExecutionFactory ef) {
        this.ef = ef;
    }

    @TranslatorProperty(display="Swagger metadata file path", category=PropertyType.IMPORT,
            description="Swagger metadata file path.")
    public String getSwaggerFilePath() {
        return swaggerFilePath;
    }

    public void setSwaggerFilePath(String swaggerFilePath) {
        this.swaggerFilePath = swaggerFilePath;
    }

    @TranslatorProperty(display="Use Host from Swagger File", category=PropertyType.IMPORT,
            description="Use default host specified in the Swagger file; Defaults to true")
    public boolean isUseDefaultHost() {
        return this.useDefaultHost;
    }

    public void setUseDefaultHost(boolean useDefault) {
        this.useDefaultHost = useDefault;
    }

    @TranslatorProperty(display="Preferred Scheme", category=PropertyType.IMPORT,
            description="Preferred Scheme to use when Swagger file supports multiple invocation schemes like http, https etc.")
    public String getPreferredScheme() {
        return this.preferredScheme;
    }

    public void setPreferredScheme(String scheme) {
        this.preferredScheme = scheme;
    }

    @TranslatorProperty(display="Preferred Accept Header", category=PropertyType.IMPORT,
            description="Preferred Accept MIME type header, this should be one of the Swagger "
                    + "'produces' types; default is application/json")
    public String getPreferredProduces() {
        return this.preferredProduces;
    }

    public void setPreferredProduces(String accept) {
        this.preferredProduces = accept;
    }

    @TranslatorProperty(display="Preferred Content-type Header", category=PropertyType.IMPORT,
            description="Preferred Content-type header, this should be one of the Swagger 'consume' "
                    + "types, default is application/json")
    public String getPreferredConsumes() {
        return this.preferredConsumes;
    }

    public void setPreferredConsumes(String type) {
        this.preferredConsumes = type;
    }

    @Override
    public void process(MetadataFactory mf, WSConnection connection) throws TranslatorException {
        Swagger swagger = getSchema(connection);
        String basePath = swagger.getBasePath();
        if (basePath == null) {
            basePath = "/"; //$NON-NLS-1$
        }
        String scheme = null;
        if(swagger.getSchemes().size() > 0) {
            if (this.preferredScheme == null) {
                scheme = swagger.getSchemes().get(0).toValue();
            } else {
                for (Scheme s : swagger.getSchemes()) {
                    if (s.toValue().equalsIgnoreCase(this.preferredScheme)) {
                        scheme = s.toValue();
                        break;
                    }
                }
                if (scheme == null) {
                    scheme = swagger.getSchemes().get(0).toValue();
                }
            }
        } else {
            scheme = preferredScheme;
            if (scheme == null) {
                scheme = "https"; //$NON-NLS-1$
            }
        }

        String httpHost = null;
        if(swagger.getHost() != null && !swagger.getHost().trim().isEmpty()) {
            httpHost = scheme + "://" + swagger.getHost();//$NON-NLS-1$
        }

        if (this.useDefaultHost && httpHost != null) {
            httpHost = httpHost + basePath;
        }

        for(Entry<String, Path> entry : swagger.getPaths().entrySet()) {
            addProcedure(mf, swagger,
                    ((httpHost != null) ? httpHost : basePath), entry.getKey(),
                    entry.getValue());
        }
    }

    private String buildURL(String basePath, String endpoint) {
        if (endpoint.startsWith("/")) {
            if (basePath.endsWith("/")) {
                return basePath+endpoint.substring(1);
            } else {
                return basePath+endpoint;
            }
        }
        if (basePath.endsWith("/")) {
            return basePath+endpoint;
        }
        return basePath+"/"+endpoint;
    }

    private String getSchemes(Operation op) {
        StringBuilder sb = new StringBuilder();
        for (Scheme s:op.getSchemes()) {
            if (sb.length() > 0) {
                sb.append(',');
            }
            sb.append(s.name());
        }
        return sb.toString();
    }

    private static <T> T nvl(T value, T ifNull) {
        if (value == null) {
            return ifNull;
        }
        return value;
    }

    //to maintain a stable order
    public static Map<HttpMethod, Operation> getOperationMap(Path operations) {
        Map<HttpMethod, Operation> result = new LinkedHashMap<HttpMethod, Operation>();

        if (operations.getGet() != null) {
            result.put(HttpMethod.GET, operations.getGet());
        }
        if (operations.getPut() != null) {
            result.put(HttpMethod.PUT, operations.getPut());
        }
        if (operations.getPost() != null) {
            result.put(HttpMethod.POST, operations.getPost());
        }
        if (operations.getDelete() != null) {
            result.put(HttpMethod.DELETE, operations.getDelete());
        }
        if (operations.getPatch() != null) {
            result.put(HttpMethod.PATCH, operations.getPatch());
        }
        if (operations.getHead() != null) {
            result.put(HttpMethod.HEAD, operations.getHead());
        }
        if (operations.getOptions() != null) {
            result.put(HttpMethod.OPTIONS, operations.getOptions());
        }

        return result;
    }


    private void addProcedure(MetadataFactory mf, Swagger swagger,
            String basePath, String endpoint, Path operations)
            throws TranslatorException {

        for(Entry<HttpMethod, Operation> entry : getOperationMap(operations).entrySet()){

            Operation operation = entry.getValue();
            String produces = getTypes(nvl(operation.getProduces(), swagger.getProduces()), getPreferredProduces());
            String consumes = getTypes(nvl(operation.getConsumes(), swagger.getConsumes()), getPreferredConsumes());
            String name = operation.getOperationId();
            if (name == null) {
                //determine a name from the endpoint
                int start = 0;
                if (endpoint.startsWith("/")) { //$NON-NLS-1$
                    start = 1;
                }
                name = endpoint.substring(start);
                name = name.replaceAll("\\{([^}]*)\\}", "$1"); //$NON-NLS-1$ //$NON-NLS-2$
                if (operations.getOperationMap().entrySet().size() > 1) {
                    name = name + "_" + entry.getKey().name(); //$NON-NLS-1$
                }
            }

            Procedure procedure = mf.addProcedure(name);
            procedure.setVirtual(false);
            procedure.setProperty(METHOD, entry.getKey().name());
            if (operation.getSchemes() != null && !operation.getSchemes().isEmpty()) {
                procedure.setProperty(SCHEME, getSchemes(operation));
            }
            procedure.setProperty(URI, buildURL(basePath, endpoint));
            procedure.setProperty(PRODUCES, produces);
            procedure.setProperty(CONSUMES, consumes);
            procedure.setAnnotation(getOperationSummary(operation));
            for (Entry<String, Object> extension:operation.getVendorExtensions().entrySet()) {
                procedure.setProperty(extension.getKey(), extension.getValue().toString());
            }

            addProcedureParameters(mf, swagger, procedure, operation);

            boolean returnAdded = false;
            Map<String, Response> respMap = operation.getResponses();
            for (String code : respMap.keySet()) {
                if (code.equalsIgnoreCase("default")) {
                    continue;
                }
                int httpCode = Integer.valueOf(code);
                // Success codes
                if (httpCode > 100 && httpCode < 300) {
                    Response resp = respMap.get(code);
                    returnAdded = buildResponse(mf, swagger, procedure, resp);
                    break;
                }
            }

            if (!returnAdded && respMap.get("default") != null) {
                Response resp = respMap.get("default");
                returnAdded = buildResponse(mf, swagger, procedure, resp);
            }

        }
    }

    private boolean buildResponse(final MetadataFactory mf, final Swagger swagger,
            final Procedure procedure, final Response resp) throws TranslatorException {
        PropertyAction pa = new PropertyAction() {
            @Override
            public void execute(String name, String nameInSource,
                    Property property, boolean array) {
                String type = getPropertyType(property, array);
                Column c = mf.addProcedureResultSetColumn(name, type, procedure);
                if (!name.equalsIgnoreCase(nameInSource)) {
                    c.setNameInSource(nameInSource);
                }
            }
        };

        Property schema = resp.getSchema();
        if (schema != null) {
            if (isSimple(schema)) {
                boolean array = false;
                if (schema instanceof ArrayProperty) {
                    schema = ((ArrayProperty)schema).getItems();
                    array = true;
                }
                if(resp.getHeaders() == null|| resp.getHeaders().isEmpty()) {
                    String type = SwaggerTypeManager.teiidType(schema.getType(), schema.getFormat(), array);
                    mf.addProcedureParameter("return", type, ProcedureParameter.Type.ReturnValue, procedure);
                } else {
                    Map<String, Property> properties = new LinkedHashMap<String, Property>();
                    properties.put("return", schema);
                    walkProperties(swagger, properties, null, null, pa);
                }
            } else {
                // since the return is always a collection unwrap the array without any issues.
                if (schema instanceof ArrayProperty) {
                    schema = ((ArrayProperty)schema).getItems();
                }

                if (schema instanceof ObjectProperty) {
                    walkProperties(swagger, ((ObjectProperty)schema).getProperties(),
                            null,
                            null,
                            pa);
                } else if (schema instanceof RefProperty) {
                    String modelName = ((RefProperty)schema).getSimpleRef();
                    Model model = swagger.getDefinitions().get(modelName);
                    walkProperties(swagger, model.getProperties(),
                            null,
                            null,
                            pa);
                } else if (schema instanceof MapProperty){
                    Property property = ((MapProperty)schema).getAdditionalProperties();
                    String type = SwaggerTypeManager.teiidType(property.getType(), property.getFormat(), false);
                    Column c = mf.addProcedureResultSetColumn(KEY_NAME, "string", procedure);
                    c.setNameInSource(KEY_NAME);
                    c = mf.addProcedureResultSetColumn(KEY_VALUE, type, procedure);
                    c.setNameInSource(KEY_VALUE);
                } else {
                    throw new TranslatorException("File properties are not supported");
                }
            }
        }

        Map<String, Property> headers = resp.getHeaders();
        if (headers != null && !headers.isEmpty()) {
            walkProperties(swagger, headers, null, null, pa);
        }
        return procedure.getResultSet() != null;
    }

    private boolean isSimple(Property property) {
        if (property instanceof ArrayProperty) {
            ArrayProperty ap = (ArrayProperty)property;
            return isSimple(ap.getItems());
        }
        else if (property instanceof RefProperty) {
            return false;
        }
        else if (property instanceof ObjectProperty) {
            return false;
        }
        else if (property instanceof MapProperty) {
            return false;
        }
        else if (property instanceof FileProperty) {
            return false;
        }
        return true;
    }

    interface PropertyAction {
        void execute(String name, String nameInSource,
                    Property property, boolean array);
    }

    private void walkProperties(final Swagger swagger,
            final Map<String,Property> properties, final String namePrefix,
            final String nisPrefix, final PropertyAction pa) {
        walkProperties(swagger, new HashSet<Property>(), properties, namePrefix, nisPrefix, pa);
    }

    private void walkProperties(final Swagger swagger, Set<Property> parents,
            final Map<String,Property> properties, final String namePrefix,
            final String nisPrefix, final PropertyAction pa) {

        if (properties == null) {
            return;
        }

        final PropertyVisitor visitor = new PropertyVisitor() {
            @Override
            public void visit(String name, Property property) {
                pa.execute(fqn(namePrefix,name), nis(nisPrefix, name, false), property, false);
            }
            @Override
            public void visit(String name, ArrayProperty property) {
                if (isSimple(property)) {
                    // the array type defined in the type of the property
                    pa.execute(fqn(namePrefix,name), nis(nisPrefix,name, false), property.getItems(), true);
                } else {
                    // if Object or Ref, array does not matter as return is already a resultset.
                    Property items = property.getItems();
                    parents.add(property);
                    if (items instanceof ObjectProperty) {
                        String modelName = ((ObjectProperty)items).getName();
                        walkProperties(swagger, parents,
                                ((ObjectProperty) items).getProperties(),
                                fqn(fqn(namePrefix, name), modelName),
                                nis(nis(nisPrefix, name, true), modelName, false),
                                pa);
                    } else if (items instanceof RefProperty) {
                        String modelName = ((RefProperty)items).getSimpleRef();
                        Model model = swagger.getDefinitions().get(modelName);
                        walkProperties(swagger, parents, model.getProperties(),
                                fqn(fqn(namePrefix, name), modelName),
                                nis(nis(nisPrefix, name, true), modelName, false),
                                pa);
                    } else {
                        walkProperties(swagger, parents,
                                properties, fqn(namePrefix, name),
                                nis(nisPrefix, name, true), pa);
                    }
                    parents.remove(property);
                }
            }
            @Override
            public void visit(String name, FileProperty property) {
                //TODO:
            }
            @Override
            public void visit(String name, MapProperty property) {
                //TODO:
            }
            @Override
            public void visit(String name, ObjectProperty property) {
                parents.add(property);
                walkProperties(swagger, parents,
                        property.getProperties(), fqn(namePrefix, name),
                        nis(nisPrefix, name, false), pa);
                parents.remove(property);
            }
            @Override
            public void visit(String name, RefProperty property) {
                parents.add(property);
                Model model = swagger.getDefinitions().get(property.getSimpleRef());
                walkProperties(swagger, parents,
                        model.getProperties(), fqn(namePrefix, name),
                        nis(nisPrefix, name, false), pa);
                parents.remove(property);
            }
        };

        for (Entry<String, Property> p:properties.entrySet()) {
            if (parents.contains(p.getValue())) {
                //we don't yet handle recursive properties
                //TODO: could be an error condition
                continue;
            }
            visitor.accept(p.getKey(), p.getValue());
        }
    }

    private String fqn(String prefix, String name) {
        if (name == null) {
            return prefix;
        }
        return prefix == null?name:prefix+"_"+name;
    }

    private String nis(String prefix, String name, boolean array) {
        String nis = null;
        if (name == null) {
            nis = prefix;
        } else {
            nis = prefix == null?name:prefix+"/"+name;
        }
        if (array) {
            nis = nis+ARRAY_SUFFIX;
        }
        return nis;
    }

    private static String getPropertyType(Property property,
            boolean array) {
        String type = DataTypeManager.DefaultDataTypes.STRING;
        if (property != null) {
            type = SwaggerTypeManager.teiidType(property.getType(), property.getFormat(), array);
        } else if (array) {
            type += ARRAY_SUFFIX;
        }
        return type;
    }

    private void addProcedureParameters(final MetadataFactory mf, final Swagger swagger,
            final Procedure procedure, final Operation operation) throws TranslatorException {
        for(final Parameter parameter : operation.getParameters()) {

            if (parameter instanceof BodyParameter) {
                PropertyAction pa = new PropertyAction() {
                    @Override
                    public void execute(String name, String nameInSource,
                            Property property, boolean array) {
                        String type = getPropertyType(property, array);
                        if (procedure.getParameterByName(nameInSource) == null) {
                            ProcedureParameter param = mf.addProcedureParameter(name, type, Type.In, procedure);
                            param.setProperty(PARAMETER_TYPE, parameter.getIn());
                            if (property != null && !property.getRequired()) {
                                param.setProperty(BaseColumn.DEFAULT_HANDLING, BaseColumn.OMIT_DEFAULT);
                            }
                            param.setNullType(NullType.No_Nulls);
                            param.setAnnotation(property!=null?property.getDescription():null);
                            if (!name.equalsIgnoreCase(nameInSource)) {
                                param.setNameInSource(nameInSource);
                            }
                        }
                    }
                };
                Model model = ((BodyParameter)parameter).getSchema();
                if (model instanceof RefModel) {
                    RefModel refModel = (RefModel)model;
                    if (refModel.getProperties() != null) {
                        walkProperties(swagger, refModel.getProperties(), null, null, pa);
                    } else if (refModel.getReference() != null) {
                        Model m = swagger.getDefinitions().get(refModel.getSimpleRef());
                        walkProperties(swagger, m.getProperties(), null, null, pa);
                    }
                } else {
                    if ((model instanceof ModelImpl) && model.getProperties() != null) {
                        walkProperties(swagger, model.getProperties(), null, null, pa);
                    } else {
                        ProcedureParameter p = mf.addProcedureParameter(
                                parameter.getName(),
                                DataTypeManager.DefaultDataTypes.CLOB, Type.In,
                                procedure);
                        p.setProperty(PARAMETER_TYPE, parameter.getIn());
                        p.setNullType(NullType.No_Nulls);
                        p.setAnnotation(parameter.getDescription());
                    }
                }
            } else {
                String name = parameter.getName();
                ProcedureParameter pp = null;
                String type = null;
                Object defaultValue = null;
                String collectionFormat = null;

                if(parameter instanceof AbstractSerializableParameter) {
                    AbstractSerializableParameter p  = (AbstractSerializableParameter) parameter;
                    type = p.getType();
                    if (p.getType().equalsIgnoreCase("array")){
                        Property ap = p.getItems();
                        type = ap.getType();
                    }
                    type = SwaggerTypeManager.teiidType(type, p.getFormat(), p.getItems() != null);
                    defaultValue = p.getDefaultValue();
                    collectionFormat = p.getCollectionFormat();
                } else {
                    throw new MetadataException("Unknown property type" + parameter.getClass().getName()); //$NON-NLS-1$
                }

                pp = mf.addProcedureParameter(name, type, Type.In, procedure);
                pp.setProperty(PARAMETER_TYPE, parameter.getIn());

                boolean required = parameter.getRequired();
                if (!required) {
                    pp.setProperty(BaseColumn.DEFAULT_HANDLING, BaseColumn.OMIT_DEFAULT);
                }
                pp.setNullType(NullType.No_Nulls);

                pp.setAnnotation(parameter.getDescription());
                if (defaultValue != null) {
                    pp.setDefaultValue(defaultValue.toString());
                }
                if (collectionFormat != null) {
                    pp.setProperty(COLLECION_FORMAT, collectionFormat);
                }
                // extended properties
                for (Entry<String, Object> extension:parameter.getVendorExtensions().entrySet()) {
                    pp.setProperty(extension.getKey(), extension.getValue().toString());
                }
            }
        }
    }

    private String getOperationSummary(Operation operation) {
        String description = operation.getDescription();
        if(description == null || description.equals("")) { //$NON-NLS-1$
            description = operation.getSummary();
        }
        return description;
    }

    private String getTypes(List<String> types, String preferred) {
        String selected = null;
        if(types != null && !types.isEmpty()) {
            for(String type : types){
                if (preferred != null) {
                    if (preferred.equalsIgnoreCase(type)) {
                        selected = preferred;
                    }
                }
            }
            if (selected == null) {
                selected = types.get(0);
            }
        }
        return selected;
    }

    protected Swagger getSchema(WSConnection conn) throws TranslatorException {
        Swagger swagger = null;

        try {
            String swaggerFile = getSwaggerFilePath();

            if( swaggerFile != null &&  !swaggerFile.isEmpty()) {
                File f = new File(swaggerFile);

                if(!f.exists() || !f.isFile()) {
                    throw new TranslatorException(SwaggerPlugin.Event.TEIID28019,
                            SwaggerPlugin.Util.gs(SwaggerPlugin.Event.TEIID28019, swaggerFile));
                }

                SwaggerParser parser = new SwaggerParser();
                swagger =  parser.read(f.getAbsolutePath(), null, true);
            } else {
                BaseQueryExecution execution = new BaseQueryExecution(this.ef, null, null, conn);
                Map<String, List<String>> headers = new HashMap<String, List<String>>();
                BinaryWSProcedureExecution call = execution.buildInvokeHTTP("GET", "swagger.json", null, headers); //$NON-NLS-1$ //$NON-NLS-2$
                call.execute();
                if (call.getResponseCode() != 200) {
                    throw new TranslatorException(SwaggerPlugin.Event.TEIID28015,
                            SwaggerPlugin.Util.gs(SwaggerPlugin.Event.TEIID28015,call.getResponseCode()));
                }

                Blob out = (Blob)call.getOutputParameterValues().get(0);
                ObjectMapper objectMapper = new ObjectMapper();
                JsonNode rootNode = objectMapper.readTree(out.getBinaryStream());
                swagger =  new SwaggerParser().read(rootNode, true);
            }
        } catch (Exception e) {
            throw new TranslatorException(SwaggerPlugin.Event.TEIID28016, e,
                    SwaggerPlugin.Util.gs(SwaggerPlugin.Event.TEIID28016, e));
        }

        return swagger;
    }
}
