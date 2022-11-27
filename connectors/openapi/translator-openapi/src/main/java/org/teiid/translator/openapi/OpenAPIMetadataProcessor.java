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
package org.teiid.translator.openapi;

import java.sql.Blob;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.BaseColumn;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.Column;
import org.teiid.metadata.ExtensionMetadataProperty;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.ProcedureParameter.Type;
import org.teiid.metadata.RestMetadataExtension;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.TranslatorProperty.PropertyType;
import org.teiid.translator.swagger.BaseQueryExecution;
import org.teiid.translator.swagger.SwaggerPlugin;
import org.teiid.translator.swagger.SwaggerTypeManager;
import org.teiid.translator.ws.BinaryWSProcedureExecution;
import org.teiid.translator.ws.WSConnection;

import io.swagger.parser.OpenAPIParser;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.PathItem.HttpMethod;
import io.swagger.v3.oas.models.headers.Header;
import io.swagger.v3.oas.models.media.ArraySchema;
import io.swagger.v3.oas.models.media.Content;
import io.swagger.v3.oas.models.media.FileSchema;
import io.swagger.v3.oas.models.media.MapSchema;
import io.swagger.v3.oas.models.media.MediaType;
import io.swagger.v3.oas.models.media.ObjectSchema;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.oas.models.parameters.Parameter.StyleEnum;
import io.swagger.v3.oas.models.parameters.RequestBody;
import io.swagger.v3.oas.models.responses.ApiResponse;
import io.swagger.v3.oas.models.responses.ApiResponses;
import io.swagger.v3.oas.models.servers.Server;
import io.swagger.v3.parser.converter.SwaggerConverter;
import io.swagger.v3.parser.core.extensions.SwaggerParserExtension;
import io.swagger.v3.parser.core.models.ParseOptions;
import io.swagger.v3.parser.core.models.SwaggerParseResult;
import io.swagger.v3.parser.util.RefUtils;

public class OpenAPIMetadataProcessor implements MetadataProcessor<WSConnection>{
    private static final String ARRAY_SUFFIX = "[]"; //$NON-NLS-1$
    public static final String KEY_NAME = "key_name"; //$NON-NLS-1$
    public static final String KEY_VALUE = "key_value"; //$NON-NLS-1$

    @ExtensionMetadataProperty(applicable=Procedure.class, datatype=String.class, display="URI",
            description="Used to define endpoint of the procedure", required=true)
    public final static String URI = RestMetadataExtension.URI;

    @ExtensionMetadataProperty(applicable=Procedure.class, datatype=String.class, display="Http Method",
            description="Http method used to execute the procedure", required=true,
            allowed="GET,POST,PUT,DELETE,OPTIONS,HEAD,PATCH")
    public final static String METHOD = RestMetadataExtension.METHOD;

    @ExtensionMetadataProperty(applicable=Procedure.class, datatype=String.class, display="Produces",
            description="Used to define content type produced by this procedure, default JSON assumed")
    public final static String PRODUCES = RestMetadataExtension.PRODUCES;

    @ExtensionMetadataProperty(applicable=Procedure.class, datatype=String.class, display="Consumes",
            description="Used to define content type consumed by this procedure with body type parameters. Default JSON assumed")
    public final static String CONSUMES = RestMetadataExtension.CONSUMES;

    @ExtensionMetadataProperty(applicable=ProcedureParameter.class, datatype=String.class, display="Parameter Type",
            description="Parameter type, as to how the parameter is being provided to the procedure", required=true,
            allowed="PATH,QUERY,FORM,FORMDATA,BODY,HEADER")
    public final static String PARAMETER_TYPE = RestMetadataExtension.PARAMETER_TYPE;

    @ExtensionMetadataProperty(applicable=ProcedureParameter.class, datatype=String.class, display="Collection Format",
            description="Determines the format of the array if type array is used, like CSV,TSV etc.",
            allowed="CSV,SSV,TSV,PIPES,MULTI")
    public final static String COLLECION_FORMAT = RestMetadataExtension.COLLECION_FORMAT;


    private String metadataUrl;
    private String server;
    private String preferredProduces = "application/json"; //$NON-NLS-1$
    private String preferredConsumes = "application/json"; //$NON-NLS-1$
    private OpenAPIExecutionFactory ef;

    public OpenAPIMetadataProcessor(OpenAPIExecutionFactory ef) {
        this.ef = ef;
    }

    @TranslatorProperty(display="OpenAPI metadata URL", category=PropertyType.IMPORT,
            description="OpenAPI metadata URL")
    public String getMetadataUrl() {
        return metadataUrl;
    }

    public void setMetadataUrl(String metadataUrl) {
        this.metadataUrl = metadataUrl;
    }

    @TranslatorProperty(display="Server", category=PropertyType.IMPORT,
            description="Server to use when multiple servers are defined")
    public String getServer() {
        return server;
    }

    public void setServer(String server) {
        this.server = server;
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
        SwaggerParseResult swagger = getSchema(connection);
        OpenAPI openapi = swagger.getOpenAPI();
        List<String> messages = swagger.getMessages();
        if (messages != null && !messages.isEmpty()) {
            if (openapi == null) {
                throw new TranslatorException(messages.iterator().next());
            }
            LogManager.logInfo(LogConstants.CTX_CONNECTOR, messages.iterator().next());
        }
        List<Server> servers = openapi.getServers();
        Server toUse = null;
        if (server != null) {
            for (Server s : servers) {
                if (s.getUrl().equals(server)) {
                    toUse = s;
                    break;
                }
            }
        } else {
            toUse = servers.iterator().next();
        }

        for(Entry<String, PathItem> entry : openapi.getPaths().entrySet()) {
            addProcedure(mf, swagger, toUse, entry.getKey(), entry.getValue());
        }
    }

    private String buildURL(String basePath, String endpoint) {
        if (endpoint.startsWith("/")) { //$NON-NLS-1$
            if (basePath.endsWith("/")) { //$NON-NLS-1$
                return basePath+endpoint.substring(1);
            }
            return basePath+endpoint;
        }
        if (basePath.endsWith("/")) { //$NON-NLS-1$
            return basePath+endpoint;
        }
        return basePath+"/"+endpoint; //$NON-NLS-1$
    }

    public static Map<HttpMethod, Operation> getOperationMap(PathItem operations) {
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


    private void addProcedure(MetadataFactory mf, SwaggerParseResult swagger,
            Server toUse, String endpoint, PathItem operations) throws TranslatorException {

        Map<HttpMethod, Operation> operationsMap = getOperationMap(operations);

        for(Entry<HttpMethod, Operation> entry : operationsMap.entrySet()){
            Operation operation = entry.getValue();

            String name = operation.getOperationId();
            if (name == null) {
                //determine a name from the endpoint
                int start = 0;
                if (endpoint.startsWith("/")) { //$NON-NLS-1$
                    start = 1;
                }
                name = endpoint.substring(start);
                name = name.replaceAll("\\{([^}]*)\\}", "$1"); //$NON-NLS-1$ //$NON-NLS-2$
                if (operationsMap.entrySet().size() > 1) {
                    name = name + "_" + entry.getKey().name(); //$NON-NLS-1$
                }
            }

            Procedure procedure = mf.addProcedure(name);
            procedure.setVirtual(false);
            procedure.setProperty(METHOD, entry.getKey().name());
            procedure.setProperty(URI, buildURL(toUse.getUrl(), endpoint));

            addProcedureParameters(mf, swagger, procedure, operation);

            procedure.setAnnotation(getOperationSummary(operation));
            addExtensions(operation.getExtensions(), procedure);

            boolean returnAdded = false;
            ApiResponses respMap = operation.getResponses();
            for (String code : respMap.keySet()) {
                if (code.equalsIgnoreCase("default")) { //$NON-NLS-1$
                    continue;
                }
                int httpCode = Integer.valueOf(code);
                // Success codes
                if (httpCode > 100 && httpCode < 300) {
                    ApiResponse resp = respMap.get(code);
                    returnAdded = buildResponse(mf, swagger, procedure, resp);
                    break;
                }
            }

            if (!returnAdded && respMap.get("default") != null) { //$NON-NLS-1$
                ApiResponse resp = respMap.get("default"); //$NON-NLS-1$
                returnAdded = buildResponse(mf, swagger, procedure, resp);
            }

        }
    }

    private boolean buildResponse(final MetadataFactory mf, final SwaggerParseResult swagger,
            final Procedure procedure, final ApiResponse resp) throws TranslatorException {
        SchemaAction pa = new SchemaAction() {
            @Override
            public void execute(String name, String nameInSource,
                    Schema property, boolean array) {
                String type = getSchemaType(property, array);
                Column c = mf.addProcedureResultSetColumn(name, type, procedure);
                if (!name.equalsIgnoreCase(nameInSource)) {
                    c.setNameInSource(nameInSource);
                }
            }
        };

        Content content = resp.getContent();
        if (content != null && !content.isEmpty()) {
            MediaType mediaType = processMediaType(procedure, content, preferredProduces, PRODUCES);
            Schema<?> schema = mediaType.getSchema();
            if (isSimple(schema)) {
                boolean array = false;
                if (schema instanceof ArraySchema) {
                    schema = ((ArraySchema)schema).getItems();
                    array = true;
                }
                if(resp.getHeaders() == null|| resp.getHeaders().isEmpty()) {
                    String type = SwaggerTypeManager.teiidType(schema.getType(), schema.getFormat(), array);
                    mf.addProcedureParameter("return", type, ProcedureParameter.Type.ReturnValue, procedure);
                } else {
                    Map<String, Schema> properties = new LinkedHashMap<String, Schema>();
                    properties.put("return", schema);
                    walkSchema(swagger, properties, null, null, pa);
                }
            } else {
                // since the return is always a collection unwrap the array without any issues.
                if (schema instanceof ArraySchema) {
                    schema = ((ArraySchema)schema).getItems();
                }

                if (schema instanceof ObjectSchema) {
                    walkSchema(swagger, ((ObjectSchema)schema).getProperties(),
                            null,
                            null,
                            pa);
                } else if (schema instanceof MapSchema){
                    Object property = ((MapSchema)schema).getAdditionalProperties();
                    if (property instanceof Schema) {
                        Schema<?> s = (Schema)property;
                        String type = SwaggerTypeManager.teiidType(s.getType(), s.getFormat(), false);
                        Column c = mf.addProcedureResultSetColumn(KEY_NAME, "string", procedure);
                        c.setNameInSource(KEY_NAME);
                        c = mf.addProcedureResultSetColumn(KEY_VALUE, type, procedure);
                        c.setNameInSource(KEY_VALUE);
                    }
                } else if (schema instanceof FileSchema){
                    throw new TranslatorException("File properties are not supported");
                } else {
                    Schema s = getSchemaFromRef(swagger, schema);
                    if (s != null) {
                        walkSchema(swagger, s.getProperties(),
                                null,
                                null,
                                pa);
                    }
                }
            }
        }

        Map<String, Header> headers = resp.getHeaders();
        if (headers != null) {
            headers.values().stream().forEach(h -> walkSchema(swagger,
                    h.getSchema().getProperties(), null, null, pa));
        }
        return procedure.getResultSet() != null;
    }

    private boolean isSimple(Schema<?> property) {
        if (property instanceof ArraySchema) {
            ArraySchema ap = (ArraySchema)property;
            return isSimple(ap.getItems());
        }
        else if (property instanceof ObjectSchema) {
            ObjectSchema os = (ObjectSchema)property;
            if ("object".equals(os.getType()) && (os.getProperties() == null || os.getProperties().isEmpty())) { //$NON-NLS-1$
                return true; //untypedproperty
            }
            return false;
        }
        else if (property instanceof MapSchema) {
            return false;
        }
        else if (property instanceof FileSchema) {
            return false;
        }

        if (property.get$ref() != null) {
            return false;
        }

        return true;
    }

    interface SchemaAction {
        void execute(String name, String nameInSource,
                Schema<?> schema, boolean array);
    }

    private void walkSchema(final SwaggerParseResult swagger,
            final Map<String,Schema> properties, final String namePrefix,
            final String nisPrefix, final SchemaAction pa) {
        walkSchema(swagger, new HashSet<Schema<?>>(), properties, namePrefix, nisPrefix, pa);
    }

    private void walkSchema(final SwaggerParseResult swagger, Set<Schema<?>> parents,
            final Map<String,Schema> properties, final String namePrefix,
            final String nisPrefix, final SchemaAction pa) {

        if (properties == null) {
            return;
        }

        final SchemaVisitor visitor = new SchemaVisitor() {
            @Override
            public void visit(String name, ArraySchema property) {
                if (isSimple(property)) {
                    // the array type defined in the type of the property
                    pa.execute(fqn(namePrefix,name), nis(nisPrefix,name, false), property.getItems(), true);
                } else {
                    // if Object or Ref, array does not matter as return is already a resultset.
                    Schema<?> items = property.getItems();
                    parents.add(property);
                    if (items instanceof ObjectSchema) {
                        String modelName = ((ObjectSchema)items).getName();
                        walkSchema(swagger, parents,
                                ((ObjectSchema) items).getProperties(),
                                fqn(fqn(namePrefix, name), modelName),
                                nis(nis(nisPrefix, name, true), modelName, false),
                                pa);
                    } else {
                        String refSchemaName = items.get$ref();
                        if (refSchemaName != null) {
                            Schema s = getSchemaFromRef(swagger, items);
                            String refName = RefUtils.computeDefinitionName(refSchemaName);
                            walkSchema(swagger, parents, s.getProperties(),
                                    fqn(fqn(namePrefix, name), refName),
                                    nis(nis(nisPrefix, name, true), refName, false),
                                    pa);
                        } else {
                            walkSchema(swagger, parents,
                                    properties, fqn(namePrefix, name),
                                    nis(nisPrefix, name, true), pa);
                        }
                    }
                    parents.remove(property);
                }
            }
            @Override
            public void visit(String name, ObjectSchema property) {
                parents.add(property);
                walkSchema(swagger, parents,
                        property.getProperties(), fqn(namePrefix, name),
                        nis(nisPrefix, name, false), pa);
                parents.remove(property);
            }
            @Override
            public void visit(String name, Schema property) {
                String schemaName = property.get$ref();
                if (schemaName != null) {
                    Schema s = getSchemaFromRef(swagger, property);
                    if (s != null) {
                        parents.add(property);
                        walkSchema(swagger, parents,
                                s.getProperties(), fqn(namePrefix, name),
                                nis(nisPrefix, name, false), pa);
                        parents.remove(property);
                    }
                } else {
                    pa.execute(fqn(namePrefix,name), nis(nisPrefix, name, false), property, false);
                }
            }
        };

        for (Entry<String, Schema> p:properties.entrySet()) {
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

    private static String getSchemaType(Schema<?> property,
            boolean array) {
        String type = DataTypeManager.DefaultDataTypes.STRING;
        if (property != null) {
            type = SwaggerTypeManager.teiidType(property.getType(), property.getFormat(), array);
        } else if (array) {
            type += ARRAY_SUFFIX;
        }
        return type;
    }

    private void addProcedureParameters(MetadataFactory mf,
            SwaggerParseResult swagger, Procedure procedure,
            Operation operation) {

        if (operation.getParameters() != null) {
            for(final Parameter parameter : operation.getParameters()) {
                String name = parameter.getName();
                ProcedureParameter pp = null;
                Object defaultValue = null;
                StyleEnum style = parameter.getStyle();
                boolean array = false;

                Schema<?> schema = parameter.getSchema();

                String type = schema.getType();
                if (schema instanceof ArraySchema){
                    Schema ap = ((ArraySchema)schema).getItems();
                    type = ap.getType();
                    array = true;
                }
                type = SwaggerTypeManager.teiidType(type, schema.getFormat(), array);
                defaultValue = schema.getDefault();

                pp = mf.addProcedureParameter(name, type, Type.In, procedure);
                pp.setProperty(PARAMETER_TYPE, parameter.getIn());

                Boolean required = parameter.getRequired();
                if (required == null || !required) {
                    pp.setProperty(BaseColumn.DEFAULT_HANDLING, BaseColumn.OMIT_DEFAULT);
                }
                pp.setNullType(NullType.No_Nulls);

                pp.setAnnotation(parameter.getDescription());
                if (defaultValue != null) {
                    pp.setDefaultValue(defaultValue.toString());
                }
                if (style != null) {
                    pp.setProperty(COLLECION_FORMAT, style.toString());
                }
                // extended properties
                addExtensions(parameter.getExtensions(), pp);
            }
        }

        if (operation.getRequestBody() != null) {
            RequestBody requestBody = operation.getRequestBody();
            SchemaAction pa = new SchemaAction() {
                @Override
                public void execute(String name, String nameInSource,
                        Schema property, boolean array) {
                    String type = getSchemaType(property, array);
                    if (procedure.getParameterByName(nameInSource) == null) {
                        ProcedureParameter param = mf.addProcedureParameter(name, type, Type.In, procedure);
                        param.setProperty(PARAMETER_TYPE, "body"); //$NON-NLS-1$
                        if (property != null &&
                                (requestBody.getRequired() == null || !requestBody.getRequired()) &&
                                (property.getRequired() == null || property.getRequired().isEmpty())) {
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
            Content content = requestBody.getContent();
            MediaType mediaType = processMediaType(procedure, content, preferredConsumes, CONSUMES);
            Schema schema = mediaType.getSchema();

            if (schema.get$ref() != null) {
                if (schema.getProperties() != null) {
                    walkSchema(swagger, schema.getProperties(), null, null, pa);
                } else {
                    Schema s = getSchemaFromRef(swagger, schema);
                    if (s != null) {
                        walkSchema(swagger, s.getProperties(), null, null, pa);
                    }
                }
            } else {
                if (schema.getProperties() != null) {
                    walkSchema(swagger, schema.getProperties(), null, null, pa);
                } else {
                    ProcedureParameter p = mf.addProcedureParameter(
                            "body", //TODO: choose a unique name
                            DataTypeManager.DefaultDataTypes.CLOB, Type.In,
                            procedure);
                    p.setProperty(PARAMETER_TYPE, "body"); //$NON-NLS-1$
                    p.setNullType(NullType.No_Nulls);
                    p.setAnnotation(requestBody.getDescription());
                }
            }
        }
    }

    private void addExtensions(final Map<String, Object> extensions,
            AbstractMetadataRecord record) {
        if (extensions != null) {
            for (Entry<String, Object> extension:extensions.entrySet()) {
                record.setProperty(extension.getKey(), extension.getValue().toString());
            }
        }
    }

    private MediaType processMediaType(Procedure procedure, Content content,
            String defaultType, String propertyKey) {
        MediaType mediaType = content.get(defaultType);
        if (mediaType == null) {
            Entry<String, MediaType> entry = content.entrySet().iterator().next();
            defaultType = entry.getKey();
            mediaType = entry.getValue();
        }
        if (!"*/*".equals(defaultType)) { //$NON-NLS-1$
            procedure.setProperty(propertyKey, defaultType);
        }
        return mediaType;
    }

    private Schema getSchemaFromRef(SwaggerParseResult swagger, Schema schema) {
        String ref = schema.get$ref();
        if (ref == null) {
            return null;
        }
        String name = RefUtils.computeDefinitionName(ref);
        return swagger.getOpenAPI().getComponents().getSchemas().get(name);
    }

    private String getOperationSummary(Operation operation) {
        String description = operation.getDescription();
        if(description == null || description.equals("")) { //$NON-NLS-1$
            description = operation.getSummary();
        }
        return description;
    }

    protected SwaggerParseResult getSchema(WSConnection conn) throws TranslatorException {
        SwaggerParseResult swagger = null;

        try {
            String url = metadataUrl;

            if( url == null) {
                //assume microprofile relative url
                url = "openapi"; //$NON-NLS-1$
            }

            OpenAPIParser parser = new OpenAPIParser() {
                /*
                 * The service loader mechanism doesn't seem to work well in wildfly,
                 * so we manually do it here
                 */
                @Override
                protected List<SwaggerParserExtension> getExtensions() {
                    List<SwaggerParserExtension> extensions = super.getExtensions();
                    SwaggerConverter converter = new SwaggerConverter();
                    if (!extensions.contains(converter)) {
                        extensions.add(converter);
                    }
                    return extensions;
                }
            };
            ParseOptions parseOptions = new ParseOptions();
            parseOptions.setResolve(true);

            if (url.startsWith("classpath:") || url.startsWith("file:")) { //$NON-NLS-1$ //$NON-NLS-2$
                swagger = parser.readLocation(url, null, parseOptions);
            } else {
                BaseQueryExecution execution = new BaseQueryExecution(this.ef, null, null, conn);
                Map<String, List<String>> headers = new HashMap<String, List<String>>();
                BinaryWSProcedureExecution call = execution.buildInvokeHTTP("GET", url, null, headers); //$NON-NLS-1$
                call.execute();
                if (call.getResponseCode() != 200) {
                    throw new TranslatorException(SwaggerPlugin.Event.TEIID28015,
                            SwaggerPlugin.Util.gs(SwaggerPlugin.Event.TEIID28015,call.getResponseCode()));
                }
                Blob out = (Blob)call.getOutputParameterValues().get(0);
                String contents = ObjectConverterUtil.convertToString(out.getBinaryStream());
                swagger = parser.readContents(contents, null, parseOptions);
            }
        } catch (Exception e) {
            throw new TranslatorException(SwaggerPlugin.Event.TEIID28016, e,
                    SwaggerPlugin.Util.gs(SwaggerPlugin.Event.TEIID28016, e));
        }

        return swagger;
    }
}
