/*ode.Id_ADD
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
package org.teiid.adminapi.jboss;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.ObjectListAttributeDefinition;
import org.jboss.as.controller.ObjectTypeAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.StringListAttributeDefinition;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;
import org.teiid.adminapi.DataPolicy;
import org.teiid.adminapi.DataPolicy.ResourceType;
import org.teiid.adminapi.Request.ProcessingState;
import org.teiid.adminapi.Request.ThreadState;
import org.teiid.adminapi.Translator;
import org.teiid.adminapi.impl.*;
import org.teiid.adminapi.impl.DataPolicyMetadata.PermissionMetaData;
import org.teiid.adminapi.impl.ModelMetaData.Message;
import org.teiid.adminapi.impl.ModelMetaData.Message.Severity;

public class VDBMetadataMapper implements MetadataMapper<VDBMetaData> {
    private static final String VDBNAME = "vdb-name"; //$NON-NLS-1$
    private static final String CONNECTIONTYPE = "connection-type"; //$NON-NLS-1$
    private static final String STATUS = "status"; //$NON-NLS-1$
    private static final String VERSION = "vdb-version"; //$NON-NLS-1$
    private static final String MODELS = "models"; //$NON-NLS-1$
    private static final String IMPORT_VDBS = "import-vdbs"; //$NON-NLS-1$
    private static final String OVERRIDE_TRANSLATORS = "override-translators"; //$NON-NLS-1$
    private static final String VDB_DESCRIPTION = "vdb-description"; //$NON-NLS-1$
    private static final String PROPERTIES = "properties"; //$NON-NLS-1$
    private static final String XML_DEPLOYMENT = "xml-deployment"; //$NON-NLS-1$
    private static final String DATA_POLICIES = "data-policies"; //$NON-NLS-1$
    private static final String DESCRIPTION = "description"; //$NON-NLS-1$
    private static final String ENTRIES = "entries"; //$NON-NLS-1$

    public static VDBMetadataMapper INSTANCE = new VDBMetadataMapper();

    public ModelNode wrap(VDBMetaData vdb, ModelNode node) {
        return wrap(vdb, node, true);
    }

    public ModelNode wrap(VDBMetaData vdb, ModelNode node, boolean includeMetadata) {
        if (vdb == null) {
            return null;
        }
        node.get(TYPE).set(ModelType.OBJECT);
        node.get(VDBNAME).set(vdb.getName());
        node.get(CONNECTIONTYPE).set(vdb.getConnectionType().toString());
        node.get(STATUS).set(vdb.getStatus().toString());
        node.get(VERSION).set(vdb.getVersion());
        if (vdb.getDescription() != null) {
            node.get(VDB_DESCRIPTION).set(vdb.getDescription());
        }
        node.get(XML_DEPLOYMENT).set(vdb.isXmlDeployment());

        //PROPERTIES
        addProperties(node, vdb);

        // IMPORT-VDBS
        List<VDBImportMetadata> imports = vdb.getVDBImports();
        if (imports != null && !imports.isEmpty()) {
            ModelNode importNodes = node.get(IMPORT_VDBS);
            for(VDBImportMetadata vdbImport:imports) {
                importNodes.add(VDBImportMapper.INSTANCE.wrap(vdbImport, new ModelNode()));
            }
        }

        // ENTRIES
        List<EntryMetaData> entries = vdb.getEntries();
        if (entries != null && !entries.isEmpty()) {
            ModelNode entryNodes = node.get(ENTRIES);
            for(EntryMetaData entry:entries) {
                entryNodes.add(EntryMapper.INSTANCE.wrap(entry, new ModelNode()));
            }
        }

        // MODELS
        Map<String, ModelMetaData> models = vdb.getModelMetaDatas();
        if (models != null && !models.isEmpty()) {
            ModelNode modelNodes = node.get(MODELS);
            for(ModelMetaData model:models.values()) {
                modelNodes.add(ModelMetadataMapper.INSTANCE.wrap(model, new ModelNode(), includeMetadata));
            }
        }

        // OVERRIDE_TRANSLATORS
        List<Translator> translators = vdb.getOverrideTranslators();
        if (translators != null && !translators.isEmpty()) {
            ModelNode translatorNodes = node.get(OVERRIDE_TRANSLATORS);
            for (Translator translator:translators) {
                translatorNodes.add(VDBTranslatorMetaDataMapper.INSTANCE.wrap((VDBTranslatorMetaData)translator,  new ModelNode()));
            }
        }

        // DATA_POLICIES
        List<DataPolicy> policies = vdb.getDataPolicies();
        if (policies != null && !policies.isEmpty()) {
            ModelNode dataPoliciesNodes = node.get(DATA_POLICIES);
            for (DataPolicy policy:policies) {
                dataPoliciesNodes.add(DataPolicyMetadataMapper.INSTANCE.wrap((DataPolicyMetadata)policy,  new ModelNode()));
            }
        }

        wrapDomain(vdb, node);
        return node;
    }

    public VDBMetaData unwrap(ModelNode node) {
        if (node == null)
            return null;

        VDBMetaData vdb = new VDBMetaData();
        if (node.has(VDBNAME)) {
            vdb.setName(node.get(VDBNAME).asString());
        }
        if (node.has(CONNECTIONTYPE)) {
            vdb.setConnectionType(node.get(CONNECTIONTYPE).asString());
        }
        if (node.has(STATUS)) {
            vdb.setStatus(node.get(STATUS).asString());
        }
        if (node.has(VERSION)) {
            vdb.setVersion(node.get(VERSION).asString());
        }
        if(node.has(VDB_DESCRIPTION)) {
            vdb.setDescription(node.get(VDB_DESCRIPTION).asString());
        }
        if (node.has(XML_DEPLOYMENT)) {
            vdb.setXmlDeployment(node.get(XML_DEPLOYMENT).asBoolean());
        }

        //PROPERTIES
        if (node.get(PROPERTIES).isDefined()) {
            List<ModelNode> propNodes = node.get(PROPERTIES).asList();
            for (ModelNode propNode:propNodes) {
                String[] prop = PropertyMetaDataMapper.INSTANCE.unwrap(propNode);
                if (prop != null) {
                    vdb.addProperty(prop[0], prop[1]);
                }
            }
        }

        // IMPORT-VDBS
        if (node.get(IMPORT_VDBS).isDefined()) {
            List<ModelNode> modelNodes = node.get(IMPORT_VDBS).asList();
            for(ModelNode modelNode:modelNodes) {
                VDBImportMetadata vdbImport = VDBImportMapper.INSTANCE.unwrap(modelNode);
                if (vdbImport != null) {
                    vdb.getVDBImports().add(vdbImport);
                }
            }
        }

        // ENTRIES
        if (node.get(ENTRIES).isDefined()) {
            List<ModelNode> modelNodes = node.get(ENTRIES).asList();
            for(ModelNode modelNode:modelNodes) {
                EntryMetaData entry = EntryMapper.INSTANCE.unwrap(modelNode);
                if (entry != null) {
                    vdb.getEntries().add(entry);
                }
            }
        }

        // MODELS
        if (node.get(MODELS).isDefined()) {
            List<ModelNode> modelNodes = node.get(MODELS).asList();
            for(ModelNode modelNode:modelNodes) {
                ModelMetaData model = ModelMetadataMapper.INSTANCE.unwrap(modelNode);
                if (model != null) {
                    vdb.addModel(model);
                }
            }
        }

        // OVERRIDE_TRANSLATORS
        if (node.get(OVERRIDE_TRANSLATORS).isDefined()) {
            List<ModelNode> translatorNodes = node.get(OVERRIDE_TRANSLATORS).asList();
            for (ModelNode translatorNode:translatorNodes) {
                VDBTranslatorMetaData translator = VDBTranslatorMetaDataMapper.INSTANCE.unwrap(translatorNode);
                if (translator != null) {
                    vdb.addOverideTranslator(translator);
                }
            }
        }

        // DATA_POLICIES
        if (node.get(DATA_POLICIES).isDefined()) {
            List<ModelNode> policiesNodes = node.get(DATA_POLICIES).asList();
            for (ModelNode policyNode:policiesNodes) {
                DataPolicyMetadata policy = DataPolicyMetadataMapper.INSTANCE.unwrap(policyNode);
                if (policy != null) {
                    vdb.addDataPolicy(policy);
                }

            }
        }
        unwrapDomain(vdb, node);
        return vdb;
    }

    public AttributeDefinition[] getAttributeDefinitions() {
        ObjectListAttributeDefinition properties = ObjectListAttributeDefinition.Builder.of(PROPERTIES, PropertyMetaDataMapper.INSTANCE.getAttributeDefinition()).build();
        ObjectListAttributeDefinition vdbimports = ObjectListAttributeDefinition.Builder.of(IMPORT_VDBS, VDBImportMapper.INSTANCE.getAttributeDefinition()).build();
        ObjectListAttributeDefinition models = ObjectListAttributeDefinition.Builder.of(MODELS, ModelMetadataMapper.INSTANCE.getAttributeDefinition()).build();
        ObjectListAttributeDefinition translators = ObjectListAttributeDefinition.Builder.of(OVERRIDE_TRANSLATORS, VDBTranslatorMetaDataMapper.INSTANCE.getAttributeDefinition()).build();
        ObjectListAttributeDefinition policies = ObjectListAttributeDefinition.Builder.of(DATA_POLICIES, DataPolicyMetadataMapper.INSTANCE.getAttributeDefinition()).build();

        return new AttributeDefinition[] {
                createAttribute(VDBNAME, ModelType.STRING, false),
                createAttribute(CONNECTIONTYPE, ModelType.STRING, false),
                createAttribute(STATUS, ModelType.STRING, false),
                createAttribute(VERSION, ModelType.STRING, false),
                createAttribute(VDB_DESCRIPTION, ModelType.STRING, true),
                createAttribute(XML_DEPLOYMENT, ModelType.BOOLEAN, true),
                properties,
                vdbimports,
                models,
                translators,
                policies
            };
    }



    private static void addProperties(ModelNode node, AdminObjectImpl object) {
        Map<String, String> properties = object.getPropertiesMap();
        if (properties!= null && !properties.isEmpty()) {
            ModelNode propsNode = node.get(PROPERTIES);
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                propsNode.add(PropertyMetaDataMapper.INSTANCE.wrap(entry.getKey(), entry.getValue(), new ModelNode()));
            }
        }
    }

    /**
     * model metadata mapper
     */
    public static class ModelMetadataMapper implements MetadataMapper<ModelMetaData>{
        private static final String MODEL_NAME = "model-name"; //$NON-NLS-1$
        private static final String DESCRIPTION = "description"; //$NON-NLS-1$
        private static final String VISIBLE = "visible"; //$NON-NLS-1$
        private static final String MODEL_TYPE = "model-type"; //$NON-NLS-1$
        private static final String MODELPATH = "model-path"; //$NON-NLS-1$
        private static final String PROPERTIES = "properties"; //$NON-NLS-1$
        private static final String SOURCE_MAPPINGS = "source-mappings"; //$NON-NLS-1$
        private static final String VALIDITY_ERRORS = "validity-errors"; //$NON-NLS-1$
        private static final String METADATAS= "metadatas"; //$NON-NLS-1$
        private static final String METADATA= "metadata"; //$NON-NLS-1$
        private static final String METADATA_TYPE = "metadata-type"; //$NON-NLS-1$
        private static final String METADATA_STATUS = "metadata-status"; //$NON-NLS-1$

        public static ModelMetadataMapper INSTANCE = new ModelMetadataMapper();

        public ModelNode wrap(ModelMetaData model, ModelNode node) {
            return wrap(model, node, true);
        }
        public ModelNode wrap(ModelMetaData model, ModelNode node, boolean includeMetadata) {
            if (model == null) {
                return null;
            }

            node.get(MODEL_NAME).set(model.getName());
            if (model.getDescription() != null) {
                node.get(DESCRIPTION).set(model.getDescription());
            }
            node.get(VISIBLE).set(model.isVisible());
            node.get(MODEL_TYPE).set(model.getModelType().toString());
            if (model.getPath() != null) {
                node.get(MODELPATH).set(model.getPath());
            }

            addProperties(node, model);

            Collection<SourceMappingMetadata> sources = model.getSourceMappings();
            if (sources != null && !sources.isEmpty()) {
                ModelNode sourceMappingNode = node.get(SOURCE_MAPPINGS);
                for(SourceMappingMetadata source:sources) {
                    sourceMappingNode.add(SourceMappingMetadataMapper.INSTANCE.wrap(source,  new ModelNode()));
                }
            }

            List<Message> errors = model.getMessages();
            if (errors != null && !errors.isEmpty()) {
                ModelNode errorsNode = node.get(VALIDITY_ERRORS);
                for (Message error:errors) {
                    errorsNode.add(ValidationErrorMapper.INSTANCE.wrap(error, new ModelNode()));
                }
            }
            if (includeMetadata) {
                if (!model.getSourceMetadataType().isEmpty()) {
                    ModelNode metadataNodes = node.get(METADATAS);
                    for (int i = 0; i < model.getSourceMetadataType().size(); i++) {
                        ModelNode metadataNode = new ModelNode();
                        metadataNode.get(METADATA_TYPE).set(model.getSourceMetadataType().get(i));
                        String text = model.getSourceMetadataText().get(i);
                        if (text != null) {
                            metadataNode.get(METADATA).set(text);
                        }
                        metadataNodes.add(metadataNode);
                    }
                }
            }
            node.get(METADATA_STATUS).set(model.getMetadataStatus().name());
            return node;
        }

        public ModelMetaData unwrap(ModelNode node) {
            if (node == null) {
                return null;
            }

            ModelMetaData model = new ModelMetaData();
            if (node.has(MODEL_NAME)) {
                model.setName(node.get(MODEL_NAME).asString());
            }
            if (node.has(DESCRIPTION)) {
                model.setDescription(node.get(DESCRIPTION).asString());
            }
            if (node.has(VISIBLE)) {
                model.setVisible(node.get(VISIBLE).asBoolean());
            }
            if(node.has(MODEL_TYPE)) {
                model.setModelType(node.get(MODEL_TYPE).asString());
            }
            if(node.has(MODELPATH)) {
                model.setPath(node.get(MODELPATH).asString());
            }

            if (node.get(PROPERTIES).isDefined()) {
                List<ModelNode> propNodes = node.get(PROPERTIES).asList();
                for (ModelNode propNode:propNodes) {
                    String[] prop = PropertyMetaDataMapper.INSTANCE.unwrap(propNode);
                    if (prop != null) {
                        model.addProperty(prop[0], prop[1]);
                    }
                }
            }

            if (node.get(SOURCE_MAPPINGS).isDefined()) {
                List<ModelNode> sourceMappingNodes = node.get(SOURCE_MAPPINGS).asList();
                for (ModelNode sourceMapping:sourceMappingNodes) {
                    SourceMappingMetadata source = SourceMappingMetadataMapper.INSTANCE.unwrap(sourceMapping);
                    if (source != null) {
                        model.addSourceMapping(source);
                    }
                }
            }

            if (node.get(VALIDITY_ERRORS).isDefined()) {
                List<ModelNode> errorNodes = node.get(VALIDITY_ERRORS).asList();
                for(ModelNode errorNode:errorNodes) {
                    Message error = ValidationErrorMapper.INSTANCE.unwrap(errorNode);
                    if (error != null) {
                        model.addMessage(error);
                    }
                }
            }
            if (node.get(METADATAS).isDefined()) {
                List<ModelNode> metadataNodes = node.get(METADATAS).asList();
                for (ModelNode modelNode : metadataNodes) {
                    String text = null;
                    String type = null;
                    if (modelNode.get(METADATA).isDefined()) {
                        text = modelNode.get(METADATA).asString();
                    }
                    if (modelNode.get(METADATA_TYPE).isDefined()) {
                        type = modelNode.get(METADATA_TYPE).asString();
                    }
                    model.addSourceMetadata(type, text);
                }
            }
            if (node.get(METADATA_STATUS).isDefined()) {
                model.setMetadataStatus(node.get(METADATA_STATUS).asString());
            }
            return model;
        }

        public ObjectTypeAttributeDefinition getAttributeDefinition() {
            ObjectListAttributeDefinition properties = ObjectListAttributeDefinition.Builder.of(PROPERTIES, PropertyMetaDataMapper.INSTANCE.getAttributeDefinition()).build();
            ObjectListAttributeDefinition sourceMappings = ObjectListAttributeDefinition.Builder.of(SOURCE_MAPPINGS, SourceMappingMetadataMapper.INSTANCE.getAttributeDefinition()).build();
            ObjectListAttributeDefinition errors = ObjectListAttributeDefinition.Builder.of(VALIDITY_ERRORS, ValidationErrorMapper.INSTANCE.getAttributeDefinition()).build();
            ObjectListAttributeDefinition metadatas = ObjectListAttributeDefinition.Builder.of(METADATAS, ObjectTypeAttributeDefinition.Builder.of("MetadataMapper", //$NON-NLS-1$
                    new AttributeDefinition[] {
                            createAttribute(METADATA, ModelType.STRING, true),
                            createAttribute(METADATA_TYPE, ModelType.STRING, true)
                    }).build()).build();

            return ObjectTypeAttributeDefinition.Builder.of("ModelMetadataMapper", //$NON-NLS-1$
                new AttributeDefinition[] {
                    createAttribute(MODEL_NAME, ModelType.STRING, false),
                    createAttribute(DESCRIPTION, ModelType.STRING, true),
                    createAttribute(VISIBLE, ModelType.BOOLEAN, true),
                    createAttribute(MODEL_TYPE, ModelType.STRING, false),
                    createAttribute(MODELPATH, ModelType.STRING, true),
                    createAttribute(METADATA_STATUS, ModelType.STRING, true),
                    properties,
                    sourceMappings,
                    errors,
                    metadatas
            }).build();
        }
    }

    /**
     * vdb import mapper
     */
    public static class VDBImportMapper implements MetadataMapper<VDBImportMetadata>{
        private static final String VDB_NAME = "import-vdb-name"; //$NON-NLS-1$
        private static final String VDB_VERSION = "import-vdb-version"; //$NON-NLS-1$
        private static final String IMPORT_POLICIES = "import-policies"; //$NON-NLS-1$

        public static VDBImportMapper INSTANCE = new VDBImportMapper();

        @Override
        public ModelNode wrap(VDBImportMetadata obj, ModelNode node) {
            if (obj == null) {
                return null;
            }

            node.get(VDB_NAME).set(obj.getName());
            node.get(VDB_VERSION).set(obj.getVersion());
            node.get(IMPORT_POLICIES).set(obj.isImportDataPolicies());
            return node;
        }

        public VDBImportMetadata unwrap(ModelNode node) {
            if (node == null) {
                return null;
            }

            VDBImportMetadata vdbImport = new VDBImportMetadata();
            if (node.has(VDB_NAME)) {
                vdbImport.setName(node.get(VDB_NAME).asString());
            }
            if (node.has(VDB_VERSION)) {
                vdbImport.setVersion(node.get(VDB_VERSION).asString());
            }
            if (node.has(IMPORT_POLICIES)) {
                vdbImport.setImportDataPolicies(node.get(IMPORT_POLICIES).asBoolean());
            }
            return vdbImport;
        }

        public ObjectTypeAttributeDefinition getAttributeDefinition() {
            return ObjectTypeAttributeDefinition.Builder.of("VDBImportMapper", //$NON-NLS-1$
                new AttributeDefinition[] {
                    createAttribute(VDB_NAME, ModelType.STRING, false),
                    createAttribute(VDB_VERSION, ModelType.STRING, false),
                    createAttribute(IMPORT_POLICIES, ModelType.BOOLEAN, true)
            }).build();
        }
    }

    /**
     * validation error mapper
     */
    public static class ValidationErrorMapper implements MetadataMapper<Message>{
        private static final String ERROR_PATH = "error-path"; //$NON-NLS-1$
        private static final String SEVERITY = "severity"; //$NON-NLS-1$
        private static final String MESSAGE = "message"; //$NON-NLS-1$


        public static ValidationErrorMapper INSTANCE = new ValidationErrorMapper();

        public ModelNode wrap(Message error, ModelNode node) {
            if (error == null) {
                return null;
            }

            if (error.getPath() != null) {
                node.get(ERROR_PATH).set(error.getPath());
            }
            node.get(SEVERITY).set(error.getSeverity().name());
            node.get(MESSAGE).set(error.getValue());

            return node;
        }

        public Message unwrap(ModelNode node) {
            if (node == null) {
                return null;
            }

            Message error = new Message();
            if (node.has(ERROR_PATH)) {
                error.setPath(node.get(ERROR_PATH).asString());
            }
            if (node.has(SEVERITY)) {
                error.setSeverity(Severity.valueOf(node.get(SEVERITY).asString()));
            }
            if(node.has(MESSAGE)) {
                error.setValue(node.get(MESSAGE).asString());
            }
            return error;
        }

        public ObjectTypeAttributeDefinition getAttributeDefinition() {
            return ObjectTypeAttributeDefinition.Builder.of("ValidationErrorMapper", //$NON-NLS-1$
                new AttributeDefinition[] {
                    createAttribute(ERROR_PATH, ModelType.STRING, true),
                    createAttribute(SEVERITY, ModelType.STRING, false),
                    createAttribute(MESSAGE, ModelType.STRING, false)
            }).build();
        }
    }

    /**
     * Source Mapping Metadata mapper
     */
    public static class SourceMappingMetadataMapper implements MetadataMapper<SourceMappingMetadata>{
        private static final String SOURCE_NAME = "source-name"; //$NON-NLS-1$
        private static final String JNDI_NAME = "jndi-name"; //$NON-NLS-1$
        private static final String TRANSLATOR_NAME = "translator-name"; //$NON-NLS-1$

        public static SourceMappingMetadataMapper INSTANCE = new SourceMappingMetadataMapper();

        public ModelNode wrap(SourceMappingMetadata source, ModelNode node) {
            if (source == null) {
                return null;
            }

            node.get(SOURCE_NAME).set(source.getName());
            if (source.getConnectionJndiName() != null) {
                node.get(JNDI_NAME).set(source.getConnectionJndiName());
            }
            node.get(TRANSLATOR_NAME).set(source.getTranslatorName());
            return node;
        }

        public SourceMappingMetadata unwrap(ModelNode node) {
            if (node == null) {
                return null;
            }
            SourceMappingMetadata source = new SourceMappingMetadata();
            if (node.has(SOURCE_NAME)) {
                source.setName(node.get(SOURCE_NAME).asString());
            }
            if (node.has(JNDI_NAME)) {
                source.setConnectionJndiName(node.get(JNDI_NAME).asString());
            }
            if (node.has(TRANSLATOR_NAME)) {
                source.setTranslatorName(node.get(TRANSLATOR_NAME).asString());
            }
            return source;
        }

        public ObjectTypeAttributeDefinition getAttributeDefinition() {
            return ObjectTypeAttributeDefinition.Builder.of("SourceMappingMetadataMapper", //$NON-NLS-1$
                new AttributeDefinition[] {
                    createAttribute(SOURCE_NAME, ModelType.STRING, false),
                    createAttribute(JNDI_NAME, ModelType.STRING, true),
                    createAttribute(TRANSLATOR_NAME, ModelType.STRING, false)
            }).build();
        }
    }

    /**
     * Source Mapping Metadata mapper
     */
    public static class VDBTranslatorMetaDataMapper implements MetadataMapper<VDBTranslatorMetaData>{
        private static final String TRANSLATOR_NAME = "translator-name"; //$NON-NLS-1$
        private static final String BASETYPE = "base-type"; //$NON-NLS-1$
        private static final String TRANSLATOR_DESCRIPTION = "translator-description"; //$NON-NLS-1$
        private static final String PROPERTIES = "properties"; //$NON-NLS-1$
        private static final String MODULE_NAME = "module-name"; //$NON-NLS-1$


        public static VDBTranslatorMetaDataMapper INSTANCE = new VDBTranslatorMetaDataMapper();

        public ModelNode wrap(VDBTranslatorMetaData translator, ModelNode node) {
            if (translator == null) {
                return null;
            }

            node.get(TRANSLATOR_NAME).set(translator.getName());
            if (translator.getType() != null) {
                node.get(BASETYPE).set(translator.getType());
            }
            if (translator.getDescription() != null) {
                node.get(TRANSLATOR_DESCRIPTION).set(translator.getDescription());
            }

            if (translator.getModuleName() != null) {
                node.get(MODULE_NAME).set(translator.getModuleName());
            }

            addProperties(node, translator);
            wrapDomain(translator, node);
            return node;
        }

        public VDBTranslatorMetaData unwrap(ModelNode node) {
            if (node == null) {
                return null;
            }
            VDBTranslatorMetaData translator = new VDBTranslatorMetaData();
            if (node.has(TRANSLATOR_NAME)) {
                translator.setName(node.get(TRANSLATOR_NAME).asString());
            }
            if (node.has(BASETYPE)) {
                translator.setType(node.get(BASETYPE).asString());
            }
            if (node.has(TRANSLATOR_DESCRIPTION)) {
                translator.setDescription(node.get(TRANSLATOR_DESCRIPTION).asString());
            }
            if (node.has(MODULE_NAME)) {
                translator.setModuleName(node.get(MODULE_NAME).asString());
            }

            if (node.get(PROPERTIES).isDefined()) {
                List<ModelNode> propNodes = node.get(PROPERTIES).asList();
                for (ModelNode propNode:propNodes) {
                    String[] prop = PropertyMetaDataMapper.INSTANCE.unwrap(propNode);
                    if (prop != null) {
                        translator.addProperty(prop[0], prop[1]);
                    }
                }
            }
            unwrapDomain(translator, node);
            return translator;
        }

        public ObjectTypeAttributeDefinition getAttributeDefinition() {
            return ObjectTypeAttributeDefinition.Builder.of("VDBTranslatorMetaDataMapper", //$NON-NLS-1$
                    getAttributeDefinitions()).build();
        }

        public AttributeDefinition[] getAttributeDefinitions() {
            ObjectListAttributeDefinition properties = ObjectListAttributeDefinition.Builder.of(PROPERTIES, PropertyMetaDataMapper.INSTANCE.getAttributeDefinition()).build();
            return new AttributeDefinition[] {
                    createAttribute(TRANSLATOR_NAME, ModelType.STRING, false),
                    createAttribute(BASETYPE, ModelType.STRING, false),
                    createAttribute(TRANSLATOR_DESCRIPTION, ModelType.STRING, true),
                    createAttribute(MODULE_NAME, ModelType.STRING, true),
                    properties
            };
        }
    }


    /**
     * Property Metadata mapper
     */
    public static class PropertyMetaDataMapper {
        private static final String PROPERTY_NAME = "property-name"; //$NON-NLS-1$
        private static final String PROPERTY_VALUE = "property-value"; //$NON-NLS-1$

        public static PropertyMetaDataMapper INSTANCE = new PropertyMetaDataMapper();

        public ModelNode wrap(String key, String value, ModelNode node) {
            node.get(PROPERTY_NAME).set(key);
            node.get(PROPERTY_VALUE).set(value);
            return node;
        }

        public String[] unwrap(ModelNode node) {
            if(node == null) {
                return null;
            }
            String key = null;
            String value = null;
            if (node.has(PROPERTY_NAME)) {
                key = node.get(PROPERTY_NAME).asString();
            }
            if(node.has(PROPERTY_VALUE)) {
                value = node.get(PROPERTY_VALUE).asString();
            }
            return new String[] {key, value};
        }

        public ObjectTypeAttributeDefinition getAttributeDefinition() {
            return ObjectTypeAttributeDefinition.Builder.of("PropertyMetaDataMapper", //$NON-NLS-1$
                    createAttribute(PROPERTY_NAME, ModelType.STRING, false),
                    createAttribute(PROPERTY_VALUE, ModelType.STRING, false)
            ).build();
        }
    }


    /**
     * Entry Mapper
     */
    public static class EntryMapper implements MetadataMapper<EntryMetaData>{
        private static final String PATH = "path"; //$NON-NLS-1$

        public static EntryMapper INSTANCE = new EntryMapper();

        @Override
        public ModelNode wrap(EntryMetaData obj, ModelNode node) {
            if (obj == null) {
                return null;
            }

            node.get(PATH).set(obj.getPath());
            if (obj.getDescription() != null) {
                node.get(DESCRIPTION).set(obj.getDescription());
            }

            //PROPERTIES
            addProperties(node, obj);
            return node;
        }

        public EntryMetaData unwrap(ModelNode node) {
            if (node == null) {
                return null;
            }

            EntryMetaData entry = new EntryMetaData();
            if (node.has(PATH)) {
                entry.setPath(node.get(PATH).asString());
            }

            if (node.has(DESCRIPTION)) {
                entry.setDescription(node.get(DESCRIPTION).asString());
            }

            //PROPERTIES
            if (node.get(PROPERTIES).isDefined()) {
                List<ModelNode> propNodes = node.get(PROPERTIES).asList();
                for (ModelNode propNode:propNodes) {
                    String[] prop = PropertyMetaDataMapper.INSTANCE.unwrap(propNode);
                    if (prop != null) {
                        entry.addProperty(prop[0], prop[1]);
                    }
                }
            }
            return entry;
        }

        public ObjectTypeAttributeDefinition getAttributeDefinition() {
            ObjectListAttributeDefinition properties = ObjectListAttributeDefinition.Builder.of(PROPERTIES, PropertyMetaDataMapper.INSTANCE.getAttributeDefinition()).build();
            return ObjectTypeAttributeDefinition.Builder.of("EntryMapper", //$NON-NLS-1$
                new AttributeDefinition[] {
                    createAttribute(PATH, ModelType.STRING, false),
                    properties
            }).build();
        }

    }

    /**
     * DataPolicy Metadata mapper
     */
    public static class DataPolicyMetadataMapper implements MetadataMapper<DataPolicyMetadata>{
        private static final String POLICY_NAME = "policy-name"; //$NON-NLS-1$
        private static final String DATA_PERMISSIONS = "data-permissions"; //$NON-NLS-1$
        private static final String MAPPED_ROLE_NAMES = "mapped-role-names"; //$NON-NLS-1$
        private static final String ALLOW_CREATE_TEMP_TABLES = "allow-create-temp-tables"; //$NON-NLS-1$
        private static final String ANY_AUTHENTICATED = "any-authenticated"; //$NON-NLS-1$
        private static final String GRANT_ALL = "grant-all"; //$NON-NLS-1$
        private static final String POLICY_DESCRIPTION = "policy-description"; //$NON-NLS-1$

        public static DataPolicyMetadataMapper INSTANCE = new DataPolicyMetadataMapper();

        public ModelNode wrap(DataPolicyMetadata policy, ModelNode node) {
            if (policy == null) {
                return null;
            }

            node.get(POLICY_NAME).set(policy.getName());
            if (policy.getDescription() != null) {
                node.get(POLICY_DESCRIPTION).set(policy.getDescription());
            }
            if (policy.isAllowCreateTemporaryTables() != null) {
                node.get(ALLOW_CREATE_TEMP_TABLES).set(policy.isAllowCreateTemporaryTables());
            }
            node.get(ANY_AUTHENTICATED).set(policy.isAnyAuthenticated());
            if (policy.isGrantAll()) {
                node.get(GRANT_ALL).set(policy.isGrantAll());
            }

            //DATA_PERMISSIONS
            List<DataPolicy.DataPermission> permissions = policy.getPermissions();
            if (permissions != null && !permissions.isEmpty()) {
                ModelNode permissionNodes = node.get(DATA_PERMISSIONS);
                for (DataPolicy.DataPermission dataPermission:permissions) {
                    permissionNodes.add(PermissionMetaDataMapper.INSTANCE.wrap((PermissionMetaData)dataPermission,  new ModelNode()));
                }
            }

            //MAPPED_ROLE_NAMES
            if (policy.getMappedRoleNames() != null && !policy.getMappedRoleNames().isEmpty()) {
                ModelNode mappedRoleNodes = node.get(MAPPED_ROLE_NAMES);
                for (String role:policy.getMappedRoleNames()) {
                    mappedRoleNodes.add(role);
                }
            }
            return node;
        }

        public DataPolicyMetadata unwrap(ModelNode node) {
            if(node == null) {
                return null;
            }
            DataPolicyMetadata policy = new DataPolicyMetadata();
            if (node.has(POLICY_NAME)) {
                policy.setName(node.get(POLICY_NAME).asString());
            }
            if (node.has(POLICY_DESCRIPTION)) {
                policy.setDescription(node.get(POLICY_DESCRIPTION).asString());
            }
            if (node.has(ALLOW_CREATE_TEMP_TABLES)) {
                policy.setAllowCreateTemporaryTables(node.get(ALLOW_CREATE_TEMP_TABLES).asBoolean());
            }
            if (node.has(ANY_AUTHENTICATED)) {
                policy.setAnyAuthenticated(node.get(ANY_AUTHENTICATED).asBoolean());
            }
            if (node.has(GRANT_ALL)) {
                policy.setGrantAll(node.get(GRANT_ALL).asBoolean());
            }

            //DATA_PERMISSIONS
            if (node.get(DATA_PERMISSIONS).isDefined()) {
                List<ModelNode> permissionNodes = node.get(DATA_PERMISSIONS).asList();
                for (ModelNode permissionNode:permissionNodes) {
                    PermissionMetaData permission = PermissionMetaDataMapper.INSTANCE.unwrap(permissionNode);
                    if (permission != null) {
                        policy.addPermission(permission);
                    }
                }
            }

            //MAPPED_ROLE_NAMES
            if (node.get(MAPPED_ROLE_NAMES).isDefined()) {
                List<ModelNode> roleNameNodes = node.get(MAPPED_ROLE_NAMES).asList();
                for (ModelNode roleNameNode:roleNameNodes) {
                    policy.addMappedRoleName(roleNameNode.asString());
                }
            }
            return policy;
        }

        public ObjectTypeAttributeDefinition getAttributeDefinition() {
            ObjectListAttributeDefinition dataPermisstions = ObjectListAttributeDefinition.Builder.of(DATA_PERMISSIONS, PermissionMetaDataMapper.INSTANCE.getAttributeDefinition()).build();
            StringListAttributeDefinition roleNames = new StringListAttributeDefinition.Builder(MAPPED_ROLE_NAMES).build();
            return ObjectTypeAttributeDefinition.Builder.of("DataPolicyMetadataMapper", //$NON-NLS-1$
                new AttributeDefinition[] {
                    createAttribute(POLICY_NAME, ModelType.STRING, true),
                    createAttribute(POLICY_DESCRIPTION, ModelType.STRING, true),
                    createAttribute(ALLOW_CREATE_TEMP_TABLES, ModelType.BOOLEAN, true),
                    createAttribute(ANY_AUTHENTICATED, ModelType.BOOLEAN, true),
                    createAttribute(GRANT_ALL, ModelType.BOOLEAN, true),
                    dataPermisstions,
                    roleNames
            }).build();
        }
    }

    public static class PermissionMetaDataMapper implements MetadataMapper<PermissionMetaData>{
        private static final String RESOURCE_NAME = "resource-name"; //$NON-NLS-1$
        private static final String RESOURCE_TYPE = "resource-type"; //$NON-NLS-1$
        private static final String ALLOW_CREATE = "allow-create"; //$NON-NLS-1$
        private static final String ALLOW_DELETE = "allow-delete"; //$NON-NLS-1$
        private static final String ALLOW_UPADTE = "allow-update"; //$NON-NLS-1$
        private static final String ALLOW_READ = "allow-read"; //$NON-NLS-1$
        private static final String ALLOW_EXECUTE = "allow-execute"; //$NON-NLS-1$
        private static final String ALLOW_ALTER = "allow-alter"; //$NON-NLS-1$
        private static final String ALLOW_LANGUAGE = "allow-language"; //$NON-NLS-1$
        private static final String CONDITION = "condition"; //$NON-NLS-1$
        private static final String MASK = "mask"; //$NON-NLS-1$
        private static final String ORDER = "order"; //$NON-NLS-1$
        private static final String CONSTRAINT = "constraint"; //$NON-NLS-1$

        public static PermissionMetaDataMapper INSTANCE = new PermissionMetaDataMapper();

        public ModelNode wrap(PermissionMetaData permission, ModelNode node) {
            if (permission == null) {
                return null;
            }

            node.get(RESOURCE_NAME).set(permission.getResourceName());
            if (permission.getResourceType() != null) {
                node.get(RESOURCE_TYPE).set(permission.getResourceType().name());
            }
            if(permission.getAllowLanguage() != null) {
                node.get(ALLOW_LANGUAGE).set(permission.getAllowLanguage().booleanValue());
                return node;
            }
            if (permission.getAllowCreate() != null) {
                node.get(ALLOW_CREATE).set(permission.getAllowCreate().booleanValue());
            }
            if (permission.getAllowDelete() != null) {
                node.get(ALLOW_DELETE).set(permission.getAllowDelete().booleanValue());
            }
            if (permission.getAllowUpdate() != null) {
                node.get(ALLOW_UPADTE).set(permission.getAllowUpdate().booleanValue());
            }
            if (permission.getAllowRead() != null) {
                node.get(ALLOW_READ).set(permission.getAllowRead().booleanValue());
            }
            if (permission.getAllowExecute() != null) {
                node.get(ALLOW_EXECUTE).set(permission.getAllowExecute().booleanValue());
            }
            if(permission.getAllowAlter() != null) {
                node.get(ALLOW_ALTER).set(permission.getAllowAlter().booleanValue());
            }
            if(permission.getCondition() != null) {
                node.get(CONDITION).set(permission.getCondition());
            }
            if(permission.getMask() != null) {
                node.get(MASK).set(permission.getMask());
            }
            if(permission.getOrder() != null) {
                node.get(ORDER).set(permission.getOrder());
            }
            if (permission.getConstraint() != null) {
                node.get(CONSTRAINT).set(permission.getConstraint());
            }
            return node;
        }

        public PermissionMetaData unwrap(ModelNode node) {
            if (node == null) {
                return null;
            }

            PermissionMetaData permission = new PermissionMetaData();
            if (node.get(RESOURCE_NAME) != null) {
                permission.setResourceName(node.get(RESOURCE_NAME).asString());
            }
            if (node.has(RESOURCE_TYPE)) {
                permission.setResourceType(ResourceType.valueOf(node.get(RESOURCE_TYPE).asString()));
            }
            if (node.has(ALLOW_LANGUAGE)) {
                permission.setAllowLanguage(node.get(ALLOW_LANGUAGE).asBoolean());
                return permission;
            }
            if (node.has(ALLOW_CREATE)) {
                permission.setAllowCreate(node.get(ALLOW_CREATE).asBoolean());
            }
            if (node.has(ALLOW_DELETE)) {
                permission.setAllowDelete(node.get(ALLOW_DELETE).asBoolean());
            }
            if (node.has(ALLOW_UPADTE)) {
                permission.setAllowUpdate(node.get(ALLOW_UPADTE).asBoolean());
            }
            if (node.has(ALLOW_READ)) {
                permission.setAllowRead(node.get(ALLOW_READ).asBoolean());
            }
            if (node.has(ALLOW_EXECUTE)) {
                permission.setAllowExecute(node.get(ALLOW_EXECUTE).asBoolean());
            }
            if (node.has(ALLOW_ALTER)) {
                permission.setAllowAlter(node.get(ALLOW_ALTER).asBoolean());
            }
            if (node.has(CONDITION)) {
                permission.setCondition(node.get(CONDITION).asString());
            }
            if (node.has(MASK)) {
                permission.setMask(node.get(MASK).asString());
            }
            if (node.has(ORDER)) {
                permission.setOrder(node.get(ORDER).asInt());
            }
            if (node.has(CONSTRAINT)) {
                permission.setConstraint(node.get(CONSTRAINT).asBoolean());
            }
            return permission;
        }

        public ObjectTypeAttributeDefinition getAttributeDefinition() {
            return ObjectTypeAttributeDefinition.Builder.of("PermissionMetaData", //$NON-NLS-1$
                new AttributeDefinition[] {
                    createAttribute(RESOURCE_NAME, ModelType.STRING, false),
                    createAttribute(RESOURCE_TYPE, ModelType.STRING, true),
                    createAttribute(ALLOW_CREATE, ModelType.BOOLEAN, true),
                    createAttribute(ALLOW_DELETE, ModelType.BOOLEAN, true),
                    createAttribute(ALLOW_UPADTE, ModelType.BOOLEAN, true),
                    createAttribute(ALLOW_READ, ModelType.BOOLEAN, true),
                    createAttribute(ALLOW_EXECUTE, ModelType.BOOLEAN, true),
                    createAttribute(ALLOW_ALTER, ModelType.BOOLEAN, true),
                    createAttribute(ALLOW_LANGUAGE, ModelType.BOOLEAN, true),
                    createAttribute(CONDITION, ModelType.STRING, true),
                    createAttribute(MASK, ModelType.STRING, true),
                    createAttribute(ORDER, ModelType.INT, true),
                    createAttribute(CONSTRAINT, ModelType.BOOLEAN, true)
            }).build();
        }
    }

    public static class EngineStatisticsMetadataMapper implements MetadataMapper<EngineStatisticsMetadata>{
        private static final String SESSION_COUNT = "session-count"; //$NON-NLS-1$
        private static final String TOTAL_MEMORY_USED_IN_KB = "total-memory-inuse-kb"; //$NON-NLS-1$
        private static final String MEMORY_IN_USE_BY_ACTIVE_PLANS = "total-memory-inuse-active-plans-kb";//$NON-NLS-1$
        private static final String DISK_WRITE_COUNT = "buffermgr-disk-write-count"; //$NON-NLS-1$
        private static final String DISK_READ_COUNT = "buffermgr-disk-read-count"; //$NON-NLS-1$
        private static final String CACHE_WRITE_COUNT = "buffermgr-cache-write-count"; //$NON-NLS-1$
        private static final String CACHE_READ_COUNT = "buffermgr-cache-read-count"; //$NON-NLS-1$
        private static final String DISK_SPACE_USED = "buffermgr-diskspace-used-mb"; //$NON-NLS-1$
        private static final String ACTIVE_PLAN_COUNT = "active-plans-count"; //$NON-NLS-1$
        private static final String WAITING_PLAN_COUNT = "waiting-plans-count"; //$NON-NLS-1$
        private static final String MAX_WAIT_PLAN_COUNT = "max-waitplan-watermark"; //$NON-NLS-1$

        public static EngineStatisticsMetadataMapper INSTANCE = new EngineStatisticsMetadataMapper();

        public ModelNode wrap(EngineStatisticsMetadata object, ModelNode node) {
            if (object == null)
                return null;

            node.get(SESSION_COUNT).set(object.getSessionCount());
            node.get(TOTAL_MEMORY_USED_IN_KB).set(object.getTotalMemoryUsedInKB());
            node.get(MEMORY_IN_USE_BY_ACTIVE_PLANS).set(object.getMemoryUsedByActivePlansInKB());
            node.get(DISK_WRITE_COUNT).set(object.getDiskWriteCount());
            node.get(DISK_READ_COUNT).set(object.getDiskReadCount());
            node.get(CACHE_WRITE_COUNT).set(object.getCacheWriteCount());
            node.get(CACHE_READ_COUNT).set(object.getCacheReadCount());
            node.get(DISK_SPACE_USED).set(object.getDiskSpaceUsedInMB());
            node.get(ACTIVE_PLAN_COUNT).set(object.getActivePlanCount());
            node.get(WAITING_PLAN_COUNT).set(object.getWaitPlanCount());
            node.get(MAX_WAIT_PLAN_COUNT).set(object.getMaxWaitPlanWaterMark());

            wrapDomain(object, node);
            return node;
        }

        public EngineStatisticsMetadata unwrap(ModelNode node) {
            if (node == null)
                return null;

            EngineStatisticsMetadata stats = new EngineStatisticsMetadata();
            stats.setSessionCount(node.get(SESSION_COUNT).asInt());
            stats.setTotalMemoryUsedInKB(node.get(TOTAL_MEMORY_USED_IN_KB).asLong());
            stats.setMemoryUsedByActivePlansInKB(node.get(MEMORY_IN_USE_BY_ACTIVE_PLANS).asLong());
            stats.setDiskWriteCount(node.get(DISK_WRITE_COUNT).asLong());
            stats.setDiskReadCount(node.get(DISK_READ_COUNT).asLong());
            stats.setCacheReadCount(node.get(CACHE_READ_COUNT).asLong());
            stats.setCacheWriteCount(node.get(CACHE_WRITE_COUNT).asLong());
            stats.setDiskSpaceUsedInMB(node.get(DISK_SPACE_USED).asLong());
            stats.setActivePlanCount(node.get(ACTIVE_PLAN_COUNT).asInt());
            stats.setWaitPlanCount(node.get(WAITING_PLAN_COUNT).asInt());
            stats.setMaxWaitPlanWaterMark(node.get(MAX_WAIT_PLAN_COUNT).asInt());

            unwrapDomain(stats, node);
            return stats;
        }

        public AttributeDefinition[] getAttributeDefinitions() {
            return new AttributeDefinition[] {
                    createAttribute(SESSION_COUNT, ModelType.INT, false),
                    createAttribute(TOTAL_MEMORY_USED_IN_KB, ModelType.LONG, false),
                    createAttribute(MEMORY_IN_USE_BY_ACTIVE_PLANS, ModelType.LONG, false),
                    createAttribute(DISK_WRITE_COUNT, ModelType.LONG, false),
                    createAttribute(DISK_READ_COUNT, ModelType.LONG, false),
                    createAttribute(CACHE_READ_COUNT, ModelType.LONG, false),
                    createAttribute(CACHE_WRITE_COUNT, ModelType.LONG, false),
                    createAttribute(DISK_SPACE_USED, ModelType.LONG, false),
                    createAttribute(ACTIVE_PLAN_COUNT, ModelType.INT, false),
                    createAttribute(WAITING_PLAN_COUNT, ModelType.INT, false),
                    createAttribute(MAX_WAIT_PLAN_COUNT, ModelType.INT, false)
            };
        }
    }

    public static class CacheStatisticsMetadataMapper implements MetadataMapper<CacheStatisticsMetadata>{
        private static final String HITRATIO = "hit-ratio"; //$NON-NLS-1$
        private static final String TOTAL_ENTRIES = "total-entries"; //$NON-NLS-1$
        private static final String REQUEST_COUNT = "request-count"; //$NON-NLS-1$

        public static CacheStatisticsMetadataMapper INSTANCE = new CacheStatisticsMetadataMapper();

        public ModelNode wrap(CacheStatisticsMetadata object, ModelNode node) {
            if (object == null)
                return null;

            node.get(TOTAL_ENTRIES).set(object.getTotalEntries());
            node.get(HITRATIO).set(String.valueOf(object.getHitRatio()));
            node.get(REQUEST_COUNT).set(object.getRequestCount());

            wrapDomain(object, node);
            return node;
        }

        public CacheStatisticsMetadata unwrap(ModelNode node) {
            if (node == null)
                return null;

            CacheStatisticsMetadata cache = new CacheStatisticsMetadata();
            cache.setTotalEntries(node.get(TOTAL_ENTRIES).asInt());
            cache.setHitRatio(Double.parseDouble(node.get(HITRATIO).asString()));
            cache.setRequestCount(node.get(REQUEST_COUNT).asInt());

            unwrapDomain(cache, node);
            return cache;
        }

        public AttributeDefinition[] getAttributeDefinitions() {
            return new AttributeDefinition[] {
                    createAttribute(TOTAL_ENTRIES, ModelType.INT, false),
                    createAttribute(HITRATIO, ModelType.STRING, false),
                    createAttribute(REQUEST_COUNT, ModelType.INT, false)
            };
        }
    }

    public static class RequestMetadataMapper implements MetadataMapper<RequestMetadata>{
        private static final String TRANSACTION_ID = "transaction-id"; //$NON-NLS-1$
        private static final String NODE_ID = "node-id"; //$NON-NLS-1$
        private static final String SOURCE_REQUEST = "source-request"; //$NON-NLS-1$
        private static final String COMMAND = "command"; //$NON-NLS-1$
        private static final String START_TIME = "start-time"; //$NON-NLS-1$
        private static final String SESSION_ID = "session-id"; //$NON-NLS-1$
        private static final String EXECUTION_ID = "execution-id"; //$NON-NLS-1$
        private static final String STATE = "processing-state"; //$NON-NLS-1$
        private static final String THREAD_STATE = "thread-state"; //$NON-NLS-1$

        public static RequestMetadataMapper INSTANCE = new RequestMetadataMapper();

        public ModelNode wrap(RequestMetadata request, ModelNode node) {
            if (request == null) {
                return null;
            }

            node.get(EXECUTION_ID).set(request.getExecutionId());
            node.get(SESSION_ID).set(request.getSessionId());
            node.get(START_TIME).set(request.getStartTime());
            node.get(COMMAND).set(request.getCommand());
            node.get(SOURCE_REQUEST).set(request.sourceRequest());
            if (request.getNodeId() != null) {
                node.get(NODE_ID).set(request.getNodeId());
            }
            if (request.getTransactionId() != null) {
                node.get(TRANSACTION_ID).set(request.getTransactionId());
            }
            node.get(STATE).set(request.getState().name());
            node.get(THREAD_STATE).set(request.getThreadState().name());

            wrapDomain(request, node);
            return node;
        }

        public RequestMetadata unwrap(ModelNode node) {
            if (node == null)
                return null;

            RequestMetadata request = new RequestMetadata();
            request.setExecutionId(node.get(EXECUTION_ID).asLong());
            request.setSessionId(node.get(SESSION_ID).asString());
            request.setStartTime(node.get(START_TIME).asLong());
            request.setCommand(node.get(COMMAND).asString());
            request.setSourceRequest(node.get(SOURCE_REQUEST).asBoolean());
            if (node.has(NODE_ID)) {
                request.setNodeId(node.get(NODE_ID).asInt());
            }
            if (node.has(TRANSACTION_ID)) {
                request.setTransactionId(node.get(TRANSACTION_ID).asString());
            }
            request.setState(ProcessingState.valueOf(node.get(STATE).asString()));
            request.setThreadState(ThreadState.valueOf(node.get(THREAD_STATE).asString()));

            unwrapDomain(request, node);
            return request;
        }

        public AttributeDefinition[] getAttributeDefinitions() {
            return new AttributeDefinition[] {
                    createAttribute(EXECUTION_ID, ModelType.LONG, false),
                    createAttribute(SESSION_ID, ModelType.STRING, false),
                    createAttribute(START_TIME, ModelType.LONG, false),
                    createAttribute(COMMAND, ModelType.STRING, false),
                    createAttribute(SOURCE_REQUEST, ModelType.BOOLEAN, false),
                    createAttribute(NODE_ID, ModelType.INT, true),
                    createAttribute(TRANSACTION_ID, ModelType.STRING, true),
                    createAttribute(STATE, ModelType.STRING, false),
                    createAttribute(THREAD_STATE, ModelType.STRING, false)
            };
        }
    }

    public static class SessionMetadataMapper implements MetadataMapper<SessionMetadata>{
        private static final String SECURITY_DOMAIN = "security-domain"; //$NON-NLS-1$
        private static final String VDB_VERSION = "vdb-version"; //$NON-NLS-1$
        private static final String VDB_NAME = "vdb-name"; //$NON-NLS-1$
        private static final String USER_NAME = "user-name"; //$NON-NLS-1$
        private static final String SESSION_ID = "session-id"; //$NON-NLS-1$
        private static final String LAST_PING_TIME = "last-ping-time"; //$NON-NLS-1$
        private static final String IP_ADDRESS = "ip-address"; //$NON-NLS-1$
        private static final String CLIENT_HOST_NAME = "client-host-address"; //$NON-NLS-1$
        private static final String CREATED_TIME = "created-time"; //$NON-NLS-1$
        private static final String APPLICATION_NAME = "application-name"; //$NON-NLS-1$
        private static final String CLIENT_HARDWARE_ADRESS = "client-hardware-address"; //$NON-NLS-1$

        public static SessionMetadataMapper INSTANCE = new SessionMetadataMapper();

        public ModelNode wrap(SessionMetadata session, ModelNode node) {
            if (session == null) {
                return null;
            }

            if (session.getApplicationName() != null) {
                node.get(APPLICATION_NAME).set(session.getApplicationName());
            }

            node.get(CREATED_TIME).set(session.getCreatedTime());

            if (session.getClientHostName() != null) {
                node.get(CLIENT_HOST_NAME).set(session.getClientHostName());
            }
            if (session.getIPAddress() != null) {
                node.get(IP_ADDRESS).set(session.getIPAddress());
            }
            node.get(LAST_PING_TIME).set(session.getLastPingTime());
            node.get(SESSION_ID).set(session.getSessionId());
            node.get(USER_NAME).set(session.getUserName());
            node.get(VDB_NAME).set(session.getVDBName());
            node.get(VDB_VERSION).set(session.getVDBVersion());
            if (session.getSecurityDomain() != null){
                node.get(SECURITY_DOMAIN).set(session.getSecurityDomain());
            }
            if (session.getClientHardwareAddress() != null) {
                node.get(CLIENT_HARDWARE_ADRESS).set(session.getClientHardwareAddress());
            }
            wrapDomain(session, node);
            return node;
        }

        public SessionMetadata unwrap(ModelNode node) {
            if (node == null)
                return null;

            SessionMetadata session = new SessionMetadata();
            if (node.has(APPLICATION_NAME)) {
                session.setApplicationName(node.get(APPLICATION_NAME).asString());
            }
            session.setCreatedTime(node.get(CREATED_TIME).asLong());

            if (node.has(CLIENT_HOST_NAME)) {
                session.setClientHostName(node.get(CLIENT_HOST_NAME).asString());
            }

            if (node.has(IP_ADDRESS)) {
                session.setIPAddress(node.get(IP_ADDRESS).asString());
            }

            session.setLastPingTime(node.get(LAST_PING_TIME).asLong());
            session.setSessionId(node.get(SESSION_ID).asString());
            session.setUserName(node.get(USER_NAME).asString());
            session.setVDBName(node.get(VDB_NAME).asString());
            session.setVDBVersion(node.get(VDB_VERSION).asInt());
            if (node.has(SECURITY_DOMAIN)) {
                session.setSecurityDomain(node.get(SECURITY_DOMAIN).asString());
            }
            if (node.has(CLIENT_HARDWARE_ADRESS)) {
                session.setClientHardwareAddress(node.get(CLIENT_HARDWARE_ADRESS).asString());
            }
            unwrapDomain(session, node);
            return session;
        }

        public AttributeDefinition[] getAttributeDefinitions() {
            return new AttributeDefinition[] {
                    createAttribute(APPLICATION_NAME, ModelType.STRING, true),
                    createAttribute(CREATED_TIME, ModelType.LONG, false),
                    createAttribute(CLIENT_HOST_NAME, ModelType.STRING, true),
                    createAttribute(CLIENT_HARDWARE_ADRESS, ModelType.STRING, true),
                    createAttribute(IP_ADDRESS, ModelType.STRING, true),
                    createAttribute(LAST_PING_TIME, ModelType.LONG, false),
                    createAttribute(SESSION_ID, ModelType.STRING, false),
                    createAttribute(USER_NAME, ModelType.STRING, false),
                    createAttribute(VDB_NAME, ModelType.STRING, false),
                    createAttribute(VDB_VERSION, ModelType.STRING, false),
                    createAttribute(SECURITY_DOMAIN, ModelType.STRING, true)
            };
        }
    }

    public static class TransactionMetadataMapper implements MetadataMapper<TransactionMetadata>{
        private static final String ID = "txn-id"; //$NON-NLS-1$
        private static final String SCOPE = "txn-scope"; //$NON-NLS-1$
        private static final String CREATED_TIME = "txn-created-time"; //$NON-NLS-1$
        private static final String ASSOCIATED_SESSION = "session-id"; //$NON-NLS-1$

        public static TransactionMetadataMapper INSTANCE = new TransactionMetadataMapper();

        public ModelNode wrap(TransactionMetadata object, ModelNode transaction) {
            if (object == null)
                return null;

            transaction.get(ASSOCIATED_SESSION).set(object.getAssociatedSession());
            transaction.get(CREATED_TIME).set(object.getCreatedTime());
            transaction.get(SCOPE).set(object.getScope());
            transaction.get(ID).set(object.getId());
            wrapDomain(object, transaction);
            return transaction;
        }

        public TransactionMetadata unwrap(ModelNode node) {
            if (node == null)
                return null;

            TransactionMetadata transaction = new TransactionMetadata();
            transaction.setAssociatedSession(node.get(ASSOCIATED_SESSION).asString());
            transaction.setCreatedTime(node.get(CREATED_TIME).asLong());
            transaction.setScope(node.get(SCOPE).asString());
            transaction.setId(node.get(ID).asString());
            unwrapDomain(transaction, node);
            return transaction;
        }

        public AttributeDefinition[] getAttributeDefinitions() {
            return new AttributeDefinition[] {
                    createAttribute(ASSOCIATED_SESSION, ModelType.STRING, false),
                    createAttribute(CREATED_TIME, ModelType.LONG, false),
                    createAttribute(SCOPE, ModelType.LONG, false),
                    createAttribute(ID, ModelType.STRING, false)
            };
        };
    }

    public static class WorkerPoolStatisticsMetadataMapper implements MetadataMapper<WorkerPoolStatisticsMetadata>{
        private static final String MAX_THREADS = "max-threads"; //$NON-NLS-1$
        private static final String HIGHEST_QUEUED = "highest-queued"; //$NON-NLS-1$
        private static final String QUEUED = "queued"; //$NON-NLS-1$
        private static final String QUEUE_NAME = "queue-name"; //$NON-NLS-1$
        private static final String TOTAL_SUBMITTED = "total-submitted"; //$NON-NLS-1$
        private static final String TOTAL_COMPLETED = "total-completed"; //$NON-NLS-1$
        private static final String HIGHEST_ACTIVE_THREADS = "highest-active-threads"; //$NON-NLS-1$
        private static final String ACTIVE_THREADS = "active-threads"; //$NON-NLS-1$

        public static WorkerPoolStatisticsMetadataMapper INSTANCE = new WorkerPoolStatisticsMetadataMapper();

        public ModelNode wrap(WorkerPoolStatisticsMetadata stats, ModelNode node) {
            if (stats == null)
                return null;
            node.get(TYPE).set(ModelType.OBJECT);
            node.get(ACTIVE_THREADS).set(stats.getActiveThreads());
            node.get(HIGHEST_ACTIVE_THREADS).set(stats.getHighestActiveThreads());
            node.get(TOTAL_COMPLETED).set(stats.getTotalCompleted());
            node.get(TOTAL_SUBMITTED).set(stats.getTotalSubmitted());
            node.get(QUEUE_NAME).set(stats.getQueueName());
            node.get(QUEUED).set(stats.getQueued());
            node.get(HIGHEST_QUEUED).set(stats.getHighestQueued());
            node.get(MAX_THREADS).set(stats.getMaxThreads());
            wrapDomain(stats, node);
            return node;
        }

        public WorkerPoolStatisticsMetadata unwrap(ModelNode node) {
            if (node == null)
                return null;

            WorkerPoolStatisticsMetadata stats = new WorkerPoolStatisticsMetadata();
            stats.setActiveThreads(node.get(ACTIVE_THREADS).asInt());
            stats.setHighestActiveThreads(node.get(HIGHEST_ACTIVE_THREADS).asInt());
            stats.setTotalCompleted(node.get(TOTAL_COMPLETED).asLong());
            stats.setTotalSubmitted(node.get(TOTAL_SUBMITTED).asLong());
            stats.setQueueName(node.get(QUEUE_NAME).asString());
            stats.setQueued(node.get(QUEUED).asInt());
            stats.setHighestQueued(node.get(HIGHEST_QUEUED).asInt());
            stats.setMaxThreads(node.get(MAX_THREADS).asInt());
            unwrapDomain(stats, node);
            return stats;
        }

        public AttributeDefinition[] getAttributeDefinitions() {
            return new AttributeDefinition[] {
                    createAttribute(ACTIVE_THREADS, ModelType.INT, false),
                    createAttribute(HIGHEST_ACTIVE_THREADS, ModelType.INT, false),
                    createAttribute(TOTAL_COMPLETED, ModelType.LONG, false),
                    createAttribute(TOTAL_SUBMITTED, ModelType.LONG, false),
                    createAttribute(QUEUE_NAME, ModelType.STRING, false),
                    createAttribute(QUEUED, ModelType.INT, false),
                    createAttribute(HIGHEST_QUEUED, ModelType.INT, false),
                    createAttribute(MAX_THREADS, ModelType.INT, false)
                };
        }
    }

    public static void wrapDomain(AdminObjectImpl anObj, ModelNode node) {
        if (anObj.getServerGroup() != null) {
            node.get(SERVER_GROUP).set(anObj.getServerGroup());
        }
        if (anObj.getHostName() != null) {
            node.get(HOST_NAME).set(anObj.getHostName());
        }
        if (anObj.getServerName() != null) {
            node.get(SERVER_NAME).set(anObj.getServerName());
        }
    }

    public static void unwrapDomain(AdminObjectImpl anObj, ModelNode node) {
        if (node.get(SERVER_GROUP).isDefined()) {
            anObj.setServerGroup(node.get(SERVER_GROUP).asString());
        }
        if (node.get(HOST_NAME).isDefined()) {
            anObj.setHostName(node.get(HOST_NAME).asString());
        }
        if (node.get(SERVER_NAME).isDefined()) {
            anObj.setServerName(node.get(SERVER_NAME).asString());
        }
    }

    private static final String SERVER_GROUP = "server-group"; //$NON-NLS-1$
    private static final String HOST_NAME = "host-name"; //$NON-NLS-1$
    private static final String SERVER_NAME = "server-name"; //$NON-NLS-1$
    private static final String TYPE = "type"; //$NON-NLS-1$

    static SimpleAttributeDefinition createAttribute(String name, ModelType dataType, boolean allowNull) {
        return new SimpleAttributeDefinitionBuilder(name, dataType, allowNull).build();
    }

}


