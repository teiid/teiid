/*ode.Id_ADD
 * JBoss, Home of Professional Open != Source.
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
package org.teiid.adminapi.impl;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;
import org.teiid.adminapi.AdminPlugin;
import org.teiid.adminapi.DataPolicy;
import org.teiid.adminapi.Model;
import org.teiid.adminapi.Request.ProcessingState;
import org.teiid.adminapi.Request.ThreadState;
import org.teiid.adminapi.Translator;
import org.teiid.adminapi.VDB.ConnectionType;
import org.teiid.adminapi.VDB.Status;
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
	private static final String DYNAMIC = "dynamic"; //$NON-NLS-1$
	private static final String DATA_POLICIES = "data-policies"; //$NON-NLS-1$
	private static final String DESCRIPTION = "description"; //$NON-NLS-1$
	private static final String ENTRIES = "entries"; //$NON-NLS-1$
	
	public static VDBMetadataMapper INSTANCE = new VDBMetadataMapper();
	
	public ModelNode wrap(VDBMetaData vdb, ModelNode node) {
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
		node.get(DYNAMIC).set(vdb.isDynamic());
		
		//PROPERTIES
		Properties properties = vdb.getProperties();
		if (properties!= null && !properties.isEmpty()) {
			ModelNode propsNode = node.get(PROPERTIES);
			for (String key:properties.stringPropertyNames()) {
				propsNode.add(PropertyMetaDataMapper.INSTANCE.wrap(key, properties.getProperty(key), new ModelNode()));
			}
		}
		
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
				modelNodes.add(ModelMetadataMapper.INSTANCE.wrap(model, new ModelNode()));
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
			vdb.setVersion(node.get(VERSION).asInt());
		}
		if(node.has(VDB_DESCRIPTION)) {
			vdb.setDescription(node.get(VDB_DESCRIPTION).asString());
		}
		if (node.has(DYNAMIC)) {
			vdb.setDynamic(node.get(DYNAMIC).asBoolean());
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
	
	public ModelNode describe(ModelNode node) {
		addAttribute(node, VDBNAME, ModelType.STRING, true); 

		ModelNode connectionsAllowed = new ModelNode();
		connectionsAllowed.add(ConnectionType.NONE.toString());
		connectionsAllowed.add(ConnectionType.ANY.toString());
		connectionsAllowed.add(ConnectionType.BY_VERSION.toString());
		addAttribute(node, CONNECTIONTYPE, ModelType.STRING, false);
		node.get(CONNECTIONTYPE).get(ALLOWED).set(connectionsAllowed);
		
		ModelNode statusAllowed = new ModelNode();
		statusAllowed.add(Status.ACTIVE.toString());
		statusAllowed.add(Status.LOADING.toString());
		statusAllowed.add(Status.FAILED.toString());
		statusAllowed.add(Status.REMOVED.toString());
		addAttribute(node, STATUS, ModelType.STRING, true);
		node.get(STATUS).get(ALLOWED).set(statusAllowed);
		
		addAttribute(node, VERSION, ModelType.INT, true);
		addAttribute(node, VDB_DESCRIPTION, ModelType.STRING, false);
		addAttribute(node, DYNAMIC, ModelType.BOOLEAN, false);
		
		ModelNode props = node.get(PROPERTIES);
		props.get(TYPE).set(ModelType.LIST);
		props.get(DESCRIPTION).set(AdminPlugin.Util.getString(PROPERTIES+DOT_DESC));
		PropertyMetaDataMapper.INSTANCE.describe(props.get(VALUE_TYPE));

		ModelNode vdbImports = node.get(IMPORT_VDBS);	
		vdbImports.get(TYPE).set(ModelType.LIST);
		VDBImportMapper.INSTANCE.describe(vdbImports.get(VALUE_TYPE));
		vdbImports.get(DESCRIPTION).set(AdminPlugin.Util.getString(IMPORT_VDBS+DOT_DESC));
		
		ModelNode models = node.get( MODELS);	
		models.get(TYPE).set(ModelType.LIST);
		ModelMetadataMapper.INSTANCE.describe(models.get(VALUE_TYPE));
		models.get(DESCRIPTION).set(AdminPlugin.Util.getString(MODELS+DOT_DESC));
		
		ModelNode translators = node.get(OVERRIDE_TRANSLATORS);
		translators.get(TYPE).set(ModelType.LIST);
		translators.get(DESCRIPTION).set(AdminPlugin.Util.getString(OVERRIDE_TRANSLATORS+DOT_DESC));
		VDBTranslatorMetaDataMapper.INSTANCE.describe(translators.get(VALUE_TYPE));
		
		ModelNode dataPolicies = node.get(DATA_POLICIES);
		dataPolicies.get(TYPE).set(ModelType.LIST);
		dataPolicies.get(DESCRIPTION).set(AdminPlugin.Util.getString(DATA_POLICIES+DOT_DESC));
		DataPolicyMetadataMapper.INSTANCE.describe(dataPolicies.get(VALUE_TYPE));
		return node;
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
		private static final String METADATA= "metadata"; //$NON-NLS-1$
		private static final String METADATA_TYPE = "metadata-type"; //$NON-NLS-1$
		
		
		public static ModelMetadataMapper INSTANCE = new ModelMetadataMapper();
		
		public ModelNode wrap(ModelMetaData model, ModelNode node) {
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

			Properties properties = model.getProperties();
			if (properties!= null && !properties.isEmpty()) {
				ModelNode propsNode = node.get(PROPERTIES); 
				for (String key:properties.stringPropertyNames()) {
					propsNode.add(PropertyMetaDataMapper.INSTANCE.wrap(key, properties.getProperty(key), new ModelNode()));
				}
			}
			
			List<SourceMappingMetadata> sources = model.getSourceMappings();
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
			if (model.getSchemaText() != null) {
				node.get(METADATA).set(model.getSchemaText());
			}
			if (model.getSchemaSourceType() != null) {
				node.get(METADATA_TYPE).set(model.getSchemaSourceType());
			}
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
			if (node.get(METADATA).isDefined()) {
				model.setSchemaText(node.get(METADATA).asString());
			}
			if (node.get(METADATA_TYPE).isDefined()) {
				model.setSchemaSourceType(node.get(METADATA_TYPE).asString());
			}
			return model;
		}
		
		public ModelNode describe(ModelNode node) {
			ModelNode modelTypes = new ModelNode();
			modelTypes.add(Model.Type.PHYSICAL.toString());
			modelTypes.add(Model.Type.VIRTUAL.toString());
			modelTypes.add(Model.Type.FUNCTION.toString());
			modelTypes.add(Model.Type.OTHER.toString());
			addAttribute(node, MODEL_NAME, ModelType.STRING, true);
			node.get(MODEL_NAME).get(ALLOWED).set(modelTypes);
			
			addAttribute(node, DESCRIPTION, ModelType.STRING, false);
			addAttribute(node, VISIBLE, ModelType.BOOLEAN, false);
			addAttribute(node, MODEL_TYPE, ModelType.STRING, true);
			addAttribute(node, MODELPATH, ModelType.STRING, false);
			
			ModelNode props = node.get(PROPERTIES);
			props.get(TYPE).set(ModelType.LIST);
			props.get(DESCRIPTION).set(AdminPlugin.Util.getString(PROPERTIES+DOT_DESC));
			PropertyMetaDataMapper.INSTANCE.describe(props.get(VALUE_TYPE));

			ModelNode source = node.get(SOURCE_MAPPINGS);
			source.get(TYPE).set(ModelType.LIST);
			source.get(DESCRIPTION).set(AdminPlugin.Util.getString(SOURCE_MAPPINGS+DOT_DESC));
			SourceMappingMetadataMapper.INSTANCE.describe(source.get(VALUE_TYPE));

			ModelNode errors = node.get(VALIDITY_ERRORS);
			errors.get(TYPE).set(ModelType.LIST);
			errors.get(DESCRIPTION).set(AdminPlugin.Util.getString(VALIDITY_ERRORS+DOT_DESC));
			ValidationErrorMapper.INSTANCE.describe(errors.get(VALUE_TYPE));
			
			return node; 
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
				vdbImport.setVersion(node.get(VDB_VERSION).asInt());
			}
			if (node.has(IMPORT_POLICIES)) {
				vdbImport.setImportDataPolicies(node.get(IMPORT_POLICIES).asBoolean());
			}
			return vdbImport;
		}
		
		public ModelNode describe(ModelNode node) {
			addAttribute(node, VDB_NAME, ModelType.STRING, true);
			addAttribute(node, VDB_VERSION, ModelType.INT, true);
			addAttribute(node, IMPORT_POLICIES, ModelType.BOOLEAN, false);
			return node; 
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
		
		public ModelNode describe(ModelNode node) {
			addAttribute(node, ERROR_PATH, ModelType.STRING, false); 
			addAttribute(node, SEVERITY, ModelType.STRING, true);
			addAttribute(node, MESSAGE, ModelType.STRING, true);
			return node; 
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
		
		public ModelNode describe(ModelNode node) {
			addAttribute(node, SOURCE_NAME, ModelType.STRING, true); 
			addAttribute(node, JNDI_NAME, ModelType.STRING, true);
			addAttribute(node, TRANSLATOR_NAME, ModelType.STRING, true);
			return node; 
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

			Properties properties = translator.getProperties();
			if (properties!= null && !properties.isEmpty()) {
				ModelNode propsNode = node.get(PROPERTIES); 
				for (String key:properties.stringPropertyNames()) {
					propsNode.add(PropertyMetaDataMapper.INSTANCE.wrap(key, properties.getProperty(key), new ModelNode()));
				}
			}
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
		
		public ModelNode describe(ModelNode node) {
			addAttribute(node, TRANSLATOR_NAME, ModelType.STRING, true); 
			addAttribute(node, BASETYPE, ModelType.STRING, true);
			addAttribute(node, TRANSLATOR_DESCRIPTION, ModelType.STRING, false);
			addAttribute(node, MODULE_NAME, ModelType.STRING, false);
			
			ModelNode props = node.get(PROPERTIES);
			props.get(TYPE).set(ModelType.LIST);
			props.get(DESCRIPTION).set(AdminPlugin.Util.getString(PROPERTIES+DOT_DESC));
			PropertyMetaDataMapper.INSTANCE.describe(props.get(VALUE_TYPE));
			return node; 
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
		
		public ModelNode describe(ModelNode node) {
			addAttribute(node, PROPERTY_NAME, ModelType.STRING, true);
			addAttribute(node, PROPERTY_VALUE, ModelType.STRING, true);
			return node; 
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
			Properties properties = obj.getProperties();
			if (properties!= null && !properties.isEmpty()) {
				ModelNode propsNode = node.get(PROPERTIES);
				for (String key:properties.stringPropertyNames()) {
					propsNode.add(PropertyMetaDataMapper.INSTANCE.wrap(key, properties.getProperty(key), new ModelNode()));
				}
			}
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
		
		public ModelNode describe(ModelNode node) {
			addAttribute(node, PATH, ModelType.STRING, true);
			
			ModelNode props = node.get(PROPERTIES);
			props.get(TYPE).set(ModelType.LIST);
			props.get(DESCRIPTION).set(AdminPlugin.Util.getString(PROPERTIES+DOT_DESC));
			PropertyMetaDataMapper.INSTANCE.describe(props.get(VALUE_TYPE));
			return node; 
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
		
		public ModelNode describe(ModelNode node) {
			addAttribute(node, POLICY_NAME, ModelType.STRING, true);
			addAttribute(node, POLICY_DESCRIPTION, ModelType.STRING, false);
			addAttribute(node, ALLOW_CREATE_TEMP_TABLES, ModelType.BOOLEAN, false);
			addAttribute(node, ANY_AUTHENTICATED, ModelType.BOOLEAN, false);
			
			ModelNode permissions = node.get(DATA_PERMISSIONS);
			permissions.get(TYPE).set(ModelType.LIST);
			permissions.get(DESCRIPTION).set(AdminPlugin.Util.getString(DATA_PERMISSIONS+DOT_DESC));
			
			PermissionMetaDataMapper.INSTANCE.describe(permissions.get(VALUE_TYPE));
			
			ModelNode roleNames = node.get(MAPPED_ROLE_NAMES);
			roleNames.get(TYPE).set(ModelType.LIST);
			roleNames.get(DESCRIPTION).set(AdminPlugin.Util.getString(MAPPED_ROLE_NAMES+DOT_DESC));
			roleNames.get(VALUE_TYPE).set(ModelType.STRING);
			return node; 
		}
	}	
	
	public static class PermissionMetaDataMapper implements MetadataMapper<PermissionMetaData>{
		private static final String RESOURCE_NAME = "resource-name"; //$NON-NLS-1$
		private static final String ALLOW_CREATE = "allow-create"; //$NON-NLS-1$
		private static final String ALLOW_DELETE = "allow-delete"; //$NON-NLS-1$
		private static final String ALLOW_UPADTE = "allow-update"; //$NON-NLS-1$
		private static final String ALLOW_READ = "allow-read"; //$NON-NLS-1$
		private static final String ALLOW_EXECUTE = "allow-execute"; //$NON-NLS-1$
		private static final String ALLOW_ALTER = "allow-alter"; //$NON-NLS-1$
		private static final String ALLOW_LANGUAGE = "allow-language"; //$NON-NLS-1$
		private static final String CONDITION = "condition"; //$NON-NLS-1$
		
		public static PermissionMetaDataMapper INSTANCE = new PermissionMetaDataMapper();
		
		public ModelNode wrap(PermissionMetaData permission, ModelNode node) {
			if (permission == null) {
				return null;
			}
			
			node.get(RESOURCE_NAME).set(permission.getResourceName());
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
			return permission;
		}
		public ModelNode describe(ModelNode node) {
			addAttribute(node, RESOURCE_NAME, ModelType.STRING, true);
			addAttribute(node, ALLOW_CREATE, ModelType.BOOLEAN, false);
			addAttribute(node, ALLOW_DELETE, ModelType.BOOLEAN, false);
			addAttribute(node, ALLOW_UPADTE, ModelType.BOOLEAN, false);
			addAttribute(node, ALLOW_READ, ModelType.BOOLEAN, false);
			addAttribute(node, ALLOW_EXECUTE, ModelType.BOOLEAN, false);
			addAttribute(node, ALLOW_ALTER, ModelType.BOOLEAN, false);
			addAttribute(node, ALLOW_LANGUAGE, ModelType.BOOLEAN, false);
			return node;
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
		
		public ModelNode describe(ModelNode node) {
			addAttribute(node, SESSION_COUNT, ModelType.INT, true);
			addAttribute(node, TOTAL_MEMORY_USED_IN_KB, ModelType.LONG, true);
			addAttribute(node, MEMORY_IN_USE_BY_ACTIVE_PLANS, ModelType.LONG, true);
			addAttribute(node, DISK_WRITE_COUNT, ModelType.LONG, true);
			addAttribute(node, DISK_READ_COUNT, ModelType.LONG, true);
			addAttribute(node, CACHE_READ_COUNT, ModelType.LONG, true);
			addAttribute(node, CACHE_WRITE_COUNT, ModelType.LONG, true);
			addAttribute(node, DISK_SPACE_USED, ModelType.LONG, true);
			addAttribute(node, ACTIVE_PLAN_COUNT, ModelType.INT, true);
			addAttribute(node, WAITING_PLAN_COUNT, ModelType.INT, true);
			addAttribute(node, MAX_WAIT_PLAN_COUNT, ModelType.INT, true);
			return node;
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
			node.get(HITRATIO).set(object.getHitRatio());
			node.get(REQUEST_COUNT).set(object.getRequestCount());
			
			wrapDomain(object, node);
			return node;
		}

		public CacheStatisticsMetadata unwrap(ModelNode node) {
			if (node == null)
				return null;
				
			CacheStatisticsMetadata cache = new CacheStatisticsMetadata();
			cache.setTotalEntries(node.get(TOTAL_ENTRIES).asInt());
			cache.setHitRatio(node.get(HITRATIO).asDouble());
			cache.setRequestCount(node.get(REQUEST_COUNT).asInt());
			
			unwrapDomain(cache, node);
			return cache;
		}
		
		public ModelNode describe(ModelNode node) {
			addAttribute(node, TOTAL_ENTRIES, ModelType.STRING, true);
			addAttribute(node, HITRATIO, ModelType.STRING, true);
			addAttribute(node, REQUEST_COUNT, ModelType.STRING, true);
			return node; 		
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
		
		public ModelNode describe(ModelNode node) {
			addAttribute(node, EXECUTION_ID, ModelType.LONG, true);
			addAttribute(node, SESSION_ID, ModelType.STRING, true);
			addAttribute(node, START_TIME, ModelType.LONG, true);
			addAttribute(node, COMMAND, ModelType.STRING, true);
			addAttribute(node, SOURCE_REQUEST, ModelType.BOOLEAN, true);
			addAttribute(node, NODE_ID, ModelType.INT, false);
			addAttribute(node, TRANSACTION_ID, ModelType.STRING, false);
			addAttribute(node, STATE, ModelType.STRING, true);
			addAttribute(node, THREAD_STATE, ModelType.STRING, true);
			return node; 		
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
		
		public ModelNode describe(ModelNode node) {
			addAttribute(node, APPLICATION_NAME, ModelType.STRING, false);
			addAttribute(node, CREATED_TIME, ModelType.LONG, true);
			addAttribute(node, CLIENT_HOST_NAME, ModelType.LONG, true);
			addAttribute(node, IP_ADDRESS, ModelType.STRING, true);
			addAttribute(node, LAST_PING_TIME, ModelType.LONG, true);
			addAttribute(node, SESSION_ID, ModelType.STRING, true);
			addAttribute(node, USER_NAME, ModelType.STRING, true);
			addAttribute(node, VDB_NAME, ModelType.STRING, true);
			addAttribute(node, VDB_VERSION, ModelType.INT, true);
			addAttribute(node, SECURITY_DOMAIN, ModelType.STRING, false);
			return node;
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
		
		public ModelNode describe(ModelNode node) {
			addAttribute(node, ASSOCIATED_SESSION, ModelType.STRING, true);
			addAttribute(node, CREATED_TIME, ModelType.LONG, true);
			addAttribute(node, SCOPE, ModelType.LONG, true);
			addAttribute(node, ID, ModelType.STRING, true);
			return node;
		}
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
		
		public ModelNode describe(ModelNode node) {
			addAttribute(node, ACTIVE_THREADS, ModelType.INT, true);
			addAttribute(node, HIGHEST_ACTIVE_THREADS, ModelType.INT, true);
			addAttribute(node, TOTAL_COMPLETED, ModelType.LONG, true);
			addAttribute(node, TOTAL_SUBMITTED, ModelType.LONG, true);
			addAttribute(node, QUEUE_NAME, ModelType.STRING, true);
			addAttribute(node, QUEUED, ModelType.INT, true);
			addAttribute(node, HIGHEST_QUEUED, ModelType.INT, true);
			addAttribute(node, MAX_THREADS, ModelType.INT, true);
			return node;
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
	private static final String DOT_DESC = ".describe"; //$NON-NLS-1$
	private static final String TYPE = "type"; //$NON-NLS-1$
	private static final String REQUIRED = "required"; //$NON-NLS-1$
	private static final String ALLOWED = "allowed"; //$NON-NLS-1$
	private static final String VALUE_TYPE = "value-type"; //$NON-NLS-1$
	static ModelNode addAttribute(ModelNode node, String name, ModelType dataType, boolean required) {
		node.get(name, TYPE).set(dataType);
        node.get(name, DESCRIPTION).set(AdminPlugin.Util.getString(name+DOT_DESC));
        node.get(name, REQUIRED).set(required);
        return node;
    }
	
}


