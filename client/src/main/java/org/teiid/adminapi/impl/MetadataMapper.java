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
package org.teiid.adminapi.impl;

import java.util.List;
import java.util.Map;

import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;
import org.teiid.adminapi.AdminPlugin;
import org.teiid.adminapi.DataPolicy;
import org.teiid.adminapi.Model;
import org.teiid.adminapi.Translator;
import org.teiid.adminapi.Request.ProcessingState;
import org.teiid.adminapi.Request.ThreadState;
import org.teiid.adminapi.VDB.ConnectionType;
import org.teiid.adminapi.VDB.Status;
import org.teiid.adminapi.impl.DataPolicyMetadata.PermissionMetaData;
import org.teiid.adminapi.impl.ModelMetaData.ValidationError;
 

public class MetadataMapper {
	private static final String VDBNAME = "vdb-name"; //$NON-NLS-1$
	private static final String CONNECTIONTYPE = "connection-type"; //$NON-NLS-1$
	private static final String STATUS = "status"; //$NON-NLS-1$
	private static final String VERSION = "vdb-version"; //$NON-NLS-1$
	private static final String URL = "url"; //$NON-NLS-1$
	private static final String MODELS = "models"; //$NON-NLS-1$
	private static final String OVERRIDE_TRANSLATORS = "override-translators"; //$NON-NLS-1$
	private static final String DESCRIPTION = "description"; //$NON-NLS-1$
	private static final String PROPERTIES = "properties"; //$NON-NLS-1$
	private static final String DYNAMIC = "dynamic"; //$NON-NLS-1$
	private static final String DATA_POLICIES = "data-policies"; //$NON-NLS-1$
	
	
	public static ModelNode wrap(VDBMetaData vdb, ModelNode node) {
		if (vdb == null) {
			return null;
		}
		node.get(ModelNodeConstants.TYPE).set(ModelType.OBJECT);
			
		node.get(VDBNAME).set(vdb.getName());
		node.get(CONNECTIONTYPE).set(vdb.getConnectionType().toString());
		node.get(STATUS).set(vdb.getStatus().toString());
		node.get(VERSION).set(vdb.getVersion());
		if (vdb.getUrl() != null) {
		}
		if (vdb.getDescription() != null) {
			node.get(DESCRIPTION).set(vdb.getDescription());
		}
		node.get(DYNAMIC).set(vdb.isDynamic());
		
		//PROPERTIES
		List<PropertyMetadata> properties = vdb.getJAXBProperties();
		if (properties!= null && !properties.isEmpty()) {
			ModelNode propsNode = node.get(CHILDREN, PROPERTIES); 
			for (PropertyMetadata prop:properties) {
				propsNode.add(PropertyMetaDataMapper.wrap(prop, new ModelNode()));
			}
		}
		
		// MODELS
		Map<String, ModelMetaData> models = vdb.getModelMetaDatas();
		if (models != null && !models.isEmpty()) {
			ModelNode modelNodes = node.get(CHILDREN, MODELS);		
			for(ModelMetaData model:models.values()) {
				modelNodes.add(ModelMetadataMapper.wrap(model, new ModelNode()));
			}
		}
		
		// OVERRIDE_TRANSLATORS
		List<Translator> translators = vdb.getOverrideTranslators();
		if (translators != null && !translators.isEmpty()) {
			ModelNode translatorNodes = node.get(CHILDREN, OVERRIDE_TRANSLATORS);
			for (Translator translator:translators) {
				translatorNodes.add(VDBTranslatorMetaDataMapper.wrap((VDBTranslatorMetaData)translator,  new ModelNode()));
			}
		}
		
		// DATA_POLICIES
		List<DataPolicy> policies = vdb.getDataPolicies();
		if (policies != null && !policies.isEmpty()) {
			ModelNode dataPoliciesNodes = node.get(CHILDREN, DATA_POLICIES);
			for (DataPolicy policy:policies) {
				dataPoliciesNodes.add(DataPolicyMetadataMapper.wrap((DataPolicyMetadata)policy,  new ModelNode()));
			}
		}
		return node;
	}

	public static VDBMetaData unwrap(ModelNode node) {
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
		if (node.has(URL)) {
			vdb.setUrl(node.get(URL).asString());
		}
		if(node.has(DESCRIPTION)) {
			vdb.setDescription(node.get(DESCRIPTION).asString());
		}
		if (node.has(DYNAMIC)) {
			vdb.setDynamic(node.get(DYNAMIC).asBoolean());
		}

		//PROPERTIES
		if (node.get(CHILDREN, PROPERTIES).isDefined()) {
			List<ModelNode> propNodes = node.get(CHILDREN, PROPERTIES).asList();
			for (ModelNode propNode:propNodes) {
				PropertyMetadata prop = PropertyMetaDataMapper.unwrap(propNode);
				if (prop != null) {
					vdb.addProperty(prop.getName(), prop.getValue());
				}
			}
		}
		
		// MODELS
		if (node.get(CHILDREN, MODELS).isDefined()) {
			List<ModelNode> modelNodes = node.get(CHILDREN, MODELS).asList();
			for(ModelNode modelNode:modelNodes) {
				ModelMetaData model = ModelMetadataMapper.unwrap(modelNode);
				if (model != null) {
					vdb.addModel(model);	
				}
			}
		}
		
		// OVERRIDE_TRANSLATORS
		if (node.get(CHILDREN, OVERRIDE_TRANSLATORS).isDefined()) {
			List<ModelNode> translatorNodes = node.get(CHILDREN, OVERRIDE_TRANSLATORS).asList();
			for (ModelNode translatorNode:translatorNodes) {
				VDBTranslatorMetaData translator = VDBTranslatorMetaDataMapper.unwrap(translatorNode);
				if (translator != null) {
					vdb.addOverideTranslator(translator);
				}
			}
		}
		
		// DATA_POLICIES
		if (node.get(CHILDREN, DATA_POLICIES).isDefined()) {
			List<ModelNode> policiesNodes = node.get(CHILDREN, DATA_POLICIES).asList();
			for (ModelNode policyNode:policiesNodes) {
				DataPolicyMetadata policy = DataPolicyMetadataMapper.unwrap(policyNode);
				if (policy != null) {
					vdb.addDataPolicy(policy);	
				}
				
			}
		}
		return vdb;
	}
	
	public static ModelNode describe(ModelNode node) {
		node.get(TYPE).set(ModelType.OBJECT);
		addAttribute(node, VDBNAME, ModelType.STRING, true); 

		ModelNode connectionsAllowed = new ModelNode();
		connectionsAllowed.add(ConnectionType.NONE.toString());
		connectionsAllowed.add(ConnectionType.ANY.toString());
		connectionsAllowed.add(ConnectionType.BY_VERSION.toString());
		addAttribute(node, CONNECTIONTYPE, ModelType.STRING, false).get(ALLOWED).set(connectionsAllowed);
		
		ModelNode statusAllowed = new ModelNode();
		statusAllowed.add(Status.ACTIVE.toString());
		statusAllowed.add(Status.INACTIVE.toString());
		addAttribute(node, STATUS, ModelType.STRING, true).get(ALLOWED).set(statusAllowed);
		
		addAttribute(node, VERSION, ModelType.INT, true);
		addAttribute(node, URL, ModelType.STRING, false);
		addAttribute(node, DESCRIPTION, ModelType.STRING, false);
		addAttribute(node, DYNAMIC, ModelType.BOOLEAN, false);
		
		ModelNode props = node.get(CHILDREN, PROPERTIES);
		props.get(DESCRIPTION).set(AdminPlugin.Util.getString(PROPERTIES+DOT_DESC));
		PropertyMetaDataMapper.describe(props);

		ModelNode models = node.get(CHILDREN, MODELS);		
		ModelMetadataMapper.describe(models);
		models.get(DESCRIPTION).set(AdminPlugin.Util.getString(MODELS+DOT_DESC));
		models.get(MIN_OCCURS).set(1);
		
		ModelNode translators = node.get(CHILDREN, OVERRIDE_TRANSLATORS);
		translators.get(DESCRIPTION).set(AdminPlugin.Util.getString(OVERRIDE_TRANSLATORS+DOT_DESC));
		VDBTranslatorMetaDataMapper.describe(translators);
		
		ModelNode dataPolicies = node.get(CHILDREN, DATA_POLICIES);
		dataPolicies.get(DESCRIPTION).set(AdminPlugin.Util.getString(DATA_POLICIES+DOT_DESC));
		DataPolicyMetadataMapper.describe(dataPolicies);
		return node;
	}
	
	/**
	 * model metadata mapper
	 */
	public static class ModelMetadataMapper {
		private static final String MODEL_NAME = "model-name"; //$NON-NLS-1$
		private static final String DESCRIPTION = "description"; //$NON-NLS-1$
		private static final String VISIBLE = "visible"; //$NON-NLS-1$
		private static final String MODEL_TYPE = "model-type"; //$NON-NLS-1$
		private static final String MODELPATH = "model-path"; //$NON-NLS-1$
		private static final String PROPERTIES = "properties"; //$NON-NLS-1$
		private static final String SOURCE_MAPPINGS = "source-mappings"; //$NON-NLS-1$
		private static final String VALIDITY_ERRORS = "validity-errors"; //$NON-NLS-1$
		
		public static ModelNode wrap(ModelMetaData model, ModelNode node) {
			if (model == null) {
				return null;
			}
			node.get(TYPE).set(ModelType.OBJECT);
			
			node.get(MODEL_NAME).set(model.getName());
			if (model.getDescription() != null) {
				node.get(DESCRIPTION).set(model.getDescription());
			}
			node.get(VISIBLE).set(model.isVisible());
			node.get(MODEL_TYPE).set(model.getModelType().toString());
			if (model.getPath() != null) {
				node.get(MODELPATH).set(model.getPath());
			}

			List<PropertyMetadata> properties = model.getJAXBProperties();
			if (properties!= null && !properties.isEmpty()) {
				ModelNode propsNode = node.get(CHILDREN, PROPERTIES); 
				for (PropertyMetadata prop:properties) {
					propsNode.add(PropertyMetaDataMapper.wrap(prop,  new ModelNode()));
				}
			}
			
			List<SourceMappingMetadata> sources = model.getSourceMappings();
			if (sources != null && !sources.isEmpty()) {
				ModelNode sourceMappingNode = node.get(CHILDREN, SOURCE_MAPPINGS);
				for(SourceMappingMetadata source:sources) {
					sourceMappingNode.add(SourceMappingMetadataMapper.wrap(source,  new ModelNode()));
				}
			}
			
			List<ValidationError> errors = model.getErrors();
			if (errors != null && !errors.isEmpty()) {
				ModelNode errorsNode = node.get(CHILDREN, VALIDITY_ERRORS);
				for (ValidationError error:errors) {
					errorsNode.add(ValidationErrorMapper.wrap(error));
				}
			}
			return node;
		}
		
		public static ModelMetaData unwrap(ModelNode node) {
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

			if (node.get(CHILDREN, PROPERTIES).isDefined()) {
				List<ModelNode> propNodes = node.get(CHILDREN, PROPERTIES).asList();
				for (ModelNode propNode:propNodes) {
					PropertyMetadata prop = PropertyMetaDataMapper.unwrap(propNode);
					if (prop != null) {
						model.addProperty(prop.getName(), prop.getValue());
					}
				}
			}
		
			if (node.get(CHILDREN, SOURCE_MAPPINGS).isDefined()) {
				List<ModelNode> sourceMappingNodes = node.get(CHILDREN, SOURCE_MAPPINGS).asList();
				for (ModelNode sourceMapping:sourceMappingNodes) {
					SourceMappingMetadata source = SourceMappingMetadataMapper.unwrap(sourceMapping);
					if (source != null) {
						model.addSourceMapping(source);
					}
				}
			}
			
			if (node.get(CHILDREN, VALIDITY_ERRORS).isDefined()) {
				List<ModelNode> errorNodes = node.get(CHILDREN, VALIDITY_ERRORS).asList();
				for(ModelNode errorNode:errorNodes) {
					ValidationError error = ValidationErrorMapper.unwrap(errorNode);
					if (error != null) {
						model.addError(error);
					}
				}
			}
			return model;
		}
		
		public static ModelNode describe(ModelNode node) {
			node.get(TYPE).set(ModelType.OBJECT);
			
			ModelNode modelTypes = new ModelNode();
			modelTypes.add(Model.Type.PHYSICAL.toString());
			modelTypes.add(Model.Type.VIRTUAL.toString());
			modelTypes.add(Model.Type.FUNCTION.toString());
			modelTypes.add(Model.Type.OTHER.toString());
			addAttribute(node, MODEL_NAME, ModelType.STRING, true).get(ALLOWED).set(modelTypes);
			
			addAttribute(node, DESCRIPTION, ModelType.STRING, false);
			addAttribute(node, VISIBLE, ModelType.BOOLEAN, false);
			addAttribute(node, MODEL_TYPE, ModelType.STRING, true);
			addAttribute(node, MODELPATH, ModelType.STRING, false);
			
			ModelNode props = node.get(CHILDREN, PROPERTIES);
			props.get(DESCRIPTION).set(AdminPlugin.Util.getString(PROPERTIES+DOT_DESC));
			PropertyMetaDataMapper.describe(props);

			ModelNode source = node.get(CHILDREN, SOURCE_MAPPINGS);
			source.get(DESCRIPTION).set(AdminPlugin.Util.getString(SOURCE_MAPPINGS+DOT_DESC));
			SourceMappingMetadataMapper.describe(source);

			ModelNode errors = node.get(CHILDREN, VALIDITY_ERRORS);
			errors.get(DESCRIPTION).set(AdminPlugin.Util.getString(VALIDITY_ERRORS+DOT_DESC));
			ValidationErrorMapper.describe(errors);
			
			return node; 
		}
	}	
	
	/**
	 * validation error mapper
	 */
	public static class ValidationErrorMapper {
		private static final String ERROR_PATH = "error-path"; //$NON-NLS-1$
		private static final String SEVERITY = "severity"; //$NON-NLS-1$
		private static final String MESSAGE = "message"; //$NON-NLS-1$
		
		public static ModelNode wrap(ValidationError error) {
			if (error == null) {
				return null;
			}
			
			ModelNode node = new ModelNode();
			node.get(TYPE).set(ModelType.OBJECT);
			if (error.getPath() != null) {
				node.get(ERROR_PATH).set(error.getPath());
			}
			node.get(SEVERITY).set(error.getSeverity());
			node.get(MESSAGE).set(error.getValue());
			
			return node;
		}
		
		public static ValidationError unwrap(ModelNode node) {
			if (node == null) {
				return null;
			}
			
			ValidationError error = new ValidationError();
			if (node.has(ERROR_PATH)) {
				error.setPath(node.get(ERROR_PATH).asString());
			}
			if (node.has(SEVERITY)) {
				error.setSeverity(node.get(SEVERITY).asString());
			}
			if(node.has(MESSAGE)) {
				error.setValue(node.get(MESSAGE).asString());
			}
			return error;
		}
		
		public static ModelNode describe(ModelNode node) {
			node.get(TYPE).set(ModelType.OBJECT);
			addAttribute(node, ERROR_PATH, ModelType.STRING, false); 
			addAttribute(node, SEVERITY, ModelType.STRING, true);
			addAttribute(node, MESSAGE, ModelType.STRING, true);
			return node; 
		}			
	}		
	
	/**
	 * Source Mapping Metadata mapper
	 */
	public static class SourceMappingMetadataMapper {
		private static final String SOURCE_NAME = "source-name"; //$NON-NLS-1$
		private static final String JNDI_NAME = "jndi-name"; //$NON-NLS-1$
		private static final String TRANSLATOR_NAME = "translator-name"; //$NON-NLS-1$
		
		public static ModelNode wrap(SourceMappingMetadata source, ModelNode node) {
			if (source == null) {
				return null;
			}
			
			node.get(TYPE).set(ModelType.OBJECT);
			
			node.get(SOURCE_NAME).set(source.getName());
			node.get(JNDI_NAME).set(source.getConnectionJndiName());
			node.get(TRANSLATOR_NAME).set(source.getTranslatorName());
			return node;
		}
		
		public static SourceMappingMetadata unwrap(ModelNode node) {
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
		
		public static ModelNode describe(ModelNode node) {
			node.get(TYPE).set(ModelType.OBJECT);
			addAttribute(node, SOURCE_NAME, ModelType.STRING, true); 
			addAttribute(node, JNDI_NAME, ModelType.STRING, true);
			addAttribute(node, TRANSLATOR_NAME, ModelType.STRING, true);
			return node; 
		}		
	}		
	
	/**
	 * Source Mapping Metadata mapper
	 */
	public static class VDBTranslatorMetaDataMapper {
		private static final String TRANSLATOR_NAME = "translator-name"; //$NON-NLS-1$
		private static final String BASETYPE = "base-type"; //$NON-NLS-1$
		private static final String DESCRIPTION = "description"; //$NON-NLS-1$
		private static final String PROPERTIES = "properties"; //$NON-NLS-1$
		private static final String MODULE_NAME = "module-name"; //$NON-NLS-1$
		
		public static ModelNode wrap(VDBTranslatorMetaData translator, ModelNode node) {
			if (translator == null) {
				return null;
			}
			node.get(TYPE).set(ModelType.OBJECT);
			
			node.get(TRANSLATOR_NAME).set(translator.getName());
			if (translator.getType() != null) {
				node.get(BASETYPE).set(translator.getType());
			}
			if (translator.getDescription() != null) {
				node.get(DESCRIPTION).set(translator.getDescription());
			}
			
			if (translator.getModuleName() != null) {
				node.get(MODULE_NAME).set(translator.getModuleName());
			}

			List<PropertyMetadata> properties = translator.getJAXBProperties();
			if (properties!= null && !properties.isEmpty()) {
				ModelNode propsNode = node.get(CHILDREN, PROPERTIES); 
				for (PropertyMetadata prop:properties) {
					propsNode.add(PropertyMetaDataMapper.wrap(prop, new ModelNode()));
				}
			}
			return node;
		}
		
		public static VDBTranslatorMetaData unwrap(ModelNode node) {
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
			if (node.has(DESCRIPTION)) {
				translator.setDescription(node.get(DESCRIPTION).asString());
			}
			if (node.has(MODULE_NAME)) {
				translator.setModuleName(node.get(MODULE_NAME).asString());
			}
			
			if (node.get(CHILDREN,PROPERTIES).isDefined()) {
				List<ModelNode> propNodes = node.get(CHILDREN, PROPERTIES).asList();
				for (ModelNode propNode:propNodes) {
					PropertyMetadata prop = PropertyMetaDataMapper.unwrap(propNode);
					if (prop != null) {
						translator.addProperty(prop.getName(), prop.getValue());
					}
				}
			}
			return translator;
		}
		
		public static ModelNode describe(ModelNode node) {
			node.get(TYPE).set(ModelType.OBJECT);
			addAttribute(node, TRANSLATOR_NAME, ModelType.STRING, true); 
			addAttribute(node, BASETYPE, ModelType.STRING, true);
			addAttribute(node, DESCRIPTION, ModelType.STRING, false);
			addAttribute(node, MODULE_NAME, ModelType.STRING, false);
			
			ModelNode props = node.get(CHILDREN, PROPERTIES);
			props.get(DESCRIPTION).set(AdminPlugin.Util.getString(PROPERTIES+DOT_DESC));
			PropertyMetaDataMapper.describe(props);
			return node; 
		}		
	}	
	
	/**
	 * Property Metadata mapper
	 */
	public static class PropertyMetaDataMapper {
		private static final String PROPERTY_NAME = "property-name"; //$NON-NLS-1$
		private static final String PROPERTY_VALUE = "property-value"; //$NON-NLS-1$
		
		public static ModelNode wrap(PropertyMetadata property, ModelNode node) {
			if (property == null) {
				return null;
			}			
			node.get(ModelNodeConstants.TYPE).set(ModelType.OBJECT);
			
			node.get(PROPERTY_NAME).set(property.getName());
			node.get(PROPERTY_VALUE).set(property.getValue());
			
			return node;
		}
		
		public static PropertyMetadata unwrap(ModelNode node) {
			if(node == null) {
				return null;
			}
			PropertyMetadata property = new PropertyMetadata();
			if (node.has(PROPERTY_NAME)) {
				property.setName(node.get(PROPERTY_NAME).asString());
			}
			if(node.has(PROPERTY_VALUE)) {
				property.setValue(node.get(PROPERTY_VALUE).asString());
			}
			return property;
		}
		
		public static ModelNode describe(ModelNode node) {
			node.get(TYPE).set(ModelType.OBJECT);
			addAttribute(node, PROPERTY_NAME, ModelType.STRING, true);
			addAttribute(node, PROPERTY_VALUE, ModelType.STRING, true);
			return node; 
		}
	}		
	
	/**
	 * DataPolicy Metadata mapper
	 */
	public static class DataPolicyMetadataMapper {
		private static final String POLICY_NAME = "policy-name"; //$NON-NLS-1$
		private static final String DATA_PERMISSIONS = "data-permissions"; //$NON-NLS-1$
		private static final String MAPPED_ROLE_NAMES = "mapped-role-names"; //$NON-NLS-1$
		private static final String ALLOW_CREATE_TEMP_TABLES = "allow-create-temp-tables"; //$NON-NLS-1$
		private static final String ANY_AUTHENTICATED = "any-authenticated"; //$NON-NLS-1$
		
		public static ModelNode wrap(DataPolicyMetadata policy, ModelNode node) {
			if (policy == null) {
				return null;
			}			
			node.get(ModelNodeConstants.TYPE).set(ModelType.OBJECT);
			
			node.get(POLICY_NAME).set(policy.getName());
			if (policy.getDescription() != null) {
				node.get(DESCRIPTION).set(policy.getDescription());
			}
			if (policy.isAllowCreateTemporaryTables() != null) {
				node.get(ALLOW_CREATE_TEMP_TABLES).set(policy.isAllowCreateTemporaryTables());
			}
			node.get(ANY_AUTHENTICATED).set(policy.isAnyAuthenticated());
			
			//DATA_PERMISSIONS
			List<DataPolicy.DataPermission> permissions = policy.getPermissions();
			if (permissions != null && !permissions.isEmpty()) {
				ModelNode permissionNodes = node.get(CHILDREN, DATA_PERMISSIONS); 
				for (DataPolicy.DataPermission dataPermission:permissions) {
					permissionNodes.add(PermissionMetaDataMapper.wrap((PermissionMetaData)dataPermission,  new ModelNode()));
				}			
			}
			
			//MAPPED_ROLE_NAMES
			if (policy.getMappedRoleNames() != null && !policy.getMappedRoleNames().isEmpty()) {
				ModelNode mappedRoleNodes = node.get(CHILDREN, MAPPED_ROLE_NAMES);
				for (String role:policy.getMappedRoleNames()) {
					mappedRoleNodes.add(role);
				}
			}
			return node;
		}
		
		public static DataPolicyMetadata unwrap(ModelNode node) {
			if(node == null) {
				return null;
			}
			DataPolicyMetadata policy = new DataPolicyMetadata();
			if (node.has(POLICY_NAME)) {
				policy.setName(node.get(POLICY_NAME).asString());
			}
			if (node.has(DESCRIPTION)) {
				policy.setDescription(node.get(DESCRIPTION).asString());
			}
			if (node.has(ALLOW_CREATE_TEMP_TABLES)) {
				policy.setAllowCreateTemporaryTables(node.get(ALLOW_CREATE_TEMP_TABLES).asBoolean());
			}
			if (node.has(ANY_AUTHENTICATED)) {
				policy.setAnyAuthenticated(node.get(ANY_AUTHENTICATED).asBoolean());
			}
			
			//DATA_PERMISSIONS
			if (node.get(CHILDREN, DATA_PERMISSIONS).isDefined()) {
				List<ModelNode> permissionNodes = node.get(CHILDREN, DATA_PERMISSIONS).asList();
				for (ModelNode permissionNode:permissionNodes) {
					PermissionMetaData permission = PermissionMetaDataMapper.unwrap(permissionNode);
					if (permission != null) {
						policy.addPermission(permission);
					}
				}
			}

			//MAPPED_ROLE_NAMES
			if (node.get(CHILDREN, MAPPED_ROLE_NAMES).isDefined()) {
				List<ModelNode> roleNameNodes = node.get(CHILDREN, MAPPED_ROLE_NAMES).asList();
				for (ModelNode roleNameNode:roleNameNodes) {
					policy.addMappedRoleName(roleNameNode.asString());
				}			
			}
			return policy;
		}
		
		public static ModelNode describe(ModelNode node) {
			node.get(TYPE).set(ModelType.OBJECT);
			addAttribute(node, POLICY_NAME, ModelType.STRING, true);
			addAttribute(node, DESCRIPTION, ModelType.STRING, false);
			addAttribute(node, ALLOW_CREATE_TEMP_TABLES, ModelType.BOOLEAN, false);
			addAttribute(node, ANY_AUTHENTICATED, ModelType.BOOLEAN, false);
			
			ModelNode permissions = node.get(CHILDREN, DATA_PERMISSIONS);
			PropertyMetaDataMapper.describe(permissions);
			permissions.get(DESCRIPTION).set(AdminPlugin.Util.getString(DATA_PERMISSIONS+DOT_DESC));
			permissions.get(MIN_OCCURS).set(1);
			
			ModelNode roleNames = node.get(CHILDREN, MAPPED_ROLE_NAMES);
			roleNames.get(TYPE).set(ModelType.LIST);
			roleNames.get(DESCRIPTION).set(AdminPlugin.Util.getString(MAPPED_ROLE_NAMES+DOT_DESC));
			roleNames.get("value-type").set(ModelType.STRING); //$NON-NLS-1$
			return node; 
		}
	}	
	
	public static class PermissionMetaDataMapper{
		private static final String RESOURCE_NAME = "resource-name"; //$NON-NLS-1$
		private static final String ALLOW_CREATE = "allow-create"; //$NON-NLS-1$
		private static final String ALLOW_DELETE = "allow-delete"; //$NON-NLS-1$
		private static final String ALLOW_UPADTE = "allow-update"; //$NON-NLS-1$
		private static final String ALLOW_READ = "allow-read"; //$NON-NLS-1$
		private static final String ALLOW_EXECUTE = "allow-execute"; //$NON-NLS-1$
		private static final String ALLOW_ALTER = "allow-alter"; //$NON-NLS-1$
		
		
		
		public static ModelNode wrap(PermissionMetaData permission, ModelNode node) {
			if (permission == null) {
				return null;
			}
			
			node.get(ModelNodeConstants.TYPE).set(ModelType.OBJECT);
			
			node.get(RESOURCE_NAME).set(permission.getResourceName());
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
			return node;
		}
		
		public static PermissionMetaData unwrap(ModelNode node) {
			if (node == null) {
				return null;
			}
			
			PermissionMetaData permission = new PermissionMetaData();
			if (node.get(RESOURCE_NAME) != null) {
				permission.setResourceName(node.get(RESOURCE_NAME).asString());
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
			return permission;
		}
		public static ModelNode describe(ModelNode node) {
			addAttribute(node, RESOURCE_NAME, ModelType.STRING, true);
			addAttribute(node, ALLOW_CREATE, ModelType.BOOLEAN, false);
			addAttribute(node, ALLOW_DELETE, ModelType.BOOLEAN, false);
			addAttribute(node, ALLOW_UPADTE, ModelType.BOOLEAN, false);
			addAttribute(node, ALLOW_READ, ModelType.BOOLEAN, false);
			addAttribute(node, ALLOW_EXECUTE, ModelType.BOOLEAN, false);
			addAttribute(node, ALLOW_ALTER, ModelType.BOOLEAN, false);
			return node;
		}
	}
	
	public static class CacheStatisticsMetadataMapper {
		private static final String HITRATIO = "hitRatio"; //$NON-NLS-1$
		private static final String TOTAL_ENTRIES = "totalEntries"; //$NON-NLS-1$
		private static final String REQUEST_COUNT = "requestCount"; //$NON-NLS-1$
		
		public static ModelNode wrap(CacheStatisticsMetadata object, ModelNode node) {
			if (object == null)
				return null;
			
			node.get(ModelNodeConstants.TYPE).set(ModelType.OBJECT);
			
			node.get(TOTAL_ENTRIES).set(object.getTotalEntries());
			node.get(HITRATIO).set(object.getHitRatio());
			node.get(REQUEST_COUNT).set(object.getRequestCount());
			
			return node;
		}

		public static CacheStatisticsMetadata unwrap(ModelNode node) {
			if (node == null)
				return null;
				
			CacheStatisticsMetadata cache = new CacheStatisticsMetadata();
			cache.setTotalEntries(node.get(TOTAL_ENTRIES).asInt());
			cache.setHitRatio(node.get(HITRATIO).asDouble());
			cache.setRequestCount(node.get(REQUEST_COUNT).asInt());
			return cache;
		}
		
		public static ModelNode describe(ModelNode node) {
			node.get(TYPE).set(ModelType.OBJECT);
			addAttribute(node, TOTAL_ENTRIES, ModelType.STRING, true);
			addAttribute(node, HITRATIO, ModelType.STRING, true);
			addAttribute(node, REQUEST_COUNT, ModelType.STRING, true);
			return node; 		
		}
	}	
	
	public static class RequestMetadataMapper {
		private static final String TRANSACTION_ID = "transaction-id"; //$NON-NLS-1$
		private static final String NODE_ID = "node-id"; //$NON-NLS-1$
		private static final String SOURCE_REQUEST = "source-request"; //$NON-NLS-1$
		private static final String COMMAND = "command"; //$NON-NLS-1$
		private static final String START_TIME = "start-time"; //$NON-NLS-1$
		private static final String SESSION_ID = "session-id"; //$NON-NLS-1$
		private static final String EXECUTION_ID = "execution-id"; //$NON-NLS-1$
		private static final String STATE = "processing-state"; //$NON-NLS-1$
		private static final String THREAD_STATE = "thread-state"; //$NON-NLS-1$
		
		
		public static ModelNode wrap(RequestMetadata request, ModelNode node) {
			if (request == null) {
				return null;
			}
			node.get(ModelNodeConstants.TYPE).set(ModelType.OBJECT);
			
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
			return node;
		}

		public static RequestMetadata unwrap(ModelNode node) {
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
			return request;
		}
		
		public static ModelNode describe(ModelNode node) {
			node.get(TYPE).set(ModelType.OBJECT);
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
	
	public static class SessionMetadataMapper {
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
		
		
		public static ModelNode wrap(SessionMetadata session, ModelNode node) {
			if (session == null) {
				return null;
			}
			node.get(ModelNodeConstants.TYPE).set(ModelType.OBJECT);
				
			if (session.getApplicationName() != null) {
				node.get(APPLICATION_NAME).set(session.getApplicationName());
			}
			node.get(CREATED_TIME).set(session.getCreatedTime());
			node.get(CLIENT_HOST_NAME).set(session.getClientHostName());
			node.get(IP_ADDRESS).set(session.getIPAddress());
			node.get(LAST_PING_TIME).set(session.getLastPingTime());
			node.get(SESSION_ID).set(session.getSessionId());
			node.get(USER_NAME).set(session.getUserName());
			node.get(VDB_NAME).set(session.getVDBName());
			node.get(VDB_VERSION).set(session.getVDBVersion());
			if (session.getSecurityDomain() != null){
				node.get(SECURITY_DOMAIN).set(session.getSecurityDomain());
			}
			return node;
		}

		public static SessionMetadata unwrap(ModelNode node) {
			if (node == null)
				return null;
				
			SessionMetadata session = new SessionMetadata();
			if (node.has(APPLICATION_NAME)) {
				session.setApplicationName(node.get(APPLICATION_NAME).asString());
			}
			session.setCreatedTime(node.get(CREATED_TIME).asLong());
			session.setClientHostName(node.get(CLIENT_HOST_NAME).asString());
			session.setIPAddress(node.get(IP_ADDRESS).asString());
			session.setLastPingTime(node.get(LAST_PING_TIME).asLong());
			session.setSessionId(node.get(SESSION_ID).asString());
			session.setUserName(node.get(USER_NAME).asString());
			session.setVDBName(node.get(VDB_NAME).asString());
			session.setVDBVersion(node.get(VDB_VERSION).asInt());
			if (node.has(SECURITY_DOMAIN)) {
				session.setSecurityDomain(node.get(SECURITY_DOMAIN).asString());
			}
			return session;
		}
		
		public static ModelNode describe(ModelNode node) {
			node.get(TYPE).set(ModelType.OBJECT);
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
	
	public static class TransactionMetadataMapper {
		private static final String ID = "txn-id"; //$NON-NLS-1$
		private static final String SCOPE = "txn-scope"; //$NON-NLS-1$
		private static final String CREATED_TIME = "txn-created-time"; //$NON-NLS-1$
		private static final String ASSOCIATED_SESSION = "session-id"; //$NON-NLS-1$
		
		public static ModelNode wrap(TransactionMetadata object, ModelNode transaction) {
			if (object == null)
				return null;
			
			transaction.get(ModelNodeConstants.TYPE).set(ModelType.OBJECT);
			transaction.get(ASSOCIATED_SESSION).set(object.getAssociatedSession());
			transaction.get(CREATED_TIME).set(object.getCreatedTime());
			transaction.get(SCOPE).set(object.getScope());
			transaction.get(ID).set(object.getId());
			
			return transaction;
		}

		public static TransactionMetadata unwrap(ModelNode node) {
			if (node == null)
				return null;

			TransactionMetadata transaction = new TransactionMetadata();
			transaction.setAssociatedSession(node.get(ASSOCIATED_SESSION).asString());
			transaction.setCreatedTime(node.get(CREATED_TIME).asLong());
			transaction.setScope(node.get(SCOPE).asString());
			transaction.setId(node.get(ID).asString());
			return transaction;
		}
		
		public static ModelNode describe(ModelNode node) {
			node.get(TYPE).set(ModelType.OBJECT);
			addAttribute(node, ASSOCIATED_SESSION, ModelType.STRING, true);
			addAttribute(node, CREATED_TIME, ModelType.LONG, true);
			addAttribute(node, SCOPE, ModelType.LONG, true);
			addAttribute(node, ID, ModelType.STRING, true);
			return node;
		}
	}	

	public static class WorkerPoolStatisticsMetadataMapper {
		private static final String MAX_THREADS = "max-threads"; //$NON-NLS-1$
		private static final String HIGHEST_QUEUED = "highest-queued"; //$NON-NLS-1$
		private static final String QUEUED = "queued"; //$NON-NLS-1$
		private static final String QUEUE_NAME = "queue-name"; //$NON-NLS-1$
		private static final String TOTAL_SUBMITTED = "total-submitted"; //$NON-NLS-1$
		private static final String TOTAL_COMPLETED = "total-completed"; //$NON-NLS-1$
		private static final String HIGHEST_ACTIVE_THREADS = "highest-active-threads"; //$NON-NLS-1$
		private static final String ACTIVE_THREADS = "active-threads"; //$NON-NLS-1$
		
		
		public static ModelNode wrap(WorkerPoolStatisticsMetadata stats, ModelNode node) {
			if (stats == null)
				return null;
			node.get(ModelNodeConstants.TYPE).set(ModelType.OBJECT);
			
			node.get(ACTIVE_THREADS).set(stats.getActiveThreads());
			node.get(HIGHEST_ACTIVE_THREADS).set(stats.getHighestActiveThreads());
			node.get(TOTAL_COMPLETED).set(stats.getTotalCompleted());
			node.get(TOTAL_SUBMITTED).set(stats.getTotalSubmitted());
			node.get(QUEUE_NAME).set(stats.getQueueName());
			node.get(QUEUED).set(stats.getQueued());
			node.get(HIGHEST_QUEUED).set(stats.getHighestQueued());
			node.get(MAX_THREADS).set(stats.getMaxThreads());
			
			return node;
		}

		public static WorkerPoolStatisticsMetadata unwrapMetaValue(ModelNode node) {
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
			return stats;
		}
		
		public static ModelNode describe(ModelNode node) {
			node.get(TYPE).set(ModelType.OBJECT);
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
	
	private static final String CHILDREN = "children"; //$NON-NLS-1$
	private static final String ATTRIBUTES = "attributes"; //$NON-NLS-1$
	private static final String DOT_DESC = ".describe"; //$NON-NLS-1$
	private static final String TYPE = "type"; //$NON-NLS-1$
	private static final String MIN_OCCURS = "min-occurs"; //$NON-NLS-1$
	private static final String REQUIRED = "required"; //$NON-NLS-1$
	private static final String ALLOWED = "allowed"; //$NON-NLS-1$
	static ModelNode addAttribute(ModelNode node, String name, ModelType dataType, boolean required) {
		node.get(ATTRIBUTES, name, TYPE).set(dataType);
        node.get(ATTRIBUTES, name, DESCRIPTION).set(AdminPlugin.Util.getString(name+DOT_DESC));
        node.get(ATTRIBUTES, name, REQUIRED).set(required);
        return node;
    }
}


