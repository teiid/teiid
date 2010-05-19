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
package org.teiid.rhq.plugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.naming.NamingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.managed.api.ComponentType;
import org.jboss.managed.api.ManagedComponent;
import org.jboss.managed.api.ManagedObject;
import org.jboss.managed.api.ManagedProperty;
import org.jboss.managed.plugins.ManagedObjectImpl;
import org.jboss.metatype.api.types.MetaType;
import org.jboss.metatype.api.values.CollectionValueSupport;
import org.jboss.metatype.api.values.EnumValueSupport;
import org.jboss.metatype.api.values.GenericValueSupport;
import org.jboss.metatype.api.values.MetaValue;
import org.jboss.metatype.api.values.MetaValueFactory;
import org.jboss.metatype.api.values.SimpleValue;
import org.jboss.metatype.api.values.SimpleValueSupport;
import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.configuration.PropertyList;
import org.rhq.core.domain.configuration.PropertyMap;
import org.rhq.core.domain.configuration.PropertySimple;
import org.rhq.core.pluginapi.inventory.DiscoveredResourceDetails;
import org.rhq.core.pluginapi.inventory.InvalidPluginConfigurationException;
import org.rhq.core.pluginapi.inventory.ResourceDiscoveryComponent;
import org.rhq.core.pluginapi.inventory.ResourceDiscoveryContext;
import org.teiid.adminapi.Request;
import org.teiid.rhq.plugin.util.PluginConstants;
import org.teiid.rhq.plugin.util.ProfileServiceUtil;

/**
 * Discovery component for VDBs
 * 
 */
public class VDBDiscoveryComponent implements ResourceDiscoveryComponent {

	private final Log log = LogFactory.getLog(this.getClass());

	public Set<DiscoveredResourceDetails> discoverResources(
			ResourceDiscoveryContext discoveryContext)
			throws InvalidPluginConfigurationException, Exception {
		Set<DiscoveredResourceDetails> discoveredResources = new HashSet<DiscoveredResourceDetails>();

		Set<ManagedComponent> vdbs = ProfileServiceUtil
				.getManagedComponents(new ComponentType(
						PluginConstants.ComponentType.VDB.TYPE,
						PluginConstants.ComponentType.VDB.SUBTYPE));

		for (ManagedComponent mcVdb : vdbs) {

			String vdbKey = mcVdb.getDeployment().getName();
			String vdbName = ProfileServiceUtil.getSimpleValue(mcVdb, "name",
					String.class);
			Integer vdbVersion = ProfileServiceUtil.getSimpleValue(mcVdb,
					"version", Integer.class);
			String vdbDescription = ProfileServiceUtil.getSimpleValue(mcVdb,
					"description", String.class);
			String vdbStatus = ProfileServiceUtil.getSimpleValue(mcVdb,
					"status", String.class);
			String vdbURL = ProfileServiceUtil.getSimpleValue(mcVdb, "url",
					String.class);

			/**
			 * 
			 * A discovered resource must have a unique key, that must stay the
			 * same when the resource is discovered the next time
			 */
			DiscoveredResourceDetails detail = new DiscoveredResourceDetails(
					discoveryContext.getResourceType(), // ResourceType
					vdbKey, // Resource Key
					vdbName, // Resource Name
					vdbVersion.toString(), // Version
					PluginConstants.ComponentType.VDB.DESCRIPTION, // Description
					discoveryContext.getDefaultPluginConfiguration(), // Plugin
					// Config
					null // Process info from a process scan
			);

			// Get plugin config map for models
			Configuration configuration = detail.getPluginConfiguration();

			configuration.put(new PropertySimple("name", vdbName));
			configuration.put(new PropertySimple("version", vdbVersion));
			configuration
					.put(new PropertySimple("description", vdbDescription));
			configuration.put(new PropertySimple("status", vdbStatus));
			configuration.put(new PropertySimple("url", vdbURL));

			getModels(mcVdb, configuration);

		//	getProperties(mcVdb, configuration);

			detail.setPluginConfiguration(configuration);

			// Add to return values
			discoveredResources.add(detail);
			log.info("Discovered Teiid VDB: " + vdbName);
		}

		return discoveredResources;
	}

	/**
	 * @param mcVdb
	 * @param configuration
	 * @throws Exception
	 */
	private void getModels(ManagedComponent mcVdb, Configuration configuration)
			throws Exception {
		// Get models from VDB
		ManagedProperty property = mcVdb.getProperty("models");
		CollectionValueSupport valueSupport = (CollectionValueSupport) property
				.getValue();
		MetaValue[] metaValues = valueSupport.getElements();

		PropertyList sourceModelsList = new PropertyList("sourceModels");
		configuration.put(sourceModelsList);
		
		PropertyList multiSourceModelsList = new PropertyList("multisourceModels");
		configuration.put(multiSourceModelsList);

		PropertyList logicalModelsList = new PropertyList("logicalModels");
		configuration.put(logicalModelsList);

		PropertyList errorList = new PropertyList("errorList");
		configuration.put(errorList);

		for (MetaValue value : metaValues) {
			GenericValueSupport genValueSupport = (GenericValueSupport) value;
			ManagedObjectImpl managedObject = (ManagedObjectImpl) genValueSupport
					.getValue();

			Boolean isSource = Boolean.TRUE;
			try {
				isSource = ProfileServiceUtil.booleanValue(managedObject
						.getProperty("source").getValue());
			} catch (Exception e) {
				throw e;
			}

			Boolean supportMultiSource = Boolean.TRUE;
			try {
				supportMultiSource = ProfileServiceUtil.booleanValue(managedObject.getProperty("supportsMultiSourceBindings").getValue());
			} catch (Exception e) {
				throw e;
			}

			String modelName = managedObject.getName();
			ManagedProperty connectorBinding = managedObject.getProperty("sourceMappings");
			Collection<Map<String, String>> sourceList = new ArrayList<Map<String, String>>();
			getSourceMappingValue(connectorBinding.getValue(), sourceList);
			String visibility = ((SimpleValueSupport) managedObject.getProperty("visible").getValue()).getValue().toString();
			String type = ((EnumValueSupport) managedObject.getProperty("modelType").getValue()).getValue().toString();

			// Get any model errors/warnings
			MetaValue errors = managedObject.getProperty("errors").getValue();
			if (errors != null) {
				CollectionValueSupport errorValueSupport = (CollectionValueSupport) errors;
				MetaValue[] errorArray = errorValueSupport.getElements();
				for (MetaValue error : errorArray) {
					GenericValueSupport errorGenValueSupport = (GenericValueSupport) error;
					ManagedObject errorMo = (ManagedObject) errorGenValueSupport.getValue();
					String severity = ((SimpleValue) errorMo.getProperty("severity").getValue()).getValue().toString();
					String message = ((SimpleValue) errorMo.getProperty("value").getValue()).getValue().toString();
					PropertyMap errorMap = new PropertyMap("errorMap",
							new PropertySimple("severity", severity),
							new PropertySimple("message", message));
					errorList.add(errorMap);
				}
			}

			for (Map<String, String> sourceMap : sourceList) {

				if (isSource) {
					String sourceName =  sourceMap.get("name");
					String jndiName = sourceMap.get("jndiName");
					String translatorName =  sourceMap.get("translatorName");
					
					PropertyMap model = new PropertyMap("model",
							new PropertySimple("name", modelName),
							new PropertySimple("sourceName", sourceName),
							new PropertySimple("jndiName", jndiName),
							new PropertySimple("translatorName", translatorName),
							new PropertySimple("visibility", visibility),
							new PropertySimple("supportsMultiSource",supportMultiSource));

					model.getSimple("jndiName").setOverride(false);
					sourceModelsList.add(model);
				} else {
					PropertyMap model = new PropertyMap("model",
							new PropertySimple("name", modelName),
							new PropertySimple("type", type),
							new PropertySimple("visibility", visibility));

					logicalModelsList.add(model);
				}
			}
		}
	}

	/**
	 * @param <T>
	 * @param pValue
	 * @param list
	 */
	public static <T> void getSourceMappingValue(MetaValue pValue,
			Collection<Map<String, String>> list) {
		Map<String, String> map = new HashMap<String, String>();
		list.add(map);
		MetaType metaType = pValue.getMetaType();
		if (metaType.isCollection()) {
			for (MetaValue value : ((CollectionValueSupport) pValue) 
					.getElements()) {
				GenericValueSupport genValue = ((GenericValueSupport) value);
				ManagedObject mo = (ManagedObject) genValue.getValue();
				String sourceName = mo.getName();
				String jndi = ((SimpleValue) mo.getProperty("connectionJndiName").getValue()).getValue().toString();
				String translatorName = ((SimpleValue) mo.getProperty("translatorName").getValue()).getValue().toString();
				map.put("name", sourceName);
				map.put("jndiName", jndi);
				map.put("translatorName", translatorName);
			}
		} else {
			throw new IllegalStateException(pValue+ " is not a Collection type");
		}
	}

	/**
	 * @param mc
	 * @param configuration
	 * @throws Exception
	 */
	private void getProperties(ManagedComponent mcVdb,
			Configuration configuration) {

		ManagedProperty mp = mcVdb.getProperty("JAXBProperties");
		Collection<Object> list = new ArrayList<Object>();
		getRequestCollectionValue(mp.getValue(), list);
		PropertyMap vdbPropertyMap = new PropertyMap("vdbProperties");
		configuration.put(vdbPropertyMap);
		setProperties(mp, vdbPropertyMap);

	}

	/**
	 * @param mcMap
	 * @param propertyMap
	 */
	private void setProperties(ManagedProperty mProp, PropertyMap propertyMap) {
		//String value = ProfileServiceUtil.stringValue(mProp.getValue());
		PropertySimple prop = new PropertySimple(mProp.getName(), "test");
		propertyMap.put(prop);

	}
	
	public static <T> void getRequestCollectionValue(MetaValue pValue,
			Collection<Object> list) {
		MetaType metaType = pValue.getMetaType();
		if (metaType.isCollection()) {
			for (MetaValue value : ((CollectionValueSupport) pValue)
					.getElements()) {
				SimpleValueSupport property = (SimpleValueSupport)value;
					list.add(property);
			}
		}
	}

}