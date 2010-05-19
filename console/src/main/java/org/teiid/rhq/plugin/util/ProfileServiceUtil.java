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

package org.teiid.rhq.plugin.util;

import java.io.File;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.deployers.spi.management.KnownDeploymentTypes;
import org.jboss.deployers.spi.management.ManagementView;
import org.jboss.deployers.spi.management.deploy.DeploymentManager;
import org.jboss.managed.api.ComponentType;
import org.jboss.managed.api.ManagedComponent;
import org.jboss.managed.api.ManagedDeployment;
import org.jboss.managed.api.ManagedProperty;
import org.jboss.managed.api.annotation.ViewUse;
import org.jboss.metatype.api.types.MapCompositeMetaType;
import org.jboss.metatype.api.types.MetaType;
import org.jboss.metatype.api.types.SimpleMetaType;
import org.jboss.metatype.api.values.EnumValue;
import org.jboss.metatype.api.values.MetaValue;
import org.jboss.metatype.api.values.SimpleValue;
import org.jboss.profileservice.spi.ProfileService;
import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.configuration.Property;
import org.rhq.core.domain.configuration.PropertyMap;
import org.rhq.core.domain.configuration.PropertySimple;
import org.rhq.core.domain.configuration.definition.ConfigurationDefinition;
import org.rhq.core.domain.configuration.definition.ConfigurationTemplate;
import org.rhq.core.domain.configuration.definition.PropertyDefinition;
import org.rhq.core.domain.configuration.definition.PropertyDefinitionList;
import org.rhq.core.domain.configuration.definition.PropertyDefinitionMap;
import org.rhq.core.domain.configuration.definition.PropertyDefinitionSimple;
import org.rhq.core.domain.configuration.definition.PropertySimpleType;
import org.rhq.core.domain.resource.ResourceType;
import org.teiid.rhq.plugin.TranslatorComponent;
import org.teiid.rhq.plugin.adapter.api.PropertyAdapter;
import org.teiid.rhq.plugin.adapter.api.PropertyAdapterFactory;

import com.sun.istack.NotNull;
import com.sun.istack.Nullable;

public class ProfileServiceUtil {

	protected final static Log LOG = LogFactory
			.getLog(ProfileServiceUtil.class);
	private static ComponentType DQPTYPE = new ComponentType("teiid", "dqp");
	private static String DQPNAME = "org.teiid.jboss.deployers.RuntimeEngineDeployer";
	private static final Map<String, ComponentType> COMPONENT_TYPE_CACHE = new HashMap<String, ComponentType>();
	private static final Map<String, KnownDeploymentTypes> DEPLOYMENT_TYPE_CACHE = new HashMap<String, KnownDeploymentTypes>();
	private static final Map<String, Configuration> DEFAULT_PLUGIN_CONFIG_CACHE = new HashMap<String, Configuration>();

	protected static final String PLUGIN = "ProfileService";

	public static ComponentType getComponentType(
			@NotNull ResourceType resourceType) {
		String resourceTypeName = resourceType.getName();
		if (COMPONENT_TYPE_CACHE.containsKey(resourceTypeName))
			return COMPONENT_TYPE_CACHE.get(resourceTypeName);
		Configuration defaultPluginConfig = getDefaultPluginConfiguration(resourceType);
		String type = defaultPluginConfig.getSimpleValue(
				TranslatorComponent.Config.COMPONENT_TYPE, null);
		if (type == null || type.equals(""))
			throw new IllegalStateException(
					"Required plugin configuration property '"
							+ TranslatorComponent.Config.COMPONENT_TYPE
							+ "' is not defined in default template.");
		String subtype = defaultPluginConfig.getSimpleValue(
				TranslatorComponent.Config.COMPONENT_SUBTYPE, null);
		if (subtype == null || subtype.equals(""))
			throw new IllegalStateException(
					"Required plugin configuration property '"
							+ TranslatorComponent.Config.COMPONENT_SUBTYPE
							+ "' is not defined in default template.");
		ComponentType componentType = new ComponentType(type, subtype);
		COMPONENT_TYPE_CACHE.put(resourceTypeName, componentType);
		return componentType;
	}

	private static Configuration getDefaultPluginConfiguration(
			ResourceType resourceType) {
		ConfigurationTemplate pluginConfigDefaultTemplate = resourceType
				.getPluginConfigurationDefinition().getDefaultTemplate();
		return (pluginConfigDefaultTemplate != null) ? pluginConfigDefaultTemplate
				.createConfiguration()
				: new Configuration();
	}

	/**
	 * @param name
	 * @param componentType
	 * @return
	 */
	public static boolean isManagedComponent(ManagementView managementView,
			String name, ComponentType componentType) {
		boolean isDeployed = false;
		if (name != null) {
			try {
				ManagedComponent component = getManagedComponent(componentType,
						name);
				if (component != null)
					isDeployed = true;
			} catch (Exception e) {
				// Setting it to true to be safe than sorry, since there might
				// be a component
				// already deployed in the AS.
				isDeployed = true;
			}
		}
		return isDeployed;
	}

	/**
	 * Get the passed in {@link ManagedComponent}
	 * 
	 * @return {@link ManagedComponent}
	 * @throws NamingException
	 * @throws Exception
	 */
	public static ManagedComponent getManagedComponent(
			ComponentType componentType, String componentName)
			throws NamingException, Exception {
		ProfileService ps = getProfileService();
		ManagementView mv = getManagementView(ps, true);

		ManagedComponent mc = mv.getComponent(componentName, componentType);

		return mc;
	}

	/**
	 * Get the {@link ManagedComponent} for the {@link ComponentType} and sub
	 * type.
	 * 
	 * @return Set of {@link ManagedComponent}s
	 * @throws NamingException
	 *             , Exception
	 * @throws Exception
	 */
	public static Set<ManagedComponent> getManagedComponents(
			ComponentType componentType) throws NamingException, Exception {
		ProfileService ps = getProfileService();
		ManagementView mv = getManagementView(ps, true);

		Set<ManagedComponent> mcSet = mv.getComponentsForType(componentType);

		return mcSet;
	}

	/**
	 * @param {@link ManagementView}
	 * @return
	 */
	public static ManagementView getManagementView(ProfileService ps,
			boolean load) {
		ManagementView mv = ps.getViewManager();
		if (load) {
			mv.load();
		}
		return mv;
	}

	/**
	 * Get the {@link DeploymentManager} from the ProfileService
	 * 
	 * @return DeploymentManager
	 * @throws NamingException
	 * @throws Exception
	 */
	public static DeploymentManager getDeploymentManager()
			throws NamingException, Exception {
		ProfileService ps = getProfileService();
		DeploymentManager deploymentManager = ps.getDeploymentManager();

		return deploymentManager;
	}

	/**
	 * @return {@link ProfileService}
	 * @throws NamingException
	 *             , Exception
	 */
	public static ProfileService getProfileService() throws NamingException {
		InitialContext ic = new InitialContext();
		ProfileService ps = (ProfileService) ic
				.lookup(PluginConstants.PROFILE_SERVICE);
		return ps;
	}

	/**
	 * @return {@link File}
	 * @throws NamingException
	 *             , Exception
	 */
	public static File getDeployDirectory() throws NamingException, Exception {
		ProfileService ps = getProfileService();
		ManagementView mv = getManagementView(ps, false);
		Set<ManagedDeployment> warDeployments;
		try {
			warDeployments = mv
					.getDeploymentsForType(KnownDeploymentTypes.JavaEEWebApplication
							.getType());
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
		ManagedDeployment standaloneWarDeployment = null;
		for (ManagedDeployment warDeployment : warDeployments) {
			if (warDeployment.getParent() == null) {
				standaloneWarDeployment = warDeployment;
				break;
			}
		}
		if (standaloneWarDeployment == null)
			// This could happen if no standalone WARs, including the admin
			// console WAR, have been fully deployed yet.
			return null;
		URL warUrl;
		try {
			warUrl = new URL(standaloneWarDeployment.getName());
		} catch (MalformedURLException e) {
			throw new IllegalStateException(e);
		}
		File warFile = new File(warUrl.getPath());
		File deployDir = warFile.getParentFile();
		return deployDir;
	}

	public static ManagedComponent getDQPManagementView()
			throws NamingException, Exception {

		return getManagedComponent(DQPTYPE, DQPNAME);
	}

	public static String stringValue(MetaValue v1) throws Exception {
		if (v1 != null) {
			MetaType type = v1.getMetaType();
			if (type instanceof SimpleMetaType) {
				SimpleValue simple = (SimpleValue) v1;
				return simple.getValue().toString();
			}
			throw new Exception("Failed to convert value to string value");
		}
		return null;
	}

	public static Boolean booleanValue(MetaValue v1) throws Exception {
		if (v1 != null) {
			MetaType type = v1.getMetaType();
			if (type instanceof SimpleMetaType) {
				SimpleValue simple = (SimpleValue) v1;
				return Boolean.valueOf(simple.getValue().toString());
			}
			throw new Exception("Failed to convert value to boolean value");
		}
		return null;
	}

	public static <T> T getSimpleValue(ManagedComponent mc, String prop,
			Class<T> expectedType) {
		ManagedProperty mp = mc.getProperty(prop);
		if (mp != null) {
			MetaType metaType = mp.getMetaType();
			if (metaType.isSimple()) {
				SimpleValue simpleValue = (SimpleValue) mp.getValue();
				return expectedType.cast((simpleValue != null) ? simpleValue
						.getValue() : null);
			} else if (metaType.isEnum()) {
				EnumValue enumValue = (EnumValue) mp.getValue();
				return expectedType.cast((enumValue != null) ? enumValue
						.getValue() : null);
			}
			throw new IllegalStateException(prop + " is not a simple type");
		}
		return null;
	}

	public static Map<String, PropertySimple> getCustomProperties(
			Configuration pluginConfig) {
		Map<String, PropertySimple> customProperties = new LinkedHashMap<String, PropertySimple>();
		if (pluginConfig == null)
			return customProperties;
		PropertyMap customPropsMap = pluginConfig.getMap("custom-properties");
		if (customPropsMap != null) {
			Collection<Property> customProps = customPropsMap.getMap().values();
			for (Property customProp : customProps) {
				if (!(customProp instanceof PropertySimple)) {
					LOG
							.error("Custom property definitions in plugin configuration must be simple properties - property "
									+ customProp + " is not - ignoring...");
					continue;
				}
				customProperties.put(customProp.getName(),
						(PropertySimple) customProp);
			}
		}
		return customProperties;
	}

	public static Configuration convertManagedObjectToConfiguration(
			Map<String, ManagedProperty> managedProperties,
			Map<String, PropertySimple> customProps, ResourceType resourceType) {
		Configuration config = new Configuration();
		ConfigurationDefinition configDef = resourceType
				.getResourceConfigurationDefinition();
		Map<String, PropertyDefinition> propDefs = configDef
				.getPropertyDefinitions();
		Set<String> propNames = managedProperties.keySet();
		for (String propName : propNames) {
			PropertyDefinition propertyDefinition = propDefs.get(propName);
			ManagedProperty managedProperty = managedProperties.get(propName);
			if (propertyDefinition == null) {
				if (!managedProperty.hasViewUse(ViewUse.STATISTIC))
					LOG
							.debug(resourceType
									+ " does not define a property corresponding to ManagedProperty '"
									+ propName + "'.");
				continue;
			}
			if (managedProperty == null) {
				// This should never happen, but don't let it blow us up.
				LOG.error("ManagedProperty '" + propName
						+ "' has a null value in the ManagedProperties Map.");
				continue;
			}
			MetaValue metaValue = managedProperty.getValue();
			if (managedProperty.isRemoved() || metaValue == null) {
				// Don't even add a Property to the Configuration if the
				// ManagedProperty is flagged as removed or has a
				// null value.
				continue;
			}
			PropertySimple customProp = customProps.get(propName);
			PropertyAdapter<Property, PropertyDefinition> propertyAdapter = PropertyAdapterFactory
					.getCustomPropertyAdapter(customProp);
			if (propertyAdapter == null)
				propertyAdapter = PropertyAdapterFactory
						.getPropertyAdapter(metaValue);
			if (propertyAdapter == null) {
				LOG
						.error("Unable to find a PropertyAdapter for ManagedProperty '"
								+ propName
								+ "' with MetaType ["
								+ metaValue.getMetaType()
								+ "] for ResourceType '"
								+ resourceType.getName() + "'.");
				continue;
			}
			Property property = propertyAdapter.convertToProperty(metaValue,
					propertyDefinition);
			config.put(property);
		}
		return config;
	}

	public static void convertConfigurationToManagedProperties(
			Map<String, ManagedProperty> managedProperties,
			Configuration configuration, ResourceType resourceType) {
		ConfigurationDefinition configDefinition = resourceType
				.getResourceConfigurationDefinition();
		for (ManagedProperty managedProperty : managedProperties.values()) {
			String propertyName = managedProperty.getName();
			PropertyDefinition propertyDefinition = configDefinition
					.get(propertyName);
			if (propertyDefinition==null){
				//The managed property is not defined in the configuration
				continue;
			}
			populateManagedPropertyFromProperty(managedProperty,
						propertyDefinition, configuration);
			}
		return;
	}

	private static void populateManagedPropertyFromProperty(ManagedProperty managedProperty,
			PropertyDefinition propertyDefinition, Configuration configuration) {
    	// If the ManagedProperty defines a default value, assume it's more
		// definitive than any default value that may
		// have been defined in the plugin descriptor, and update the
		// PropertyDefinition to use that as its default
		// value.
		MetaValue defaultValue = managedProperty.getDefaultValue();
		if (defaultValue != null)
			updateDefaultValueOnPropertyDefinition(propertyDefinition,
					defaultValue);
		MetaValue metaValue = managedProperty.getValue();
		PropertyAdapter propertyAdapter = null;
		if (metaValue != null) {
			LOG.trace("Populating existing MetaValue of type "
					+ metaValue.getMetaType() + " from RHQ property "
					+ propertyDefinition.getName() + " with definition " + propertyDefinition
					+ "...");
			propertyAdapter = PropertyAdapterFactory
						.getPropertyAdapter(metaValue);
			propertyAdapter.populateMetaValueFromProperty(configuration.getSimple(propertyDefinition.getName()), metaValue,
					propertyDefinition);
		} else {
			MetaType metaType = managedProperty.getMetaType(); 
			if (propertyAdapter == null)
				propertyAdapter = PropertyAdapterFactory
						.getPropertyAdapter(metaType);
			LOG.trace("Converting property " + propertyDefinition.getName() + " with definition "
					+ propertyDefinition + " to MetaValue of type " + metaType
					+ "...");
			metaValue = propertyAdapter.convertToMetaValue(configuration.getSimple(propertyDefinition.getName()),
					propertyDefinition, metaType);
			managedProperty.setValue(metaValue);
		}
	}

	private static void updateDefaultValueOnPropertyDefinition(
			PropertyDefinition propertyDefinition,
			@NotNull MetaValue defaultValue) {
		if (!(propertyDefinition instanceof PropertyDefinitionSimple)) {
			LOG
					.debug("Cannot update default value on non-simple property definition "
							+ propertyDefinition
							+ "(default value is "
							+ defaultValue + ").");
			return;
		}
		MetaType metaType = defaultValue.getMetaType();
		if (!metaType.isSimple() && !metaType.isEnum()) {
			LOG.debug("Cannot update default value on " + propertyDefinition
					+ ", because default value's type (" + metaType
					+ ") is not simple or enum.");
			return;
		}
		PropertyDefinitionSimple propertyDefinitionSimple = (PropertyDefinitionSimple) propertyDefinition;
		if (metaType.isSimple()) {
			SimpleValue defaultSimpleValue = (SimpleValue) defaultValue;
			Serializable value = defaultSimpleValue.getValue();
			propertyDefinitionSimple.setDefaultValue((value != null) ? value
					.toString() : null);
		} else { // defaultValueMetaType.isEnum()
			EnumValue defaultEnumValue = (EnumValue) defaultValue;
			Serializable value = defaultEnumValue.getValue();
			propertyDefinitionSimple.setDefaultValue((value != null) ? value
					.toString() : null);
		}
	}

	public static MetaType convertPropertyDefinitionToMetaType(
			PropertyDefinition propDef) {
		MetaType memberMetaType;
		if (propDef instanceof PropertyDefinitionSimple) {
			PropertySimpleType propSimpleType = ((PropertyDefinitionSimple) propDef)
					.getType();
			memberMetaType = convertPropertySimpleTypeToSimpleMetaType(propSimpleType);
		} else if (propDef instanceof PropertyDefinitionList) {
			// TODO (very low priority, since lists of lists are not going to be
			// at all common)
			memberMetaType = null;
		} else if (propDef instanceof PropertyDefinitionMap) {
			Map<String, PropertyDefinition> memberPropDefs = ((PropertyDefinitionMap) propDef)
					.getPropertyDefinitions();
			if (memberPropDefs.isEmpty())
				throw new IllegalStateException(
						"PropertyDefinitionMap doesn't contain any member PropertyDefinitions.");
			// NOTE: We assume member prop defs are all of the same type, since
			// for MapCompositeMetaTypes, they have to be.
			PropertyDefinition mapMemberPropDef = memberPropDefs.values()
					.iterator().next();
			MetaType mapMemberMetaType = convertPropertyDefinitionToMetaType(mapMemberPropDef);
			memberMetaType = new MapCompositeMetaType(mapMemberMetaType);
		} else {
			throw new IllegalStateException(
					"List member PropertyDefinition has unknown type: "
							+ propDef.getClass().getName());
		}
		return memberMetaType;
	}
	
	 private static MetaType convertPropertySimpleTypeToSimpleMetaType(PropertySimpleType memberSimpleType)
	    {
	        MetaType memberMetaType;
	        Class memberClass;
	        switch (memberSimpleType)
	        {
	            case BOOLEAN:
	                memberClass = Boolean.class;
	                break;
	            case INTEGER:
	                memberClass = Integer.class;
	                break;
	            case LONG:
	                memberClass = Long.class;
	                break;
	            case FLOAT:
	                memberClass = Float.class;
	                break;
	            case DOUBLE:
	                memberClass = Double.class;
	                break;
	            default:
	                memberClass = String.class;
	                break;
	        }
	        memberMetaType = SimpleMetaType.resolve(memberClass.getName());
	        return memberMetaType;
	    }


}
