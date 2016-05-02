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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.naming.NamingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.deployers.spi.management.KnownDeploymentTypes;
import org.jboss.deployers.spi.management.ManagementView;
import org.jboss.deployers.spi.management.deploy.DeploymentManager;
import org.jboss.managed.api.ComponentType;
import org.jboss.managed.api.ManagedCommon;
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
import org.jboss.metatype.api.values.SimpleValueSupport;
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
import org.rhq.plugins.jbossas5.connection.ProfileServiceConnection;
import org.teiid.rhq.plugin.TranslatorComponent;
import org.teiid.rhq.plugin.adapter.api.PropertyAdapter;
import org.teiid.rhq.plugin.adapter.api.PropertyAdapterFactory;

import com.sun.istack.NotNull;

public class ProfileServiceUtil {

	protected final static Log LOG = LogFactory
			.getLog(PluginConstants.DEFAULT_LOGGER_CATEGORY); 
	private static ComponentType DQPTYPE = new ComponentType("teiid", "dqp"); //$NON-NLS-1$ //$NON-NLS-2$
	private static String DQPNAME = "RuntimeEngineDeployer"; //$NON-NLS-1$
	private static String BUFFERSERVICE = "BufferService"; //$NON-NLS-1$
	private static final Map<String, ComponentType> COMPONENT_TYPE_CACHE = new HashMap<String, ComponentType>();
	
	protected static final String PLUGIN = "ProfileService"; //$NON-NLS-1$

	public static ComponentType getComponentType(
			@NotNull ResourceType resourceType) {
		String resourceTypeName = resourceType.getName();
		if (COMPONENT_TYPE_CACHE.containsKey(resourceTypeName))
			return COMPONENT_TYPE_CACHE.get(resourceTypeName);
		Configuration defaultPluginConfig = getDefaultPluginConfiguration(resourceType);
		String type = defaultPluginConfig.getSimpleValue(
				TranslatorComponent.Config.COMPONENT_TYPE, null);
		if (type == null || type.equals("")) //$NON-NLS-1$
			throw new IllegalStateException(
					"Required plugin configuration property '" //$NON-NLS-1$
							+ TranslatorComponent.Config.COMPONENT_TYPE
							+ "' is not defined in default template."); //$NON-NLS-1$
		String subtype = defaultPluginConfig.getSimpleValue(
				TranslatorComponent.Config.COMPONENT_SUBTYPE, null);
		if (subtype == null || subtype.equals("")) //$NON-NLS-1$
			throw new IllegalStateException(
					"Required plugin configuration property '" //$NON-NLS-1$
							+ TranslatorComponent.Config.COMPONENT_SUBTYPE 
							+ "' is not defined in default template."); //$NON-NLS-1$
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
	public static boolean isManagedComponent(
			ProfileServiceConnection connection, String name,
			ComponentType componentType) {
		boolean isDeployed = false;
		if (name != null) {
			try {
				ManagedComponent component = getManagedComponent(connection,
						componentType, name);
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
			ProfileServiceConnection connection, ComponentType componentType,
			String componentName) throws NamingException, Exception {
		ManagedComponent mc = connection.getManagementView().getComponent(
				componentName, componentType);

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
	public static Set<ManagedComponent> getManagedComponents(ProfileServiceConnection connection,
			ComponentType componentType) throws NamingException, Exception {
		
		Set<ManagedComponent> mcSet = connection.getManagementView().getComponentsForType(componentType);

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
	public static DeploymentManager getDeploymentManager(ProfileServiceConnection connection)
			throws NamingException, Exception {

		return connection.getDeploymentManager();
	}

	/**
	 * @return {@link File}
	 * @throws NamingException
	 *             , Exception
	 */
	public static File getDeployDirectory(ProfileServiceConnection connection) throws NamingException, Exception {
		Set<ManagedDeployment> warDeployments;
		try {
			warDeployments = connection.getManagementView()
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

	public static ManagedComponent getRuntimeEngineDeployer(
			ProfileServiceConnection connection) throws NamingException,
			Exception {

		return getManagedComponent(connection, DQPTYPE, DQPNAME);
	}
	
	public static ManagedComponent getBufferService(
			ProfileServiceConnection connection) throws NamingException,
			Exception {

		return getManagedComponent(connection, DQPTYPE, BUFFERSERVICE);
	}

	public static String stringValue(MetaValue v1) throws Exception {
		if (v1 != null) {
			MetaType type = v1.getMetaType();
			if (type instanceof SimpleMetaType) {
				SimpleValue simple = (SimpleValue) v1;
				return simple.getValue().toString();
			}
			throw new Exception("Failed to convert value to string value"); //$NON-NLS-1$
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
			throw new Exception("Failed to convert value to boolean value"); //$NON-NLS-1$
		}
		return null;
	}

	public static Double doubleValue(MetaValue v1) throws Exception {
		if (v1 != null) {
			MetaType type = v1.getMetaType();
			if (type instanceof SimpleMetaType) {
				SimpleValue simple = (SimpleValue) v1;
				return Double.valueOf(simple.getValue().toString());
			}
			throw new Exception("Failed to convert value to double value"); //$NON-NLS-1$
		}
		return null;
	}
	
	public static Long longValue(MetaValue v1) throws Exception {
		if (v1 != null) {
			MetaType type = v1.getMetaType();
			if (type instanceof SimpleMetaType) {
				SimpleValue simple = (SimpleValue) v1;
				return Long.valueOf(simple.getValue().toString());
			}
			throw new Exception("Failed to convert value to long value"); //$NON-NLS-1$
		}
		return null;
	}
	
	public static Integer integerValue(MetaValue v1) throws Exception {
		if (v1 != null) {
			MetaType type = v1.getMetaType();
			if (type instanceof SimpleMetaType) {
				SimpleValue simple = (SimpleValue) v1;
				return Integer.valueOf(simple.getValue().toString());
			}
			throw new Exception("Failed to convert value to integer value"); //$NON-NLS-1$
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
			throw new IllegalStateException(prop + " is not a simple type"); //$NON-NLS-1$
		}
		return null;
	}

	public static <T> T getSimpleValue(MetaValue prop,
			Class<T> expectedType) {
		if (prop != null) {
			MetaType metaType = prop.getMetaType();
			if (metaType.isSimple()) {
				SimpleValue simpleValue = (SimpleValue) prop;
				return expectedType.cast((simpleValue != null) ? simpleValue
						.getValue() : null);
			} else if (metaType.isEnum()) {
				EnumValue enumValue = (EnumValue) prop;
				return expectedType.cast((enumValue != null) ? enumValue
						.getValue() : null);
			}
			throw new IllegalStateException(prop + " is not a simple type"); //$NON-NLS-1$
		}
		return null;
	}
	
	public static <T> T getSimpleValue(ManagedCommon mc, String prop,
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
			throw new IllegalArgumentException(prop + " is not a simple type"); //$NON-NLS-1$
		}
		return null;
	}

	public static Map<String, PropertySimple> getCustomProperties(
			Configuration pluginConfig) {
		Map<String, PropertySimple> customProperties = new LinkedHashMap<String, PropertySimple>();
		if (pluginConfig == null)
			return customProperties;
		PropertyMap customPropsMap = pluginConfig.getMap("custom-properties"); //$NON-NLS-1$
		if (customPropsMap != null) {
			Collection<Property> customProps = customPropsMap.getMap().values();
			for (Property customProp : customProps) {
				if (!(customProp instanceof PropertySimple)) {
					LOG
							.error("Custom property definitions in plugin configuration must be simple properties - property " //$NON-NLS-1$
									+ customProp + " is not - ignoring..."); //$NON-NLS-1$
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
									+ " does not define a property corresponding to ManagedProperty '" //$NON-NLS-1$
									+ propName + "'."); //$NON-NLS-1$
				continue;
			}
			if (managedProperty == null) {
				// This should never happen, but don't let it blow us up.
				LOG.error("ManagedProperty '" + propName //$NON-NLS-1$
						+ "' has a null value in the ManagedProperties Map."); //$NON-NLS-1$
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
						.error("Unable to find a PropertyAdapter for ManagedProperty '" //$NON-NLS-1$
								+ propName
								+ "' with MetaType [" //$NON-NLS-1$
								+ metaValue.getMetaType()
								+ "] for ResourceType '" //$NON-NLS-1$
								+ resourceType.getName() + "'."); //$NON-NLS-1$
				continue; 
			}
			Property property = propertyAdapter.convertToProperty(metaValue,
					propertyDefinition);
			config.put(property);
		}
		return config;
	}

	public static void convertConfigurationToManagedProperties(Map<String, ManagedProperty> managedProperties, Configuration configuration, ResourceType resourceType, String prefix) {
		ConfigurationDefinition configDefinition = resourceType.getResourceConfigurationDefinition();
		for (ManagedProperty managedProperty : managedProperties.values()) {
			String propertyName = managedProperty.getName();
			if (prefix != null) {
				propertyName = prefix + "." + propertyName; //$NON-NLS-1$
			}
			PropertyDefinition propertyDefinition = configDefinition.get(propertyName);
			if (propertyDefinition == null) {
				// The managed property is not defined in the configuration
				continue;
			}
			populateManagedPropertyFromProperty(managedProperty,propertyDefinition, configuration);
		}
		return;
	}

	public static void populateManagedPropertyFromProperty(ManagedProperty managedProperty, PropertyDefinition propertyDefinition, Configuration configuration) {
		// If the ManagedProperty defines a default value, assume it's more
		// definitive than any default value that may
		// have been defined in the plugin descriptor, and update the
		// PropertyDefinition to use that as its default
		// value.
		MetaValue defaultValue = managedProperty.getDefaultValue();
		if (defaultValue != null) {
			updateDefaultValueOnPropertyDefinition(propertyDefinition,defaultValue);
		}
		MetaValue metaValue = managedProperty.getValue();
		PropertyAdapter propertyAdapter = null;
		if (metaValue != null) {
			LOG.trace("Populating existing MetaValue of type " //$NON-NLS-1$
					+ metaValue.getMetaType() + " from Teiid property " //$NON-NLS-1$
					+ propertyDefinition.getName() + " with definition " //$NON-NLS-1$
					+ propertyDefinition + "..."); //$NON-NLS-1$
			propertyAdapter = PropertyAdapterFactory.getPropertyAdapter(metaValue);

			propertyAdapter.populateMetaValueFromProperty(configuration.getSimple(propertyDefinition.getName()), metaValue, propertyDefinition);
			managedProperty.setValue(metaValue);
		} else {
			MetaType metaType = managedProperty.getMetaType();
			propertyAdapter = PropertyAdapterFactory.getPropertyAdapter(metaType);
			LOG.trace("Converting property " + propertyDefinition.getName() 	+ " with definition " + propertyDefinition 	+ " to MetaValue of type " + metaType + "..."); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			metaValue = propertyAdapter.convertToMetaValue(configuration.getSimple(propertyDefinition.getName()),propertyDefinition, metaType);
			managedProperty.setValue(metaValue);
		}

	}

	private static void updateDefaultValueOnPropertyDefinition(
			PropertyDefinition propertyDefinition,
			@NotNull MetaValue defaultValue) {
		if (!(propertyDefinition instanceof PropertyDefinitionSimple)) {
			LOG.debug("Cannot update default value on non-simple property definition " //$NON-NLS-1$
							+ propertyDefinition + "(default value is " //$NON-NLS-1$
							+ defaultValue + ")."); //$NON-NLS-1$
			return;
		}
		MetaType metaType = defaultValue.getMetaType();
		if (!metaType.isSimple() && !metaType.isEnum()) {
			LOG.debug("Cannot update default value on " + propertyDefinition //$NON-NLS-1$
					+ ", because default value's type (" + metaType //$NON-NLS-1$
					+ ") is not simple or enum."); //$NON-NLS-1$
			return;
		}
		PropertyDefinitionSimple propertyDefinitionSimple = (PropertyDefinitionSimple) propertyDefinition;
		if (metaType.isSimple()) {
			SimpleValue defaultSimpleValue = (SimpleValue) defaultValue;
			Serializable value = defaultSimpleValue.getValue();
			propertyDefinitionSimple.setDefaultValue((value != null) ? value.toString() : null);
		} else { // defaultValueMetaType.isEnum()
			EnumValue defaultEnumValue = (EnumValue) defaultValue;
			Serializable value = defaultEnumValue.getValue();
			propertyDefinitionSimple.setDefaultValue((value != null) ? value.toString() : null);
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
			//Need to handle RHQ 4.4 and 4.2. Return types changed for PropertyDefinitionMap.getPropertyDefinitions()
            //so we need to check types.
            Object object = ((PropertyDefinitionMap) propDef).getPropertyDefinitions();
            Iterable<PropertyDefinition> propDefIter = null;
            List<PropertyDefinition> propDefList = null;
            
            if (object instanceof Map){
            	propDefIter = (Iterable<PropertyDefinition>) ((Map)object).values().iterator();
            }else{
            	propDefList =  (List<PropertyDefinition>)object;
            }
    	
			if ((propDefIter != null &! propDefIter.iterator().hasNext()) || (propDefList != null && propDefList.isEmpty())) 
				throw new IllegalStateException(
						"PropertyDefinitionMap doesn't contain any member PropertyDefinitions."); //$NON-NLS-1$
			// NOTE: We assume member prop defs are all of the same type, since
			// for MapCompositeMetaTypes, they have to be.
			PropertyDefinition mapMemberPropDef = propDefIter != null?propDefIter.iterator().next():propDefList.listIterator().next();
			MetaType mapMemberMetaType = convertPropertyDefinitionToMetaType(mapMemberPropDef);
			memberMetaType = new MapCompositeMetaType(mapMemberMetaType);
		} else {
			throw new IllegalStateException(
					"List member PropertyDefinition has unknown type: " //$NON-NLS-1$
							+ propDef.getClass().getName());
		}
		return memberMetaType;
	}

	private static MetaType convertPropertySimpleTypeToSimpleMetaType(
			PropertySimpleType memberSimpleType) {
		MetaType memberMetaType;
		Class memberClass;
		switch (memberSimpleType) {
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

	public static SimpleValue wrap(MetaType type, String value)
			throws Exception {
		if (type instanceof SimpleMetaType) {
			SimpleMetaType st = (SimpleMetaType) type;

			if (SimpleMetaType.BIGDECIMAL.equals(st)) {
				return new SimpleValueSupport(st, new BigDecimal(value));
			} else if (SimpleMetaType.BIGINTEGER.equals(st)) {
				return new SimpleValueSupport(st, new BigInteger(value));
			} else if (SimpleMetaType.BOOLEAN.equals(st)) {
				return new SimpleValueSupport(st, Boolean.valueOf(value));
			} else if (SimpleMetaType.BOOLEAN_PRIMITIVE.equals(st)) {
				return new SimpleValueSupport(st, Boolean.valueOf(value)
						.booleanValue());
			} else if (SimpleMetaType.BYTE.equals(st)) {
				return new SimpleValueSupport(st, new Byte(value.getBytes()[0]));
			} else if (SimpleMetaType.BYTE_PRIMITIVE.equals(st)) {
				return new SimpleValueSupport(st, value.getBytes()[0]);
			} else if (SimpleMetaType.CHARACTER.equals(st)) {
				return new SimpleValueSupport(st,
						new Character(value.charAt(0)));
			} else if (SimpleMetaType.CHARACTER_PRIMITIVE.equals(st)) {
				return new SimpleValueSupport(st, value.charAt(0));
			} else if (SimpleMetaType.DATE.equals(st)) {
				try {
					return new SimpleValueSupport(st, SimpleDateFormat
							.getInstance().parse(value));
				} catch (ParseException e) {
					throw new Exception(
							"Failed to convert value to SimpleValue", e); //$NON-NLS-1$
				}
			} else if (SimpleMetaType.DOUBLE.equals(st)) {
				return new SimpleValueSupport(st, Double.valueOf(value));
			} else if (SimpleMetaType.DOUBLE_PRIMITIVE.equals(st)) {
				return new SimpleValueSupport(st, Double.parseDouble(value));
			} else if (SimpleMetaType.FLOAT.equals(st)) {
				return new SimpleValueSupport(st, Float.parseFloat(value));
			} else if (SimpleMetaType.FLOAT_PRIMITIVE.equals(st)) {
				return new SimpleValueSupport(st, Float.valueOf(value));
			} else if (SimpleMetaType.INTEGER.equals(st)) {
				return new SimpleValueSupport(st, Integer.valueOf(value));
			} else if (SimpleMetaType.INTEGER_PRIMITIVE.equals(st)) {
				return new SimpleValueSupport(st, Integer.parseInt(value));
			} else if (SimpleMetaType.LONG.equals(st)) {
				return new SimpleValueSupport(st, Long.valueOf(value));
			} else if (SimpleMetaType.LONG_PRIMITIVE.equals(st)) {
				return new SimpleValueSupport(st, Long.parseLong(value));
			} else if (SimpleMetaType.SHORT.equals(st)) {
				return new SimpleValueSupport(st, Short.valueOf(value));
			} else if (SimpleMetaType.SHORT_PRIMITIVE.equals(st)) {
				return new SimpleValueSupport(st, Short.parseShort(value));
			} else if (SimpleMetaType.STRING.equals(st)) {
				return new SimpleValueSupport(st, value);
			}
		}
		throw new Exception("Failed to convert value to SimpleValue"); //$NON-NLS-1$
	}
	
	/**
	 * @return
	 */
	public static List<PropertyDefinition> reflectivelyInvokeGetMapMethod(PropertyDefinition propertyDefinitionMap) {
		//Need to handle RHQ 4.4 and 4.2. Return types changed for PropertyDefinitionMap.getPropertyDefinitions()
		//so we need to check method availability. getMap() was added for 4.4, so if we can find it via reflection, invoke it.
		//Otherwise it is less than 4.4 so we will invoke getPropertyDefinitions which returned a map in 4.2.
		Method method = null;
		Object object = null;
		    
		try {
			method = PropertyDefinitionMap.class.getMethod("getMap", null);
		} catch (SecurityException e) {
			//Nothing to do here
		} catch (NoSuchMethodException e) {
			//This will happen if we are running a version of RHQ less that 4.4
		}
		  
		try {
			if (method == null){
				method = PropertyDefinitionMap.class.getMethod("getPropertyDefinitions", null);
			}
			object = method.invoke(propertyDefinitionMap, null);
		} catch (IllegalArgumentException e) {
			throwReflectionException(e);
		} catch (IllegalAccessException e) {
			throwReflectionException(e);
		} catch (InvocationTargetException e) {
			throwReflectionException(e);
		} catch (SecurityException e) {
			throwReflectionException(e);
		} catch (NoSuchMethodException e) {
			throwReflectionException(e);
		}
		
		
		return new ArrayList<PropertyDefinition>(((Map)object).values());
	}

	/**
	 * @param object
	 */
	private static void throwReflectionException(Exception e) {
		throw new RuntimeException("Error reflectively returning PropertyDefinition map: " + e.getLocalizedMessage());
	}

}
