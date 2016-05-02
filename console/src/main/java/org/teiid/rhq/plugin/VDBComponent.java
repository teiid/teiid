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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.naming.NamingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.deployers.spi.management.ManagementView;
import org.jboss.managed.api.ComponentType;
import org.jboss.managed.api.ManagedComponent;
import org.jboss.managed.api.ManagedObject;
import org.jboss.managed.api.ManagedProperty;
import org.jboss.managed.plugins.ManagedObjectImpl;
import org.jboss.metatype.api.types.MetaType;
import org.jboss.metatype.api.types.SimpleMetaType;
import org.jboss.metatype.api.values.CollectionValueSupport;
import org.jboss.metatype.api.values.CompositeValueSupport;
import org.jboss.metatype.api.values.EnumValueSupport;
import org.jboss.metatype.api.values.GenericValue;
import org.jboss.metatype.api.values.GenericValueSupport;
import org.jboss.metatype.api.values.MetaValue;
import org.jboss.metatype.api.values.SimpleValue;
import org.jboss.metatype.api.values.SimpleValueSupport;
import org.mc4j.ems.connection.EmsConnection;
import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.configuration.ConfigurationUpdateStatus;
import org.rhq.core.domain.configuration.Property;
import org.rhq.core.domain.configuration.PropertyList;
import org.rhq.core.domain.configuration.PropertyMap;
import org.rhq.core.domain.configuration.PropertySimple;
import org.rhq.core.domain.measurement.AvailabilityType;
import org.rhq.core.domain.measurement.MeasurementDataNumeric;
import org.rhq.core.domain.measurement.MeasurementDataTrait;
import org.rhq.core.domain.measurement.MeasurementReport;
import org.rhq.core.domain.measurement.MeasurementScheduleRequest;
import org.rhq.core.pluginapi.configuration.ConfigurationFacet;
import org.rhq.core.pluginapi.configuration.ConfigurationUpdateReport;
import org.rhq.core.pluginapi.inventory.CreateResourceReport;
import org.rhq.core.pluginapi.inventory.ResourceContext;
import org.rhq.plugins.jbossas5.connection.ProfileServiceConnection;
import org.teiid.rhq.admin.DQPManagementView;
import org.teiid.rhq.plugin.util.PluginConstants;
import org.teiid.rhq.plugin.util.PluginConstants.ComponentType.Platform;
import org.teiid.rhq.plugin.util.PluginConstants.ComponentType.VDB;
import org.teiid.rhq.plugin.util.PluginConstants.Operation;
import org.teiid.rhq.plugin.util.ProfileServiceUtil;

/**
 * Component class for a Teiid VDB
 * 
 */
public class VDBComponent extends Facet {
	private final Log LOG = LogFactory
			.getLog(PluginConstants.DEFAULT_LOGGER_CATEGORY);

	/*
	 * (non-Javadoc)
	 * 
	 * @seeorg.teiid.rhq.plugin.Facet#start(org.rhq.core.pluginapi.inventory.
	 * ResourceContext)
	 */
	public void start(ResourceContext context) {
		this.setComponentName(context.getPluginConfiguration().getSimpleValue(
				"fullName", null));
		this.resourceConfiguration = context.getPluginConfiguration();
		this.componentType = PluginConstants.ComponentType.VDB.NAME;
		super.start(context);
	}

	@Override
	protected void setOperationArguments(String name,
			Configuration configuration, Map<String, Object> valueMap) {
		// Parameter logic for VDB Metrics
		String key = VDB.NAME;
		valueMap.put(key, this.resourceConfiguration.getSimpleValue("name",
				null));
		String version = VDB.VERSION;
		valueMap.put(version, this.resourceConfiguration.getSimpleValue(
				VDB.VERSION, null));

		// Parameter logic for VDB Operations
		if (name.equals(VDB.Operations.KILL_REQUEST)) {
			valueMap.put(Operation.Value.REQUEST_ID, configuration.getSimple(
					Operation.Value.REQUEST_ID).getLongValue());
			valueMap.put(Operation.Value.SESSION_ID, configuration.getSimple(
					Operation.Value.SESSION_ID).getLongValue());
		} else if (name.equals(VDB.Operations.CLEAR_CACHE)) {
				valueMap.put(Operation.Value.CACHE_TYPE, configuration.getSimple(
					Operation.Value.CACHE_TYPE).getStringValue());
		} else if (name.equals(Platform.Operations.KILL_SESSION)) {
			valueMap.put(Operation.Value.SESSION_ID, configuration.getSimple(
					Operation.Value.SESSION_ID).getLongValue());
		} else if (name.equals(Platform.Operations.KILL_SESSION)) {
			valueMap.put(Operation.Value.SESSION_ID, configuration.getSimple(
					Operation.Value.SESSION_ID).getLongValue());
		} else if (name.equals(VDB.Operations.RELOAD_MATVIEW)) {
			valueMap
					.put(Operation.Value.MATVIEW_SCHEMA, configuration
							.getSimple(Operation.Value.MATVIEW_SCHEMA)
							.getStringValue());
			valueMap.put(Operation.Value.MATVIEW_TABLE, configuration
					.getSimple(Operation.Value.MATVIEW_TABLE).getStringValue());
			valueMap.put(Operation.Value.INVALIDATE_MATVIEW, configuration
					.getSimple(Operation.Value.INVALIDATE_MATVIEW)
					.getBooleanValue());
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.teiid.rhq.plugin.Facet#getAvailability()
	 */
	@Override
	public AvailabilityType getAvailability() {
		// TODO Remove vdb version after no longer viable in Teiid
		String version = this.resourceConfiguration.getSimpleValue("version",
				null);
		String status = DQPManagementView.getVDBStatus(getConnection(),
				this.name);
		if (status.equals("ACTIVE")) {
			return AvailabilityType.UP;
		}

		return AvailabilityType.DOWN;
	}

	@Override
	protected void setMetricArguments(String name, Configuration configuration,
			Map<String, Object> valueMap) {
		// Parameter logic for VDB Metrics
		String key = VDB.NAME;
		valueMap.put(key, this.name);
	}

	@Override
	public void getValues(MeasurementReport report,
			Set<MeasurementScheduleRequest> requests) throws Exception {

		DQPManagementView view = new DQPManagementView();

		Map<String, Object> valueMap = new HashMap<String, Object>();
		setMetricArguments(VDB.NAME, null, valueMap);

		for (MeasurementScheduleRequest request : requests) {
			String name = request.getName();
			LOG.debug("Measurement name = " + name); //$NON-NLS-1$

			Object metricReturnObject = view.getMetric(getConnection(),
					getComponentType(), this.getComponentIdentifier(), name,
					valueMap);

			try {
				if (request.getName().equals(
						PluginConstants.ComponentType.VDB.Metrics.ERROR_COUNT)) {
					String message = "";
					if (((Integer) metricReturnObject) > 0) {
						message = "** There are "
								+ ((Integer) metricReturnObject)
								+ " errors reported for this VDB. See the Configuration tab for details. **";
					} else {
						message = "** There are no errors reported for this VDB. **";
					}

					report.addData(new MeasurementDataTrait(request, message));
				} else {
					if (request
							.getName()
							.equals(
									PluginConstants.ComponentType.VDB.Metrics.QUERY_COUNT)) {
						report.addData(new MeasurementDataTrait(request,
								(String) metricReturnObject));
					} else {
						if (request
								.getName()
								.equals(
										PluginConstants.ComponentType.VDB.Metrics.SESSION_COUNT)) {
							report.addData(new MeasurementDataNumeric(request,
									(Double) metricReturnObject));
						} else {
							if (request
									.getName()
									.equals(
											PluginConstants.ComponentType.VDB.Metrics.STATUS)) {
								if (((String) metricReturnObject)
										.equals("ACTIVE")) {
									report.addData(new MeasurementDataTrait(
											request, "UP"));
								} else {
									report.addData(new MeasurementDataTrait(
											request, "DOWN"));
								}
							} else {
								if (request
										.getName()
										.equals(
												PluginConstants.ComponentType.VDB.Metrics.LONG_RUNNING_QUERIES)) {
									report.addData(new MeasurementDataNumeric(
											request,
											(Double) metricReturnObject));
								}
							}

						}
					}
				}

			} catch (Exception e) {
				LOG.error("Failed to obtain measurement [" + name //$NON-NLS-1$
						+ "]. Cause: " + e); //$NON-NLS-1$
				// throw(e);
			}
		}

	}

	@Override
	String getComponentType() {
		return PluginConstants.ComponentType.VDB.NAME;
	}

	/**
	 * The plugin container will call this method when it has a new
	 * configuration for your managed resource. Your plugin will re-configure
	 * the managed resource in your own custom way, setting its configuration
	 * based on the new values of the given configuration.
	 * 
	 * @see ConfigurationFacet#updateResourceConfiguration(ConfigurationUpdateReport)
	 */
	public void updateResourceConfiguration(ConfigurationUpdateReport report) {

		Configuration resourceConfig = report.getConfiguration();
		resourceConfiguration = resourceConfig.deepCopy();

		// First update simple properties
		super.updateResourceConfiguration(report);

		// Then update models
		ManagementView managementView = null;
		ComponentType componentType = new ComponentType(
				PluginConstants.ComponentType.VDB.TYPE,
				PluginConstants.ComponentType.VDB.SUBTYPE);

		ManagedComponent managedComponent = null;
		CollectionValueSupport modelsMetaValue = null;
		report.setStatus(ConfigurationUpdateStatus.SUCCESS);
		try {

			managementView = getConnection().getManagementView();
			managedComponent = managementView.getComponent(this.name,
					componentType);
			modelsMetaValue = (CollectionValueSupport) managedComponent
					.getProperty("models").getValue();
			GenericValue[] models = (GenericValue[]) modelsMetaValue
					.getElements();
			List<Property> multiSourceModelsPropertyList = resourceConfiguration
					.getList("multiSourceModels").getList();
			List<Property> singleSourceModelsPropertyList = resourceConfiguration
					.getList("singleSourceModels").getList();
			ArrayList<List<Property>> sourceMappingList = new ArrayList<List<Property>>();
			sourceMappingList.add(singleSourceModelsPropertyList);
			sourceMappingList.add(multiSourceModelsPropertyList);
			PropertyMap model = null;
			Iterator<List<Property>> sourceMappingListIterator = sourceMappingList
					.iterator();
			while (sourceMappingListIterator.hasNext()) {
				List<Property> sourceList = sourceMappingListIterator.next();
				for (int i = 0; i < sourceList.size(); i++) {
					model = (PropertyMap) sourceList.get(i);
					String sourceName = ((PropertySimple) model
							.get("sourceName")).getStringValue(); //$NON-NLS-1$
					if (sourceName.equals("See below"))
						continue; // This is a multisource model which we will
									// handle separately
					String modelName = ((PropertySimple) model.get("name")) //$NON-NLS-1$
							.getStringValue();
					String dsName = ((PropertySimple) model.get("jndiName")) //$NON-NLS-1$
							.getStringValue();

					ManagedObject managedModel = null;
					if (models != null && models.length != 0) {
						for (GenericValue genValue : models) {
							ManagedObject mo = (ManagedObject) ((GenericValueSupport) genValue)
									.getValue();
							String name = ProfileServiceUtil.getSimpleValue(mo,
									"name", String.class); //$NON-NLS-1$
							if (modelName.equals(name)) {
								managedModel = mo;
								break;
							}
						}
					}

					ManagedProperty sourceMappings = null;
					if (managedModel != null) {

						sourceMappings = managedModel
								.getProperty("sourceMappings");//$NON-NLS-1$

						if (sourceMappings != null) {
							CollectionValueSupport mappings = (CollectionValueSupport) sourceMappings
									.getValue();
							GenericValue[] mappingsArray = (GenericValue[]) mappings
									.getElements();
							for (GenericValue sourceGenValue : mappingsArray) {
								ManagedObject sourceMo = (ManagedObject) ((GenericValueSupport) sourceGenValue)
										.getValue();
								String sName = ProfileServiceUtil
										.getSimpleValue(sourceMo,
												"name", String.class);//$NON-NLS-1$
								if (sName.equals(sourceName)) {
									// set the jndi name for the ds.
									ManagedProperty jndiProperty = sourceMo
											.getProperty("connectionJndiName"); //$NON-NLS-1$
									jndiProperty
											.setValue(ProfileServiceUtil.wrap(
													SimpleMetaType.STRING,
													dsName));
									break;
								}
							}
						}
					}
				}
			}

			try {
				managementView.updateComponent(managedComponent);
				managementView.load();
			} catch (Exception e) {
				LOG.error("Unable to update component ["
						+ managedComponent.getName() + "] of type "
						+ componentType + ".", e);
				report.setStatus(ConfigurationUpdateStatus.FAILURE);
				report.setErrorMessageFromThrowable(e);
			}
		} catch (Exception e) {
			LOG.error("Unable to process update request", e);
			report.setStatus(ConfigurationUpdateStatus.FAILURE);
			report.setErrorMessageFromThrowable(e);
		}

	}

	@Override
	public Configuration loadResourceConfiguration() {

		ManagedComponent mcVdb = null;
		try {
			mcVdb = ProfileServiceUtil.getManagedComponent(getConnection(),
					new org.jboss.managed.api.ComponentType(
							PluginConstants.ComponentType.VDB.TYPE,
							PluginConstants.ComponentType.VDB.SUBTYPE),
					this.name);
		} catch (NamingException e) {
			final String msg = "NamingException in loadResourceConfiguration(): " + e.getExplanation(); //$NON-NLS-1$
			LOG.error(msg, e);
		} catch (Exception e) {
			final String msg = "Exception in loadResourceConfiguration(): " + e.getMessage(); //$NON-NLS-1$
			LOG.error(msg, e);
		}

		String vdbName = ProfileServiceUtil.getSimpleValue(mcVdb, "name",
				String.class);
		Integer vdbVersion = ProfileServiceUtil.getSimpleValue(mcVdb,
				"version", Integer.class);
		String vdbDescription = ProfileServiceUtil.getSimpleValue(mcVdb,
				"description", String.class);
		String vdbStatus = ProfileServiceUtil.getSimpleValue(mcVdb, "status",
				String.class);
		String connectionType = ProfileServiceUtil.getSimpleValue(mcVdb,
				"connectionType", String.class);
		String vdbURL = ProfileServiceUtil.getSimpleValue(mcVdb, "url",
				String.class);

		// Get plugin config map for models
		Configuration configuration = resourceContext.getPluginConfiguration();

		configuration.put(new PropertySimple("name", vdbName));
		configuration.put(new PropertySimple("version", vdbVersion));
		configuration.put(new PropertySimple("description", vdbDescription));
		configuration.put(new PropertySimple("status", vdbStatus));
		configuration.put(new PropertySimple("url", vdbURL));
		configuration.put(new PropertySimple("connectionType", connectionType));

		try {
			getTranslators(mcVdb, configuration);
		} catch (Exception e) {
			final String msg = "Exception in loadResourceConfiguration(): " + e.getMessage(); //$NON-NLS-1$
			LOG.error(msg, e);
		}

		getModels(mcVdb, configuration);

		return configuration;

	}

	@Override
	public CreateResourceReport createResource(
			CreateResourceReport createResourceReport) {

		createContentBasedResource(createResourceReport);
		return createResourceReport;
	}

	/**
	 * @param mcVdb
	 * @param configuration
	 * @throws Exception
	 */
	private void getModels(ManagedComponent mcVdb, Configuration configuration) {
		// Get models from VDB
		ManagedProperty property = mcVdb.getProperty("models");
		CollectionValueSupport valueSupport = (CollectionValueSupport) property
				.getValue();
		MetaValue[] metaValues = valueSupport.getElements();

		PropertyList sourceModelsList = new PropertyList("singleSourceModels");
		configuration.put(sourceModelsList);

		PropertyList multiSourceModelsList = new PropertyList(
				"multiSourceModels");
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
				LOG.error(e.getMessage());
			}

			Boolean supportMultiSource = Boolean.TRUE;
			try {
				supportMultiSource = ProfileServiceUtil
						.booleanValue(managedObject.getProperty(
								"supportsMultiSourceBindings").getValue());
			} catch (Exception e) {
				LOG.error(e.getMessage());
			}

			String modelName = managedObject.getName();
			ManagedProperty connectorBinding = managedObject
					.getProperty("sourceMappings");
			Collection<Map<String, String>> sourceList = new ArrayList<Map<String, String>>();

			getSourceMappingValue(connectorBinding.getValue(), sourceList);

			String visibility = ((SimpleValueSupport) managedObject
					.getProperty("visible").getValue()).getValue().toString();
			String type = ((EnumValueSupport) managedObject.getProperty(
					"modelType").getValue()).getValue().toString();

			// Get any model errors/warnings
			MetaValue errors = managedObject.getProperty("errors").getValue();
			if (errors != null) {
				CollectionValueSupport errorValueSupport = (CollectionValueSupport) errors;
				MetaValue[] errorArray = errorValueSupport.getElements();

				for (MetaValue error : errorArray) {
					GenericValueSupport errorGenValueSupport = (GenericValueSupport) error;

					ManagedObject errorMo = (ManagedObject) errorGenValueSupport
							.getValue();
					String severity = ((SimpleValue) errorMo.getProperty(
							"severity").getValue()).getValue().toString();
					String message = ((SimpleValue) errorMo
							.getProperty("value").getValue()).getValue()
							.toString();

					PropertyMap errorMap = new PropertyMap("errorMap",
							new PropertySimple("severity", severity),
							new PropertySimple("message", message));
					errorList.add(errorMap);
				}
			}

			for (Map<String, String> sourceMap : sourceList) {

				if (isSource) {
					String sourceName = (String) sourceMap.get("name");
					String jndiName = (String) sourceMap.get("jndiName");
					String translatorName = (String) sourceMap
							.get("translatorName");
					PropertyMap multiSourceModel = null;

					PropertyMap model = null;
					if (supportMultiSource) {
						// TODO need to loop through multisource models
						multiSourceModel = new PropertyMap("map",
								new PropertySimple("name", modelName),
								new PropertySimple("sourceName", sourceName),
								new PropertySimple("jndiName", jndiName),
								new PropertySimple("translatorName",
										translatorName));

						multiSourceModelsList.add(multiSourceModel);

						model = new PropertyMap("map", new PropertySimple(
								"name", modelName), new PropertySimple(
								"sourceName", "See below"), new PropertySimple(
								"jndiName", "See below"), new PropertySimple(
								"translatorName", "See below"),
								new PropertySimple("visibility", visibility),
								new PropertySimple("supportsMultiSource", true));
						sourceModelsList.add(model);
					} else {
						model = new PropertyMap("map", new PropertySimple(
								"name", modelName), new PropertySimple(
								"sourceName", sourceName), new PropertySimple(
								"jndiName", jndiName), new PropertySimple(
								"translatorName", translatorName),
								new PropertySimple("visibility", visibility),
								new PropertySimple("supportsMultiSource",
										supportMultiSource));
						sourceModelsList.add(model);
					}
				} else {
					PropertyMap model = new PropertyMap("map",
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
				String jndi = ((SimpleValue) mo.getProperty(
						"connectionJndiName").getValue()).getValue().toString();
				String translatorName = ((SimpleValue) mo.getProperty(
						"translatorName").getValue()).getValue().toString();
				map.put("name", sourceName);
				map.put("jndiName", jndi);
				map.put("translatorName", translatorName);
			}
		} else {
			throw new IllegalStateException(pValue
					+ " is not a Collection type");
		}
	}

	/**
	 * @param mcVdb
	 * @param configuration
	 * @throws Exception 
	 */
	private void getTranslators(ManagedComponent mcVdb,
			Configuration configuration) throws Exception {
		// Get models from VDB
		ManagedProperty property = mcVdb.getProperty("overrideTranslators");
		if (property == null) {
			return;
		}
		CollectionValueSupport valueSupport = (CollectionValueSupport) property
				.getValue();
		MetaValue[] metaValues = valueSupport.getElements();

		PropertyList translatorsList = new PropertyList("translators");
		configuration.put(translatorsList);

		for (MetaValue value : metaValues) {
			GenericValueSupport genValueSupport = (GenericValueSupport) value;
			ManagedObjectImpl managedObject = (ManagedObjectImpl) genValueSupport
					.getValue();

			String translatorName = ProfileServiceUtil.getSimpleValue(
					managedObject, "name", String.class);
			String translatorType = ProfileServiceUtil.getSimpleValue(
					managedObject, "type", String.class);
			ManagedProperty properties = managedObject.getProperty("property");

			if (properties != null) {
				CollectionValueSupport props = (CollectionValueSupport) properties
						.getValue();
				for (MetaValue propertyMetaData : props) {
					String propertyName = ProfileServiceUtil
							.stringValue(((CompositeValueSupport) propertyMetaData)
									.get("name"));
					String propertyValue = ProfileServiceUtil
							.stringValue(((CompositeValueSupport) propertyMetaData)
									.get("value"));
					PropertyMap translatorMap = null;

					translatorMap = new PropertyMap("translatorMap",
							new PropertySimple("name", translatorName),
							new PropertySimple("type", translatorType),
							new PropertySimple("propertyName", propertyName),
							new PropertySimple("propertyValue", propertyValue));
					// Only want translator name and value to show up for the
					// first row,
					// so we will blank them out here.
					translatorName = "";
					translatorType = "";
					translatorsList.add(translatorMap);
				}
			}
		}
	}

	/**
	 * @param <T>
	 * @param pValue
	 * @param list
	 */
	public static <T> void getPropertyValues(MetaValue pValue,
			Collection<Map<String, String>> list) {
		Map<String, String> map = new HashMap<String, String>();
		list.add(map);
		MetaType metaType = pValue.getMetaType();
		if (metaType.isCollection()) {
			for (MetaValue value : ((CollectionValueSupport) pValue)
					.getElements()) {
				CompositeValueSupport compValue = ((CompositeValueSupport) value);
				for (MetaValue propValue : compValue.values()) {
					String propertyName = ((CompositeValueSupport) propValue)
							.get("name").toString();
					String propertyValue = ((CompositeValueSupport) propValue)
							.get("value").toString();
					map.put("name", propertyName);
					map.put("value", propertyValue);
				}
			}
		} else {
			throw new IllegalStateException(pValue
					+ " is not a Collection type");
		}
	}

	@Override
	public ProfileServiceConnection getConnection() {
		return ((PlatformComponent) this.resourceContext
				.getParentResourceComponent()).getConnection();
	}

	@Override
	public EmsConnection getEmsConnection() {
		return null;
	}

	@Override
	public ResourceContext getResourceContext() {
		return this.getResourceContext();
	}

}
