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
import java.util.Map;
import java.util.Set;

import javax.naming.NamingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.managed.api.ManagedComponent;
import org.jboss.managed.api.ManagedObject;
import org.jboss.managed.api.ManagedProperty;
import org.jboss.managed.plugins.ManagedObjectImpl;
import org.jboss.metatype.api.types.MetaType;
import org.jboss.metatype.api.values.CollectionValueSupport;
import org.jboss.metatype.api.values.EnumValueSupport;
import org.jboss.metatype.api.values.GenericValueSupport;
import org.jboss.metatype.api.values.MetaValue;
import org.jboss.metatype.api.values.SimpleValue;
import org.jboss.metatype.api.values.SimpleValueSupport;
import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.configuration.PropertyList;
import org.rhq.core.domain.configuration.PropertyMap;
import org.rhq.core.domain.configuration.PropertySimple;
import org.rhq.core.domain.measurement.AvailabilityType;
import org.rhq.core.domain.measurement.MeasurementDataNumeric;
import org.rhq.core.domain.measurement.MeasurementDataTrait;
import org.rhq.core.domain.measurement.MeasurementReport;
import org.rhq.core.domain.measurement.MeasurementScheduleRequest;
import org.rhq.core.pluginapi.inventory.ResourceContext;
import org.teiid.rhq.admin.DQPManagementView;
import org.teiid.rhq.comm.ConnectionConstants;
import org.teiid.rhq.plugin.util.PluginConstants;
import org.teiid.rhq.plugin.util.ProfileServiceUtil;
import org.teiid.rhq.plugin.util.PluginConstants.Operation;
import org.teiid.rhq.plugin.util.PluginConstants.ComponentType.Platform;
import org.teiid.rhq.plugin.util.PluginConstants.ComponentType.VDB;

/**
 * Component class for a Teiid VDB
 * 
 */
public class VDBComponent extends Facet {
	private final Log LOG = LogFactory.getLog(VDBComponent.class);

	/*
	 * (non-Javadoc)
	 * 
	 * @seeorg.teiid.rhq.plugin.Facet#start(org.rhq.core.pluginapi.inventory.
	 * ResourceContext)
	 */
	@Override
	public void start(ResourceContext context) {
		this.setComponentName(context.getPluginConfiguration().getSimpleValue(
				"name", null));
		this.resourceConfiguration=context.getPluginConfiguration();
		super.start(context);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.teiid.rhq.plugin.Facet#getComponentName()
	 */
	@Override
	public String getComponentName() {
		return this.name;
	}

	@Override
	protected void setOperationArguments(String name,
			Configuration configuration, Map<String, Object> valueMap) {
		// Parameter logic for VDB Operations
		if (name.equals(VDB.Operations.KILL_REQUEST)) {
			valueMap.put(Operation.Value.REQUEST_ID, configuration.getSimple(
					Operation.Value.REQUEST_ID).getLongValue());
			valueMap.put(Operation.Value.SESSION_ID, configuration.getSimple(
					Operation.Value.SESSION_ID).getLongValue());
		} else if (name.equals(Platform.Operations.KILL_SESSION)) {
			valueMap.put(Operation.Value.SESSION_ID, configuration.getSimple(
					Operation.Value.SESSION_ID).getLongValue());
		} else if (name.equals(Platform.Operations.GET_PROPERTIES)) {
			String key = ConnectionConstants.IDENTIFIER;
			valueMap.put(key, getComponentIdentifier());
		} else if (name.equals(Platform.Operations.KILL_SESSION)) {
			valueMap.put(Operation.Value.SESSION_ID, configuration.getSimple(
					Operation.Value.SESSION_ID).getLongValue());
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
		String status = DQPManagementView.getVDBStatus(this.getComponentName(),
				1);
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
		valueMap.put(key, this.resourceConfiguration.getSimpleValue("name",
				null));
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

			Object metricReturnObject = view.getMetric(getComponentType(), this
					.getComponentIdentifier(), name, valueMap);

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
								report.addData(new MeasurementDataTrait(
										request, (String) metricReturnObject));
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

	@Override
	public Configuration loadResourceConfiguration() {

		ManagedComponent mcVdb = null;
		try {
			mcVdb = ProfileServiceUtil.getManagedComponent(
					new org.jboss.managed.api.ComponentType(
							PluginConstants.ComponentType.VDB.TYPE,
							PluginConstants.ComponentType.VDB.SUBTYPE), this
							.getComponentName());
		} catch (NamingException e) {
			final String msg = "NamingException in getVDBStatus(): " + e.getExplanation(); //$NON-NLS-1$
			LOG.error(msg, e);
		} catch (Exception e) {
			final String msg = "Exception in getVDBStatus(): " + e.getMessage(); //$NON-NLS-1$
			LOG.error(msg, e);
		}

		// Get plugin config map for models
		Configuration configuration = resourceContext.getPluginConfiguration();

		// configuration.put(new PropertySimple("name", vdbName));
		// configuration.put(new PropertySimple("version", vdbVersion));
		// configuration
		// .put(new PropertySimple("description", vdbDescription));
		// configuration.put(new PropertySimple("status", vdbStatus));
		// configuration.put(new PropertySimple("url", vdbURL));

		getModels(mcVdb, configuration);

		return configuration;

	}

	/**
	 * @param mcVdb
	 * @param configuration
	 * @throws Exception
	 */
	private void getModels(ManagedComponent mcVdb, Configuration configuration)
			 {
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
					PropertyMap multiSourceModel = null;
					PropertyMap multiSourceModel2 = null;

				PropertyMap model = null;
				if (supportMultiSource){
					//TODO need to loop through multisource models
						multiSourceModel = new PropertyMap("model",
									new PropertySimple("name", modelName),
									new PropertySimple("sourceName", sourceName),
									new PropertySimple("jndiName", jndiName));
						multiSourceModelsList.add(multiSourceModel);
					 model = new PropertyMap("model",
							new PropertySimple("name", modelName),
							new PropertySimple("sourceName", "See below"),
							new PropertySimple("jndiName", "See below"),
							new PropertySimple("visibility", visibility),
							new PropertySimple("supportsMultiSource",
									true));
					 	sourceModelsList.add(model);
					 	multiSourceModel = new PropertyMap("model",
								new PropertySimple("name", modelName),
								new PropertySimple("sourceName", sourceName),
								new PropertySimple("jndiName", jndiName));
					multiSourceModelsList.add(multiSourceModel);
					}else{
						 model = new PropertyMap("model",
								new PropertySimple("name", modelName),
								new PropertySimple("sourceName", sourceName),
								new PropertySimple("jndiName", jndiName),
								new PropertySimple("visibility", visibility),
								new PropertySimple("supportsMultiSource",
										supportMultiSource));
						 sourceModelsList.add(model);
					}
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
				String jndi = ((SimpleValue) mo.getProperty("jndiName")
						.getValue()).getValue().toString();
				map.put("name", sourceName);
				map.put("jndiName", jndi);
			}
		} else {
			throw new IllegalStateException(pValue
					+ " is not a Collection type");
		}
	}

}
