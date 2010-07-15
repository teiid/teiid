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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.naming.NamingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.deployers.spi.management.ManagementView;
import org.jboss.managed.api.ComponentType;
import org.jboss.managed.api.ManagedComponent;
import org.jboss.managed.api.ManagedProperty;
import org.jboss.managed.api.RunState;
import org.mc4j.ems.connection.EmsConnection;
import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.configuration.ConfigurationUpdateStatus;
import org.rhq.core.domain.configuration.PropertySimple;
import org.rhq.core.domain.measurement.AvailabilityType;
import org.rhq.core.domain.measurement.MeasurementDataNumeric;
import org.rhq.core.domain.measurement.MeasurementReport;
import org.rhq.core.domain.measurement.MeasurementScheduleRequest;
import org.rhq.core.pluginapi.configuration.ConfigurationUpdateReport;
import org.rhq.core.pluginapi.inventory.InvalidPluginConfigurationException;
import org.rhq.core.pluginapi.inventory.ResourceComponent;
import org.rhq.core.pluginapi.inventory.ResourceContext;
import org.rhq.plugins.jbossas5.ApplicationServerComponent;
import org.rhq.plugins.jbossas5.ProfileServiceComponent;
import org.rhq.plugins.jbossas5.connection.ProfileServiceConnection;
import org.teiid.rhq.admin.DQPManagementView;
import org.teiid.rhq.plugin.util.PluginConstants;
import org.teiid.rhq.plugin.util.ProfileServiceUtil;
import org.teiid.rhq.plugin.util.PluginConstants.Operation;
import org.teiid.rhq.plugin.util.PluginConstants.ComponentType.Platform;

/**
 * 
 */
public class PlatformComponent extends Facet {
	private final Log LOG = LogFactory
			.getLog(PluginConstants.DEFAULT_LOGGER_CATEGORY);

	String[] PLATFORM_SERVICES_NAMES = { "RuntimeEngineDeployer",
			"BufferService", "SessionService", "JdbcSocketConfiguration" };

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
		this.resourceConfiguration = context.getPluginConfiguration();
		super.start(context);
	}

	/**
	 * @see org.teiid.rhq.plugin.Facet#getComponentType()
	 * @since 7.0
	 */
	@Override
	String getComponentType() {
		return PluginConstants.ComponentType.Platform.NAME;
	}

	@Override
	public AvailabilityType getAvailability() {

		RunState runState;
		try {
			runState = ProfileServiceUtil.getRuntimeEngineDeployer(getConnection())
					.getRunState();
		} catch (NamingException e) {
			LOG
					.error("Naming exception getting: "
							+ PluginConstants.ComponentType.Platform.TEIID_RUNTIME_ENGINE);
			return AvailabilityType.DOWN;
		} catch (Exception e) {
			LOG
					.error("Exception getting: "
							+ PluginConstants.ComponentType.Platform.TEIID_RUNTIME_ENGINE);
			return AvailabilityType.DOWN;
		}
		return (runState == RunState.RUNNING) ? AvailabilityType.UP
				: AvailabilityType.DOWN;

	}

	@Override
	protected void setOperationArguments(String name,
			Configuration configuration, Map<String, Object> valueMap) {
		// Parameter logic for System Operations
		if (name.equals(Platform.Operations.KILL_REQUEST)) {
			valueMap.put(Operation.Value.REQUEST_ID, configuration.getSimple(
					Operation.Value.REQUEST_ID).getLongValue());
			valueMap.put(Operation.Value.SESSION_ID, configuration.getSimple(
					Operation.Value.SESSION_ID).getLongValue());
		} else if (name.equals(Platform.Operations.KILL_REQUEST)) {
			valueMap.put(Operation.Value.TRANSACTION_ID, configuration
					.getSimple(Operation.Value.TRANSACTION_ID).getLongValue());
		} else if (name.equals(Platform.Operations.KILL_SESSION)) {
			valueMap.put(Operation.Value.SESSION_ID, configuration.getSimple(
					Operation.Value.SESSION_ID).getLongValue());
		}

	}

	@Override
	public void getValues(MeasurementReport report,
			Set<MeasurementScheduleRequest> requests) throws Exception {

		DQPManagementView view = new DQPManagementView();

		Map<String, Object> valueMap = new HashMap<String, Object>();

		try {
			for (MeasurementScheduleRequest request : requests) {
				String name = request.getName();
				LOG.debug("Measurement name = " + name); //$NON-NLS-1$

				// Initialize any parameters to be used in the retrieval of
				// metric values

				Object metricReturnObject = view.getMetric(getConnection(),
						getComponentType(), this.getComponentIdentifier(),
						name, valueMap);

				try {
					if (request
							.getName()
							.equals(
									PluginConstants.ComponentType.Platform.Metrics.QUERY_COUNT)) {
						report.addData(new MeasurementDataNumeric(request,
								(Double) metricReturnObject));
					} else {
						if (request
								.getName()
								.equals(
										PluginConstants.ComponentType.Platform.Metrics.SESSION_COUNT)) {
							report.addData(new MeasurementDataNumeric(request,
									(Double) metricReturnObject));
						} else {
							if (request
									.getName()
									.equals(
											PluginConstants.ComponentType.Platform.Metrics.LONG_RUNNING_QUERIES)) {
								report.addData(new MeasurementDataNumeric(
										request, (Double) metricReturnObject));
							} else {
								if (request
										.getName()
										.equals(
												PluginConstants.ComponentType.Platform.Metrics.BUFFER_USAGE)) {
									report.addData(new MeasurementDataNumeric(
											request,
											(Double) metricReturnObject));
								}
							}
						}
					}

				} catch (Exception e) {
					LOG.error("Failed to obtain measurement [" + name //$NON-NLS-1$
							+ "]. Cause: " + e); //$NON-NLS-1$
					throw (e);
				}
			}
		} catch (Exception e) {
			LOG.error("Failed to obtain measurement [" + name //$NON-NLS-1$
					+ "]. Cause: " + e); //$NON-NLS-1$
			throw (e);
		}

	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
		super.stop();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.teiid.rhq.plugin.Facet#updateResourceConfiguration(org.rhq.core.pluginapi
	 * .configuration.ConfigurationUpdateReport)
	 */
	@Override
	public void updateResourceConfiguration(ConfigurationUpdateReport report) {

		resourceConfiguration = report.getConfiguration().deepCopy();

		Configuration resourceConfig = report.getConfiguration();

		ManagementView managementView = null;
		ComponentType componentType = new ComponentType(
				PluginConstants.ComponentType.Platform.TEIID_TYPE,
				PluginConstants.ComponentType.Platform.TEIID_SUB_TYPE);

		ManagedComponent managedComponent = null;
		report.setStatus(ConfigurationUpdateStatus.SUCCESS);
		try {

			managementView = getConnection().getManagementView();

			for (String serviceName : PLATFORM_SERVICES_NAMES) {

				managedComponent = managementView.getComponent(serviceName,
						componentType);
				Map<String, ManagedProperty> managedProperties = managedComponent
						.getProperties();

				ProfileServiceUtil.convertConfigurationToManagedProperties(
						managedProperties, resourceConfig, resourceContext
								.getResourceType());

				try {
					managementView.updateComponent(managedComponent);
				} catch (Exception e) {
					LOG.error("Unable to update component ["
							+ managedComponent.getName() + "] of type "
							+ componentType + ".", e);
					report.setStatus(ConfigurationUpdateStatus.FAILURE);
					report.setErrorMessageFromThrowable(e);
				}
			}
		} catch (Exception e) {
			LOG.error("Unable to process update request", e);
			report.setStatus(ConfigurationUpdateStatus.FAILURE);
			report.setErrorMessageFromThrowable(e);
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.teiid.rhq.plugin.Facet#loadResourceConfiguration()
	 */
	@Override
	public Configuration loadResourceConfiguration() {

		// Get plugin config
		Configuration c = resourceContext.getPluginConfiguration();

		getProperties(c);

		return c;

	}

	/**
	 * @param mc
	 * @param configuration
	 * @throws Exception
	 */
	private void getProperties(Configuration configuration) {

		// Get all ManagedComponents of type Teiid and subtype dqp
		Set<ManagedComponent> mcSet = null;
		try {
			mcSet = ProfileServiceUtil
					.getManagedComponents(
							getConnection(),
							new org.jboss.managed.api.ComponentType(
									PluginConstants.ComponentType.Platform.TEIID_TYPE,
									PluginConstants.ComponentType.Platform.TEIID_SUB_TYPE));
		} catch (NamingException e) {
			LOG
					.error("NamingException getting components in Platform loadConfiguration(): "
							+ e.getMessage());
		} catch (Exception e) {
			LOG
					.error("Exception getting components in Platform loadConfiguration(): "
							+ e.getMessage());
		}

		for (ManagedComponent mc : mcSet) {
			Map<String, ManagedProperty> mcMap = mc.getProperties();
			setProperties(mcMap, configuration);
		}
	}

	/**
	 * @param mcMap
	 * @param configuration
	 */
	private void setProperties(Map<String, ManagedProperty> mcMap,
			Configuration configuration) {
		for (ManagedProperty mProp : mcMap.values()) {
			try {
				String value = ProfileServiceUtil.stringValue(mProp.getValue());
				PropertySimple prop = new PropertySimple(mProp.getName(), value);
				configuration.put(prop);
			} catch (Exception e) {
				LOG
						.error("Exception setting properties in Platform loadConfiguration(): "
								+ e.getMessage());
			}
		}
	}

	@Override
	public ProfileServiceConnection getConnection() {
		return ((ApplicationServerComponent) this.resourceContext
				.getParentResourceComponent()).getConnection();
	}

	@Override
	public EmsConnection getEmsConnection() {
		// TODO Auto-generated method stub
		return null;
	}

}