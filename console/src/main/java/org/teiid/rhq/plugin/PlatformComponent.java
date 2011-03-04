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
import org.rhq.core.pluginapi.inventory.ResourceContext;
import org.rhq.plugins.jbossas5.ApplicationServerComponent;
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
	private final Log LOG = LogFactory.getLog(PluginConstants.DEFAULT_LOGGER_CATEGORY);

	@Override
	public void start(ResourceContext context) {
		this.setComponentName(context.getPluginConfiguration().getSimpleValue(	"name", null)); //$NON-NLS-1$
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
			runState = ProfileServiceUtil.getRuntimeEngineDeployer(getConnection()).getRunState();
		} catch (NamingException e) {
			LOG	.debug("Naming exception getting: " + PluginConstants.ComponentType.Platform.TEIID_RUNTIME_ENGINE); //$NON-NLS-1$
			return AvailabilityType.DOWN;
		} catch (Exception e) {
			LOG	.debug("Exception getting: " 	+ PluginConstants.ComponentType.Platform.TEIID_RUNTIME_ENGINE); //$NON-NLS-1$
			return AvailabilityType.DOWN;
		}
		return (runState == RunState.RUNNING) ? AvailabilityType.UP: AvailabilityType.DOWN;
	}

	@Override
	protected void setOperationArguments(String name,
			Configuration configuration, Map<String, Object> valueMap) {
		// Parameter logic for System Operations
		if (name.equals(Platform.Operations.KILL_REQUEST)) {
			valueMap.put(Operation.Value.REQUEST_ID, configuration.getSimple(Operation.Value.REQUEST_ID).getLongValue());
			valueMap.put(Operation.Value.SESSION_ID, configuration.getSimple(Operation.Value.SESSION_ID).getStringValue());
		} else if (name.equals(Platform.Operations.KILL_REQUEST)) {
			valueMap.put(Operation.Value.TRANSACTION_ID, configuration.getSimple(Operation.Value.TRANSACTION_ID).getStringValue());
		} else if (name.equals(Platform.Operations.KILL_SESSION)) {
			valueMap.put(Operation.Value.SESSION_ID, configuration.getSimple(Operation.Value.SESSION_ID).getStringValue());
		} else if (name.equals(Platform.Operations.DEPLOY_VDB_BY_URL)) {
			valueMap.put(Operation.Value.VDB_URL, configuration.getSimple(Operation.Value.VDB_URL).getStringValue());
			valueMap.put(Operation.Value.VDB_DEPLOY_NAME, configuration.getSimple(Operation.Value.VDB_DEPLOY_NAME).getStringValue());
			valueMap.put(Operation.Value.VDB_VERSION, configuration.getSimple(Operation.Value.VDB_VERSION).getIntegerValue());
		}
	}

	@Override
	public void getValues(MeasurementReport report, Set<MeasurementScheduleRequest> requests) throws Exception {

		DQPManagementView view = new DQPManagementView();

		Map<String, Object> valueMap = new HashMap<String, Object>();

		try {
			for (MeasurementScheduleRequest request : requests) {
				String name = request.getName();
				LOG.debug("Measurement name = " + name); //$NON-NLS-1$

				// Initialize any parameters to be used in the retrieval of
				// metric values

				Object metric = view.getMetric(getConnection(),
						getComponentType(), this.getComponentIdentifier(),
						name, valueMap);

				if (metric instanceof Double) {
					report.addData(new MeasurementDataNumeric(request, (Double) metric));
				}
				else if (metric instanceof Integer ){
					report.addData(new MeasurementDataNumeric(request, new Double(((Integer)metric).doubleValue())));
				}
				else if (metric instanceof Long){
					report.addData(new MeasurementDataNumeric(request, new Double(((Long)metric).longValue())));
				}
				else {
					LOG.error("Metric value must be a numeric value"); //$NON-NLS-1$
				}
			}
		} catch (Exception e) {
			LOG.error("Failed to obtain measurement [" + name 	+ "]. Cause: " + e); //$NON-NLS-1$ //$NON-NLS-2$
			throw (e);
		}
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
		super.stop();
	}

	@Override
	public void updateResourceConfiguration(ConfigurationUpdateReport report) {

		resourceConfiguration = report.getConfiguration().deepCopy();

		Configuration resourceConfig = report.getConfiguration();

		ManagementView managementView = null;
		ComponentType componentType = new ComponentType(
				PluginConstants.ComponentType.Platform.TEIID_TYPE,
				PluginConstants.ComponentType.Platform.TEIID_SUB_TYPE);

		report.setStatus(ConfigurationUpdateStatus.SUCCESS);
		try {

			managementView = getConnection().getManagementView();
			Set<ManagedComponent> allComponents = managementView.getComponentsForType(componentType);
			
			for (ManagedComponent managedComponent : allComponents) {
				
				Map<String, ManagedProperty> managedProperties = managedComponent.getProperties();

				
				ProfileServiceUtil.convertConfigurationToManagedProperties(managedProperties, resourceConfig, resourceContext.getResourceType(), managedComponent.getName());

				try {
					managementView.updateComponent(managedComponent);
				} catch (Exception e) {
					LOG.error("Unable to update component [" //$NON-NLS-1$
							+ managedComponent.getName() + "] of type " //$NON-NLS-1$
							+ componentType + ".", e); //$NON-NLS-1$
					report.setStatus(ConfigurationUpdateStatus.FAILURE);
					report.setErrorMessageFromThrowable(e);
				}
			}
		} catch (Exception e) {
			LOG.error("Unable to process update request", e); //$NON-NLS-1$
			report.setStatus(ConfigurationUpdateStatus.FAILURE);
			report.setErrorMessageFromThrowable(e);
		}
		
		managementView.load();

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.teiid.rhq.plugin.Facet#loadResourceConfiguration()
	 */
	@Override
	public Configuration loadResourceConfiguration() {

		// Get plugin config
		Configuration c = resourceConfiguration;
		
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
			mcSet = ProfileServiceUtil.getManagedComponents(getConnection(),
					new org.jboss.managed.api.ComponentType(
									PluginConstants.ComponentType.Platform.TEIID_TYPE,
									PluginConstants.ComponentType.Platform.TEIID_SUB_TYPE));
		} catch (NamingException e) {
			LOG.error("NamingException getting components in Platform loadConfiguration(): "	+ e.getMessage()); //$NON-NLS-1$
		} catch (Exception e) {
			LOG.error("Exception getting components in Platform loadConfiguration(): "	+ e.getMessage()); //$NON-NLS-1$
		}
		
		for (ManagedComponent mc : mcSet) {
			Map<String, ManagedProperty> mcMap = mc.getProperties();
			String name = mc.getName();			
			setProperties(name, mcMap, configuration);
		}
	}

	/**
	 * @param mcMap
	 * @param configuration
	 */
	private void setProperties(String compName, Map<String, ManagedProperty> mcMap, Configuration configuration) {
		for (ManagedProperty mProp : mcMap.values()) {
			try {
				String value = ProfileServiceUtil.stringValue(mProp.getValue());
				PropertySimple prop = new PropertySimple(compName+"."+mProp.getName(), value); //$NON-NLS-1$
				configuration.put(prop);
			} catch (Exception e) {
				LOG.error("Exception setting properties in Platform loadConfiguration(): "	+ e.getMessage()); //$NON-NLS-1$
			}
		}
	}

	@Override
	public ProfileServiceConnection getConnection() {
		return ((ApplicationServerComponent) this.resourceContext.getParentResourceComponent()).getConnection();
	}

	@Override
	public EmsConnection getEmsConnection() {
		return null;
	}

}