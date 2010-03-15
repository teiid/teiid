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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.measurement.MeasurementDataNumeric;
import org.rhq.core.domain.measurement.MeasurementDataTrait;
import org.rhq.core.domain.measurement.MeasurementReport;
import org.rhq.core.domain.measurement.MeasurementScheduleRequest;
import org.teiid.rhq.admin.DQPManagementView;
import org.teiid.rhq.comm.ConnectionConstants;
import org.teiid.rhq.plugin.util.PluginConstants;
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
	 * @see org.teiid.rhq.plugin.Facet#getComponentName()
	 */
	@Override
	public String getComponentName() {
		return PluginConstants.ComponentType.VDB.NAME;
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
	
	@Override
	protected void setMetricArguments(String name,
			Configuration configuration, Map<String, Object> valueMap) {
		// Parameter logic for VDB Metrics
		String key = VDB.NAME;
		valueMap.put(key, this.resourceConfiguration.getSimpleValue("name", null));
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
				if (request
						.getName()
						.equals(
								PluginConstants.ComponentType.VDB.Metrics.QUERY_COUNT)) {
					report.addData(new MeasurementDataTrait(request, (String)metricReturnObject));
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
							report.addData(new MeasurementDataTrait(request,
									 (String) metricReturnObject));
						} else {
							if (request
									.getName()
									.equals(
											PluginConstants.ComponentType.VDB.Metrics.LONG_RUNNING_QUERIES)) {
								report.addData(new MeasurementDataNumeric(
										request, (Double) metricReturnObject));
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
	
}

