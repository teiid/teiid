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
import org.rhq.core.domain.measurement.MeasurementReport;
import org.rhq.core.domain.measurement.MeasurementScheduleRequest;
import org.teiid.rhq.comm.Connection;
import org.teiid.rhq.comm.ConnectionConstants;
import org.teiid.rhq.comm.ConnectionConstants.ComponentType;
import org.teiid.rhq.comm.ConnectionConstants.ComponentType.Runtime.Queries.Query;
import org.teiid.rhq.comm.ConnectionConstants.ComponentType.Runtime.System.Metrics;
import org.teiid.rhq.comm.ConnectionConstants.ComponentType.Runtime.System.Operations;


/**
 * 
 */
public class SystemComponent extends Facet {
	private final Log LOG = LogFactory.getLog(SystemComponent.class);

	/**
	 * Property is used to identify an unreachable system
	 */
	protected static final String UNREACHABLE_NAME = "UNREACHABLE_SYSTEM"; //$NON-NLS-1$

	/**
	 * @see org.teiid.rhq.plugin.Facet#getComponentType()
	 * @since 4.3
	 */
	@Override
	String getComponentType() {
		return ConnectionConstants.ComponentType.Runtime.System.TYPE;
	}
	
	protected void setOperationArguments(String name, Configuration configuration,
			Map valueMap) {

		// Parameter logic for System Operations
		if (name.equals(Query.GET_QUERIES) ||
			name.equals(Operations.GET_LONGRUNNINGQUERIES)) {
			Boolean includeSourceQueries = configuration.getSimple(ConnectionConstants.ComponentType.Operation.Value.INCLUDE_SOURCE_QUERIES).getBooleanValue();
			Integer long_running_value = getResourceConfiguration().getSimple(ConnectionConstants.ComponentType.Operation.Value.LONG_RUNNING_QUERY_LIMIT).getIntegerValue();
			valueMap.put(ConnectionConstants.ComponentType.Operation.Value.INCLUDE_SOURCE_QUERIES, includeSourceQueries);
			valueMap.put(ConnectionConstants.ComponentType.Operation.Value.LONG_RUNNING_QUERY_LIMIT, long_running_value);				
		}else if (name.equals(Operations.BOUNCE_SYSTEM)) {
			Boolean waitUntilFinished = configuration.getSimple(ConnectionConstants.ComponentType.Operation.Value.WAIT_UNTIL_FINISHED).getBooleanValue();
			valueMap.put(ConnectionConstants.ComponentType.Operation.Value.WAIT_UNTIL_FINISHED, waitUntilFinished);
		}else if (name.equals(ConnectionConstants.ComponentType.Operation.KILL_REQUEST)) {
			String key = ConnectionConstants.ComponentType.Operation.Value.REQUEST_ID;
			valueMap.put(key, configuration.getSimple(key).getStringValue());
		}else if (name.equals(ConnectionConstants.ComponentType.Operation.GET_PROPERTIES) ) {
			String key = ConnectionConstants.IDENTIFIER;
			valueMap.put(key, getComponentIdentifier());
		}
 		
	}	


	@Override
	public void getValues(MeasurementReport report,
			Set<MeasurementScheduleRequest> requests) throws Exception {
		
		// because the sytsem object will be created before the use actually connects, checks have to be
		// made not to perform actions that will require a connection before its available
		if (!this.isAvailable()) {
			return;
		}

		Connection conn = null;
		Map valueMap = new HashMap();

		try {
			conn = getConnection();
			if (!conn.isValid()) {
				return;
			}
			for (MeasurementScheduleRequest request : requests) {
				String name = request.getName();
				LOG.debug("Measurement name = " + name); //$NON-NLS-1$
				
				//Initialize any parameters to be used in the retrieval of metric values
				if (request.getName().equals(Metrics.LONG_RUNNING_QUERIES)) {
					Integer value = getResourceConfiguration().getSimple(ConnectionConstants.ComponentType.Operation.Value.LONG_RUNNING_QUERY_LIMIT).getIntegerValue();
					valueMap.put(ComponentType.Operation.Value.LONG_RUNNING_QUERY_LIMIT, value);
				}
				
				Object metricReturnObject = conn.getMetric(getComponentType(),
						this.getComponentIdentifier(),
						name, 
						valueMap);

				try {
					if (request.getName().equals(
							Metrics.QUERY_COUNT)) {
						report.addData(new MeasurementDataNumeric(request,
								(Double) metricReturnObject));
					} else {
						if (request.getName().equals(
								Metrics.SESSION_COUNT)) {
							report.addData(new MeasurementDataNumeric(request,
									(Double) metricReturnObject));
						} else {
							if (request.getName().equals(
									Metrics.LONG_RUNNING_QUERIES)) {
								report.addData(new MeasurementDataNumeric(
										request, (Double) metricReturnObject));
							}
						}
					}

				} catch (Exception e) {
					LOG.error("Failed to obtain measurement [" + name //$NON-NLS-1$
							+ "]. Cause: " + e); //$NON-NLS-1$
					// throw(e);
				}
			}
		} finally {
			if (conn != null) {
				conn.close();
			}
		}
	}
	
//
//	@Override
//	public void updateResourceConfiguration(ConfigurationUpdateReport report) {
//		
//		Properties props = System.getProperties();
//		
//		Iterator<PropertySimple> pluginPropIter = report.getConfiguration().getSimpleProperties().values().iterator();
//		
//		while (pluginPropIter.hasNext()){
//			PropertySimple pluginProp = pluginPropIter.next();
//			props.put(pluginProp.getName(), pluginProp.getStringValue());
//		}
//		
//		SingletonConnectionManager.getInstance().initialize(props);
//		super.updateResourceConfiguration(report);
//		
//	}	

}