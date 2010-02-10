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
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.naming.NamingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.managed.api.ComponentType;
import org.jboss.managed.api.ManagedComponent;
import org.jboss.managed.api.RunState;
import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.configuration.PropertySimple;
import org.rhq.core.domain.measurement.AvailabilityType;
import org.rhq.core.domain.measurement.MeasurementDataNumeric;
import org.rhq.core.domain.measurement.MeasurementReport;
import org.rhq.core.domain.measurement.MeasurementScheduleRequest;
import org.rhq.core.pluginapi.configuration.ConfigurationUpdateReport;
import org.teiid.rhq.admin.utils.SingletonConnectionManager;
import org.teiid.rhq.comm.Connection;
import org.teiid.rhq.comm.ConnectionConstants;
//import org.teiid.rhq.comm.ConnectionConstants;
//import org.teiid.rhq.comm.Connection;
//import org.teiid.rhq.comm.ConnectionConstants;
//import org.teiid.rhq.comm.ConnectionConstants.ComponentType.Runtime.System.Metrics;
import org.teiid.rhq.plugin.util.PluginConstants;
import org.teiid.rhq.plugin.util.ProfileServiceUtil;
import org.teiid.rhq.plugin.util.PluginConstants.Operation;
import org.teiid.rhq.plugin.util.PluginConstants.ComponentType.Runtime.Metrics;


/**
 * 
 */
public class PlatformComponent extends Facet {
	private final Log LOG = LogFactory.getLog(PlatformComponent.class);

	/**
	 * @see org.teiid.rhq.plugin.Facet#getComponentType()
	 * @since 4.3
	 */
	@Override
	String getComponentType() {
		return null;
	}
	
	@Override
	public AvailabilityType getAvailability() {
		RunState runState = null;
		ManagedComponent mc;
		try {
			mc = ProfileServiceUtil.getManagedComponent(
					new ComponentType(PluginConstants.ComponentType.Runtime.TYPE,
							PluginConstants.ComponentType.Runtime.SUBTYPE),
					PluginConstants.ComponentType.Runtime.TEIID_RUNTIME_ENGINE);
			runState = mc.getRunState();
		} catch (NamingException e) {
			LOG.error("Naming exception getting: " + PluginConstants.ComponentType.Runtime.TEIID_RUNTIME_ENGINE);
	        return AvailabilityType.DOWN;
		} catch (Exception e) {
			LOG.error("Exception getting: " + PluginConstants.ComponentType.Runtime.TEIID_RUNTIME_ENGINE);
			return AvailabilityType.DOWN;
		}
		
		return (runState == RunState.RUNNING) ? AvailabilityType.UP : AvailabilityType.DOWN;
		
	}

	
	protected void setOperationArguments(String name, Configuration configuration,
			Map valueMap) {
 		
	}	


	@Override
	public void getValues(MeasurementReport report,
			Set<MeasurementScheduleRequest> requests) throws Exception {
	}
	
	
	
	@Override
	public void stop() {
		// TODO Auto-generated method stub
		super.stop();
	}

	@Override
	public void updateResourceConfiguration(ConfigurationUpdateReport report) {
		
		Properties props = System.getProperties();
		
		Iterator<PropertySimple> pluginPropIter = report.getConfiguration().getSimpleProperties().values().iterator();
		
		while (pluginPropIter.hasNext()){
			PropertySimple pluginProp = pluginPropIter.next();
			props.put(pluginProp.getName(), pluginProp.getStringValue());
		}
		
		SingletonConnectionManager.getInstance().initialize(props);
		super.updateResourceConfiguration(report);
		
	}	

}