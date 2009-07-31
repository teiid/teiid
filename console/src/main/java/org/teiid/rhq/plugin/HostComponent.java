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

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.configuration.PropertySimple;
import org.rhq.core.domain.measurement.MeasurementDataNumeric;
import org.rhq.core.domain.measurement.MeasurementReport;
import org.rhq.core.domain.measurement.MeasurementScheduleRequest;
import org.rhq.core.pluginapi.configuration.ConfigurationFacet;
import org.rhq.core.pluginapi.inventory.InvalidPluginConfigurationException;
import org.rhq.core.pluginapi.measurement.MeasurementFacet;
import org.teiid.rhq.comm.ConnectionConstants;
import org.teiid.rhq.comm.ConnectionException;
import org.teiid.rhq.comm.ConnectionConstants.ComponentType.Runtime.Host;


/**
 * Component class for the MetaMatrix Host Controller process.
 * 
 */
public class HostComponent extends Facet {
	private final Log LOG = LogFactory
			.getLog(HostComponent.class);


	public static final String CONNECTOR_ADDRESS_CONFIG_PROPERTY = "connectorAddress"; //$NON-NLS-1$

	public static final String CONNECTION_TYPE = "type"; //$NON-NLS-1$

	public static final String PARENT_TYPE = "PARENT"; //$NON-NLS-1$
	
	public static final String INSTALL_DIR = "install.dir"; //$NON-NLS-1$
	
    
	private String install_dir;
    /** 
     * @see org.teiid.rhq.plugin.Facet#getComponentType()
     * @since 1.0
     */
    @Override
    String getComponentType() {
        return Host.TYPE;
    }
    
    
    
    String getInstallDirectory() {
    	
    	if (install_dir != null) {
    		return install_dir;
    	}
    	install_dir = resourceContext.getPluginConfiguration()
		.getSimpleProperties().get(INSTALL_DIR)
		.getStringValue(); 
 
    	return install_dir;
    }


	/**
	 * The plugin container will call this method when your resource component
	 * has been scheduled to collect some measurements now. It is within this
	 * method that you actually talk to the managed resource and collect the
	 * measurement data that is has emitted.
	 * 
	 * @see MeasurementFacet#getValues(MeasurementReport, Set)
	 */
	public void getValues(MeasurementReport report,
			Set<MeasurementScheduleRequest> requests) {
		for (MeasurementScheduleRequest request : requests) {
			String name = request.getName();

			// TODO: based on the request information, you must collect the
			// requested measurement(s)
			// you can use the name of the measurement to determine what you
			// actually need to collect
			try {
				Number value = new Integer(1); // dummy measurement value -
												// this should come from the
												// managed resource
				report.addData(new MeasurementDataNumeric(request, value
						.doubleValue()));
			} catch (Exception e) {
				LOG.error("Failed to obtain measurement [" + name //$NON-NLS-1$
						+ "]. Cause: " + e); //$NON-NLS-1$
			}
		}

		return;
	}
	
	protected void setOperationArguments(String name, Configuration configuration,
			Map argumentMap) {

		if (name.equals(ConnectionConstants.ComponentType.Operation.GET_PROPERTIES)){
			String key = ConnectionConstants.IDENTIFIER;
			argumentMap.put(key, getComponentIdentifier());
		}
 		
	} 
	
	/**
	 * The plugin container will call this method and it needs to obtain the
	 * current configuration of the managed resource. Your plugin will obtain
	 * the managed resource's configuration in your own custom way and populate
	 * the returned Configuration object with the managed resource's
	 * configuration property values.
	 * 
	 * @see ConfigurationFacet#loadResourceConfiguration()
	 * 
	 */
	@Override
	public Configuration loadResourceConfiguration() {
		// here we simulate the loading of the managed resource's configuration
		Configuration config = this.getResourceConfiguration() ;
		if (config == null) {
			// for this example, we will create a simple dummy configuration to
			// start with.
			// note that it is empty, so we're assuming there are no required
			// configs in the plugin descriptor.
			config = new Configuration();
		}

   		Properties props;
		try {
			props = getConnection().getProperties(this.getComponentType(), this.getComponentIdentifier());
		} catch (ConnectionException e) {
			LOG.error("Failed to obtain host properties for [" + this.getComponentIdentifier() //$NON-NLS-1$
					+ "]. Cause: " + e); //$NON-NLS-1$
			 throw new InvalidPluginConfigurationException(e); 
		}
        
   		if (props != null && props.size() > 0) {
   			Iterator it=props.keySet().iterator();
   			while(it.hasNext())  {
   				String k = (String)it.next();
   				
   				config.put(new PropertySimple(k, props.get(k)));

   			}
   			
   		}

				
		this.setResourceConfiguration(config);
		return this.getResourceConfiguration();
	}

}