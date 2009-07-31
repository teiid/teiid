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
import org.rhq.core.domain.configuration.PropertyList;
import org.rhq.core.domain.measurement.AvailabilityType;
import org.rhq.core.domain.measurement.MeasurementReport;
import org.rhq.core.domain.measurement.MeasurementScheduleRequest;
import org.rhq.core.pluginapi.configuration.ConfigurationFacet;
import org.rhq.core.pluginapi.inventory.ResourceContext;
import org.teiid.rhq.comm.ConnectionConstants.ComponentType.Runtime.Session;
import org.teiid.rhq.comm.ConnectionConstants.ComponentType.Runtime.Session.Query;
import org.teiid.rhq.plugin.objects.ExecutedResourceConfigurationResultImpl;


/**
 * MetaMatrix Connector component class
 * 
 */
public class SessionComponent extends Facet {

	private final Log LOG = LogFactory.getLog(SessionComponent.class);
	
	
	private ExecutedResourceConfigurationResultImpl getSessions = null;

	/**
	 * @see org.teiid.rhq.plugin.Facet#getComponentType()
	 * @since 1.0
	 */
	@Override
	String getComponentType() {
		return Session.TYPE;
	}
	
	public void start(ResourceContext context) {
		super.start(context);
		
		Map defns = context.getResourceType().getResourceConfigurationDefinition().getPropertyDefinitions();
		
		getSessions = new ExecutedResourceConfigurationResultImpl(
				this.getComponentType(),
				Query.GET_SESSIONS, 
				defns);
	}
	
	protected void setOperationArguments(String name, Configuration configuration,
			Map argumentMap) {

		
	}	
	
	@Override
	public AvailabilityType getAvailability() {
		return AvailabilityType.UP;

	}

	@Override
	public void getValues(MeasurementReport arg0,
			Set<MeasurementScheduleRequest> arg1) throws Exception {
		// TODO Auto-generated method stub

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
  		
  		getSessions.reset();
		
		execute(getSessions, new HashMap());


  		config.put( (PropertyList) getSessions.getResult());
         
  				
  		this.setResourceConfiguration(config);
  		return this.getResourceConfiguration();
  	}	


//  	class SessionComparable implements Comparator {
//
//		public int compare(Object arg0, Object arg1) {
//			// TODO Auto-generated method stub
//			Component a = (Component) arg0;
//			Component b = (Component) arg1;
//			
//		        if ( a == null && b == null ) {
//		            return 0;
//		        }
//		        if ( a != null && b == null ) {
//		            return 1;
//		        }
//		        if ( a == null && b != null ) {
//		            return -1;
//		        }
//		        int result = a.get.compareTo(b.getDisplayName());
//		        return result;
//		}


  		
 // 	}

}