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

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.configuration.PropertySimple;
import org.rhq.core.domain.resource.ResourceType;
import org.rhq.core.pluginapi.inventory.DiscoveredResourceDetails;
import org.rhq.core.pluginapi.inventory.InvalidPluginConfigurationException;
import org.rhq.core.pluginapi.inventory.ResourceDiscoveryComponent;
import org.rhq.core.pluginapi.inventory.ResourceDiscoveryContext;
import org.teiid.rhq.comm.Component;
import org.teiid.rhq.comm.Connection;
import org.teiid.rhq.comm.ConnectionException;


/**
 * Discovery component used to discover the monitored connector bindings
 * 
 */
public abstract class NodeChildrenDiscoveryComponent extends NodeDiscoveryComponent {


	private final Log LOG = LogFactory.getLog(NodeChildrenDiscoveryComponent.class);

	/**
	 * @see ResourceDiscoveryComponent#discoverResources(ResourceDiscoveryContext)
	 */

	@Override
	protected void addResources(Set<DiscoveredResourceDetails> found, ResourceType resourceType, Facet parent)
		throws InvalidPluginConfigurationException {
		Connection conn = null;
		   try {
		        conn = getConnection();
		        if (!conn.isValid()) {
		            return;
		        }
		        Collection<Component> components =  getComponents(conn, parent);
		        
		        if (components == null || components.size() == 0) {
		            LOG.info("No components were found to be configured for parent "+ parent.getComponentIdentifier()); //$NON-NLS-1$
		            return;
		        }
		         
		        Iterator<Component> comIter = components.iterator();
		        
		        while (comIter.hasNext()) {
		          Component comp =  comIter.next();           
		          LOG.info("Processing component "+ comp.getName()); //$NON-NLS-1$
		          
		          DiscoveredResourceDetails resource = this.createResource(resourceType, comp);
           
		          found.add(resource);      
		        }
	       } catch (InvalidPluginConfigurationException ipe) {
	            throw ipe;
	        } catch (Throwable t) {
	            throw new InvalidPluginConfigurationException(t);            
	            
	        } finally {
	            if (conn != null) {
	                conn.close();
	            }
	        } 

		
	}
	
	protected DiscoveredResourceDetails createResource(ResourceType resourceType, Component component)
	throws InvalidPluginConfigurationException {
		
		DiscoveredResourceDetails resource = createResource(resourceType, 
      		  systemkey + "|" + component.getIdentifier(), 
      		component.getName(), 
      		(component.getDescription()!=null?component.getDescription():""));

  	                   
	    Configuration configuration = resource.getPluginConfiguration();
	    configuration.put(new PropertySimple(Component.NAME, component.getName()));
	    configuration.put(new PropertySimple(Component.IDENTIFIER, component.getIdentifier()));
	    configuration.put(new PropertySimple(Component.SYSTEM_KEY, systemkey));
	    
	    addAdditionalProperties(configuration, component);
	    
	    return resource;
	
	}
	
	protected void addAdditionalProperties(Configuration configuration, Component component) throws InvalidPluginConfigurationException {
		
	}
	
	abstract Collection<Component> getComponents(Connection conn, Facet parent) throws ConnectionException;
	

}