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

import java.util.Collections;
import java.util.HashSet;
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
import org.teiid.rhq.admin.utils.SingletonConnectionManager;
import org.teiid.rhq.comm.Component;
import org.teiid.rhq.comm.Connection;
import org.teiid.rhq.comm.ConnectionConstants;


/**
 * Discovery component used to discover the monitored services
 *
 */
public class NodeDiscoveryComponent implements
		ResourceDiscoveryComponent {
	private static final Log LOG = LogFactory
			.getLog(NodeDiscoveryComponent.class);
    
    private static SingletonConnectionManager connMgr  = SingletonConnectionManager.getInstance();

    protected String systemkey = null;
    

	public Set<DiscoveredResourceDetails> discoverResources(
			ResourceDiscoveryContext discoveryContext) throws InvalidPluginConfigurationException, Exception {
		
        
        String name = discoveryContext.getResourceType().getName();
  
        LOG.info("Discovering " + name); //$NON-NLS-1$

        // if no servers have been defined, do not discover resources
        if (!connMgr.hasServersDefined()) {
            LOG.info("Unable to discover " + name + ", no JBEDSP Platforms defined"); //$NON-NLS-1$
            return Collections.EMPTY_SET;
        }

        Facet parent = (Facet) discoveryContext.getParentResourceComponent();
         
        String parentName = parent.getComponentName(); 
        systemkey = parent.getSystemKey();
        
        LOG.info("Discovering JBEDSP " + name + " for " + (parentName!=null?parentName:"PARENTNAMENOTFOUND")); //$NON-NLS-1$ //$NON-NLS-2$
        
		Set<DiscoveredResourceDetails> found = new HashSet<DiscoveredResourceDetails>();           
              
        addResources(found, discoveryContext.getResourceType(), parent);
        
       return found;
	}
	
	protected void addResources(Set<DiscoveredResourceDetails> found, ResourceType resourceType, Facet parent)
		throws InvalidPluginConfigurationException {
		 String identifier = parent.getComponentIdentifier()+ "|" + resourceType.getName();
        DiscoveredResourceDetails resource = createResource(resourceType, identifier,  resourceType.getName(), resourceType.getDescription());
           
        found.add(resource);           	  
		
	}
		
	protected DiscoveredResourceDetails createResource(ResourceType resourceType, String identifier, String name, String description)
		throws InvalidPluginConfigurationException {

        DiscoveredResourceDetails resource = new DiscoveredResourceDetails(resourceType,
      		  identifier, name,
      		  ConnectionConstants.VERSION, 
      		  (description!=null?description:""), 
      		  null, 
      		  null);
      	                   
        Configuration configuration = resource.getPluginConfiguration();
        configuration.put(new PropertySimple(Component.NAME, name));
        configuration.put(new PropertySimple(Component.IDENTIFIER, identifier));
        configuration.put(new PropertySimple(Component.SYSTEM_KEY, systemkey));
        
        addAdditionalProperties(configuration);
        
        return resource;
		
	}
	

	
	protected void addAdditionalProperties(Configuration configuration) throws InvalidPluginConfigurationException {
		
	}
	
	protected Connection getConnection() throws InvalidPluginConfigurationException{
		try {
			return connMgr.getConnection(this.systemkey);

		} catch (Throwable t) {
          throw new InvalidPluginConfigurationException(t);            
          
      } 
	}
	
}