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

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.managed.api.ComponentType;
import org.jboss.managed.api.ManagedComponent;
import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.configuration.PropertySimple;
import org.rhq.core.pluginapi.inventory.DiscoveredResourceDetails;
import org.rhq.core.pluginapi.inventory.InvalidPluginConfigurationException;
import org.rhq.core.pluginapi.inventory.ResourceDiscoveryComponent;
import org.rhq.core.pluginapi.inventory.ResourceDiscoveryContext;
import org.rhq.plugins.jbossas5.ApplicationServerComponent;
import org.rhq.plugins.jbossas5.connection.ProfileServiceConnection;
import org.teiid.rhq.plugin.util.PluginConstants;
import org.teiid.rhq.plugin.util.ProfileServiceUtil;

/**
 * This is the parent node for a Teiid system
 */
public class PlatformDiscoveryComponent implements ResourceDiscoveryComponent {

	private final Log log = LogFactory.getLog(PluginConstants.DEFAULT_LOGGER_CATEGORY);

	/**
	 * Review the javadoc for both {@link ResourceDiscoveryComponent} and
	 * {@link ResourceDiscoveryContext} to learn what you need to do in this
	 * method.
	 * 
	 * @see ResourceDiscoveryComponent#discoverResources(ResourceDiscoveryContext)
	 */
	public Set<DiscoveredResourceDetails> discoverResources(
			ResourceDiscoveryContext discoveryContext)
			throws InvalidPluginConfigurationException, Exception {

		Set<DiscoveredResourceDetails> discoveredResources = new HashSet<DiscoveredResourceDetails>();
		ProfileServiceConnection connection = ((ApplicationServerComponent) discoveryContext.getParentResourceComponent()).getConnection();
		
		ManagedComponent mc = ProfileServiceUtil.getManagedComponent(connection,
				new ComponentType(
						PluginConstants.ComponentType.Platform.TEIID_TYPE,
						PluginConstants.ComponentType.Platform.TEIID_SUB_TYPE),
						PluginConstants.ComponentType.Platform.TEIID_RUNTIME_ENGINE);
		
		if (mc==null){
			//No Teiid instance found
			return discoveredResources;
		}
		
		String version = ProfileServiceUtil.getSimpleValue(mc, "runtimeVersion", String.class); //$NON-NLS-1$
			
		/**
		 * 
		 * A discovered resource must have a unique key, that must stay the same
		 * when the resource is discovered the next time
		 */
		DiscoveredResourceDetails detail = new DiscoveredResourceDetails(
				discoveryContext.getResourceType(), // ResourceType
				mc.getName(), // Resource Key
				PluginConstants.ComponentType.Platform.TEIID_ENGINE_RESOURCE_NAME, // Resource name
				version,
				PluginConstants.ComponentType.Platform.TEIID_ENGINE_RESOURCE_DESCRIPTION, // Description
				discoveryContext.getDefaultPluginConfiguration(), // Plugin
				// Config
				null // Process info from a process scan
		);

		Configuration configuration = detail.getPluginConfiguration();
		configuration.put(new PropertySimple(
				PluginConstants.Operation.Value.LONG_RUNNING_QUERY_LIMIT, 600));
		detail.setPluginConfiguration(configuration);

		// Add to return values
		discoveredResources.add(detail);
		log.info("Discovered Teiid instance: " + mc.getName()); //$NON-NLS-1$
		return discoveredResources;

	}

}