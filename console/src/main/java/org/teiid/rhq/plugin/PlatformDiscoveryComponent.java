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

import javax.naming.InitialContext;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.deployers.spi.management.ManagementView;
import org.jboss.managed.api.ComponentType;
import org.jboss.managed.api.ManagedComponent;
import org.jboss.profileservice.spi.ProfileService;
import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.configuration.PropertySimple;
import org.rhq.core.pluginapi.inventory.DiscoveredResourceDetails;
import org.rhq.core.pluginapi.inventory.InvalidPluginConfigurationException;
import org.rhq.core.pluginapi.inventory.ResourceDiscoveryComponent;
import org.rhq.core.pluginapi.inventory.ResourceDiscoveryContext;
import org.teiid.rhq.plugin.util.PluginConstants;

/**
 * This is the parent node for a MetaMatrix system
 */
public class PlatformDiscoveryComponent implements ResourceDiscoveryComponent {
	
	private static final Log LOG = LogFactory
			.getLog(PlatformDiscoveryComponent.class);
	
	public static final String p = "connectorAddress"; //$NON-NLS-1$


	private final Log log = LogFactory.getLog(this.getClass());

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

		InitialContext ic = new InitialContext();
		ProfileService ps = (ProfileService) ic.lookup(PluginConstants.PROFILE_SERVICE);

		ManagementView vm = ps.getViewManager();
		vm.load();
		ComponentType type = new ComponentType(PluginConstants.CONNECTION_FACTORY_TYPE, PluginConstants.NO_TX_SUBTYPE);
		ManagedComponent mc = vm.getComponent(PluginConstants.TEIID_RUNTIME_ENGINE,
				type);

		/*
		 * Currently this uses a hardcoded remote address for access to the
		 * MBean server This needs to be switched to check if we e.g. run inside
		 * a JBossAS to which we have a connection already that we can reuse.
		 */
		Configuration c = new Configuration(); // TODO get from
												// defaultPluginConfig

		String managerName = mc.getName();
		
		c.put(new PropertySimple("objectName", managerName));
		/**
		 * 
		 * A discovered resource must have a unique key, that must stay the same
		 * when the resource is discovered the next time
		 */
		DiscoveredResourceDetails detail = new DiscoveredResourceDetails(
				discoveryContext.getResourceType(), // ResourceType
				managerName, // Resource Key
				PluginConstants.TEIID_ENGINE_RESOURCE_NAME, // Resource Name
				null, // Version TODO can we get that from discovery ?
				PluginConstants.TEIID_ENGINE_RESOURCE_DESCRIPTION, // Description
				c, // Plugin Config
				null // Process info from a process scan
		);

		// Add to return values
		discoveredResources.add(detail);
		log.info("Discovered Teiid instance: " + managerName);
		return discoveredResources;
	
	}
}