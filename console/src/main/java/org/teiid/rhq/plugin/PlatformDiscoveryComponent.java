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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.naming.NamingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.managed.api.ComponentType;
import org.jboss.managed.api.ManagedComponent;
import org.jboss.managed.api.ManagedObject;
import org.jboss.managed.api.ManagedProperty;
import org.jboss.managed.plugins.ManagedComponentImpl;
import org.jboss.managed.plugins.ManagedObjectImpl;
import org.jboss.metatype.api.values.CollectionValueSupport;
import org.jboss.metatype.api.values.EnumValueSupport;
import org.jboss.metatype.api.values.GenericValueSupport;
import org.jboss.metatype.api.values.MetaValue;
import org.jboss.metatype.api.values.SimpleValue;
import org.jboss.metatype.api.values.SimpleValueSupport;
import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.configuration.Property;
import org.rhq.core.domain.configuration.PropertyList;
import org.rhq.core.domain.configuration.PropertyMap;
import org.rhq.core.domain.configuration.PropertySimple;
import org.rhq.core.pluginapi.inventory.DiscoveredResourceDetails;
import org.rhq.core.pluginapi.inventory.InvalidPluginConfigurationException;
import org.rhq.core.pluginapi.inventory.ResourceDiscoveryComponent;
import org.rhq.core.pluginapi.inventory.ResourceDiscoveryContext;
import org.teiid.rhq.plugin.util.PluginConstants;
import org.teiid.rhq.plugin.util.ProfileServiceUtil;

/**
 * This is the parent node for a Teiid system
 */
public class PlatformDiscoveryComponent implements ResourceDiscoveryComponent  {

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

		ManagedComponent mc = ProfileServiceUtil.getManagedComponent(
				new ComponentType("teiid", "dqp"),
				PluginConstants.ComponentType.Platform.TEIID_RUNTIME_ENGINE);

		Configuration c = new Configuration();
		/**
		 * 
		 * A discovered resource must have a unique key, that must stay the same
		 * when the resource is discovered the next time
		 */
		DiscoveredResourceDetails detail = new DiscoveredResourceDetails(
				discoveryContext.getResourceType(), // ResourceType
				mc.getName(), // Resource Key
				PluginConstants.ComponentType.Platform.TEIID_ENGINE_RESOURCE_NAME, // Resource
				// Name
				null, // Version TODO can we get that from discovery ?
				PluginConstants.ComponentType.Platform.TEIID_ENGINE_RESOURCE_DESCRIPTION, // Description
				discoveryContext.getDefaultPluginConfiguration(), // Plugin
				// Config
				null // Process info from a process scan
		);
		
		Configuration configuration = detail.getPluginConfiguration();
		
		getProperties(configuration);

		// Add to return values
		discoveredResources.add(detail);
		log.info("Discovered Teiid instance: " + mc.getName());
		detail.setPluginConfiguration(configuration);
		return discoveredResources;

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
			mcSet = ProfileServiceUtil
					.getManagedComponents(new org.jboss.managed.api.ComponentType(
							PluginConstants.ComponentType.Platform.TEIID_TYPE,
							PluginConstants.ComponentType.Platform.TEIID_SUB_TYPE));
		} catch (NamingException e) {
			log.error("NamingException getting components in PlatformDisocovery: " + e.getMessage());
		} catch (Exception e) {
			log.error("Exception getting components in PlatformDisocovery: " + e.getMessage());
		}

		for (ManagedComponent mc : mcSet) {
			String componentName = (String) mc.getComponentName();
			if (componentName
					.equals("RuntimeEngineDeployer")) {
				Map<String, ManagedProperty> mcMap = mc.getProperties();
				PropertyMap teiidPropertyMap = new PropertyMap("teiidProperties");
				configuration.put(teiidPropertyMap);
				configuration.put(new PropertySimple(PluginConstants.Operation.Value.LONG_RUNNING_QUERY_LIMIT,600));
				setProperties(mcMap, teiidPropertyMap);
			} else if (componentName
					.equals("BufferService")) {
				Map<String, ManagedProperty> mcMap = mc.getProperties();
				PropertyMap bufferServicePropertyMap = new PropertyMap("bufferServiceProperties");
				configuration.put(bufferServicePropertyMap);
				setProperties(mcMap, bufferServicePropertyMap);
			} else if (componentName
					.equals("SessionService")) {
				Map<String, ManagedProperty> mcMap = mc.getProperties();
				PropertyMap sessionServicePropertyMap = new PropertyMap("sessionServiceProperties");
				configuration.put(sessionServicePropertyMap);
				setProperties(mcMap, sessionServicePropertyMap);
			} else if (componentName
					.equals("AuthorizationService")) {
				Map<String, ManagedProperty> mcMap = mc.getProperties();
				PropertyMap authorizationServicePropertyMap = new PropertyMap("authorizationServiceProperties");
				configuration.put(authorizationServicePropertyMap);
				setProperties(mcMap, authorizationServicePropertyMap);
			} else if (componentName
					.equals("JdbcSocketConfiguration")) {
				Map<String, ManagedProperty> mcMap = mc.getProperties();
				PropertyMap socketConfigurationPropertyMap = new PropertyMap("jdbcSocketConfigurationProperties");
				configuration.put(socketConfigurationPropertyMap);
				setProperties(mcMap, socketConfigurationPropertyMap);
			}
		}
	}

	/**
	 * @param mcMap
	 * @param propertyMap
	 */
	private void setProperties(Map<String, ManagedProperty> mcMap,
			PropertyMap propertyMap) {
		for (ManagedProperty mProp : mcMap.values()) {
			try {
				String value = ProfileServiceUtil.stringValue(mProp.getValue());
				PropertySimple prop = new PropertySimple(mProp.getName(), value);
				propertyMap.put(prop);
			} catch (Exception e) {
				log.error("Exception setting properties in PlatformDiscovery: " + e.getMessage());
			}
		}
	}
	
}