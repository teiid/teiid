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
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.managed.api.ComponentType;
import org.jboss.managed.api.ManagedComponent;
import org.jboss.managed.api.ManagedObject;
import org.jboss.managed.api.ManagedProperty;
import org.jboss.managed.plugins.ManagedObjectImpl;
import org.jboss.metatype.api.values.CollectionValueSupport;
import org.jboss.metatype.api.values.GenericValueSupport;
import org.jboss.metatype.api.values.MetaValue;
import org.jboss.metatype.api.values.MetaValueFactory;
import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.configuration.PropertyList;
import org.rhq.core.domain.configuration.PropertyMap;
import org.rhq.core.domain.configuration.PropertySimple;
import org.rhq.core.pluginapi.inventory.DiscoveredResourceDetails;
import org.rhq.core.pluginapi.inventory.InvalidPluginConfigurationException;
import org.rhq.core.pluginapi.inventory.ResourceDiscoveryComponent;
import org.rhq.core.pluginapi.inventory.ResourceDiscoveryContext;
import org.rhq.plugins.jbossas5.connection.ProfileServiceConnection;
import org.teiid.rhq.plugin.util.PluginConstants;
import org.teiid.rhq.plugin.util.ProfileServiceUtil;

/**
 * Discovery component for Data Roles of a VDB
 * 
 */
public class DataRoleDiscoveryComponent implements ResourceDiscoveryComponent {

	private final Log log = LogFactory
			.getLog(PluginConstants.DEFAULT_LOGGER_CATEGORY);

	public Set<DiscoveredResourceDetails> discoverResources(
			ResourceDiscoveryContext discoveryContext)
			throws InvalidPluginConfigurationException, Exception {
		Set<DiscoveredResourceDetails> discoveredResources = new HashSet<DiscoveredResourceDetails>();
		VDBComponent parenComponent = (VDBComponent) discoveryContext
				.getParentResourceComponent();
		ProfileServiceConnection connection = parenComponent.getConnection();

		ManagedComponent mcVdb = ProfileServiceUtil.getManagedComponent(
				connection, new ComponentType(
						PluginConstants.ComponentType.VDB.TYPE,
						PluginConstants.ComponentType.VDB.SUBTYPE),
				parenComponent.name);

		// Get data roles from VDB
		ManagedProperty property = mcVdb.getProperty("dataPolicies");
		if (property != null) {
			CollectionValueSupport valueSupport = (CollectionValueSupport) property
					.getValue();
			MetaValue[] metaValues = valueSupport.getElements();

			for (MetaValue value : metaValues) {
				GenericValueSupport genValueSupport = (GenericValueSupport) value;
				ManagedObjectImpl managedObject = (ManagedObjectImpl) genValueSupport
						.getValue();

				String dataRoleName = ProfileServiceUtil.getSimpleValue(
						managedObject, "name", String.class);
				Boolean anyAuthenticated = ProfileServiceUtil.getSimpleValue(
						managedObject, "anyAuthenticated", Boolean.class);
				String description = ProfileServiceUtil.getSimpleValue(
						managedObject, "description", String.class);

				/**
				 * 
				 * A discovered resource must have a unique key, that must stay
				 * the same when the resource is discovered the next time
				 */
				DiscoveredResourceDetails detail = new DiscoveredResourceDetails(
						discoveryContext.getResourceType(), // ResourceType
						dataRoleName, // Resource Key
						dataRoleName, // Resource Name
						null, // Version
						PluginConstants.ComponentType.DATA_ROLE.DESCRIPTION, // Description
						discoveryContext.getDefaultPluginConfiguration(), // Plugin
						// Config
						null // Process info from a process scan
				);

				Configuration configuration = detail.getPluginConfiguration();

				configuration.put(new PropertySimple("name", dataRoleName));
				configuration.put(new PropertySimple("anyAuthenticated",
						anyAuthenticated));
				configuration
						.put(new PropertySimple("description", description));

				PropertyList mappedRoleNameList = new PropertyList(
						"mappedRoleNameList");
				configuration.put(mappedRoleNameList);
				ManagedProperty mappedRoleNames = managedObject
						.getProperty("mappedRoleNames");
				if (mappedRoleNames != null) {
					List<String> props = (List<String>) MetaValueFactory
							.getInstance().unwrap(mappedRoleNames.getValue());
					for (String mappedRoleName : props) {
						mappedRoleNameList.add(new PropertySimple("name", mappedRoleName));
					}

				}
				// Add to return values
				discoveredResources.add(detail);
				log.debug("Discovered Teiid VDB Data Role: " + dataRoleName);
			}
		}
		
		return discoveredResources;

	}
}