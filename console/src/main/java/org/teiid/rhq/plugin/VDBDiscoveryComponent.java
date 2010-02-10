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
import org.jboss.deployers.spi.management.ManagementView;
import org.jboss.managed.api.ComponentType;
import org.jboss.managed.api.ManagedComponent;
import org.jboss.managed.api.ManagedDeployment;
import org.jboss.managed.api.ManagedProperty;
import org.jboss.managed.plugins.ManagedObjectImpl;
import org.jboss.metatype.api.values.CollectionValueSupport;
import org.jboss.metatype.api.values.GenericValueSupport;
import org.jboss.metatype.api.values.MetaValue;
import org.jboss.metatype.api.values.SimpleValueSupport;
import org.rhq.core.domain.configuration.Configuration;
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
 * Discovery component for VDs
 * 
 */
public class VDBDiscoveryComponent implements ResourceDiscoveryComponent {

	private final Log log = LogFactory.getLog(this.getClass());

	public Set<DiscoveredResourceDetails> discoverResources(
			ResourceDiscoveryContext discoveryContext)
			throws InvalidPluginConfigurationException, Exception {
		Set<DiscoveredResourceDetails> discoveredResources = new HashSet<DiscoveredResourceDetails>();

		Set<ManagedComponent> vdbs = ProfileServiceUtil
				.getManagedComponents(new ComponentType(
						PluginConstants.ComponentType.VDB.TYPE,
						PluginConstants.ComponentType.VDB.SUBTYPE));

		for (ManagedComponent mcVdb : vdbs) {

			String vdbName = ((SimpleValueSupport) mcVdb.getProperty("name")
					.getValue()).getValue().toString();
//			ManagementView managementView = ProfileServiceUtil
//			.getManagementView(ProfileServiceUtil.getProfileService(),
//					false);
			//ManagedDeployment managedDeployment = managementView.getDeploymentNamesForType(arg0)(vdbName);
			//Set deploymentNames = null;
			
//			try
//	        {
//	            deploymentNames = managementView.getDeploymentNames();
//	        }
//	        catch (Exception e)
//	        {
//	            log.error("Unable to get deployment for type " , e);
//	        }
			String vdbVersion = ((SimpleValueSupport) mcVdb.getProperty(
					"version").getValue()).getValue().toString();
			// TODO: Correct this after deploying proper VDB/Metadata
			String vdbDescription = "description"; // mcVdb.getProperty("description");
			String vdbStatus = "active"; // mcVdb.getProperty("status");
			String vdbURL = "url"; // mcVdb.getProperty("url");

			/**
			 * 
			 * A discovered resource must have a unique key, that must stay the
			 * same when the resource is discovered the next time
			 */
			DiscoveredResourceDetails detail = new DiscoveredResourceDetails(
					discoveryContext.getResourceType(), // ResourceType
					vdbName, // Resource Key
					vdbName, // Resource Name
					vdbVersion, // Version
					PluginConstants.ComponentType.VDB.DESCRIPTION, // Description
					discoveryContext.getDefaultPluginConfiguration(), // Plugin
																		// Config
					null // Process info from a process scan
			);

			// Get plugin config map for models
			Configuration configuration = detail.getPluginConfiguration();

			configuration.put(new PropertySimple("name", vdbName));
			configuration.put(new PropertySimple("version", vdbVersion));
			configuration
					.put(new PropertySimple("description", vdbDescription));
			configuration.put(new PropertySimple("status", vdbStatus));
			configuration.put(new PropertySimple("url", vdbURL));

			getModels(mcVdb, configuration);

			detail.setPluginConfiguration(configuration);

			// Add to return values
			discoveredResources.add(detail);
			log.info("Discovered Teiid VDB: " + vdbName);
		}

		return discoveredResources;
	}

	/**
	 * @param mcVdb
	 * @param configuration
	 */
	private void getModels(ManagedComponent mcVdb, Configuration configuration) {
		// Get models from VDB
		ManagedProperty property = mcVdb.getProperty("models");
		CollectionValueSupport valueSupport = (CollectionValueSupport) property
				.getValue();
		MetaValue[] metaValues = valueSupport.getElements();

		PropertyList modelsList = new PropertyList("models");
		configuration.put(modelsList);

		for (MetaValue value : metaValues) {
			GenericValueSupport genValueSupport = (GenericValueSupport) value;
			ManagedObjectImpl managedObject = (ManagedObjectImpl) genValueSupport
					.getValue();
			String modelName = managedObject.getName();
			String type = ((SimpleValueSupport) managedObject.getProperty(
					"modelType").getValue()).getValue().toString();
			String visibility = ((SimpleValueSupport) managedObject
					.getProperty("visible").getValue()).getValue().toString();
			String path = ((SimpleValueSupport) managedObject.getProperty(
					"path").getValue()).getValue().toString();

			PropertyMap model = new PropertyMap("model", new PropertySimple(
					"name", modelName), new PropertySimple("type", type),
					new PropertySimple("path", path), new PropertySimple(
							"visibility", visibility));
			modelsList.add(model);
		}
	}

}
