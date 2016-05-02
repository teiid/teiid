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
import java.util.List;
import java.util.Set;

import javax.naming.NamingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.deployers.spi.management.ManagementView;
import org.jboss.managed.api.ComponentType;
import org.jboss.managed.api.ManagedComponent;
import org.jboss.managed.api.ManagedProperty;
import org.jboss.managed.plugins.ManagedObjectImpl;
import org.jboss.metatype.api.types.CollectionMetaType;
import org.jboss.metatype.api.types.MetaType;
import org.jboss.metatype.api.types.MetaTypeFactory;
import org.jboss.metatype.api.types.SimpleMetaType;
import org.jboss.metatype.api.values.CollectionValueSupport;
import org.jboss.metatype.api.values.GenericValueSupport;
import org.jboss.metatype.api.values.MetaValue;
import org.jboss.metatype.api.values.SimpleValueSupport;
import org.mc4j.ems.connection.EmsConnection;
import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.configuration.ConfigurationUpdateStatus;
import org.rhq.core.domain.configuration.Property;
import org.rhq.core.domain.configuration.PropertyList;
import org.rhq.core.domain.configuration.PropertyMap;
import org.rhq.core.domain.configuration.PropertySimple;
import org.rhq.core.domain.configuration.definition.ConfigurationDefinition;
import org.rhq.core.domain.configuration.definition.PropertyDefinition;
import org.rhq.core.domain.configuration.definition.PropertyDefinitionList;
import org.rhq.core.domain.configuration.definition.PropertyDefinitionMap;
import org.rhq.core.domain.measurement.AvailabilityType;
import org.rhq.core.domain.measurement.MeasurementReport;
import org.rhq.core.domain.measurement.MeasurementScheduleRequest;
import org.rhq.core.pluginapi.configuration.ConfigurationFacet;
import org.rhq.core.pluginapi.configuration.ConfigurationUpdateReport;
import org.rhq.core.pluginapi.inventory.CreateResourceReport;
import org.rhq.core.pluginapi.inventory.InvalidPluginConfigurationException;
import org.rhq.core.pluginapi.inventory.ResourceComponent;
import org.rhq.core.pluginapi.inventory.ResourceContext;
import org.rhq.plugins.jbossas5.connection.ProfileServiceConnection;
import org.teiid.rhq.plugin.adapter.api.PropertyAdapter;
import org.teiid.rhq.plugin.adapter.api.PropertyAdapterFactory;
import org.teiid.rhq.plugin.util.PluginConstants;
import org.teiid.rhq.plugin.util.ProfileServiceUtil;

/**
 * Component class for a Teiid VDB Data Role
 * 
 */
public class DataRoleComponent extends Facet {
	private final Log LOG = LogFactory
			.getLog(PluginConstants.DEFAULT_LOGGER_CATEGORY);

	/*
	 * (non-Javadoc)
	 * 
	 * @seeorg.teiid.rhq.plugin.Facet#start(org.rhq.core.pluginapi.inventory.
	 * ResourceContext)
	 */
	public void start(ResourceContext context) {
		this.resourceConfiguration = context.getPluginConfiguration();
		this.componentType = PluginConstants.ComponentType.DATA_ROLE.NAME;
		super.start(context);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.teiid.rhq.plugin.Facet#getAvailability()
	 */
	@Override
	public AvailabilityType getAvailability() {
		return ((VDBComponent) this.resourceContext
				.getParentResourceComponent()).getAvailability();
	}

	@Override
	String getComponentType() {
		return PluginConstants.ComponentType.DATA_ROLE.NAME;
	}

	@Override
	public void getValues(MeasurementReport arg0,
			Set<MeasurementScheduleRequest> arg1) throws Exception {
		// TODO Auto-generated method stub

	}

	/**
	 * The plugin container will call this method when it has a new
	 * configuration for your managed resource. Your plugin will re-configure
	 * the managed resource in your own custom way, setting its configuration
	 * based on the new values of the given configuration.
	 * 
	 * @see ConfigurationFacet#updateResourceConfiguration(ConfigurationUpdateReport)
	 */
	public void updateResourceConfiguration(ConfigurationUpdateReport report) {

		Configuration resourceConfig = report.getConfiguration();
		resourceConfiguration = resourceConfig.deepCopy();

		// Get the vdb and update date role anyAuthenticated and MappedRoleNames
		ManagementView managementView = null;
		ComponentType componentType = new ComponentType(
				PluginConstants.ComponentType.VDB.TYPE,
				PluginConstants.ComponentType.VDB.SUBTYPE);

		ManagedComponent managedComponent = null;
		ManagedProperty anyAuthenticatedMp = null;
		report.setStatus(ConfigurationUpdateStatus.SUCCESS);
		try {

			managementView = getConnection().getManagementView();
			managedComponent = managementView.getComponent(
					((VDBComponent) this.resourceContext
							.getParentResourceComponent()).name, componentType);
			ManagedProperty mp = managedComponent.getProperty("dataPolicies");//$NON-NLS-1$
			CollectionValueSupport dataRolesListMp = (CollectionValueSupport) mp
					.getValue();
			String name = resourceConfiguration.getSimpleValue("name", null); //$NON-NLS-1$
			String anyAuthenticated = resourceConfiguration.getSimpleValue(
					"anyAuthenticated", null); //$NON-NLS-1$

			for (MetaValue val : dataRolesListMp.getElements()) {
				GenericValueSupport genValueSupport = (GenericValueSupport) val;
				ManagedObjectImpl managedObject = (ManagedObjectImpl) genValueSupport
						.getValue();

				for (String dataRolesProp : managedObject.getPropertyNames()) {
					ManagedProperty property = managedObject
							.getProperty(dataRolesProp);

					String pname = ProfileServiceUtil.stringValue(managedObject
							.getProperty("name").getValue()); //$NON-NLS-1$
					if (!pname.equals(name)) {
						continue;
					}

					anyAuthenticatedMp = managedObject
							.getProperty("anyAuthenticated"); //$NON-NLS-1$
					anyAuthenticatedMp.setValue(ProfileServiceUtil.wrap(
							SimpleMetaType.BOOLEAN, anyAuthenticated));
					List<Property> mappedRoleNamePropertyList = resourceConfiguration.getList("mappedRoleNameList").getList(); //$NON-NLS-1$
					List<String> mappedRoleNameList = new ArrayList<String>();
					
					for (Property mappedRoleNameProperty : mappedRoleNamePropertyList){
						String mappedRoleNameString = ((PropertyMap)mappedRoleNameProperty).getSimpleValue("name", null); //$NON-NLS-1$
						mappedRoleNameList.add(mappedRoleNameString);
					}
					ManagedProperty mappedRoleNameMp = managedObject.getProperty("mappedRoleNames"); //$NON-NLS-1$
					mappedRoleNameMp.setValue(convertListOfStringsToMetaValue(mappedRoleNameList));
				}

				try {
					managementView.updateComponent(managedComponent);
					managementView.load();
				} catch (Exception e) {
					LOG.error("Unable to update component [" //$NON-NLS-1$
							+ managedComponent.getName() + "] of type " //$NON-NLS-1$
							+ componentType + ".", e); //$NON-NLS-1$
					report.setStatus(ConfigurationUpdateStatus.FAILURE);
					report.setErrorMessageFromThrowable(e);
				}
			}
		} catch (Exception e) {
			LOG.error("Unable to process update request", e); //$NON-NLS-1$
			report.setStatus(ConfigurationUpdateStatus.FAILURE);
			report.setErrorMessageFromThrowable(e);
		}

	}

	/**
	 * @param mappedRoleNameList
	 */
	private MetaValue convertListOfStringsToMetaValue(List<String> mappedRoleNameList) {
		 
		 MetaValue[] listMemberValues = new MetaValue[mappedRoleNameList.size()];
		 int memberIndex = 0;
		 
		 for (String mappedRoleName : mappedRoleNameList)
	     {
	      MetaValue mappedRoleNameValue = SimpleValueSupport.wrap(mappedRoleName);
	      listMemberValues[memberIndex++] = mappedRoleNameValue;
	     }
	     return new CollectionValueSupport( new CollectionMetaType("java.util.List", SimpleMetaType.STRING), //$NON-NLS-1$
	    		                            listMemberValues);
	     
	}

	@Override
	public Configuration loadResourceConfiguration() {

		VDBComponent parentComponent = (VDBComponent) this.resourceContext
				.getParentResourceComponent();
		ManagedComponent mcVdb = null;
		Configuration configuration = resourceContext.getPluginConfiguration();
		try {
			mcVdb = ProfileServiceUtil.getManagedComponent(getConnection(),
					new ComponentType(PluginConstants.ComponentType.VDB.TYPE,
							PluginConstants.ComponentType.VDB.SUBTYPE),
					parentComponent.name);
		} catch (NamingException e) {
			final String msg = "NamingException in loadResourceConfiguration(): " + e.getMessage(); //$NON-NLS-1$
			LOG.error(msg, e);
		} catch (Exception e) {
			final String msg = "Exception in loadResourceConfiguration(): " + e.getMessage(); //$NON-NLS-1$
			LOG.error(msg, e);
		}

		// Get data roles from VDB
		ManagedProperty property = mcVdb.getProperty("dataPolicies"); //$NON-NLS-1$
		if (property != null) {
			CollectionValueSupport valueSupport = (CollectionValueSupport) property
					.getValue();
			MetaValue[] metaValues = valueSupport.getElements();

			for (MetaValue value : metaValues) {
				GenericValueSupport genValueSupport = (GenericValueSupport) value;
				ManagedObjectImpl managedObject = (ManagedObjectImpl) genValueSupport
						.getValue();

				String dataRoleName = ProfileServiceUtil.getSimpleValue(
						managedObject, "name", String.class); //$NON-NLS-1$
				Boolean anyAuthenticated = ProfileServiceUtil.getSimpleValue(
						managedObject, "anyAuthenticated", Boolean.class); //$NON-NLS-1$
				String description = ProfileServiceUtil.getSimpleValue(
						managedObject, "description", String.class); //$NON-NLS-1$

				configuration.put(new PropertySimple("name", dataRoleName)); //$NON-NLS-1$
				configuration.put(new PropertySimple("anyAuthenticated", //$NON-NLS-1$ 
						anyAuthenticated));
				configuration
						.put(new PropertySimple("description", description)); //$NON-NLS-1$

				PropertyList mappedRoleNameList = new PropertyList(
						"mappedRoleNameList"); //$NON-NLS-1$
				configuration.put(mappedRoleNameList);
				ManagedProperty mappedRoleNames = managedObject
						.getProperty("mappedRoleNames"); //$NON-NLS-1$
				if (mappedRoleNames != null) {
					CollectionValueSupport props = (CollectionValueSupport) mappedRoleNames
							.getValue();
					for (MetaValue mappedRoleName : props.getElements()) {
						PropertyMap mappedRoleNameMap = null;

						try {
							mappedRoleNameMap = new PropertyMap(
									"map", //$NON-NLS-1$
									new PropertySimple(
											"name", (ProfileServiceUtil.stringValue(mappedRoleName)))); //$NON-NLS-1$
						} catch (Exception e) {
							final String msg = "Exception in loadResourceConfiguration(): " + e.getMessage(); //$NON-NLS-1$
							LOG.error(msg, e);
						}
						mappedRoleNameList.add(mappedRoleNameMap);
					}
				}
			}
		}

		return configuration;

	}

	@Override
	public CreateResourceReport createResource(
			CreateResourceReport createResourceReport) {

		createContentBasedResource(createResourceReport);
		return createResourceReport;
	}

	@Override
	public ProfileServiceConnection getConnection() {
		return ((VDBComponent) this.resourceContext
				.getParentResourceComponent()).getConnection();
	}

	@Override
	public EmsConnection getEmsConnection() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResourceContext getResourceContext() {
		// TODO Auto-generated method stub
		return this.getResourceContext();
	}


}
