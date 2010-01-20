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

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.configuration.ConfigurationUpdateStatus;
import org.rhq.core.domain.content.PackageType;
import org.rhq.core.domain.content.transfer.DeployPackageStep;
import org.rhq.core.domain.content.transfer.DeployPackagesResponse;
import org.rhq.core.domain.content.transfer.RemovePackagesResponse;
import org.rhq.core.domain.content.transfer.ResourcePackageDetails;
import org.rhq.core.domain.measurement.AvailabilityType;
import org.rhq.core.domain.measurement.MeasurementReport;
import org.rhq.core.domain.measurement.MeasurementScheduleRequest;
import org.rhq.core.pluginapi.configuration.ConfigurationFacet;
import org.rhq.core.pluginapi.configuration.ConfigurationUpdateReport;
import org.rhq.core.pluginapi.content.ContentFacet;
import org.rhq.core.pluginapi.content.ContentServices;
import org.rhq.core.pluginapi.inventory.CreateChildResourceFacet;
import org.rhq.core.pluginapi.inventory.CreateResourceReport;
import org.rhq.core.pluginapi.inventory.DeleteResourceFacet;
import org.rhq.core.pluginapi.inventory.InvalidPluginConfigurationException;
import org.rhq.core.pluginapi.inventory.ResourceComponent;
import org.rhq.core.pluginapi.inventory.ResourceContext;
import org.rhq.core.pluginapi.measurement.MeasurementFacet;
import org.rhq.core.pluginapi.operation.OperationFacet;
import org.rhq.core.pluginapi.operation.OperationResult;
import org.teiid.rhq.admin.utils.SingletonConnectionManager;
import org.teiid.rhq.comm.Component;
import org.teiid.rhq.comm.Connection;
import org.teiid.rhq.comm.ConnectionException;
import org.teiid.rhq.comm.ExecutedResult;
import org.teiid.rhq.comm.VMComponent;
import org.teiid.rhq.plugin.objects.ExecutedOperationResultImpl;


/**
 * This class implements required RHQ interfaces and provides common logic used
 * by all MetaMatrix components.
 */
public abstract class Facet implements ResourceComponent,
		MeasurementFacet, OperationFacet, ConfigurationFacet, ContentFacet,
		DeleteResourceFacet, CreateChildResourceFacet {

	protected static SingletonConnectionManager connMgr = SingletonConnectionManager
			.getInstance();

	protected final Log LOG = LogFactory
			.getLog(Facet.class);
	
	/**
	 * Represents the resource configuration of the custom product being
	 * managed.
	 */
	protected Configuration resourceConfiguration;


	/**
	 * All AMPS plugins are stateful - this context contains information that
	 * your resource component can use when performing its processing.
	 */
	protected ResourceContext<?> resourceContext;

	private String systemKey;

	private String name;

	private String identifier;

	// may be null when the component is not a host or vm
	private String port = null;

	protected boolean isAvailable = false;

	abstract String getComponentType();

	/**
	 * This is called when your component has been started with the given
	 * context. You normally initialize some internal state of your component as
	 * well as attempt to make a stateful connection to your managed resource.
	 * 
	 * @see ResourceComponent#start(ResourceContext)
	 */
	public void start(ResourceContext context) {
		resourceContext = context;
	}

	/**
	 * This is called when the component is being stopped, usually due to the
	 * plugin container shutting down. You can perform some cleanup here; though
	 * normally not much needs to be done here.
	 * 
	 * @see ResourceComponent#stop()
	 */
	public void stop() {
		this.isAvailable = false;
	}
	
	/**
	 * @return the resourceConfiguration
	 */
	public Configuration getResourceConfiguration() {
		return resourceConfiguration;
	}

	/**
	 * @param resourceConfiguration the resourceConfiguration to set
	 */
	public void setResourceConfiguration(Configuration resourceConfiguration) {
		this.resourceConfiguration = resourceConfiguration;
	}

	public String getSystemKey() {
		return systemKey;
	}

	public String getComponentName() {
		return name;
	}

	protected void setComponentName(String componentName) {
		this.name = componentName;
	}

	public String getPort() {
		return port;
	}

	public String getComponentIdentifier() {
		return identifier;
	}

	protected void setComponentIdentifier(String identifier) {
		this.identifier = identifier;
	}

	protected void setOperationArguments(String name, Configuration configuration,
			Map argumentMap) {
// moved this logic up to the associated implemented class
		throw new InvalidPluginConfigurationException("Not implemented on component type " + this.getComponentType() + " named " + this.getComponentName());
				 
		
	}

	protected void execute(final ExecutedResult result,
			final Map valueMap) {
		Connection conn = null;
		try {
			conn = getConnection();

			if (!conn.isValid()) {
				return;
			}
			conn.executeOperation(result, valueMap);

		} catch (Exception e) {
			final String msg = "Error invoking operation [" + name + "]. Cause: " + e; //$NON-NLS-1$ //$NON-NLS-2$
			LOG.error(msg);
			throw new RuntimeException(msg);
		} finally {
			conn.close();
		}
	}

	/**
	 * The connection will be returned when it is available. If it is not, then
	 * null will be returned. Resource methods will not throw exceptions as a
	 * MetaMatrix System goes up and down. That state will be indicated by it's
	 * availability state.
	 * 
	 * @return
	 */

	public Connection getConnection() throws ConnectionException {
		Connection conn = connMgr.getConnection(getSystemKey());
		if (conn.isValid()) {
			this.isAvailable = true;
		} else {
			this.isAvailable = false;
		}

		return conn;
	}

	/*
	 * (non-Javadoc)
	 * This method is called by JON to check the availability of the inventoried component on a time scheduled basis
	 * 
	 * @see org.rhq.core.pluginapi.inventory.ResourceComponent#getAvailability()
	 */
	public AvailabilityType getAvailability() {

		LOG.debug("Checking availability of  " + identifier); //$NON-NLS-1$
		
		return AvailabilityType.UP;
	}

	/**
	 * Helper method that indicates the latest status based on the last
	 * getAvailabilit() call.
	 * 
	 * @return true if the resource is available
	 */
	protected boolean isAvailable() {
		return this.isAvailable;
	}

	/**
	 * The plugin container will call this method when your resource component
	 * has been scheduled to collect some measurements now. It is within this
	 * method that you actually talk to the managed resource and collect the
	 * measurement data that is has emitted.
	 * 
	 * @see MeasurementFacet#getValues(MeasurementReport, Set)
	 */
	public abstract void getValues(MeasurementReport arg0,
			Set<MeasurementScheduleRequest> arg1) throws Exception;

	/**
	 * The plugin container will call this method when it wants to invoke an
	 * operation on your managed resource. Your plugin will connect to the
	 * managed resource and invoke the analogous operation in your own custom
	 * way.
	 * 
	 * @see OperationFacet#invokeOperation(String, Configuration)
	 */
	public OperationResult invokeOperation(String name,
			Configuration configuration) {
		Map valueMap = new HashMap();
		Connection conn = null;

		Set operationDefinitionSet = this.resourceContext.getResourceType()
				.getOperationDefinitions();


		ExecutedResult result = new ExecutedOperationResultImpl(
				this.getComponentType(),
				name, 
				operationDefinitionSet);
			
		setOperationArguments(name, configuration, valueMap);
		
		execute(result, valueMap);
		
		return ((ExecutedOperationResultImpl) result).getOperationResult();

	}

	/**
	 * The plugin container will call this method and it needs to obtain the
	 * current configuration of the managed resource. Your plugin will obtain
	 * the managed resource's configuration in your own custom way and populate
	 * the returned Configuration object with the managed resource's
	 * configuration property values.
	 * 
	 * @see ConfigurationFacet#loadResourceConfiguration()
	 */
	public Configuration loadResourceConfiguration() {
		// here we simulate the loading of the managed resource's configuration

		if (resourceConfiguration == null) {
			// for this example, we will create a simple dummy configuration to
			// start with.
			// note that it is empty, so we're assuming there are no required
			// configs in the plugin descriptor.
			resourceConfiguration = this.resourceContext.getPluginConfiguration();
		}
		
		Configuration config = resourceConfiguration;

		return config;
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
		// this simulates the plugin taking the new configuration and
		// reconfiguring the managed resource
		resourceConfiguration = report.getConfiguration().deepCopy();

		report.setStatus(ConfigurationUpdateStatus.SUCCESS);
	}

	/**
	 * When this is called, the plugin is responsible for scanning its managed
	 * resource and look for content that need to be managed for that resource.
	 * This method should only discover packages of the given package type.
	 * 
	 * @see ContentFacet#discoverDeployedPackages(PackageType)
	 */
	public Set<ResourcePackageDetails> discoverDeployedPackages(PackageType type) {
		return null;
	}

	/**
	 * The plugin container calls this method when new packages need to be
	 * deployed/installed on resources.
	 * 
	 * @see ContentFacet#deployPackages(Set, ContentServices)
	 */
	public DeployPackagesResponse deployPackages(
			Set<ResourcePackageDetails> packages,
			ContentServices contentServices) {
		return null;
	}

	/**
	 * When a remote client wants to see the actual data content for an
	 * installed package, this method will be called. This method must return a
	 * stream of data containing the full content of the package.
	 * 
	 * @see ContentFacet#retrievePackageBits(ResourcePackageDetails)
	 */
	public InputStream retrievePackageBits(ResourcePackageDetails packageDetails) {
		return null;
	}

	/**
	 * This is the method that is used when the component has to create the
	 * installation steps and their results.
	 * 
	 * @see ContentFacet#generateInstallationSteps(ResourcePackageDetails)
	 */
	public List<DeployPackageStep> generateInstallationSteps(
			ResourcePackageDetails packageDetails) {
		return null;
	}

	/**
	 * This is called when the actual content of packages should be deleted from
	 * the managed resource.
	 * 
	 * @see ContentFacet#removePackages(Set)
	 */
	public RemovePackagesResponse removePackages(
			Set<ResourcePackageDetails> packages) {
		return null;
	}

	/**
	 * When called, the plugin container is asking the plugin to create a new
	 * managed resource. The new resource's details need to be added to the
	 * given report.
	 * 
	 * @see CreateChildResourceFacet#createResource(CreateResourceReport)
	 */
	public CreateResourceReport createResource(CreateResourceReport report) {
		return null;
	}

	/**
	 * When called, the plugin container is asking the plugin to delete a
	 * managed resource.
	 * 
	 * @see DeleteResourceFacet#deleteResource()
	 */
	public void deleteResource() {
	}
	
	
}
