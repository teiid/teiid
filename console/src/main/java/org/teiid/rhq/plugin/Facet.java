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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.deployers.spi.management.ManagementView;
import org.jboss.deployers.spi.management.deploy.DeploymentManager;
import org.jboss.deployers.spi.management.deploy.DeploymentProgress;
import org.jboss.deployers.spi.management.deploy.DeploymentStatus;
import org.jboss.managed.api.ComponentType;
import org.jboss.managed.api.DeploymentTemplateInfo;
import org.jboss.managed.api.ManagedComponent;
import org.jboss.managed.api.ManagedProperty;
import org.jboss.profileservice.spi.NoSuchDeploymentException;
import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.configuration.ConfigurationUpdateStatus;
import org.rhq.core.domain.configuration.Property;
import org.rhq.core.domain.configuration.PropertySimple;
import org.rhq.core.domain.configuration.definition.ConfigurationTemplate;
import org.rhq.core.domain.content.PackageDetailsKey;
import org.rhq.core.domain.content.PackageType;
import org.rhq.core.domain.content.transfer.ContentResponseResult;
import org.rhq.core.domain.content.transfer.DeployIndividualPackageResponse;
import org.rhq.core.domain.content.transfer.DeployPackageStep;
import org.rhq.core.domain.content.transfer.DeployPackagesResponse;
import org.rhq.core.domain.content.transfer.RemovePackagesResponse;
import org.rhq.core.domain.content.transfer.ResourcePackageDetails;
import org.rhq.core.domain.measurement.AvailabilityType;
import org.rhq.core.domain.measurement.MeasurementReport;
import org.rhq.core.domain.measurement.MeasurementScheduleRequest;
import org.rhq.core.domain.resource.CreateResourceStatus;
import org.rhq.core.domain.resource.ResourceType;
import org.rhq.core.pluginapi.configuration.ConfigurationFacet;
import org.rhq.core.pluginapi.configuration.ConfigurationUpdateReport;
import org.rhq.core.pluginapi.content.ContentFacet;
import org.rhq.core.pluginapi.content.ContentServices;
import org.rhq.core.pluginapi.content.version.PackageVersions;
import org.rhq.core.pluginapi.inventory.CreateChildResourceFacet;
import org.rhq.core.pluginapi.inventory.CreateResourceReport;
import org.rhq.core.pluginapi.inventory.DeleteResourceFacet;
import org.rhq.core.pluginapi.inventory.InvalidPluginConfigurationException;
import org.rhq.core.pluginapi.inventory.ResourceComponent;
import org.rhq.core.pluginapi.inventory.ResourceContext;
import org.rhq.core.pluginapi.measurement.MeasurementFacet;
import org.rhq.core.pluginapi.operation.OperationFacet;
import org.rhq.core.pluginapi.operation.OperationResult;
import org.rhq.core.util.exception.ThrowableUtil;
import org.rhq.plugins.jbossas5.ProfileServiceComponent;
import org.rhq.plugins.jbossas5.connection.ProfileServiceConnection;
import org.teiid.rhq.admin.DQPManagementView;
import org.teiid.rhq.plugin.deployer.Deployer;
import org.teiid.rhq.plugin.deployer.RemoteDeployer;
import org.teiid.rhq.plugin.objects.ExecutedOperationResultImpl;
import org.teiid.rhq.plugin.objects.ExecutedResult;
import org.teiid.rhq.plugin.util.DeploymentUtils;
import org.teiid.rhq.plugin.util.PluginConstants;
import org.teiid.rhq.plugin.util.ProfileServiceUtil;
import org.teiid.rhq.plugin.util.PluginConstants.Operation;

/**
 * This class implements required RHQ interfaces and provides common logic used
 * by all MetaMatrix components.
 */
public abstract class Facet implements
		ProfileServiceComponent<ResourceComponent>, MeasurementFacet,
		OperationFacet, ConfigurationFacet, ContentFacet, DeleteResourceFacet,
		CreateChildResourceFacet {

	protected final Log LOG = LogFactory
			.getLog(PluginConstants.DEFAULT_LOGGER_CATEGORY);

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

	protected String name;

	private String identifier;

	protected String componentType;

	protected boolean isAvailable = false;

	private final Log log = LogFactory.getLog(this.getClass());

	private File deploymentFile;
	private static final String BACKUP_FILE_EXTENSION = ".rej"; //$NON-NLS-1$

	/**
	 * The name of the ManagedDeployment (e.g.:
	 * C:/opt/jboss-5.0.0.GA/server/default/deploy/foo.vdb).
	 */
	protected String deploymentName;
	protected String deploymentUrl;

	private PackageVersions versions = null;

	/**
	 * Name of the backing package type that will be used when discovering
	 * packages. This corresponds to the name of the package type defined in the
	 * plugin descriptor. For simplicity, the package type for VDBs is called
	 * "vdb". This is still unique within the context of the parent resource
	 * type and lets this class use the same package type name in both cases.
	 */
	private static final String PKG_TYPE_VDB = "vdb"; //$NON-NLS-1$

	/**
	 * Architecture string used in describing discovered packages.
	 */
	private static final String ARCHITECTURE = "noarch"; //$NON-NLS-1$

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
		deploymentName = context.getResourceKey();
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
	 * @param resourceConfiguration
	 *            the resourceConfiguration to set
	 */
	public void setResourceConfiguration(Configuration resourceConfiguration) {
		this.resourceConfiguration = resourceConfiguration;
	}

	public String componentType() {
		return name;
	}

	protected void setComponentName(String componentName) {
		this.name = componentName;
	}

	public String getComponentIdentifier() {
		return identifier;
	}

	protected void setComponentIdentifier(String identifier) {
		this.identifier = identifier;
	}

	protected void setOperationArguments(String name,
			Configuration configuration, Map<String, Object> argumentMap) {
		// moved this logic up to the associated implemented class
		throw new InvalidPluginConfigurationException(
				"Not implemented on component type " + this.getComponentType() //$NON-NLS-1$
						+ " named " + this.name); //$NON-NLS-1$

	}

	protected void setMetricArguments(String name, Configuration configuration,
			Map<String, Object> argumentMap) {
		// moved this logic up to the associated implemented class
		throw new InvalidPluginConfigurationException(
				"Not implemented on component type " + this.getComponentType() //$NON-NLS-1$
						+ " named " + this.name); //$NON-NLS-1$

	}

	protected void execute(final ProfileServiceConnection connection,
			final ExecutedResult result, final Map<String, Object> valueMap) {
		DQPManagementView dqp = new DQPManagementView();

		try {
			dqp.executeOperation(connection, result, valueMap);
		} catch (Exception e) {
			new RuntimeException(e);
		}

	}

	/*
	 * (non-Javadoc) This method is called by JON to check the availability of
	 * the inventoried component on a time scheduled basis
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
		return true;
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
		Map<String, Object> valueMap = new HashMap<String, Object>();

		Set operationDefinitionSet = this.resourceContext.getResourceType()
				.getOperationDefinitions();

		ExecutedResult result = new ExecutedOperationResultImpl(this
				.getComponentType(), name, operationDefinitionSet);

		setOperationArguments(name, configuration, valueMap);

		execute(getConnection(), result, valueMap);

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
			resourceConfiguration = this.resourceContext
					.getPluginConfiguration();
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

		resourceConfiguration = report.getConfiguration().deepCopy();

		Configuration resourceConfig = report.getConfiguration();

		ManagementView managementView = null;
		ComponentType componentType = null;
		if (this.getComponentType().equals(
				PluginConstants.ComponentType.VDB.NAME)) {
			componentType = new ComponentType(
					PluginConstants.ComponentType.VDB.TYPE,
					PluginConstants.ComponentType.VDB.SUBTYPE);
		} else {
			report.setStatus(ConfigurationUpdateStatus.FAILURE);
			report
					.setErrorMessage("Update not implemented for the component type."); //$NON-NLS-1$
		}

		ManagedComponent managedComponent = null;
		report.setStatus(ConfigurationUpdateStatus.SUCCESS);
		try {

			managementView = getConnection().getManagementView();
			managedComponent = managementView.getComponent(this.name,
					componentType);
			Map<String, ManagedProperty> managedProperties = managedComponent
					.getProperties();

			ProfileServiceUtil.convertConfigurationToManagedProperties(managedProperties, resourceConfig, resourceContext.getResourceType(), null);

			try {
				managementView.updateComponent(managedComponent);
			} catch (Exception e) {
				LOG.error("Unable to update component [" //$NON-NLS-1$
						+ managedComponent.getName() + "] of type " //$NON-NLS-1$
						+ componentType + ".", e); //$NON-NLS-1$
				report.setStatus(ConfigurationUpdateStatus.FAILURE);
				report.setErrorMessageFromThrowable(e);
			}
		} catch (Exception e) {
			LOG.error("Unable to process update request", e); //$NON-NLS-1$
			report.setStatus(ConfigurationUpdateStatus.FAILURE);
			report.setErrorMessageFromThrowable(e);
		}
	}

	/**
	 * @return
	 * @throws Exception
	 */
	protected Map<String, ManagedProperty> getManagedProperties()
			throws Exception {
		return null;
	}

	/**
	 * @param managedComponent
	 * @throws Exception
	 */
	protected void updateComponent(ManagedComponent managedComponent)
			throws Exception {
		log.trace("Updating " + this.name + " with component " //$NON-NLS-1$ //$NON-NLS-2$
				+ managedComponent.toString() + "..."); //$NON-NLS-1$
		ManagementView managementView = getConnection().getManagementView();
		managementView.updateComponent(managedComponent);

	}

	@Override
	public void deleteResource() throws Exception {

		DeploymentManager deploymentManager = getConnection()
				.getDeploymentManager();
		
		log.debug("Stopping deployment [" + this.deploymentUrl + "]..."); //$NON-NLS-1$ //$NON-NLS-2$
		DeploymentProgress progress = deploymentManager
				.stop(this.deploymentUrl);
		DeploymentStatus stopStatus = DeploymentUtils.run(progress);
		if (stopStatus.isFailed()) {
			log.error("Failed to stop deployment '" + this.deploymentUrl //$NON-NLS-1$
					+ "'.", stopStatus.getFailure()); //$NON-NLS-1$
			throw new Exception("Failed to stop deployment '" //$NON-NLS-1$
					+ this.deploymentName + "' - cause: " //$NON-NLS-1$
					+ stopStatus.getFailure());
		}
		log.debug("Removing deployment [" + this.deploymentUrl + "]..."); //$NON-NLS-1$ //$NON-NLS-2$
		progress = deploymentManager.remove(this.deploymentUrl);
		DeploymentStatus removeStatus = DeploymentUtils.run(progress);
		if (removeStatus.isFailed()) {
			log.error("Failed to remove deployment '" + this.deploymentUrl //$NON-NLS-1$
					+ "'.", removeStatus.getFailure()); //$NON-NLS-1$
			throw new Exception("Failed to remove deployment '" //$NON-NLS-1$
					+ this.deploymentName + "' - cause: " //$NON-NLS-1$
					+ removeStatus.getFailure());
		}

	}

	@Override
	public DeployPackagesResponse deployPackages(
			Set<ResourcePackageDetails> packages,
			ContentServices contentServices) {
		// You can only update the one application file referenced by this
		// resource, so punch out if multiple are
		// specified.
		if (packages.size() != 1) {
			log
					.warn("Request to update a VDB file contained multiple packages: " //$NON-NLS-1$
							+ packages);
			DeployPackagesResponse response = new DeployPackagesResponse(
					ContentResponseResult.FAILURE);
			response
					.setOverallRequestErrorMessage("When updating a VDB, only one VDB can be updated at a time."); //$NON-NLS-1$
			return response;
		}

		ResourcePackageDetails packageDetails = packages.iterator().next();

		log.debug("Updating VDB file '" + this.deploymentFile + "' using [" //$NON-NLS-1$ //$NON-NLS-2$
				+ packageDetails + "]..."); //$NON-NLS-1$

		log.debug("Writing new VDB bits to temporary file..."); //$NON-NLS-1$
		File tempFile;
		try {
			tempFile = writeNewAppBitsToTempFile(contentServices,
					packageDetails);
		} catch (Exception e) {
			return failApplicationDeployment(
					"Error writing new application bits to temporary file - cause: " //$NON-NLS-1$
							+ e, packageDetails);
		}
		log.debug("Wrote new VDB bits to temporary file '" + tempFile //$NON-NLS-1$
				+ "'."); //$NON-NLS-1$

		boolean deployExploded = this.deploymentFile.isDirectory();

		// Backup the original app file/dir to <filename>.rej.
		File backupOfOriginalFile = new File(this.deploymentFile.getPath()
				+ BACKUP_FILE_EXTENSION);
		log.debug("Backing up existing VDB '" + this.deploymentFile //$NON-NLS-1$
				+ "' to '" + backupOfOriginalFile + "'..."); //$NON-NLS-1$ //$NON-NLS-2$
		try {
			if (backupOfOriginalFile.exists())
				FileUtils.forceDelete(backupOfOriginalFile);
			if (this.deploymentFile.isDirectory())
				FileUtils.copyDirectory(this.deploymentFile,
						backupOfOriginalFile, true);
			else
				FileUtils.copyFile(this.deploymentFile, backupOfOriginalFile,
						true);
		} catch (Exception e) {
			throw new RuntimeException("Failed to backup existing EAR/WAR '" //$NON-NLS-1$
					+ this.deploymentFile + "' to '" + backupOfOriginalFile //$NON-NLS-1$
					+ "'."); //$NON-NLS-1$
		}

		// Now stop the original app.
		try {
			DeploymentManager deploymentManager = getConnection()
					.getDeploymentManager();
			DeploymentProgress progress = deploymentManager
					.stop(this.deploymentUrl);
			DeploymentUtils.run(progress);
		} catch (Exception e) {
			throw new RuntimeException("Failed to stop deployment [" //$NON-NLS-1$
					+ this.deploymentUrl + "].", e); //$NON-NLS-1$
		}

		// And then remove it (this will delete the physical file/dir from the
		// deploy dir).
		try {
			DeploymentManager deploymentManager = getConnection()
					.getDeploymentManager();
			DeploymentProgress progress = deploymentManager
					.remove(this.deploymentUrl);
			DeploymentUtils.run(progress);
		} catch (Exception e) {
			throw new RuntimeException("Failed to remove deployment [" //$NON-NLS-1$
					+ this.deploymentUrl + "].", e); //$NON-NLS-1$
		}

		// Deploy away!
		log.debug("Deploying '" + tempFile + "'..."); //$NON-NLS-1$ //$NON-NLS-2$
		DeploymentManager deploymentManager = getConnection()
				.getDeploymentManager();
		try {
			DeploymentUtils.deployArchive(deploymentManager, tempFile,
					deployExploded);
		} catch (Exception e) {
			// Deploy failed - rollback to the original app file...
			log.debug("Redeploy failed - rolling back to original archive...", //$NON-NLS-1$
					e);
			String errorMessage = ThrowableUtil.getAllMessages(e);
			try {
				// Delete the new app, which failed to deploy.
				FileUtils.forceDelete(this.deploymentFile);
				// Need to re-deploy the original file - this generally should
				// succeed.
				DeploymentUtils.deployArchive(deploymentManager,
						backupOfOriginalFile, deployExploded);
				errorMessage += " ***** ROLLED BACK TO ORIGINAL APPLICATION FILE. *****"; //$NON-NLS-1$
			} catch (Exception e1) {
				log.debug("Rollback failed!", e1); //$NON-NLS-1$
				errorMessage += " ***** FAILED TO ROLLBACK TO ORIGINAL APPLICATION FILE. *****: " //$NON-NLS-1$
						+ ThrowableUtil.getAllMessages(e1);
			}
			log.info("Failed to update VDB file '" + this.deploymentFile //$NON-NLS-1$
					+ "' using [" + packageDetails + "]."); //$NON-NLS-1$ //$NON-NLS-2$
			return failApplicationDeployment(errorMessage, packageDetails);
		}

		// Deploy was successful!

		deleteBackupOfOriginalFile(backupOfOriginalFile);
		persistApplicationVersion(packageDetails, this.deploymentFile);

		DeployPackagesResponse response = new DeployPackagesResponse(
				ContentResponseResult.SUCCESS);
		DeployIndividualPackageResponse packageResponse = new DeployIndividualPackageResponse(
				packageDetails.getKey(), ContentResponseResult.SUCCESS);
		response.addPackageResponse(packageResponse);

		log.debug("Updated VDB file '" + this.deploymentFile //$NON-NLS-1$
				+ "' successfully - returning response [" + response + "]..."); //$NON-NLS-1$ //$NON-NLS-2$

		return response;
	}

	private void deleteBackupOfOriginalFile(File backupOfOriginalFile) {
		log.debug("Deleting backup of original file '" + backupOfOriginalFile //$NON-NLS-1$
				+ "'..."); //$NON-NLS-1$
		try {
			FileUtils.forceDelete(backupOfOriginalFile);
		} catch (Exception e) {
			// not critical.
			log.warn("Failed to delete backup of original file: " //$NON-NLS-1$
					+ backupOfOriginalFile);
		}
	}
	
	private void persistApplicationVersion(ResourcePackageDetails packageDetails, File appFile)
    {
        String packageName = appFile.getName();
        log.debug("Persisting application version '" + packageDetails.getVersion() + "' for package '" + packageName //$NON-NLS-1$ //$NON-NLS-2$
                + "'"); //$NON-NLS-1$
        PackageVersions versions = loadPackageVersions();
        versions.putVersion(packageName, packageDetails.getVersion());
    }

	private File writeNewAppBitsToTempFile(ContentServices contentServices,
			ResourcePackageDetails packageDetails) throws Exception {
		File tempDir = this.resourceContext.getTemporaryDirectory();
		File tempFile = new File(tempDir, this.deploymentFile.getName());

		OutputStream tempOutputStream = null;
		try {
			tempOutputStream = new BufferedOutputStream(new FileOutputStream(
					tempFile));
			long bytesWritten = contentServices.downloadPackageBits(
					this.resourceContext.getContentContext(), packageDetails
							.getKey(), tempOutputStream, true);
			log
					.debug("Wrote " + bytesWritten + " bytes to '" + tempFile //$NON-NLS-1$ //$NON-NLS-2$
							+ "'."); //$NON-NLS-1$
		} catch (IOException e) {
			log.error(
					"Error writing updated application bits to temporary location: " //$NON-NLS-1$
							+ tempFile, e);
			throw e;
		} finally {
			if (tempOutputStream != null) {
				try {
					tempOutputStream.close();
				} catch (IOException e) {
					log.error("Error closing temporary output stream", e); //$NON-NLS-1$
				}
			}
		}
		if (!tempFile.exists()) {
			log.error("Temporary file for application update not written to: " //$NON-NLS-1$
					+ tempFile);
			throw new Exception();
		}
		return tempFile;
	}

	/**
	 * Creates the necessary transfer objects to report a failed application
	 * deployment (update).
	 * 
	 * @param errorMessage
	 *            reason the deploy failed
	 * @param packageDetails
	 *            describes the update being made
	 * @return response populated to reflect a failure
	 */
	private DeployPackagesResponse failApplicationDeployment(
			String errorMessage, ResourcePackageDetails packageDetails) {
		DeployPackagesResponse response = new DeployPackagesResponse(
				ContentResponseResult.FAILURE);

		DeployIndividualPackageResponse packageResponse = new DeployIndividualPackageResponse(
				packageDetails.getKey(), ContentResponseResult.FAILURE);
		packageResponse.setErrorMessage(errorMessage);

		response.addPackageResponse(packageResponse);

		return response;
	}

	@Override
	public Set<ResourcePackageDetails> discoverDeployedPackages(PackageType arg0) {

		// PLEASE DO NOT REMOVE THIS METHOD. IT IS REQUIRED FOR THE CONTENT TAB.

		Configuration pluginConfig = this.resourceContext
				.getPluginConfiguration();
		this.deploymentUrl = pluginConfig.getSimple("url").getStringValue(); //$NON-NLS-1$

		if (this.deploymentUrl != null) {
			this.deploymentFile = new File(this.deploymentUrl
					.substring(deploymentUrl.indexOf(":/") + 1)); //$NON-NLS-1$
		}

		if (!deploymentFile.exists())
			throw new IllegalStateException("Deployment file '" //$NON-NLS-1$
					+ this.deploymentFile + "' for " + this.getComponentType() //$NON-NLS-1$
					+ " does not exist."); //$NON-NLS-1$

		String fileName = deploymentFile.getName();
		org.rhq.core.pluginapi.content.version.PackageVersions packageVersions = loadPackageVersions();
		String version = packageVersions.getVersion(fileName);
		if (version == null) {
			// This is either the first time we've discovered this VDB, or
			// someone purged the PC's data dir.
			version = "1.0"; //$NON-NLS-1$
			packageVersions.putVersion(fileName, version);
			packageVersions.saveToDisk();
		}

		// Package name is the deployment's file name (e.g. foo.ear).
		PackageDetailsKey key = new PackageDetailsKey(fileName, version,
				PKG_TYPE_VDB, ARCHITECTURE);
		ResourcePackageDetails packageDetails = new ResourcePackageDetails(key);
		packageDetails.setFileName(fileName);
		packageDetails.setLocation(deploymentFile.getPath());
		if (!deploymentFile.isDirectory())
			packageDetails.setFileSize(deploymentFile.length());
		packageDetails.setFileCreatedDate(null);  
		Set<ResourcePackageDetails> packages = new HashSet<ResourcePackageDetails>();
		packages.add(packageDetails);

		return packages;
	}

	@Override
	public List<DeployPackageStep> generateInstallationSteps(
			ResourcePackageDetails arg0) {
		return null;
	}

	@Override
	public RemovePackagesResponse removePackages(
			Set<ResourcePackageDetails> arg0) {
		return null;
	}

	@Override
	public InputStream retrievePackageBits(ResourcePackageDetails packageDetails) {
		return null;
	}

	protected static Configuration getDefaultPluginConfiguration(
			ResourceType resourceType) {
		ConfigurationTemplate pluginConfigDefaultTemplate = resourceType
				.getPluginConfigurationDefinition().getDefaultTemplate();
		return (pluginConfigDefaultTemplate != null) ? pluginConfigDefaultTemplate
				.createConfiguration()
				: new Configuration();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.rhq.core.pluginapi.inventory.CreateChildResourceFacet#createResource
	 * (org.rhq.core.pluginapi.inventory.CreateResourceReport)
	 */
	@Override
	public CreateResourceReport createResource(CreateResourceReport report) {
		ResourceType resourceType = report.getResourceType();
		// if (resourceType.getName().equals("Translators")) {
		// createConfigurationBasedResource(report);
		// } else {
		createContentBasedResource(report);
		// }

		return report;
	}

	private CreateResourceReport createConfigurationBasedResource(
			CreateResourceReport createResourceReport) {
		ResourceType resourceType = createResourceReport.getResourceType();
		Configuration defaultPluginConfig = getDefaultPluginConfiguration(resourceType);
		Configuration resourceConfig = createResourceReport
				.getResourceConfiguration();
		String resourceName = getResourceName(defaultPluginConfig,
				resourceConfig);
		ComponentType componentType = ProfileServiceUtil
				.getComponentType(resourceType);
		ManagementView managementView = null;
		;
		managementView = getConnection().getManagementView();

		if (ProfileServiceUtil.isManagedComponent(getConnection(),
				resourceName, componentType)) {
			createResourceReport.setStatus(CreateResourceStatus.FAILURE);
			createResourceReport.setErrorMessage("A " + resourceType.getName() //$NON-NLS-1$
					+ " named '" + resourceName + "' already exists."); //$NON-NLS-1$ //$NON-NLS-2$
			return createResourceReport;
		}

		createResourceReport.setResourceName(resourceName);
		String resourceKey = getResourceKey(resourceType, resourceName);
		createResourceReport.setResourceKey(resourceKey);

		PropertySimple templateNameProperty = resourceConfig
				.getSimple(TranslatorComponent.Config.TEMPLATE_NAME);
		String templateName = templateNameProperty.getStringValue();

		DeploymentTemplateInfo template;
		try {
			template = managementView.getTemplate(templateName);
			Map<String, ManagedProperty> managedProperties = template.getProperties();

			ProfileServiceUtil.convertConfigurationToManagedProperties(managedProperties, resourceConfig, resourceType, null);

			LOG.debug("Applying template [" + templateName //$NON-NLS-1$
					+ "] to create ManagedComponent of type [" + componentType //$NON-NLS-1$
					+ "]..."); //$NON-NLS-1$
			try {
				managementView.applyTemplate(resourceName, template);
				managementView.process();
				createResourceReport.setStatus(CreateResourceStatus.SUCCESS);
			} catch (Exception e) {
				LOG.error("Unable to apply template [" + templateName //$NON-NLS-1$
						+ "] to create ManagedComponent of type " //$NON-NLS-1$
						+ componentType + ".", e); //$NON-NLS-1$
				createResourceReport.setStatus(CreateResourceStatus.FAILURE);
				createResourceReport.setException(e);
			}
		} catch (NoSuchDeploymentException e) {
			LOG.error("Unable to find template [" + templateName + "].", e); //$NON-NLS-1$ //$NON-NLS-2$
			createResourceReport.setStatus(CreateResourceStatus.FAILURE);
			createResourceReport.setException(e);
		} catch (Exception e) {
			LOG.error("Unable to process create request", e); //$NON-NLS-1$
			createResourceReport.setStatus(CreateResourceStatus.FAILURE);
			createResourceReport.setException(e);
		}
		return createResourceReport;
	}

	protected void createContentBasedResource(
			CreateResourceReport createResourceReport) {

		Property versionProp = createResourceReport.getPackageDetails().getDeploymentTimeConfiguration().get(Operation.Value.VDB_VERSION);
		String name = createResourceReport.getPackageDetails().getKey().getName();
		name = name.substring(name.lastIndexOf(File.separatorChar)+1);
		String userSpecifiedName = createResourceReport.getUserSpecifiedResourceName();
		String deployName = (userSpecifiedName !=null ? userSpecifiedName : name);
		
				
		if (versionProp!=null){
			
			Integer vdbVersion = ((PropertySimple)versionProp).getIntegerValue();
			//strip off vdb extension if user added it
			if (deployName.endsWith(DQPManagementView.VDB_EXT)){  
				deployName = deployName.substring(0, deployName.lastIndexOf(DQPManagementView.VDB_EXT));  
			}
			if (vdbVersion!=null){
				deployName = deployName + "." + ((Integer)vdbVersion).toString() + DQPManagementView.VDB_EXT; //$NON-NLS-1$ 
			}
			//add vdb extension if there was no version
			if (!deployName.endsWith(DQPManagementView.VDB_EXT) &&  !deployName.endsWith(DQPManagementView.DYNAMIC_VDB_EXT)){ 
				deployName = deployName + DQPManagementView.VDB_EXT;  
			}

			//null out version 
			PropertySimple nullVersionProperty = new PropertySimple(Operation.Value.VDB_VERSION, null);
			createResourceReport.getPackageDetails().getDeploymentTimeConfiguration().put(nullVersionProperty);
			createResourceReport.setUserSpecifiedResourceName(deployName);
		}
		
		getDeployer().deploy(createResourceReport, createResourceReport.getResourceType());

	}
	
    private Deployer getDeployer() {
        ProfileServiceConnection profileServiceConnection = getConnection();
        return new RemoteDeployer(profileServiceConnection, this.resourceContext);
    }

	private static String getResourceName(Configuration pluginConfig,
			Configuration resourceConfig) {
		PropertySimple resourceNameProp = pluginConfig
				.getSimple(TranslatorComponent.Config.RESOURCE_NAME);
		if (resourceNameProp == null
				|| resourceNameProp.getStringValue() == null)
			throw new IllegalStateException("Property [" //$NON-NLS-1$
					+ TranslatorComponent.Config.RESOURCE_NAME
					+ "] is not defined in the default plugin configuration."); //$NON-NLS-1$
		String resourceNamePropName = resourceNameProp.getStringValue();
		PropertySimple propToUseAsResourceName = resourceConfig
				.getSimple(resourceNamePropName);
		if (propToUseAsResourceName == null)
			throw new IllegalStateException("Property [" + resourceNamePropName //$NON-NLS-1$
					+ "] is not defined in initial Resource configuration."); //$NON-NLS-1$
		return propToUseAsResourceName.getStringValue();
	}

	private String getResourceKey(ResourceType resourceType, String resourceName) {
		ComponentType componentType = ProfileServiceUtil
				.getComponentType(resourceType);
		if (componentType == null)
			throw new IllegalStateException("Unable to map " + resourceType //$NON-NLS-1$
					+ " to a ComponentType."); //$NON-NLS-1$
		return componentType.getType() + ":" + componentType.getSubtype() + ":" //$NON-NLS-1$ //$NON-NLS-2$
				+ resourceName;
	}

	/**
	 * Returns an instantiated and loaded versions store access point.
	 * 
	 * @return will not be <code>null</code>
	 */
	private org.rhq.core.pluginapi.content.version.PackageVersions loadPackageVersions() {
		if (this.versions == null) {
			ResourceType resourceType = resourceContext.getResourceType();
			String pluginName = resourceType.getPlugin();
			File dataDirectoryFile = resourceContext.getDataDirectory();
			dataDirectoryFile.mkdirs();
			String dataDirectory = dataDirectoryFile.getAbsolutePath();
			log.trace("Creating application versions store with plugin name [" //$NON-NLS-1$
					+ pluginName + "] and data directory [" + dataDirectory //$NON-NLS-1$
					+ "]"); //$NON-NLS-1$
			this.versions = new PackageVersions(pluginName, dataDirectory);
			this.versions.loadFromDisk();
		}

		return this.versions;
	}

}
