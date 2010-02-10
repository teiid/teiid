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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
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
import org.jboss.deployers.spi.management.deploy.DeploymentManager;
import org.jboss.deployers.spi.management.deploy.DeploymentProgress;
import org.jboss.deployers.spi.management.deploy.DeploymentStatus;
import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.configuration.ConfigurationUpdateStatus;
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
import org.rhq.core.util.ZipUtil;
import org.rhq.core.util.exception.ThrowableUtil;
import org.teiid.rhq.admin.utils.SingletonConnectionManager;
import org.teiid.rhq.comm.Connection;
import org.teiid.rhq.comm.ConnectionException;
import org.teiid.rhq.comm.ExecutedResult;
import org.teiid.rhq.plugin.objects.ExecutedOperationResultImpl;
import org.teiid.rhq.plugin.util.DeploymentUtils;
import org.teiid.rhq.plugin.util.ProfileServiceUtil;

/**
 * This class implements required RHQ interfaces and provides common logic used
 * by all MetaMatrix components.
 */
public abstract class Facet implements ResourceComponent, MeasurementFacet,
		OperationFacet, ConfigurationFacet, ContentFacet, DeleteResourceFacet,
		CreateChildResourceFacet {

	protected static SingletonConnectionManager connMgr = SingletonConnectionManager
			.getInstance();

	protected final Log LOG = LogFactory.getLog(Facet.class);

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

	/**
	 * Name of the backing package type that will be used when discovering
	 * packages. This corresponds to the name of the package type defined in the
	 * plugin descriptor.
	 */
	private static final String PKG_TYPE_FILE = "vdb";

	/**
	 * Architecture string used in describing discovered packages.
	 */
	private static final String ARCHITECTURE = "noarch";

	private static final String BACKUP_FILE_EXTENSION = ".rej";

	private final Log log = LogFactory.getLog(this.getClass());

	private PackageVersions versions;

	/**
	 * The name of the ManagedDeployment (e.g.:
	 * vfszip:/C:/opt/jboss-5.0.0.GA/server/default/deploy/foo.vdb).
	 */
	protected String deploymentName;

	/**
	 * The type of the ManagedDeployment.
	 */
	// protected KnownDeploymentTypes deploymentType;
	/**
	 * The absolute path of the deployment file (e.g.:
	 * C:/opt/jboss-5.0.0.GA/server/default/deploy/foo.vdb).
	 */
	protected File deploymentFile;

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
	 * @param resourceConfiguration
	 *            the resourceConfiguration to set
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

	protected void setOperationArguments(String name,
			Configuration configuration, Map argumentMap) {
		// moved this logic up to the associated implemented class
		throw new InvalidPluginConfigurationException(
				"Not implemented on component type " + this.getComponentType()
						+ " named " + this.getComponentName());

	}

	protected void execute(final ExecutedResult result, final Map valueMap) {
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

		ExecutedResult result = new ExecutedOperationResultImpl(this
				.getComponentType(), name, operationDefinitionSet);

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
		// this simulates the plugin taking the new configuration and
		// reconfiguring the managed resource
		resourceConfiguration = report.getConfiguration().deepCopy();

		report.setStatus(ConfigurationUpdateStatus.SUCCESS);
	}

	@Override
	public void deleteResource() throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public DeployPackagesResponse deployPackages(
			Set<ResourcePackageDetails> packages,
			ContentServices contentServices) {
		String resourceTypeName = this.resourceContext.getResourceType()
				.getName();

		// You can only update the one application file referenced by this
		// resource, so punch out if multiple are
		// specified.
		if (packages.size() != 1) {
			log.warn("Request to update " + resourceTypeName
					+ " file contained multiple packages: " + packages);
			DeployPackagesResponse response = new DeployPackagesResponse(
					ContentResponseResult.FAILURE);
			response.setOverallRequestErrorMessage("Only one "
					+ resourceTypeName + " can be updated at a time.");
			return response;
		}

		ResourcePackageDetails packageDetails = packages.iterator().next();

		log.debug("Updating VDB file '" + this.deploymentFile + "' using ["
				+ packageDetails + "]...");
		// Find location of existing application.
		if (!this.deploymentFile.exists()) {
			return failApplicationDeployment(
					"Could not find application to update at location: "
							+ this.deploymentFile, packageDetails);
		}

		log.debug("Writing new EAR/WAR bits to temporary file...");
		File tempFile;
		try {
			tempFile = writeNewAppBitsToTempFile(contentServices,
					packageDetails);
		} catch (Exception e) {
			return failApplicationDeployment(
					"Error writing new application bits to temporary file - cause: "
							+ e, packageDetails);
		}
		log.debug("Wrote new EAR/WAR bits to temporary file '" + tempFile
				+ "'.");

		boolean deployExploded = this.deploymentFile.isDirectory();

		// Backup the original app file/dir.
		File tempDir = resourceContext.getTemporaryDirectory();
		File backupDir = new File(tempDir, "deployBackup");
		File backupOfOriginalFile = new File(backupDir, this.deploymentFile
				.getName());
		log.debug("Backing up existing EAR/WAR '" + this.deploymentFile
				+ "' to '" + backupOfOriginalFile + "'...");
		try {
			if (backupOfOriginalFile.exists()) {
				FileUtils.forceDelete(backupOfOriginalFile);
			}
			if (this.deploymentFile.isDirectory()) {
				FileUtils.copyDirectory(this.deploymentFile,
						backupOfOriginalFile, true);
			} else {
				FileUtils.copyFile(this.deploymentFile, backupOfOriginalFile,
						true);
			}
		} catch (Exception e) {
			throw new RuntimeException("Failed to backup existing "
					+ resourceTypeName + "'" + this.deploymentFile + "' to '"
					+ backupOfOriginalFile + "'.");
		}

		// Now stop the original app.
		try {
			DeploymentManager deploymentManager = ProfileServiceUtil
					.getDeploymentManager();
			DeploymentProgress progress = deploymentManager
					.stop(this.deploymentName);
			DeploymentUtils.run(progress);
		} catch (Exception e) {
			throw new RuntimeException("Failed to stop deployment ["
					+ this.deploymentName + "].", e);
		}

		// And then remove it (this will delete the physical file/dir from the
		// deploy dir).
		try {
			DeploymentManager deploymentManager = ProfileServiceUtil
					.getDeploymentManager();
			DeploymentProgress progress = deploymentManager
					.remove(this.deploymentName);
			DeploymentUtils.run(progress);
		} catch (Exception e) {
			throw new RuntimeException("Failed to remove deployment ["
					+ this.deploymentName + "].", e);
		}

		// Deploy away!
		log.debug("Deploying '" + tempFile + "'...");
		DeploymentManager deploymentManager = null;
		try {
			deploymentManager = ProfileServiceUtil.getDeploymentManager();
			DeploymentUtils.deployArchive(deploymentManager, tempFile,
					deployExploded);
		} catch (Exception e) {
			// Deploy failed - rollback to the original app file...
			log.debug("Redeploy failed - rolling back to original archive...",
					e);
			String errorMessage = ThrowableUtil.getAllMessages(e);
			try {
				// Try to delete the new app file, which failed to deploy, if it
				// still exists.
				if (this.deploymentFile.exists()) {
					try {
						FileUtils.forceDelete(this.deploymentFile);
					} catch (IOException e1) {
						log.debug("Failed to delete application file '"
								+ this.deploymentFile
								+ "' that failed to deploy.", e1);
					}
				}
				// Now redeploy the original file - this generally should
				// succeed.
				DeploymentUtils.deployArchive(deploymentManager,
						backupOfOriginalFile, deployExploded);
				errorMessage += " ***** ROLLED BACK TO ORIGINAL APPLICATION FILE. *****";
			} catch (Exception e1) {
				log.debug("Rollback failed!", e1);
				errorMessage += " ***** FAILED TO ROLLBACK TO ORIGINAL APPLICATION FILE. *****: "
						+ ThrowableUtil.getAllMessages(e1);
			}
			log
					.info("Failed to update " + resourceTypeName + " file '"
							+ this.deploymentFile + "' using ["
							+ packageDetails + "].");
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

		log.debug("Updated " + resourceTypeName + " file '"
				+ this.deploymentFile + "' successfully - returning response ["
				+ response + "]...");

		return response;
	}

	@Override
	public Set<ResourcePackageDetails> discoverDeployedPackages(PackageType arg0) {
		if (!this.deploymentFile.exists())
			throw new IllegalStateException("Deployment file '"
					+ this.deploymentFile + "' for " + "VDB Archive"
					+ " does not exist.");

		String fileName = this.deploymentFile.getName();
		PackageVersions packageVersions = loadPackageVersions();
		String version = packageVersions.getVersion(fileName);
		if (version == null) {
			// This is either the first time we've discovered this VDB, or
			// someone purged the PC's data dir.
			version = "1.0";
			packageVersions.putVersion(fileName, version);
			packageVersions.saveToDisk();
		}

		// Package name is the deployment's file name (e.g. foo.ear).
		PackageDetailsKey key = new PackageDetailsKey(fileName, version,
				PKG_TYPE_FILE, ARCHITECTURE);
		ResourcePackageDetails packageDetails = new ResourcePackageDetails(key);
		packageDetails.setFileName(fileName);
		packageDetails.setLocation(this.deploymentFile.getPath());
		if (!this.deploymentFile.isDirectory())
			packageDetails.setFileSize(this.deploymentFile.length());
		packageDetails.setFileCreatedDate(null); // TODO: get created date via
		// SIGAR
		Set<ResourcePackageDetails> packages = new HashSet<ResourcePackageDetails>();
		packages.add(packageDetails);

		return packages;
	}

	@Override
	public List<DeployPackageStep> generateInstallationSteps(
			ResourcePackageDetails arg0) {
		return null;
	}

	public RemovePackagesResponse removePackages(
			Set<ResourcePackageDetails> packages) {
		throw new UnsupportedOperationException(
				"Cannot remove the package backing an VDB resource.");
	}

	@Override
	public InputStream retrievePackageBits(ResourcePackageDetails packageDetails) {
		File packageFile = new File(packageDetails.getName());
		File fileToSend;
		try {
			if (packageFile.isDirectory()) {
				fileToSend = File.createTempFile("rhq", ".zip");
				ZipUtil.zipFileOrDirectory(packageFile, fileToSend);
			} else
				fileToSend = packageFile;
			return new BufferedInputStream(new FileInputStream(fileToSend));
		} catch (IOException e) {
			throw new RuntimeException("Failed to retrieve package bits for "
					+ packageDetails, e);
		}
	}

	/**
	 * Returns an instantiated and loaded versions store access point.
	 * 
	 * @return will not be <code>null</code>
	 */
	private PackageVersions loadPackageVersions() {
		if (this.versions == null) {
			ResourceType resourceType = this.resourceContext.getResourceType();
			String pluginName = resourceType.getPlugin();
			File dataDirectoryFile = this.resourceContext.getDataDirectory();
			dataDirectoryFile.mkdirs();
			String dataDirectory = dataDirectoryFile.getAbsolutePath();
			log.trace("Creating application versions store with plugin name ["
					+ pluginName + "] and data directory [" + dataDirectory
					+ "]");
			this.versions = new PackageVersions(pluginName, dataDirectory);
			this.versions.loadFromDisk();
		}

		return this.versions;
	}

	/**
	 * Creates the necessary transfer objects to report a failed application
	 * deployment (update).
	 * 
	 * @param errorMessage
	 *            reason the deploy failed
	 * @param packageDetails
	 *            describes the update being made
	 * 
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

	private File writeNewAppBitsToTempFile(ContentServices contentServices,
			ResourcePackageDetails packageDetails) throws Exception {
		File tempDir = resourceContext.getTemporaryDirectory();
		File tempFile = new File(tempDir, this.deploymentFile.getName());

		OutputStream tempOutputStream = null;
		try {
			tempOutputStream = new BufferedOutputStream(new FileOutputStream(
					tempFile));
			long bytesWritten = contentServices.downloadPackageBits(
					resourceContext.getContentContext(), packageDetails
							.getKey(), tempOutputStream, true);
			log
					.debug("Wrote " + bytesWritten + " bytes to '" + tempFile
							+ "'.");
		} catch (IOException e) {
			log.error(
					"Error writing updated application bits to temporary location: "
							+ tempFile, e);
			throw e;
		} finally {
			if (tempOutputStream != null) {
				try {
					tempOutputStream.close();
				} catch (IOException e) {
					log.error("Error closing temporary output stream", e);
				}
			}
		}
		if (!tempFile.exists()) {
			log.error("Temporary file for application update not written to: "
					+ tempFile);
			throw new Exception();
		}
		return tempFile;
	}

	private void persistApplicationVersion(
			ResourcePackageDetails packageDetails, File appFile) {
		String packageName = appFile.getName();
		PackageVersions versions = loadApplicationVersions();
		versions.putVersion(packageName, packageDetails.getVersion());
	}

	private void deleteBackupOfOriginalFile(File backupOfOriginalFile) {
		try {
			FileUtils.forceDelete(backupOfOriginalFile);
		} catch (Exception e) {
			// not critical.
			log.warn("Failed to delete backup of original file: "
					+ backupOfOriginalFile);
		}
	}

	/**
	 * Returns an instantiated and loaded versions store access point.
	 * 
	 * @return will not be <code>null</code>
	 */
	private PackageVersions loadApplicationVersions() {
		if (versions == null) {
			ResourceType resourceType = resourceContext.getResourceType();
			String pluginName = resourceType.getPlugin();

			File dataDirectoryFile = resourceContext.getDataDirectory();

			if (!dataDirectoryFile.exists()) {
				dataDirectoryFile.mkdir();
			}

			String dataDirectory = dataDirectoryFile.getAbsolutePath();

			log.debug("Creating application versions store with plugin name ["
					+ pluginName + "] and data directory [" + dataDirectory
					+ "]");

			versions = new PackageVersions(pluginName, dataDirectory);
			versions.loadFromDisk();
		}

		return versions;
	}

	@Override
	public CreateResourceReport createResource(CreateResourceReport createResourceReport) {
		ResourcePackageDetails details = createResourceReport
				.getPackageDetails();
		PackageDetailsKey key = details.getKey();
		// This is the full path to a temporary file which was written by the UI
		// layer.
		String archivePath = key.getName();

		try {
			File archiveFile = new File(archivePath);

			if (!DeploymentUtils.hasCorrectExtension(archiveFile.getName(), resourceContext.getResourceType())) {
				createResourceReport.setStatus(CreateResourceStatus.FAILURE);
				createResourceReport
						.setErrorMessage("Incorrect extension specified on filename ["
								+ archivePath + "]");
				return createResourceReport;
			}

			Configuration deployTimeConfig = details
					.getDeploymentTimeConfiguration();
			@SuppressWarnings( { "ConstantConditions" })
		//	boolean deployExploded = deployTimeConfig.getSimple(
		//			"deployExploded").getBooleanValue();

			DeploymentManager deploymentManager = ProfileServiceUtil.getDeploymentManager();
			DeploymentUtils.deployArchive(deploymentManager, archiveFile, false);

			createResourceReport.setResourceName(archivePath);
			createResourceReport.setResourceKey(archivePath);
			createResourceReport.setStatus(CreateResourceStatus.SUCCESS);
			
		} catch (Throwable t) {
			log.error("Error deploying application for report: "
					+ createResourceReport, t);
			createResourceReport.setStatus(CreateResourceStatus.FAILURE);
			createResourceReport.setException(t);
		}
		
		return createResourceReport;
		
	}

}
