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
package org.teiid.rhq.plugin.util;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.deployers.spi.management.deploy.DeploymentManager;
import org.jboss.deployers.spi.management.deploy.DeploymentProgress;
import org.jboss.deployers.spi.management.deploy.DeploymentStatus;
import org.rhq.core.domain.resource.ResourceType;
import org.rhq.core.util.exception.ThrowableUtil;

/**
 * A set of utility methods for deploying applications.
 *
 */
public class DeploymentUtils {
    private static final Log LOG = LogFactory.getLog(PluginConstants.DEFAULT_LOGGER_CATEGORY);

    public static boolean hasCorrectExtension(String archiveFileName, ResourceType resourceType) {
        String expectedExtension = "vdb"; //$NON-NLS-1$
        int lastPeriod = archiveFileName.lastIndexOf("."); //$NON-NLS-1$
        String extension = (lastPeriod != -1) ? archiveFileName.substring(lastPeriod + 1) : null;
        // Use File.equals() to compare the extensions so case-sensitivity is correct for this platform.
        return (extension != null && new File(extension).equals(new File(expectedExtension))) || isDynamicVDB(archiveFileName);
    }

    private static boolean isDynamicVDB(String archiveFileName) {
        String dynamicFile = "-vdb.xml"; //$NON-NLS-1$
        if (archiveFileName.length()<8) return false;
        String fileEnding = archiveFileName.substring(archiveFileName.length()-8);
        return (new File(dynamicFile).equals(new File(fileEnding)));
    }
    
    /**
     * Deploys (i.e. distributes then starts) the specified archive file.
     *
     * @param deploymentManager
     * @param archiveFile
     * @param deployExploded
     * 
     * @return
     *
     * @throws Exception if the deployment fails for any reason
     */
    public static void deployArchive(DeploymentManager deploymentManager, File archiveFile, boolean deployExploded)
        throws Exception {
        String archiveFileName = archiveFile.getName();
        LOG.debug("Deploying '" + archiveFileName + "' (deployExploded=" + deployExploded + ")..."); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        URL contentURL;
        try {
            contentURL = archiveFile.toURI().toURL();
        }
        catch (MalformedURLException e) {
            throw new IllegalArgumentException("Failed to convert archive file path '" + archiveFile + "' to URL.", e); //$NON-NLS-1$ //$NON-NLS-2$
        }
        
        deployAndStart(deploymentManager, contentURL, archiveFileName);
    }

    /**
     * Deploys (i.e. distributes then starts) the specified archive file.
     *
     * @param fileName
     * @param deploymentManager
     * @param archiveUrl
     * @param deployExploded
     * 
     * @return
     *
     * @throws Exception if the deployment fails for any reason
     */
    public static void deployArchive(String fileName, DeploymentManager deploymentManager, URL contentURL, boolean deployExploded)
        throws Exception {
        String archiveFileName = fileName;
        LOG.debug("Deploying '" + archiveFileName + "' (deployExploded=" + deployExploded + ")..."); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
         
        deployAndStart(deploymentManager, contentURL, archiveFileName);
    }

	/**
	 * @param deploymentManager
	 * @param contentURL
	 * @param archiveFileName
	 * @throws Exception
	 */
	private static void deployAndStart(DeploymentManager deploymentManager,
			URL contentURL, String archiveFileName) throws Exception {
		DeploymentProgress progress = null;
        DeploymentStatus distributeStatus;
        Exception distributeFailure = null;
        try {
            progress = deploymentManager.distribute(archiveFileName, contentURL, true);
            distributeStatus = run(progress);
            if (distributeStatus.isFailed()) {
                distributeFailure = (distributeStatus.getFailure() != null) ? distributeStatus.getFailure() :
                        new Exception("Distribute failed for unknown reason."); //$NON-NLS-1$
            }
        }
        catch (Exception e) {
            distributeFailure = e;
        }
        if (distributeFailure != null) {
            throw new Exception("Failed to distribute '" + contentURL + "' to '" + archiveFileName + "' - cause: "  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                    + ThrowableUtil.getAllMessages(distributeFailure));
        }

        // Now that we've successfully distributed the deployment, we need to start it.
        String[] deploymentNames = progress.getDeploymentID().getRepositoryNames();
        DeploymentStatus startStatus;
        Exception startFailure = null;
        try {
            progress = deploymentManager.start(deploymentNames);
            startStatus = run(progress);
            if (startStatus.isFailed()) {
                startFailure = (startStatus.getFailure() != null) ? startStatus.getFailure() :
                        new Exception("Start failed for unknown reason."); //$NON-NLS-1$
            }
        }
        catch (Exception e) {
            startFailure = e;
        }
        if (startFailure != null) {
            LOG.error("Failed to start deployment " + Arrays.asList(deploymentNames) //$NON-NLS-1$
                + " during deployment of '" + archiveFileName + "'. Backing out the deployment...", startFailure); //$NON-NLS-1$ //$NON-NLS-2$
            // If start failed, the app is invalid, so back out the deployment.
            DeploymentStatus removeStatus;
            Exception removeFailure = null;
            try {
                progress = deploymentManager.remove(deploymentNames);
                removeStatus = run(progress);
                if (removeStatus.isFailed()) {
                    removeFailure = (removeStatus.getFailure() != null) ? removeStatus.getFailure() :
                        new Exception("Remove failed for unknown reason."); //$NON-NLS-1$
                }
            }
            catch (Exception e) {
                removeFailure = e;
            }
            if (removeFailure != null) {
                LOG.error("Failed to remove deployment " + Arrays.asList(deploymentNames) //$NON-NLS-1$
                    + " after start failure.", removeFailure); //$NON-NLS-1$
            }
            throw new Exception("Failed to start deployment " + Arrays.asList(deploymentNames) //$NON-NLS-1$
                + " during deployment of '" + archiveFileName + "' - cause: " + //$NON-NLS-1$ //$NON-NLS-2$
                    ThrowableUtil.getAllMessages(startFailure));
        }
        // If we made it this far, the deployment (distribution+start) was successful.
        return;
	}
    
    public static DeploymentStatus run(DeploymentProgress progress) {
        progress.run();
        return progress.getDeploymentStatus();
    }

    private DeploymentUtils() {
    }    
    
}
