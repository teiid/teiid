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
package org.teiid.rhq.plugin.deployer;

import java.io.File;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.deployers.spi.management.deploy.DeploymentManager;
import org.rhq.core.domain.content.PackageDetailsKey;
import org.rhq.core.domain.content.transfer.ResourcePackageDetails;
import org.rhq.core.domain.resource.CreateResourceStatus;
import org.rhq.core.domain.resource.ResourceType;
import org.rhq.core.pluginapi.inventory.CreateResourceReport;
import org.rhq.plugins.jbossas5.connection.ProfileServiceConnection;
import org.teiid.rhq.plugin.util.DeploymentUtils;

/**
 * Abstract base class capturing the common deploy functionality for embedded
 * and remote scenarios.
 * 
 */
public abstract class AbstractDeployer implements Deployer {

	private final Log log = LogFactory.getLog(this.getClass());

	private ProfileServiceConnection profileServiceConnection;

	protected AbstractDeployer(ProfileServiceConnection profileService) {
		this.profileServiceConnection = profileService;
	}

	public void deploy(CreateResourceReport createResourceReport, ResourceType resourceType) {
        File archiveFile = null;
        try {
            ResourcePackageDetails details = createResourceReport.getPackageDetails();
            PackageDetailsKey key = details.getKey();

            archiveFile = prepareArchive(createResourceReport.getUserSpecifiedResourceName(), key, resourceType);

            String archiveName = archiveFile.getName();

            if (!DeploymentUtils.hasCorrectExtension(archiveName, resourceType)) {
                createResourceReport.setStatus(CreateResourceStatus.FAILURE);
                createResourceReport.setErrorMessage("Incorrect extension specified on filename [" + archiveName + "]");
                return;
            }

            DeploymentManager deploymentManager = this.profileServiceConnection.getDeploymentManager();
            
            DeploymentUtils.deployArchive(deploymentManager, archiveFile, false);
            
            // Deployment was successful!
            createResourceReport.setResourceName(archiveName);
            createResourceReport.setResourceKey(archiveName);
            createResourceReport.setStatus(CreateResourceStatus.SUCCESS);

        } catch (Throwable t) {
            log.error("Error deploying application for request [" + createResourceReport + "].", t);
            createResourceReport.setStatus(CreateResourceStatus.FAILURE);
            createResourceReport.setException(t);
        } finally {
            if (archiveFile != null) {
                destroyArchive(archiveFile);
            }
        }
        
    }

	protected Log getLog() {
		return log;
	}

	protected ProfileServiceConnection getProfileServiceConnection() {
		return profileServiceConnection;
	}

	protected abstract File prepareArchive(PackageDetailsKey key,
			ResourceType resourceType);
	
	protected abstract File prepareArchive(String userSpecifiedName, PackageDetailsKey key,
			ResourceType resourceType);

	protected abstract void destroyArchive(File archive);
	
}
