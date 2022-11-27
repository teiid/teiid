/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.jboss;

import java.io.Closeable;
import java.io.IOException;

import org.jboss.as.server.deployment.Attachments;
import org.jboss.as.server.deployment.DeploymentMountProvider;
import org.jboss.as.server.deployment.DeploymentPhaseContext;
import org.jboss.as.server.deployment.DeploymentUnit;
import org.jboss.as.server.deployment.DeploymentUnitProcessingException;
import org.jboss.as.server.deployment.DeploymentUnitProcessor;
import org.jboss.as.server.deployment.MountType;
import org.jboss.as.server.deployment.module.ModuleRootMarker;
import org.jboss.as.server.deployment.module.ModuleSpecification;
import org.jboss.as.server.deployment.module.MountHandle;
import org.jboss.as.server.deployment.module.ResourceRoot;
import org.jboss.vfs.VFS;
import org.jboss.vfs.VFSUtils;
import org.jboss.vfs.VirtualFile;

/**
 * Handle mounting descriptor files and marking them as a DEPLOYMENT_ROOT
 *
 */
public class FileRootMountProcessor implements DeploymentUnitProcessor {

    public FileRootMountProcessor(String fileSuffix) {
        this.fileSuffix = fileSuffix;
    }

    @Override
    public void deploy(DeploymentPhaseContext phaseContext) throws DeploymentUnitProcessingException {
        DeploymentUnit deploymentUnit = phaseContext.getDeploymentUnit();

        if (deploymentUnit.getAttachment( Attachments.DEPLOYMENT_ROOT ) != null ||
                !deploymentUnit.getName().toLowerCase().endsWith( this.fileSuffix )) {
            return;
        }

        final DeploymentMountProvider deploymentMountProvider = deploymentUnit.getAttachment( Attachments.SERVER_DEPLOYMENT_REPOSITORY );
        if(deploymentMountProvider == null) {
            throw new DeploymentUnitProcessingException( "No deployment repository available." );
        }

        final VirtualFile deploymentContents = deploymentUnit.getAttachment( Attachments.DEPLOYMENT_CONTENTS );

        // internal deployments do not have any contents, so there is nothing to mount
        if (deploymentContents == null)
            return;

        String deploymentName = deploymentUnit.getName();
        final VirtualFile deploymentRoot = VFS.getChild( "content/" + deploymentName );
        Closeable handle = null;
        final MountHandle mountHandle;
        boolean failed = false;
        try {
            handle = deploymentMountProvider.mountDeploymentContent( deploymentContents, deploymentRoot, MountType.REAL );
            mountHandle = new MountHandle( handle );
        } catch (IOException e) {
            failed = true;
            throw new DeploymentUnitProcessingException( "Failed to mount " + this.fileSuffix + " file", e );
        } finally {
            if (failed) {
                VFSUtils.safeClose( handle );
            }
        }
        final ResourceRoot resourceRoot = new ResourceRoot( deploymentRoot, mountHandle );
        ModuleRootMarker.mark(resourceRoot);
        deploymentUnit.putAttachment( Attachments.DEPLOYMENT_ROOT, resourceRoot );
        deploymentUnit.putAttachment( Attachments.MODULE_SPECIFICATION, new ModuleSpecification() );
    }

    @Override
    public void undeploy(DeploymentUnit context) {
        final ResourceRoot knobRoot = context.removeAttachment(Attachments.DEPLOYMENT_ROOT);
        if (knobRoot != null) {
            final MountHandle mountHandle = knobRoot.getMountHandle();
            VFSUtils.safeClose( mountHandle );
        }
    }

    private String fileSuffix;
}