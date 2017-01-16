/*
 * Copyright 2008-2013 Red Hat, Inc, and individual contributors.
 * 
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 * 
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
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