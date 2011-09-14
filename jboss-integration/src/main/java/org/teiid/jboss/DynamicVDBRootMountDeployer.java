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
package org.teiid.jboss;

import java.io.Closeable;

import org.jboss.as.server.deployment.*;
import org.jboss.as.server.deployment.module.ModuleRootMarker;
import org.jboss.as.server.deployment.module.ModuleSpecification;
import org.jboss.as.server.deployment.module.ResourceRoot;
import org.jboss.vfs.VFSUtils;
import org.jboss.vfs.VirtualFile;



class DynamicVDBRootMountDeployer  implements DeploymentUnitProcessor {
	private static final String DYNAMIC_VDB_STRUCTURE = "-vdb.xml"; //$NON-NLS-1$
	
	public void deploy(DeploymentPhaseContext phaseContext) throws DeploymentUnitProcessingException {
        final DeploymentUnit deploymentUnit = phaseContext.getDeploymentUnit();
        
        if(deploymentUnit.getAttachment(Attachments.DEPLOYMENT_ROOT) != null) {
            return;
        }
        
        final String deploymentName = deploymentUnit.getName();
        final VirtualFile deploymentContents = deploymentUnit.getAttachment(Attachments.DEPLOYMENT_CONTENTS);

        // internal deployments do not have any contents, so there is nothing to mount
        if (deploymentContents == null)
            return;
        
        if (deploymentName.endsWith(DYNAMIC_VDB_STRUCTURE)) {
            // use the contents directly
            // nothing was mounted
            final ResourceRoot resourceRoot = new ResourceRoot(deploymentContents, null);
            ModuleRootMarker.mark(resourceRoot);
            deploymentUnit.putAttachment(Attachments.DEPLOYMENT_ROOT, resourceRoot);
            deploymentUnit.putAttachment(Attachments.MODULE_SPECIFICATION, new ModuleSpecification());            
        }
    }

    public void undeploy(DeploymentUnit context) {
        final ResourceRoot resourceRoot = context.removeAttachment(Attachments.DEPLOYMENT_ROOT);
        if (resourceRoot != null) {
            final Closeable mountHandle = resourceRoot.getMountHandle();
            VFSUtils.safeClose(mountHandle);
        }
    }
}
