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

import org.jboss.as.server.deployment.*;
import org.jboss.as.server.deployment.module.ModuleRootMarker;
import org.jboss.as.server.deployment.module.ModuleSpecification;
import org.jboss.as.server.deployment.module.ResourceRoot;
import org.jboss.vfs.VFSUtils;
import org.jboss.vfs.VirtualFile;



class DynamicVDBRootMountDeployer  implements DeploymentUnitProcessor {
    private static final String DYNAMIC_VDB_STRUCTURE = "-vdb.xml"; //$NON-NLS-1$
    private static final String DDL_VDB_STRUCTURE = "-vdb.ddl"; //$NON-NLS-1$

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

        if (deploymentName.endsWith(DYNAMIC_VDB_STRUCTURE) || deploymentName.endsWith(DDL_VDB_STRUCTURE)) {
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
