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

import org.jboss.as.server.deployment.Attachments;
import org.jboss.as.server.deployment.DeploymentPhaseContext;
import org.jboss.as.server.deployment.DeploymentUnit;
import org.jboss.as.server.deployment.DeploymentUnitProcessingException;
import org.jboss.as.server.deployment.DeploymentUnitProcessor;
import org.jboss.vfs.VirtualFile;
import org.teiid.query.metadata.VDBResources;



class VDBStructureDeployer  implements DeploymentUnitProcessor {
    private static final String VDB_EXTENSION = ".vdb"; //$NON-NLS-1$
    private static final String DYNAMIC_VDB_STRUCTURE = "-vdb.xml"; //$NON-NLS-1$
    private static final String DDL_VDB_STRUCTURE = "-vdb.ddl"; //$NON-NLS-1$

    @Override
    public void deploy(final DeploymentPhaseContext phaseContext)  throws DeploymentUnitProcessingException {

        DeploymentUnit deploymentUnit = phaseContext.getDeploymentUnit();

        String deploymentName = deploymentUnit.getName();
        VirtualFile file = deploymentUnit.getAttachment(Attachments.DEPLOYMENT_ROOT).getRoot();
        if (file == null) {
            return;
        }

        if(deploymentName.toLowerCase().endsWith(VDB_EXTENSION)) {
            VirtualFile metainf = file.getChild("META-INF"); //$NON-NLS-1$
            if (metainf == null) {
                return;
            }

            if (metainf.getChild(VDBResources.DEPLOYMENT_FILE) == null) {
                return;
            }
            // adds a TYPE attachment.
            TeiidAttachments.setAsVDBDeployment(deploymentUnit);
        }
        else if (deploymentName.toLowerCase().endsWith(DYNAMIC_VDB_STRUCTURE)) {
            TeiidAttachments.setAsVDBXMLDeployment(deploymentUnit);
        }
        else if (deploymentName.toLowerCase().endsWith(DDL_VDB_STRUCTURE)) {
            TeiidAttachments.setAsVDBDDLDeployment(deploymentUnit);
        }
    }


    @Override
    public void undeploy(final DeploymentUnit context) {
    }

}
