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
	
	@Override
	public void deploy(final DeploymentPhaseContext phaseContext)  throws DeploymentUnitProcessingException {
		
        DeploymentUnit deploymentUnit = phaseContext.getDeploymentUnit();
        
        String deploymentName = deploymentUnit.getName();
        VirtualFile file = deploymentUnit.getAttachment(Attachments.DEPLOYMENT_ROOT).getRoot();
        if (file == null) {
        	return;
        }
        
        if(deploymentName.endsWith(VDB_EXTENSION)) {
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
        else if (deploymentName.endsWith(DYNAMIC_VDB_STRUCTURE)) {
	        TeiidAttachments.setAsVDBXMLDeployment(deploymentUnit);			        	
        }
	}
	
	
	@Override
	public void undeploy(final DeploymentUnit context) {
	}

}
