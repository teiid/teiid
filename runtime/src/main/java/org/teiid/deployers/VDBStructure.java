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
package org.teiid.deployers;

import java.io.Closeable;
import java.io.IOException;

import org.jboss.as.server.deployment.*;
import org.jboss.as.server.deployment.module.ModuleRootMarker;
import org.jboss.as.server.deployment.module.MountHandle;
import org.jboss.as.server.deployment.module.ResourceRoot;
import org.jboss.as.server.deployment.module.TempFileProviderService;
import org.jboss.vfs.VFS;
import org.jboss.vfs.VirtualFile;
import org.teiid.metadata.VdbConstants;



public class VDBStructure  implements DeploymentUnitProcessor {
	private static final String VDB_EXTENSION = ".vdb"; //$NON-NLS-1$
	private static final String DYNAMIC_VDB_STRUCTURE = "-vdb.xml"; //$NON-NLS-1$
	
	@Override
	public void deploy(final DeploymentPhaseContext phaseContext)  throws DeploymentUnitProcessingException {
		
        DeploymentUnit deploymentUnit = phaseContext.getDeploymentUnit();
        
        VirtualFile file = deploymentUnit.getAttachment(Attachments.DEPLOYMENT_ROOT).getRoot();
        if (file == null) {
        	return;
        }
        
        if(file.getLowerCaseName().endsWith(VDB_EXTENSION)) {

        	try {
				final Closeable closable = VFS.mountZip(file, file, TempFileProviderService.provider());
				final ResourceRoot vdbArchiveRoot = new ResourceRoot(file.getName(), file, new MountHandle(closable));
				ModuleRootMarker.mark(vdbArchiveRoot);
				
				VirtualFile metainf = file.getChild("META-INF"); //$NON-NLS-1$
				if (metainf == null) {
					return;
				}
				
				if (metainf.getChild(VdbConstants.DEPLOYMENT_FILE) == null) {
					return;
				}
				// adds a TYPE attachment.
				TeiidAttachments.setAsVDBDeployment(deploymentUnit);
			} catch (IOException e) {
				throw new DeploymentUnitProcessingException("failed to process " + file, e); //$NON-NLS-1$
			}			
        }
        else if (file.getLowerCaseName().endsWith(DYNAMIC_VDB_STRUCTURE)) {
	        TeiidAttachments.setAsDynamicVDBDeployment(deploymentUnit);			        	
        }
	}
	
	
	@Override
	public void undeploy(final DeploymentUnit context) {
		
	}

}
