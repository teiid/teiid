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
import java.io.IOException;
import java.util.List;

import org.jboss.as.server.deployment.*;
import org.jboss.as.server.deployment.module.*;
import org.jboss.modules.Module;
import org.jboss.modules.ModuleIdentifier;
import org.jboss.modules.ModuleLoadException;
import org.jboss.modules.ModuleLoader;
import org.jboss.vfs.VFS;
import org.jboss.vfs.VirtualFile;
import org.jboss.vfs.VirtualFileFilter;
import org.jboss.vfs.VisitorAttributes;
import org.jboss.vfs.util.SuffixMatchFilter;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.deployers.TeiidAttachments;

public class VDBDependencyProcessor implements DeploymentUnitProcessor {
	public static final String LIB = "/lib"; //$NON-NLS-1$
	private static final VirtualFileFilter DEFAULT_JAR_LIB_FILTER = new SuffixMatchFilter(".jar", VisitorAttributes.DEFAULT); //$NON-NLS-1$
	
	@Override
	public void deploy(DeploymentPhaseContext phaseContext) throws DeploymentUnitProcessingException {
		DeploymentUnit deploymentUnit = phaseContext.getDeploymentUnit();
		if (!TeiidAttachments.isVDBDeployment(deploymentUnit)) {
			return;
		}
		
		if (!TeiidAttachments.isDynamicVDB(deploymentUnit)) {
			final ResourceRoot deploymentResourceRoot = deploymentUnit.getAttachment(Attachments.DEPLOYMENT_ROOT);
			final VirtualFile deploymentRoot = deploymentResourceRoot.getRoot();
	        if(deploymentRoot == null) {
	            return;
	        }
			
			try {
				final VirtualFile libDir = deploymentRoot.getChild(LIB);
				if (libDir.exists()) {
					final List<VirtualFile> archives = libDir.getChildren(DEFAULT_JAR_LIB_FILTER);
					for (final VirtualFile archive : archives) {
						try {
							final Closeable closable = VFS.mountZip(archive, archive,TempFileProviderService.provider());
							final ResourceRoot jarArchiveRoot = new ResourceRoot(archive.getName(), archive, new MountHandle(closable));
							ModuleRootMarker.mark(jarArchiveRoot);
							deploymentUnit.addToAttachmentList(Attachments.RESOURCE_ROOTS, jarArchiveRoot);
						} catch (IOException e) {
							throw new DeploymentUnitProcessingException("failed to process " + archive, e); //$NON-NLS-1$
						}
					}
				}	
			} catch(IOException e) {
				throw new DeploymentUnitProcessingException(e);
			}
		}
		
		// add translators as dependent modules to this VDB.
        try {
			final ModuleSpecification moduleSpecification = deploymentUnit.getAttachment(Attachments.MODULE_SPECIFICATION);
			final ModuleLoader moduleLoader = Module.getCallerModule().getModule(ModuleIdentifier.create("org.jboss.teiid")).getModuleLoader(); //$NON-NLS-1$
			VDBMetaData vdb = deploymentUnit.getAttachment(TeiidAttachments.VDB_METADATA);

			for (ModelMetaData model: vdb.getModelMetaDatas().values()) {
				for (String source:model.getSourceNames()) {
					moduleSpecification.addLocalDependency(new ModuleDependency(moduleLoader, ModuleIdentifier.create(model.getSourceTranslatorName(source)), false, false, false));		
				}
			}
		} catch (ModuleLoadException e) {
			throw new DeploymentUnitProcessingException(e);
		}
	}

	@Override
	public void undeploy(DeploymentUnit context) {
	}
}
