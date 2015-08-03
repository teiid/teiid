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
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.jboss.as.server.deployment.Attachments;
import org.jboss.as.server.deployment.DeploymentPhaseContext;
import org.jboss.as.server.deployment.DeploymentUnit;
import org.jboss.as.server.deployment.DeploymentUnitProcessingException;
import org.jboss.as.server.deployment.DeploymentUnitProcessor;
import org.jboss.as.server.deployment.module.ModuleDependency;
import org.jboss.as.server.deployment.module.ModuleRootMarker;
import org.jboss.as.server.deployment.module.ModuleSpecification;
import org.jboss.as.server.deployment.module.MountHandle;
import org.jboss.as.server.deployment.module.ResourceRoot;
import org.jboss.as.server.deployment.module.TempFileProviderService;
import org.jboss.modules.Module;
import org.jboss.modules.ModuleIdentifier;
import org.jboss.modules.ModuleLoadException;
import org.jboss.modules.ModuleLoader;
import org.jboss.vfs.VFS;
import org.jboss.vfs.VirtualFile;
import org.jboss.vfs.VirtualFileFilter;
import org.jboss.vfs.VisitorAttributes;
import org.jboss.vfs.util.SuffixMatchFilter;
import org.teiid.adminapi.impl.VDBMetaData;

class VDBDependencyDeployer implements DeploymentUnitProcessor {
	public static final String LIB = "/lib"; //$NON-NLS-1$
	private static final VirtualFileFilter DEFAULT_JAR_LIB_FILTER = new SuffixMatchFilter(".jar", VisitorAttributes.DEFAULT); //$NON-NLS-1$
	
	@Override
	public void deploy(DeploymentPhaseContext phaseContext) throws DeploymentUnitProcessingException {
		DeploymentUnit deploymentUnit = phaseContext.getDeploymentUnit();
		if (!TeiidAttachments.isVDBDeployment(deploymentUnit)) {
			return;
		}
		
		
		final VDBMetaData deployment = deploymentUnit.getAttachment(TeiidAttachments.VDB_METADATA);
		ArrayList<ModuleDependency> localDependencies = new ArrayList<ModuleDependency>();
		ArrayList<ModuleDependency> userDependencies = new ArrayList<ModuleDependency>();
		String moduleNames = deployment.getPropertyValue("lib"); //$NON-NLS-1$
        if (moduleNames != null) {
        	StringTokenizer modules = new StringTokenizer(moduleNames);
        	while (modules.hasMoreTokens()) {
        		String moduleName = modules.nextToken().trim();
            	ModuleIdentifier lib = ModuleIdentifier.create(moduleName);
            	ModuleLoader moduleLoader = Module.getCallerModuleLoader();
    	        
            	try {
    	        	moduleLoader.loadModule(lib);
    	        	localDependencies.add(new ModuleDependency(moduleLoader, ModuleIdentifier.create(moduleName), false, false, false, false));
    	        } catch (ModuleLoadException e) {
    	        	// this is to handle JAR based deployments which take on name like "deployment.<jar-name>"
    	        	moduleLoader = deploymentUnit.getAttachment(Attachments.SERVICE_MODULE_LOADER);
    	        	try {
    	        		moduleLoader.loadModule(lib);
    	        		userDependencies.add(new ModuleDependency(moduleLoader, ModuleIdentifier.create(moduleName), false, false, false, true));
    				} catch (ModuleLoadException e1) {
    		        	throw new DeploymentUnitProcessingException(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50088, moduleName, deployment.getName(), deployment.getVersion(), e1));					
    				}
    	        }
        	}
        }
        
		if (!TeiidAttachments.isVDBXMLDeployment(deploymentUnit)) {
			try {
				final ResourceRoot deploymentResourceRoot = deploymentUnit.getAttachment(Attachments.DEPLOYMENT_ROOT);
				final VirtualFile deploymentRoot = deploymentResourceRoot.getRoot();
		        if(deploymentRoot == null) {
		            return;
		        }
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
							throw new DeploymentUnitProcessingException(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50018, archive), e); 
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
			moduleSpecification.addLocalDependency(new ModuleDependency(moduleLoader, ModuleIdentifier.create("org.jboss.teiid.api"), false, false, false, false)); //$NON-NLS-1$
			moduleSpecification.addLocalDependency(new ModuleDependency(moduleLoader, ModuleIdentifier.create("org.jboss.teiid.common-core"), false, false, false, false)); //$NON-NLS-1$
			moduleSpecification.addLocalDependency(new ModuleDependency(moduleLoader, ModuleIdentifier.create("javax.api"), false, false, false, false)); //$NON-NLS-1$
        	if (!localDependencies.isEmpty()) {
        		moduleSpecification.addLocalDependencies(localDependencies);
        	}
        	if (!userDependencies.isEmpty()) {
        		moduleSpecification.addUserDependencies(userDependencies);
        	}
		} catch (ModuleLoadException e) {
			throw new DeploymentUnitProcessingException(IntegrationPlugin.Event.TEIID50018.name(), e);
		}
	}

	@Override
	public void undeploy(DeploymentUnit context) {
	}
}
