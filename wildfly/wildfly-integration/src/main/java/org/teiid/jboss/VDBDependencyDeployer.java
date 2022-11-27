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
import org.jboss.as.server.moduleservice.ServiceModuleLoader;
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
        ServiceModuleLoader sml = deploymentUnit.getAttachment(Attachments.SERVICE_MODULE_LOADER);
        deployment.addAttachment(ServiceModuleLoader.class, sml);
        ArrayList<ModuleDependency> localDependencies = new ArrayList<ModuleDependency>();
        ArrayList<ModuleDependency> userDependencies = new ArrayList<ModuleDependency>();
        String moduleNames = deployment.getPropertyValue("lib"); //$NON-NLS-1$
        ModuleLoader callerModuleLoader = Module.getCallerModuleLoader();
        if (moduleNames != null) {
            StringTokenizer modules = new StringTokenizer(moduleNames);
            while (modules.hasMoreTokens()) {
                String moduleName = modules.nextToken().trim();
                try {
                    callerModuleLoader.loadModule(moduleName);
                    localDependencies.add(new ModuleDependency(callerModuleLoader, ModuleIdentifier.create(moduleName), false, false, false, false));
                } catch (ModuleLoadException e) {
                    // this is to handle JAR based deployments which take on name like "deployment.<jar-name>"
                    try {
                        sml.loadModule(moduleName);
                        userDependencies.add(new ModuleDependency(sml, ModuleIdentifier.create(moduleName), false, false, false, true));
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
