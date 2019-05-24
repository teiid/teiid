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
import org.jboss.as.server.deployment.module.ModuleDependency;
import org.jboss.as.server.deployment.module.ModuleSpecification;
import org.jboss.modules.Module;
import org.jboss.modules.ModuleIdentifier;
import org.jboss.modules.ModuleLoadException;
import org.jboss.modules.ModuleLoader;

public class TranslatorDependencyDeployer implements DeploymentUnitProcessor {

    @Override
    public void deploy(DeploymentPhaseContext phaseContext) throws DeploymentUnitProcessingException {
        DeploymentUnit deploymentUnit = phaseContext.getDeploymentUnit();
        try {
            final ModuleSpecification moduleSpecification = deploymentUnit.getAttachment(Attachments.MODULE_SPECIFICATION);
            final ModuleLoader moduleLoader = Module.getCallerModule().getModule(ModuleIdentifier.create("org.jboss.teiid")).getModuleLoader(); //$NON-NLS-1$
            moduleSpecification.addLocalDependency(new ModuleDependency(moduleLoader, ModuleIdentifier.create("org.jboss.teiid.api"), false, false, false, false)); //$NON-NLS-1$
            moduleSpecification.addLocalDependency(new ModuleDependency(moduleLoader, ModuleIdentifier.create("org.jboss.teiid.common-core"), false, false, false, false)); //$NON-NLS-1$
            moduleSpecification.addLocalDependency(new ModuleDependency(moduleLoader, ModuleIdentifier.create("javax.api"), false, false, false, false)); //$NON-NLS-1$
            moduleSpecification.addLocalDependency(new ModuleDependency(moduleLoader, ModuleIdentifier.create("javax.resource.api"), false, false, false, false)); //$NON-NLS-1$
        } catch (ModuleLoadException e) {
            throw new DeploymentUnitProcessingException(e);
        }
    }

    @Override
    public void undeploy(DeploymentUnit context) {
    }

}
