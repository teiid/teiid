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
