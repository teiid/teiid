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

import java.util.ServiceLoader;

import org.jboss.as.server.deployment.Attachments;
import org.jboss.as.server.deployment.DeploymentPhaseContext;
import org.jboss.as.server.deployment.DeploymentUnit;
import org.jboss.as.server.deployment.DeploymentUnitProcessingException;
import org.jboss.as.server.deployment.DeploymentUnitProcessor;
import org.jboss.modules.Module;
import org.jboss.msc.service.ServiceBuilder;
import org.jboss.msc.service.ServiceController;
import org.jboss.msc.service.ServiceName;
import org.jboss.msc.service.ServiceRegistry;
import org.jboss.msc.service.ServiceTarget;
import org.jboss.msc.service.ServiceController.Mode;
import org.teiid.adminapi.impl.VDBTranslatorMetaData;
import org.teiid.deployers.TranslatorUtil;
import org.teiid.deployers.VDBStatusChecker;
import org.teiid.dqp.internal.datamgr.TranslatorRepository;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.translator.ExecutionFactory;

/**
 * Deploy Translator from a JAR file
 */
public final class TranslatorDeployer implements DeploymentUnitProcessor {

	@Override
    public void deploy(final DeploymentPhaseContext phaseContext) throws DeploymentUnitProcessingException {
        final DeploymentUnit deploymentUnit = phaseContext.getDeploymentUnit();
        final ServiceTarget target = phaseContext.getServiceTarget();
        
        if (!TeiidAttachments.isTranslator(deploymentUnit)) {
        	return;
        }
        
        String moduleName = deploymentUnit.getName();
        final Module module = deploymentUnit.getAttachment(Attachments.MODULE);
        ClassLoader translatorLoader =  module.getClassLoader();
        
        final ServiceLoader<ExecutionFactory> serviceLoader =  ServiceLoader.load(ExecutionFactory.class, translatorLoader);
        if (serviceLoader != null) {
        	for (ExecutionFactory ef:serviceLoader) {
        		VDBTranslatorMetaData metadata = TranslatorUtil.buildTranslatorMetadata(ef, moduleName);        		
        		if (metadata == null) {
        			throw new DeploymentUnitProcessingException(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50070, moduleName)); 
        		}
        		deploymentUnit.putAttachment(TeiidAttachments.TRANSLATOR_METADATA, metadata);
        		metadata.addProperty(TranslatorUtil.DEPLOYMENT_NAME, moduleName);
        		metadata.addAttchment(ClassLoader.class, translatorLoader);
        		
        		LogManager.logInfo(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50006, metadata.getName()));
        		
        		buildService(target, metadata);
        	}
        }
    }

	static void buildService(final ServiceTarget target,
			VDBTranslatorMetaData metadata) {
		TranslatorService translatorService = new TranslatorService(metadata);
		ServiceBuilder<VDBTranslatorMetaData> builder = target.addService(TeiidServiceNames.translatorServiceName(metadata.getName()), translatorService);
		builder.addDependency(TeiidServiceNames.TRANSLATOR_REPO, TranslatorRepository.class, translatorService.repositoryInjector);
		builder.addDependency(TeiidServiceNames.VDB_STATUS_CHECKER, VDBStatusChecker.class, translatorService.statusCheckerInjector);
		builder.setInitialMode(ServiceController.Mode.ACTIVE).install();
	}

    @Override
    public void undeploy(final DeploymentUnit context) {
    	if (!TeiidAttachments.isTranslator(context)) {
        	return;
        }
    	VDBTranslatorMetaData metadata = context.getAttachment(TeiidAttachments.TRANSLATOR_METADATA);
    	if (metadata == null) {
    		return;
    	}
    	final ServiceRegistry registry = context.getServiceRegistry();
        final ServiceName serviceName = TeiidServiceNames.translatorServiceName(metadata.getName());
        final ServiceController<?> controller = registry.getService(serviceName);
        if (controller != null) {
        	controller.setMode(Mode.REMOVE);
        }
    }
}
