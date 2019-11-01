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
                metadata.addAttachment(ClassLoader.class, translatorLoader);

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
