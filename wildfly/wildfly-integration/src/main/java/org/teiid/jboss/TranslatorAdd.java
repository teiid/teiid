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

import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP_ADDR;
import static org.teiid.jboss.TeiidConstants.TRANSLATOR_MODULE_ATTRIBUTE;
import static org.teiid.jboss.TeiidConstants.TRANSLATOR_SLOT_ATTRIBUTE;
import static org.teiid.jboss.TeiidConstants.asString;
import static org.teiid.jboss.TeiidConstants.isDefined;

import java.util.ServiceLoader;

import org.jboss.as.controller.AbstractAddStepHandler;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.PathAddress;
import org.jboss.dmr.ModelNode;
import org.jboss.modules.Module;
import org.jboss.modules.ModuleIdentifier;
import org.jboss.modules.ModuleLoadException;
import org.jboss.modules.ModuleLoader;
import org.jboss.msc.service.ServiceTarget;
import org.teiid.adminapi.impl.VDBTranslatorMetaData;
import org.teiid.deployers.TranslatorUtil;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.translator.ExecutionFactory;

class TranslatorAdd extends AbstractAddStepHandler {
    public static TranslatorAdd INSTANCE = new TranslatorAdd();

    @Override
    protected void populateModel(final ModelNode operation, final ModelNode model) throws OperationFailedException{
        TRANSLATOR_MODULE_ATTRIBUTE.validateAndSet(operation, model);
        TRANSLATOR_SLOT_ATTRIBUTE.validateAndSet(operation, model);
    }

    @Override
    protected void performRuntime(final OperationContext context,
            final ModelNode operation, final ModelNode model)
            throws OperationFailedException {

        final ModelNode address = operation.require(OP_ADDR);
        final PathAddress pathAddress = PathAddress.pathAddress(address);

        final String translatorName = pathAddress.getLastElement().getValue();

        String moduleName = null;
        if (isDefined(TRANSLATOR_MODULE_ATTRIBUTE, operation, context)) {
            moduleName = asString(TRANSLATOR_MODULE_ATTRIBUTE, operation, context);
        }

        String slot = null;
        if (isDefined(TRANSLATOR_SLOT_ATTRIBUTE, operation, context)) {
            slot = asString(TRANSLATOR_SLOT_ATTRIBUTE, operation, context);
        }

        final ServiceTarget target = context.getServiceTarget();

        final Module module;
        ClassLoader translatorLoader = this.getClass().getClassLoader();
        ModuleLoader ml = Module.getCallerModuleLoader();
        if (moduleName != null && ml != null) {
            try {
                ModuleIdentifier id = ModuleIdentifier.create(moduleName);
                if (slot != null) {
                    id = ModuleIdentifier.create(moduleName, slot);
                }
                module = ml.loadModule(id);
                translatorLoader = module.getClassLoader();
            } catch (ModuleLoadException e) {
                throw new OperationFailedException(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50007, moduleName, translatorName), e);
            }
        }

        boolean added = false;
        final ServiceLoader<ExecutionFactory> serviceLoader =  ServiceLoader.load(ExecutionFactory.class, translatorLoader);
        if (serviceLoader != null) {
            for (ExecutionFactory ef:serviceLoader) {
                VDBTranslatorMetaData metadata = TranslatorUtil.buildTranslatorMetadata(ef, moduleName);
                if (metadata == null) {
                    throw new OperationFailedException(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50008, translatorName));
                }

                metadata.addAttachment(ClassLoader.class, translatorLoader);
                if (translatorName.equalsIgnoreCase(metadata.getName())) {
                    LogManager.logInfo(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50006, metadata.getName()));

                    TranslatorDeployer.buildService(target, metadata);
                    added = true;
                    break;
                }
            }
        }

        if (!added) {
            throw new OperationFailedException(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50009, translatorName, moduleName));
        }
    }
}
