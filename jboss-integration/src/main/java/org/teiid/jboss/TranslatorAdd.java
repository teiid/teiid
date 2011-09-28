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

import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.*;
import static org.teiid.jboss.Configuration.addAttribute;

import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.ServiceLoader;

import org.jboss.as.controller.*;
import org.jboss.as.controller.descriptions.DescriptionProvider;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;
import org.jboss.modules.Module;
import org.jboss.modules.ModuleIdentifier;
import org.jboss.modules.ModuleLoadException;
import org.jboss.msc.service.ServiceBuilder;
import org.jboss.msc.service.ServiceController;
import org.jboss.msc.service.ServiceTarget;
import org.teiid.adminapi.impl.VDBTranslatorMetaData;
import org.teiid.deployers.TranslatorUtil;
import org.teiid.dqp.internal.datamgr.TranslatorRepository;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.translator.ExecutionFactory;

class TranslatorAdd extends AbstractAddStepHandler implements DescriptionProvider {

    @Override
    public ModelNode getModelDescription(final Locale locale) {
        final ResourceBundle bundle = IntegrationPlugin.getResourceBundle(locale);
        final ModelNode operation = new ModelNode();
        operation.get(OPERATION_NAME).set(ADD);
        operation.get(DESCRIPTION).set(bundle.getString("translator.add")); //$NON-NLS-1$
        
        addAttribute(operation, Configuration.TRANSLATOR_MODULE, REQUEST_PROPERTIES, bundle.getString(Configuration.TRANSLATOR_MODULE+Configuration.DESC), ModelType.STRING, true, null);
        return operation;
    }
    
	@Override
	protected void populateModel(final ModelNode operation, final ModelNode model) throws OperationFailedException{
        populate(operation, model);
	}
	
	static void populate(ModelNode operation, ModelNode model) {
		final String moduleName = operation.require(Configuration.TRANSLATOR_MODULE).asString();
		model.get(Configuration.TRANSLATOR_MODULE).set(moduleName);		
	}
	
	@Override
    protected void performRuntime(final OperationContext context, final ModelNode operation, final ModelNode model,
            final ServiceVerificationHandler verificationHandler, final List<ServiceController<?>> newControllers) throws OperationFailedException {

        final ModelNode address = operation.require(OP_ADDR);
        final PathAddress pathAddress = PathAddress.pathAddress(address);

    	final String translatorName = pathAddress.getLastElement().getValue();
		
        final String moduleName = operation.require(Configuration.TRANSLATOR_MODULE).asString();
		
        final ServiceTarget target = context.getServiceTarget();

        final Module module;
        try {
            module = Module.getCallerModuleLoader().loadModule(ModuleIdentifier.create(moduleName));
        } catch (ModuleLoadException e) {
            throw new OperationFailedException(e, new ModelNode().set(IntegrationPlugin.Util.getString("failed_load_module", moduleName, translatorName))); //$NON-NLS-1$
        }
        
        boolean added = false;
        final ServiceLoader<ExecutionFactory> serviceLoader = module.loadService(ExecutionFactory.class);
        if (serviceLoader != null) {
        	for (ExecutionFactory ef:serviceLoader) {
        		VDBTranslatorMetaData metadata = TranslatorUtil.buildTranslatorMetadata(ef, moduleName);
        		if (metadata == null) {
        			throw new OperationFailedException( new ModelNode().set(IntegrationPlugin.Util.getString("error_adding_translator", translatorName))); //$NON-NLS-1$ 
        		}
        		
        		if (translatorName.equalsIgnoreCase(metadata.getName())) {
	        		LogManager.logInfo(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.getString("translator.added", metadata.getName())); //$NON-NLS-1$
	        		
	        		TranslatorService translatorService = new TranslatorService(metadata);
	        		ServiceBuilder<VDBTranslatorMetaData> builder = target.addService(TeiidServiceNames.translatorServiceName(metadata.getName()), translatorService);
	        		builder.addDependency(TeiidServiceNames.TRANSLATOR_REPO, TranslatorRepository.class, translatorService.repositoryInjector);
	                builder.setInitialMode(ServiceController.Mode.ACTIVE).install();
	                added = true;
        		}
        	}
        }
        
        if (!added) {
        	throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString("translator.failed-to-load", translatorName, moduleName))); //$NON-NLS-1$
        }
    }    
}
