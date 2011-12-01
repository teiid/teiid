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

import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.ADD;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.DESCRIPTION;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OPERATION_NAME;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP_ADDR;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.REQUEST_PROPERTIES;

import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.ServiceLoader;

import org.jboss.as.controller.AbstractAddStepHandler;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.ServiceVerificationHandler;
import org.jboss.as.controller.descriptions.DescriptionProvider;
import org.jboss.dmr.ModelNode;
import org.jboss.modules.Module;
import org.jboss.modules.ModuleIdentifier;
import org.jboss.modules.ModuleLoadException;
import org.jboss.modules.ModuleLoader;
import org.jboss.msc.service.ServiceBuilder;
import org.jboss.msc.service.ServiceController;
import org.jboss.msc.service.ServiceTarget;
import org.teiid.adminapi.impl.VDBTranslatorMetaData;
import org.teiid.deployers.TranslatorUtil;
import org.teiid.deployers.VDBStatusChecker;
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
        
        Element.TRANSLATOR_MODULE_ATTRIBUTE.describe(operation, REQUEST_PROPERTIES, bundle);
        return operation;
    }
    
	@Override
	protected void populateModel(final ModelNode operation, final ModelNode model) throws OperationFailedException{
        populate(operation, model);
	}
	
	static void populate(ModelNode operation, ModelNode model) {
		Element.TRANSLATOR_MODULE_ATTRIBUTE.populate(operation, model);
	}
	
	@Override
    protected void performRuntime(final OperationContext context, final ModelNode operation, final ModelNode model,
            final ServiceVerificationHandler verificationHandler, final List<ServiceController<?>> newControllers) throws OperationFailedException {

        final ModelNode address = operation.require(OP_ADDR);
        final PathAddress pathAddress = PathAddress.pathAddress(address);

    	final String translatorName = pathAddress.getLastElement().getValue();
		
    	String moduleName = null;
    	if (Element.TRANSLATOR_MODULE_ATTRIBUTE.isDefined(operation)) {
    		moduleName = Element.TRANSLATOR_MODULE_ATTRIBUTE.asString(operation);
    	}
		
        final ServiceTarget target = context.getServiceTarget();

        final Module module;
        ClassLoader translatorLoader = this.getClass().getClassLoader();
        ModuleLoader ml = Module.getCallerModuleLoader();
        if (moduleName != null && ml != null) {
	        try {
            	module = ml.loadModule(ModuleIdentifier.create(moduleName));
            	translatorLoader = module.getClassLoader();
	        } catch (ModuleLoadException e) {
	            throw new OperationFailedException(e, new ModelNode().set(IntegrationPlugin.Util.getString("failed_load_module", moduleName, translatorName))); //$NON-NLS-1$
	        }
        }
        
        boolean added = false;
        final ServiceLoader<ExecutionFactory> serviceLoader =  ServiceLoader.load(ExecutionFactory.class, translatorLoader);
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
	        		builder.addDependency(TeiidServiceNames.VDB_STATUS_CHECKER, VDBStatusChecker.class, translatorService.statusCheckerInjector);
	        		newControllers.add(builder.setInitialMode(ServiceController.Mode.ACTIVE).install());
	                added = true;
	                break;
        		}
        	}
        }
        
        if (!added) {
        	throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString("translator.failed-to-load", translatorName, moduleName))); //$NON-NLS-1$
        }
    }    
}
