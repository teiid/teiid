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

import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.DESCRIPTION;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OPERATION_NAME;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.REMOVE;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.REQUEST_PROPERTIES;
import static org.teiid.jboss.Configuration.addAttribute;

import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;

import org.jboss.as.controller.AbstractAddStepHandler;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.ServiceVerificationHandler;
import org.jboss.as.controller.descriptions.DescriptionProvider;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;
import org.jboss.msc.service.ServiceController;
import org.jboss.msc.service.ServiceRegistry;

public class QueryEngineRemove extends AbstractAddStepHandler implements DescriptionProvider {

	@Override
	public ModelNode getModelDescription(Locale locale) {
        final ResourceBundle bundle = IntegrationPlugin.getResourceBundle(locale);
        final ModelNode operation = new ModelNode();
        operation.get(OPERATION_NAME).set(REMOVE);
        operation.get(DESCRIPTION).set(bundle.getString("engine.remove")); //$NON-NLS-1$    
        addAttribute(operation, Configuration.ENGINE_NAME, REQUEST_PROPERTIES, bundle.getString(Configuration.ENGINE_NAME+Configuration.DESC), ModelType.STRING, true, null);
        return operation;
	}
	
	@Override
	protected void populateModel(final ModelNode operation, final ModelNode model) throws OperationFailedException {
		final String name = model.require(Configuration.ENGINE_NAME).asString();
		model.get(Configuration.ENGINE_NAME).set(name);
	}
	
	@Override
    protected void performRuntime(final OperationContext context, final ModelNode operation, final ModelNode model,
            final ServiceVerificationHandler verificationHandler, final List<ServiceController<?>> newControllers) throws OperationFailedException {
		
		final String engineName = model.require(Configuration.ENGINE_NAME).asString();
        final ServiceRegistry registry = context.getServiceRegistry(true);
        final ServiceController<?> controller = registry.getService(TeiidServiceNames.translatorServiceName(engineName));
        if (controller != null) {
            controller.setMode(ServiceController.Mode.REMOVE);
        }
	}
}
