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

import java.util.Locale;
import java.util.ResourceBundle;

import org.jboss.as.controller.*;
import org.jboss.as.controller.descriptions.DescriptionProvider;
import org.jboss.as.controller.operations.common.Util;
import org.jboss.dmr.ModelNode;
import org.jboss.msc.service.ServiceController;
import org.jboss.msc.service.ServiceRegistry;

public class TranslatorRemove implements DescriptionProvider, ModelAddOperationHandler {

	@Override
	public ModelNode getModelDescription(Locale locale) {
        final ResourceBundle bundle = IntegrationPlugin.getResourceBundle(locale);
        final ModelNode operation = new ModelNode();
        operation.get(OPERATION_NAME).set(REMOVE);
        operation.get(DESCRIPTION).set(bundle.getString("translator.add")); //$NON-NLS-1$        
        return operation;
	}

	@Override
	public OperationResult execute(final OperationContext context, final ModelNode operation, final ResultHandler resultHandler) throws OperationFailedException {
        final ModelNode opAddr = operation.require(OP_ADDR);

        // Compensating is add
        final ModelNode model = context.getSubModel();
        final String translatorName = model.get(Configuration.TRANSLATOR_NAME).asString();

        final ModelNode compensating = Util.getEmptyOperation(ADD, opAddr);
        compensating.get(Configuration.TRANSLATOR_NAME).set(translatorName);

        if (context.getRuntimeContext() != null) {
            context.getRuntimeContext().setRuntimeTask(new RuntimeTask() {
                public void execute(final RuntimeTaskContext context) throws OperationFailedException {
                    final ServiceRegistry registry = context.getServiceRegistry();
                    final ServiceController<?> controller = registry.getService(TeiidServiceNames.translatorServiceName(translatorName));
                    if (controller != null) {
                        controller.setMode(ServiceController.Mode.REMOVE);
                    }
                    resultHandler.handleResultComplete();
                }
            });
        } else {
            resultHandler.handleResultComplete();
        }
        return new BasicOperationResult(compensating);
    }
}
