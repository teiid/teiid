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

import org.jboss.as.controller.AbstractRemoveStepHandler;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.descriptions.DescriptionProvider;
import org.jboss.as.naming.deployment.ContextNames;
import org.jboss.dmr.ModelNode;
import org.jboss.msc.service.ServiceController;
import org.jboss.msc.service.ServiceName;
import org.jboss.msc.service.ServiceRegistry;
import org.teiid.transport.LocalServerConnection;

class QueryEngineRemove extends AbstractRemoveStepHandler implements DescriptionProvider {

	@Override
    protected void performRuntime(OperationContext context, ModelNode operation, ModelNode model) {

        final ModelNode address = operation.require(OP_ADDR);
        final PathAddress pathAddress = PathAddress.pathAddress(address);

    	String engineName = pathAddress.getLastElement().getValue();

    	final ServiceRegistry serviceRegistry = context.getServiceRegistry(true);
    	ServiceName serviceName = TeiidServiceNames.engineServiceName(engineName);
		final ServiceController<?> controller = serviceRegistry.getService(serviceName);
		if (controller != null) {			 
			 context.removeService(serviceName);
		}

		final ServiceName referenceFactoryServiceName = TeiidServiceNames.engineServiceName(engineName).append("reference-factory"); //$NON-NLS-1$
		final ServiceController<?> referceFactoryController = serviceRegistry.getService(referenceFactoryServiceName);
		if (referceFactoryController != null) {			 
			 context.removeService(referenceFactoryServiceName);
		}
		
        final ContextNames.BindInfo bindInfo = ContextNames.bindInfoFor(LocalServerConnection.TEIID_RUNTIME_CONTEXT+engineName);
        final ServiceController<?> binderController = serviceRegistry.getService(bindInfo.getBinderServiceName());
        if (binderController != null) {
        	context.removeService(bindInfo.getBinderServiceName());
        }
    }

	@Override
	public ModelNode getModelDescription(Locale locale) {
        final ResourceBundle bundle = IntegrationPlugin.getResourceBundle(locale);
        final ModelNode operation = new ModelNode();
        operation.get(OPERATION_NAME).set(REMOVE);
        operation.get(DESCRIPTION).set(bundle.getString(REMOVE+DESCRIBE));
        return operation;
	}
    
}
