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

import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP_ADDR;

import org.jboss.as.controller.AbstractRemoveStepHandler;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.naming.deployment.ContextNames;
import org.jboss.dmr.ModelNode;
import org.jboss.msc.service.ServiceController;
import org.jboss.msc.service.ServiceName;
import org.jboss.msc.service.ServiceRegistry;
import org.teiid.transport.LocalServerConnection;

class TransportRemove extends AbstractRemoveStepHandler {
	public static TransportRemove INSTANCE = new TransportRemove();
	
	@Override
    protected void performRuntime(OperationContext context, ModelNode operation, ModelNode model) {

        final ModelNode address = operation.require(OP_ADDR);
        final PathAddress pathAddress = PathAddress.pathAddress(address);

    	String transportName = pathAddress.getLastElement().getValue();

    	final ServiceRegistry serviceRegistry = context.getServiceRegistry(true);
    	
    	ServiceName serviceName = TeiidServiceNames.transportServiceName(transportName);
		final ServiceController<?> controller = serviceRegistry.getService(serviceName);
		if (controller != null) {
			TransportService transport = TransportService.class.cast(controller.getValue());
			
			if (transport.isEmbedded()) {
				final ContextNames.BindInfo bindInfo = ContextNames.bindInfoFor(LocalServerConnection.jndiNameForRuntime(transportName));
				context.removeService(bindInfo.getBinderServiceName());
				context.removeService(TeiidServiceNames.embeddedTransportServiceName(transportName).append("reference-factory")); //$NON-NLS-1$
			}
			context.removeService(serviceName);
		}
    }
}
