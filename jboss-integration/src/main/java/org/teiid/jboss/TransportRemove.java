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

import org.jboss.as.controller.AbstractRemoveStepHandler;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
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
    protected void performRuntime(OperationContext context,
            final ModelNode operation, final ModelNode model)
            throws OperationFailedException {
        final ModelNode address = operation.require(OP_ADDR);
        final PathAddress pathAddress = PathAddress.pathAddress(address);

    	String transportName = pathAddress.getLastElement().getValue();

    	final ServiceRegistry serviceRegistry = context.getServiceRegistry(true);
    	
    	ServiceName serviceName = TeiidServiceNames.transportServiceName(transportName);
		final ServiceController<?> controller = serviceRegistry.getService(serviceName);
		if (controller != null) {
			TransportService transport = TransportService.class.cast(controller.getValue());
			
			if (transport.isLocal()) {
				final ContextNames.BindInfo bindInfo = ContextNames.bindInfoFor(LocalServerConnection.jndiNameForRuntime(transportName));
				context.removeService(bindInfo.getBinderServiceName());
				context.removeService(TeiidServiceNames.localTransportServiceName(transportName).append("reference-factory")); //$NON-NLS-1$
			}
			context.removeService(serviceName);
		}
    }
}
