/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2011, Red Hat, Inc., and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.teiid.jboss;

import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.DESCRIPTION;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OPERATION_NAME;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP_ADDR;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.REPLY_PROPERTIES;

import java.util.Locale;
import java.util.ResourceBundle;

import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.descriptions.DescriptionProvider;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.dmr.ModelNode;

public abstract class BaseOperationHandler<T> implements DescriptionProvider, OperationStepHandler {
	private static final String DESCRIBE = ".describe"; //$NON-NLS-1$
	private static final String REPLY = ".reply"; //$NON-NLS-1$
	protected static final String MISSING = ".missing"; //$NON-NLS-1$
	
	private String operationName; 
	
	protected BaseOperationHandler(String operationName){
		this.operationName = operationName;
	}
	
	public void register(ManagementResourceRegistration subsystem) {
		subsystem.registerOperationHandler(this.operationName, this, this);
	}
	
	public String name() {
		return this.operationName;
	}
	
    public void execute(OperationContext context, ModelNode operation) throws OperationFailedException {
        if (context.isNormalServer()) {
            context.addStep(new OperationStepHandler() {
                public void execute(OperationContext context, ModelNode operation) throws OperationFailedException {

                    final ModelNode address = operation.require(OP_ADDR);
                    final PathAddress pathAddress = PathAddress.pathAddress(address);
                    
                    executeOperation(context, getService(context, pathAddress, operation), operation);
                	
                	context.completeStep();
                }
                
            }, OperationContext.Stage.RUNTIME);            
        }
        context.completeStep();
    }
	
    @SuppressWarnings("unused")
	protected T getService(OperationContext context, PathAddress pathAddress, ModelNode operation) throws OperationFailedException{
    	return null;
    }
	
    @Override
    public ModelNode getModelDescription(final Locale locale) {
        final ResourceBundle bundle = IntegrationPlugin.getResourceBundle(locale);
        final ModelNode operation = new ModelNode();
        operation.get(OPERATION_NAME).set(this.operationName);
        operation.get(DESCRIPTION).set(bundle.getString(name()+DESCRIBE));
		
        ModelNode reply = operation.get(REPLY_PROPERTIES);
		reply.get(DESCRIPTION).set(bundle.getString(name()+REPLY));
		
        describeParameters(operation, bundle);
        return operation;
    }	
    
    protected String getParameterDescription(ResourceBundle bundle, String paramName) {
    	return bundle.getString(name()+"."+paramName+DESCRIBE); //$NON-NLS-1$ 
    }    
    
	abstract protected void executeOperation(OperationContext context, T service, ModelNode operation) throws OperationFailedException;
	
	protected void describeParameters(@SuppressWarnings("unused") ModelNode operationNode, @SuppressWarnings("unused")ResourceBundle bundle) {
	}
}
