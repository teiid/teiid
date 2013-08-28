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

import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.*;

import java.util.Locale;
import java.util.ResourceBundle;

import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationDefinition;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.SimpleOperationDefinitionBuilder;
import org.jboss.as.controller.descriptions.ModelDescriptionConstants;
import org.jboss.as.controller.descriptions.StandardResourceDescriptionResolver;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.as.controller.services.path.ResolvePathHandler;
import org.jboss.dmr.ModelNode;

public abstract class BaseOperationHandler<T> implements OperationStepHandler {
	private static final String DESCRIBE = ".describe"; //$NON-NLS-1$
	protected static final String MISSING = ".missing"; //$NON-NLS-1$
	protected static final String REPLY = ".reply"; //$NON-NLS-1$
	
	private String operationName; 
	
	protected BaseOperationHandler(String operationName){
		this.operationName = operationName;
	}
	
	public void register(ManagementResourceRegistration subsystem) {
		subsystem.registerOperationHandler(getOperationDefinition(), this);
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
                	
                	context.stepCompleted();
                }
                
            }, OperationContext.Stage.RUNTIME);            
        }
        context.stepCompleted();
    }
	
    @SuppressWarnings("unused")
	protected T getService(OperationContext context, PathAddress pathAddress, ModelNode operation) throws OperationFailedException{
    	return null;
    }
	
    public OperationDefinition getOperationDefinition() {
        SimpleOperationDefinitionBuilder builder = new SimpleOperationDefinitionBuilder(this.operationName, new TeiidResourceDescriptionResolver(this.operationName));
        builder.setRuntimeOnly();
        describeParameters(builder);
        return builder.build();
    }	
    
    static class TeiidResourceDescriptionResolver extends StandardResourceDescriptionResolver {
        private final String operationName;

        public ResourceBundle getResourceBundle(Locale locale) {
            if (locale == null) {
                locale = Locale.getDefault();
            }
            return IntegrationPlugin.getResourceBundle(locale);
        }

        @Override
        public String getResourceAttributeDescription(String attributeName, Locale locale, ResourceBundle bundle) {
        	return bundle.getString(attributeName);
        }
        
        
        public TeiidResourceDescriptionResolver(final String operationName) {
            super(ModelDescriptionConstants.PATH, IntegrationPlugin.BUNDLE_NAME, ResolvePathHandler.class.getClassLoader(), false, false);
            this.operationName = operationName;
        }

        @Override
        public String getOperationDescription(String operationName, Locale locale, ResourceBundle bundle) {
            if (this.operationName.equals(operationName)) {
                return bundle.getString(operationName+DESCRIBE);
            }
            return super.getOperationParameterDescription(operationName, operationName, locale, bundle);
        }

        @Override
        public String getOperationParameterDescription(final String operationName, final String paramName, final Locale locale, final ResourceBundle bundle) {
            if (this.operationName.equals(operationName)) {
           		return bundle.getString(this.operationName+"."+paramName+DESCRIBE); //$NON-NLS-1$
            }
            return super.getOperationParameterDescription(operationName, paramName, locale, bundle);
        }

        @Override
        public String getOperationReplyDescription(String operationName, Locale locale, ResourceBundle bundle) {
            if (this.operationName.equals(operationName)) {
                return bundle.getString(this.operationName+BaseOperationHandler.REPLY);
            }
            return super.getOperationReplyDescription(operationName, locale, bundle);
        }
    }    
    
	abstract protected void executeOperation(OperationContext context, T service, ModelNode operation) throws OperationFailedException;
	
	protected void describeParameters(@SuppressWarnings("unused") SimpleOperationDefinitionBuilder builder) {
	}
	
}
