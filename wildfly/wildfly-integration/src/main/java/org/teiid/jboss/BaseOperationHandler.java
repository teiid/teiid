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

import java.util.Locale;
import java.util.ResourceBundle;

import org.jboss.as.controller.*;
import org.jboss.as.controller.descriptions.ModelDescriptionConstants;
import org.jboss.as.controller.descriptions.StandardResourceDescriptionResolver;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.as.controller.services.path.ResolvePathHandler;
import org.jboss.dmr.ModelNode;

public abstract class BaseOperationHandler<T> implements OperationStepHandler {
    /*
    public static final SensitivityClassification ACCESS_CONTROL = new SensitivityClassification(TeiidExtension.TEIID_SUBSYSTEM, "access-control", false, true, true); //$NON-NLS-1$
    public static final SensitiveTargetAccessConstraintDefinition ACCESS_CONTROL_DEF = new SensitiveTargetAccessConstraintDefinition(ACCESS_CONTROL);
    */

    private static final String DESCRIBE = ".describe"; //$NON-NLS-1$
    protected static final String MISSING = ".missing"; //$NON-NLS-1$
    protected static final String REPLY = ".reply"; //$NON-NLS-1$

    private String operationName;
    // this is flaf indicates that changes the runtime state of a service
    private boolean changesRuntime = false;

    protected BaseOperationHandler(String operationName){
        this.operationName = operationName;
    }

    protected BaseOperationHandler(String operationName, boolean changesRuntime){
        this.operationName = operationName;
        this.changesRuntime = changesRuntime;
    }

    public void register(ManagementResourceRegistration subsystem) {
        subsystem.registerOperationHandler(getOperationDefinition(), this);
    }

    public String name() {
        return this.operationName;
    }

    public boolean isChangesRuntimes() {
        return this.changesRuntime;
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
        /*builder.setAccessConstraints(ACCESS_CONTROL_DEF);*/
        //if (!isChangesRuntimes()) {
        //    builder.setReadOnly();
        //}
        describeParameters(builder);
        return builder.build();
    }

    static class TeiidResourceDescriptionResolver extends StandardResourceDescriptionResolver {
        private final String operationName;

        @Override
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
