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

import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.SimpleResourceDefinition;
import org.jboss.as.controller.operations.common.GenericSubsystemDescribeHandler;
import org.jboss.as.controller.registry.ManagementResourceRegistration;

public class TranslatorResourceDefinition extends SimpleResourceDefinition {
    public static final PathElement TRANSLATOR_PATH = PathElement.pathElement(Element.TRANSLATOR_ELEMENT.getLocalName());
    /*
    private final List<AccessConstraintDefinition> accessConstraints;
    */
    public TranslatorResourceDefinition() {
        super(TRANSLATOR_PATH, TeiidExtension.getResourceDescriptionResolver(Element.TRANSLATOR_ELEMENT.getLocalName()),
                TranslatorAdd.INSTANCE,
                TranslatorRemove.INSTANCE);
        /*
        ApplicationTypeConfig atc = new ApplicationTypeConfig(TeiidExtension.TEIID_SUBSYSTEM, Element.TRANSLATOR_ELEMENT.getLocalName());
        this.accessConstraints = new ApplicationTypeAccessConstraintDefinition(atc).wrapAsList();
        */
    }

    @Override
    public void registerOperations(ManagementResourceRegistration resourceRegistration) {
        super.registerOperations(resourceRegistration);
        resourceRegistration.registerOperationHandler(GenericSubsystemDescribeHandler.DEFINITION,  GenericSubsystemDescribeHandler.INSTANCE);
    }

    @Override
    public void registerAttributes(ManagementResourceRegistration resourceRegistration) {
        resourceRegistration.registerReadWriteAttribute(TeiidConstants.TRANSLATOR_MODULE_ATTRIBUTE, null, new AttributeWrite(TeiidConstants.TRANSLATOR_MODULE_ATTRIBUTE));
        resourceRegistration.registerReadWriteAttribute(TeiidConstants.TRANSLATOR_SLOT_ATTRIBUTE, null, new AttributeWrite(TeiidConstants.TRANSLATOR_SLOT_ATTRIBUTE));
    }

    @Override
    public void registerChildren(ManagementResourceRegistration resourceRegistration) {
    }

    /*
    @Override
    public List<AccessConstraintDefinition> getAccessConstraints() {
        return this.accessConstraints;
    }
    */
}
