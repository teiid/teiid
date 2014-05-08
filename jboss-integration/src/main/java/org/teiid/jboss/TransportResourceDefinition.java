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

import java.util.List;

import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.SimpleResourceDefinition;
import org.jboss.as.controller.access.constraint.ApplicationTypeConfig;
import org.jboss.as.controller.access.management.AccessConstraintDefinition;
import org.jboss.as.controller.access.management.ApplicationTypeAccessConstraintDefinition;
import org.jboss.as.controller.operations.common.GenericSubsystemDescribeHandler;
import org.jboss.as.controller.registry.ManagementResourceRegistration;

class TransportResourceDefinition extends SimpleResourceDefinition {
	public static final PathElement TRANSPORT_PATH = PathElement.pathElement(Element.TRANSPORT_ELEMENT.getLocalName());
	private final List<AccessConstraintDefinition> accessConstraints;
	
	public TransportResourceDefinition() {
		super(TRANSPORT_PATH, TeiidExtension.getResourceDescriptionResolver(Element.TRANSPORT_ELEMENT.getLocalName()), 
				TransportAdd.INSTANCE,
				TransportRemove.INSTANCE);
        ApplicationTypeConfig atc = new ApplicationTypeConfig(TeiidExtension.TEIID_SUBSYSTEM, Element.TRANSPORT_ELEMENT.getLocalName());
        this.accessConstraints = new ApplicationTypeAccessConstraintDefinition(atc).wrapAsList();       
	}
	
    @Override
    public void registerOperations(ManagementResourceRegistration resourceRegistration) {
        super.registerOperations(resourceRegistration);
        resourceRegistration.registerOperationHandler(GenericSubsystemDescribeHandler.DEFINITION,  GenericSubsystemDescribeHandler.INSTANCE);
    }

    @Override
    public void registerAttributes(ManagementResourceRegistration resourceRegistration) {
		for (int i = 0; i < TransportAdd.ATTRIBUTES.length; i++) {
			resourceRegistration.registerReadWriteAttribute(TransportAdd.ATTRIBUTES[i], null, new AttributeWrite(TransportAdd.ATTRIBUTES[i]));
		}    	
    }

    @Override
    public void registerChildren(ManagementResourceRegistration resourceRegistration) {
    }
    
    @Override
    public List<AccessConstraintDefinition> getAccessConstraints() {
        return this.accessConstraints;
    }     
}
