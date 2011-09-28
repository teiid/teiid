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
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.descriptions.DescriptionProvider;
import org.jboss.as.controller.descriptions.ModelDescriptionConstants;
import org.jboss.as.controller.registry.Resource;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.Property;

public class TeiidSubsystemDescribe implements OperationStepHandler, DescriptionProvider {

	@Override
	public ModelNode getModelDescription(Locale locale) {
		final ResourceBundle bundle = IntegrationPlugin.getResourceBundle(locale);
		
        ModelNode node = new ModelNode();
        node.get(ModelDescriptionConstants.DESCRIPTION).set("teiid subsystem"); //$NON-NLS-1$
        node.get(ModelDescriptionConstants.HEAD_COMMENT_ALLOWED).set(true);
        node.get(ModelDescriptionConstants.TAIL_COMMENT_ALLOWED).set(true);
        node.get(ModelDescriptionConstants.NAMESPACE).set(Namespace.CURRENT.getUri());
        
        TeiidBootServicesAdd.describeTeiidRoot(bundle, ATTRIBUTES, node);
        node.get(CHILDREN, Configuration.QUERY_ENGINE, DESCRIPTION).set(bundle.getString(Configuration.QUERY_ENGINE+Configuration.DESC)); 
        node.get(CHILDREN, Configuration.QUERY_ENGINE, REQUIRED).set(true);
        
        node.get(CHILDREN, Configuration.TRANSLATOR, DESCRIPTION).set(bundle.getString(Configuration.TRANSLATOR+Configuration.DESC));
        node.get(CHILDREN, Configuration.TRANSLATOR, REQUIRED).set(true);

        return node;
    }

    public void execute(OperationContext context, ModelNode operation) throws OperationFailedException {
        ModelNode result = context.getResult();

        PathAddress rootAddress = PathAddress.pathAddress(PathAddress.pathAddress(operation.require(ModelDescriptionConstants.OP_ADDR)).getLastElement());
        ModelNode subModel = Resource.Tools.readModel(context.readResource(PathAddress.EMPTY_ADDRESS));

        final ModelNode subsystemAdd = new ModelNode();
        subsystemAdd.get(OP).set(ADD);
        subsystemAdd.get(OP_ADDR).set(rootAddress.toModelNode());

        TeiidBootServicesAdd.populate(subModel, subsystemAdd);
        result.add(subsystemAdd);
        
        if (subModel.hasDefined(Configuration.QUERY_ENGINE)) {
            for (Property container : subModel.get(Configuration.QUERY_ENGINE).asPropertyList()) {
                ModelNode address = rootAddress.toModelNode();
                address.add(Configuration.QUERY_ENGINE, container.getName());
                
                final ModelNode addOperation = new ModelNode();
                addOperation.get(OP).set(ADD);
                addOperation.get(OP_ADDR).set(address);
                
                QueryEngineAdd.populate(container.getValue(), addOperation);
                
                result.add(addOperation);
            }
        }
        
        if (subModel.hasDefined(Configuration.TRANSLATOR)) {
            for (Property container : subModel.get(Configuration.TRANSLATOR).asPropertyList()) {
                ModelNode address = rootAddress.toModelNode();
                address.add(Configuration.TRANSLATOR, container.getName());
                
                final ModelNode addOperation = new ModelNode();
                addOperation.get(OP).set(ADD);
                addOperation.get(OP_ADDR).set(address);
                
                TranslatorAdd.populate(container.getValue(), addOperation);
                
                result.add(addOperation);
            }
        }        

        context.completeStep();
    }
}
