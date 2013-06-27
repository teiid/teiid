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

import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.SimpleResourceDefinition;
import org.jboss.as.controller.operations.common.GenericSubsystemDescribeHandler;
import org.jboss.as.controller.registry.ManagementResourceRegistration;

public class TeiidSubsytemResourceDefinition extends SimpleResourceDefinition {
	protected static final PathElement PATH_SUBSYSTEM = PathElement.pathElement(SUBSYSTEM, TeiidExtension.TEIID_SUBSYSTEM);

	public TeiidSubsytemResourceDefinition() {
		super(PATH_SUBSYSTEM,TeiidExtension.getResourceDescriptionResolver(TeiidExtension.TEIID_SUBSYSTEM),TeiidAdd.INSTANCE, TeiidRemove.INSTANCE);
	}

    @Override
    public void registerOperations(ManagementResourceRegistration resourceRegistration) {
        super.registerOperations(resourceRegistration);
        resourceRegistration.registerOperationHandler(GenericSubsystemDescribeHandler.DEFINITION,  GenericSubsystemDescribeHandler.INSTANCE);
        
		// teiid level admin api operation handlers
		new GetTranslator().register(resourceRegistration);
		new ListTranslators().register(resourceRegistration);
		new ListVDBs().register(resourceRegistration);
		new GetVDB().register(resourceRegistration);
		new CacheTypes().register(resourceRegistration);
		new ClearCache().register(resourceRegistration);
		new CacheStatistics().register(resourceRegistration);
		new AddDataRole().register(resourceRegistration);
		new RemoveDataRole().register(resourceRegistration);
		new AddAnyAuthenticatedDataRole().register(resourceRegistration);
		new RestartVDB().register(resourceRegistration);
		new AssignDataSource().register(resourceRegistration);
		new UpdateSource().register(resourceRegistration);
		new RemoveSource().register(resourceRegistration);
		new AddSource().register(resourceRegistration);
		new ChangeVDBConnectionType().register(resourceRegistration);
		new RemoveAnyAuthenticatedDataRole().register(resourceRegistration);
		new ListRequests().register(resourceRegistration);
		new ListSessions().register(resourceRegistration);
		new ListRequestsPerSession().register(resourceRegistration);
		new ListRequestsPerVDB().register(resourceRegistration);
		new ListLongRunningRequests().register(resourceRegistration);
		new TerminateSession().register(resourceRegistration);
		new CancelRequest().register(resourceRegistration);
		new GetPlan().register(resourceRegistration);
		new WorkerPoolStatistics().register(resourceRegistration);
		new ListTransactions().register(resourceRegistration);
		new TerminateTransaction().register(resourceRegistration);
		new ExecuteQuery().register(resourceRegistration);
		new MarkDataSourceAvailable().register(resourceRegistration);
		new ReadRARDescription().register(resourceRegistration);
		new GetSchema().register(resourceRegistration);
		new EngineStatistics().register(resourceRegistration);        
    }

    @Override
    public void registerAttributes(ManagementResourceRegistration resourceRegistration) {
    	resourceRegistration.registerReadOnlyAttribute(TeiidConstants.RUNTIME_VERSION, new GetRuntimeVersion(TeiidConstants.RUNTIME_VERSION.getName())); 
    	resourceRegistration.registerReadOnlyAttribute(TeiidConstants.ACTIVE_SESSION_COUNT, new GetActiveSessionsCount(TeiidConstants.ACTIVE_SESSION_COUNT.getName()));
    	
		for (int i = 0; i < TeiidAdd.ATTRIBUTES.length; i++) {
			resourceRegistration.registerReadWriteAttribute(TeiidAdd.ATTRIBUTES[i], null, new AttributeWrite(TeiidAdd.ATTRIBUTES[i]));
		}    	
    }

    @Override
    public void registerChildren(ManagementResourceRegistration resourceRegistration) {
    	resourceRegistration.registerSubModel(new TranslatorResourceDefinition());
    	resourceRegistration.registerSubModel(new TransportResourceDefinition());
    }
}
