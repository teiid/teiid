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

import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.SUBSYSTEM;

import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.SimpleResourceDefinition;
import org.jboss.as.controller.operations.common.GenericSubsystemDescribeHandler;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.as.controller.registry.OperationEntry;

public class TeiidSubsytemResourceDefinition extends SimpleResourceDefinition {
    protected static final PathElement PATH_SUBSYSTEM = PathElement.pathElement(SUBSYSTEM, TeiidExtension.TEIID_SUBSYSTEM);
    private boolean server;

    public TeiidSubsytemResourceDefinition(boolean server) {
        super(new Parameters(PATH_SUBSYSTEM, TeiidExtension.getResourceDescriptionResolver(TeiidExtension.TEIID_SUBSYSTEM))
              .setAddHandler(TeiidAdd.INSTANCE).setRemoveHandler(TeiidRemove.INSTANCE)
              .setAddRestartLevel(OperationEntry.Flag.RESTART_ALL_SERVICES)
              .setRemoveRestartLevel(OperationEntry.Flag.RESTART_ALL_SERVICES));
        this.server = server;
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
        new ReadTranslatorProperties().register(resourceRegistration);
    }

    @Override
    public void registerAttributes(ManagementResourceRegistration resourceRegistration) {
        if (this.server) {
            resourceRegistration.registerMetric(TeiidConstants.RUNTIME_VERSION, new GetRuntimeVersion(TeiidConstants.RUNTIME_VERSION.getName()));
            resourceRegistration.registerMetric(TeiidConstants.ACTIVE_SESSION_COUNT, new GetActiveSessionsCount(TeiidConstants.ACTIVE_SESSION_COUNT.getName()));
        }

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
