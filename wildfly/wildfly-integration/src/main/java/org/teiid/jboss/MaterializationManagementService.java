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

import java.util.concurrent.ScheduledExecutorService;

import org.jboss.msc.service.Service;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StartException;
import org.jboss.msc.service.StopContext;
import org.jboss.msc.value.InjectedValue;
import org.teiid.deployers.VDBRepository;
import org.teiid.dqp.internal.process.DQPCore;
import org.teiid.runtime.MaterializationManager;

class MaterializationManagementService implements Service<MaterializationManager> {
    private ScheduledExecutorService scheduler;
    private MaterializationManager manager;
    protected final InjectedValue<DQPCore> dqpInjector = new InjectedValue<DQPCore>();
    protected final InjectedValue<VDBRepository> vdbRepositoryInjector = new InjectedValue<VDBRepository>();
    protected final InjectedValue<NodeTracker> nodeTrackerInjector = new InjectedValue<NodeTracker>();
    private JBossLifeCycleListener shutdownListener;

    public MaterializationManagementService(JBossLifeCycleListener shutdownListener, ScheduledExecutorService scheduler) {
        this.shutdownListener = shutdownListener;
        this.scheduler = scheduler;
    }

    @Override
    public void start(StartContext context) throws StartException {
        manager = new MaterializationManager(shutdownListener) {
            @Override
            public ScheduledExecutorService getScheduledExecutorService() {
                return scheduler;
            }

            @Override
            public DQPCore getDQP() {
                return dqpInjector.getValue();
            }

            @Override
            public VDBRepository getVDBRepository() {
                return vdbRepositoryInjector.getValue();
            }
        };

        vdbRepositoryInjector.getValue().addListener(manager);

        if (nodeTrackerInjector.getValue() != null) {
            nodeTrackerInjector.getValue().addNodeListener(manager);
        }
    }

    @Override
    public void stop(StopContext context) {
        scheduler.shutdownNow();
        vdbRepositoryInjector.getValue().removeListener(manager);
        NodeTracker value = nodeTrackerInjector.getValue();
        if (value != null) {
            value.removeNodeListener(manager);
        }
    }

    @Override
    public MaterializationManager getValue() throws IllegalStateException, IllegalArgumentException {
        return this.manager;
    }
}
