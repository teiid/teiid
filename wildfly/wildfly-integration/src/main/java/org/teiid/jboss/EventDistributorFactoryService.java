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

import org.jboss.msc.service.Service;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StartException;
import org.jboss.msc.service.StopContext;
import org.jboss.msc.value.InjectedValue;
import org.teiid.deployers.VDBRepository;
import org.teiid.dqp.internal.process.DQPCore;
import org.teiid.query.ObjectReplicator;
import org.teiid.services.AbstractEventDistributorFactoryService;
import org.teiid.services.InternalEventDistributorFactory;

public class EventDistributorFactoryService extends AbstractEventDistributorFactoryService implements Service<InternalEventDistributorFactory> {

    InjectedValue<ObjectReplicator> objectReplicatorInjector = new InjectedValue<ObjectReplicator>();
    InjectedValue<VDBRepository> vdbRepositoryInjector = new InjectedValue<VDBRepository>();
    DQPCore dqpCore;

    @Override
    public void start(StartContext context) throws StartException {
        start();
    }

    @Override
    public void stop(StopContext context) {
        stop();
    }

    @Override
    protected ObjectReplicator getObjectReplicator() {
        return objectReplicatorInjector.getValue();
    }

    @Override
    protected VDBRepository getVdbRepository() {
        return vdbRepositoryInjector.getValue();
    }

    @Override
    protected DQPCore getDQPCore() {
        return dqpCore;
    }

}
