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
package org.teiid.services;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.teiid.Replicated;
import org.teiid.deployers.EventDistributorImpl;
import org.teiid.deployers.VDBRepository;
import org.teiid.dqp.internal.process.DQPCore;
import org.teiid.events.EventDistributor;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.query.ObjectReplicator;
import org.teiid.runtime.RuntimePlugin;

public abstract class AbstractEventDistributorFactoryService implements InternalEventDistributorFactory {

    private EventDistributor replicatableEventDistributor;
    private EventDistributor eventDistributorProxy;

    public InternalEventDistributorFactory getValue() throws IllegalStateException, IllegalArgumentException {
        return this;
    }

    protected abstract VDBRepository getVdbRepository();
    protected abstract ObjectReplicator getObjectReplicator();
    protected abstract DQPCore getDQPCore();

    public void start() {
        final EventDistributor ed = new EventDistributorImpl() {
            @Override
            public VDBRepository getVdbRepository() {
                return AbstractEventDistributorFactoryService.this.getVdbRepository();
            }

            @Override
            public DQPCore getDQPCore() {
                return AbstractEventDistributorFactoryService.this.getDQPCore();
            }
        };

        ObjectReplicator objectReplicator = getObjectReplicator();
        // this instance is by use of teiid internally; only invokes the remote instances
        if (objectReplicator != null) {
            try {
                this.replicatableEventDistributor = objectReplicator.replicate("$TEIID_ED$", EventDistributor.class, ed, 0); //$NON-NLS-1$
            } catch (Exception e) {
                LogManager.logError(LogConstants.CTX_RUNTIME, e, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40088, this));
            }
        }

        // for external client to call. invokes local instance and remote ones too.
        this.eventDistributorProxy = (EventDistributor)Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class[] {EventDistributor.class}, new InvocationHandler() {

            @Override
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                Replicated annotation = method.getAnnotation(Replicated.class);
                Object result = null;
                try {
                    if (replicatableEventDistributor == null || (annotation != null && annotation.remoteOnly())) {
                        result = method.invoke(ed, args);
                    }
                    if (replicatableEventDistributor != null) {
                        result = method.invoke(replicatableEventDistributor, args);
                    }
                } catch (InvocationTargetException e) {
                    throw e.getTargetException();
                }
                return result;
            }
        });
    }

    public void stop() {
        ObjectReplicator objectReplicator = getObjectReplicator();
        if (objectReplicator != null && this.replicatableEventDistributor != null) {
            objectReplicator.stop(this.replicatableEventDistributor);
            this.replicatableEventDistributor = null;
        }
    }

    @Override
    public EventDistributor getReplicatedEventDistributor() {
        return replicatableEventDistributor;
    }

    @Override
    public EventDistributor getEventDistributor() {
        return eventDistributorProxy;
    }
}
