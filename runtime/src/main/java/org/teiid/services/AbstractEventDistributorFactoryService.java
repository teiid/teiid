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
package org.teiid.services;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.teiid.adminapi.Admin;
import org.teiid.deployers.EventDistributorImpl;
import org.teiid.deployers.VDBRepository;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository.ExecutionFactoryProvider;
import org.teiid.events.EventDistributor;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.MetadataException;
import org.teiid.query.ObjectReplicator;
import org.teiid.runtime.RuntimePlugin;

public abstract class AbstractEventDistributorFactoryService implements InternalEventDistributorFactory {
	
	private EventDistributor replicatableEventDistributor;
	private EventDistributor eventDistributorProxy;
	
	public InternalEventDistributorFactory getValue() throws IllegalStateException, IllegalArgumentException {
		return this;
	}
	
	protected abstract Admin getAdmin();
	protected abstract VDBRepository getVdbRepository();
	protected abstract ObjectReplicator getObjectReplicator();
	protected abstract ConnectorManagerRepository getConnectorManagerRepository();
	protected abstract ClassLoader getClassLoader(String[] path) throws MetadataException;
	
	public void start() {
		final EventDistributor ed = new EventDistributorImpl() {
			@Override
			public VDBRepository getVdbRepository() {
				return AbstractEventDistributorFactoryService.this.getVdbRepository();
			}

            @Override
            public ExecutionFactoryProvider getExecutionFactoryProvider() {
                return AbstractEventDistributorFactoryService.this.getConnectorManagerRepository().getProvider();
            }

            @Override
            public ConnectorManagerRepository getConnectorManagerRepository() {
                return AbstractEventDistributorFactoryService.this.getConnectorManagerRepository();
            }

            @Override
            public Admin getAdmin() {
                return AbstractEventDistributorFactoryService.this.getAdmin();
            }

            @Override
            public ClassLoader getClassLoader(String[] path) {
                return AbstractEventDistributorFactoryService.this.getClassLoader(path);
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
				try {
					method.invoke(ed, args);
					if (replicatableEventDistributor != null) {
						method.invoke(replicatableEventDistributor, args);
					}
				} catch (InvocationTargetException e) {
					throw e.getTargetException();
				}
				return null;
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
